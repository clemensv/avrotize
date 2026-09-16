
import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

from fastavro.schema import parse_schema

from avrotize.avrotothrift import convert_avro_to_thrift
from avrotize.thriftstructure import convert_structure_to_thrift, convert_thrift_to_structure
from avrotize.thrifttoavro import convert_thrift_to_avro


class TestThriftConversion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[1]
        cls.fixture = cls.root / "test" / "thrift" / "complex.thrift"
        cls.out_dir = cls.root / "test" / "thrift" / "out"
        cls.out_dir.mkdir(parents=True, exist_ok=True)
        cls.avro_path = cls.out_dir / "complex.avsc"
        convert_thrift_to_avro(str(cls.fixture), str(cls.avro_path))
        with cls.avro_path.open(encoding="utf-8") as f:
            cls.schema = json.load(f)
        cls.by_name = {item["name"]: item for item in cls.schema if isinstance(item, dict) and "name" in item}

    @classmethod
    def tearDownClass(cls):
        if cls.out_dir.exists():
            shutil.rmtree(cls.out_dir)

    def record(self, name):
        return self.by_name[name]

    def field(self, record_name, field_name):
        record = self.record(record_name)
        return next(f for f in record["fields"] if f["name"] == field_name)

    def test_import_resolves_to_worktree(self):
        import avrotize
        self.assertTrue(str(Path(avrotize.__file__).resolve()).startswith(str(self.root)))

    def test_generated_avro_is_valid_fastavro(self):
        parse_schema(self.schema)

    def test_namespace_uses_star_namespace(self):
        self.assertEqual(self.record("User")["namespace"], "com.example.thrift")

    def test_base_type_mapping(self):
        expected = {
            "enabled": "boolean",
            "b": "int",
            "tiny": "int",
            "small": "int",
            "medium": "int",
            "large": "long",
            "ratio": "double",
            "payload": "bytes",
            "name": ["null", "string"],
        }
        for field_name, avro_type in expected.items():
            self.assertEqual(self.field("User", field_name)["type"], avro_type)

    def test_required_and_optional_defaults(self):
        self.assertEqual(self.field("User", "enabled")["default"], True)
        self.assertNotIn("default", self.field("User", "id"))
        self.assertEqual(self.field("User", "name")["default"], None)

    def test_field_ids_are_preserved_as_annotations(self):
        self.assertEqual(self.field("User", "addresses")["thrift_id"], 13)

    def test_typedef_is_resolved(self):
        self.assertEqual(self.field("User", "id")["type"], "long")
        self.assertEqual(self.field("SearchKey", "id")["type"], ["null", "long"])

    def test_enum_ordinals_are_preserved(self):
        status = self.record("Status")
        self.assertEqual(status["type"], "enum")
        self.assertEqual(status["symbols"], ["UNKNOWN", "ACTIVE", "SUSPENDED"])
        self.assertEqual(status["ordinals"], {"UNKNOWN": 0, "ACTIVE": 10, "SUSPENDED": 20})

    def test_struct_and_exception_become_records(self):
        self.assertEqual(self.record("Address")["type"], "record")
        self.assertEqual(self.record("ServiceError")["type"], "record")
        self.assertIn("Thrift exception", self.record("ServiceError")["doc"])

    def test_union_becomes_nullable_record_fields(self):
        search = self.record("SearchKey")
        self.assertIn("Thrift union", search["doc"])
        self.assertEqual(self.field("SearchKey", "email")["type"], ["null", "string"])
        self.assertEqual(self.field("SearchKey", "email")["default"], None)

    def test_list_and_set_become_arrays(self):
        self.assertEqual(self.field("User", "tags")["type"], {"type": "array", "items": "string"})
        self.assertEqual(self.field("User", "scores")["type"], {"type": "array", "items": "int"})

    def test_string_key_map_becomes_avro_map(self):
        self.assertEqual(self.field("User", "addresses")["type"], {"type": "map", "values": "com.example.thrift.Address"})

    def test_non_string_key_map_becomes_entry_array(self):
        names = self.field("User", "names_by_id")["type"]
        self.assertEqual(names["type"], "array")
        entry = names["items"]
        self.assertEqual(entry["type"], "record")
        self.assertEqual(entry["fields"][0], {"name": "key", "type": "int"})
        self.assertEqual(entry["fields"][1], {"name": "value", "type": "string"})

    def test_nested_struct_reference_is_preserved(self):
        self.assertEqual(self.field("User", "home")["type"], ["null", "com.example.thrift.Address"])

    def test_service_and_const_are_skipped(self):
        self.assertNotIn("UserService", self.by_name)
        self.assertNotIn("BUILD", self.by_name)

    def test_avro_to_thrift_emits_valid_idl_shape(self):
        thrift_path = self.out_dir / "from_avro.thrift"
        convert_avro_to_thrift(str(self.avro_path), str(thrift_path))
        text = thrift_path.read_text(encoding="utf-8")
        self.assertIn("namespace * com.example.thrift", text)
        self.assertIn("struct User {", text)
        self.assertIn("enum Status {", text)
        self.assertIn("2: optional string name;", text)
        self.assertIn("11: required list<string> tags;", text)
        self.assertIn("13: required map<string, Address> addresses;", text)

    def test_round_trip_thrift_to_avro_to_thrift_to_avro_preserves_core_structure(self):
        thrift_path = self.out_dir / "roundtrip.thrift"
        avro_path = self.out_dir / "roundtrip.avsc"
        convert_avro_to_thrift(str(self.avro_path), str(thrift_path))
        convert_thrift_to_avro(str(thrift_path), str(avro_path))
        roundtrip = json.loads(avro_path.read_text(encoding="utf-8"))
        roundtrip_names = {item["name"] for item in roundtrip}
        self.assertTrue({"User", "Address", "Status"}.issubset(roundtrip_names))
        parse_schema(roundtrip)

    def test_thrift_to_json_structure_bridge(self):
        structure_path = self.out_dir / "complex.struct.json"
        convert_thrift_to_structure(str(self.fixture), str(structure_path))
        data = json.loads(structure_path.read_text(encoding="utf-8"))
        self.assertIn("$root", data)
        self.assertIn("definitions", data)
        self.assertIn("com", data["definitions"])
        self.assertIn("User", data["definitions"]["com"]["example"]["thrift"])

    def test_json_structure_to_thrift_bridge(self):
        structure_path = self.out_dir / "complex.struct.json"
        thrift_path = self.out_dir / "from_structure.thrift"
        convert_thrift_to_structure(str(self.fixture), str(structure_path))
        convert_structure_to_thrift(str(structure_path), str(thrift_path), namespace="com.example.fromstructure")
        text = thrift_path.read_text(encoding="utf-8")
        self.assertIn("namespace * com.example.fromstructure", text)
        self.assertIn("struct", text)

    def test_cli_smoke_thrift_to_avro(self):
        cli_out = self.out_dir / "cli.avsc"
        result = subprocess.run(
            [sys.executable, "-m", "avrotize", "thrift2a", str(self.fixture), "--out", str(cli_out)],
            cwd=self.root,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertTrue(cli_out.exists())
        parse_schema(json.loads(cli_out.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
