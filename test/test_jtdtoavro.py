"""Tests for JSON Type Definition converters."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
from typing import Any

from fastavro import parse_schema

from avrotize.avrotojtd import AvroToJtdConverter, convert_avro_to_jtd
from avrotize.jtdtoavro import JtdToAvroConverter, convert_jtd_to_avro
from avrotize.jtdtostructure import convert_jtd_to_structure
from avrotize.structuretojtd import convert_structure_to_jtd


class TestJtdConverters(unittest.TestCase):
    """Focused tests for JTD conversion support."""

    root = Path(__file__).resolve().parents[1]
    fixtures = root / "test" / "jtd"
    generated = fixtures / "generated"

    @classmethod
    def setUpClass(cls) -> None:
        if cls.generated.exists():
            shutil.rmtree(cls.generated)
        cls.generated.mkdir(parents=True)

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.generated.exists():
            shutil.rmtree(cls.generated)

    def _convert_dict(self, schema: dict[str, Any], namespace: str | None = "example") -> Any:
        return JtdToAvroConverter(namespace=namespace).convert(schema, root_name="Root")

    def _assert_valid_avro(self, schema: Any) -> None:
        parse_schema(schema)

    def _root_jtd(self, schema: dict[str, Any]) -> dict[str, Any]:
        if "definitions" in schema and "ref" in schema:
            return schema["definitions"][schema["ref"]]
        return schema

    def _assert_type_mapping(self, jtd_type: str, avro_type: str, logical_type: str | None = None) -> None:
        avro = self._convert_dict({"type": jtd_type})
        self.assertIsInstance(avro, dict)
        self.assertEqual(avro["type"], avro_type)
        self.assertEqual(avro["jtdType"], jtd_type)
        if logical_type:
            self.assertEqual(avro["logicalType"], logical_type)
        self._assert_valid_avro(avro)
        self.assertEqual(AvroToJtdConverter().convert(avro), {"type": jtd_type})

    def test_boolean_type_mapping(self) -> None:
        self._assert_type_mapping("boolean", "boolean")

    def test_float32_type_mapping(self) -> None:
        self._assert_type_mapping("float32", "float")

    def test_float64_type_mapping(self) -> None:
        self._assert_type_mapping("float64", "double")

    def test_int8_type_mapping(self) -> None:
        self._assert_type_mapping("int8", "int")

    def test_uint8_type_mapping(self) -> None:
        self._assert_type_mapping("uint8", "int")

    def test_int16_type_mapping(self) -> None:
        self._assert_type_mapping("int16", "int")

    def test_uint16_type_mapping(self) -> None:
        self._assert_type_mapping("uint16", "int")

    def test_int32_type_mapping(self) -> None:
        self._assert_type_mapping("int32", "int")

    def test_uint32_type_mapping(self) -> None:
        self._assert_type_mapping("uint32", "long")

    def test_string_type_mapping(self) -> None:
        self._assert_type_mapping("string", "string")

    def test_timestamp_type_mapping_uses_valid_logical_type(self) -> None:
        self._assert_type_mapping("timestamp", "long", "timestamp-millis")

    def test_enum_symbols_are_sanitized_and_round_trip(self) -> None:
        avro = self._convert_dict({"enum": ["ok", "bad-value", "123"]})
        self._assert_valid_avro(avro)
        self.assertEqual(avro["symbols"], ["ok", "bad_value", "_123"])
        self.assertEqual(avro["jtdEnumSymbols"], {"bad_value": "bad-value", "_123": "123"})
        self.assertEqual(AvroToJtdConverter().convert(avro), {"enum": ["ok", "bad-value", "123"]})

    def test_elements_map_to_avro_array(self) -> None:
        avro = self._convert_dict({"elements": {"type": "string"}})
        self._assert_valid_avro(avro)
        self.assertEqual(avro["type"], "array")
        self.assertEqual(avro["items"]["type"], "string")
        self.assertEqual(AvroToJtdConverter().convert(avro), {"elements": {"type": "string"}})

    def test_values_map_to_avro_map(self) -> None:
        avro = self._convert_dict({"values": {"type": "int32"}})
        self._assert_valid_avro(avro)
        self.assertEqual(avro["type"], "map")
        self.assertEqual(avro["values"]["type"], "int")
        self.assertEqual(AvroToJtdConverter().convert(avro), {"values": {"type": "int32"}})

    def test_properties_and_optional_properties(self) -> None:
        avro = self._convert_dict({"properties": {"id": {"type": "string"}}, "optionalProperties": {"age": {"type": "uint8"}}})
        self._assert_valid_avro(avro)
        fields = {field["name"]: field for field in avro["fields"]}
        self.assertEqual(fields["id"]["type"]["type"], "string")
        self.assertEqual(fields["age"]["type"][0], "null")
        self.assertIsNone(fields["age"]["default"])
        jtd = self._root_jtd(AvroToJtdConverter().convert(avro))
        self.assertIn("id", jtd["properties"])
        self.assertIn("age", jtd["optionalProperties"])

    def test_nullable_type_maps_to_avro_union(self) -> None:
        avro = self._convert_dict({"type": "string", "nullable": True})
        self._assert_valid_avro(avro)
        self.assertEqual(avro[0], "null")
        self.assertEqual(AvroToJtdConverter().convert(avro), {"type": "string", "nullable": True})

    def test_additional_properties_metadata_is_preserved(self) -> None:
        avro = self._convert_dict({"properties": {"id": {"type": "string"}}, "additionalProperties": True})
        self._assert_valid_avro(avro)
        self.assertTrue(avro["jtdAdditionalProperties"])
        jtd = self._root_jtd(AvroToJtdConverter().convert(avro))
        self.assertTrue(jtd["additionalProperties"])

    def test_discriminator_mapping_becomes_avro_union(self) -> None:
        schema = json.loads((self.fixtures / "vehicle.jtd.json").read_text(encoding="utf-8"))
        avro = self._convert_dict(schema)
        self._assert_valid_avro(avro)
        self.assertIsInstance(avro, list)
        self.assertEqual({branch["jtdMappingKey"] for branch in avro}, {"car", "boat-type"})
        round_tripped = AvroToJtdConverter().convert(avro)
        self.assertEqual(round_tripped["discriminator"], "kind")
        self.assertEqual(set(round_tripped["mapping"]), {"car", "boat-type"})

    def test_ref_and_definitions_generate_named_avro_types(self) -> None:
        schema = json.loads((self.fixtures / "node.jtd.json").read_text(encoding="utf-8"))
        avro = self._convert_dict(schema)
        self._assert_valid_avro(avro)
        self.assertEqual(len(avro), 1)
        self.assertEqual(avro[0]["name"], "Node")
        child_type = next(field for field in avro[0]["fields"] if field["name"] == "child")["type"]
        self.assertEqual(child_type, ["null", "example.Node"])

    def test_nested_fixture_generates_valid_avro(self) -> None:
        out = self.generated / "person.avsc"
        convert_jtd_to_avro(str(self.fixtures / "person.jtd.json"), str(out), namespace="example")
        avro = json.loads(out.read_text(encoding="utf-8"))
        self._assert_valid_avro(avro)
        root = next(item for item in avro if isinstance(item, dict) and item.get("jtdAdditionalProperties"))
        self.assertEqual(root["type"], "record")
        self.assertTrue(root["jtdAdditionalProperties"])

    def test_round_trip_preserves_core_record_semantics(self) -> None:
        avro_path = self.generated / "roundtrip.avsc"
        jtd_path = self.generated / "roundtrip.jtd.json"
        source = {"properties": {"id": {"type": "string"}, "when": {"type": "timestamp"}}, "optionalProperties": {"count": {"type": "uint32"}}}
        source_path = self.generated / "roundtrip-source.jtd.json"
        source_path.write_text(json.dumps(source), encoding="utf-8")
        convert_jtd_to_avro(str(source_path), str(avro_path), namespace="example")
        convert_avro_to_jtd(str(avro_path), str(jtd_path))
        converted = json.loads(jtd_path.read_text(encoding="utf-8"))
        root = self._root_jtd(converted)
        self.assertEqual(root["properties"], source["properties"])
        self.assertEqual(root["optionalProperties"], source["optionalProperties"])

    def test_jtd_to_structure_bridge_writes_json_structure(self) -> None:
        out = self.generated / "person.struct.json"
        convert_jtd_to_structure(str(self.fixtures / "person.jtd.json"), str(out), namespace="example")
        structure = json.loads(out.read_text(encoding="utf-8"))
        self.assertIn("$schema", structure)
        self.assertIn("definitions", structure)
        self.assertIn("$root", structure)

    def test_structure_to_jtd_bridge_writes_jtd(self) -> None:
        out = self.generated / "structure-person.jtd.json"
        convert_structure_to_jtd(str(self.fixtures / "structure_person.struct.json"), str(out))
        jtd = json.loads(out.read_text(encoding="utf-8"))
        root = self._root_jtd(jtd)
        self.assertIn("id", root["properties"])
        self.assertIn("age", root["optionalProperties"])

    def test_avro_record_type_selection(self) -> None:
        avro = [
            {"type": "record", "name": "A", "fields": [{"name": "value", "type": "string"}]},
            {"type": "record", "name": "B", "fields": [{"name": "count", "type": {"type": "int", "jtdType": "uint8"}}]},
        ]
        jtd = self._root_jtd(AvroToJtdConverter(record_type="B").convert(avro))
        self.assertIn("count", jtd["properties"])

    def test_commands_json_is_valid_and_registers_jtd_commands(self) -> None:
        commands = json.loads((self.root / "avrotize" / "commands.json").read_text(encoding="utf-8"))
        names = {command["command"] for command in commands}
        self.assertTrue({"jtd2a", "a2jtd", "jtd2s", "s2jtd"}.issubset(names))

    def test_import_path_is_worktree(self) -> None:
        import avrotize

        self.assertTrue(str(avrotize.__file__).startswith(str(self.root)))

    def test_cli_smoke_jtd2a(self) -> None:
        out = self.generated / "cli-person.avsc"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(self.root)
        result = subprocess.run(
            [sys.executable, "-m", "avrotize", "jtd2a", str(self.fixtures / "person.jtd.json"), "--out", str(out), "--namespace", "example"],
            cwd=str(self.root),
            env=env,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self._assert_valid_avro(json.loads(out.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
