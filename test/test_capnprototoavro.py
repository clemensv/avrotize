import json
import os
import re
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

from fastavro import parse_schema

from avrotize.avrotocapnproto import convert_avro_to_capnproto, convert_json_structure_to_capnproto
from avrotize.capnprototoavro import convert_capnproto_to_avro, convert_capnproto_to_json_structure, parse_capnproto_schema


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "test" / "capnp" / "comprehensive.capnp"
AVRO_FIXTURE = ROOT / "test" / "capnp" / "telemetry.avsc"
OUT = ROOT / "test" / "capnp" / "generated"


class TestCapnpToAvro(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if OUT.exists():
            shutil.rmtree(OUT)
        OUT.mkdir(parents=True)
        cls.avro_path = OUT / "comprehensive.avsc"
        convert_capnproto_to_avro(str(FIXTURE), str(cls.avro_path), namespace="example.capnp")
        cls.avro = json.loads(cls.avro_path.read_text(encoding="utf-8"))
        cls.by_name = {schema["name"]: schema for schema in cls.avro if isinstance(schema, dict) and "name" in schema}

    def field(self, record_name, field_name):
        record = self.by_name[record_name]
        return next(field for field in record["fields"] if field["name"] == field_name)

    def non_null_type(self, record_name, field_name):
        field_type = self.field(record_name, field_name)["type"]
        if isinstance(field_type, list):
            return next(item for item in field_type if item != "null")
        return field_type

    def test_avro_schema_is_valid(self):
        parse_schema(self.avro)

    def test_bool_primitive_maps_to_boolean(self):
        self.assertEqual(self.non_null_type("PrimitiveSample", "boolField"), "boolean")

    def test_signed_32_bit_primitives_map_to_int(self):
        self.assertEqual(self.non_null_type("PrimitiveSample", "int8Field"), "int")
        self.assertEqual(self.non_null_type("PrimitiveSample", "int16Field"), "int")
        self.assertEqual(self.non_null_type("PrimitiveSample", "int32Field"), "int")

    def test_signed_64_bit_primitive_maps_to_long(self):
        self.assertEqual(self.non_null_type("PrimitiveSample", "int64Field"), "long")

    def test_unsigned_primitives_map_to_long(self):
        for name in ["uint8Field", "uint16Field", "uint32Field", "uint64Field"]:
            with self.subTest(name=name):
                self.assertEqual(self.non_null_type("PrimitiveSample", name), "long")

    def test_floating_point_primitives_map_to_float_and_double(self):
        self.assertEqual(self.non_null_type("PrimitiveSample", "float32Field"), "float")
        self.assertEqual(self.non_null_type("PrimitiveSample", "float64Field"), "double")

    def test_text_data_and_void_primitives(self):
        self.assertEqual(self.non_null_type("PrimitiveSample", "textField"), "string")
        self.assertEqual(self.non_null_type("PrimitiveSample", "dataField"), "bytes")
        self.assertEqual(self.field("PrimitiveSample", "voidField")["type"], "null")

    def test_struct_fields_are_nullable_and_ordinals_preserved(self):
        field = self.field("Person", "name")
        self.assertEqual(field["type"], ["null", "string"])
        self.assertIsNone(field["default"])
        self.assertEqual(field["capnpOrdinal"], 0)
        self.assertEqual(self.field("Person", "catName")["capnpOrdinal"], 9)

    def test_enum_symbols_and_ordinals_preserved(self):
        color = self.by_name["Color"]
        self.assertEqual(color["symbols"], ["red", "green", "blue"])
        self.assertEqual(color["capnpOrdinals"], {"red": 0, "green": 1, "blue": 2})

    def test_lists_map_to_arrays(self):
        tags = self.non_null_type("Person", "tags")
        scores = self.non_null_type("Person", "scores")
        self.assertEqual(tags, {"type": "array", "items": "string"})
        self.assertEqual(scores, {"type": "array", "items": "int"})

    def test_references_to_structs_and_enums(self):
        self.assertEqual(self.non_null_type("Person", "address"), "example.capnp.Address")
        self.assertEqual(self.non_null_type("Person", "favorite"), "example.capnp.Color")

    def test_group_maps_to_inline_nested_record(self):
        contact = self.non_null_type("Person", "contact")
        self.assertEqual(contact["type"], "record")
        self.assertEqual(contact["name"], "Contact")
        self.assertEqual([f["name"] for f in contact["fields"]], ["email", "phone"])

    def test_union_members_map_to_nullable_tagged_fields(self):
        for name in ["noPet", "dogName", "catName"]:
            field = self.field("Person", name)
            self.assertEqual(field["capnpUnion"], "union")
            self.assertIn("null", field["type"] if isinstance(field["type"], list) else [field["type"]])

    def test_nested_struct_and_enum_references(self):
        self.assertIn("Preference", self.by_name)
        self.assertEqual(self.non_null_type("Person", "prefs"), {"type": "array", "items": "example.capnp.Person_types.Preference"})
        self.assertIn("Status", self.by_name)
        self.assertEqual(self.non_null_type("Person", "status"), "example.capnp.Person_types.Status")

    def test_avro_to_capnproto_emits_parseable_schema(self):
        capnp_path = OUT / "telemetry.capnp"
        convert_avro_to_capnproto(str(AVRO_FIXTURE), str(capnp_path), namespace="example.avro")
        text = capnp_path.read_text(encoding="utf-8")
        self.assertIn("@0xd12f8a7e9b0c4a6f;", text)
        self.assertIn("struct Telemetry", text)
        self.assertIn("id @0 :Text;", text)
        self.assertIn("samples @2 :List(Int64);", text)
        parsed = parse_capnproto_schema(text)
        self.assertEqual(parsed.structs[0].name, "Telemetry")

    def test_round_trip_capnp_avro_capnp_preserves_core_structure(self):
        capnp_path = OUT / "roundtrip.capnp"
        avro_path = OUT / "roundtrip.avsc"
        convert_avro_to_capnproto(str(self.avro_path), str(capnp_path), namespace="example.capnp")
        convert_capnproto_to_avro(str(capnp_path), str(avro_path), namespace="example.roundtrip")
        roundtrip = json.loads(avro_path.read_text(encoding="utf-8"))
        names = {schema["name"] for schema in roundtrip if isinstance(schema, dict) and "name" in schema}
        self.assertTrue({"PrimitiveSample", "Address", "Person", "Color"}.issubset(names))
        normalized = re.sub(r"\s+", " ", capnp_path.read_text(encoding="utf-8"))
        self.assertIn("struct Person", normalized)
        self.assertIn("union {", normalized)

    def test_capnp_to_json_structure_bridge(self):
        struct_path = OUT / "comprehensive.struct.json"
        convert_capnproto_to_json_structure(str(FIXTURE), str(struct_path), namespace="example.capnp")
        data = json.loads(struct_path.read_text(encoding="utf-8"))
        self.assertIsInstance(data, dict)
        self.assertTrue(data)

    def test_json_structure_to_capnproto_bridge(self):
        struct_path = OUT / "bridge.struct.json"
        capnp_path = OUT / "bridge.capnp"
        convert_capnproto_to_json_structure(str(FIXTURE), str(struct_path), namespace="example.capnp")
        convert_json_structure_to_capnproto(str(struct_path), str(capnp_path), namespace="example.capnp")
        text = capnp_path.read_text(encoding="utf-8")
        self.assertIn("struct", text)
        self.assertIn("@0xd12f8a7e9b0c4a6f;", text)

    def test_cli_smoke_capnp2a(self):
        out_path = OUT / "cli.avsc"
        result = subprocess.run(
            [sys.executable, "-m", "avrotize", "capnp2a", str(FIXTURE), "--out", str(out_path), "--namespace", "example.cli"],
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        parse_schema(json.loads(out_path.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
