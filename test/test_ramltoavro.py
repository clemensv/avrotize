import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml
from fastavro import parse_schema

from avrotize.avrotoraml import convert_avro_to_raml
from avrotize.jstructtoraml import convert_json_structure_to_raml
from avrotize.ramltoavro import convert_raml_to_avro
from avrotize.ramltojstruct import convert_raml_to_json_structure


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "test" / "raml" / "library.raml"
API_FIXTURE = ROOT / "test" / "raml" / "api-with-types.raml"


class RamlDataTypesConversionTests(unittest.TestCase):
    def convert_fixture(self):
        tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name) / "schema.avsc"
        convert_raml_to_avro(str(FIXTURE), str(out), namespace="example.raml")
        with open(out, "r", encoding="utf-8") as handle:
            return json.load(handle), out, Path(tmp.name)

    def named(self, schema, name):
        items = schema if isinstance(schema, list) else [schema]
        return next(item for item in items if item["name"] == name)

    def field(self, record, name):
        return next(field for field in record["fields"] if field["name"] == name)

    def test_scalar_mappings(self):
        schema, _, _ = self.convert_fixture()
        person = self.named(schema, "Person")
        self.assertEqual(self.field(person, "id")["type"], "long")
        self.assertEqual(self.field(person, "name")["type"], "string")
        self.assertEqual(self.field(person, "score")["type"], "double")
        self.assertEqual(self.field(person, "active")["type"], ["null", "boolean"])
        self.assertEqual(self.field(person, "avatar")["type"], "bytes")

    def test_date_time_datetime_logical_types_are_valid(self):
        schema, _, _ = self.convert_fixture()
        person = self.named(schema, "Person")
        self.assertEqual(self.field(person, "birthday")["type"], {"type": "int", "logicalType": "date"})
        self.assertEqual(self.field(person, "alarm")["type"], {"type": "int", "logicalType": "time-millis"})
        self.assertEqual(self.field(person, "created")["type"], {"type": "long", "logicalType": "timestamp-millis"})
        parse_schema(schema)

    def test_object_properties_to_record(self):
        schema, _, _ = self.convert_fixture()
        person = self.named(schema, "Person")
        self.assertEqual(person["type"], "record")
        self.assertIn("id", [field["name"] for field in person["fields"]])

    def test_optional_question_mark_property(self):
        schema, _, _ = self.convert_fixture()
        nickname = self.field(self.named(schema, "Person"), "nickname")
        self.assertEqual(nickname["type"], ["null", "string"])
        self.assertIsNone(nickname["default"])

    def test_optional_required_false_property(self):
        schema, _, _ = self.convert_fixture()
        active = self.field(self.named(schema, "Person"), "active")
        self.assertEqual(active["type"], ["null", "boolean"])
        self.assertIsNone(active["default"])

    def test_arrays_shorthand_and_items(self):
        schema, _, _ = self.convert_fixture()
        person = self.named(schema, "Person")
        self.assertEqual(self.field(person, "tags")["type"], {"type": "array", "items": "string"})
        self.assertEqual(self.field(person, "values")["type"], {"type": "array", "items": "long"})

    def test_union_string_syntax(self):
        schema, _, _ = self.convert_fixture()
        choice = self.field(self.named(schema, "Person"), "choice")
        self.assertEqual(choice["type"], ["string", "long"])

    def test_enum_conversion(self):
        schema, _, _ = self.convert_fixture()
        status = self.named(schema, "Status")
        self.assertEqual(status["type"], "enum")
        self.assertEqual(status["symbols"], ["new", "active", "suspended"])

    def test_map_convention_conversion(self):
        schema, _, _ = self.convert_fixture()
        metadata = self.field(self.named(schema, "Person"), "metadata")
        self.assertEqual(metadata["type"], {"type": "map", "values": "string"})

    def test_nested_object_conversion(self):
        schema, _, _ = self.convert_fixture()
        child = self.field(self.named(schema, "Person"), "child")
        self.assertEqual(child["type"]["type"], "record")
        self.assertEqual(child["type"]["name"], "Person_child")

    def test_named_reference_conversion(self):
        schema, _, _ = self.convert_fixture()
        person = self.named(schema, "Person")
        self.assertEqual(self.field(person, "address")["type"], "example.raml.Address")
        self.assertEqual(self.field(person, "status")["type"], "example.raml.Status")

    def test_description_maps_to_doc(self):
        schema, _, _ = self.convert_fixture()
        person = self.named(schema, "Person")
        self.assertEqual(person["doc"], "Person record")
        self.assertEqual(self.field(person, "id")["doc"], "Identifier")

    def test_reverse_emits_parseable_raml_library(self):
        schema, avro_path, work = self.convert_fixture()
        raml_path = work / "schema.raml"
        convert_avro_to_raml(str(avro_path), str(raml_path), namespace="example.raml")
        text = raml_path.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("#%RAML 1.0 Library"))
        parsed = yaml.safe_load("\n".join(text.splitlines()[1:]))
        self.assertIn("Person", parsed["types"])

    def test_round_trip_preserves_normalized_data_types(self):
        _, avro_path, work = self.convert_fixture()
        raml_path = work / "roundtrip.raml"
        avro_again = work / "roundtrip.avsc"
        convert_avro_to_raml(str(avro_path), str(raml_path), namespace="example.raml")
        convert_raml_to_avro(str(raml_path), str(avro_again), namespace="example.raml")
        with open(avro_again, "r", encoding="utf-8") as handle:
            schema_again = json.load(handle)
        person = self.named(schema_again, "Person")
        self.assertEqual(self.field(person, "birthday")["type"], {"type": "int", "logicalType": "date"})
        self.assertEqual(self.field(person, "tags")["type"], {"type": "array", "items": "string"})
        parse_schema(schema_again)

    def test_json_structure_bridge_raml_to_structure(self):
        _, _, work = self.convert_fixture()
        struct_path = work / "schema.struct.json"
        convert_raml_to_json_structure(str(FIXTURE), str(struct_path), namespace="example.raml")
        data = json.loads(struct_path.read_text(encoding="utf-8"))
        self.assertTrue(data)

    def test_json_structure_bridge_structure_to_raml(self):
        _, _, work = self.convert_fixture()
        struct_path = work / "schema.struct.json"
        raml_path = work / "from-structure.raml"
        convert_raml_to_json_structure(str(FIXTURE), str(struct_path), namespace="example.raml")
        convert_json_structure_to_raml(str(struct_path), str(raml_path), namespace="example.raml")
        self.assertTrue(raml_path.read_text(encoding="utf-8").startswith("#%RAML 1.0 Library"))

    def test_out_of_scope_resource_method_keys_do_not_crash(self):
        tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name) / "api.avsc"
        convert_raml_to_avro(str(API_FIXTURE), str(out), namespace="example.raml")
        schema = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(self.named(schema, "Item")["fields"][0]["name"], "id")

    def test_cli_smoke_raml2a(self):
        tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name) / "cli.avsc"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)
        result = subprocess.run(
            [sys.executable, "-m", "avrotize", "raml2a", str(FIXTURE), "--out", str(out), "--namespace", "example.raml"],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        parse_schema(json.loads(out.read_text(encoding="utf-8")))

    def test_avro_schema_validity(self):
        schema, _, _ = self.convert_fixture()
        parsed = parse_schema(schema)
        self.assertTrue(parsed)


if __name__ == "__main__":
    unittest.main()
