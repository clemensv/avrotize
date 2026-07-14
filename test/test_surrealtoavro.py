"""Tests for SurrealQL schema to Avrotize Schema conversion."""

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

from fastavro import parse_schema

from avrotize.surrealtoavro import SurrealToAvroConverter, convert_surrealql_to_avro


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "test" / "surrealql" / "person.surql"


class TestSurrealToAvro(unittest.TestCase):
    """Validate SurrealQL to Avro mappings."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = SurrealToAvroConverter(namespace="example.surreal").convert_schema(FIXTURE.read_text(encoding="utf-8"))
        cls.records = {record["name"]: record for record in cls.schema}
        cls.person = cls.records["person"]
        cls.fields = {field["name"]: field for field in cls.person["fields"]}

    def test_multiple_tables_are_converted(self) -> None:
        self.assertEqual({"person", "order"}, set(self.records))
        self.assertEqual("SurrealDB SCHEMALESS table; Avro captures declared fields only.", self.records["order"].get("doc"))

    def test_namespace_is_applied(self) -> None:
        self.assertEqual("example.surreal", self.person["namespace"])

    def test_string_maps_to_avro_string(self) -> None:
        self.assertEqual("string", self.fields["name"]["type"])

    def test_int_maps_to_avro_long(self) -> None:
        address = self.fields["address"]["type"]
        zip_field = {field["name"]: field for field in address["fields"]}["zip"]
        self.assertEqual("long", zip_field["type"])

    def test_float_maps_to_avro_double(self) -> None:
        self.assertEqual("double", self.fields["score"]["type"])

    def test_decimal_maps_to_valid_decimal_logical_type(self) -> None:
        self.assertEqual({"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9}, self.fields["price"]["type"])

    def test_bool_maps_to_boolean(self) -> None:
        self.assertEqual("boolean", self.fields["active"]["type"])

    def test_datetime_maps_to_timestamp_millis(self) -> None:
        self.assertEqual({"type": "long", "logicalType": "timestamp-millis"}, self.fields["created_at"]["type"])

    def test_duration_maps_to_string(self) -> None:
        self.assertEqual("string", self.fields["elapsed"]["type"])

    def test_uuid_maps_to_valid_uuid_logical_type(self) -> None:
        self.assertEqual({"type": "string", "logicalType": "uuid"}, self.fields["external_id"]["type"])

    def test_bytes_maps_to_bytes(self) -> None:
        self.assertEqual("bytes", self.fields["payload"]["type"])

    def test_option_type_is_nullable_with_null_default(self) -> None:
        self.assertEqual(["null", "long"], self.fields["age"]["type"])
        self.assertIsNone(self.fields["age"]["default"])

    def test_dotted_paths_become_nested_records(self) -> None:
        address = self.fields["address"]["type"]
        self.assertEqual("record", address["type"])
        nested = {field["name"]: field for field in address["fields"]}
        self.assertEqual("string", nested["city"]["type"])
        self.assertEqual("long", nested["zip"]["type"])

    def test_inline_arrays_and_sets_map_to_arrays(self) -> None:
        self.assertEqual({"type": "array", "items": "string"}, self.fields["tags"]["type"])
        self.assertEqual({"type": "array", "items": "string"}, self.fields["roles"]["type"])

    def test_array_item_paths_become_array_of_records(self) -> None:
        phones = self.fields["phones"]["type"]
        self.assertEqual("array", phones["type"])
        item_fields = {field["name"]: field for field in phones["items"]["fields"]}
        self.assertEqual("string", item_fields["kind"]["type"])
        self.assertEqual("string", item_fields["number"]["type"])

    def test_record_reference_maps_to_string_id(self) -> None:
        self.assertEqual("string", self.fields["id"]["type"]["type"])
        self.assertEqual("record<person>", self.fields["id"]["type"]["surrealType"])

    def test_geometry_maps_to_string(self) -> None:
        self.assertEqual("string", self.fields["shape"]["type"])

    def test_generated_avro_is_valid(self) -> None:
        parse_schema(self.schema)

    def test_convert_function_writes_json_file(self) -> None:
        out_dir = ROOT / "test" / "surrealql" / "generated"
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / "person.avsc"
        try:
            convert_surrealql_to_avro(str(FIXTURE), str(out_path), namespace="example.surreal")
            loaded = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual("person", loaded[0]["name"])
            parse_schema(loaded)
        finally:
            if out_path.exists():
                out_path.unlink()
            if out_dir.exists() and not any(out_dir.iterdir()):
                out_dir.rmdir()

    def test_commands_json_is_valid(self) -> None:
        with open(ROOT / "avrotize" / "commands.json", encoding="utf-8") as commands_file:
            commands = json.load(commands_file)
        self.assertIn("surreal2a", {command["command"] for command in commands})
        self.assertIn("a2surreal", {command["command"] for command in commands})

    def test_cli_smoke_surreal2a(self) -> None:
        out_dir = ROOT / "test" / "surrealql" / "generated"
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / "cli-person.avsc"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)
        try:
            result = subprocess.run(
                [sys.executable, "-m", "avrotize", "surreal2a", str(FIXTURE), "--out", str(out_path)],
                cwd=ROOT,
                env=env,
                check=True,
                text=True,
                capture_output=True,
            )
            self.assertIn("Executing Convert SurrealQL schema", result.stdout)
            parse_schema(json.loads(out_path.read_text(encoding="utf-8")))
        finally:
            if out_path.exists():
                out_path.unlink()
            if out_dir.exists() and not any(out_dir.iterdir()):
                out_dir.rmdir()


if __name__ == "__main__":
    unittest.main()
