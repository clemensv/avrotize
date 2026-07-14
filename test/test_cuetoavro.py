"""Tests for CUE subset conversion."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from fastavro import parse_schema

from avrotize.avrotocue import convert_avro_to_cue
from avrotize.cuetoavro import CueToAvroConverter, convert_cue_to_avro
from avrotize.cuetostructure import convert_cue_to_json_structure
from avrotize.structuretocue import convert_json_structure_to_cue


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "test" / "cue"


class CueToAvroTests(unittest.TestCase):
    """Focused coverage for the documented CUE schema subset."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(dir=FIXTURES)
        self.addCleanup(self.tmp.cleanup)
        self.tmp_path = Path(self.tmp.name)

    def convert_text(self, text: str):
        return CueToAvroConverter(namespace="com.example").convert_text(text, root_name="Root")

    def person_schema(self):
        out = self.tmp_path / "person.avsc"
        convert_cue_to_avro(str(FIXTURES / "person.cue"), str(out))
        with out.open("r", encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def record_by_name(schema, name: str):
        records = schema if isinstance(schema, list) else [schema]
        return next(record for record in records if record["name"] == name)

    @staticmethod
    def field(record, name: str):
        return next(field for field in record["fields"] if field["name"] == name)

    def test_import_resolves_to_worktree(self):
        import avrotize

        self.assertTrue(str(ROOT) in avrotize.__file__)

    def test_definition_becomes_record(self):
        schema = self.convert_text("#Person: { name: string }")
        self.assertEqual("record", schema["type"])
        self.assertEqual("Person", schema["name"])

    def test_basic_types_mapping(self):
        schema = self.convert_text("#Types: { s: string, i: int, f: float, n: number, b: bool, by: bytes, z: null }")
        expected = {"s": "string", "i": "long", "f": "double", "n": "double", "b": "boolean", "by": "bytes", "z": "null"}
        for name, avro_type in expected.items():
            self.assertEqual(avro_type, self.field(schema, name)["type"])

    def test_required_and_optional_fields(self):
        schema = self.convert_text("#Person: { name: string, nickname?: string }")
        self.assertEqual("string", self.field(schema, "name")["type"])
        self.assertEqual(["null", "string"], self.field(schema, "nickname")["type"])
        self.assertIsNone(self.field(schema, "nickname")["default"])

    def test_nested_struct_becomes_nested_record(self):
        schema = self.convert_text("#Person: { address: { street: string } }")
        address = self.field(schema, "address")["type"]
        self.assertEqual("record", address["type"])
        self.assertEqual("Person_address", address["name"])

    def test_list_becomes_array(self):
        schema = self.convert_text("#Person: { tags: [...string] }")
        self.assertEqual({"type": "array", "items": "string"}, self.field(schema, "tags")["type"])

    def test_open_struct_becomes_map(self):
        schema = self.convert_text("#Person: { labels: { [string]: int } }")
        self.assertEqual({"type": "map", "values": "long"}, self.field(schema, "labels")["type"])

    def test_disjunction_becomes_union(self):
        schema = self.convert_text("#Thing: { value: string | int | null }")
        self.assertEqual(["string", "long", "null"], self.field(schema, "value")["type"])

    def test_string_disjunction_becomes_enum(self):
        schema = self.convert_text('#Thing: { status: "new" | "active" | "closed" }')
        enum_type = self.field(schema, "status")["type"]
        self.assertEqual("enum", enum_type["type"])
        self.assertEqual(["NEW", "ACTIVE", "CLOSED"], enum_type["symbols"])

    def test_reference_to_definition(self):
        schema = self.person_schema()
        person = self.record_by_name(schema, "Person")
        self.assertEqual("Address", self.field(person, "address")["type"])

    def test_default_value_mapping(self):
        schema = self.convert_text('#Person: { role: string | *"user" }')
        self.assertEqual("user", self.field(schema, "role")["default"])

    def test_fixture_avro_is_valid(self):
        parse_schema(self.person_schema())

    def test_avro_to_cue_emits_parseable_subset(self):
        schema = self.convert_text("#Person: { name: string, tags: [...string] }")
        avro_path = self.tmp_path / "person.avsc"
        cue_path = self.tmp_path / "person.cue"
        avro_path.write_text(json.dumps(schema), encoding="utf-8")
        convert_avro_to_cue(str(avro_path), str(cue_path), namespace="com.example")
        reparsed = CueToAvroConverter(namespace="com.example").convert_text(cue_path.read_text(encoding="utf-8"), "Root")
        self.assertEqual("Person", reparsed["name"])
        self.assertEqual({"type": "array", "items": "string"}, self.field(reparsed, "tags")["type"])

    def test_round_trip_preserves_supported_structure(self):
        schema = self.convert_text('#Person: { name: string, age?: int, status: "new" | "active", labels: { [string]: string } }')
        avro_path = self.tmp_path / "person.avsc"
        cue_path = self.tmp_path / "person.cue"
        avro_path.write_text(json.dumps(schema), encoding="utf-8")
        convert_avro_to_cue(str(avro_path), str(cue_path))
        reparsed = CueToAvroConverter().convert_text(cue_path.read_text(encoding="utf-8"), "Root")
        self.assertEqual(schema["name"], reparsed["name"])
        self.assertEqual(self.field(schema, "age")["type"], self.field(reparsed, "age")["type"])
        self.assertEqual(self.field(schema, "labels")["type"], self.field(reparsed, "labels")["type"])

    def test_cue_to_json_structure_bridge(self):
        out = self.tmp_path / "person.struct.json"
        convert_cue_to_json_structure(str(FIXTURES / "person.cue"), str(out))
        doc = json.loads(out.read_text(encoding="utf-8"))
        self.assertIn("$root", doc)
        self.assertIn("definitions", doc)

    def test_json_structure_to_cue_bridge(self):
        struct_path = self.tmp_path / "person.struct.json"
        cue_path = self.tmp_path / "person.cue"
        convert_cue_to_json_structure(str(FIXTURES / "person.cue"), str(struct_path))
        convert_json_structure_to_cue(str(struct_path), str(cue_path))
        self.assertIn("#Person:", cue_path.read_text(encoding="utf-8"))

    def test_unsupported_construct_does_not_crash(self):
        out = self.tmp_path / "unsupported.avsc"
        convert_cue_to_avro(str(FIXTURES / "unsupported.cue"), str(out))
        schema = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual("Metric", schema["name"])
        self.assertIn("CUE conversion notes", schema.get("doc", ""))

    def test_cli_smoke_cue_to_avro(self):
        out = self.tmp_path / "cli.avsc"
        result = subprocess.run(
            [sys.executable, "-m", "avrotize", "cue2a", str(FIXTURES / "person.cue"), "--out", str(out)],
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertTrue(out.exists())
        parse_schema(json.loads(out.read_text(encoding="utf-8")))

    def test_commands_json_is_valid_and_registered(self):
        commands = json.loads((ROOT / "avrotize" / "commands.json").read_text(encoding="utf-8"))
        names = {command["command"] for command in commands}
        self.assertTrue({"cue2a", "a2cue", "cue2s", "s2cue"}.issubset(names))


if __name__ == "__main__":
    unittest.main()
