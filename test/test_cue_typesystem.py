"""Full type-system range and adversarial coverage for the CUE converters.

This suite deliberately exercises every branch in avrotize/cuetoavro.py,
avrotize/avrotocue.py, and the CUE <-> JSON Structure bridge. It complements
the happy-path fixtures in test_cuetoavro.py and pins current lossy behavior.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from typing import Any

from fastavro.schema import parse_schema

from avrotize.avrotocue import convert_avro_to_cue
from avrotize.cuetoavro import CueToAvroConverter, convert_cue_to_avro
from avrotize.cuetostructure import convert_cue_to_json_structure
from avrotize.structuretocue import convert_json_structure_to_cue


ROOT = Path(__file__).resolve().parents[1]
NS = "example.cue"


class CueTypeSystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    # -- helpers ---------------------------------------------------------------

    def cue_text_to_avro(self, cue: str, namespace: str | None = NS, root_name: str = "Root"):
        converter = CueToAvroConverter(namespace=namespace)
        schema = converter.convert_text(textwrap.dedent(cue).strip(), root_name=root_name)
        parse_schema(schema)
        by_name = self.by_name(schema)
        return schema, by_name, converter.warnings

    def cue_file_to_avro(self, cue: str, filename: str = "schema.cue", namespace: str | None = NS):
        src = self.tmp / filename
        src.write_text(textwrap.dedent(cue).strip(), encoding="utf-8")
        out = self.tmp / "schema.avsc"
        convert_cue_to_avro(str(src), str(out), namespace=namespace)
        schema = json.loads(out.read_text(encoding="utf-8"))
        parse_schema(schema)
        return schema, self.by_name(schema)

    def avro_to_cue(self, schema: Any, namespace: str | None = None) -> str:
        parse_schema(schema)
        src = self.tmp / "schema.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "schema.cue"
        convert_avro_to_cue(str(src), str(out), namespace=namespace)
        return out.read_text(encoding="utf-8")

    def cue_to_structure(self, cue: str) -> dict[str, Any]:
        src = self.tmp / "schema.cue"
        out = self.tmp / "schema.struct.json"
        src.write_text(textwrap.dedent(cue).strip(), encoding="utf-8")
        convert_cue_to_json_structure(str(src), str(out), namespace=NS)
        return json.loads(out.read_text(encoding="utf-8"))

    def structure_to_cue(self, structure: dict[str, Any]) -> str:
        src = self.tmp / "schema.struct.json"
        out = self.tmp / "schema.cue"
        src.write_text(json.dumps(structure), encoding="utf-8")
        convert_json_structure_to_cue(str(src), str(out), namespace=NS)
        return out.read_text(encoding="utf-8")

    @staticmethod
    def by_name(schema: Any) -> dict[str, dict[str, Any]]:
        schemas = schema if isinstance(schema, list) else [schema]
        return {s["name"]: s for s in schemas if isinstance(s, dict) and "name" in s}

    @staticmethod
    def field(record: dict[str, Any], name: str) -> dict[str, Any]:
        return next(field for field in record["fields"] if field["name"] == name)

    def struct_def(self, structure: dict[str, Any], *path: str) -> dict[str, Any]:
        node: Any = structure["definitions"]
        for part in path:
            node = node[part]
        return node

    # -- CUE -> Avro: full primitive range -------------------------------------

    def test_every_supported_cue_primitive_maps_to_expected_avro(self):
        _, by_name, _ = self.cue_text_to_avro(
            """
            #Prims: {
              b: bool
              s: string
              by: bytes
              i: int
              f: float
              n: number
              z: null
            }
            """
        )
        expected = {
            "b": "boolean",
            "s": "string",
            "by": "bytes",
            "i": "long",
            "f": "double",
            "n": "double",
            "z": "null",
        }
        for name, avro_type in expected.items():
            with self.subTest(field=name):
                self.assertEqual(self.field(by_name["Prims"], name)["type"], avro_type)

    def test_cue_numeric_subtypes_and_top_are_currently_unsupported_and_pinned(self):
        _, by_name, warnings = self.cue_text_to_avro(
            """
            #NumericAliases: {
              i8: int8
              i16: int16
              i32: int32
              i64: int64
              u8: uint8
              u16: uint16
              u32: uint32
              u64: uint64
              f32: float32
              f64: float64
              top: _
            }
            """
        )
        for name in ("i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "top"):
            with self.subTest(field=name):
                self.assertEqual(self.field(by_name["NumericAliases"], name)["type"], "string")
        self.assertIn("unknown identifier 'int8' mapped as a string", warnings)
        self.assertIn("unknown identifier 'uint64' mapped as a string", warnings)
        self.assertIn("unknown identifier '_' mapped as a string", warnings)

    def test_literal_types_map_to_avro_primitives(self):
        _, by_name, _ = self.cue_text_to_avro(
            """
            #Literals: {
              flag: true
              count: 7
              ratio: 1.25
              label: "ready"
            }
            """
        )
        self.assertEqual(self.field(by_name["Literals"], "flag")["type"], "boolean")
        self.assertEqual(self.field(by_name["Literals"], "count")["type"], "long")
        self.assertEqual(self.field(by_name["Literals"], "ratio")["type"], "double")
        self.assertEqual(self.field(by_name["Literals"], "label")["type"], "string")

    # -- CUE -> Avro: containers, unions, defaults -----------------------------

    def test_open_list_nested_list_and_map_patterns(self):
        _, by_name, _ = self.cue_text_to_avro(
            """
            #Containers: {
              tags: [...string]
              matrix: [...[...int]]
              labels: { [string]: bool }
              nestedMap: { [string]: [...number] }
            }
            """
        )
        rec = by_name["Containers"]
        self.assertEqual(self.field(rec, "tags")["type"], {"type": "array", "items": "string"})
        self.assertEqual(
            self.field(rec, "matrix")["type"],
            {"type": "array", "items": {"type": "array", "items": "long"}},
        )
        self.assertEqual(self.field(rec, "labels")["type"], {"type": "map", "values": "boolean"})
        self.assertEqual(
            self.field(rec, "nestedMap")["type"],
            {"type": "map", "values": {"type": "array", "items": "double"}},
        )

    def test_structs_nested_structs_and_root_fields(self):
        schema, by_name, _ = self.cue_text_to_avro(
            """
            top: string
            nested: {
              child: {
                value: int
              }
            }
            """,
            root_name="Document",
        )
        self.assertEqual(schema["name"], "Document")
        self.assertEqual(self.field(by_name["Document"], "top")["type"], "string")
        nested = self.field(by_name["Document"], "nested")["type"]
        self.assertEqual(nested["type"], "record")
        self.assertEqual(nested["name"], "Document_nested")
        child = self.field(nested, "child")["type"]
        self.assertEqual(child["name"], "Document_nested_child")
        self.assertEqual(self.field(child, "value")["type"], "long")

    def test_disjunctions_map_to_unions_and_deduplicate_branches(self):
        _, by_name, _ = self.cue_text_to_avro("#Thing: { value: string | int | null | string }")
        self.assertEqual(self.field(by_name["Thing"], "value")["type"], ["string", "long", "null"])

    def test_optional_fields_are_nullable_with_null_default(self):
        _, by_name, _ = self.cue_text_to_avro("#Maybe: { name?: string, count?: int | null }")
        self.assertEqual(self.field(by_name["Maybe"], "name")["type"], ["null", "string"])
        self.assertIsNone(self.field(by_name["Maybe"], "name")["default"])
        self.assertEqual(self.field(by_name["Maybe"], "count")["type"], ["null", "long"])
        self.assertIsNone(self.field(by_name["Maybe"], "count")["default"])

    def test_default_values_are_preserved(self):
        _, by_name, _ = self.cue_text_to_avro(
            """
            #Defaults: {
              role: string | *"user"
              enabled: bool | *true
              retries: int | *3
              scale: number | *1.5
              absent: null | *null
            }
            """
        )
        rec = by_name["Defaults"]
        self.assertEqual(self.field(rec, "role")["default"], "user")
        self.assertEqual(self.field(rec, "enabled")["default"], True)
        self.assertEqual(self.field(rec, "retries")["default"], 3)
        self.assertEqual(self.field(rec, "scale")["default"], 1.5)
        self.assertIsNone(self.field(rec, "absent")["default"])

    def test_string_literal_disjunction_maps_to_enum_with_original_symbol_map(self):
        _, by_name, _ = self.cue_text_to_avro('#Thing: { status: "new" | *"in-progress" | "closed" }')
        enum_type = self.field(by_name["Thing"], "status")["type"]
        self.assertEqual(enum_type["type"], "enum")
        self.assertEqual(enum_type["name"], "Thing_status_enum")
        self.assertEqual(enum_type["symbols"], ["NEW", "IN_PROGRESS", "CLOSED"])
        self.assertEqual(enum_type["default"], "IN_PROGRESS")
        self.assertEqual(enum_type["x-cue-symbols"], {"NEW": "new", "IN_PROGRESS": "in-progress", "CLOSED": "closed"})

    def test_enum_symbol_duplicate_after_sanitization_is_suffixed(self):
        _, by_name, _ = self.cue_text_to_avro('#E: { value: "a-b" | "a_b" | "a b" }')
        enum_type = self.field(by_name["E"], "value")["type"]
        self.assertEqual(enum_type["symbols"], ["A_B", "A_B_1", "A_B_2"])
        self.assertEqual(enum_type["x-cue-symbols"], {"A_B": "a-b", "A_B_1": "a_b", "A_B_2": "a b"})

    # -- named definitions and references --------------------------------------

    def test_definitions_become_named_records_and_references_are_names(self):
        schema, by_name, _ = self.cue_text_to_avro(
            """
            #Address: { street: string }
            #Person: {
              name: string
              address: #Address
            }
            """
        )
        self.assertIsInstance(schema, list)
        self.assertEqual(set(by_name), {"Address", "Person"})
        self.assertEqual(self.field(by_name["Person"], "address")["type"], "Address")

    def test_constraints_are_dropped_with_conversion_notes(self):
        _, by_name, warnings = self.cue_text_to_avro(
            """
            #Constrained: {
              age: int >=0
              code: string =~"^[A-Z]+$"
            }
            """
        )
        self.assertEqual(self.field(by_name["Constrained"], "age")["type"], "long")
        self.assertEqual(self.field(by_name["Constrained"], "code")["type"], "string")
        self.assertIn("field 'age' has unsupported trailing constraint tokens", warnings)
        self.assertIn("field 'code' has unsupported trailing constraint tokens", warnings)
        self.assertIn("CUE conversion notes", by_name["Constrained"]["doc"])

    # -- recursion -------------------------------------------------------------

    def test_self_recursive_definition_is_valid_avro(self):
        schema, by_name, _ = self.cue_text_to_avro("#Node: { value: int, next?: #Node }")
        parse_schema(schema)
        self.assertEqual(self.field(by_name["Node"], "next")["type"], ["null", "Node"])

    def test_mutually_recursive_definitions_are_valid_avro(self):
        converter = CueToAvroConverter(namespace=NS)
        schema = converter.convert_text("#A: { b?: #B }\n#B: { a?: #A }", root_name="Root")
        parse_schema(schema)
        by_name = self.by_name(schema)
        self.assertEqual(self.field(by_name["A"], "b")["type"], ["null", "B"])
        self.assertEqual(self.field(by_name["B"], "a")["type"], ["null", "A"])

    # -- Avro -> CUE: full primitive and container range -----------------------

    def test_avro_primitives_map_to_cue(self):
        schema = {
            "type": "record",
            "name": "Prims",
            "namespace": NS,
            "fields": [
                {"name": "z", "type": "null"},
                {"name": "b", "type": "boolean"},
                {"name": "i", "type": "int"},
                {"name": "l", "type": "long"},
                {"name": "f", "type": "float"},
                {"name": "d", "type": "double"},
                {"name": "by", "type": "bytes"},
                {"name": "s", "type": "string"},
            ],
        }
        cue = self.avro_to_cue(schema)
        self.assertIn("package cue", cue)
        expected_lines = {
            "z: null",
            "b: bool",
            "i: int",
            "l: int",
            "f: float",
            "d: number",
            "by: bytes",
            "s: string",
        }
        for line in expected_lines:
            with self.subTest(line=line):
                self.assertIn(line, cue)

    def test_avro_complex_types_map_to_cue(self):
        schema = {
            "type": "record",
            "name": "Complex",
            "namespace": NS,
            "fields": [
                {"name": "arr", "type": {"type": "array", "items": "long"}},
                {"name": "map", "type": {"type": "map", "values": "boolean"}},
                {"name": "opt", "type": ["null", "string"], "default": None},
                {"name": "union", "type": ["string", "int", "null"]},
                {
                    "name": "enum",
                    "type": {"type": "enum", "name": "Color", "symbols": ["RED", "BLUE"], "x-cue-symbols": {"RED": "red", "BLUE": "blue"}},
                },
                {"name": "rec", "type": {"type": "record", "name": "Inner", "fields": [{"name": "x", "type": "int"}]}},
            ],
        }
        cue = self.avro_to_cue(schema)
        self.assertIn("arr: [...int]", cue)
        self.assertIn("map: { [string]: bool }", cue)
        self.assertIn("opt?: string | *null", cue)
        self.assertIn("union?: string | int | null", cue)
        self.assertIn('enum: "red" | "blue"', cue)
        self.assertIn("rec: {\n    x: int\n  }", cue)

    def test_avro_named_reference_maps_to_cue_definition_reference(self):
        schema = [
            {"type": "record", "name": "Address", "namespace": NS, "fields": [{"name": "street", "type": "string"}]},
            {"type": "record", "name": "Person", "namespace": NS, "fields": [{"name": "address", "type": "example.cue.Address"}]},
        ]
        cue = self.avro_to_cue(schema)
        self.assertIn("#Address:", cue)
        self.assertIn("#Person:", cue)
        self.assertIn("address: #Address", cue)

    # -- round-trip fidelity ---------------------------------------------------

    def test_cue_avro_cue_round_trip_preserves_supported_structure_and_pins_lossy_types(self):
        schema, by_name, _ = self.cue_text_to_avro(
            """
            #Round: {
              i: int
              f: float
              n: number
              tags: [...string]
              labels: { [string]: int }
              choice: string | int | null
              small: int8
            }
            """
        )
        cue = self.avro_to_cue(schema)
        reparsed, reparsed_by_name, warnings = self.cue_text_to_avro(cue, root_name="Ignored")
        self.assertEqual(reparsed["name"], "Round")
        self.assertEqual(self.field(reparsed_by_name["Round"], "i")["type"], "long")
        self.assertEqual(self.field(reparsed_by_name["Round"], "f")["type"], "double")
        self.assertEqual(self.field(reparsed_by_name["Round"], "n")["type"], "double")
        self.assertEqual(self.field(reparsed_by_name["Round"], "tags")["type"], {"type": "array", "items": "string"})
        self.assertEqual(self.field(reparsed_by_name["Round"], "labels")["type"], {"type": "map", "values": "long"})
        self.assertEqual(self.field(reparsed_by_name["Round"], "choice")["type"], ["null", "string", "long"])
        self.assertEqual(self.field(by_name["Round"], "small")["type"], "string")
        self.assertEqual(self.field(reparsed_by_name["Round"], "small")["type"], "string")
        self.assertEqual(warnings, [])

    def test_cue_structure_cue_round_trip_pins_losses(self):
        structure = self.cue_to_structure(
            """
            #Types: {
              b: bool
              s: string
              by: bytes
              i: int
              f: float
              n: number
              z: null
              arr: [...string]
              map: { [string]: int }
              opt?: string
              e: "red" | "blue"
            }
            """
        )
        props = self.struct_def(structure, "example", "cue", "Types")["properties"]
        self.assertEqual(props["b"]["type"], "boolean")
        self.assertEqual(props["s"]["type"], "string")
        self.assertEqual(props["by"]["type"], "binary")
        self.assertEqual(props["i"]["type"], "int64")
        self.assertEqual(props["f"]["type"], "double")
        self.assertEqual(props["n"]["type"], "double")
        self.assertEqual(props["z"]["type"], "null")
        self.assertEqual(props["arr"], {"type": "array", "items": {"type": "string"}})
        self.assertEqual(props["map"], {"type": "map", "values": {"type": "int64"}})
        self.assertEqual(props["opt"], {"type": "string", "default": None})
        self.assertEqual(props["e"], {"$ref": "#/definitions/example/cue/Types_e_enum"})

        cue = self.structure_to_cue(structure)
        self.assertIn("f: number", cue)  # CUE float -> Avro double -> CUE number
        self.assertIn("opt: string | *null", cue)  # optional marker is lost through JSON Structure
        self.assertIn("e: #Types_e_enum", cue)  # enum definition is referenced, not re-expanded

    # -- JSON Structure bridge: full source type range -------------------------

    def test_json_structure_full_primitive_range_maps_to_cue_via_avro(self):
        required = ["z", "b", "i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "f32", "f64", "num", "s", "bin", "arr", "map"]
        structure = {
            "$root": "#/definitions/Types",
            "definitions": {
                "Types": {
                    "name": "Types",
                    "type": "object",
                    "properties": {
                        "z": {"type": "null"},
                        "b": {"type": "boolean"},
                        "i8": {"type": "int8"},
                        "i16": {"type": "int16"},
                        "i32": {"type": "int32"},
                        "i64": {"type": "int64"},
                        "u8": {"type": "uint8"},
                        "u16": {"type": "uint16"},
                        "u32": {"type": "uint32"},
                        "u64": {"type": "uint64"},
                        "f32": {"type": "float32"},
                        "f64": {"type": "float64"},
                        "num": {"type": "number"},
                        "s": {"type": "string"},
                        "bin": {"type": "binary"},
                        "arr": {"type": "array", "items": {"type": "string"}},
                        "map": {"type": "map", "values": {"type": "int64"}},
                    },
                    "required": required,
                }
            },
        }
        cue = self.structure_to_cue(structure)
        for line in (
            "z: null",
            "b: bool",
            "i8: int",
            "i16: int",
            "i32: int",
            "i64: int",
            "u8: int",
            "u16: int",
            "u32: int",
            "u64: int",
            "f32: float",
            "f64: number",
            "num: number",
            "s: string",
            "bin: bytes",
            "arr: [...string]",
            "map: { [string]: int }",
        ):
            with self.subTest(line=line):
                self.assertIn(line, cue)

    # -- adversarial / error paths --------------------------------------------

    def test_missing_cue_file_raises_filenotfounderror(self):
        with self.assertRaises(FileNotFoundError):
            convert_cue_to_avro(str(self.tmp / "missing.cue"), str(self.tmp / "out.avsc"))

    def test_missing_avro_file_raises_filenotfounderror(self):
        with self.assertRaises(FileNotFoundError):
            convert_avro_to_cue(str(self.tmp / "missing.avsc"), str(self.tmp / "out.cue"))

    def test_malformed_cue_is_graceful_with_warning_and_valid_avro(self):
        schema, by_name, warnings = self.cue_text_to_avro("#Bad: { x: string", root_name="Root")
        self.assertEqual(schema["name"], "Bad")
        self.assertEqual(self.field(by_name["Bad"], "x")["type"], "string")
        self.assertIn("expected '}' near ''", warnings)
        self.assertIn("CUE conversion notes", schema["doc"])

    def test_empty_document_produces_empty_root_record(self):
        schema, by_name, _ = self.cue_text_to_avro("", root_name="Document")
        self.assertEqual(schema, {"type": "record", "name": "Document", "fields": [], "namespace": NS})
        self.assertEqual(by_name["Document"]["fields"], [])

    def test_unsupported_constructs_do_not_crash_and_emit_notes(self):
        schema, by_name, warnings = self.cue_text_to_avro(
            """
            #Metric: {
              selector: =~"cpu|mem"
              unknown: @tag(foo)
              concrete: string
            }
            """
        )
        self.assertEqual(schema["name"], "Metric")
        self.assertEqual(self.field(by_name["Metric"], "selector")["type"], "string")
        self.assertEqual(self.field(by_name["Metric"], "unknown")["type"], "string")
        self.assertEqual(self.field(by_name["Metric"], "concrete")["type"], "string")
        self.assertTrue(any("unsupported expression token" in warning for warning in warnings))
        self.assertIn("CUE conversion notes", schema["doc"])

    @unittest.expectedFailure
    def test_fixed_length_list_does_not_hang_or_degrade(self):
        # BUG: parsing [3]string never advances past the closing bracket and hangs.
        code = (
            "from avrotize.cuetoavro import CueToAvroConverter; "
            "s=CueToAvroConverter().convert_text('#A: { xs: [3]string }','Root'); "
            "assert s['fields'][0]['type'] == {'type':'array','items':'string'}"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            timeout=3,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)

    def test_duplicate_after_name_sanitization_is_disambiguated(self):
        schema, by_name, _ = self.cue_text_to_avro('#D: { "a-b": string, a_b: int }')
        names = [field["name"] for field in by_name["D"]["fields"]]
        self.assertEqual(names, ["a_b", "a_b_1"])
        parse_schema(schema)

    def test_unicode_string_values_and_quoted_non_ascii_field_names(self):
        schema, by_name = self.cue_file_to_avro(
            """
            #Unicode: {
              "café": string
              label: string | *"café ❤"
            }
            """,
            filename="unicode.cue",
        )
        rec = by_name["Unicode"]
        cafe = self.field(rec, "caf_")
        self.assertEqual(cafe["type"], "string")
        self.assertEqual(cafe["altnames"], {"cue": "café"})
        self.assertEqual(self.field(rec, "label")["default"], "café ❤")
        parse_schema(schema)


if __name__ == "__main__":
    unittest.main()
