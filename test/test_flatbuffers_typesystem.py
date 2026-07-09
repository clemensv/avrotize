"""Full type-system range and adversarial coverage for FlatBuffers converters.

This suite deliberately exercises every FlatBuffers <-> Avrotize type mapping
branch (see avrotize/flatbufferstoavro.py and avrotize/avrotoflatbuffers.py)
plus JSON Structure bridge behavior, recursion, lossy round trips, and error
paths. It complements the happy-path fixtures in test_flatbufferstoavro.py.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from fastavro.schema import parse_schema

from avrotize.avrotoflatbuffers import convert_avro_to_flatbuffers
from avrotize.flatbufferstoavro import FlatBuffersParser, convert_flatbuffers_to_avro
from avrotize.flatbufferstojstruct import convert_flatbuffers_to_json_structure
from avrotize.jstructtoflatbuffers import convert_json_structure_to_flatbuffers

ROOT = Path(__file__).resolve().parents[1]
NS = "Example.Flat.Types"


class FlatBuffersTypeSystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    # -- helpers ---------------------------------------------------------------

    def fbs_to_avro(self, idl: str, namespace: str | None = None):
        src = self.tmp / "schema.fbs"
        src.write_text(idl, encoding="utf-8")
        out = self.tmp / "schema.avsc"
        convert_flatbuffers_to_avro(str(src), str(out), namespace=namespace)
        schema = json.loads(out.read_text(encoding="utf-8"))
        parse_schema(schema)  # every generated Avro schema must be valid Avro
        by_name = {s["name"]: s for s in schema if isinstance(s, dict) and "name" in s}
        return schema, by_name

    def fbs_to_avro_without_parse(self, idl: str, namespace: str | None = None):
        src = self.tmp / "schema.fbs"
        src.write_text(idl, encoding="utf-8")
        out = self.tmp / "schema.avsc"
        convert_flatbuffers_to_avro(str(src), str(out), namespace=namespace)
        return json.loads(out.read_text(encoding="utf-8"))

    def avro_to_fbs(self, schema: Any, namespace: str | None = None) -> str:
        src = self.tmp / "schema.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "schema.fbs"
        convert_avro_to_flatbuffers(str(src), str(out), namespace=namespace)
        return out.read_text(encoding="utf-8")

    def fbs_to_structure(self, idl: str):
        src = self.tmp / "schema.fbs"
        src.write_text(idl, encoding="utf-8")
        out = self.tmp / "schema.struct.json"
        convert_flatbuffers_to_json_structure(str(src), str(out))
        return json.loads(out.read_text(encoding="utf-8"))

    def structure_to_fbs(self, structure: dict[str, Any]) -> str:
        src = self.tmp / "schema.struct.json"
        src.write_text(json.dumps(structure), encoding="utf-8")
        out = self.tmp / "schema.fbs"
        convert_json_structure_to_flatbuffers(str(src), str(out))
        return out.read_text(encoding="utf-8")

    def field(self, by_name, record, name):
        return next(f for f in by_name[record]["fields"] if f["name"] == name)

    @staticmethod
    def non_null(avro_type):
        return [t for t in avro_type if t != "null"][0] if isinstance(avro_type, list) and "null" in avro_type else avro_type

    # -- FlatBuffers -> Avro: full scalar range --------------------------------

    def test_every_flatbuffers_scalar_maps_to_expected_avro_type(self):
        idl = f"""
        namespace {NS};
        table Scalars {{
          f_bool:bool (required);
          f_byte:byte (required);
          f_int8:int8 (required);
          f_ubyte:ubyte (required);
          f_uint8:uint8 (required);
          f_short:short (required);
          f_int16:int16 (required);
          f_ushort:ushort (required);
          f_uint16:uint16 (required);
          f_int:int (required);
          f_int32:int32 (required);
          f_uint:uint (required);
          f_uint32:uint32 (required);
          f_long:long (required);
          f_int64:int64 (required);
          f_ulong:ulong (required);
          f_uint64:uint64 (required);
          f_float:float (required);
          f_float32:float32 (required);
          f_double:double (required);
          f_float64:float64 (required);
          f_string:string (required);
        }}
        root_type Scalars;
        """
        _, by_name = self.fbs_to_avro(idl)
        expected = {
            "f_bool": "boolean",
            "f_byte": "int",
            "f_int8": "int",
            "f_ubyte": "int",
            "f_uint8": "int",
            "f_short": "int",
            "f_int16": "int",
            "f_ushort": "int",
            "f_uint16": "int",
            "f_int": "int",
            "f_int32": "int",
            "f_uint": "long",
            "f_uint32": "long",
            "f_long": "long",
            "f_int64": "long",
            "f_ulong": "long",
            "f_uint64": "long",
            "f_float": "float",
            "f_float32": "float",
            "f_double": "double",
            "f_float64": "double",
            "f_string": "string",
        }
        for name, avro in expected.items():
            with self.subTest(field=name):
                field = self.field(by_name, "Scalars", name)
                self.assertEqual(field["type"], avro)
                self.assertTrue(field["fbsRequired"])

    def test_table_optionality_required_and_defaults_are_preserved(self):
        idl = f"""
        namespace {NS};
        table Defaults {{
          optional_count:int = 7;
          required_name:string (required);
          required_flag:bool = true (required);
        }}
        root_type Defaults;
        """
        _, by_name = self.fbs_to_avro(idl)
        self.assertEqual(self.field(by_name, "Defaults", "optional_count")["type"], ["null", "int"])
        self.assertIsNone(self.field(by_name, "Defaults", "optional_count")["default"])
        self.assertEqual(self.field(by_name, "Defaults", "optional_count")["fbsDefault"], 7)
        self.assertEqual(self.field(by_name, "Defaults", "required_name")["type"], "string")
        self.assertTrue(self.field(by_name, "Defaults", "required_name")["fbsRequired"])
        self.assertEqual(self.field(by_name, "Defaults", "required_flag")["type"], "boolean")
        self.assertEqual(self.field(by_name, "Defaults", "required_flag")["default"], True)

    # -- FlatBuffers -> Avro: containers ---------------------------------------

    def test_vectors_map_to_arrays_and_ubyte_vector_maps_to_bytes(self):
        idl = f"""
        namespace {NS};
        table Weapon {{ name:string (required); }}
        table Containers {{
          ints:[int];
          strings:[string];
          weapons:[Weapon];
          bytes:[ubyte];
          uint8_bytes:[uint8];
        }}
        root_type Containers;
        """
        _, by_name = self.fbs_to_avro(idl)
        self.assertEqual(self.non_null(self.field(by_name, "Containers", "ints")["type"]), {"type": "array", "items": "int"})
        self.assertEqual(self.non_null(self.field(by_name, "Containers", "strings")["type"]), {"type": "array", "items": "string"})
        self.assertEqual(
            self.non_null(self.field(by_name, "Containers", "weapons")["type"]),
            {"type": "array", "items": f"{NS}.Weapon"},
        )
        self.assertEqual(self.non_null(self.field(by_name, "Containers", "bytes")["type"]), "bytes")
        self.assertEqual(self.non_null(self.field(by_name, "Containers", "uint8_bytes")["type"]), "bytes")

    @unittest.expectedFailure
    def test_nested_vectors_map_to_nested_avro_arrays(self):
        # BUG: FlatBuffersParser only accepts one vector bracket level; [[int]]
        # raises ValueError("Expected ']', got 'int'") instead of producing an
        # Avro array whose items are arrays.
        idl = f"namespace {NS}; table Nested {{ matrix:[[int]]; }} root_type Nested;"
        _, by_name = self.fbs_to_avro(idl)
        self.assertEqual(
            self.non_null(self.field(by_name, "Nested", "matrix")["type"]),
            {"type": "array", "items": {"type": "array", "items": "int"}},
        )

    # -- FlatBuffers -> Avro: named types --------------------------------------

    def test_named_types_namespace_enum_struct_union_references_and_root_type(self):
        idl = f"""
        namespace {NS};
        enum Color : ubyte {{ Red = 1, Green, Blue = 4 }}
        struct Vec3 {{ x:float = 0.0; y:float = 0.0; z:float = 0.0; }}
        table Sword {{ damage:short = 5; }}
        table Spell {{ cost:uint = 99; }}
        union Equipment {{ Sword, Spell }}
        table Monster {{
          pos:Vec3 (required);
          favorite:Color = Blue;
          equip:Equipment;
          swords:[Sword];
        }}
        root_type Monster;
        """
        schema, by_name = self.fbs_to_avro(idl)
        self.assertEqual([s["name"] for s in schema], ["Color", "Vec3", "Sword", "Spell", "Monster"])
        self.assertEqual(by_name["Color"]["namespace"], NS)
        self.assertEqual(by_name["Color"]["symbols"], ["Red", "Green", "Blue"])
        self.assertEqual(by_name["Color"]["ordinals"], {"Red": 1, "Green": 2, "Blue": 4})
        self.assertIn("base type ubyte", by_name["Color"]["doc"])
        self.assertIn("fixed inline memory layout", by_name["Vec3"]["doc"])
        self.assertEqual(self.field(by_name, "Vec3", "x")["default"], 0.0)
        self.assertEqual(self.field(by_name, "Monster", "pos")["type"], f"{NS}.Vec3")
        self.assertEqual(self.non_null(self.field(by_name, "Monster", "favorite")["type"]), f"{NS}.Color")
        self.assertEqual(self.field(by_name, "Monster", "favorite")["fbsDefault"], "Blue")
        self.assertEqual(self.field(by_name, "Monster", "equip")["type"], ["null", f"{NS}.Sword", f"{NS}.Spell"])
        self.assertEqual(self.non_null(self.field(by_name, "Monster", "swords")["type"]), {"type": "array", "items": f"{NS}.Sword"})
        self.assertTrue(by_name["Monster"]["root_type"])

    # -- recursion -------------------------------------------------------------

    def test_self_recursive_table_emits_valid_nullable_named_reference(self):
        idl = f"""
        namespace {NS};
        table Node {{ value:int; next:Node; }}
        root_type Node;
        """
        _, by_name = self.fbs_to_avro(idl)
        self.assertEqual(self.field(by_name, "Node", "next")["type"], ["null", f"{NS}.Node"])

    def test_mutually_recursive_tables_emit_parseable_avro(self):
        idl = f"""
        namespace {NS};
        table A {{ b:B; }}
        table B {{ a:A; }}
        root_type A;
        """
        _, by_name = self.fbs_to_avro(idl)
        a_b = self.non_null(self.field(by_name, "A", "b")["type"])
        self.assertEqual(a_b["name"] if isinstance(a_b, dict) else a_b, "B")
        b_a = next(f for f in a_b["fields"] if f["name"] == "a")
        self.assertEqual(b_a["type"], ["null", f"{NS}.A"])

    # -- Avro -> FlatBuffers: full primitive range -----------------------------

    def test_avro_primitives_map_to_expected_flatbuffers_types(self):
        schema = {
            "type": "record",
            "name": "Prims",
            "namespace": NS,
            "root_type": True,
            "fields": [
                {"name": "fb", "type": "boolean"},
                {"name": "fi", "type": "int"},
                {"name": "fl", "type": "long"},
                {"name": "ff", "type": "float"},
                {"name": "fd", "type": "double"},
                {"name": "fbytes", "type": "bytes"},
                {"name": "fs", "type": "string"},
                {"name": "maybe", "type": ["null", "string"], "default": None},
            ],
        }
        text = self.avro_to_fbs(schema)
        self.assertIn("fb: bool;", text)
        self.assertIn("fi: int;", text)
        self.assertIn("fl: long;", text)
        self.assertIn("ff: float;", text)
        self.assertIn("fd: double;", text)
        self.assertIn("fbytes: [ubyte];", text)
        self.assertIn("fs: string (required);", text)
        self.assertIn("maybe: string;", text)
        self.assertIn("root_type Prims;", text)
        FlatBuffersParser(text).parse()

    @unittest.expectedFailure
    def test_avro_null_primitive_is_not_emitted_as_invalid_flatbuffers_type(self):
        # BUG: Avro primitive "null" renders as a FlatBuffers field type named
        # "null", which is not a FlatBuffers scalar, table, struct, enum, or union.
        schema = {"type": "record", "name": "N", "namespace": NS, "fields": [{"name": "nothing", "type": "null"}]}
        text = self.avro_to_fbs(schema)
        self.assertNotIn("nothing: null", text)

    def test_avro_containers_enums_and_unions_map_to_flatbuffers(self):
        schema = [
            {"type": "enum", "name": "Kind", "namespace": NS, "symbols": ["A", "B"], "ordinals": {"A": 5, "B": 6}},
            {"type": "record", "name": "Left", "namespace": NS, "fields": [{"name": "x", "type": "int"}]},
            {"type": "record", "name": "Right", "namespace": NS, "fields": [{"name": "s", "type": "string"}]},
            {
                "type": "record",
                "name": "Holder",
                "namespace": NS,
                "root_type": True,
                "fields": [
                    {"name": "ints", "type": {"type": "array", "items": "int"}},
                    {"name": "bytes", "type": {"type": "array", "items": "bytes"}},
                    {"name": "lookup", "type": {"type": "map", "values": "string"}},
                    {"name": "kind", "type": f"{NS}.Kind"},
                    {"name": "either", "type": [f"{NS}.Left", f"{NS}.Right"]},
                ],
            },
        ]
        text = self.avro_to_fbs(schema)
        self.assertIn("enum Kind : int { A = 5, B = 6 }", text)
        self.assertIn("ints: [int];", text)
        self.assertIn("bytes: [[ubyte]];", text)  # lossy/invalid nested vector for array<bytes>, pinned below
        self.assertIn("lookup: string (required);", text)  # Avro map has no FlatBuffers equivalent here
        self.assertIn("kind: Kind;", text)
        self.assertIn("union Holder_either_Union { Left, Right }", text)
        self.assertIn("either: Holder_either_Union (required);", text)

    @unittest.expectedFailure
    def test_avro_nested_arrays_emit_parseable_flatbuffers(self):
        # BUG: avro_type_to_fbs recursively renders nested arrays as [[int]],
        # which FlatBuffersParser cannot parse and FlatBuffers schemas do not
        # support directly.
        schema = {
            "type": "record",
            "name": "Nested",
            "namespace": NS,
            "fields": [{"name": "matrix", "type": {"type": "array", "items": {"type": "array", "items": "int"}}}],
        }
        text = self.avro_to_fbs(schema)
        FlatBuffersParser(text).parse()

    # -- round-trip fidelity ---------------------------------------------------

    def test_fbs_avro_fbs_round_trip_pins_lossy_integer_and_struct_behavior(self):
        idl = f"""
        namespace {NS};
        struct Vec {{ x:short = 3; }}
        table Root {{
          b:byte (required);
          u:uint (required);
          bytes:[ubyte];
          pos:Vec (required);
        }}
        root_type Root;
        """
        schema, by_name = self.fbs_to_avro(idl)
        text = self.avro_to_fbs(schema)
        self.assertIn("namespace Example.Flat.Types;", text)
        self.assertIn("table Vec {", text)  # FlatBuffers struct is lossy: Avro record returns as table
        self.assertIn("x: int = 3;", text)  # short collapses through Avro int
        self.assertIn("b: int;", text)  # byte collapses through Avro int
        self.assertIn("u: long;", text)  # uint widens through Avro long
        self.assertIn("bytes: [ubyte];", text)
        self.assertIn("pos: Vec (required);", text)
        self.assertIn("root_type Root;", text)
        roundtrip_path = self.tmp / "roundtrip.fbs"
        roundtrip_path.write_text(text, encoding="utf-8")
        out = self.tmp / "roundtrip.avsc"
        convert_flatbuffers_to_avro(str(roundtrip_path), str(out))
        parse_schema(json.loads(out.read_text(encoding="utf-8")))
        self.assertTrue(by_name["Root"]["root_type"])

    @unittest.expectedFailure
    def test_table_field_fbs_default_survives_fbs_avro_fbs_round_trip(self):
        # BUG: fbsDefault on nullable table fields is ignored by avrotoflatbuffers
        # because render_record only emits defaults when field["type"] is not a list.
        idl = f"namespace {NS}; table Root {{ hp:int = 100; }} root_type Root;"
        schema, _ = self.fbs_to_avro(idl)
        text = self.avro_to_fbs(schema)
        self.assertIn("hp: int = 100;", text)

    def test_json_structure_bridge_round_trip_pins_lossy_behavior_and_root(self):
        idl = f"""
        namespace {NS};
        enum Color : ubyte {{ Red = 1, Blue = 4 }}
        struct Pos {{ x:float; }}
        table Root {{ color:Color; pos:Pos (required); count:uint; }}
        root_type Root;
        """
        structure = self.fbs_to_structure(idl)
        defs = structure["definitions"]["Example"]["Flat"]["Types"]
        self.assertEqual(structure["name"], "Root")
        self.assertEqual(structure["$root"], "#/definitions/Example/Flat/Types/Root")
        self.assertEqual(defs["Color"]["type"], "string")
        self.assertEqual(defs["Color"]["enum"], ["Red", "Blue"])
        self.assertEqual(defs["Pos"]["type"], "object")
        self.assertIn("fixed inline memory layout", defs["Pos"]["description"])
        self.assertEqual(defs["Root"]["properties"]["count"]["type"], "int64")
        text = self.structure_to_fbs(structure)
        self.assertIn("namespace Example.Flat.Types;", text)
        self.assertIn("enum Color : int { Red = 0, Blue = 1 }", text)  # enum base and ordinals are lost by bridge
        self.assertIn("table Pos {", text)  # struct returns as table through JSON Structure
        self.assertIn("count: long;", text)
        self.assertIn("root_type Root;", text)
        FlatBuffersParser(text).parse()

    # -- JSON Structure bridge: full scalar/container range --------------------

    def test_flatbuffers_to_structure_full_type_range(self):
        idl = f"""
        namespace {NS};
        enum E : short {{ A = 1, B = 2 }}
        struct S {{ x:int; }}
        table Root {{
          b:bool;
          i8:byte;
          u8:ubyte;
          i16:short;
          u16:ushort;
          i32:int;
          u32:uint;
          i64:long;
          u64:ulong;
          f32:float;
          f64:double;
          s:string (required);
          blob:[ubyte];
          nums:[int];
          e:E;
          ref:S (required);
        }}
        root_type Root;
        """
        structure = self.fbs_to_structure(idl)
        props = structure["definitions"]["Example"]["Flat"]["Types"]["Root"]["properties"]
        expected = {
            "b": "boolean",
            "i8": "int32",
            "u8": "int32",
            "i16": "int32",
            "u16": "int32",
            "i32": "int32",
            "u32": "int64",
            "i64": "int64",
            "u64": "int64",
            "f32": "float",
            "f64": "double",
            "s": "string",
            "blob": "binary",
        }
        for name, struct_type in expected.items():
            with self.subTest(field=name):
                self.assertEqual(props[name]["type"], struct_type)
        self.assertEqual(props["nums"], {"type": "array", "items": {"type": "int32"}, "default": None})
        self.assertEqual(props["e"], {"$ref": "#/definitions/Example/Flat/Types/E", "default": None})
        self.assertEqual(props["ref"], {"$ref": "#/definitions/Example/Flat/Types/S"})

    def test_structure_to_flatbuffers_full_type_range(self):
        structure = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "name": "Root",
            "$root": "#/definitions/Example/Flat/Types/Root",
            "definitions": {
                "Example": {
                    "Flat": {
                        "Types": {
                            "Root": {
                                "name": "Root",
                                "type": "object",
                                "required": ["b", "i8", "u8", "i16", "u16", "i32", "u32", "i64", "u64", "f", "d", "s", "bin"],
                                "properties": {
                                    "b": {"type": "boolean"},
                                    "i8": {"type": "int8"},
                                    "u8": {"type": "uint8"},
                                    "i16": {"type": "int16"},
                                    "u16": {"type": "uint16"},
                                    "i32": {"type": "int32"},
                                    "u32": {"type": "uint32"},
                                    "i64": {"type": "int64"},
                                    "u64": {"type": "uint64"},
                                    "f": {"type": "float"},
                                    "d": {"type": "double"},
                                    "s": {"type": "string"},
                                    "bin": {"type": "binary"},
                                    "arr": {"type": "array", "items": {"type": "uint16"}},
                                    "map": {"type": "map", "values": {"type": "string"}},
                                },
                            }
                        }
                    }
                }
            },
        }
        text = self.structure_to_fbs(structure)
        expected_lines = [
            "b: bool;",
            "i8: int;",
            "u8: int;",
            "i16: int;",
            "u16: int;",
            "i32: int;",
            "u32: long;",
            "i64: long;",
            "u64: long;",
            "f: float;",
            "d: double;",
            "s: string (required);",
            "bin: [ubyte];",
            "arr: [int];",
            "map: string;",
            "root_type Root;",
        ]
        for line in expected_lines:
            with self.subTest(line=line):
                self.assertIn(line, text)
        FlatBuffersParser(text).parse()

    # -- adversarial / error paths ---------------------------------------------

    def test_missing_input_files_raise_documented_exceptions(self):
        with self.assertRaises(FileNotFoundError):
            convert_flatbuffers_to_avro(str(self.tmp / "missing.fbs"), str(self.tmp / "out.avsc"))
        with self.assertRaises(FileNotFoundError):
            convert_avro_to_flatbuffers(str(self.tmp / "missing.avsc"), str(self.tmp / "out.fbs"))

    def test_bad_syntax_and_unterminated_table_raise_valueerror(self):
        bad = self.tmp / "bad.fbs"
        bad.write_text(f"namespace {NS}; table Bad {{ x int; }}", encoding="utf-8")
        with self.assertRaises(ValueError):
            convert_flatbuffers_to_avro(str(bad), str(self.tmp / "bad.avsc"))

        unterminated = self.tmp / "unterminated.fbs"
        unterminated.write_text(f"namespace {NS}; table Bad {{ x:int;", encoding="utf-8")
        with self.assertRaises(ValueError):
            convert_flatbuffers_to_avro(str(unterminated), str(self.tmp / "unterminated.avsc"))

    def test_empty_flatbuffers_schema_produces_empty_valid_avro_schema(self):
        schema = self.fbs_to_avro_without_parse("")
        self.assertEqual(schema, [])
        parse_schema(schema)

    def test_flatbuffers_invalid_names_are_sanitized_and_deduplicated(self):
        idl = f"""
        namespace {NS};
        enum E : int {{ alpha.one = 1, alpha_one = 2 }}
        table Bad {{ a.b:int; a_b:int; e:E; }}
        root_type Bad;
        """
        _, by_name = self.fbs_to_avro(idl)
        self.assertEqual(by_name["E"]["symbols"], ["alpha_one", "alpha_one_1"])
        self.assertIn("a_b_1", [f["name"] for f in by_name["Bad"]["fields"]])

    def test_avro_to_flatbuffers_sanitization_deduplicates_collisions(self):
        schema = {
            "type": "record",
            "name": "Collision",
            "namespace": NS,
            "fields": [{"name": "x-y", "type": "int"}, {"name": "x_y", "type": "int"}],
        }
        text = self.avro_to_fbs(schema)
        self.assertIn("x_y_1: int;", text)

    def test_unicode_string_default_value_is_preserved(self):
        idl = f'namespace {NS}; table U {{ label:string = "caf\u00e9 \u2764"; }} root_type U;'
        _, by_name = self.fbs_to_avro(idl)
        self.assertEqual(self.field(by_name, "U", "label")["fbsDefault"], "caf\u00e9 \u2764")

    def test_non_ascii_identifiers_are_supported_or_cleanly_sanitized(self):
        idl = f'namespace {NS}; table Caf\u00e9 {{ na\u00efve:string = "ok"; }} root_type Caf\u00e9;'
        _, by_name = self.fbs_to_avro(idl)
        self.assertIn("Cafe", by_name)


if __name__ == "__main__":
    unittest.main()
