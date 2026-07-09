"""Full type-system range and adversarial coverage for Cap'n Proto converters.

This suite enumerates the Cap'n Proto <-> Avrotize/Avro mapping branches in
avrotize/capnprototoavro.py and avrotize/avrotocapnproto.py. It complements
the happy-path fixture tests in test_capnprototoavro.py.
"""

from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path

from fastavro.schema import parse_schema

from avrotize.avrotocapnproto import convert_avro_to_capnproto, convert_json_structure_to_capnproto
from avrotize.capnprototoavro import (
    convert_capnproto_to_avro,
    convert_capnproto_to_json_structure,
    parse_capnproto_schema,
)

ROOT = Path(__file__).resolve().parents[1]
NS = "example.capnp"


class CapnProtoTypeSystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    # -- helpers ---------------------------------------------------------------

    def capnp_to_avro(self, schema_text: str, namespace: str | None = NS):
        src = self.tmp / "schema.capnp"
        src.write_text(schema_text, encoding="utf-8")
        out = self.tmp / "schema.avsc"
        convert_capnproto_to_avro(str(src), str(out), namespace=namespace)
        schema = json.loads(out.read_text(encoding="utf-8"))
        parse_schema(schema)  # every generated Avro schema must be valid
        by_name = {s["name"]: s for s in schema if isinstance(s, dict) and "name" in s}
        return schema, by_name

    def avro_to_capnp(self, schema, namespace: str | None = NS) -> str:
        parse_schema(schema)
        src = self.tmp / "schema.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "schema.capnp"
        convert_avro_to_capnproto(str(src), str(out), namespace=namespace)
        return out.read_text(encoding="utf-8")

    def capnp_to_structure(self, schema_text: str):
        src = self.tmp / "schema.capnp"
        src.write_text(schema_text, encoding="utf-8")
        out = self.tmp / "schema.struct.json"
        convert_capnproto_to_json_structure(str(src), str(out), namespace=NS)
        return json.loads(out.read_text(encoding="utf-8"))

    def field(self, by_name, record, name):
        return next(f for f in by_name[record]["fields"] if f["name"] == name)

    def non_null(self, avro_type):
        if isinstance(avro_type, list):
            return next(t for t in avro_type if t != "null")
        return avro_type

    def struct_def(self, data, name="Prims"):
        return data["definitions"]["example"]["capnp"][name]

    # -- Cap'n Proto -> Avro: full primitive range ----------------------------

    def test_every_capnp_primitive_maps_to_expected_avro(self):
        capnp = """
        @0xbf5147a1d903c2f0;
        struct Prims {
          voidField @0 :Void;
          boolField @1 :Bool;
          int8Field @2 :Int8;
          int16Field @3 :Int16;
          int32Field @4 :Int32;
          int64Field @5 :Int64;
          uint8Field @6 :UInt8;
          uint16Field @7 :UInt16;
          uint32Field @8 :UInt32;
          uint64Field @9 :UInt64;
          float32Field @10 :Float32;
          float64Field @11 :Float64;
          textField @12 :Text;
          dataField @13 :Data;
        }
        """
        _, by_name = self.capnp_to_avro(capnp)
        expected = {
            "voidField": "null",
            "boolField": "boolean",
            "int8Field": "int",
            "int16Field": "int",
            "int32Field": "int",
            "int64Field": "long",
            "uint8Field": "long",
            "uint16Field": "long",
            "uint32Field": "long",
            "uint64Field": "long",
            "float32Field": "float",
            "float64Field": "double",
            "textField": "string",
            "dataField": "bytes",
        }
        for name, avro_type in expected.items():
            with self.subTest(field=name):
                field = self.field(by_name, "Prims", name)
                self.assertEqual(self.non_null(field["type"]), avro_type)
                self.assertEqual(field["capnpOrdinal"], int(re.search(r"(\d+)$", str(field["capnpOrdinal"])).group(1)))
                self.assertEqual(field["default"], None)
        self.assertEqual(self.field(by_name, "Prims", "voidField")["type"], "null")

    def test_field_ordinals_are_preserved_and_fields_are_nullable(self):
        capnp = """
        @0xbf5147a1d903c2f1;
        struct S {
          later @9 :Text;
          first @0 :UInt16;
          middle @5 :Bool;
        }
        """
        _, by_name = self.capnp_to_avro(capnp)
        self.assertEqual([f["name"] for f in by_name["S"]["fields"]], ["first", "middle", "later"])
        self.assertEqual(self.field(by_name, "S", "first")["capnpOrdinal"], 0)
        self.assertEqual(self.field(by_name, "S", "middle")["capnpOrdinal"], 5)
        self.assertEqual(self.field(by_name, "S", "later")["capnpOrdinal"], 9)
        self.assertEqual(self.field(by_name, "S", "later")["type"], ["null", "string"])

    # -- Cap'n Proto -> Avro: containers --------------------------------------

    def test_lists_and_nested_lists_map_to_arrays(self):
        capnp = """
        @0xbf5147a1d903c2f2;
        struct C {
          texts @0 :List(Text);
          ints @1 :List(Int32);
          nested @2 :List(List(UInt16));
          records @3 :List(Item);
        }
        struct Item { value @0 :Text; }
        """
        _, by_name = self.capnp_to_avro(capnp)
        self.assertEqual(self.non_null(self.field(by_name, "C", "texts")["type"]), {"type": "array", "items": "string"})
        self.assertEqual(self.non_null(self.field(by_name, "C", "ints")["type"]), {"type": "array", "items": "int"})
        self.assertEqual(
            self.non_null(self.field(by_name, "C", "nested")["type"]),
            {"type": "array", "items": {"type": "array", "items": "long"}},
        )
        self.assertEqual(
            self.non_null(self.field(by_name, "C", "records")["type"]),
            {"type": "array", "items": f"{NS}.Item"},
        )

    # -- Cap'n Proto -> Avro: named types -------------------------------------

    def test_enums_symbols_and_capnp_ordinals_are_preserved(self):
        capnp = """
        @0xbf5147a1d903c2f3;
        enum Color { red @0; green @3; blue @7; }
        struct Paint { color @0 :Color; }
        """
        _, by_name = self.capnp_to_avro(capnp)
        self.assertEqual(by_name["Color"]["type"], "enum")
        self.assertEqual(by_name["Color"]["symbols"], ["red", "green", "blue"])
        self.assertEqual(by_name["Color"]["capnpOrdinals"], {"red": 0, "green": 3, "blue": 7})
        self.assertEqual(self.field(by_name, "Paint", "color")["type"], ["null", f"{NS}.Color"])

    def test_struct_refs_nested_namespaces_groups_and_unions(self):
        capnp = """
        @0xbf5147a1d903c2f4;
        struct Address { street @0 :Text; }
        enum Color { red @0; green @1; }
        struct Person {
          name @0 :Text;
          address @1 :Address;
          favorite @2 :Color;
          contact @3 :group {
            email @0 :Text;
            phone @1 :Text;
          }
          union {
            noPet @4 :Void;
            dogName @5 :Text;
            score @6 :UInt8;
          }
          struct Preference { label @0 :Text; weight @1 :Float64; }
          prefs @7 :List(Preference);
          enum Status { active @0; inactive @1; }
          status @8 :Status;
        }
        """
        _, by_name = self.capnp_to_avro(capnp)
        self.assertEqual(self.field(by_name, "Person", "address")["type"], ["null", f"{NS}.Address"])
        self.assertEqual(self.field(by_name, "Person", "favorite")["type"], ["null", f"{NS}.Color"])

        contact = self.non_null(self.field(by_name, "Person", "contact")["type"])
        self.assertEqual(contact["type"], "record")
        self.assertEqual(contact["name"], "Contact")
        self.assertEqual(contact["namespace"], f"{NS}.Person_types")
        self.assertEqual([f["name"] for f in contact["fields"]], ["email", "phone"])

        self.assertEqual(self.field(by_name, "Person", "noPet")["type"], "null")
        self.assertEqual(self.field(by_name, "Person", "noPet")["capnpUnion"], "union")
        self.assertEqual(self.field(by_name, "Person", "dogName")["type"], ["null", "string"])
        self.assertEqual(self.field(by_name, "Person", "dogName")["capnpUnion"], "union")
        self.assertEqual(self.field(by_name, "Person", "score")["type"], ["null", "long"])
        self.assertEqual(self.field(by_name, "Person", "score")["capnpUnion"], "union")

        self.assertEqual(by_name["Preference"]["namespace"], f"{NS}.Person_types")
        self.assertEqual(self.field(by_name, "Person", "prefs")["type"], ["null", {"type": "array", "items": f"{NS}.Person_types.Preference"}])
        self.assertEqual(by_name["Status"]["namespace"], f"{NS}.Person_types")
        self.assertEqual(self.field(by_name, "Person", "status")["type"], ["null", f"{NS}.Person_types.Status"])

    def test_inline_named_union_field_maps_to_nested_record(self):
        capnp = """
        @0xbf5147a1d903c2f5;
        struct S {
          choice @0 :union {
            a @0 :Text;
            b @1 :Int32;
          }
        }
        """
        _, by_name = self.capnp_to_avro(capnp)
        choice_field = self.field(by_name, "S", "choice")
        choice = self.non_null(choice_field["type"])
        self.assertEqual(choice_field["capnpUnion"], "choice")
        self.assertEqual(choice["name"], "Choice")
        self.assertEqual([f["capnpUnion"] for f in choice["fields"]], ["choice", "choice"])
        self.assertEqual(choice["fields"][0]["type"], ["null", "string"])
        self.assertEqual(choice["fields"][1]["type"], ["null", "int"])

    def test_parse_schema_exposes_file_id(self):
        parsed = parse_capnproto_schema("@0xbf5147a1d903c2f6;\nstruct S { x @0 :Text; }")
        self.assertEqual(parsed.file_id, "0xbf5147a1d903c2f6")
        self.assertEqual(parsed.structs[0].name, "S")

    # -- recursion -------------------------------------------------------------

    def test_self_recursive_struct_is_valid_avro(self):
        capnp = """
        @0xbf5147a1d903c2f7;
        struct Node {
          value @0 :Int32;
          next @1 :Node;
          children @2 :List(Node);
        }
        """
        _, by_name = self.capnp_to_avro(capnp)
        self.assertEqual(self.field(by_name, "Node", "next")["type"], ["null", f"{NS}.Node"])
        self.assertEqual(self.field(by_name, "Node", "children")["type"], ["null", {"type": "array", "items": f"{NS}.Node"}])

    def test_mutually_recursive_structs_generate_valid_avro(self):
        capnp = """
        @0xbf5147a1d903c2f8;
        struct A { b @0 :B; }
        struct B { a @0 :A; }
        """
        self.capnp_to_avro(capnp)

    # -- Avro -> Cap'n Proto: full primitive and special range -----------------

    def test_every_avro_primitive_maps_to_expected_capnp(self):
        schema = {
            "type": "record",
            "name": "Prims",
            "namespace": "example.avro",
            "fields": [
                {"name": "fn", "type": "null"},
                {"name": "fb", "type": "boolean"},
                {"name": "fi", "type": "int"},
                {"name": "fl", "type": "long"},
                {"name": "ff", "type": "float"},
                {"name": "fd", "type": "double"},
                {"name": "fby", "type": "bytes"},
                {"name": "fs", "type": "string"},
            ],
        }
        text = self.avro_to_capnp(schema, namespace="example.avro")
        self.assertIn("@0xd12f8a7e9b0c4a6f;", text)
        self.assertIn("# namespace: example.avro", text)
        expected_lines = [
            "fn @0 :Void;",
            "fb @1 :Bool;",
            "fi @2 :Int32;",
            "fl @3 :Int64;",
            "ff @4 :Float32;",
            "fd @5 :Float64;",
            "fby @6 :Data;",
            "fs @7 :Text;",
        ]
        for line in expected_lines:
            with self.subTest(line=line):
                self.assertIn(line, text)
        self.assertEqual(parse_capnproto_schema(text).structs[0].name, "Prims")

    def test_avro_arrays_maps_fixed_nullable_and_multi_unions(self):
        schema = {
            "type": "record",
            "name": "C",
            "namespace": "example.avro",
            "fields": [
                {"name": "items", "type": {"type": "array", "items": "int"}},
                {"name": "nested", "type": {"type": "array", "items": {"type": "array", "items": "string"}}},
                {"name": "dict", "type": {"type": "map", "values": "long"}},
                {"name": "blob16", "type": {"type": "fixed", "name": "Blob16", "size": 16}},
                {"name": "maybe", "type": ["null", "string"], "default": None},
                {"name": "either", "type": ["string", "int"]},
            ],
        }
        text = self.avro_to_capnp(schema, namespace="example.avro")
        self.assertIn("items @0 :List(Int32);", text)
        self.assertIn("nested @1 :List(List(Text));", text)
        self.assertIn("struct DictEntry {", text)
        self.assertIn("key @0 :Text;", text)
        self.assertIn("value @1 :Int64;", text)
        self.assertIn("dict @2 :List(DictEntry);", text)
        self.assertIn("blob16 @3 :Data;", text)
        self.assertIn("maybe @4 :Text;", text)
        self.assertIn("struct EitherChoice {", text)
        self.assertIn("union {", text)
        self.assertIn("choice0 @0 :Text;", text)
        self.assertIn("choice1 @1 :Int32;", text)
        self.assertIn("either @5 :EitherChoice;", text)

    def test_avro_records_enums_references_and_capnp_union_metadata(self):
        schema = [
            {"type": "enum", "name": "Color", "namespace": "example.avro", "symbols": ["red", "green"]},
            {
                "type": "record",
                "name": "Pet",
                "namespace": "example.avro",
                "fields": [{"name": "name", "type": "string"}],
            },
            {
                "type": "record",
                "name": "Owner",
                "namespace": "example.avro",
                "fields": [
                    {"name": "favorite", "type": "example.avro.Color"},
                    {"name": "pet", "type": "example.avro.Pet"},
                    {"name": "noPet", "type": "null", "capnpUnion": "union"},
                    {"name": "dogName", "type": ["null", "string"], "default": None, "capnpUnion": "union"},
                ],
            },
        ]
        text = self.avro_to_capnp(schema, namespace="example.avro")
        self.assertIn("enum Color {", text)
        self.assertIn("red @0;", text)
        self.assertIn("green @1;", text)
        self.assertIn("struct Pet {", text)
        self.assertIn("favorite @0 :Color;", text)
        self.assertIn("pet @1 :Pet;", text)
        self.assertIn("union {\n    noPet @2 :Void;\n    dogName @3 :Text;\n  }", text)

    # -- lossy round-trip fidelity --------------------------------------------

    def test_capnp_avro_capnp_round_trip_pins_lossy_behavior(self):
        capnp = """
        @0xbf5147a1d903c2f9;
        struct S {
          u8 @2 :UInt8;
          i8 @5 :Int8;
          groupField @8 :group { label @0 :Text; }
          union {
            none @9 :Void;
            value @10 :Text;
          }
        }
        """
        schema, _ = self.capnp_to_avro(capnp)
        text = self.avro_to_capnp(schema, namespace=NS)
        self.assertIn("@0xd12f8a7e9b0c4a6f;", text)  # original file id is not retained
        self.assertIn("u8 @0 :Int64;", text)  # UInt8 widens to Avro long -> Int64
        self.assertIn("i8 @1 :Int32;", text)  # Int8 widens to Avro int -> Int32
        self.assertIn("struct GroupField {", text)  # Cap'n Proto group becomes a nested struct field
        self.assertIn("groupField @2 :GroupField;", text)
        self.assertIn("union {\n    none @3 :Void;\n    value @4 :Text;\n  }", text)  # ordinals are regenerated by field index

    def test_capnp_json_structure_capnp_bridge_full_range(self):
        capnp = """
        @0xbf5147a1d903c2fa;
        struct Prims {
          voidField @0 :Void;
          boolField @1 :Bool;
          int8Field @2 :Int8;
          int16Field @3 :Int16;
          int32Field @4 :Int32;
          int64Field @5 :Int64;
          uint8Field @6 :UInt8;
          uint16Field @7 :UInt16;
          uint32Field @8 :UInt32;
          uint64Field @9 :UInt64;
          float32Field @10 :Float32;
          float64Field @11 :Float64;
          textField @12 :Text;
          dataField @13 :Data;
          listText @14 :List(Text);
          nestedList @15 :List(List(UInt16));
        }
        """
        data = self.capnp_to_structure(capnp)
        props = self.struct_def(data)["properties"]
        expected = {
            "voidField": "null",
            "boolField": "boolean",
            "int8Field": "int32",
            "int16Field": "int32",
            "int32Field": "int32",
            "int64Field": "int64",
            "uint8Field": "int64",
            "uint16Field": "int64",
            "uint32Field": "int64",
            "uint64Field": "int64",
            "float32Field": "float",
            "float64Field": "double",
            "textField": "string",
            "dataField": "binary",
        }
        for name, typ in expected.items():
            with self.subTest(name=name):
                self.assertEqual(props[name]["type"], typ)
        self.assertEqual(props["listText"], {"type": "array", "items": {"type": "string"}, "default": None})
        self.assertEqual(
            props["nestedList"],
            {"type": "array", "items": {"type": "array", "items": {"type": "int64"}}, "default": None},
        )

        struct_file = self.tmp / "schema.struct.json"
        struct_file.write_text(json.dumps(data), encoding="utf-8")
        capnp_file = self.tmp / "bridge.capnp"
        convert_json_structure_to_capnproto(str(struct_file), str(capnp_file), namespace=NS)
        bridged = capnp_file.read_text(encoding="utf-8")
        self.assertIn("@0xd12f8a7e9b0c4a6f;", bridged)
        self.assertIn("uint8Field @6 :Int64;", bridged)
        self.assertIn("int8Field @2 :Int32;", bridged)
        self.assertIn("nestedList @15 :List(List(Int64));", bridged)
        parse_capnproto_schema(bridged)

    # -- adversarial / error paths --------------------------------------------

    def test_missing_capnp_input_file_raises_filenotfounderror(self):
        with self.assertRaises(FileNotFoundError):
            convert_capnproto_to_avro(str(self.tmp / "missing.capnp"), str(self.tmp / "out.avsc"), namespace=NS)

    def test_missing_avro_input_file_raises_filenotfounderror(self):
        with self.assertRaises(FileNotFoundError):
            convert_avro_to_capnproto(str(self.tmp / "missing.avsc"), str(self.tmp / "out.capnp"), namespace=NS)

    def test_malformed_capnp_bad_syntax_raises_valueerror(self):
        src = self.tmp / "bad.capnp"
        src.write_text("@0xbf5147a1d903c2fb;\nstruct S { x @0 Text; }", encoding="utf-8")
        with self.assertRaises(ValueError):
            convert_capnproto_to_avro(str(src), str(self.tmp / "bad.avsc"), namespace=NS)

    def test_unterminated_capnp_struct_raises_valueerror(self):
        src = self.tmp / "unterminated.capnp"
        src.write_text("@0xbf5147a1d903c2fc;\nstruct S { x @0 :Text;", encoding="utf-8")
        with self.assertRaises(ValueError):
            convert_capnproto_to_avro(str(src), str(self.tmp / "bad.avsc"), namespace=NS)

    def test_empty_schema_produces_empty_valid_avro_list(self):
        schema, by_name = self.capnp_to_avro("")
        self.assertEqual(schema, [])
        self.assertEqual(by_name, {})

    def test_avro_to_capnp_sanitizes_field_record_and_enum_symbol_names(self):
        # Deliberately bypass parse_schema here: this asserts the converter's
        # sanitization of Avrotize/Avro-like input containing unsafe identifiers.
        schema = {
            "type": "record",
            "name": "9-record",
            "namespace": "example.avro",
            "fields": [
                {"name": "field-name", "type": "string"},
                {"name": "1st", "type": "int"},
            ],
        }
        src = self.tmp / "unsafe.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "unsafe.capnp"
        convert_avro_to_capnproto(str(src), str(out), namespace="example.avro")
        text = out.read_text(encoding="utf-8")
        self.assertIn("struct _9_record {", text)
        self.assertIn("field_name @0 :Text;", text)
        self.assertIn("_1st @1 :Int32;", text)

        enum_src = self.tmp / "unsafe-enum.avsc"
        enum_src.write_text(json.dumps({"type": "enum", "name": "bad-enum", "symbols": ["not-ok", "2bad"]}), encoding="utf-8")
        enum_out = self.tmp / "unsafe-enum.capnp"
        convert_avro_to_capnproto(str(enum_src), str(enum_out), namespace="example.avro")
        enum_text = enum_out.read_text(encoding="utf-8")
        self.assertIn("enum bad_enum {", enum_text)
        self.assertIn("not_ok @0;", enum_text)
        self.assertIn("_2bad @1;", enum_text)

    def test_avro_to_capnp_sanitizes_inline_enum_field_type_name(self):
        schema = {
            "type": "record",
            "name": "UnsafeInlineEnum",
            "fields": [{"name": "e", "type": {"type": "enum", "name": "bad-enum", "symbols": ["ok"]}}],
        }
        src = self.tmp / "unsafe-inline-enum.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "unsafe-inline-enum.capnp"
        convert_avro_to_capnproto(str(src), str(out), namespace=NS)
        self.assertIn("e @0 :bad_enum;", out.read_text(encoding="utf-8"))

    def test_avro_to_capnp_deduplicates_after_sanitization_collisions(self):
        schema = {
            "type": "record",
            "name": "Collision",
            "fields": [{"name": "a-b", "type": "string"}, {"name": "a_b", "type": "int"}],
        }
        src = self.tmp / "collision.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "collision.capnp"
        convert_avro_to_capnproto(str(src), str(out), namespace=NS)
        text = out.read_text(encoding="utf-8")
        self.assertIn("a_b @0 :Text;", text)
        self.assertIn("a_b_1 @1 :Int32;", text)

    def test_capnp_to_avro_sanitizes_invalid_enum_symbols_cleanly(self):
        capnp = """
        @0xbf5147a1d903c2fd;
        enum E { 1bad @0; ok @1; }
        """
        src = self.tmp / "invalid-enum.capnp"
        src.write_text(capnp, encoding="utf-8")
        out = self.tmp / "invalid-enum.avsc"
        convert_capnproto_to_avro(str(src), str(out), namespace=NS)
        schema = json.loads(out.read_text(encoding="utf-8"))
        parse_schema(schema)
        self.assertEqual(schema[0]["symbols"], ["_1bad", "ok"])
        self.assertEqual(schema[0]["capnpOrdinals"], {"_1bad": 0, "ok": 1})

    def test_unicode_text_default_is_accepted_but_not_preserved(self):
        capnp = '@0xbf5147a1d903c2fe;\nstruct S { label @0 :Text = "café ❤"; }'
        _, by_name = self.capnp_to_avro(capnp)
        label = self.field(by_name, "S", "label")
        self.assertEqual(label["type"], ["null", "string"])
        self.assertEqual(label["default"], None)  # Cap'n Proto defaults are currently ignored.


if __name__ == "__main__":
    unittest.main()
