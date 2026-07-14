"""Full type-system range and adversarial coverage for the Thrift converters.

This suite deliberately exercises *every* branch of the Thrift <-> Avrotize type
mapping (see avrotize/thrifttoavro.py and avrotize/avrotothrift.py) plus error
paths, name-sanitization edge cases, recursion, and known-lossy round trips.
It complements the happy-path fixtures in test_thrifttoavro.py.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from fastavro.schema import parse_schema

from avrotize.avrotothrift import convert_avro_to_thrift
from avrotize.thrifttoavro import convert_thrift_to_avro

ROOT = Path(__file__).resolve().parents[1]
NS = "example.thrift"


class ThriftTypeSystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    # -- helpers ---------------------------------------------------------------

    def thrift_to_avro(self, idl: str, namespace: str | None = NS):
        src = self.tmp / "schema.thrift"
        src.write_text(idl, encoding="utf-8")
        out = self.tmp / "schema.avsc"
        convert_thrift_to_avro(str(src), str(out), namespace=namespace)
        schema = json.loads(out.read_text(encoding="utf-8"))
        parse_schema(schema)  # every generated schema must be valid Avro
        by_name = {s["name"]: s for s in schema if isinstance(s, dict) and "name" in s}
        return schema, by_name

    def avro_to_thrift(self, schema, namespace: str | None = NS) -> str:
        src = self.tmp / "schema.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "schema.thrift"
        convert_avro_to_thrift(str(src), str(out), namespace=namespace)
        return out.read_text(encoding="utf-8")

    def field(self, by_name, record, name):
        return next(f for f in by_name[record]["fields"] if f["name"] == name)

    # -- Thrift -> Avro: full primitive range ----------------------------------

    def test_every_thrift_primitive_maps_to_expected_avro(self):
        idl = f"""
        namespace * {NS}
        struct Prims {{
          1: required bool f_bool;
          2: required byte f_byte;
          3: required i8 f_i8;
          4: required i16 f_i16;
          5: required i32 f_i32;
          6: required i64 f_i64;
          7: required double f_double;
          8: required string f_string;
          9: required binary f_binary;
        }}
        """
        _, by_name = self.thrift_to_avro(idl)
        expected = {
            "f_bool": "boolean",
            "f_byte": "int",
            "f_i8": "int",
            "f_i16": "int",
            "f_i32": "int",
            "f_i64": "long",
            "f_double": "double",
            "f_string": "string",
            "f_binary": "bytes",
        }
        for name, avro in expected.items():
            with self.subTest(field=name):
                self.assertEqual(self.field(by_name, "Prims", name)["type"], avro)

    def test_required_primitive_has_no_default_key(self):
        idl = f"namespace * {NS}\nstruct S {{ 1: required i32 x; }}"
        _, by_name = self.thrift_to_avro(idl)
        self.assertNotIn("default", self.field(by_name, "S", "x"))

    def test_field_ids_are_preserved_and_autoincrement(self):
        idl = f"""
        namespace * {NS}
        struct S {{
          5: required i32 a;
          required i32 b;
          10: required i32 c;
          required i32 d;
        }}
        """
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(self.field(by_name, "S", "a")["thrift_id"], 5)
        self.assertEqual(self.field(by_name, "S", "b")["thrift_id"], 6)
        self.assertEqual(self.field(by_name, "S", "c")["thrift_id"], 10)
        self.assertEqual(self.field(by_name, "S", "d")["thrift_id"], 11)

    # -- Thrift -> Avro: containers --------------------------------------------

    def test_list_and_set_map_to_array(self):
        idl = f"""
        namespace * {NS}
        struct S {{
          1: required list<i32> l;
          2: required set<string> s;
        }}
        """
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(self.field(by_name, "S", "l")["type"], {"type": "array", "items": "int"})
        self.assertEqual(self.field(by_name, "S", "s")["type"], {"type": "array", "items": "string"})

    def test_string_keyed_map_maps_to_avro_map(self):
        idl = f"namespace * {NS}\nstruct S {{ 1: required map<string, i64> m; }}"
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(self.field(by_name, "S", "m")["type"], {"type": "map", "values": "long"})

    def test_non_string_keyed_map_maps_to_entry_array(self):
        idl = f"namespace * {NS}\nstruct S {{ 1: required map<i32, string> m; }}"
        _, by_name = self.thrift_to_avro(idl)
        m = self.field(by_name, "S", "m")["type"]
        self.assertEqual(m["type"], "array")
        entry = m["items"]
        self.assertEqual(entry["type"], "record")
        self.assertEqual(entry["fields"][0], {"name": "key", "type": "int"})
        self.assertEqual(entry["fields"][1], {"name": "value", "type": "string"})

    def test_nested_containers(self):
        idl = f"""
        namespace * {NS}
        struct S {{
          1: required list<list<string>> ll;
          2: required map<string, list<i32>> mli;
        }}
        """
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(
            self.field(by_name, "S", "ll")["type"],
            {"type": "array", "items": {"type": "array", "items": "string"}},
        )
        self.assertEqual(
            self.field(by_name, "S", "mli")["type"],
            {"type": "map", "values": {"type": "array", "items": "int"}},
        )

    # -- Thrift -> Avro: named types -------------------------------------------

    def test_enum_ordinals_explicit_and_running(self):
        idl = f"""
        namespace * {NS}
        enum E {{ A = 5, B, C = 10, D }}
        struct S {{ 1: required E e; }}
        """
        _, by_name = self.thrift_to_avro(idl)
        e = by_name["E"]
        self.assertEqual(e["type"], "enum")
        self.assertEqual(e["symbols"], ["A", "B", "C", "D"])
        self.assertEqual(e["ordinals"], {"A": 5, "B": 6, "C": 10, "D": 11})

    def test_enum_symbol_dedup_after_sanitization(self):
        # "alpha.one" and "alpha_one" both sanitize to alpha_one; the second is suffixed.
        idl = f"namespace * {NS}\nenum E {{ alpha.one, alpha_one }}"
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(by_name["E"]["symbols"], ["alpha_one", "alpha_one_1"])

    def test_typedef_chain_is_resolved(self):
        idl = f"""
        namespace * {NS}
        typedef i64 UserId
        typedef UserId AccountId
        struct S {{ 1: required AccountId aid; }}
        """
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(self.field(by_name, "S", "aid")["type"], "long")

    def test_union_fields_are_nullable_with_null_default(self):
        idl = f"""
        namespace * {NS}
        union U {{ 1: string a; 2: i32 b; }}
        """
        _, by_name = self.thrift_to_avro(idl)
        u = by_name["U"]
        self.assertEqual(u["type"], "record")
        self.assertIn("union", u["doc"])
        self.assertEqual(self.field(by_name, "U", "a")["type"], ["null", "string"])
        self.assertEqual(self.field(by_name, "U", "a")["default"], None)
        self.assertEqual(self.field(by_name, "U", "b")["type"], ["null", "int"])

    def test_exception_becomes_record_with_doc(self):
        idl = f"namespace * {NS}\nexception Boom {{ 1: required string message; }}"
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(by_name["Boom"]["type"], "record")
        self.assertIn("exception", by_name["Boom"]["doc"])

    def test_optional_field_becomes_nullable_union(self):
        idl = f"namespace * {NS}\nstruct S {{ 1: optional string name; }}"
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(self.field(by_name, "S", "name")["type"], ["null", "string"])
        self.assertEqual(self.field(by_name, "S", "name")["default"], None)

    def test_default_values_preserved_for_required_fields(self):
        idl = f"""
        namespace * {NS}
        struct S {{
          1: required bool flag = true;
          2: required i32 count = 7;
          3: required string label = "hi";
        }}
        """
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(self.field(by_name, "S", "flag")["default"], True)
        self.assertEqual(self.field(by_name, "S", "count")["default"], 7)
        self.assertEqual(self.field(by_name, "S", "label")["default"], "hi")

    def test_service_and_const_are_skipped(self):
        idl = f"""
        namespace * {NS}
        const i32 BUILD = 42
        struct S {{ 1: required i32 x; }}
        service Svc {{ void ping(); }}
        """
        _, by_name = self.thrift_to_avro(idl)
        self.assertIn("S", by_name)
        self.assertNotIn("Svc", by_name)
        self.assertNotIn("BUILD", by_name)

    # -- recursion -------------------------------------------------------------

    def test_self_recursive_struct(self):
        idl = f"""
        namespace * {NS}
        struct Node {{
          1: required i32 value;
          2: optional Node next;
        }}
        """
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(self.field(by_name, "Node", "next")["type"], ["null", f"{NS}.Node"])

    def test_mutually_recursive_structs(self):
        idl = f"""
        namespace * {NS}
        struct A {{ 1: optional B b; }}
        struct B {{ 1: optional A a; }}
        """
        schema, by_name = self.thrift_to_avro(idl)
        # A<->B cycle is broken by inlining B into A.b; B then refers to A by name.
        self.assertIn("A", by_name)
        inlined_b = next(t for t in self.field(by_name, "A", "b")["type"] if isinstance(t, dict))
        self.assertEqual(inlined_b["name"], "B")
        self.assertEqual(inlined_b["fields"][0]["type"], ["null", f"{NS}.A"])

    # -- Avro -> Thrift: full primitive range ----------------------------------

    def test_avro_primitives_map_to_thrift(self):
        schema = {
            "type": "record",
            "name": "Prims",
            "namespace": NS,
            "fields": [
                {"name": "fb", "type": "boolean"},
                {"name": "fi", "type": "int"},
                {"name": "fl", "type": "long"},
                {"name": "ff", "type": "float"},
                {"name": "fd", "type": "double"},
                {"name": "fby", "type": "bytes"},
                {"name": "fs", "type": "string"},
            ],
        }
        text = self.avro_to_thrift(schema)
        self.assertIn("required bool fb;", text)
        self.assertIn("required i32 fi;", text)
        self.assertIn("required i64 fl;", text)
        self.assertIn("required double ff;", text)  # Avro float -> Thrift double (no float in Thrift)
        self.assertIn("required double fd;", text)
        self.assertIn("required binary fby;", text)
        self.assertIn("required string fs;", text)

    def test_avro_array_and_map_map_to_thrift(self):
        schema = {
            "type": "record",
            "name": "C",
            "namespace": NS,
            "fields": [
                {"name": "l", "type": {"type": "array", "items": "int"}},
                {"name": "m", "type": {"type": "map", "values": "string"}},
            ],
        }
        text = self.avro_to_thrift(schema)
        self.assertIn("required list<i32> l;", text)
        self.assertIn("required map<string, string> m;", text)

    def test_avro_nullable_union_becomes_optional(self):
        schema = {
            "type": "record",
            "name": "N",
            "namespace": NS,
            "fields": [{"name": "maybe", "type": ["null", "string"], "default": None}],
        }
        text = self.avro_to_thrift(schema)
        self.assertIn("optional string maybe;", text)

    def test_avro_multi_type_union_falls_back_to_string(self):
        # Thrift has no untagged unions; a >1 non-null union degrades to string.
        schema = {
            "type": "record",
            "name": "M",
            "namespace": NS,
            "fields": [{"name": "either", "type": ["string", "int"]}],
        }
        text = self.avro_to_thrift(schema)
        self.assertIn("required string either;", text)

    def test_avro_enum_renders_with_ordinals(self):
        schema = [
            {"type": "enum", "name": "Color", "namespace": NS, "symbols": ["RED", "GREEN"], "ordinals": {"RED": 3, "GREEN": 4}},
        ]
        text = self.avro_to_thrift(schema)
        self.assertIn("enum Color {", text)
        self.assertIn("RED = 3;", text)
        self.assertIn("GREEN = 4;", text)

    # -- lossy round-trip fidelity (behavior is intentionally pinned) ----------

    def test_round_trip_collapses_narrow_ints_and_sets(self):
        idl = f"""
        namespace * {NS}
        struct S {{
          1: required i8 a;
          2: required i16 b;
          3: required byte c;
          4: required set<string> s;
        }}
        """
        schema, _ = self.thrift_to_avro(idl)
        text = self.avro_to_thrift(schema)
        # i8/i16/byte all collapse to Avro int -> Thrift i32; set collapses to list.
        self.assertIn("required i32 a;", text)
        self.assertIn("required i32 b;", text)
        self.assertIn("required i32 c;", text)
        self.assertIn("required list<string> s;", text)

    # -- adversarial / error paths ---------------------------------------------

    def test_missing_thrift_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            convert_thrift_to_avro(str(self.tmp / "nope.thrift"), str(self.tmp / "o.avsc"))

    def test_missing_avro_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            convert_avro_to_thrift(str(self.tmp / "nope.avsc"), str(self.tmp / "o.thrift"))

    def test_unterminated_struct_raises_valueerror(self):
        idl = f"namespace * {NS}\nstruct S {{ 1: required i32 x;"
        src = self.tmp / "bad.thrift"
        src.write_text(idl, encoding="utf-8")
        with self.assertRaises(ValueError):
            convert_thrift_to_avro(str(src), str(self.tmp / "o.avsc"))

    def test_struct_without_name_raises_valueerror(self):
        idl = f"namespace * {NS}\nstruct {{ 1: required i32 x; }}"
        src = self.tmp / "bad.thrift"
        src.write_text(idl, encoding="utf-8")
        with self.assertRaises(ValueError):
            convert_thrift_to_avro(str(src), str(self.tmp / "o.avsc"))

    def test_empty_document_produces_empty_schema(self):
        src = self.tmp / "empty.thrift"
        src.write_text("", encoding="utf-8")
        out = self.tmp / "empty.avsc"
        convert_thrift_to_avro(str(src), str(out), namespace=NS)
        self.assertEqual(json.loads(out.read_text(encoding="utf-8")), [])

    # -- unicode ---------------------------------------------------------------

    def test_unicode_string_default_round_trips(self):
        idl = f'namespace * {NS}\nstruct S {{ 1: required string label = "caf\u00e9 \u2764"; }}'
        _, by_name = self.thrift_to_avro(idl)
        self.assertEqual(self.field(by_name, "S", "label")["default"], "caf\u00e9 \u2764")


if __name__ == "__main__":
    unittest.main()
