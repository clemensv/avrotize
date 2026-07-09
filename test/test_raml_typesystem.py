"""Full type-system range and adversarial coverage for RAML 1.0 data types.

This suite enumerates the mapping branches in avrotize/ramltoavro.py,
avrotize/avrotoraml.py, avrotize/ramltojstruct.py, and
avrotize/jstructtoraml.py. It complements the happy-path RAML tests.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import yaml
from fastavro.schema import parse_schema

from avrotize.avrotoraml import convert_avro_to_raml
from avrotize.jstructtoraml import convert_json_structure_to_raml
from avrotize.ramltoavro import convert_raml_to_avro
from avrotize.ramltojstruct import convert_raml_to_json_structure


ROOT = Path(__file__).resolve().parents[1]
NS = "example.raml"


class RamlTypeSystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    # -- helpers ---------------------------------------------------------------

    def raml_to_avro(self, text: str):
        src = self.tmp / "schema.raml"
        src.write_text(text, encoding="utf-8")
        out = self.tmp / "schema.avsc"
        convert_raml_to_avro(str(src), str(out), namespace=NS)
        schema = json.loads(out.read_text(encoding="utf-8"))
        parse_schema(schema)
        return schema, self.by_name(schema), out

    def avro_to_raml(self, schema) -> tuple[str, dict]:
        parse_schema(schema)
        src = self.tmp / "schema.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "schema.raml"
        convert_avro_to_raml(str(src), str(out), namespace=NS)
        text = out.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("#%RAML 1.0 Library\n"))
        parsed = yaml.safe_load("\n".join(text.splitlines()[1:]))
        return text, parsed

    def raml_to_structure(self, text: str) -> dict:
        src = self.tmp / "schema.raml"
        src.write_text(text, encoding="utf-8")
        out = self.tmp / "schema.struct.json"
        convert_raml_to_json_structure(str(src), str(out), namespace=NS)
        return json.loads(out.read_text(encoding="utf-8"))

    def structure_to_raml(self, structure: dict) -> tuple[str, dict]:
        src = self.tmp / "schema.struct.json"
        src.write_text(json.dumps(structure), encoding="utf-8")
        out = self.tmp / "from-structure.raml"
        convert_json_structure_to_raml(str(src), str(out), namespace=NS)
        text = out.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("#%RAML 1.0 Library\n"))
        parsed = yaml.safe_load("\n".join(text.splitlines()[1:]))
        return text, parsed

    def by_name(self, schema):
        items = schema if isinstance(schema, list) else [schema]
        return {item["name"]: item for item in items if isinstance(item, dict) and "name" in item}

    def field(self, record: dict, name: str) -> dict:
        return next(field for field in record["fields"] if field["name"] == name)

    # -- RAML -> Avro: full scalar range ---------------------------------------

    def test_every_raml_scalar_maps_to_expected_avro(self):
        schema, by_name, _ = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  Scalars:
    type: object
    properties:
      s: string
      b: boolean
      i: integer
      n: number
      nilv: nil
      filev: file
      datev: date-only
      timev: time-only
      datetimeOnlyv: datetime-only
      datetimev: datetime
      anyv: any
"""
        )
        expected = {
            "s": "string",
            "b": "boolean",
            "i": "long",
            "n": "double",
            "nilv": "null",
            "filev": "bytes",
            "datev": {"type": "int", "logicalType": "date"},
            "timev": {"type": "int", "logicalType": "time-millis"},
            "datetimeOnlyv": {"type": "long", "logicalType": "timestamp-millis"},
            "datetimev": {"type": "long", "logicalType": "timestamp-millis"},
            "anyv": "string",
        }
        self.assertEqual(set(by_name), {"Scalars"})
        for field_name, avro_type in expected.items():
            with self.subTest(field=field_name):
                self.assertEqual(self.field(by_name["Scalars"], field_name)["type"], avro_type)
        parse_schema(schema)

    def test_named_scalar_aliases_wrap_in_value_record(self):
        schema, by_name, _ = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  Name: string
  Count:
    type: integer
  Moment:
    type: datetime
"""
        )
        self.assertEqual(self.field(by_name["Name"], "value")["type"], "string")
        self.assertEqual(self.field(by_name["Count"], "value")["type"], "long")
        self.assertEqual(
            self.field(by_name["Moment"], "value")["type"],
            {"type": "long", "logicalType": "timestamp-millis"},
        )
        parse_schema(schema)

    # -- Avro -> RAML: full primitive/logical range ----------------------------

    def test_every_avro_primitive_and_logical_type_maps_to_expected_raml(self):
        _, parsed = self.avro_to_raml(
            {
                "type": "record",
                "name": "Scalars",
                "namespace": NS,
                "fields": [
                    {"name": "nullv", "type": "null"},
                    {"name": "boolv", "type": "boolean"},
                    {"name": "intv", "type": "int"},
                    {"name": "longv", "type": "long"},
                    {"name": "floatv", "type": "float"},
                    {"name": "doublev", "type": "double"},
                    {"name": "bytesv", "type": "bytes"},
                    {"name": "stringv", "type": "string"},
                    {"name": "datev", "type": {"type": "int", "logicalType": "date"}},
                    {"name": "timeMillisv", "type": {"type": "int", "logicalType": "time-millis"}},
                    {"name": "timeMicrosv", "type": {"type": "long", "logicalType": "time-micros"}},
                    {"name": "timestampMillisv", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                    {"name": "timestampMicrosv", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                ],
            }
        )
        self.assertEqual(
            parsed["types"]["Scalars"]["properties"],
            {
                "nullv": "nil",
                "boolv": "boolean",
                "intv": "integer",
                "longv": "integer",
                "floatv": "number",
                "doublev": "number",
                "bytesv": "file",
                "stringv": "string",
                "datev": "date-only",
                "timeMillisv": "time-only",
                "timeMicrosv": "time-only",
                "timestampMillisv": "datetime",
                "timestampMicrosv": "datetime",
            },
        )

    # -- containers and type constructs ----------------------------------------

    def test_raml_containers_map_to_exact_avro_shapes(self):
        _, by_name, _ = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  Containers:
    type: object
    properties:
      shorthand: string[]
      explicit:
        type: array
        items: integer
      nestedArrays: string[][]
      additionalMap:
        type: object
        additionalProperties: number
      slashMap:
        type: object
        properties:
          //: boolean
      child:
        type: object
        properties:
          value: number
"""
        )
        rec = by_name["Containers"]
        self.assertEqual(self.field(rec, "shorthand")["type"], {"type": "array", "items": "string"})
        self.assertEqual(self.field(rec, "explicit")["type"], {"type": "array", "items": "long"})
        self.assertEqual(
            self.field(rec, "nestedArrays")["type"],
            {"type": "array", "items": {"type": "array", "items": "string"}},
        )
        self.assertEqual(self.field(rec, "additionalMap")["type"], {"type": "map", "values": "double"})
        self.assertEqual(self.field(rec, "slashMap")["type"], {"type": "map", "values": "boolean"})
        self.assertEqual(
            self.field(rec, "child")["type"],
            {
                "type": "record",
                "name": "Containers_child",
                "fields": [{"name": "value", "type": "double"}],
                "namespace": NS,
            },
        )

    def test_raml_object_optional_union_enum_reference_and_doc_constructs(self):
        _, by_name, _ = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  Address:
    type: object
    properties:
      street: string
  Status:
    type: string
    description: Status doc
    enum: [new, active, suspended]
  Person:
    type: object
    description: Person record
    properties:
      id:
        type: integer
        description: Identifier
      nickname?: string
      active:
        type: boolean
        required: false
      choice: string | integer
      address: Address
      status: Status
"""
        )
        self.assertEqual(by_name["Status"]["type"], "enum")
        self.assertEqual(by_name["Status"]["symbols"], ["new", "active", "suspended"])
        self.assertEqual(by_name["Status"]["doc"], "Status doc")
        person = by_name["Person"]
        self.assertEqual(person["doc"], "Person record")
        self.assertEqual(self.field(person, "id")["doc"], "Identifier")
        self.assertEqual(self.field(person, "nickname")["type"], ["null", "string"])
        self.assertIsNone(self.field(person, "nickname")["default"])
        self.assertEqual(self.field(person, "active")["type"], ["null", "boolean"])
        self.assertIsNone(self.field(person, "active")["default"])
        self.assertEqual(self.field(person, "choice")["type"], ["string", "long"])
        self.assertEqual(self.field(person, "address")["type"], f"{NS}.Address")
        self.assertEqual(self.field(person, "status")["type"], f"{NS}.Status")

    def test_avro_containers_and_constructs_map_to_expected_raml(self):
        _, parsed = self.avro_to_raml(
            [
                {"type": "record", "name": "Address", "namespace": NS, "fields": [{"name": "street", "type": "string"}]},
                {"type": "enum", "name": "Status", "namespace": NS, "symbols": ["new", "active"], "doc": "Status doc"},
                {
                    "type": "record",
                    "name": "Person",
                    "namespace": NS,
                    "doc": "Person doc",
                    "fields": [
                        {"name": "id", "type": "long", "doc": "Identifier"},
                        {"name": "tags", "type": {"type": "array", "items": "string"}},
                        {"name": "meta", "type": {"type": "map", "values": "string"}},
                        {
                            "name": "child",
                            "type": {"type": "record", "name": "Child", "fields": [{"name": "x", "type": "double"}]},
                        },
                        {"name": "maybe", "type": ["null", "boolean"], "default": None},
                        {"name": "choice", "type": ["string", "long"]},
                        {"name": "address", "type": f"{NS}.Address"},
                        {"name": "status", "type": f"{NS}.Status"},
                    ],
                },
            ]
        )
        self.assertEqual(parsed["types"]["Status"], {"type": "string", "enum": ["new", "active"], "description": "Status doc"})
        person = parsed["types"]["Person"]
        self.assertEqual(person["description"], "Person doc")
        self.assertEqual(person["properties"]["id"], {"type": "integer", "description": "Identifier"})
        self.assertEqual(person["properties"]["tags"], "string[]")
        self.assertEqual(person["properties"]["meta"], {"type": "object", "properties": {"//": "string"}})
        self.assertEqual(person["properties"]["child"]["properties"], {"x": "number"})
        self.assertEqual(person["properties"]["maybe?"], "boolean")
        self.assertEqual(person["properties"]["choice"], "string | integer")
        self.assertEqual(person["properties"]["address"], "Address")
        self.assertEqual(person["properties"]["status"], "Status")

    # -- recursion -------------------------------------------------------------

    def test_self_recursive_type_is_valid_and_pinned(self):
        schema, by_name, _ = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  Node:
    type: object
    properties:
      value: integer
      next?: Node
"""
        )
        self.assertEqual(self.field(by_name["Node"], "next")["type"], ["null", f"{NS}.Node"])
        self.assertEqual(by_name["Node"]["dependencies"], [f"{NS}.Node"])
        parse_schema(schema)

    def test_mutually_recursive_types_inline_dependency_and_remain_valid(self):
        schema, by_name, _ = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  A:
    type: object
    properties:
      b?: B
  B:
    type: object
    properties:
      a?: A
"""
        )
        self.assertEqual(set(by_name), {"A"})
        inlined_b = next(item for item in self.field(by_name["A"], "b")["type"] if isinstance(item, dict))
        self.assertEqual(inlined_b["name"], "B")
        self.assertEqual(inlined_b["fields"][0]["type"], ["null", f"{NS}.A"])
        parse_schema(schema)

    def test_forward_reference_to_enum_and_array_element_without_namespace_is_ordered(self):
        # Regression: without a namespace the RAML converter previously emitted the
        # top-level Avro types in an order that placed a record before the enum and
        # array-element record it references, so fastavro rejected the output with
        # UnknownType. Dependencies are now re-derived from the actual field
        # references and topologically ordered regardless of namespace.
        src = self.tmp / "nons.raml"
        src.write_text(
            """#%RAML 1.0 Library
types:
  Order:
    type: object
    properties:
      status: Status
      lines: LineItem[]
  Status:
    type: string
    enum: [open, closed]
  LineItem:
    type: object
    properties:
      sku: string
""",
            encoding="utf-8",
        )
        out = self.tmp / "nons.avsc"
        convert_raml_to_avro(str(src), str(out), namespace=None)
        schema = json.loads(out.read_text(encoding="utf-8"))
        parse_schema(schema)
        by_name = self.by_name(schema)
        self.assertEqual(set(by_name), {"Order", "Status", "LineItem"})
        self.assertEqual(self.field(by_name["Order"], "status")["type"], "Status")
        self.assertEqual(
            self.field(by_name["Order"], "lines")["type"],
            {"type": "array", "items": "LineItem"},
        )
        names_in_order = [item["name"] for item in schema]
        self.assertLess(names_in_order.index("Status"), names_in_order.index("Order"))
        self.assertLess(names_in_order.index("LineItem"), names_in_order.index("Order"))

    # -- round-trip fidelity ---------------------------------------------------

    def test_raml_avro_raml_round_trip_emits_parseable_library_and_pins_shapes(self):
        _, _, avro_path = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  Sample:
    type: object
    description: Sample doc
    properties:
      datev: date-only
      timev: time-only
      dtv: datetime
      tags: string[]
      mapv:
        type: object
        additionalProperties: integer
      maybe?: string
"""
        )
        out = self.tmp / "roundtrip.raml"
        convert_avro_to_raml(str(avro_path), str(out), namespace=NS)
        text = out.read_text(encoding="utf-8")
        self.assertTrue(text.startswith("#%RAML 1.0 Library\n"))
        parsed = yaml.safe_load("\n".join(text.splitlines()[1:]))
        props = parsed["types"]["Sample"]["properties"]
        self.assertEqual(props["datev"], "date-only")
        self.assertEqual(props["timev"], "time-only")
        self.assertEqual(props["dtv"], "datetime")
        self.assertEqual(props["tags"], "string[]")
        self.assertEqual(props["mapv"], {"type": "object", "properties": {"//": "integer"}})
        self.assertEqual(props["maybe?"], "string")

    def test_raml_structure_bridge_full_scalar_range_is_pinned(self):
        struct = self.raml_to_structure(
            """#%RAML 1.0 Library
types:
  Scalars:
    type: object
    properties:
      s: string
      b: boolean
      i: integer
      n: number
      nilv: nil
      filev: file
      datev: date-only
      timev: time-only
      datetimeOnlyv: datetime-only
      datetimev: datetime
      arr: string[]
      mapv:
        type: object
        properties:
          //: integer
"""
        )
        props = struct["definitions"]["example"]["raml"]["Scalars"]["properties"]
        self.assertEqual(props["s"], {"type": "string"})
        self.assertEqual(props["b"], {"type": "boolean"})
        self.assertEqual(props["i"], {"type": "int64"})
        self.assertEqual(props["n"], {"type": "double"})
        self.assertEqual(props["nilv"], {"type": "null"})
        self.assertEqual(props["filev"], {"type": "binary"})
        self.assertEqual(props["datev"], {"type": "int32", "logicalType": "date"})
        self.assertEqual(props["timev"], {"type": "string"})
        self.assertEqual(props["datetimeOnlyv"], {"type": "int64", "logicalType": "timestampMillis"})
        self.assertEqual(props["datetimev"], {"type": "int64", "logicalType": "timestampMillis"})
        self.assertEqual(props["arr"], {"type": "array", "items": {"type": "string"}})
        self.assertEqual(props["mapv"], {"type": "map", "values": {"type": "int64"}})

    def test_json_structure_to_raml_full_range_is_pinned(self):
        _, parsed = self.structure_to_raml(
            {
                "$schema": "https://json-structure.org/meta/core/v0/#",
                "$root": "#/definitions/Root",
                "definitions": {
                    "Root": {
                        "type": "object",
                        "properties": {
                            "s": {"type": "string"},
                            "b": {"type": "boolean"},
                            "i8": {"type": "int8"},
                            "i16": {"type": "int16"},
                            "i32": {"type": "int32"},
                            "i64": {"type": "int64"},
                            "u32": {"type": "uint32"},
                            "num": {"type": "number"},
                            "flt": {"type": "float"},
                            "dbl": {"type": "double"},
                            "bin": {"type": "binary"},
                            "nul": {"type": "null"},
                            "date": {"type": "date"},
                            "time": {"type": "time"},
                            "dt": {"type": "datetime"},
                            "dur": {"type": "duration"},
                            "uuid": {"type": "uuid"},
                            "dec": {"type": "decimal"},
                            "arr": {"type": "array", "items": {"type": "string"}},
                            "map": {"type": "map", "values": {"type": "int32"}},
                        },
                        "required": [
                            "s",
                            "b",
                            "i8",
                            "i16",
                            "i32",
                            "i64",
                            "u32",
                            "num",
                            "flt",
                            "dbl",
                            "bin",
                            "nul",
                            "date",
                            "time",
                            "dt",
                            "dur",
                            "uuid",
                            "dec",
                            "arr",
                            "map",
                        ],
                    }
                },
            }
        )
        self.assertEqual(
            parsed["types"]["Root"]["properties"],
            {
                "s": "string",
                "b": "boolean",
                "i8": "integer",
                "i16": "integer",
                "i32": "integer",
                "i64": "integer",
                "u32": "integer",
                "num": "number",
                "flt": "number",
                "dbl": "number",
                "bin": "file",
                "nul": "nil",
                "date": "string",
                "time": "string",
                "dt": "string",
                "dur": "string",
                "uuid": "string",
                "dec": "string",
                "arr": "string[]",
                "map": {"type": "object", "properties": {"//": "integer"}},
            },
        )

    def test_raml_structure_raml_round_trip_pins_temporal_loss(self):
        struct = self.raml_to_structure(
            """#%RAML 1.0 Library
types:
  Temporal:
    type: object
    properties:
      datev: date-only
      timev: time-only
      dtv: datetime
"""
        )
        _, parsed = self.structure_to_raml(struct)
        self.assertEqual(
            parsed["types"]["Temporal"]["properties"],
            {"datev": "string", "timev": "string", "dtv": "string"},
        )

    # -- adversarial / error paths --------------------------------------------

    def test_missing_raml_file_raises_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            convert_raml_to_avro(str(self.tmp / "missing.raml"), str(self.tmp / "out.avsc"), namespace=NS)

    def test_broken_yaml_raises_yaml_error(self):
        src = self.tmp / "bad.raml"
        src.write_text("#%RAML 1.0 Library\ntypes:\n  Bad: [unterminated\n", encoding="utf-8")
        with self.assertRaises(yaml.YAMLError):
            convert_raml_to_avro(str(src), str(self.tmp / "bad.avsc"), namespace=NS)

    def test_non_mapping_types_section_raises_value_error(self):
        src = self.tmp / "bad-types.raml"
        src.write_text("#%RAML 1.0 Library\ntypes:\n  - Bad\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "types section must be a mapping"):
            convert_raml_to_avro(str(src), str(self.tmp / "bad.avsc"), namespace=NS)

    def test_missing_type_declaration_gracefully_defaults_to_string_value_record(self):
        _, by_name, _ = self.raml_to_avro("#%RAML 1.0 Library\ntypes:\n  Missing:\n")
        self.assertEqual(self.field(by_name["Missing"], "value")["type"], "string")

    def test_empty_and_types_less_documents_gracefully_emit_empty_schema(self):
        for name, text in {
            "empty.raml": "",
            "api.raml": "#%RAML 1.0\ntitle: API\n/items:\n  get:\n    responses: {}\n",
        }.items():
            with self.subTest(name=name):
                src = self.tmp / name
                out = self.tmp / f"{name}.avsc"
                src.write_text(text, encoding="utf-8")
                convert_raml_to_avro(str(src), str(out), namespace=NS)
                schema = json.loads(out.read_text(encoding="utf-8"))
                self.assertEqual(schema, [])
                parse_schema(schema)

    def test_out_of_scope_resource_and_method_keys_inside_properties_do_not_crash(self):
        _, by_name, _ = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  ResourceLike:
    type: object
    properties:
      id: string
      /items:
        get:
          queryParameters:
            q: string
      get:
        responses: {}
      post:
        body: {}
      delete:
        responses: {}
"""
        )
        self.assertEqual([field["name"] for field in by_name["ResourceLike"]["fields"]], ["id"])

    def test_unicode_identifiers_are_sanitized_and_unicode_defaults_survive(self):
        _, by_name, _ = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  Café:
    type: object
    properties:
      naïve-name:
        type: string
        default: café ❤
"""
        )
        self.assertIn("Caf_", by_name)
        field = self.field(by_name["Caf_"], "na_ve_name")
        self.assertEqual(field["type"], "string")
        self.assertEqual(field["default"], "café ❤")

    def test_duplicate_field_names_after_sanitization_are_deduplicated(self):
        _, by_name, _ = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  Weird:
    type: object
    properties:
      a-b: string
      a_b: integer
"""
        )
        self.assertEqual([field["name"] for field in by_name["Weird"]["fields"]], ["a_b", "a_b_1"])

    def test_duplicate_enum_symbols_after_sanitization_are_deduplicated(self):
        self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  E:
    type: string
    enum: [a-b, a_b]
"""
        )

    def test_duplicate_type_names_after_sanitization_are_deduplicated(self):
        schema, _, _ = self.raml_to_avro(
            """#%RAML 1.0 Library
types:
  A-B:
    type: object
    properties:
      x: string
  A_B:
    type: object
    properties:
      y: string
"""
        )
        self.assertEqual([item["name"] for item in schema], ["A_B", "A_B_1"])


if __name__ == "__main__":
    unittest.main()
