"""Full type-system range and adversarial coverage for Smithy converters.

This suite enumerates the Smithy <-> Avrotize Schema (Avro) mapping branches in
avrotize/smithytoavro.py, avrotize/avrotosmithy.py, and the JSON Structure
bridge wrappers. It complements the happy-path fixture tests in
test_smithytoavro.py.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fastavro.schema import parse_schema

from avrotize.avrotosmithy import convert_avro_to_smithy
from avrotize.jstructtosmithy import convert_json_structure_to_smithy
from avrotize.smithytoavro import convert_smithy_to_avro
from avrotize.smithytojstruct import convert_smithy_to_json_structure


ROOT = Path(__file__).resolve().parents[1]
NS = "com.example.smithytypes"


class SmithyTypeSystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    # -- helpers ---------------------------------------------------------------

    def assert_valid_avro(self, schema) -> None:
        if schema:
            parse_schema(json.loads(json.dumps(schema)))

    def smithy_to_avro(self, idl: str, namespace: str | None = None):
        src = self.tmp / "schema.smithy"
        src.write_text(idl, encoding="utf-8")
        out = self.tmp / "schema.avsc"
        convert_smithy_to_avro(str(src), str(out), namespace=namespace)
        schema = json.loads(out.read_text(encoding="utf-8"))
        self.assert_valid_avro(schema)
        by_name = {s["name"]: s for s in schema if isinstance(s, dict) and "name" in s}
        return schema, by_name

    def avro_to_smithy(self, schema, namespace: str | None = None) -> str:
        self.assert_valid_avro(schema)
        src = self.tmp / "schema.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "schema.smithy"
        convert_avro_to_smithy(str(src), str(out), namespace=namespace)
        return out.read_text(encoding="utf-8")

    def smithy_to_structure(self, idl: str):
        src = self.tmp / "schema.smithy"
        src.write_text(idl, encoding="utf-8")
        out = self.tmp / "schema.struct.json"
        convert_smithy_to_json_structure(str(src), str(out))
        return json.loads(out.read_text(encoding="utf-8"))

    def structure_to_smithy(self, structure, namespace: str = NS) -> str:
        src = self.tmp / "schema.struct.json"
        src.write_text(json.dumps(structure), encoding="utf-8")
        out = self.tmp / "schema.fromstruct.smithy"
        convert_json_structure_to_smithy(str(src), str(out), namespace=namespace)
        return out.read_text(encoding="utf-8")

    def field(self, by_name, record: str, name: str):
        return next(f for f in by_name[record]["fields"] if f["name"] == name)

    def definition(self, structure, *path: str):
        node = structure["definitions"]
        for part in path:
            node = node[part]
        return node

    # -- Smithy -> Avro: full simple-shape range ------------------------------

    def test_every_smithy_simple_shape_maps_to_exact_avro(self):
        idl = f"""
        namespace {NS}
        structure Scalars {{
            @required blobValue: Blob
            @required booleanValue: Boolean
            @required stringValue: String
            @required byteValue: Byte
            @required shortValue: Short
            @required integerValue: Integer
            @required longValue: Long
            @required floatValue: Float
            @required doubleValue: Double
            @required bigIntegerValue: BigInteger
            @required bigDecimalValue: BigDecimal
            @required timestampValue: Timestamp
            @required documentValue: Document
        }}
        """
        _, by_name = self.smithy_to_avro(idl)
        expected = {
            "blobValue": "bytes",
            "booleanValue": "boolean",
            "stringValue": "string",
            "byteValue": "int",
            "shortValue": "int",
            "integerValue": "int",
            "longValue": "long",
            "floatValue": "float",
            "doubleValue": "double",
            "bigIntegerValue": "string",
            "bigDecimalValue": "double",
            "timestampValue": {"type": "long", "logicalType": "timestamp-millis"},
            "documentValue": "string",
        }
        for name, avro_type in expected.items():
            with self.subTest(field=name):
                self.assertEqual(self.field(by_name, "Scalars", name)["type"], avro_type)

    def test_required_optional_nullable_and_default_members(self):
        idl = f"""
        namespace {NS}
        structure Defaults {{
            @required id: String
            nickname: String
            @default("guest") displayName: String
            @default(7) count: Integer
            @default(null) maybe: String
        }}
        """
        _, by_name = self.smithy_to_avro(idl)
        self.assertEqual(self.field(by_name, "Defaults", "id")["type"], "string")
        self.assertNotIn("default", self.field(by_name, "Defaults", "id"))
        self.assertEqual(self.field(by_name, "Defaults", "nickname")["type"], ["null", "string"])
        self.assertIsNone(self.field(by_name, "Defaults", "nickname")["default"])
        self.assertEqual(self.field(by_name, "Defaults", "displayName")["type"], ["string", "null"])
        self.assertEqual(self.field(by_name, "Defaults", "displayName")["default"], "guest")
        self.assertEqual(self.field(by_name, "Defaults", "count")["type"], ["int", "null"])
        self.assertEqual(self.field(by_name, "Defaults", "count")["default"], 7)
        self.assertEqual(self.field(by_name, "Defaults", "maybe")["type"], ["null", "string"])
        self.assertIsNone(self.field(by_name, "Defaults", "maybe")["default"])

    def test_documentation_deprecated_and_tags_traits_map_to_doc(self):
        idl = f'''
        namespace {NS}
        @documentation("Shape doc")
        @deprecated
        @tags(["alpha", "beta"])
        structure Documented {{
            @documentation("Field doc")
            @deprecated
            name: String
        }}
        '''
        _, by_name = self.smithy_to_avro(idl)
        self.assertEqual(by_name["Documented"]["doc"], 'Shape doc Deprecated. Tags: [ "alpha" , "beta" ]')
        self.assertEqual(self.field(by_name, "Documented", "name")["doc"], "Field doc Deprecated.")

    # -- Smithy -> Avro: aggregate and named shapes ----------------------------

    def test_list_and_map_wrappers_and_member_references(self):
        idl = f"""
        namespace {NS}
        @documentation("Names doc")
        list Names {{
            @documentation("one name")
            member: String
        }}
        map Scores {{
            key: String
            value: Integer
        }}
        structure Holder {{
            names: Names
            scores: Scores
        }}
        """
        _, by_name = self.smithy_to_avro(idl)
        self.assertEqual(by_name["Names"]["smithyShape"], "list")
        self.assertEqual(by_name["Names"]["doc"], "Names doc")
        self.assertEqual(by_name["Names"]["fields"][0]["doc"], "one name")
        self.assertEqual(by_name["Names"]["fields"][0]["type"], {"type": "array", "items": "string"})
        self.assertEqual(by_name["Scores"]["smithyShape"], "map")
        self.assertEqual(by_name["Scores"]["fields"][0]["type"], {"type": "map", "values": "int"})
        self.assertEqual(self.field(by_name, "Holder", "names")["type"], ["null", {"type": "array", "items": "string"}])
        self.assertEqual(self.field(by_name, "Holder", "scores")["type"], ["null", {"type": "map", "values": "int"}])

    def test_map_with_non_string_key_is_pinned_as_avro_map_with_doc_warning(self):
        idl = f"""
        namespace {NS}
        map BadKeyed {{
            key: Integer
            value: String
        }}
        """
        _, by_name = self.smithy_to_avro(idl)
        self.assertEqual(by_name["BadKeyed"]["fields"][0]["type"], {"type": "map", "values": "string"})
        self.assertIn("original key type was Integer", by_name["BadKeyed"]["doc"])

    @unittest.expectedFailure
    def test_set_shape_maps_to_avro_array(self):
        # BUG: SmithyIdlParser does not recognize `set` shapes, so they are skipped.
        idl = f"namespace {NS}\nset UniqueNames {{ member: String }}"
        _, by_name = self.smithy_to_avro(idl)
        self.assertEqual(by_name["UniqueNames"]["smithyShape"], "set")
        self.assertEqual(by_name["UniqueNames"]["fields"][0]["type"], {"type": "array", "items": "string"})

    def test_nested_structures_and_named_member_references(self):
        idl = f"""
        namespace {NS}
        structure Address {{
            @required city: String
        }}
        structure Person {{
            @required address: Address
            previous: Address
        }}
        """
        _, by_name = self.smithy_to_avro(idl)
        self.assertEqual(self.field(by_name, "Address", "city")["type"], "string")
        self.assertEqual(self.field(by_name, "Person", "address")["type"], "Address")
        self.assertEqual(self.field(by_name, "Person", "previous")["type"], ["null", "Address"])

    def test_union_enum_and_int_enum_preserve_exact_shape_metadata(self):
        idl = f"""
        namespace {NS}
        union Payload {{
            text: String
            count: Integer
        }}
        enum Color {{
            RED
            GREEN
        }}
        intEnum Status {{
            OK = 0
            ERROR = 9
            UNKNOWN
        }}
        """
        _, by_name = self.smithy_to_avro(idl)
        self.assertEqual(by_name["Payload"]["smithyShape"], "union")
        self.assertEqual([f["name"] for f in by_name["Payload"]["fields"]], ["text", "count"])
        self.assertEqual(self.field(by_name, "Payload", "text")["type"], ["null", "string"])
        self.assertEqual(by_name["Color"]["symbols"], ["RED", "GREEN"])
        self.assertNotIn("ordinals", by_name["Color"])
        self.assertEqual(by_name["Status"]["symbols"], ["OK", "ERROR", "UNKNOWN"])
        self.assertEqual(by_name["Status"]["ordinals"], {"OK": 0, "ERROR": 9, "UNKNOWN": 2})
        self.assertEqual(by_name["Status"]["smithyShape"], "intEnum")

    # -- recursion -------------------------------------------------------------

    def test_self_recursive_structure_is_valid_avro(self):
        idl = f"""
        namespace {NS}
        structure Node {{
            @required value: Integer
            next: Node
        }}
        """
        _, by_name = self.smithy_to_avro(idl)
        self.assertEqual(self.field(by_name, "Node", "next")["type"], ["null", "Node"])

    def test_mutually_recursive_structures_are_valid_avro(self):
        idl = f"""
        namespace {NS}
        structure A {{ b: B }}
        structure B {{ a: A }}
        """
        self.smithy_to_avro(idl)

    # -- Avro -> Smithy: full primitive and aggregate range --------------------

    def test_every_avro_primitive_maps_to_expected_smithy_member_type(self):
        schema = {
            "type": "record",
            "name": "AvroScalars",
            "namespace": NS,
            "fields": [
                {"name": "nullValue", "type": "null"},
                {"name": "booleanValue", "type": "boolean"},
                {"name": "bytesValue", "type": "bytes"},
                {"name": "stringValue", "type": "string"},
                {"name": "intValue", "type": "int"},
                {"name": "longValue", "type": "long"},
                {"name": "floatValue", "type": "float"},
                {"name": "doubleValue", "type": "double"},
                {"name": "timestampValue", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "decimalValue", "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
            ],
        }
        text = self.avro_to_smithy(schema)
        expected = {
            "nullValue": "Document",
            "booleanValue": "Boolean",
            "bytesValue": "Blob",
            "stringValue": "String",
            "intValue": "Integer",
            "longValue": "Long",
            "floatValue": "Float",
            "doubleValue": "Double",
            "timestampValue": "Timestamp",
            "decimalValue": "Blob",
        }
        for name, smithy_type in expected.items():
            with self.subTest(field=name):
                self.assertIn(f"{name}: {smithy_type}", text)
        self.assertNotIn("precision", text)

    def test_avro_record_required_nullable_default_and_doc_to_smithy_traits(self):
        schema = {
            "type": "record",
            "name": "Traits",
            "namespace": NS,
            "doc": "Shape doc",
            "fields": [
                {"name": "id", "type": "string", "doc": "ID doc"},
                {"name": "nickname", "type": ["null", "string"], "default": None},
                {"name": "displayName", "type": "string", "default": "guest"},
                {"name": "count", "type": "int", "default": 7},
            ],
        }
        text = self.avro_to_smithy(schema)
        self.assertIn('@documentation("Shape doc")\nstructure Traits', text)
        self.assertIn('@documentation("ID doc")\n    @required\n    id: String', text)
        self.assertIn("    nickname: String", text)
        self.assertNotIn("@required\n    nickname", text)
        self.assertIn("@default(\"guest\")\n    displayName: String", text)
        self.assertIn("@default(7)\n    count: Integer", text)

    def test_avro_enum_int_enum_list_map_union_and_nested_record_to_smithy(self):
        schema = [
            {"type": "enum", "name": "Color", "namespace": NS, "symbols": ["RED", "GREEN"]},
            {"type": "enum", "name": "Status", "namespace": NS, "symbols": ["OK", "ERROR"], "ordinals": {"OK": 0, "ERROR": 9}},
            {"type": "record", "name": "Names", "namespace": NS, "smithyShape": "list", "fields": [{"name": "member", "type": {"type": "array", "items": "string"}}]},
            {"type": "record", "name": "Tags", "namespace": NS, "smithyShape": "map", "fields": [{"name": "entries", "type": {"type": "map", "values": "int"}}]},
            {"type": "record", "name": "Payload", "namespace": NS, "smithyShape": "union", "fields": [{"name": "text", "type": ["null", "string"]}, {"name": "count", "type": ["null", "int"]}]},
            {"type": "record", "name": "Container", "namespace": NS, "fields": [
                {"name": "names", "type": {"type": "array", "items": "string"}},
                {"name": "tags", "type": {"type": "map", "values": "int"}},
                {"name": "choice", "type": ["string", "int"]},
                {"name": "nested", "type": {"type": "record", "name": "Nested", "fields": [{"name": "ok", "type": "boolean"}]}},
            ]},
        ]
        text = self.avro_to_smithy(schema)
        self.assertIn("enum Color", text)
        self.assertIn("intEnum Status", text)
        self.assertIn("OK = 0", text)
        self.assertIn("ERROR = 9", text)
        self.assertIn("list Names", text)
        self.assertIn("map Tags", text)
        self.assertIn("union Payload", text)
        self.assertIn("names: Names", text)
        self.assertIn("tags: Tags", text)
        self.assertIn("union ContainerChoiceUnion", text)
        self.assertIn("structure Nested", text)

    # -- round-trip fidelity ---------------------------------------------------

    def test_smithy_avro_smithy_round_trip_pins_lossy_simple_shapes(self):
        idl = f"""
        namespace {NS}
        structure Lossy {{
            @required byteValue: Byte
            @required shortValue: Short
            @required integerValue: Integer
            @required bigIntegerValue: BigInteger
            @required bigDecimalValue: BigDecimal
            @required documentValue: Document
            @required timestampValue: Timestamp
        }}
        """
        schema, _ = self.smithy_to_avro(idl)
        text = self.avro_to_smithy(schema)
        self.assertIn("byteValue: Integer", text)
        self.assertIn("shortValue: Integer", text)
        self.assertIn("integerValue: Integer", text)
        self.assertIn("bigIntegerValue: String", text)
        self.assertIn("bigDecimalValue: Double", text)
        self.assertIn("documentValue: String", text)
        self.assertIn("timestampValue: Timestamp", text)
        reparsed, by_name = self.smithy_to_avro(text)
        self.assert_valid_avro(reparsed)
        self.assertEqual(self.field(by_name, "Lossy", "bigDecimalValue")["type"], "double")

    def test_json_structure_bridge_pins_lossy_round_trip(self):
        idl = f"""
        namespace {NS}
        @documentation("Bridge doc")
        structure Bridge {{
            @required flag: Boolean
            count: Integer
            amount: BigDecimal
            when: Timestamp
            blob: Blob
            doc: Document
        }}
        """
        structure = self.smithy_to_structure(idl)
        bridge = self.definition(structure, "com", "example", "smithytypes", "Bridge")
        self.assertEqual(bridge["description"], "Bridge doc")
        self.assertEqual(bridge["required"], ["flag"])
        self.assertEqual(bridge["properties"]["flag"], {"type": "boolean"})
        self.assertEqual(bridge["properties"]["count"], {"type": "int32"})
        self.assertEqual(bridge["properties"]["amount"], {"type": "double"})
        self.assertEqual(bridge["properties"]["when"], {"type": "int64", "logicalType": "timestampMillis"})
        self.assertEqual(bridge["properties"]["blob"], {"type": "binary"})
        self.assertEqual(bridge["properties"]["doc"], {"type": "string"})

        text = self.structure_to_smithy(structure)
        self.assertIn("namespace " + NS, text)
        self.assertIn("@documentation(\"Bridge doc\")", text)
        self.assertIn("flag: Boolean", text)
        self.assertIn("count: Integer", text)
        self.assertIn("amount: Double", text)
        self.assertIn("when: String", text)
        self.assertIn("blob: Blob", text)
        self.assertIn("doc: String", text)
        self.assertIn("@required\n    flag: Boolean", text)  # genuinely-required member stays required
        self.assertNotIn("@required\n    count: Integer", text)  # optionality now preserved through the bridge (was lost when default:null decorated optional fields)

    # -- JSON Structure bridge: full JSON Structure type range -----------------

    def test_json_structure_full_type_range_to_smithy(self):
        structure = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/schemas/Bridge",
            "name": "Bridge",
            "$root": "#/definitions/com/example/Bridge",
            "definitions": {
                "com": {
                    "example": {
                        "Bridge": {
                            "name": "Bridge",
                            "type": "object",
                            "properties": {
                                "nullValue": {"type": "null"},
                                "booleanValue": {"type": "boolean"},
                                "int8Value": {"type": "int8"},
                                "int16Value": {"type": "int16"},
                                "int32Value": {"type": "int32"},
                                "int64Value": {"type": "int64"},
                                "uint32Value": {"type": "uint32"},
                                "float32Value": {"type": "float32"},
                                "float64Value": {"type": "float64"},
                                "numberValue": {"type": "number"},
                                "stringValue": {"type": "string"},
                                "binaryValue": {"type": "binary"},
                                "dateValue": {"type": "date"},
                                "datetimeValue": {"type": "datetime"},
                                "timeValue": {"type": "time"},
                                "durationValue": {"type": "duration"},
                                "uuidValue": {"type": "uuid"},
                                "decimalValue": {"type": "decimal"},
                                "arrayValue": {"type": "array", "items": "string"},
                                "setValue": {"type": "set", "items": "string"},
                                "mapValue": {"type": "map", "values": "int32"},
                                "anyValue": {"type": "any"},
                                "choiceValue": {"type": "choice", "choices": {"text": "string", "count": "int32"}},
                            },
                            "required": [
                                "nullValue", "booleanValue", "int8Value", "int16Value", "int32Value", "int64Value",
                                "uint32Value", "float32Value", "float64Value", "numberValue", "stringValue",
                                "binaryValue", "dateValue", "datetimeValue", "timeValue", "durationValue",
                                "uuidValue", "decimalValue", "arrayValue", "setValue", "mapValue", "anyValue",
                                "choiceValue",
                            ],
                        }
                    }
                }
            },
        }
        text = self.structure_to_smithy(structure)
        expected_members = {
            "nullValue": "Document",
            "booleanValue": "Boolean",
            "int8Value": "Integer",
            "int16Value": "Integer",
            "int32Value": "Integer",
            "int64Value": "Long",
            "uint32Value": "Long",
            "float32Value": "Float",
            "float64Value": "Double",
            "numberValue": "Double",
            "stringValue": "String",
            "binaryValue": "Blob",
            "dateValue": "String",
            "datetimeValue": "String",
            "timeValue": "String",
            "durationValue": "String",
            "uuidValue": "String",
            "decimalValue": "String",
            "arrayValue": "BridgeArrayValueList",
            "setValue": "BridgeSetValueList",
            "mapValue": "BridgeMapValueMap",
            "anyValue": "BridgeAnyValueUnion",
            "choiceValue": "Choice_text_count",
        }
        for member, smithy_type in expected_members.items():
            with self.subTest(member=member):
                self.assertIn(f"{member}: {smithy_type}", text)
        self.assertIn("list BridgeArrayValueList", text)
        self.assertIn("list BridgeSetValueList", text)
        self.assertIn("map BridgeMapValueMap", text)
        self.assertIn("union BridgeAnyValueUnion", text)
        self.assertIn("structure Choice_text_count", text)

    # -- out-of-scope model constructs ----------------------------------------

    def test_service_operation_resource_metadata_apply_do_not_drop_adjacent_shapes(self):
        idl = f"""
        $version: "2.0"
        namespace {NS}
        metadata authors = ["team"]
        structure Before {{ @required id: String }}
        service ExampleService {{ operations: [GetBefore] }}
        operation GetBefore {{ input: Before, output: After }}
        resource Thing {{ identifiers: {{ thingId: String }}, read: GetBefore }}
        apply Before @deprecated
        structure After {{ @required ok: Boolean }}
        """
        schema, by_name = self.smithy_to_avro(idl)
        self.assertEqual([s["name"] for s in schema], ["Before", "After"])
        self.assertEqual(self.field(by_name, "Before", "id")["type"], "string")
        self.assertEqual(self.field(by_name, "After", "ok")["type"], "boolean")

    # -- adversarial / error paths --------------------------------------------

    def test_missing_input_files_raise_filenotfounderror(self):
        with self.assertRaises(FileNotFoundError):
            convert_smithy_to_avro(str(self.tmp / "missing.smithy"), str(self.tmp / "out.avsc"))
        with self.assertRaises(FileNotFoundError):
            convert_avro_to_smithy(str(self.tmp / "missing.avsc"), str(self.tmp / "out.smithy"))

    def test_malformed_smithy_raises_valueerror(self):
        src = self.tmp / "bad.smithy"
        src.write_text(f"namespace {NS}\nstructure Bad {{ name: String", encoding="utf-8")
        with self.assertRaises(ValueError):
            convert_smithy_to_avro(str(src), str(self.tmp / "bad.avsc"))

    def test_empty_model_produces_empty_avro_schema(self):
        src = self.tmp / "empty.smithy"
        src.write_text("", encoding="utf-8")
        out = self.tmp / "empty.avsc"
        convert_smithy_to_avro(str(src), str(out))
        self.assertEqual(json.loads(out.read_text(encoding="utf-8")), [])

    def test_non_ascii_identifiers_are_sanitized_and_do_not_drop_following_members(self):
        _, by_name = self.smithy_to_avro(f"namespace {NS}\nstructure Café {{ naïve: String, label: String }}")
        self.assertIn("Caf", by_name)
        self.assertEqual([f["name"] for f in by_name["Caf"]["fields"]], ["ve", "label"])

    def test_duplicate_after_sanitization_collision_is_rejected(self):
        _, by_name = self.smithy_to_avro(f"namespace {NS}\nstructure Collision {{ a.b: String, a_b: Integer }}")
        names = [f["name"] for f in by_name["Collision"]["fields"]]
        self.assertEqual(len(names), len(set(names)))

    def test_unicode_string_defaults_are_preserved(self):
        _, by_name = self.smithy_to_avro(f'namespace {NS}\nstructure UnicodeDefault {{ @default("café ❤") label: String }}')
        self.assertEqual(self.field(by_name, "UnicodeDefault", "label")["default"], "café ❤")


if __name__ == "__main__":
    unittest.main()
