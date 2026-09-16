import json
import os
import sys
import tempfile
import unittest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import asn1tools

from avrotize.structuretoasn1 import convert_structure_to_asn1


class TestStructureToAsn1(unittest.TestCase):
    """Tests for converting JSON Structure schema to ASN.1 modules."""

    def _tmp(self, name):
        out_dir = os.path.join(tempfile.gettempdir(), "avrotize")
        os.makedirs(out_dir, exist_ok=True)
        return os.path.join(out_dir, name)

    def convert_and_validate(self, struct_name):
        """Convert a struct fixture to ASN.1 and assert the module compiles."""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", struct_name + ".struct.json")
        asn1_path = self._tmp(struct_name + ".asn1")
        convert_structure_to_asn1(struct_path, asn1_path)
        asn1tools.compile_files([asn1_path])
        return asn1_path

    def test_convert_basic_types(self):
        self.convert_and_validate("basic-types")

    def test_convert_numeric_types(self):
        self.convert_and_validate("numeric-types")

    def test_convert_temporal_types(self):
        self.convert_and_validate("temporal-types")

    def test_convert_collections(self):
        self.convert_and_validate("collections")

    def test_convert_choice_types(self):
        self.convert_and_validate("choice-types")

    def test_convert_tuple_test(self):
        self.convert_and_validate("tuple-test")

    def test_convert_nested_objects(self):
        self.convert_and_validate("nested-objects")

    def test_convert_complex_scenario(self):
        self.convert_and_validate("complex-scenario")

    def test_convert_edge_cases(self):
        self.convert_and_validate("edge-cases")

    def test_numeric_range_constraints(self):
        """Sized integers must carry faithful ASN.1 range constraints."""
        asn1_path = self.convert_and_validate("numeric-types")
        with open(asn1_path, "r", encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn("INTEGER (-128..127)", text)          # int8
        self.assertIn("INTEGER (-32768..32767)", text)       # int16
        self.assertIn("INTEGER (0..4294967295)", text)       # uint32
        self.assertIn("INTEGER (0..18446744073709551615)", text)  # uint64

    def test_full_type_system_mapping(self):
        """Pin the full JSON Structure -> ASN.1 type mapping across the type system range."""
        schema = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "AllTypes",
            "properties": {
                "boolField": {"type": "boolean"},
                "stringField": {"type": "string"},
                "intField": {"type": "integer"},
                "numberField": {"type": "number"},
                "int8Field": {"type": "int8"},
                "uint8Field": {"type": "uint8"},
                "int16Field": {"type": "int16"},
                "uint16Field": {"type": "uint16"},
                "int32Field": {"type": "int32"},
                "uint32Field": {"type": "uint32"},
                "int64Field": {"type": "int64"},
                "uint64Field": {"type": "uint64"},
                "floatField": {"type": "float"},
                "doubleField": {"type": "double"},
                "decimalField": {"type": "decimal"},
                "binaryField": {"type": "binary"},
                "dateField": {"type": "date"},
                "timeField": {"type": "time"},
                "datetimeField": {"type": "datetime"},
                "durationField": {"type": "duration"},
                "uuidField": {"type": "uuid"},
                "uriField": {"type": "uri"},
                "anyField": {"type": "any"},
                "arrayField": {"type": "array", "items": {"type": "string"}},
                "setField": {"type": "set", "items": {"type": "int32"}},
                "mapField": {"type": "map", "values": {"type": "number"}},
                "tupleField": {
                    "type": "tuple",
                    "tuple": ["lat", "lon"],
                    "properties": {"lat": {"type": "double"}, "lon": {"type": "double"}},
                },
                "choiceField": {
                    "type": "choice",
                    "choices": {"asText": {"type": "string"}, "asNumber": {"type": "integer"}},
                },
                "nullableField": {"type": ["null", "string"]},
                "nestedObject": {
                    "type": "object",
                    "name": "Nested",
                    "properties": {"value": {"type": "string"}},
                },
            },
            "required": ["boolField", "intField"],
        }
        struct_path = self._tmp("alltypes_struct.struct.json")
        with open(struct_path, "w", encoding="utf-8") as handle:
            json.dump(schema, handle)
        asn1_path = self._tmp("alltypes_struct.asn1")
        convert_structure_to_asn1(struct_path, asn1_path)

        asn1tools.compile_files([asn1_path])

        with open(asn1_path, "r", encoding="utf-8") as handle:
            text = handle.read()

        self.assertIn("boolField BOOLEAN", text)
        self.assertIn("stringField UTF8String", text)
        self.assertIn("intField INTEGER", text)
        self.assertIn("numberField REAL", text)
        self.assertIn("int8Field INTEGER (-128..127)", text)
        self.assertIn("uint8Field INTEGER (0..255)", text)
        self.assertIn("int16Field INTEGER (-32768..32767)", text)
        self.assertIn("uint16Field INTEGER (0..65535)", text)
        self.assertIn("int32Field INTEGER (-2147483648..2147483647)", text)
        self.assertIn("uint32Field INTEGER (0..4294967295)", text)
        self.assertIn("int64Field INTEGER (-9223372036854775808..9223372036854775807)", text)
        self.assertIn("uint64Field INTEGER (0..18446744073709551615)", text)
        self.assertIn("floatField REAL", text)
        self.assertIn("doubleField REAL", text)
        self.assertIn("decimalField REAL", text)
        self.assertIn("binaryField OCTET STRING", text)
        self.assertIn("dateField DATE", text)
        self.assertIn("timeField TIME-OF-DAY", text)
        self.assertIn("datetimeField DATE-TIME", text)
        self.assertIn("durationField UTF8String", text)
        self.assertIn("uuidField UTF8String", text)
        self.assertIn("uriField UTF8String", text)
        self.assertIn("anyField ANY", text)
        self.assertIn("arrayField SEQUENCE OF UTF8String", text)
        self.assertIn("setField SET OF INTEGER (-2147483648..2147483647)", text)
        self.assertIn("mapField SEQUENCE OF SEQUENCE { key UTF8String, value REAL }", text)
        self.assertIn("tupleField SEQUENCE { lat REAL, lon REAL }", text)
        self.assertIn("choiceField CHOICE {", text)
        self.assertIn("nullableField UTF8String OPTIONAL", text)
        self.assertIn("nestedObject Nested", text)
        self.assertIn("Nested ::= SEQUENCE", text)
        # Required vs optional: required members carry no OPTIONAL, others do.
        self.assertIn("intField INTEGER,", text)
        self.assertIn("numberField REAL OPTIONAL", text)


    def test_wrapped_type_reference_resolves(self):
        """Canonical `{"type": {"$ref": ...}}` references must resolve to the named type."""
        schema = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "Order",
            "properties": {
                "id": {"type": "string"},
                "customer": {"type": {"$ref": "#/definitions/Customer"}},
                "bareRef": {"$ref": "#/definitions/Customer"},
            },
            "required": ["id"],
            "definitions": {
                "Customer": {
                    "type": "object",
                    "name": "Customer",
                    "properties": {"name": {"type": "string"}},
                }
            },
        }
        struct_path = self._tmp("wrapped_ref.struct.json")
        with open(struct_path, "w", encoding="utf-8") as handle:
            json.dump(schema, handle)
        asn1_path = self._tmp("wrapped_ref.asn1")
        convert_structure_to_asn1(struct_path, asn1_path)
        asn1tools.compile_files([asn1_path])
        with open(asn1_path, "r", encoding="utf-8") as handle:
            text = handle.read()
        # Both the wrapped and the bare form must resolve to the type reference, not ANY.
        self.assertIn("customer Customer", text)
        self.assertIn("bareRef Customer", text)
        self.assertNotIn("customer ANY", text)
        self.assertIn("Customer ::= SEQUENCE", text)

    def test_string_enum_becomes_enumerated(self):
        """A string `enum` must map to an ASN.1 ENUMERATED preserving the symbol labels."""
        schema = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "Task",
            "properties": {
                "status": {"type": "string", "enum": ["open", "in-progress", "done"]},
            },
            "required": ["status"],
        }
        struct_path = self._tmp("string_enum.struct.json")
        with open(struct_path, "w", encoding="utf-8") as handle:
            json.dump(schema, handle)
        asn1_path = self._tmp("string_enum.asn1")
        convert_structure_to_asn1(struct_path, asn1_path)
        asn1tools.compile_files([asn1_path])
        with open(asn1_path, "r", encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn("status ENUMERATED { open(0), in-progress(1), done(2) }", text)

    def test_integer_enum_becomes_value_constraint(self):
        """An integer `enum` must map to an ASN.1 INTEGER value constraint (exact values)."""
        schema = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "Priority",
            "properties": {
                "level": {"type": "int32", "enum": [1, 2, 3, 5, 8]},
                "fixed": {"type": "int32", "const": 42},
            },
            "required": ["level"],
        }
        struct_path = self._tmp("int_enum.struct.json")
        with open(struct_path, "w", encoding="utf-8") as handle:
            json.dump(schema, handle)
        asn1_path = self._tmp("int_enum.asn1")
        convert_structure_to_asn1(struct_path, asn1_path)
        asn1tools.compile_files([asn1_path])
        with open(asn1_path, "r", encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn("level INTEGER (1 | 2 | 3 | 5 | 8)", text)
        self.assertIn("fixed INTEGER (42)", text)

    def test_extends_merges_base_properties(self):
        """`$extends` must merge base properties into the derived SEQUENCE."""
        schema = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "Dog",
            "$extends": "#/definitions/Animal",
            "properties": {"breed": {"type": "string"}},
            "required": ["breed"],
            "definitions": {
                "Animal": {
                    "type": "object",
                    "name": "Animal",
                    "abstract": True,
                    "properties": {"name": {"type": "string"}, "legs": {"type": "int32"}},
                    "required": ["name"],
                }
            },
        }
        struct_path = self._tmp("extends.struct.json")
        with open(struct_path, "w", encoding="utf-8") as handle:
            json.dump(schema, handle)
        asn1_path = self._tmp("extends.asn1")
        convert_structure_to_asn1(struct_path, asn1_path)
        asn1tools.compile_files([asn1_path])
        with open(asn1_path, "r", encoding="utf-8") as handle:
            text = handle.read()
        # Base members (name, legs) must appear alongside the derived member (breed).
        self.assertIn("name UTF8String", text)
        self.assertIn("legs INTEGER", text)
        self.assertIn("breed UTF8String", text)

    def test_nested_namespace_references(self):
        """Definitions nested in namespaces and referenced by pointer must resolve + emit."""
        schema = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "Root",
            "properties": {
                "color": {"type": {"$ref": "#/definitions/com/example/Color"}},
            },
            "required": ["color"],
            "definitions": {
                "com": {
                    "example": {
                        "Color": {
                            "type": "string",
                            "enum": ["red", "green", "blue"],
                        }
                    }
                }
            },
        }
        struct_path = self._tmp("nested_ns.struct.json")
        with open(struct_path, "w", encoding="utf-8") as handle:
            json.dump(schema, handle)
        asn1_path = self._tmp("nested_ns.asn1")
        convert_structure_to_asn1(struct_path, asn1_path)
        asn1tools.compile_files([asn1_path])
        with open(asn1_path, "r", encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn("color Color", text)
        self.assertIn("Color ::= ENUMERATED { red(0), green(1), blue(2) }", text)


if __name__ == "__main__":
    unittest.main()
