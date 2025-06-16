"""Tests for JSON Structure to JSON Schema conversion."""

import json
import unittest
from avrotize.structuretojsons import convert_structure_to_json_schema_string


class TestStructureToJsonSchemaConversion(unittest.TestCase):
    """Test cases for JSON Structure to JSON Schema conversion."""

    def test_basic_object_conversion(self):
        """Test basic object type conversion."""
        structure = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "Person",
            "properties": {
                "id": {"type": "uuid"},
                "name": {"type": "string", "maxLength": 100},
                "age": {"type": "int32", "minimum": 0, "maximum": 150}
            },
            "required": ["id", "name"]
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
        self.assertEqual(schema["title"], "Person")
        self.assertEqual(schema["type"], "object")
        self.assertEqual(schema["required"], ["id", "name"])
        
        # Check UUID type conversion
        self.assertEqual(schema["properties"]["id"]["type"], "string")
        self.assertEqual(schema["properties"]["id"]["format"], "uuid")
        
        # Check string with maxLength
        self.assertEqual(schema["properties"]["name"]["type"], "string")
        self.assertEqual(schema["properties"]["name"]["maxLength"], 100)
        
        # Check int32 with min/max
        self.assertEqual(schema["properties"]["age"]["type"], "integer")
        self.assertEqual(schema["properties"]["age"]["minimum"], 0)
        self.assertEqual(schema["properties"]["age"]["maximum"], 150)

    def test_array_and_set_conversion(self):
        """Test array and set type conversion."""
        structure = {
            "type": "object",
            "properties": {
                "items": {"type": "array", "items": {"type": "string"}},
                "tags": {"type": "set", "items": {"type": "string"}}
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        # Check array
        self.assertEqual(schema["properties"]["items"]["type"], "array")
        self.assertEqual(schema["properties"]["items"]["items"]["type"], "string")
        
        # Check set (becomes array with uniqueItems)
        self.assertEqual(schema["properties"]["tags"]["type"], "array")
        self.assertEqual(schema["properties"]["tags"]["uniqueItems"], True)
        self.assertEqual(schema["properties"]["tags"]["items"]["type"], "string")

    def test_map_conversion(self):
        """Test map type conversion."""
        structure = {
            "type": "object",
            "properties": {
                "attributes": {"type": "map", "values": {"type": "string"}}
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        # Check map (becomes object with additionalProperties)
        self.assertEqual(schema["properties"]["attributes"]["type"], "object")
        self.assertEqual(schema["properties"]["attributes"]["additionalProperties"]["type"], "string")

    def test_choice_conversion(self):
        """Test choice type conversion."""
        structure = {
            "type": "object",
            "properties": {
                "value": {
                    "type": "choice",
                    "choices": [
                        {"type": "string"},
                        {"type": "int32"}
                    ]
                }
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        # Check choice (becomes oneOf)
        self.assertIn("oneOf", schema["properties"]["value"])
        self.assertEqual(len(schema["properties"]["value"]["oneOf"]), 2)
        self.assertEqual(schema["properties"]["value"]["oneOf"][0]["type"], "string")
        self.assertEqual(schema["properties"]["value"]["oneOf"][1]["type"], "integer")

    def test_inline_choice_with_selector_constraints(self):
        """Test inline choice with selector produces proper const constraints."""
        structure = {
            "type": "object",
            "properties": {
                "entity": {
                    "type": "choice",
                    "name": "EntityChoice",
                    "$extends": "#/definitions/BaseEntity",
                    "selector": "entityType",
                    "choices": {
                        "Person": { "$ref": "#/definitions/Person" },
                        "Company": { "$ref": "#/definitions/Company" }
                    }
                }
            },
            "definitions": {
                "BaseEntity": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" },
                        "entityType": { "type": "string" }
                    }
                },
                "Person": {
                    "type": "object",
                    "$extends": "#/definitions/BaseEntity",
                    "properties": {
                        "age": { "type": "int32" }
                    }
                },
                "Company": {
                    "type": "object",
                    "$extends": "#/definitions/BaseEntity",
                    "properties": {
                        "employees": { "type": "int32" }
                    }
                }
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        # Check that it's a oneOf with two choices
        entity_choice = schema["properties"]["entity"]
        self.assertIn("oneOf", entity_choice)
        self.assertEqual(len(entity_choice["oneOf"]), 2)
        
        # Check Person choice has entityType constrained to "Person"
        person_choice = entity_choice["oneOf"][0]
        self.assertEqual(person_choice["type"], "object")
        self.assertIn("properties", person_choice)
        self.assertIn("entityType", person_choice["properties"])
        self.assertEqual(person_choice["properties"]["entityType"], {"const": "Person"})
        self.assertIn("age", person_choice["properties"])
        
        # Check Company choice has entityType constrained to "Company"
        company_choice = entity_choice["oneOf"][1]
        self.assertEqual(company_choice["type"], "object")
        self.assertIn("properties", company_choice)
        self.assertIn("entityType", company_choice["properties"])
        self.assertEqual(company_choice["properties"]["entityType"], {"const": "Company"})
        self.assertIn("employees", company_choice["properties"])

    def test_decimal_with_precision_scale(self):
        """Test decimal type with precision and scale."""
        structure = {
            "type": "object",
            "properties": {
                "price": {
                    "type": "decimal",
                    "precision": 10,
                    "scale": 2,
                    "currency": "USD"
                }
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        price_prop = schema["properties"]["price"]
        self.assertEqual(price_prop["type"], "string")  # decimal is string-based per JSON Structure spec
        self.assertIn("pattern", price_prop)  # Should have a pattern for decimal format
        self.assertEqual(price_prop["x-precision"], 10)
        self.assertEqual(price_prop["x-scale"], 2)
        self.assertEqual(price_prop["x-currency"], "USD")

    def test_temporal_types(self):
        """Test temporal type conversions."""
        structure = {
            "type": "object",
            "properties": {
                "date_field": {"type": "date"},
                "time_field": {"type": "time"},
                "datetime_field": {"type": "datetime"},
                "duration_field": {"type": "duration"}
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        # All temporal types become string with appropriate format
        self.assertEqual(schema["properties"]["date_field"]["type"], "string")
        self.assertEqual(schema["properties"]["date_field"]["format"], "date")
        
        self.assertEqual(schema["properties"]["time_field"]["type"], "string")
        self.assertEqual(schema["properties"]["time_field"]["format"], "time")
        
        self.assertEqual(schema["properties"]["datetime_field"]["type"], "string")
        self.assertEqual(schema["properties"]["datetime_field"]["format"], "date-time")
        
        self.assertEqual(schema["properties"]["duration_field"]["type"], "string")
        self.assertEqual(schema["properties"]["duration_field"]["format"], "duration")

    def test_numeric_types(self):
        """Test various numeric type conversions."""
        structure = {
            "type": "object",
            "properties": {
                "byte_val": {"type": "int8"},
                "short_val": {"type": "int16"},
                "int_val": {"type": "int32"},
                "long_val": {"type": "int64"},
                "ubyte_val": {"type": "uint8"},
                "ushort_val": {"type": "uint16"},
                "uint_val": {"type": "uint32"},
                "ulong_val": {"type": "uint64"},
                "float_val": {"type": "float"},
                "double_val": {"type": "double"}
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        # Check int8
        int8_prop = schema["properties"]["byte_val"]
        self.assertEqual(int8_prop["type"], "integer")
        self.assertEqual(int8_prop["minimum"], -128)
        self.assertEqual(int8_prop["maximum"], 127)
        
        # Check uint8
        uint8_prop = schema["properties"]["ubyte_val"]
        self.assertEqual(uint8_prop["type"], "integer")
        self.assertEqual(uint8_prop["minimum"], 0)
        self.assertEqual(uint8_prop["maximum"], 255)
        
        # Check float
        float_prop = schema["properties"]["float_val"]
        self.assertEqual(float_prop["type"], "number")
        self.assertEqual(float_prop["format"], "float")
        
        # Check double
        double_prop = schema["properties"]["double_val"]
        self.assertEqual(double_prop["type"], "number")
        self.assertEqual(double_prop["format"], "double")

    def test_validation_constraints(self):
        """Test validation constraint preservation."""
        structure = {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 100,
                    "pattern": "^[A-Za-z]+$"
                },
                "count": {
                    "type": "int32",
                    "minimum": 1,
                    "maximum": 1000,
                    "multipleOf": 5
                }
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        # Check string constraints
        name_prop = schema["properties"]["name"]
        self.assertEqual(name_prop["minLength"], 1)
        self.assertEqual(name_prop["maxLength"], 100)
        self.assertEqual(name_prop["pattern"], "^[A-Za-z]+$")
        
        # Check numeric constraints
        count_prop = schema["properties"]["count"]
        self.assertEqual(count_prop["minimum"], 1)
        self.assertEqual(count_prop["maximum"], 1000)
        self.assertEqual(count_prop["multipleOf"], 5)

    def test_bytes_type(self):
        """Test bytes type conversion."""
        structure = {
            "type": "object",
            "properties": {
                "data": {"type": "bytes"}
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        # bytes becomes string with format: byte
        data_prop = schema["properties"]["data"]
        self.assertEqual(data_prop["type"], "string")
        self.assertEqual(data_prop["format"], "byte")

    def test_invalid_json_structure(self):
        """Test error handling for invalid JSON Structure."""
        invalid_structure = "{ invalid json"
        with self.assertRaises(ValueError) as context:
            convert_structure_to_json_schema_string(invalid_structure)
        
        self.assertIn("Invalid JSON Structure document", str(context.exception))

    def test_enum_and_const_validation(self):
        """Test enum and const validation constraint preservation."""
        structure = {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["active", "inactive", "pending"]
                },
                "type_constant": {
                    "type": "string",
                    "const": "user"
                },
                "priority": {
                    "type": "int32",
                    "enum": [1, 2, 3, 4, 5]
                }
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        # Check string enum
        status_prop = schema["properties"]["status"]
        self.assertEqual(status_prop["type"], "string")
        self.assertEqual(status_prop["enum"], ["active", "inactive", "pending"])
        
        # Check const
        const_prop = schema["properties"]["type_constant"]
        self.assertEqual(const_prop["type"], "string")
        self.assertEqual(const_prop["const"], "user")
        
        # Check numeric enum
        priority_prop = schema["properties"]["priority"]
        self.assertEqual(priority_prop["type"], "integer")
        self.assertEqual(priority_prop["enum"], [1, 2, 3, 4, 5])

    def test_uses_directive_preservation(self):
        """Test $uses directive preservation as x-uses extension."""
        structure = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "$uses": ["JSONStructureUnits", "JSONStructureAlternateNames"],
            "type": "object",
            "properties": {
                "weight": {
                    "type": "double",
                    "unit": "kg"
                },
                "name": {
                    "type": "string",
                    "altnames": {
                        "json": "display_name",
                        "lang:en": "Name"
                    }
                }
            }
        }
        
        result = convert_structure_to_json_schema_string(json.dumps(structure))
        schema = json.loads(result)
        
        # Check $uses preservation
        self.assertEqual(schema["x-uses"], ["JSONStructureUnits", "JSONStructureAlternateNames"])
        
        # Check that extensions are properly converted
        self.assertEqual(schema["properties"]["weight"]["x-unit"], "kg")
        self.assertEqual(schema["properties"]["name"]["x-altnames"]["json"], "display_name")


if __name__ == '__main__':
    unittest.main()
