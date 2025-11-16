"""Tests for jstructtoavro module."""

import json
import os
import tempfile
import unittest

from avrotize.jstructtoavro import JsonStructureToAvro, convert_json_structure_to_avro
from avrotize.avrotojstruct import AvroToJsonStructure


class TestJsonStructureToAvro(unittest.TestCase):
    """Test cases for JsonStructureToAvro converter."""

    def setUp(self):
        """Set up test fixtures."""
        self.converter = JsonStructureToAvro()
        self.maxDiff = None

    def test_simple_record(self):
        """Test converting a simple record."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Person",
            "definitions": {
                "Person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "age": {"type": "int32"}
                    },
                    "required": ["name", "age"]
                }
            }
        }

        result = self.converter.convert(structure)

        self.assertEqual(result['type'], 'record')
        self.assertEqual(result['name'], 'Person')
        self.assertEqual(len(result['fields']), 2)
        self.assertEqual(result['fields'][0]['name'], 'name')
        self.assertEqual(result['fields'][0]['type'], 'string')
        self.assertEqual(result['fields'][1]['name'], 'age')
        self.assertEqual(result['fields'][1]['type'], 'int')

    def test_optional_fields(self):
        """Test converting record with optional fields."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Person",
            "definitions": {
                "Person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "age": {"type": "int32"}
                    },
                    "required": ["name"]
                }
            }
        }

        result = self.converter.convert(structure)

        # age should be nullable with null default
        age_field = next(f for f in result['fields'] if f['name'] == 'age')
        self.assertEqual(age_field['type'], ['null', 'int'])
        self.assertIsNone(age_field['default'])

    def test_nested_namespace(self):
        """Test converting record with nested namespace."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/com/example/Person",
            "definitions": {
                "com": {
                    "example": {
                        "Person": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"}
                            },
                            "required": ["name"]
                        }
                    }
                }
            }
        }

        result = self.converter.convert(structure)

        self.assertEqual(result['namespace'], 'com.example')
        self.assertEqual(result['name'], 'Person')

    def test_enum_type(self):
        """Test converting enum type."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Color",
            "definitions": {
                "Color": {
                    "type": "string",
                    "enum": ["RED", "GREEN", "BLUE"]
                }
            }
        }

        result = self.converter.convert(structure)

        self.assertEqual(result['type'], 'enum')
        self.assertEqual(result['name'], 'Color')
        self.assertEqual(result['symbols'], ["RED", "GREEN", "BLUE"])

    def test_array_type(self):
        """Test converting array type."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/StringList",
            "definitions": {
                "StringList": {
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    },
                    "required": ["items"]
                }
            }
        }

        result = self.converter.convert(structure)

        items_field = result['fields'][0]
        self.assertEqual(items_field['type']['type'], 'array')
        self.assertEqual(items_field['type']['items'], 'string')

    def test_map_type(self):
        """Test converting map type."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/StringMap",
            "definitions": {
                "StringMap": {
                    "type": "object",
                    "properties": {
                        "data": {
                            "type": "map",
                            "values": {"type": "string"}
                        }
                    },
                    "required": ["data"]
                }
            }
        }

        result = self.converter.convert(structure)

        data_field = result['fields'][0]
        self.assertEqual(data_field['type']['type'], 'map')
        self.assertEqual(data_field['type']['values'], 'string')

    def test_type_reference(self):
        """Test converting type with references."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Employee",
            "definitions": {
                "Address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"}
                    },
                    "required": ["street", "city"]
                },
                "Employee": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "address": {"$ref": "#/definitions/Address"}
                    },
                    "required": ["name", "address"]
                }
            }
        }

        result = self.converter.convert(structure)

        # Should convert both types
        self.assertEqual(len(self.converter.converted_types), 2)
        
        # Result should be a list when there are multiple types
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        
        # Find the Employee record
        employee = next(r for r in result if r['name'] == 'Employee')
        
        # Check reference is resolved
        address_field = next(f for f in employee['fields'] if f['name'] == 'address')
        self.assertEqual(address_field['type'], 'Address')

    def test_logical_types(self):
        """Test converting logical types."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Event",
            "definitions": {
                "Event": {
                    "type": "object",
                    "properties": {
                        "timestamp": {
                            "type": "int64",
                            "logicalType": "timestampMillis"
                        },
                        "uuid": {
                            "type": "string",
                            "logicalType": "uuid"
                        }
                    },
                    "required": ["timestamp", "uuid"]
                }
            }
        }

        result = self.converter.convert(structure)

        timestamp_field = next(f for f in result['fields'] if f['name'] == 'timestamp')
        self.assertEqual(timestamp_field['type']['type'], 'long')
        self.assertEqual(timestamp_field['type']['logicalType'], 'timestamp-millis')

        uuid_field = next(f for f in result['fields'] if f['name'] == 'uuid')
        self.assertEqual(uuid_field['type']['type'], 'string')
        self.assertEqual(uuid_field['type']['logicalType'], 'uuid')

    def test_round_trip_conversion(self):
        """Test round-trip conversion: Avro -> Structure -> Avro."""
        # Start with a simple Avro schema
        original_avro = {
            "type": "record",
            "name": "TestRecord",
            "namespace": "com.example",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"}
            ]
        }

        # Convert to structure
        to_struct_converter = AvroToJsonStructure()
        structure = to_struct_converter.convert(original_avro, "com.example")

        # Convert back to Avro
        result = self.converter.convert(structure)

        # Verify key properties
        self.assertEqual(result['type'], 'record')
        self.assertEqual(result['name'], 'TestRecord')
        self.assertEqual(result['namespace'], 'com.example')
        self.assertEqual(len(result['fields']), 3)

    def test_file_conversion(self):
        """Test file-based conversion."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Simple",
            "definitions": {
                "Simple": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"}
                    },
                    "required": ["value"]
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            struct_file = os.path.join(tmpdir, "test.struct.json")
            avro_file = os.path.join(tmpdir, "test.avsc")

            # Write structure file
            with open(struct_file, 'w', encoding='utf-8') as f:
                json.dump(structure, f)

            # Convert
            convert_json_structure_to_avro(struct_file, avro_file)

            # Verify output file exists and is valid
            self.assertTrue(os.path.exists(avro_file))
            
            with open(avro_file, 'r', encoding='utf-8') as f:
                result = json.load(f)
            
            self.assertEqual(result['name'], 'Simple')

    def test_fixed_type(self):
        """Test converting fixed-length binary type."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/MD5",
            "definitions": {
                "MD5": {
                    "type": "binary",
                    "byteLength": 16,
                    "description": "MD5 hash"
                }
            }
        }

        result = self.converter.convert(structure)

        self.assertEqual(result['type'], 'fixed')
        self.assertEqual(result['name'], 'MD5')
        self.assertEqual(result['size'], 16)
        self.assertEqual(result['doc'], 'MD5 hash')


if __name__ == '__main__':
    unittest.main()
