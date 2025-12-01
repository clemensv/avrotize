"""
Tests for JSON Structure to CDDL conversion.
"""

import os
import sys
import json
import unittest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretocddl import (
    StructureToCddlConverter,
    convert_structure_to_cddl,
    convert_structure_to_cddl_files
)


class TestStructureToCddl(unittest.TestCase):
    """Test cases for JSON Structure to CDDL conversion."""

    def test_simple_object(self):
        """Test conversion of a simple object type."""
        structure = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "definitions": {
                "person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "age": {"type": "int64"}
                    },
                    "required": ["name", "age"],
                    "name": "person"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        # Check basic structure
        self.assertIn('person', result)
        self.assertIn('name:', result)
        self.assertIn('age:', result)
        self.assertIn('tstr', result)
        self.assertIn('int', result)

    def test_optional_fields(self):
        """Test conversion of optional fields."""
        structure = {
            "definitions": {
                "person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "email": {"type": "string"}
                    },
                    "required": ["name"],
                    "name": "person"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        # email should have optional marker
        self.assertIn('? email', result)
        # name should not have optional marker
        lines = result.split('\n')
        name_line = [l for l in lines if 'name:' in l and 'email' not in l][0]
        self.assertNotIn('?', name_line.split('name:')[0])

    def test_primitive_types(self):
        """Test conversion of primitive types."""
        structure = {
            "definitions": {
                "types": {
                    "type": "object",
                    "properties": {
                        "int_field": {"type": "int64"},
                        "uint_field": {"type": "uint64"},
                        "float_field": {"type": "float64"},
                        "bool_field": {"type": "boolean"},
                        "text_field": {"type": "string"},
                        "bytes_field": {"type": "binary"}
                    },
                    "required": ["int_field", "uint_field", "float_field", "bool_field", "text_field", "bytes_field"],
                    "name": "types"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('int', result)
        self.assertIn('uint', result)
        self.assertIn('float64', result)
        self.assertIn('bool', result)
        self.assertIn('tstr', result)
        self.assertIn('bstr', result)

    def test_array_type(self):
        """Test conversion of array types."""
        structure = {
            "definitions": {
                "list_type": {
                    "type": "array",
                    "items": {"type": "string"},
                    "name": "list_type"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('list-type', result)
        self.assertIn('[*', result)
        self.assertIn('tstr', result)

    def test_array_with_constraints(self):
        """Test conversion of arrays with min/max items."""
        structure = {
            "definitions": {
                "bounded_list": {
                    "type": "array",
                    "items": {"type": "int64"},
                    "minItems": 1,
                    "maxItems": 10,
                    "name": "bounded_list"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('[1*10', result)

    def test_map_type(self):
        """Test conversion of map types."""
        structure = {
            "definitions": {
                "string_map": {
                    "type": "map",
                    "keys": {"type": "string"},
                    "values": {"type": "int64"},
                    "name": "string_map"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('=>', result)
        self.assertIn('tstr', result)
        self.assertIn('int', result)

    def test_union_type(self):
        """Test conversion of union types."""
        structure = {
            "definitions": {
                "result": {
                    "type": ["string", "int64", "null"],
                    "name": "result"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('/', result)  # CDDL choice separator
        self.assertIn('tstr', result)
        self.assertIn('int', result)
        self.assertIn('nil', result)

    def test_enum_type(self):
        """Test conversion of enum types."""
        structure = {
            "definitions": {
                "status": {
                    "type": "string",
                    "enum": ["pending", "active", "completed"],
                    "name": "status"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('"pending"', result)
        self.assertIn('"active"', result)
        self.assertIn('"completed"', result)
        self.assertIn('/', result)

    def test_type_reference(self):
        """Test conversion of type references."""
        structure = {
            "definitions": {
                "address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"}
                    },
                    "required": ["street"],
                    "name": "address"
                },
                "person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "home": {"$ref": "#/definitions/address"}
                    },
                    "required": ["name", "home"],
                    "name": "person"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('address', result)
        self.assertIn('home: address', result)

    def test_extends_to_unwrap(self):
        """Test conversion of $extends to CDDL unwrap operator."""
        structure = {
            "definitions": {
                "base_header": {
                    "type": "object",
                    "properties": {
                        "version": {"type": "int64"},
                        "timestamp": {"type": "int64"}
                    },
                    "required": ["version", "timestamp"],
                    "name": "base_header",
                    "altnames": {"cddl": "base-header"}
                },
                "extended_header": {
                    "type": "object",
                    "$extends": "#/definitions/base_header",
                    "properties": {
                        "message_id": {"type": "string"}
                    },
                    "required": ["message_id"],
                    "name": "extended_header",
                    "altnames": {"cddl": "extended-header"}
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('extended-header', result)
        self.assertIn('~base-header', result)

    def test_string_constraints(self):
        """Test conversion of string constraints."""
        structure = {
            "definitions": {
                "constrained_string": {
                    "type": "object",
                    "properties": {
                        "fixed_length": {"type": "string", "minLength": 5, "maxLength": 5},
                        "bounded": {"type": "string", "minLength": 1, "maxLength": 100},
                        "pattern_match": {"type": "string", "pattern": "^[a-z]+$"}
                    },
                    "required": ["fixed_length", "bounded", "pattern_match"],
                    "name": "constrained_string"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('.size', result)
        self.assertIn('.regexp', result)

    def test_numeric_constraints(self):
        """Test conversion of numeric constraints."""
        structure = {
            "definitions": {
                "bounded_int": {
                    "type": "object",
                    "properties": {
                        "positive": {"type": "int64", "minimum": 0},
                        "percentage": {"type": "int64", "minimum": 0, "maximum": 100}
                    },
                    "required": ["positive", "percentage"],
                    "name": "bounded_int"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        # Single bound uses control operator
        self.assertIn('.ge', result)
        # Double bound uses range syntax (0..100)
        self.assertIn('0..100', result)

    def test_default_value(self):
        """Test conversion of default values."""
        structure = {
            "definitions": {
                "with_defaults": {
                    "type": "object",
                    "properties": {
                        "count": {"type": "int64", "default": 0},
                        "name": {"type": "string", "default": "unknown"}
                    },
                    "required": [],
                    "name": "with_defaults"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('.default 0', result)
        self.assertIn('.default "unknown"', result)

    def test_description_as_comment(self):
        """Test that descriptions become CDDL comments."""
        structure = {
            "definitions": {
                "documented_type": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string"}
                    },
                    "required": ["field"],
                    "name": "documented_type",
                    "description": "This is a documented type"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('; This is a documented type', result)

    def test_altnames_preserved(self):
        """Test that altnames.cddl is used for naming."""
        structure = {
            "definitions": {
                "my_type": {
                    "type": "object",
                    "properties": {
                        "some_field": {
                            "type": "string",
                            "altnames": {"cddl": "some-field"}
                        }
                    },
                    "required": ["some_field"],
                    "name": "my_type",
                    "altnames": {"cddl": "my-type"}
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('my-type', result)
        self.assertIn('some-field', result)

    def test_tuple_type(self):
        """Test conversion of tuple types."""
        structure = {
            "definitions": {
                "coordinate": {
                    "type": "tuple",
                    "tuple": ["x", "y", "z"],
                    "properties": {
                        "x": {"type": "float64"},
                        "y": {"type": "float64"},
                        "z": {"type": "float64"}
                    },
                    "name": "coordinate"
                }
            }
        }
        
        result = convert_structure_to_cddl(json.dumps(structure))
        
        self.assertIn('[', result)
        self.assertIn('float64', result)


class TestStructureToCddlFiles(unittest.TestCase):
    """Test file-based conversion."""

    def test_file_conversion(self):
        """Test converting a JSON Structure file to CDDL."""
        import tempfile
        
        structure = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "$id": "https://example.com/test.json",
            "definitions": {
                "test_type": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "int64"}
                    },
                    "required": ["value"],
                    "name": "test_type"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.struct.json', delete=False) as f:
            json.dump(structure, f)
            input_path = f.name
        
        try:
            output_path = input_path.replace('.struct.json', '.cddl')
            result = convert_structure_to_cddl_files(input_path, output_path)
            
            self.assertIn('test-type', result)
            self.assertTrue(os.path.exists(output_path))
            
            with open(output_path, 'r') as f:
                file_content = f.read()
            self.assertEqual(result, file_content)
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)


if __name__ == '__main__':
    unittest.main()
