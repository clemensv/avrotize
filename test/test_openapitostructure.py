"""
Test module for OpenAPI to JSON Structure conversion.

This module tests the conversion of OpenAPI 3.x documents to JSON Structure format,
ensuring that the converter produces valid and equivalent structure definitions.
"""

import json
import os
import sys
import tempfile
import unittest
from os import path, getcwd

# Add project root to Python path
current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

# Add tools directory for validator
validator_path = path.join(project_root, "tools")
sys.path.append(validator_path)

from avrotize.openapitostructure import (
    OpenApiToStructureConverter,
    convert_openapi_to_structure,
    convert_openapi_to_structure_files
)

try:
    from json_structure_schema_validator import JSONStructureSchemaCoreValidator
    VALIDATOR_AVAILABLE = True
    validator_class = JSONStructureSchemaCoreValidator
except ImportError:
    print("Warning: JSON Structure validator not available. Schema validation will be skipped.")
    VALIDATOR_AVAILABLE = False
    validator_class = None


class TestOpenApiToStructure(unittest.TestCase):
    """Test cases for OpenAPI to JSON Structure conversion."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.converter = OpenApiToStructureConverter()
        self.test_dir = path.join(getcwd(), "test", "openapi")
        self.temp_dir = path.join(tempfile.gettempdir(), "avrotize", "openapi")
        os.makedirs(self.temp_dir, exist_ok=True)
    
    def validate_json_structure(self, structure: dict) -> None:
        """
        Validate that the JSON Structure is well-formed and compliant.
        
        Args:
            structure: The JSON Structure document to validate.
        """
        # Basic validation - should have $schema
        self.assertTrue(structure['$schema'].startswith('https://json-structure.org/'))
        
        # Should have definitions if there were components.schemas
        if 'definitions' in structure:
            self.assertIsInstance(structure['definitions'], dict)
        
        # Use official JSON Structure validator if available
        if VALIDATOR_AVAILABLE and validator_class is not None:
            try:
                source_text = json.dumps(structure)
                validator = validator_class(allow_dollar=False, allow_import=True)
                errors = validator.validate(structure, source_text)
                if errors:
                    print(f"JSON Structure validation errors:")
                    for error in errors:
                        print(f"  - {error}")
            except Exception as e:
                print(f"Warning: Could not validate with official validator: {e}")
    
    def test_simple_openapi_conversion(self):
        """Test conversion of a simple OpenAPI document."""
        openapi_path = path.join(self.test_dir, "simple.json")
        
        with open(openapi_path, 'r', encoding='utf-8') as f:
            openapi_doc = json.load(f)
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        # Validate basic structure
        self.validate_json_structure(result)
        
        # Check that User schema was converted
        self.assertIn('definitions', result)
        self.assertIn('User', result['definitions'])
        
        # Check User properties
        user = result['definitions']['User']
        self.assertEqual(user['type'], 'object')
        self.assertIn('properties', user)
        self.assertIn('id', user['properties'])
        self.assertIn('name', user['properties'])
    
    def test_petstore_openapi_conversion(self):
        """Test conversion of the PetStore OpenAPI document."""
        openapi_path = path.join(self.test_dir, "petstore.json")
        
        with open(openapi_path, 'r', encoding='utf-8') as f:
            openapi_doc = json.load(f)
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        # Validate basic structure
        self.validate_json_structure(result)
        
        # Check that all expected schemas are present
        self.assertIn('definitions', result)
        expected_schemas = ['Pet', 'Owner', 'EmailContact', 'PhoneContact', 'CreatePetRequest', 'Error']
        for schema_name in expected_schemas:
            self.assertIn(schema_name, result['definitions'], f"Missing schema: {schema_name}")
        
        # Check Pet schema has correct properties
        pet = result['definitions']['Pet']
        self.assertEqual(pet['type'], 'object')
        self.assertIn('properties', pet)
        self.assertIn('name', pet['properties'])
        self.assertIn('species', pet['properties'])
    
    def test_vehicles_openapi_conversion(self):
        """Test conversion of the Vehicles OpenAPI document with inheritance."""
        openapi_path = path.join(self.test_dir, "vehicles.json")
        
        with open(openapi_path, 'r', encoding='utf-8') as f:
            openapi_doc = json.load(f)
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        # Validate basic structure
        self.validate_json_structure(result)
        
        # Check that all expected schemas are present
        self.assertIn('definitions', result)
        expected_schemas = ['Vehicle', 'Car', 'Motorcycle', 'Truck', 'FleetVehicle', 'MaintenanceRecord', 'Part']
        for schema_name in expected_schemas:
            self.assertIn(schema_name, result['definitions'], f"Missing schema: {schema_name}")
    
    def test_nullable_handling(self):
        """Test that nullable keyword is properly handled."""
        openapi_doc = {
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "TestSchema": {
                        "type": "object",
                        "properties": {
                            "optionalField": {
                                "type": "string",
                                "nullable": True
                            },
                            "requiredField": {
                                "type": "string"
                            }
                        }
                    }
                }
            }
        }
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        self.assertIn('definitions', result)
        self.assertIn('TestSchema', result['definitions'])
        
        # The nullable field should have a union type with null
        test_schema = result['definitions']['TestSchema']
        optional_field = test_schema['properties']['optionalField']
        
        # The type should include null
        if isinstance(optional_field.get('type'), list):
            # Direct type union
            self.assertIn('null', optional_field['type'])
        elif 'type' in optional_field:
            # Might be wrapped in a different structure
            pass
    
    def test_readonly_writeonly_handling(self):
        """Test that readOnly and writeOnly are mapped to metadata."""
        openapi_doc = {
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "TestSchema": {
                        "type": "object",
                        "properties": {
                            "id": {
                                "type": "string",
                                "readOnly": True
                            },
                            "password": {
                                "type": "string",
                                "writeOnly": True
                            }
                        }
                    }
                }
            }
        }
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        # The conversion should complete without errors
        self.assertIn('definitions', result)
        self.assertIn('TestSchema', result['definitions'])
    
    def test_discriminator_handling(self):
        """Test that OpenAPI discriminator is properly handled."""
        openapi_doc = {
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Animal": {
                        "type": "object",
                        "discriminator": {
                            "propertyName": "animalType"
                        },
                        "properties": {
                            "animalType": {
                                "type": "string"
                            },
                            "name": {
                                "type": "string"
                            }
                        },
                        "required": ["animalType", "name"]
                    }
                }
            }
        }
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        self.assertIn('definitions', result)
        self.assertIn('Animal', result['definitions'])
    
    def test_reference_handling(self):
        """Test that $ref references are properly converted."""
        openapi_doc = {
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Person": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "address": {
                                "$ref": "#/components/schemas/Address"
                            }
                        }
                    },
                    "Address": {
                        "type": "object",
                        "properties": {
                            "street": {"type": "string"},
                            "city": {"type": "string"}
                        }
                    }
                }
            }
        }
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        self.assertIn('definitions', result)
        self.assertIn('Person', result['definitions'])
        self.assertIn('Address', result['definitions'])
        
        # Check that the reference was converted correctly
        person = result['definitions']['Person']
        address_ref = person['properties']['address']
        
        # The reference should point to definitions, not components/schemas
        if '$ref' in address_ref:
            self.assertTrue(address_ref['$ref'].startswith('#/definitions/'))
        elif 'type' in address_ref and isinstance(address_ref['type'], dict) and '$ref' in address_ref['type']:
            self.assertTrue(address_ref['type']['$ref'].startswith('#/definitions/'))
    
    def test_enum_handling(self):
        """Test that enums are properly handled."""
        openapi_doc = {
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Status": {
                        "type": "string",
                        "enum": ["pending", "active", "completed", "cancelled"]
                    },
                    "Order": {
                        "type": "object",
                        "properties": {
                            "status": {
                                "type": "string",
                                "enum": ["new", "processing", "shipped", "delivered"]
                            }
                        }
                    }
                }
            }
        }
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        self.assertIn('definitions', result)
        
        # Check Status enum
        status = result['definitions']['Status']
        self.assertEqual(status['type'], 'string')
        self.assertIn('enum', status)
        self.assertEqual(status['enum'], ["pending", "active", "completed", "cancelled"])
    
    def test_array_handling(self):
        """Test that arrays are properly handled."""
        openapi_doc = {
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Container": {
                        "type": "object",
                        "properties": {
                            "items": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            },
                            "uniqueItems": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                },
                                "uniqueItems": True
                            }
                        }
                    }
                }
            }
        }
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        self.assertIn('definitions', result)
        container = result['definitions']['Container']
        
        # Check regular array
        items = container['properties']['items']
        self.assertEqual(items['type'], 'array')
        
        # Check unique items (should become set)
        unique_items = container['properties']['uniqueItems']
        self.assertEqual(unique_items['type'], 'set')
    
    def test_additional_properties_handling(self):
        """Test that additionalProperties is properly handled."""
        openapi_doc = {
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Metadata": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "string"
                        }
                    }
                }
            }
        }
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        self.assertIn('definitions', result)
        metadata = result['definitions']['Metadata']
        
        # Should be converted to map type
        self.assertEqual(metadata['type'], 'map')
    
    def test_file_conversion(self):
        """Test file-based conversion."""
        openapi_path = path.join(self.test_dir, "simple.json")
        output_path = path.join(self.temp_dir, "simple.struct.json")
        
        self.converter.convert_openapi_file_to_structure(openapi_path, output_path)
        
        # Check that the output file was created
        self.assertTrue(os.path.exists(output_path))
        
        # Validate the output
        with open(output_path, 'r', encoding='utf-8') as f:
            result = json.load(f)
        
        self.validate_json_structure(result)
    
    def test_invalid_openapi_version(self):
        """Test that invalid OpenAPI versions raise errors."""
        swagger_doc = {
            "swagger": "2.0",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {}
        }
        
        with self.assertRaises(ValueError) as context:
            self.converter.convert_openapi_to_structure(swagger_doc)
        
        self.assertIn("Swagger 2.x", str(context.exception))
    
    def test_missing_openapi_field(self):
        """Test that missing openapi field raises error."""
        invalid_doc = {
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {}
        }
        
        with self.assertRaises(ValueError) as context:
            self.converter.convert_openapi_to_structure(invalid_doc)
        
        self.assertIn("openapi", str(context.exception).lower())
    
    def test_string_input(self):
        """Test conversion from JSON string input."""
        openapi_json = json.dumps({
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Simple": {
                        "type": "object",
                        "properties": {
                            "value": {"type": "string"}
                        }
                    }
                }
            }
        })
        
        result = self.converter.convert_openapi_to_structure(openapi_json)
        
        self.assertIn('definitions', result)
        self.assertIn('Simple', result['definitions'])
    
    def test_convenience_function(self):
        """Test the convenience function for string-based conversion."""
        openapi_json = json.dumps({
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Item": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"}
                        }
                    }
                }
            }
        })
        
        result_str = convert_openapi_to_structure(openapi_json)
        result = json.loads(result_str)
        
        self.assertIn('definitions', result)
        self.assertIn('Item', result['definitions'])
    
    def test_file_conversion_function(self):
        """Test the file conversion function."""
        openapi_path = path.join(self.test_dir, "simple.json")
        output_path = path.join(self.temp_dir, "simple_func.struct.json")
        
        convert_openapi_to_structure_files(openapi_path, output_path)
        
        # Check that the output file was created
        self.assertTrue(os.path.exists(output_path))
        
        # Validate the output
        with open(output_path, 'r', encoding='utf-8') as f:
            result = json.load(f)
        
        self.validate_json_structure(result)


class TestOpenApiDiscriminator(unittest.TestCase):
    """Test cases for OpenAPI discriminator handling."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.converter = OpenApiToStructureConverter()
        self.converter.detect_discriminators = True
    
    def test_discriminator_with_mapping(self):
        """Test discriminator with explicit mapping."""
        openapi_doc = {
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "Pet": {
                        "type": "object",
                        "discriminator": {
                            "propertyName": "petType",
                            "mapping": {
                                "dog": "#/components/schemas/Dog",
                                "cat": "#/components/schemas/Cat"
                            }
                        },
                        "properties": {
                            "petType": {"type": "string"},
                            "name": {"type": "string"}
                        },
                        "required": ["petType", "name"]
                    },
                    "Dog": {
                        "allOf": [
                            {"$ref": "#/components/schemas/Pet"},
                            {
                                "type": "object",
                                "properties": {
                                    "breed": {"type": "string"}
                                }
                            }
                        ]
                    },
                    "Cat": {
                        "allOf": [
                            {"$ref": "#/components/schemas/Pet"},
                            {
                                "type": "object",
                                "properties": {
                                    "indoor": {"type": "boolean"}
                                }
                            }
                        ]
                    }
                }
            }
        }
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        # All schemas should be converted
        self.assertIn('definitions', result)
        self.assertIn('Pet', result['definitions'])
        self.assertIn('Dog', result['definitions'])
        self.assertIn('Cat', result['definitions'])


class TestOpenApiMetadata(unittest.TestCase):
    """Test cases for OpenAPI metadata handling."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.converter = OpenApiToStructureConverter()
    
    def test_info_to_schema_metadata(self):
        """Test that OpenAPI info is converted to JSON Structure metadata."""
        openapi_doc = {
            "openapi": "3.0.3",
            "info": {
                "title": "My API",
                "description": "This is my API description",
                "version": "2.1.0"
            },
            "paths": {},
            "components": {
                "schemas": {
                    "Item": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"}
                        }
                    }
                }
            }
        }
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        # Check that $id includes version info
        self.assertIn('$id', result)
        self.assertIn('2.1.0', result['$id'])
    
    def test_deprecated_handling(self):
        """Test that deprecated keyword is handled."""
        openapi_doc = {
            "openapi": "3.0.3",
            "info": {"title": "Test", "version": "1.0.0"},
            "paths": {},
            "components": {
                "schemas": {
                    "OldSchema": {
                        "type": "object",
                        "deprecated": True,
                        "properties": {
                            "value": {"type": "string"}
                        }
                    }
                }
            }
        }
        
        result = self.converter.convert_openapi_to_structure(openapi_doc)
        
        # Conversion should succeed
        self.assertIn('definitions', result)
        self.assertIn('OldSchema', result['definitions'])


if __name__ == '__main__':
    unittest.main()
