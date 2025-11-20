"""
Tests for JSON Structure to XSD conversion
"""
import os
import sys
import tempfile
from os import path, getcwd
import xmlschema
import xmlunittest
import lxml.etree as ET


current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretoxsd import convert_structure_to_xsd


class TestStructureToXSD(xmlunittest.XmlTestCase):
    """Test cases for JSON Structure to XSD conversion"""
    
    def create_xsd_from_structure(self, struct_name, xsd_path='', xsd_ref_path='', namespace=''):
        """Convert a JSON Structure schema to XSD and optionally validate against reference"""
        cwd = getcwd()
        struct_path = path.join(cwd, "test", "jsons", struct_name + ".struct.json")
        
        if not xsd_path:
            xsd_path = struct_name + ".xsd"
        
        if not xsd_ref_path:
            xsd_ref_full_path = path.join(cwd, "test", "jsons", struct_name + "-ref.xsd")
        else:
            xsd_ref_full_path = path.join(cwd, "test", "jsons", xsd_ref_path)
        
        xsd_full_path = path.join(tempfile.gettempdir(), "avrotize", xsd_path)
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(xsd_full_path), exist_ok=True)
        
        # Convert JSON Structure to XSD
        convert_structure_to_xsd(struct_path, xsd_full_path, namespace)
        
        # Validate the XSD is well-formed
        try:
            xmlschema.XMLSchema(xsd_full_path)
        except Exception as e:
            # Print the generated XSD for debugging
            with open(xsd_full_path, 'r') as f:
                print(f"Generated XSD:\n{f.read()}")
            raise e
        
        # Load and compare if reference exists
        with open(xsd_full_path, 'r') as file:
            xsd_full = file.read()
        
        if os.path.exists(xsd_ref_full_path):
            with open(xsd_ref_full_path, 'r') as file:
                xsd_ref = file.read()
            self.assertXmlEquivalentOutputs(xsd_full, xsd_ref)
    
    def test_convert_simple_structure_to_xsd(self):
        """Test converting a simple JSON Structure schema to XSD"""
        # For now, we'll create a simple test schema
        import json
        cwd = getcwd()
        test_schema = {
            "type": "object",
            "name": "SimpleRecord",
            "namespace": "com.example",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "int32"},
                "active": {"type": "boolean"}
            },
            "required": ["name"]
        }
        
        # Write test schema
        test_schema_path = path.join(tempfile.gettempdir(), "avrotize", "simple-test.struct.json")
        os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
        with open(test_schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        # Convert to XSD
        xsd_path = path.join(tempfile.gettempdir(), "avrotize", "simple-test.xsd")
        convert_structure_to_xsd(test_schema_path, xsd_path)
        
        # Validate the XSD is well-formed
        xmlschema.XMLSchema(xsd_path)
        
        # Load and verify basic structure
        with open(xsd_path, 'r') as f:
            xsd_content = f.read()
        
        # Check that basic elements are present
        self.assertIn('SimpleRecord', xsd_content)
        self.assertIn('name', xsd_content)
        self.assertIn('age', xsd_content)
        self.assertIn('active', xsd_content)
    
    def test_convert_enum_structure_to_xsd(self):
        """Test converting a JSON Structure enum to XSD"""
        import json
        test_schema = {
            "type": "object",
            "name": "StatusRecord",
            "properties": {
                "status": {
                    "enum": ["active", "inactive", "pending"],
                    "name": "StatusEnum"
                }
            }
        }
        
        # Write test schema
        test_schema_path = path.join(tempfile.gettempdir(), "avrotize", "enum-test.struct.json")
        os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
        with open(test_schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        # Convert to XSD
        xsd_path = path.join(tempfile.gettempdir(), "avrotize", "enum-test.xsd")
        convert_structure_to_xsd(test_schema_path, xsd_path)
        
        # Validate the XSD is well-formed
        xmlschema.XMLSchema(xsd_path)
        
        # Load and verify enum structure
        with open(xsd_path, 'r') as f:
            xsd_content = f.read()
        
        self.assertIn('StatusEnum', xsd_content)
        self.assertIn('active', xsd_content)
        self.assertIn('inactive', xsd_content)
        self.assertIn('pending', xsd_content)
    
    def test_convert_array_structure_to_xsd(self):
        """Test converting a JSON Structure array to XSD"""
        import json
        test_schema = {
            "type": "object",
            "name": "ListRecord",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            }
        }
        
        # Write test schema
        test_schema_path = path.join(tempfile.gettempdir(), "avrotize", "array-test.struct.json")
        os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
        with open(test_schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        # Convert to XSD
        xsd_path = path.join(tempfile.gettempdir(), "avrotize", "array-test.xsd")
        convert_structure_to_xsd(test_schema_path, xsd_path)
        
        # Validate the XSD is well-formed
        xmlschema.XMLSchema(xsd_path)
        
        # Load and verify array structure
        with open(xsd_path, 'r') as f:
            xsd_content = f.read()
        
        self.assertIn('items', xsd_content)
        self.assertIn('maxOccurs', xsd_content)
    
    def test_convert_map_structure_to_xsd(self):
        """Test converting a JSON Structure map to XSD"""
        import json
        test_schema = {
            "type": "object",
            "name": "MapRecord",
            "properties": {
                "metadata": {
                    "type": "map",
                    "values": {"type": "string"}
                }
            }
        }
        
        # Write test schema
        test_schema_path = path.join(tempfile.gettempdir(), "avrotize", "map-test.struct.json")
        os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
        with open(test_schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        # Convert to XSD
        xsd_path = path.join(tempfile.gettempdir(), "avrotize", "map-test.xsd")
        convert_structure_to_xsd(test_schema_path, xsd_path)
        
        # Validate the XSD is well-formed
        xmlschema.XMLSchema(xsd_path)
        
        # Load and verify map structure
        with open(xsd_path, 'r') as f:
            xsd_content = f.read()
        
        self.assertIn('metadata', xsd_content)
        self.assertIn('key', xsd_content)
        self.assertIn('value', xsd_content)
    
    def test_convert_union_structure_to_xsd(self):
        """Test converting a JSON Structure union (anyOf) to XSD"""
        import json
        test_schema = {
            "type": "object",
            "name": "UnionRecord",
            "properties": {
                "value": {
                    "anyOf": [
                        {"type": "string"},
                        {"type": "int32"}
                    ]
                }
            }
        }
        
        # Write test schema
        test_schema_path = path.join(tempfile.gettempdir(), "avrotize", "union-test.struct.json")
        os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
        with open(test_schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        # Convert to XSD
        xsd_path = path.join(tempfile.gettempdir(), "avrotize", "union-test.xsd")
        convert_structure_to_xsd(test_schema_path, xsd_path)
        
        # Validate the XSD is well-formed
        xmlschema.XMLSchema(xsd_path)
        
        # Load and verify union structure
        with open(xsd_path, 'r') as f:
            xsd_content = f.read()
        
        self.assertIn('value', xsd_content)
    
    def test_convert_nested_object_structure_to_xsd(self):
        """Test converting a nested JSON Structure object to XSD"""
        import json
        test_schema = {
            "type": "object",
            "name": "PersonRecord",
            "properties": {
                "name": {"type": "string"},
                "address": {
                    "type": "object",
                    "name": "Address",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"}
                    }
                }
            }
        }
        
        # Write test schema
        test_schema_path = path.join(tempfile.gettempdir(), "avrotize", "nested-test.struct.json")
        os.makedirs(os.path.dirname(test_schema_path), exist_ok=True)
        with open(test_schema_path, 'w') as f:
            json.dump(test_schema, f)
        
        # Convert to XSD
        xsd_path = path.join(tempfile.gettempdir(), "avrotize", "nested-test.xsd")
        convert_structure_to_xsd(test_schema_path, xsd_path)
        
        # Validate the XSD is well-formed
        xmlschema.XMLSchema(xsd_path)
        
        # Load and verify nested structure
        with open(xsd_path, 'r') as f:
            xsd_content = f.read()
        
        self.assertIn('PersonRecord', xsd_content)
        self.assertIn('Address', xsd_content)
        self.assertIn('street', xsd_content)


if __name__ == '__main__':
    import unittest
    unittest.main()
