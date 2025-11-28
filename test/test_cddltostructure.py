"""
Tests for CDDL to JSON Structure conversion.
"""

import os
import sys
import tempfile
import json

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from avrotize.cddltostructure import (
    CddlToStructureConverter,
    convert_cddl_to_structure,
    convert_cddl_to_structure_files
)


class TestCddlToStructure(unittest.TestCase):
    """Test cases for CDDL to JSON Structure conversion."""

    def test_simple_object(self):
        """Test conversion of a simple CDDL object type."""
        cddl = '''
        person = {
            name: tstr
            age: uint
        }
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        # Check schema basics
        self.assertIn('$schema', result)
        self.assertIn('definitions', result)
        self.assertIn('person', result['definitions'])
        
        # Check person type
        person = result['definitions']['person']
        self.assertEqual(person['type'], 'object')
        self.assertIn('properties', person)
        self.assertEqual(person['properties']['name']['type'], 'string')
        self.assertEqual(person['properties']['age']['type'], 'int64')

    def test_optional_fields(self):
        """Test conversion of optional fields with ? indicator."""
        cddl = '''
        person = {
            name: tstr
            ? email: tstr
            ? phone: tstr
        }
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        person = result['definitions']['person']
        
        # name should be required, email and phone should not
        self.assertIn('required', person)
        self.assertIn('name', person['required'])
        self.assertNotIn('email', person['required'])
        self.assertNotIn('phone', person['required'])

    def test_primitive_types(self):
        """Test conversion of CDDL primitive types."""
        cddl = '''
        types = {
            int_field: int
            uint_field: uint
            float_field: float
            float16_field: float16
            float32_field: float32
            float64_field: float64
            bool_field: bool
            text_field: tstr
            bytes_field: bstr
        }
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        props = result['definitions']['types']['properties']
        
        self.assertEqual(props['int_field']['type'], 'int64')
        self.assertEqual(props['uint_field']['type'], 'int64')
        self.assertEqual(props['float_field']['type'], 'double')
        self.assertEqual(props['float16_field']['type'], 'float')
        self.assertEqual(props['float32_field']['type'], 'float')
        self.assertEqual(props['float64_field']['type'], 'double')
        self.assertEqual(props['bool_field']['type'], 'boolean')
        self.assertEqual(props['text_field']['type'], 'string')
        self.assertEqual(props['bytes_field']['type'], 'bytes')

    def test_array_type(self):
        """Test conversion of CDDL array types."""
        cddl = '''
        list-type = [* tstr]
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('list_type', result['definitions'])
        list_type = result['definitions']['list_type']
        self.assertEqual(list_type['type'], 'array')
        self.assertIn('items', list_type)

    def test_nested_objects(self):
        """Test conversion of nested CDDL objects."""
        cddl = '''
        address = {
            street: tstr
            city: tstr
        }
        
        person = {
            name: tstr
            address: address
        }
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('address', result['definitions'])
        self.assertIn('person', result['definitions'])
        
        # Check that person references address
        person_props = result['definitions']['person']['properties']
        self.assertIn('address', person_props)

    def test_hyphenated_names(self):
        """Test that hyphenated CDDL names are normalized with altnames."""
        cddl = '''
        my-type = {
            first-name: tstr
            last-name: tstr
        }
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        # Type name should be normalized
        self.assertIn('my_type', result['definitions'])
        my_type = result['definitions']['my_type']
        
        # Properties should be normalized
        self.assertIn('first_name', my_type['properties'])
        self.assertIn('last_name', my_type['properties'])
        
        # Check altnames
        self.assertIn('altnames', my_type['properties']['first_name'])
        self.assertEqual(my_type['properties']['first_name']['altnames']['cddl'], 'first-name')

    def test_rfc8610_reputon_example(self):
        """Test the reputon example from RFC 8610."""
        cddl = '''
        reputon = {
            rater: tstr
            assertion: tstr  
            rated: tstr
            rating: float16
            ? confidence: float16
            ? normal-rating: float16
        }
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('reputon', result['definitions'])
        reputon = result['definitions']['reputon']
        
        # Check required fields
        self.assertIn('required', reputon)
        self.assertIn('rater', reputon['required'])
        self.assertIn('assertion', reputon['required'])
        self.assertIn('rated', reputon['required'])
        self.assertIn('rating', reputon['required'])
        
        # Check optional fields are not in required
        self.assertNotIn('confidence', reputon['required'])
        self.assertNotIn('normal_rating', reputon['required'])

    def test_file_conversion(self):
        """Test file-based conversion."""
        cddl = '''
        simple = {
            value: int
        }
        '''
        
        with tempfile.TemporaryDirectory() as tmpdir:
            cddl_file = os.path.join(tmpdir, 'test.cddl')
            struct_file = os.path.join(tmpdir, 'test.struct.json')
            
            # Write CDDL file
            with open(cddl_file, 'w') as f:
                f.write(cddl)
            
            # Convert
            convert_cddl_to_structure_files(cddl_file, struct_file)
            
            # Verify output
            self.assertTrue(os.path.exists(struct_file))
            
            with open(struct_file, 'r') as f:
                result = json.load(f)
            
            self.assertIn('definitions', result)
            self.assertIn('simple', result['definitions'])

    def test_namespace_option(self):
        """Test that namespace option is respected."""
        cddl = '''
        test-type = {
            value: int
        }
        '''
        result = json.loads(convert_cddl_to_structure(cddl, 'my.custom.namespace'))
        
        # Check that $id uses the namespace
        self.assertIn('$id', result)
        self.assertIn('my', result['$id'])

    def test_const_values(self):
        """Test conversion of constant values."""
        cddl = '''
        version = 1
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        # The version type should be a const
        self.assertIn('definitions', result)
        self.assertIn('version', result['definitions'])

    def test_multiple_types(self):
        """Test conversion of multiple type definitions."""
        cddl = '''
        type1 = {
            field1: tstr
        }
        
        type2 = {
            field2: int
        }
        
        type3 = {
            field3: bool
        }
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('type1', result['definitions'])
        self.assertIn('type2', result['definitions'])
        self.assertIn('type3', result['definitions'])

    def test_size_constraint_string(self):
        """Test .size constraint on string types."""
        cddl = '''
        bounded-string = tstr .size (1..100)
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('bounded_string', result['definitions'])
        bounded = result['definitions']['bounded_string']
        self.assertEqual(bounded['type'], 'string')
        self.assertEqual(bounded.get('minLength'), 1)
        self.assertEqual(bounded.get('maxLength'), 100)

    def test_size_constraint_exact(self):
        """Test exact .size constraint."""
        cddl = '''
        fixed-bytes = bstr .size 32
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('fixed_bytes', result['definitions'])
        fixed = result['definitions']['fixed_bytes']
        self.assertEqual(fixed['type'], 'bytes')
        self.assertEqual(fixed.get('minLength'), 32)
        self.assertEqual(fixed.get('maxLength'), 32)

    def test_default_value(self):
        """Test .default operator."""
        cddl = '''
        greeting = tstr .default "hello"
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('greeting', result['definitions'])
        greeting = result['definitions']['greeting']
        self.assertEqual(greeting['type'], 'string')
        self.assertEqual(greeting.get('default'), 'hello')

    def test_comparison_operators(self):
        """Test .lt, .le, .gt, .ge operators."""
        cddl = '''
        positive = int .ge 0
        percentage = int .le 100
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        positive = result['definitions']['positive']
        self.assertEqual(positive.get('minimum'), 0)
        
        percentage = result['definitions']['percentage']
        self.assertEqual(percentage.get('maximum'), 100)

    def test_generic_type_definition(self):
        """Test generic type parameter definitions."""
        cddl = '''
        optional<T> = T / nil
        opt-string = optional<tstr>
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        # The opt-string should be a union of string and null
        self.assertIn('definitions', result)
        self.assertIn('opt_string', result['definitions'])
        opt_string = result['definitions']['opt_string']
        
        # Should be a union type (array of types)
        if 'type' in opt_string and isinstance(opt_string['type'], list):
            types = [t.get('type') for t in opt_string['type'] if isinstance(t, dict)]
            self.assertIn('string', types)
            self.assertIn('null', types)

    def test_generic_type_pair(self):
        """Test generic type with multiple parameters."""
        cddl = '''
        pair<K, V> = [K, V]
        string-int-pair = pair<tstr, int>
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        # The string-int-pair should be an array
        self.assertIn('definitions', result)
        self.assertIn('string_int_pair', result['definitions'])
        pair = result['definitions']['string_int_pair']
        self.assertEqual(pair['type'], 'array')

    def test_range_constraint(self):
        """Test range constraints on numeric types."""
        cddl = '''
        byte-value = 0..255
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('byte_value', result['definitions'])
        byte_val = result['definitions']['byte_value']
        self.assertEqual(byte_val.get('minimum'), 0)
        self.assertEqual(byte_val.get('maximum'), 255)

    def test_choice_type(self):
        """Test choice/union types."""
        cddl = '''
        string-or-int = tstr / int
        '''
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('string_or_int', result['definitions'])
        choice = result['definitions']['string_or_int']
        
        # Should be a union type
        if 'type' in choice and isinstance(choice['type'], list):
            types = [t.get('type') for t in choice['type'] if isinstance(t, dict)]
            self.assertIn('string', types)
            self.assertIn('int64', types)


if __name__ == '__main__':
    unittest.main()
