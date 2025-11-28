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
        
        # The string-int-pair should be a tuple (fixed-size array with different types)
        self.assertIn('definitions', result)
        self.assertIn('string_int_pair', result['definitions'])
        # Generic template 'pair' should NOT be in definitions
        self.assertNotIn('pair', result['definitions'])
        pair = result['definitions']['string_int_pair']
        self.assertEqual(pair['type'], 'tuple')
        self.assertIn('properties', pair)
        self.assertIn('tuple', pair)

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


class TestCddlSchemaFiles(unittest.TestCase):
    """Test cases using CDDL schema files from test/cddl directory."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.cddl_dir = os.path.join(os.path.dirname(__file__), 'cddl')
    
    def _load_cddl_file(self, filename: str) -> str:
        """Load a CDDL file from the test/cddl directory."""
        filepath = os.path.join(self.cddl_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    
    def test_reputon_schema(self):
        """Test RFC 8610 Appendix E reputon example."""
        cddl = self._load_cddl_file('reputon.cddl')
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('reputon', result['definitions'])
        self.assertIn('reputon_response', result['definitions'])
        
        reputon = result['definitions']['reputon']
        self.assertEqual(reputon['type'], 'object')
        self.assertIn('rater', reputon['properties'])
        self.assertIn('rating', reputon['properties'])
    
    def test_primitives_schema(self):
        """Test primitives CDDL schema."""
        cddl = self._load_cddl_file('primitives.cddl')
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('primitives', result['definitions'])
        self.assertIn('constrained_types', result['definitions'])
        
        # Check constrained types have proper validation
        constrained = result['definitions']['constrained_types']
        props = constrained.get('properties', {})
        
        # short-string should have size constraints
        if 'short_string' in props:
            self.assertIn('minLength', props['short_string'])
            self.assertIn('maxLength', props['short_string'])
    
    def test_generics_schema(self):
        """Test generics CDDL schema."""
        cddl = self._load_cddl_file('generics.cddl')
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        # Generic templates should NOT be in definitions (they are processing hints)
        self.assertNotIn('optional', result['definitions'])
        self.assertNotIn('pair', result['definitions'])
        # Concrete instantiated types should be present
        self.assertIn('person_name', result['definitions'])
        self.assertIn('string_int_pair', result['definitions'])
        self.assertIn('api_result', result['definitions'])
    
    def test_arrays_maps_schema(self):
        """Test arrays and maps CDDL schema."""
        cddl = self._load_cddl_file('arrays_maps.cddl')
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('string_list', result['definitions'])
        self.assertIn('config_map', result['definitions'])
        
        # string_list should be an array
        string_list = result['definitions']['string_list']
        self.assertEqual(string_list['type'], 'array')
    
    def test_choices_schema(self):
        """Test choice/union CDDL schema."""
        cddl = self._load_cddl_file('choices.cddl')
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('shape', result['definitions'])
        self.assertIn('circle', result['definitions'])
        self.assertIn('event', result['definitions'])
    
    def test_control_operators_schema(self):
        """Test control operators CDDL schema."""
        cddl = self._load_cddl_file('control_operators.cddl')
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        
        # Check username has size constraints
        username = result['definitions'].get('username', {})
        self.assertEqual(username.get('minLength'), 3)
        self.assertEqual(username.get('maxLength'), 20)
        
        # Check default values
        timeout = result['definitions'].get('timeout_ms', {})
        self.assertEqual(timeout.get('default'), 5000)
    
    def test_iot_sensor_schema(self):
        """Test comprehensive IoT sensor CDDL schema."""
        cddl = self._load_cddl_file('iot_sensor.cddl')
        result = json.loads(convert_cddl_to_structure(cddl))
        
        self.assertIn('definitions', result)
        self.assertIn('device_info', result['definitions'])
        self.assertIn('telemetry_message', result['definitions'])
        self.assertIn('command_message', result['definitions'])
        self.assertIn('coordinates', result['definitions'])
        
        # Check coordinates has range constraints
        coords = result['definitions']['coordinates']
        self.assertEqual(coords['type'], 'object')


class TestCddlRegressions(unittest.TestCase):
    """Regression tests comparing output against reference files."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.cddl_dir = os.path.join(os.path.dirname(__file__), 'cddl')
    
    def _load_cddl_file(self, filename: str) -> str:
        """Load a CDDL file from the test/cddl directory."""
        filepath = os.path.join(self.cddl_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    
    def _load_reference_file(self, filename: str) -> dict:
        """Load a reference JSON Structure file."""
        filepath = os.path.join(self.cddl_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _compare_with_reference(self, cddl_filename: str):
        """Compare conversion output with reference file."""
        cddl = self._load_cddl_file(cddl_filename)
        result = json.loads(convert_cddl_to_structure(cddl))
        
        ref_filename = cddl_filename.replace('.cddl', '-ref.struct.json')
        expected = self._load_reference_file(ref_filename)
        
        self.assertEqual(result, expected, 
            f"Output for {cddl_filename} does not match reference {ref_filename}")
    
    def test_reputon_regression(self):
        """Regression test for reputon.cddl"""
        self._compare_with_reference('reputon.cddl')
    
    def test_primitives_regression(self):
        """Regression test for primitives.cddl"""
        self._compare_with_reference('primitives.cddl')
    
    def test_generics_regression(self):
        """Regression test for generics.cddl"""
        self._compare_with_reference('generics.cddl')
    
    def test_arrays_maps_regression(self):
        """Regression test for arrays_maps.cddl"""
        self._compare_with_reference('arrays_maps.cddl')
    
    def test_choices_regression(self):
        """Regression test for choices.cddl"""
        self._compare_with_reference('choices.cddl')
    
    def test_control_operators_regression(self):
        """Regression test for control_operators.cddl"""
        self._compare_with_reference('control_operators.cddl')
    
    def test_cose_headers_regression(self):
        """Regression test for cose_headers.cddl"""
        self._compare_with_reference('cose_headers.cddl')
    
    def test_cwt_claims_regression(self):
        """Regression test for cwt_claims.cddl"""
        self._compare_with_reference('cwt_claims.cddl')
    
    def test_iot_sensor_regression(self):
        """Regression test for iot_sensor.cddl"""
        self._compare_with_reference('iot_sensor.cddl')


if __name__ == '__main__':
    unittest.main()
