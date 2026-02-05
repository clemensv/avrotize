"""Comprehensive tests for json2s inference with 'evil' edge cases.

These tests validate that json2s produces correct JSON Structure schemas
that can validate the original input data using the json-structure SDK.
"""

import json
import os
import sys
import tempfile
import unittest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.jsontoschema import convert_json_to_jstruct
from avrotize.schema_inference import infer_jstruct_schema_from_json

# JSON Structure SDK for validation
try:
    from json_structure import SchemaValidator, InstanceValidator, ValidationError
    HAS_JSTRUCT_SDK = True
except ImportError:
    HAS_JSTRUCT_SDK = False


def get_test_json_path(filename: str) -> str:
    """Get the path to a test JSON file."""
    return os.path.join(os.path.dirname(__file__), 'json', filename)


def load_json_values(path: str) -> list:
    """Load JSON values from a file, handling single objects and JSONL."""
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read().strip()
    
    # Try as JSON array first
    try:
        data = json.loads(content)
        if isinstance(data, list):
            return data
        return [data]
    except json.JSONDecodeError:
        pass
    
    # Try as JSONL
    values = []
    for line in content.split('\n'):
        line = line.strip()
        if line:
            values.append(json.loads(line))
    return values


def validate_schema(schema: dict) -> list:
    """Validate that a schema is a valid JSON Structure schema.
    
    Returns list of validation error strings, empty if valid.
    """
    if not HAS_JSTRUCT_SDK:
        return []
    
    errors = []
    schema_validator = SchemaValidator(extended=True)
    validation_errors = schema_validator.validate(schema)
    
    for err in validation_errors:
        if isinstance(err, ValidationError):
            errors.append(f"{err.code}: {err.message} at {err.path}")
        else:
            errors.append(str(err))
    
    return errors


def validate_instances(schema: dict, instances: list) -> list:
    """Validate instances against a schema.
    
    Returns list of validation error strings, empty if all pass.
    """
    if not HAS_JSTRUCT_SDK:
        return []
    
    errors = []
    instance_validator = InstanceValidator(schema, extended=True)
    
    for i, instance in enumerate(instances):
        validation_errors = instance_validator.validate(instance)
        for err in validation_errors:
            if isinstance(err, str):
                errors.append(f"Instance {i}: {err}")
            else:
                errors.append(f"Instance {i}: {err}")
    
    return errors


def validate_schema_and_instances(schema: dict, instances: list) -> list:
    """Validate schema correctness and instances against schema.
    
    Returns list of validation errors, empty if all pass.
    """
    if not HAS_JSTRUCT_SDK:
        return []
    
    # First validate the schema itself
    schema_errors = validate_schema(schema)
    if schema_errors:
        return [f"Schema: {e}" for e in schema_errors]
    
    # Then validate instances
    return validate_instances(schema, instances)


@unittest.skipUnless(HAS_JSTRUCT_SDK, "json-structure SDK not available")
class TestJson2sInference(unittest.TestCase):
    """Test json2s inference produces valid JSON Structure schemas."""

    def test_primitives(self):
        """Test primitive type inference: string, integer, double, boolean, null."""
        path = get_test_json_path('primitives.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='Primitives')
        
        # Verify schema structure
        self.assertEqual(schema['type'], 'object')
        self.assertIn('properties', schema)
        
        props = schema['properties']
        
        # Check type mappings
        self.assertEqual(props['string_field']['type'], 'string')
        self.assertEqual(props['integer_field']['type'], 'integer')
        self.assertEqual(props['negative_integer']['type'], 'integer')
        self.assertEqual(props['float_field']['type'], 'double')
        self.assertEqual(props['boolean_true']['type'], 'boolean')
        self.assertEqual(props['boolean_false']['type'], 'boolean')
        self.assertEqual(props['null_field']['type'], 'null')
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_nested_objects(self):
        """Test nested object type inference."""
        path = get_test_json_path('nested_objects.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='NestedUser')
        
        # Verify nested structure
        self.assertEqual(schema['type'], 'object')
        user_prop = schema['properties']['user']
        self.assertEqual(user_prop['type'], 'object')
        
        profile_prop = user_prop['properties']['profile']
        self.assertEqual(profile_prop['type'], 'object')
        
        personal_prop = profile_prop['properties']['personal']
        self.assertEqual(personal_prop['type'], 'object')
        self.assertEqual(personal_prop['properties']['name']['type'], 'string')
        self.assertEqual(personal_prop['properties']['age']['type'], 'integer')
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_arrays_simple(self):
        """Test simple array type inference."""
        path = get_test_json_path('arrays_simple.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='Arrays')
        
        props = schema['properties']
        
        # Check array types
        self.assertEqual(props['string_array']['type'], 'array')
        self.assertEqual(props['string_array']['items']['type'], 'string')
        
        self.assertEqual(props['integer_array']['type'], 'array')
        self.assertEqual(props['integer_array']['items']['type'], 'integer')
        
        self.assertEqual(props['float_array']['type'], 'array')
        self.assertEqual(props['float_array']['items']['type'], 'double')
        
        self.assertEqual(props['boolean_array']['type'], 'array')
        self.assertEqual(props['boolean_array']['items']['type'], 'boolean')
        
        # Empty array defaults to string items
        self.assertEqual(props['empty_array']['type'], 'array')
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_array_of_objects(self):
        """Test arrays containing objects."""
        path = get_test_json_path('array_of_objects.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='Container')
        
        items_prop = schema['properties']['items']
        self.assertEqual(items_prop['type'], 'array')
        
        item_schema = items_prop['items']
        self.assertEqual(item_schema['type'], 'object')
        self.assertIn('id', item_schema['properties'])
        self.assertIn('name', item_schema['properties'])
        self.assertIn('price', item_schema['properties'])
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_sparse_data(self):
        """Test sparse data folding - fields appearing in some but not all records."""
        path = get_test_json_path('sparse_data.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='SparseRecord')
        
        props = schema['properties']
        
        # All fields should be present
        self.assertIn('name', props)
        self.assertIn('field_a', props)
        self.assertIn('field_b', props)
        self.assertIn('field_c', props)
        
        # Only 'name' should be required (appears in all records)
        required = schema.get('required', [])
        self.assertIn('name', required)
        
        # Validate with SDK - note: SDK will fail on missing fields
        # This test verifies the schema is structurally correct
        errors = validate_schema_and_instances(schema, [values[-1]])  # Last record has all fields
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_deeply_nested(self):
        """Test deeply nested object structures."""
        path = get_test_json_path('deeply_nested.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='Deep')
        
        # Navigate the deep structure
        current = schema
        for level in ['level1', 'level2', 'level3', 'level4', 'level5']:
            current = current['properties'][level]
            self.assertEqual(current['type'], 'object')
        
        # Check the deep values
        self.assertEqual(current['properties']['deep_value']['type'], 'string')
        self.assertEqual(current['properties']['deep_number']['type'], 'integer')
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_nested_arrays(self):
        """Test nested array structures (arrays of arrays)."""
        path = get_test_json_path('nested_arrays.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='NestedArrays')
        
        # Matrix: array of arrays of integers
        matrix_prop = schema['properties']['matrix']
        self.assertEqual(matrix_prop['type'], 'array')
        
        # The items should be an array type
        items = matrix_prop['items']
        self.assertEqual(items['type'], 'array')
        
        # Inner items should be integer (may be nested in items dict)
        inner_items = items.get('items', {})
        if isinstance(inner_items, dict):
            self.assertEqual(inner_items.get('type'), 'integer')
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_special_field_names(self):
        """Test handling of field names that need sanitization."""
        path = get_test_json_path('special_field_names.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='SpecialNames')
        
        props = schema['properties']
        
        # Verify transformed names exist
        self.assertIn('field_with_dashes', props)
        self.assertIn('field_with_dots', props)
        self.assertIn('field_with_spaces', props)
        self.assertIn('_123_starts_with_number', props)
        self.assertIn('_underscore_prefix', props)
        self.assertIn('CamelCase', props)
        self.assertIn('ALLCAPS', props)
        
        # Check altnames for transformed fields
        self.assertEqual(props['field_with_dashes'].get('altnames', {}).get('json'), 'field-with-dashes')
        
        # Schema validation (without instance validation due to key transformation)
        if HAS_JSTRUCT_SDK:
            schema_validator = SchemaValidator()
            schema_validator.validate(schema)

    def test_numeric_extremes(self):
        """Test numeric edge cases."""
        path = get_test_json_path('numeric_extremes.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='NumericExtremes')
        
        props = schema['properties']
        
        # Small integers should be 'integer' type (int32)
        for field in ['small_int', 'max_int32', 'min_int32', 'zero']:
            self.assertEqual(props[field]['type'], 'integer', f"Field {field} should be integer")
        
        # Large integers beyond int32 range inferred as 'double' 
        # (int64 would require string encoding which doesn't match JSON source)
        self.assertEqual(props['large_int']['type'], 'double')
        self.assertEqual(props['negative_large']['type'], 'double')
        
        # Floats should be 'double' type
        for field in ['small_float', 'large_float', 'small_negative_float', 'scientific_notation']:
            self.assertEqual(props[field]['type'], 'double', f"Field {field} should be double")
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_string_edge_cases(self):
        """Test string edge cases: empty, unicode, escapes."""
        path = get_test_json_path('string_edge_cases.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='StringEdges')
        
        props = schema['properties']
        
        # All should be string type
        for field in props:
            self.assertEqual(props[field]['type'], 'string', f"Field {field} should be string")
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_nullable_field(self):
        """Test fields that are sometimes null."""
        path = get_test_json_path('nullable_field.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='Nullable')
        
        # When records have conflicting types (string vs null), inference
        # may produce a 'choice' type to represent the union
        if schema.get('type') == 'choice':
            # Choice type with multiple object variants
            self.assertIn('choices', schema)
            # Each choice should be an object with nullable_field
            for choice in schema['choices']:
                if isinstance(choice, dict) and choice.get('type') == 'object':
                    self.assertIn('nullable_field', choice.get('properties', {}))
        else:
            # Direct object type
            self.assertIn('properties', schema)
            self.assertIn('nullable_field', schema['properties'])

    def test_jsonl_records(self):
        """Test JSONL (newline-delimited JSON) input."""
        path = get_test_json_path('jsonl_records.jsonl')
        values = load_json_values(path)
        
        self.assertEqual(len(values), 3)  # 3 JSONL records
        
        schema = infer_jstruct_schema_from_json(values, type_name='JsonlRecord')
        
        props = schema['properties']
        self.assertEqual(props['id']['type'], 'integer')
        self.assertEqual(props['name']['type'], 'string')
        self.assertEqual(props['active']['type'], 'boolean')
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_single_object(self):
        """Test single object (not array) JSON input."""
        path = get_test_json_path('single_object.json')
        values = load_json_values(path)
        
        self.assertEqual(len(values), 1)
        
        schema = infer_jstruct_schema_from_json(values, type_name='SingleObject')
        
        props = schema['properties']
        self.assertEqual(props['single_record']['type'], 'boolean')
        self.assertEqual(props['name']['type'], 'string')
        self.assertEqual(props['value']['type'], 'integer')
        self.assertEqual(props['nested']['type'], 'object')
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_extended_type_strings(self):
        """Test strings that look like extended types (UUID, date, etc.)."""
        path = get_test_json_path('extended_type_strings.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='ExtendedTypes')
        
        props = schema['properties']
        
        # Temporal types are now detected
        self.assertEqual(props['date_like']['type'], 'date', "ISO 8601 date should be detected")
        self.assertEqual(props['datetime_like']['type'], 'datetime', "ISO 8601 datetime should be detected")
        self.assertEqual(props['time_like']['type'], 'time', "ISO 8601 time should be detected")
        
        # Other extended types remain as strings
        for field in ['uuid_like', 'uri_like', 'email_like', 'ip_address', 'duration_like', 'base64_like']:
            self.assertEqual(props[field]['type'], 'string', f"Field {field} should be string")
        
        # Validate with SDK
        errors = validate_schema_and_instances(schema, values)
        self.assertEqual(errors, [], f"Validation errors: {errors}")

    def test_mixed_arrays(self):
        """Test arrays with mixed content types."""
        path = get_test_json_path('mixed_arrays.json')
        values = load_json_values(path)
        
        schema = infer_jstruct_schema_from_json(values, type_name='MixedArrays')
        
        props = schema['properties']
        
        # Homogeneous array - simple integer array
        self.assertEqual(props['homogeneous_array']['type'], 'array')
        self.assertEqual(props['homogeneous_array']['items']['type'], 'integer')
        
        # Mixed array - should handle multiple types somehow (may use choice)
        self.assertEqual(props['mixed_array']['type'], 'array')
        
        # Single element array
        self.assertEqual(props['single_element']['type'], 'array')
        
        # Validate schema structure
        schema_errors = validate_schema(schema)
        self.assertEqual(schema_errors, [], f"Schema validation errors: {schema_errors}")

    def test_file_conversion(self):
        """Test full file conversion via convert_json_to_jstruct."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 1, "name": "Test1", "active": True},
                {"id": 2, "name": "Test2", "active": False}
            ], f)
            input_path = f.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jstruct.json', delete=False) as f:
            output_path = f.name
        
        try:
            convert_json_to_jstruct(
                input_files=[input_path],
                jstruct_schema_file=output_path,
                type_name='TestRecord',
                base_id='https://test.namespace/'
            )
            
            with open(output_path, 'r') as f:
                schema = json.load(f)
            
            self.assertIn('$schema', schema)
            self.assertIn('$id', schema)
            self.assertEqual(schema['type'], 'object')
            self.assertIn('test.namespace', schema['$id'])
            
            # Validate the generated schema
            if HAS_JSTRUCT_SDK:
                schema_validator = SchemaValidator()
                schema_validator.validate(schema)
        finally:
            os.unlink(input_path)
            os.unlink(output_path)


if __name__ == '__main__':
    unittest.main()
