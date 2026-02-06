"""Comprehensive tests for json2a inference with 'evil' edge cases.

These tests validate that json2a produces correct Avro schemas
that can validate the original input data using our Avro validator.
"""

import json
import os
import sys
import tempfile
import unittest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.jsontoschema import convert_json_to_avro
from avrotize.schema_inference import infer_avro_schema_from_json
from avrotize.avrovalidator import validate_json_against_avro


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


def validate_avro_schema_and_instances(schema: dict, instances: list) -> list:
    """Validate schema correctness and instances against Avro schema.
    
    Returns list of validation errors, empty if all pass.
    """
    errors = []
    
    # Validate each instance
    for i, instance in enumerate(instances):
        instance_errors = validate_json_against_avro(instance, schema)
        if instance_errors:
            errors.append(f"Instance {i} validation failed: {instance_errors}")
    
    return errors


class TestJson2aInference(unittest.TestCase):
    """Test that json2a produces Avro schemas that validate the original JSON."""

    def assert_schema_validates_instances(self, json_file: str, type_name: str = 'Document'):
        """Helper to test that inferred schema validates original instances."""
        path = get_test_json_path(json_file)
        instances = load_json_values(path)
        
        # Infer Avro schema
        schema = infer_avro_schema_from_json(
            instances, 
            type_name=type_name,
            namespace='test.inference'
        )
        
        # Validate all instances against the schema
        errors = validate_avro_schema_and_instances(schema, instances)
        self.assertEqual(errors, [], f"Validation errors for {json_file}: {errors}")
        
        return schema

    def test_primitives(self):
        """Test primitive type inference and validation."""
        schema = self.assert_schema_validates_instances('primitives.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('string_field', fields_by_name)
        self.assertIn('integer_field', fields_by_name)
        self.assertIn('float_field', fields_by_name)
        self.assertIn('boolean_true', fields_by_name)
        self.assertIn('null_field', fields_by_name)

    def test_nested_objects(self):
        """Test nested object inference and validation."""
        schema = self.assert_schema_validates_instances('nested_objects.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('user', fields_by_name)
        
        # user should be a nested record
        user_type = fields_by_name['user']['type']
        if isinstance(user_type, list):
            # It's a union, find the record type
            user_type = next(t for t in user_type if isinstance(t, dict) and t.get('type') == 'record')
        self.assertEqual(user_type['type'], 'record')

    def test_array_of_objects(self):
        """Test array of objects inference and validation."""
        schema = self.assert_schema_validates_instances('array_of_objects.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('items', fields_by_name)
        
        # items should be an array type
        items_type = fields_by_name['items']['type']
        if isinstance(items_type, list):
            items_type = next(t for t in items_type if isinstance(t, dict))
        self.assertEqual(items_type['type'], 'array')

    def test_sparse_data(self):
        """Test sparse data folding and validation."""
        schema = self.assert_schema_validates_instances('sparse_data.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        # All fields from any record should be present
        self.assertIn('name', fields_by_name)
        self.assertIn('field_a', fields_by_name)
        self.assertIn('field_b', fields_by_name)
        self.assertIn('field_c', fields_by_name)
        # Fields not present in all records should be nullable
        self.assertIsInstance(fields_by_name['field_a']['type'], list)
        self.assertIn('null', fields_by_name['field_a']['type'])

    def test_mixed_arrays(self):
        """Test mixed array type inference and validation."""
        schema = self.assert_schema_validates_instances('mixed_arrays.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('mixed_array', fields_by_name)

    def test_special_field_names(self):
        """Test special field name handling and validation."""
        schema = self.assert_schema_validates_instances('special_field_names.json')
        
        # Check altnames are set for fields that needed sanitization
        for field in schema['fields']:
            if 'altnames' in field:
                self.assertIn('json', field['altnames'])

    def test_numeric_extremes(self):
        """Test numeric extremes inference and validation."""
        schema = self.assert_schema_validates_instances('numeric_extremes.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('small_int', fields_by_name)
        self.assertIn('large_int', fields_by_name)
        self.assertIn('large_float', fields_by_name)
        self.assertIn('scientific_notation', fields_by_name)

    def test_string_edge_cases(self):
        """Test string edge cases including unicode and validation."""
        schema = self.assert_schema_validates_instances('string_edge_cases.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('unicode_string', fields_by_name)
        self.assertIn('empty_string', fields_by_name)
        self.assertIn('special_chars', fields_by_name)

    def test_empty_objects(self):
        """Test empty object inference and validation."""
        schema = self.assert_schema_validates_instances('empty_objects.json')
        
        # File contains objects with varying fields - should fold to common schema
        self.assertEqual(schema['type'], 'record')

    def test_deeply_nested(self):
        """Test deeply nested structure inference and validation."""
        schema = self.assert_schema_validates_instances('deeply_nested.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('level1', fields_by_name)

    def test_nested_arrays(self):
        """Test nested array inference and validation."""
        schema = self.assert_schema_validates_instances('nested_arrays.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('matrix', fields_by_name)

    def test_arrays_simple(self):
        """Test simple array inference and validation."""
        schema = self.assert_schema_validates_instances('arrays_simple.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('string_array', fields_by_name)
        self.assertIn('integer_array', fields_by_name)

    def test_nullable_field(self):
        """Test nullable field inference and validation."""
        schema = self.assert_schema_validates_instances('nullable_field.json')
        
        # Schema should be a single record with nullable field
        self.assertEqual(schema['type'], 'record')
        fields_by_name = {f['name']: f for f in schema['fields']}
        # Nullable fields should be unions containing null
        nullable_type = fields_by_name['nullable_field']['type']
        self.assertIsInstance(nullable_type, list)
        self.assertIn('null', nullable_type)

    def test_single_object(self):
        """Test single object inference and validation."""
        schema = self.assert_schema_validates_instances('single_object.json')
        
        self.assertEqual(schema['type'], 'record')

    def test_jsonl_records(self):
        """Test JSONL file inference and validation."""
        schema = self.assert_schema_validates_instances('jsonl_records.jsonl')
        
        self.assertEqual(schema['type'], 'record')

    def test_extended_type_strings(self):
        """Test extended type strings (dates, etc.) inference and validation."""
        schema = self.assert_schema_validates_instances('extended_type_strings.json')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        # Check that date/time fields are present
        self.assertTrue(any('date' in name.lower() or 'time' in name.lower() 
                           for name in fields_by_name.keys()))


class TestJson2aFileConversion(unittest.TestCase):
    """Test the full file conversion flow for json2a."""

    def test_convert_json_file_to_avro(self):
        """Test converting a JSON file to Avro schema and validating."""
        json_path = get_test_json_path('primitives.json')
        
        # Load the original data from the file (not flattened)
        with open(json_path, 'r') as f:
            original_data = json.load(f)
        
        # Wrap the data as a single instance for validation
        instances = [original_data]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc', delete=False) as f:
            output_path = f.name
        
        try:
            convert_json_to_avro(
                input_files=[json_path],
                avro_schema_file=output_path,
                type_name='Primitives',
                avro_namespace='test.file'
            )
            
            with open(output_path, 'r') as f:
                schema = json.load(f)
            
            errors = validate_avro_schema_and_instances(schema, instances)
            self.assertEqual(errors, [], f"Validation errors: {errors}")
        finally:
            os.unlink(output_path)


class TestJson2aChoiceInference(unittest.TestCase):
    """Test the --infer-choices feature for discriminated unions."""

    def test_discriminated_union_inference(self):
        """Test that infer_choices detects discriminated unions."""
        json_path = get_test_json_path('choice_inference/test1_clear_discriminator.jsonl')
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc', delete=False) as f:
            output_path = f.name
        
        try:
            convert_json_to_avro(
                input_files=[json_path],
                avro_schema_file=output_path,
                type_name='Event',
                avro_namespace='test.choice',
                infer_choices=True
            )
            
            with open(output_path, 'r') as f:
                schema = json.load(f)
            
            # Should be a union of records (list)
            self.assertIsInstance(schema, list)
            self.assertGreater(len(schema), 1)
            
            # Each variant should have the discriminator field with default
            for variant in schema:
                self.assertEqual(variant['type'], 'record')
                fields_by_name = {f['name']: f for f in variant['fields']}
                self.assertIn('event_type', fields_by_name)
                self.assertIn('default', fields_by_name['event_type'])
        finally:
            os.unlink(output_path)

    def test_nested_discriminator_inference(self):
        """Test that infer_choices detects nested discriminators (envelope pattern)."""
        json_path = get_test_json_path('choice_inference/test7_nested_discriminator.jsonl')
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc', delete=False) as f:
            output_path = f.name
        
        try:
            convert_json_to_avro(
                input_files=[json_path],
                avro_schema_file=output_path,
                type_name='Envelope',
                avro_namespace='test.nested',
                infer_choices=True
            )
            
            with open(output_path, 'r') as f:
                schema = json.load(f)
            
            # Should be a record with payload field as union
            self.assertEqual(schema['type'], 'record')
            fields_by_name = {f['name']: f for f in schema['fields']}
            
            self.assertIn('payload', fields_by_name)
            self.assertIn('version', fields_by_name)
            
            # payload should be a union of records
            payload_type = fields_by_name['payload']['type']
            self.assertIsInstance(payload_type, list)
            
            # Each variant should have type field with default
            for variant in payload_type:
                self.assertEqual(variant['type'], 'record')
                variant_fields = {f['name']: f for f in variant['fields']}
                self.assertIn('type', variant_fields)
                self.assertIn('default', variant_fields['type'])
        finally:
            os.unlink(output_path)

    def test_no_choice_inference_by_default(self):
        """Test that choice inference is disabled by default."""
        json_path = get_test_json_path('choice_inference/test1_clear_discriminator.jsonl')
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc', delete=False) as f:
            output_path = f.name
        
        try:
            convert_json_to_avro(
                input_files=[json_path],
                avro_schema_file=output_path,
                type_name='Event',
                avro_namespace='test.nonchoice',
                infer_choices=False  # Default
            )
            
            with open(output_path, 'r') as f:
                schema = json.load(f)
            
            # Without infer_choices, should be single merged record
            self.assertEqual(schema['type'], 'record')
            fields_by_name = {f['name']: f for f in schema['fields']}
            
            # event_type should NOT have a default (it's a regular field)
            self.assertIn('event_type', fields_by_name)
            self.assertNotIn('default', fields_by_name['event_type'])
        finally:
            os.unlink(output_path)


if __name__ == '__main__':
    unittest.main()
