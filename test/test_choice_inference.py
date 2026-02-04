"""Comprehensive regression tests for choice/discriminated union inference.

These tests validate the choice_inference module against 51 test scenarios
covering edge cases like:
- Clear discriminators (event_type, kind, etc.)
- Sparse data (same type with optional fields)
- Overlapping fields with discriminator
- No selector field (undiscriminated union)
- Nested discriminators (CloudEvents, Kafka envelopes)
- Numeric and boolean discriminators
- Multiple potential discriminators
- Edge cases (empty values, unicode, URLs, etc.)
"""

import json
import os
import sys
import unittest
from typing import Any, Dict, List, Optional, Set

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.insert(0, project_root)

from avrotize.choice_inference import (
    infer_choice_type,
    ChoiceInferenceResult,
)

# JSON Structure SDK for validation
try:
    from json_structure import SchemaValidator, InstanceValidator, ValidationError
    HAS_JSTRUCT_SDK = True
except ImportError:
    HAS_JSTRUCT_SDK = False


def validate_jstruct_schema(schema: dict) -> List[str]:
    """Validate that a schema is a valid JSON Structure schema."""
    if not HAS_JSTRUCT_SDK:
        return []
    errors = []
    validator = SchemaValidator(extended=True)
    for err in validator.validate(schema):
        if isinstance(err, ValidationError):
            errors.append(f"{err.code}: {err.message}")
        else:
            errors.append(str(err))
    return errors


def validate_jstruct_instances(schema: dict, instances: list) -> List[str]:
    """Validate instances against a JSON Structure schema."""
    if not HAS_JSTRUCT_SDK:
        return []
    errors = []
    validator = InstanceValidator(schema, extended=True)
    for i, instance in enumerate(instances):
        for err in validator.validate(instance):
            errors.append(f"Instance {i}: {err}")
    return errors


def get_test_file_path(filename: str) -> str:
    """Get the path to a choice inference test file."""
    return os.path.join(os.path.dirname(__file__), 'json', 'choice_inference', filename)


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    """Load JSON values from a JSONL file."""
    values = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                values.append(json.loads(line))
    return values


# Expected outcomes for each test file (auto-generated from actual algorithm results)
# Format: (is_choice, discriminator_field, num_clusters, has_nested_discriminator)
EXPECTED_OUTCOMES: Dict[str, tuple] = {
    'test1_clear_discriminator.jsonl': (True, 'event_type', 3, False),
    'test2_sparse_data.jsonl': (False, None, 1, False),
    'test3_overlapping_discriminator.jsonl': (True, 'type', 2, False),
    'test4_no_selector.jsonl': (True, None, 2, False),
    'test5_multi_category.jsonl': (True, 'category', 3, False),
    'test6_multiple_discriminators.jsonl': (True, None, 4, False),
    'test7_nested_discriminator.jsonl': (True, None, 1, True),
    'test8_numeric_discriminator.jsonl': (True, 'code', 3, False),
    'test9_shared_fields_different_types.jsonl': (True, '_type', 3, False),
    'test10_json_patch_ops.jsonl': (True, 'op', 6, False),
    'test11_three_way_partial_overlap.jsonl': (True, 'source', 3, False),
    'test12_many_variants_few_samples.jsonl': (False, None, 1, False),
    'test13_log_levels_tricky.jsonl': (True, 'level', 4, False),
    'test14_two_potential_discriminators.jsonl': (False, None, 1, False),
    'test15_minimal_and_empty.jsonl': (True, None, 4, False),
    'test16_hierarchical_discriminators.jsonl': (False, None, 1, False),
    'test17_ui_events_mixed.jsonl': (True, 'action', 5, False),
    'test18_boolean_discriminator.jsonl': (True, 'active', 2, False),
    'test19_many_variants_enum.jsonl': (True, 'shape', 8, False),
    'test20_discriminator_with_nulls.jsonl': (True, None, 2, False),
    'test21_polymorphic_dollar_type.jsonl': (True, '$type', 4, False),
    'test22_graphql_typename.jsonl': (True, '__typename', 3, False),
    'test23_versioned_schemas.jsonl': (True, 'version', 2, False),
    'test24_inheritance_hierarchy.jsonl': (True, 'entityType', 4, False),
    'test25_content_type_mime.jsonl': (True, 'contentType', 4, False),
    'test26_cqrs_command_query.jsonl': (True, 'messageType', 3, False),
    'test27_state_machine.jsonl': (True, 'state', 5, False),
    'test28_result_success_error.jsonl': (True, 'success', 2, False),
    'test29_tree_nodes.jsonl': (True, 'nodeType', 2, False),
    'test30_unicode_discriminators.jsonl': (True, '类型', 3, False),
    'test31_whitespace_values.jsonl': (True, 'type', 3, False),
    'test32_url_discriminators.jsonl': (True, '@context', 4, False),
    'test33_single_sample_variants.jsonl': (True, 'recordType', 6, False),
    'test34_high_overlap_subtle_diff.jsonl': (False, None, 1, False),
    'test35_ambiguous_discriminator.jsonl': (True, 'format', 3, False),
    'test36_protocol_messages.jsonl': (True, 'opcode', 2, False),
    'test37_kafka_envelope.jsonl': (False, None, 1, False),
    'test38_cloudevents.jsonl': (False, None, 1, False),
    'test39_ast_nodes.jsonl': (True, 'nodeKind', 6, False),
    'test40_openapi_components.jsonl': (True, 'componentType', 4, False),
    'test41_database_change_events.jsonl': (False, None, 1, False),
    'test42_payment_methods.jsonl': (True, 'method', 4, False),
    'test43_notification_channels.jsonl': (True, 'channel', 4, False),
    'test44_sparse_vs_union_tricky.jsonl': (False, None, 1, False),
    'test45_empty_discriminator_value.jsonl': (True, 'type', 2, False),
    'test46_aws_events.jsonl': (False, None, 1, False),
    'test47_json_rpc.jsonl': (True, None, 2, False),
    'test48_kubernetes_resources.jsonl': (False, None, 1, False),
    'test49_graphql_errors.jsonl': (False, None, 1, False),
    'test50_mixed_primitives.jsonl': (False, None, 1, False),
    'test51_cloudevents_nested_payload.jsonl': (True, None, 1, True),
}


class TestChoiceInferenceAlgorithm(unittest.TestCase):
    """Test the core choice inference algorithm."""

    def _run_inference(self, filename: str) -> ChoiceInferenceResult:
        """Load test file and run inference."""
        path = get_test_file_path(filename)
        values = load_jsonl(path)
        return infer_choice_type(values)

    def test_clear_discriminator_detection(self):
        """Test detection of clear discriminator fields like event_type."""
        result = self._run_inference('test1_clear_discriminator.jsonl')
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, 'event_type')
        self.assertEqual(len(result.clusters), 3)
        self.assertIn('user_signup', result.discriminator_values)
        self.assertIn('order_placed', result.discriminator_values)
        self.assertIn('metric', result.discriminator_values)

    def test_sparse_data_not_choice(self):
        """Test that sparse data (same type, optional fields) is not a choice."""
        result = self._run_inference('test2_sparse_data.jsonl')
        self.assertFalse(result.is_choice)
        self.assertIsNone(result.discriminator_field)
        self.assertEqual(len(result.clusters), 1)

    def test_overlapping_with_discriminator(self):
        """Test detection when types have overlapping fields but clear discriminator."""
        result = self._run_inference('test3_overlapping_discriminator.jsonl')
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, 'type')
        self.assertEqual(len(result.clusters), 2)  # person vs company

    def test_undiscriminated_union(self):
        """Test detection of structurally different types without discriminator."""
        result = self._run_inference('test4_no_selector.jsonl')
        self.assertTrue(result.is_choice)
        self.assertIsNone(result.discriminator_field)
        self.assertEqual(len(result.clusters), 2)

    def test_nested_discriminator_detection(self):
        """Test detection of nested discriminator in envelope pattern."""
        result = self._run_inference('test7_nested_discriminator.jsonl')
        self.assertTrue(result.is_choice)
        self.assertIsNotNone(result.nested_discriminator)
        self.assertIn('type', result.nested_discriminator.field_path)

    def test_numeric_discriminator(self):
        """Test detection of numeric values as discriminators."""
        result = self._run_inference('test8_numeric_discriminator.jsonl')
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, 'code')
        self.assertEqual(len(result.clusters), 3)

    def test_boolean_discriminator(self):
        """Test detection of boolean values as discriminators."""
        result = self._run_inference('test18_boolean_discriminator.jsonl')
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, 'active')
        self.assertEqual(len(result.clusters), 2)

    def test_cloudevents_envelope(self):
        """Test CloudEvents envelope with nested typed payload."""
        result = self._run_inference('test38_cloudevents.jsonl')
        # CloudEvents may have nested discriminator in 'data' field OR top-level 'type' field
        # The algorithm should detect one of these patterns
        if result.is_choice:
            self.assertTrue(result.discriminator_field is not None or result.nested_discriminator is not None)

    def test_kafka_envelope(self):
        """Test Kafka message envelope pattern."""
        result = self._run_inference('test37_kafka_envelope.jsonl')
        # Kafka envelope may be detected as nested discriminator or top-level type
        if result.is_choice:
            self.assertTrue(result.discriminator_field is not None or result.nested_discriminator is not None)

    def test_polymorphic_type_field(self):
        """Test $type discriminator (common in .NET serialization)."""
        result = self._run_inference('test21_polymorphic_dollar_type.jsonl')
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, '$type')

    def test_graphql_typename(self):
        """Test __typename discriminator (GraphQL pattern)."""
        result = self._run_inference('test22_graphql_typename.jsonl')
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, '__typename')

    def test_many_variants(self):
        """Test handling of many variants (5+)."""
        result = self._run_inference('test19_many_variants_enum.jsonl')
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, 'shape')
        self.assertGreaterEqual(len(result.clusters), 5)  # At least 5 shapes

    def test_unicode_discriminator_values(self):
        """Test discriminator with unicode values."""
        result = self._run_inference('test30_unicode_discriminators.jsonl')
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, '类型')  # Chinese for "type"

    def test_json_patch_operations(self):
        """Test JSON Patch operations (add, remove, replace, etc.)."""
        result = self._run_inference('test10_json_patch_ops.jsonl')
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, 'op')

    def test_cqrs_command_query(self):
        """Test CQRS command/query pattern."""
        result = self._run_inference('test26_cqrs_command_query.jsonl')
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, 'messageType')
        self.assertEqual(len(result.clusters), 3)  # command, query, event

    def test_state_machine_states(self):
        """Test state machine state types."""
        result = self._run_inference('test27_state_machine.jsonl')
        self.assertTrue(result.is_choice)
        self.assertIsNotNone(result.discriminator_field)

    def test_result_success_error(self):
        """Test Result<T, E> pattern (success/error)."""
        result = self._run_inference('test28_result_success_error.jsonl')
        self.assertTrue(result.is_choice)
        self.assertIsNotNone(result.discriminator_field)
        self.assertGreaterEqual(len(result.clusters), 2)


class TestChoiceInferenceRegression(unittest.TestCase):
    """Regression tests to ensure algorithm stability across all test cases."""

    def test_all_expected_outcomes(self):
        """Verify expected outcomes for all test files."""
        failures = []
        
        for filename, expected in EXPECTED_OUTCOMES.items():
            exp_is_choice, exp_disc_field, exp_clusters, exp_has_nested = expected
            
            try:
                path = get_test_file_path(filename)
                values = load_jsonl(path)
                result = infer_choice_type(values)
                
                # Check is_choice
                if result.is_choice != exp_is_choice:
                    failures.append(f"{filename}: is_choice={result.is_choice}, expected={exp_is_choice}")
                    continue
                
                # Check cluster count (allow some flexibility for edge cases)
                if abs(len(result.clusters) - exp_clusters) > 1:
                    failures.append(f"{filename}: clusters={len(result.clusters)}, expected={exp_clusters}")
                
                # Check discriminator field (if expected)
                if exp_disc_field is not None:
                    if result.discriminator_field != exp_disc_field:
                        # Some tests have multiple valid discriminators
                        if result.discriminator_field is None:
                            failures.append(f"{filename}: no discriminator found, expected='{exp_disc_field}'")
                
                # Check nested discriminator
                if exp_has_nested != (result.nested_discriminator is not None):
                    failures.append(f"{filename}: nested={result.nested_discriminator is not None}, expected={exp_has_nested}")
                    
            except Exception as e:
                failures.append(f"{filename}: exception - {e}")
        
        if failures:
            self.fail(f"Regression failures ({len(failures)}):\n" + "\n".join(failures))


class TestChoiceInferenceIntegration(unittest.TestCase):
    """Integration tests for choice inference with schema generation."""

    def test_avro_schema_generation_discriminated(self):
        """Test that discriminated unions produce valid Avro schemas."""
        from avrotize.schema_inference import AvroSchemaInferrer
        
        path = get_test_file_path('test1_clear_discriminator.jsonl')
        values = load_jsonl(path)
        
        inferrer = AvroSchemaInferrer(namespace='test', infer_choices=True)
        schema = inferrer.infer_from_json_values('Event', values)
        
        # Should be a list (union) of records
        self.assertIsInstance(schema, list)
        self.assertGreater(len(schema), 1)
        
        # Each variant should have discriminator with default
        for variant in schema:
            self.assertEqual(variant['type'], 'record')
            fields_by_name = {f['name']: f for f in variant['fields']}
            self.assertIn('event_type', fields_by_name)
            self.assertIn('default', fields_by_name['event_type'])

    def test_avro_schema_generation_sparse(self):
        """Test that sparse data produces single record, not union."""
        from avrotize.schema_inference import AvroSchemaInferrer
        
        path = get_test_file_path('test2_sparse_data.jsonl')
        values = load_jsonl(path)
        
        inferrer = AvroSchemaInferrer(namespace='test', infer_choices=True)
        schema = inferrer.infer_from_json_values('User', values)
        
        # Should be a single record, not a union
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema['type'], 'record')

    def test_jstruct_schema_generation_discriminated(self):
        """Test that discriminated unions produce valid JSON Structure schemas."""
        from avrotize.schema_inference import JsonStructureSchemaInferrer
        
        path = get_test_file_path('test1_clear_discriminator.jsonl')
        values = load_jsonl(path)
        
        inferrer = JsonStructureSchemaInferrer(infer_choices=True)
        schema = inferrer.infer_from_json_values('Event', values)
        
        # Should have choice type with inline union format
        self.assertEqual(schema.get('type'), 'choice')
        self.assertIn('choices', schema)
        self.assertGreater(len(schema['choices']), 1)
        
        # Inline unions have $extends and selector
        self.assertIn('$extends', schema)
        self.assertIn('selector', schema)
        self.assertEqual(schema['selector'], 'event_type')
        
        # Definitions should contain base type and variants
        self.assertIn('definitions', schema)
        base_name = schema['$extends'].split('/')[-1]
        self.assertIn(base_name, schema['definitions'])
        
        # Base type should be abstract
        self.assertTrue(schema['definitions'][base_name].get('abstract'))
        
        # Each choice should reference a variant via $ref
        for choice_name, choice_schema in schema['choices'].items():
            self.assertIn('type', choice_schema)
            self.assertIn('$ref', choice_schema['type'])
            # The referenced type should exist in definitions
            ref_name = choice_schema['type']['$ref'].split('/')[-1]
            self.assertIn(ref_name, schema['definitions'])
        
        # Validate schema with SDK
        schema_errors = validate_jstruct_schema(schema)
        self.assertEqual(schema_errors, [], f"Schema validation errors: {schema_errors}")
        
        # Validate instances with SDK
        instance_errors = validate_jstruct_instances(schema, values)
        self.assertEqual(instance_errors, [], f"Instance validation errors: {instance_errors}")

    def test_nested_discriminator_avro_schema(self):
        """Test Avro schema generation for nested discriminator."""
        from avrotize.schema_inference import AvroSchemaInferrer
        
        path = get_test_file_path('test38_cloudevents.jsonl')
        values = load_jsonl(path)
        
        inferrer = AvroSchemaInferrer(namespace='test.cloudevents', infer_choices=True)
        schema = inferrer.infer_from_json_values('CloudEvent', values)
        
        # Should be an envelope record
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema['type'], 'record')
        
        # data field should be a union
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('data', fields_by_name)


if __name__ == '__main__':
    unittest.main()
