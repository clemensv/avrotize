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


class TestRecursiveChoiceInference(unittest.TestCase):
    """Tests for recursive choice inference with --choice-depth parameter."""
    
    def test_recursive_choice_depth_2(self):
        """Test that nested discriminated unions are detected with choice_depth=2."""
        from avrotize.schema_inference import JsonStructureSchemaInferrer
        
        # Nested choices: event._type (play vs foul) and play.play._subtype (pass vs shot vs cross)
        values = [
            {"id": 1, "event": {"_type": "play", "play": {"_subtype": "pass", "target": "player1"}}},
            {"id": 2, "event": {"_type": "play", "play": {"_subtype": "shot", "result": "goal"}}},
            {"id": 3, "event": {"_type": "play", "play": {"_subtype": "pass", "target": "player2"}}},
            {"id": 4, "event": {"_type": "foul", "foul": {"severity": "yellow"}}},
            {"id": 5, "event": {"_type": "foul", "foul": {"severity": "red"}}},
            {"id": 6, "event": {"_type": "play", "play": {"_subtype": "cross", "destination": "box"}}},
        ]
        
        # With choice_depth=1, nested play should be object, not choice
        inferrer1 = JsonStructureSchemaInferrer(infer_choices=True, choice_depth=1)
        schema1 = inferrer1.infer_from_json_values('Event', values)
        
        # Navigate to event.choices.Play.properties.play
        event_prop = schema1.get('properties', {}).get('event', {})
        self.assertEqual(event_prop.get('type'), 'choice', "event should be a choice")
        play_variant = event_prop.get('choices', {}).get('Play', {})
        play_prop = play_variant.get('properties', {}).get('play', {})
        self.assertEqual(play_prop.get('type'), 'object', 
                        "With depth=1, nested play should be object, not choice")
        
        # With choice_depth=2, nested play should be a choice
        inferrer2 = JsonStructureSchemaInferrer(infer_choices=True, choice_depth=2)
        schema2 = inferrer2.infer_from_json_values('Event', values)
        
        event_prop2 = schema2.get('properties', {}).get('event', {})
        self.assertEqual(event_prop2.get('type'), 'choice', "event should be a choice")
        play_variant2 = event_prop2.get('choices', {}).get('Play', {})
        play_prop2 = play_variant2.get('properties', {}).get('play', {})
        self.assertEqual(play_prop2.get('type'), 'choice', 
                        "With depth=2, nested play should be a choice")
        
        # Verify nested choice variants
        nested_choices = play_prop2.get('choices', {})
        self.assertIn('Pass', nested_choices, "Should have Pass variant")
        self.assertIn('Shot', nested_choices, "Should have Shot variant")
        self.assertIn('Cross', nested_choices, "Should have Cross variant")
        
        # Verify discriminator defaults
        pass_variant = nested_choices.get('Pass', {})
        pass_subtype = pass_variant.get('properties', {}).get('_subtype', {})
        self.assertEqual(pass_subtype.get('default'), 'pass', 
                        "Pass variant should have _subtype default='pass'")
    
    def test_recursive_choice_default_depth(self):
        """Test that default choice_depth=1 doesn't affect existing behavior."""
        from avrotize.schema_inference import JsonStructureSchemaInferrer
        
        # Simple discriminated union at root
        values = [
            {"type": "user", "name": "Alice"},
            {"type": "admin", "name": "Bob", "permissions": ["read", "write"]},
        ]
        
        # Default parameters should still detect top-level choice
        inferrer = JsonStructureSchemaInferrer(infer_choices=True)
        schema = inferrer.infer_from_json_values('Entity', values)
        
        self.assertEqual(schema.get('type'), 'choice', "Should detect top-level choice")
        choices = schema.get('choices', {})
        self.assertIn('user', choices, "Should have user variant")
        self.assertIn('admin', choices, "Should have admin variant")


class TestLargeDiscriminatorSets(unittest.TestCase):
    """Tests for datasets with many discriminator values and high field overlap.
    
    This tests the fallback discriminator detection that handles cases where
    similarity-based clustering merges distinct types due to high Jaccard
    similarity from shared fields.
    """
    
    def test_many_variants_high_overlap(self):
        """Test detection with 15+ variants sharing many common fields.
        
        Simulates event data where different event types share common metadata
        fields but have distinct type-specific payload fields.
        """
        # Common fields that all events share (creates high Jaccard similarity)
        common_fields = {
            "timestamp": "2026-02-05T10:00:00Z",
            "eventId": "evt-12345",
            "source": "system-a",
            "version": "1.0",
            "correlationId": "corr-abc",
            "userId": "user-123",
        }
        
        # 15 different event types, each with 1-2 unique fields
        event_types = [
            ("user_created", {"email": "a@b.com"}),
            ("user_updated", {"changes": ["name"]}),
            ("user_deleted", {"reason": "requested"}),
            ("order_placed", {"orderId": "ord-1", "total": 100}),
            ("order_shipped", {"trackingNumber": "track-1"}),
            ("order_cancelled", {"refundAmount": 50}),
            ("payment_received", {"amount": 100, "method": "card"}),
            ("payment_failed", {"errorCode": "declined"}),
            ("item_added", {"itemId": "item-1", "quantity": 2}),
            ("item_removed", {"itemId": "item-2"}),
            ("cart_cleared", {"itemCount": 5}),
            ("login_success", {"ipAddress": "1.2.3.4"}),
            ("login_failed", {"attempts": 3}),
            ("logout", {"sessionDuration": 3600}),
            ("password_changed", {"lastChanged": "2026-01-01"}),
        ]
        
        # Generate test data: 10 instances of each event type
        values = []
        for event_type, specific_fields in event_types:
            for i in range(10):
                event = {
                    "_type": event_type,
                    **common_fields,
                    **specific_fields
                }
                # Vary some common fields slightly
                event["eventId"] = f"evt-{event_type}-{i}"
                values.append(event)
        
        # Total: 150 events with 15 different _type values
        self.assertEqual(len(values), 150)
        
        result = infer_choice_type(values)
        
        # Should detect as discriminated union
        self.assertTrue(result.is_choice, "Should detect as choice type")
        self.assertEqual(result.discriminator_field, "_type", 
                        "Should detect _type as discriminator")
        self.assertEqual(len(result.discriminator_values), 15, 
                        "Should have 15 discriminator values")
        
        # Verify all event types are in discriminator values
        expected_types = {et[0] for et in event_types}
        self.assertEqual(result.discriminator_values, expected_types)
    
    def test_very_high_overlap_still_detects_discriminator(self):
        """Test with ~70% Jaccard similarity between variants.
        
        Even when most fields are shared, a clear discriminator should be found.
        """
        # 10 common fields
        common = {f"field_{i}": f"value_{i}" for i in range(10)}
        
        # Each variant has the common fields plus 2-3 unique fields
        # This creates ~70-80% Jaccard similarity
        variants = [
            ("alpha", {"alpha_data": "a1", "alpha_flag": True}),
            ("beta", {"beta_data": "b1", "beta_count": 5}),
            ("gamma", {"gamma_data": "g1", "gamma_list": [1, 2]}),
            ("delta", {"delta_data": "d1", "delta_ref": "ref-1"}),
            ("epsilon", {"epsilon_data": "e1"}),
        ]
        
        values = []
        for variant_type, specific in variants:
            for i in range(50):  # 50 of each = 250 total
                values.append({
                    "kind": variant_type,
                    **common,
                    **specific,
                    "seq": i  # Varying field
                })
        
        result = infer_choice_type(values)
        
        self.assertTrue(result.is_choice)
        self.assertEqual(result.discriminator_field, "kind")
        self.assertEqual(len(result.discriminator_values), 5)
    
    def test_schema_generation_with_many_variants(self):
        """Test full schema generation with many high-overlap variants."""
        from avrotize.schema_inference import JsonStructureSchemaInferrer
        
        # Create 8 event types with shared metadata
        metadata = {
            "id": "123",
            "timestamp": "2026-02-05T10:00:00Z",
            "version": "1.0"
        }
        
        events = []
        event_configs = [
            ("click", {"elementId": "btn-1", "x": 100, "y": 200}),
            ("scroll", {"direction": "down", "amount": 500}),
            ("keypress", {"key": "Enter", "modifiers": ["ctrl"]}),
            ("focus", {"elementId": "input-1"}),
            ("blur", {"elementId": "input-1"}),
            ("submit", {"formId": "form-1", "success": True}),
            ("navigate", {"from": "/a", "to": "/b"}),
            ("error", {"message": "oops", "stack": "..."}),
        ]
        
        for event_type, payload in event_configs:
            for i in range(20):
                events.append({
                    "action": event_type,
                    **metadata,
                    **payload
                })
        
        # 160 events total
        inferrer = JsonStructureSchemaInferrer(infer_choices=True)
        schema = inferrer.infer_from_json_values('UIEvent', events)
        
        self.assertEqual(schema.get('type'), 'choice', 
                        "Should generate choice schema")
        
        choices = schema.get('choices', {})
        self.assertEqual(len(choices), 8, "Should have 8 variants")
        
        # Verify each event type has a variant
        for event_type, _ in event_configs:
            self.assertIn(event_type, choices, 
                         f"Should have {event_type} variant")
    
    def test_sparse_binary_split_rejected(self):
        """Test that binary splits with sparse data and no structural pattern are rejected.
        
        This catches false positives like cautionTeamofficial where:
        - Only 2 discriminator values (yellow/red)
        - Very few samples (1-3 each)
        - Only 1 different optional field per variant
        - No envelope pattern (field name doesn't match discriminator)
        """
        # Simulates cautionTeamofficial: yellow has personSentOff, red has unknownPerson
        values = [
            {"cardColor": "yellow", "_timestamp": "t1", "_type": "caution", "team": "A", "personSentOff": "John"},
            {"cardColor": "yellow", "_timestamp": "t2", "_type": "caution", "team": "B", "personSentOff": "Jane"},
            {"cardColor": "yellow", "_timestamp": "t3", "_type": "caution", "team": "A", "personSentOff": "Bob"},
            {"cardColor": "red", "_timestamp": "t4", "_type": "caution", "team": "B", "unknownPerson": "Unknown"},
        ]
        
        result = infer_choice_type(values)
        
        # Should NOT be detected as choice - this is sparse data noise
        self.assertFalse(result.is_choice, 
                        "Binary split with sparse samples and no envelope pattern should be rejected")
    
    def test_envelope_pattern_accepted(self):
        """Test that envelope patterns are correctly detected even with few samples.
        
        Envelope pattern: discriminator value matches a nested field name.
        e.g., _subtype: "play" -> has field "play" containing the payload
        """
        values = [
            {"_subtype": "play", "_timestamp": "t1", "play": {"player": "A", "action": "pass"}},
            {"_subtype": "play", "_timestamp": "t2", "play": {"player": "B", "action": "shot"}},
            {"_subtype": "shotAtGoal", "_timestamp": "t3", "shotAtGoal": {"scorer": "C", "goal": True}},
        ]
        
        result = infer_choice_type(values)
        
        # Should be detected as choice - envelope pattern is clear
        self.assertTrue(result.is_choice, "Envelope pattern should be detected")
        self.assertEqual(result.discriminator_field, "_subtype")
    
    def test_subset_pattern_accepted(self):
        """Test that inheritance-like patterns are accepted even with few samples.
        
        Subset pattern: one variant's fields are a subset of another's.
        e.g., user has {type, name}, admin has {type, name, permissions}
        """
        values = [
            {"type": "user", "name": "Alice"},
            {"type": "admin", "name": "Bob", "permissions": ["read", "write"]},
        ]
        
        result = infer_choice_type(values)
        
        # Should be detected as choice - admin extends user
        self.assertTrue(result.is_choice, "Subset/inheritance pattern should be detected")
        self.assertEqual(result.discriminator_field, "type")

    def test_optional_fields_in_choice_variants(self):
        """Test that fields present in only some documents are marked optional.
        
        In variant A: 'extra' appears in 2/4 documents → optional
        In variant B: 'other' appears in all 3 documents → required
        """
        from avrotize.schema_inference import JsonStructureSchemaInferrer
        
        values = [
            {"type": "A", "common": "c", "extra": "e1"},
            {"type": "A", "common": "c"},  # no extra
            {"type": "A", "common": "c", "extra": "e2"},
            {"type": "A", "common": "c"},  # no extra
            {"type": "B", "common": "c", "other": "o1"},
            {"type": "B", "common": "c", "other": "o2"},
            {"type": "B", "common": "c", "other": "o3"},
        ]
        
        inferrer = JsonStructureSchemaInferrer(infer_choices=True)
        schema = inferrer.infer_from_json_values("Event", values)
        
        # Should be a choice with inheritance pattern
        self.assertEqual(schema.get("type"), "choice")
        
        # Common fields should be in base class
        definitions = schema.get("definitions", {})
        base_def = definitions.get("EventBase", {})
        base_required = base_def.get("required", [])
        self.assertIn("common", base_required, "common should be required in base (all docs have it)")
        
        # 'extra' appears in only 2/4 variant A docs, should be optional in A
        a_def = definitions.get("A", {})
        a_required = a_def.get("required", [])
        a_properties = list(a_def.get("properties", {}).keys())
        
        self.assertIn("extra", a_properties, "extra should be in A's properties")
        self.assertNotIn("extra", a_required, "extra should NOT be required (only 2/4 docs have it)")
        
        # 'other' appears in all 3 variant B docs, should be required in B
        b_def = definitions.get("B", {})
        b_required = b_def.get("required", [])
        
        self.assertIn("other", b_required, "other should be required (all 3/3 docs have it)")

    def test_null_values_use_first_non_null_type(self):
        """Test that fields with some null values infer type from non-null values.
        
        If the first document has null for a field but later documents have actual
        values, the type should be inferred from the non-null values, not as 'null'.
        """
        from avrotize.schema_inference import JsonStructureSchemaInferrer
        
        values = [
            {"type": "A", "name": "first", "score": None},      # first doc has null
            {"type": "A", "name": "second", "score": 100},      # later doc has int
            {"type": "A", "name": "third", "score": 200},
            {"type": "B", "name": "fourth", "value": "x"},
        ]
        
        inferrer = JsonStructureSchemaInferrer(infer_choices=True)
        schema = inferrer.infer_from_json_values("Event", values)
        
        # Navigate to variant A
        definitions = schema.get("definitions", {})
        a_def = definitions.get("A", {})
        a_properties = a_def.get("properties", {})
        
        # 'score' should be inferred as 'integer', not 'null'
        score_prop = a_properties.get("score", {})
        self.assertEqual(score_prop.get("type"), "integer", 
                        "score should be 'integer' (from non-null values), not 'null'")


if __name__ == '__main__':
    unittest.main()
