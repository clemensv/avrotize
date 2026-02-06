"""Tests for JSON and XML schema inference commands."""

import json
import os
import sys
import tempfile
import unittest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.jsontoschema import convert_json_to_avro, convert_json_to_jstruct
from avrotize.xmltoschema import convert_xml_to_avro, convert_xml_to_jstruct
from avrotize.schema_inference import (
    infer_avro_schema_from_json,
    infer_avro_schema_from_xml,
    infer_jstruct_schema_from_json,
    infer_jstruct_schema_from_xml,
    JsonStructureSchemaInferrer,
    AvroSchemaInferrer
)


class TestJsonToAvro(unittest.TestCase):
    """Test cases for JSON to Avro schema inference."""


    def test_simple_object(self):
        """Test inference from a simple JSON object."""
        values = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False}
        ]
        
        schema = infer_avro_schema_from_json(values, type_name='Person', namespace='com.example')
        
        self.assertEqual(schema['type'], 'record')
        self.assertEqual(schema['name'], 'Person')
        self.assertEqual(schema['namespace'], 'com.example')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('name', fields_by_name)
        self.assertIn('age', fields_by_name)
        self.assertIn('active', fields_by_name)
        self.assertEqual(fields_by_name['name']['type'], 'string')
        self.assertEqual(fields_by_name['age']['type'], 'long')
        self.assertEqual(fields_by_name['active']['type'], 'boolean')

    def test_nested_object(self):
        """Test inference from nested JSON objects."""
        values = [
            {"user": {"name": "Alice", "email": "alice@example.com"}, "score": 100}
        ]
        
        schema = infer_avro_schema_from_json(values, type_name='Event')
        
        self.assertEqual(schema['type'], 'record')
        fields_by_name = {f['name']: f for f in schema['fields']}
        
        # user should be a nested record
        user_field = fields_by_name['user']
        self.assertEqual(user_field['type']['type'], 'record')
        self.assertEqual(user_field['type']['name'], 'user')

    def test_array_of_objects(self):
        """Test inference from arrays of objects."""
        values = [
            {"items": [{"id": 1, "name": "Item1"}, {"id": 2, "name": "Item2"}]}
        ]
        
        schema = infer_avro_schema_from_json(values, type_name='Container')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        items_field = fields_by_name['items']
        self.assertEqual(items_field['type']['type'], 'array')

    def test_sparse_data_folding(self):
        """Test that sparse data is folded into a single record type."""
        values = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "email": "bob@example.com"},
            {"name": "Charlie", "age": 35, "email": "charlie@example.com"}
        ]
        
        schema = infer_avro_schema_from_json(values, type_name='Person')
        
        self.assertEqual(schema['type'], 'record')
        fields_by_name = {f['name']: f for f in schema['fields']}
        
        # All fields should be present
        self.assertIn('name', fields_by_name)
        self.assertIn('age', fields_by_name)
        self.assertIn('email', fields_by_name)

    def test_convert_json_to_avro_file(self):
        """Test the full file conversion flow."""
        # Create a temp JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 1, "name": "Test1"},
                {"id": 2, "name": "Test2"}
            ], f)
            json_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_json2a.avsc')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_json_to_avro(
                input_files=[json_file],
                avro_schema_file=output_file,
                type_name='TestRecord',
                avro_namespace='com.test'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'TestRecord')
            self.assertEqual(schema['namespace'], 'com.test')
        finally:
            os.unlink(json_file)


class TestJsonToJstruct(unittest.TestCase):
    """Test cases for JSON to JSON Structure schema inference."""

    def test_simple_object(self):
        """Test inference from a simple JSON object."""
        values = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Person', base_id='https://example.com/')
        
        self.assertEqual(schema['$schema'], 'https://json-structure.org/meta/core/v0/#')
        self.assertEqual(schema['$id'], 'https://example.com/Person')
        self.assertEqual(schema['type'], 'object')
        self.assertEqual(schema['name'], 'Person')
        
        self.assertIn('properties', schema)
        self.assertIn('name', schema['properties'])
        self.assertIn('age', schema['properties'])
        self.assertIn('active', schema['properties'])
        
        self.assertEqual(schema['properties']['name']['type'], 'string')
        self.assertEqual(schema['properties']['age']['type'], 'integer')
        self.assertEqual(schema['properties']['active']['type'], 'boolean')

    def test_nested_object(self):
        """Test inference from nested JSON objects."""
        values = [
            {"user": {"name": "Alice", "email": "alice@example.com"}, "score": 100}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Event')
        
        self.assertEqual(schema['type'], 'object')
        self.assertIn('user', schema['properties'])
        
        # user should be a nested object
        user_prop = schema['properties']['user']
        self.assertEqual(user_prop['type'], 'object')
        self.assertIn('properties', user_prop)

    def test_array_of_primitives(self):
        """Test inference from arrays of primitives."""
        values = [
            {"tags": ["a", "b", "c"]}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='TaggedItem')
        
        tags_prop = schema['properties']['tags']
        self.assertEqual(tags_prop['type'], 'array')
        self.assertIn('items', tags_prop)

    def test_sparse_data_folding(self):
        """Test that sparse data is folded into a single object type."""
        values = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "email": "bob@example.com"},
            {"name": "Charlie", "age": 35, "email": "charlie@example.com"}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Person')
        
        self.assertEqual(schema['type'], 'object')
        
        # All properties should be present
        self.assertIn('name', schema['properties'])
        self.assertIn('age', schema['properties'])
        self.assertIn('email', schema['properties'])
        
        # Only 'name' should be required (appears in all records)
        self.assertIn('required', schema)
        self.assertIn('name', schema['required'])

    def test_convert_json_to_jstruct_file(self):
        """Test the full file conversion flow."""
        # Create a temp JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"id": 1, "name": "Test1"},
                {"id": 2, "name": "Test2"}
            ], f)
            json_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_json2s.jstruct.json')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_json_to_jstruct(
                input_files=[json_file],
                jstruct_schema_file=output_file,
                type_name='TestRecord',
                base_id='https://test.example.com/'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            self.assertEqual(schema['$schema'], 'https://json-structure.org/meta/core/v0/#')
            self.assertEqual(schema['$id'], 'https://test.example.com/TestRecord')
            self.assertEqual(schema['type'], 'object')
            self.assertEqual(schema['name'], 'TestRecord')
        finally:
            os.unlink(json_file)

    def test_jstruct_schema_validates_with_sdk(self):
        """Test that generated JSON Structure schemas validate with the official SDK."""
        try:
            from json_structure import SchemaValidator
        except ImportError:
            self.skipTest("json-structure SDK not installed")

        values = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Person', base_id='https://example.com/')
        
        validator = SchemaValidator()
        errors = validator.validate(schema)
        
        self.assertEqual(errors, [], f"Schema validation failed: {errors}")

    def test_jstruct_instance_validates_with_sdk(self):
        """Test that input JSON instances validate against generated JSON Structure schemas."""
        try:
            from json_structure import InstanceValidator, SchemaValidator
        except ImportError:
            self.skipTest("json-structure SDK not installed")

        values = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False},
            {"name": "Charlie", "age": 35, "active": True, "email": "charlie@example.com"}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Person', base_id='https://example.com/')
        
        # Schema should be valid
        schema_validator = SchemaValidator()
        schema_errors = schema_validator.validate(schema)
        self.assertEqual(schema_errors, [], f"Schema validation failed: {schema_errors}")
        
        # All input instances should validate against the schema
        instance_validator = InstanceValidator(schema)
        for i, instance in enumerate(values):
            errors = instance_validator.validate_instance(instance)
            self.assertEqual(errors, [], f"Instance {i} validation failed: {errors}")


class TestXmlToAvro(unittest.TestCase):
    """Test cases for XML to Avro schema inference."""

    def test_simple_xml(self):
        """Test inference from simple XML."""
        xml_strings = [
            '<person><name>Alice</name><age>30</age></person>',
            '<person><name>Bob</name><age>25</age></person>'
        ]
        
        schema = infer_avro_schema_from_xml(xml_strings, type_name='Person', namespace='com.example')
        
        self.assertEqual(schema['type'], 'record')
        self.assertEqual(schema['name'], 'Person')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('name', fields_by_name)
        self.assertIn('age', fields_by_name)

    def test_xml_with_attributes(self):
        """Test inference from XML with attributes."""
        xml_strings = [
            '<item id="1" status="active"><name>Item1</name></item>'
        ]
        
        schema = infer_avro_schema_from_xml(xml_strings, type_name='Item')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        # Attributes should be prefixed with @
        self.assertIn('_id', fields_by_name)  # @ converted to _ by avro_name
        self.assertIn('_status', fields_by_name)
        self.assertIn('name', fields_by_name)

    def test_xml_with_nested_elements(self):
        """Test inference from nested XML."""
        xml_strings = [
            '<order><customer><name>Alice</name></customer><total>100</total></order>'
        ]
        
        schema = infer_avro_schema_from_xml(xml_strings, type_name='Order')
        
        fields_by_name = {f['name']: f for f in schema['fields']}
        self.assertIn('customer', fields_by_name)
        
        # customer should be a nested record
        customer_field = fields_by_name['customer']
        self.assertEqual(customer_field['type']['type'], 'record')

    def test_convert_xml_to_avro_file(self):
        """Test the full file conversion flow."""
        # Create a temp XML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write('<root><item><id>1</id><name>Test1</name></item></root>')
            xml_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_xml2a.avsc')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_xml_to_avro(
                input_files=[xml_file],
                avro_schema_file=output_file,
                type_name='TestRecord',
                avro_namespace='com.test'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'TestRecord')
        finally:
            os.unlink(xml_file)


class TestXmlToJstruct(unittest.TestCase):
    """Test cases for XML to JSON Structure schema inference."""

    def test_simple_xml(self):
        """Test inference from simple XML."""
        xml_strings = [
            '<person><name>Alice</name><age>30</age></person>',
            '<person><name>Bob</name><age>25</age></person>'
        ]
        
        schema = infer_jstruct_schema_from_xml(xml_strings, type_name='Person', base_id='https://example.com/')
        
        self.assertEqual(schema['$schema'], 'https://json-structure.org/meta/core/v0/#')
        self.assertEqual(schema['type'], 'object')
        
        self.assertIn('name', schema['properties'])
        self.assertIn('age', schema['properties'])

    def test_xml_with_attributes(self):
        """Test inference from XML with attributes."""
        xml_strings = [
            '<item id="1" status="active"><name>Item1</name></item>'
        ]
        
        schema = infer_jstruct_schema_from_xml(xml_strings, type_name='Item')
        
        # Attributes should be prefixed with @
        self.assertIn('_id', schema['properties'])
        self.assertIn('_status', schema['properties'])
        self.assertIn('name', schema['properties'])

    def test_convert_xml_to_jstruct_file(self):
        """Test the full file conversion flow."""
        # Create a temp XML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write('<root><item><id>1</id><name>Test1</name></item></root>')
            xml_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_xml2s.jstruct.json')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_xml_to_jstruct(
                input_files=[xml_file],
                jstruct_schema_file=output_file,
                type_name='TestRecord',
                base_id='https://test.example.com/'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            self.assertEqual(schema['$schema'], 'https://json-structure.org/meta/core/v0/#')
            self.assertEqual(schema['type'], 'object')
        finally:
            os.unlink(xml_file)

    def test_jstruct_xml_schema_validates_with_sdk(self):
        """Test that generated JSON Structure schemas from XML validate with the official SDK."""
        try:
            from json_structure import SchemaValidator
        except ImportError:
            self.skipTest("json-structure SDK not installed")

        xml_strings = [
            '<person><name>Alice</name><age>30</age></person>'
        ]
        
        schema = infer_jstruct_schema_from_xml(xml_strings, type_name='Person', base_id='https://example.com/')
        
        validator = SchemaValidator()
        errors = validator.validate(schema)
        
        self.assertEqual(errors, [], f"Schema validation failed: {errors}")


class TestJsonLines(unittest.TestCase):
    """Test cases for JSONL (JSON Lines) file support."""

    def test_jsonl_file(self):
        """Test reading JSONL files."""
        # Create a temp JSONL file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            f.write('{"id": 1, "name": "Line1"}\n')
            f.write('{"id": 2, "name": "Line2"}\n')
            f.write('{"id": 3, "name": "Line3"}\n')
            jsonl_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_jsonl.avsc')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_json_to_avro(
                input_files=[jsonl_file],
                avro_schema_file=output_file,
                type_name='LineRecord'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'LineRecord')
            
            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('id', fields_by_name)
            self.assertIn('name', fields_by_name)
        finally:
            os.unlink(jsonl_file)


class TestMultipleFiles(unittest.TestCase):
    """Test cases for processing multiple input files."""

    def test_multiple_json_files(self):
        """Test processing multiple JSON files together."""
        # Create temp JSON files with different structures
        files = []
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump([{"name": "Alice", "age": 30}], f)
                files.append(f.name)
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump([{"name": "Bob", "email": "bob@example.com"}], f)
                files.append(f.name)
            
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_multi.avsc')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_json_to_avro(
                input_files=files,
                avro_schema_file=output_file,
                type_name='MergedRecord'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            fields_by_name = {f['name']: f for f in schema['fields']}
            # Schema should contain fields from all files
            self.assertIn('name', fields_by_name)
            self.assertIn('age', fields_by_name)
            self.assertIn('email', fields_by_name)
        finally:
            for f in files:
                os.unlink(f)


class TestRoundTripValidation(unittest.TestCase):
    """
    Round-trip validation tests: ALL source instances MUST validate against
    the inferred schema. This is a critical invariant for schema inference.
    """

    @classmethod
    def setUpClass(cls):
        """Check if json-structure SDK is available."""
        try:
            from json_structure import InstanceValidator, SchemaValidator
            cls.json_structure_available = True
        except ImportError:
            cls.json_structure_available = False

    def _validate_jstruct_roundtrip(self, values, schema, msg_prefix=""):
        """Helper to validate all instances against a JSON Structure schema."""
        if not self.json_structure_available:
            self.skipTest("json-structure SDK not installed")
        
        from json_structure import InstanceValidator, SchemaValidator
        
        # First, schema itself must be valid
        schema_validator = SchemaValidator()
        schema_errors = schema_validator.validate(schema)
        self.assertEqual(schema_errors, [], f"{msg_prefix}Schema validation failed: {schema_errors}")
        
        # All input instances must validate against the schema
        instance_validator = InstanceValidator(schema)
        for i, instance in enumerate(values):
            errors = instance_validator.validate_instance(instance)
            self.assertEqual(errors, [], 
                f"{msg_prefix}Instance {i} failed validation: {errors}\nInstance: {json.dumps(instance, indent=2)}\nSchema: {json.dumps(schema, indent=2)}")

    def test_simple_object_roundtrip(self):
        """Simple objects must validate against inferred schema."""
        values = [
            {"name": "Alice", "age": 30, "active": True},
            {"name": "Bob", "age": 25, "active": False}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Person')
        self._validate_jstruct_roundtrip(values, schema, "Simple object: ")

    def test_nested_object_roundtrip(self):
        """Nested objects must validate against inferred schema."""
        values = [
            {"user": {"name": "Alice", "email": "alice@example.com"}, "score": 100},
            {"user": {"name": "Bob", "email": "bob@example.com"}, "score": 85}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Event')
        self._validate_jstruct_roundtrip(values, schema, "Nested object: ")

    def test_array_of_objects_roundtrip(self):
        """Arrays of objects must validate against inferred schema."""
        values = [
            {"items": [{"id": 1, "name": "Item1"}, {"id": 2, "name": "Item2"}]},
            {"items": [{"id": 3, "name": "Item3"}]}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Container')
        self._validate_jstruct_roundtrip(values, schema, "Array of objects: ")

    def test_sparse_data_roundtrip(self):
        """Sparse data (different fields in different instances) must validate."""
        values = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "email": "bob@example.com"},
            {"name": "Charlie", "age": 35, "email": "charlie@example.com"}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Person')
        self._validate_jstruct_roundtrip(values, schema, "Sparse data: ")

    def test_deeply_nested_roundtrip(self):
        """Deeply nested structures must validate."""
        values = [
            {"level1": {"level2": {"level3": {"value": "deep"}}}},
            {"level1": {"level2": {"level3": {"value": "deeper", "extra": 42}}}}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='DeepNest')
        self._validate_jstruct_roundtrip(values, schema, "Deeply nested: ")

    def test_mixed_types_in_array_roundtrip(self):
        """Arrays with mixed content must validate."""
        values = [
            {"data": [1, 2, 3]},
            {"data": [4, 5]}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='MixedArray')
        self._validate_jstruct_roundtrip(values, schema, "Mixed array: ")

    def test_null_values_roundtrip(self):
        """Null values in instances must validate."""
        values = [
            {"name": "Alice", "nickname": None},
            {"name": "Bob", "nickname": "Bobby"}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Person')
        self._validate_jstruct_roundtrip(values, schema, "Null values: ")

    def test_enum_inference_roundtrip(self):
        """Enum-inferred fields must validate original values."""
        values = [
            {"status": "active", "priority": "high"},
            {"status": "active", "priority": "low"},
            {"status": "inactive", "priority": "medium"},
            {"status": "pending", "priority": "high"}
        ]
        
        inferrer = JsonStructureSchemaInferrer(infer_enums=True)
        schema = inferrer.infer_from_json_values('Item', values)
        self._validate_jstruct_roundtrip(values, schema, "Enum inference: ")

    def test_choice_inference_roundtrip(self):
        """Choice-inferred schemas must validate all variant instances."""
        values = [
            {"type": "goalEvent", "playerId": "P1", "score": 1},
            {"type": "goalEvent", "playerId": "P2", "score": 2},
            {"type": "cardEvent", "playerId": "P3", "cardType": "yellow"},
            {"type": "cardEvent", "playerId": "P4", "cardType": "red"},
            {"type": "substitutionEvent", "playerIn": "P5", "playerOut": "P6"},
        ]
        
        inferrer = JsonStructureSchemaInferrer(infer_choices=True, choice_depth=1)
        schema = inferrer.infer_from_json_values('Event', values)
        self._validate_jstruct_roundtrip(values, schema, "Choice inference: ")

    def test_choice_preserves_discriminator_casing(self):
        """Choice inference must preserve original casing of discriminator values."""
        values = [
            {"type": "goalEvent", "score": 1},
            {"type": "cardEvent", "cardType": "yellow"},
            {"type": "substitution_event", "playerIn": "P5"},
        ]
        
        inferrer = JsonStructureSchemaInferrer(infer_choices=True, choice_depth=1)
        schema = inferrer.infer_from_json_values('Event', values)
        
        # Check that choice keys match original values exactly
        if 'choices' in schema:
            choice_keys = set(schema['choices'].keys())
            expected_keys = {"goalEvent", "cardEvent", "substitution_event"}
            self.assertEqual(choice_keys, expected_keys, 
                f"Choice keys don't match original discriminator values. Got: {choice_keys}")
        
        # Also validate round-trip
        self._validate_jstruct_roundtrip(values, schema, "Choice casing: ")

    def test_nested_choice_inference_roundtrip(self):
        """Nested choice inference (choice_depth > 1) must validate."""
        values = [
            {
                "type": "match",
                "events": [
                    {"eventType": "goal", "scorer": "Player1"},
                    {"eventType": "card", "recipient": "Player2", "cardColor": "yellow"}
                ]
            },
            {
                "type": "match", 
                "events": [
                    {"eventType": "substitution", "playerIn": "Player3", "playerOut": "Player4"}
                ]
            }
        ]
        
        inferrer = JsonStructureSchemaInferrer(infer_choices=True, choice_depth=2)
        schema = inferrer.infer_from_json_values('Match', values)
        self._validate_jstruct_roundtrip(values, schema, "Nested choice: ")

    def test_datetime_inference_roundtrip(self):
        """Datetime-inferred fields must validate original ISO strings."""
        values = [
            {"created": "2024-01-15T10:30:00Z", "date": "2024-01-15"},
            {"created": "2024-02-20T14:45:30Z", "date": "2024-02-20"}
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Event')
        self._validate_jstruct_roundtrip(values, schema, "Datetime inference: ")

    def test_complex_real_world_structure_roundtrip(self):
        """Complex real-world-like structures must validate."""
        values = [
            {
                "matchId": "M001",
                "timestamp": "2024-01-15T15:00:00Z",
                "homeTeam": {"id": "T1", "name": "Team A", "score": 2},
                "awayTeam": {"id": "T2", "name": "Team B", "score": 1},
                "events": [
                    {"minute": 23, "type": "goal", "player": "P1"},
                    {"minute": 45, "type": "halftime"}
                ]
            },
            {
                "matchId": "M002",
                "timestamp": "2024-01-16T18:00:00Z", 
                "homeTeam": {"id": "T3", "name": "Team C", "score": 0},
                "awayTeam": {"id": "T4", "name": "Team D", "score": 0},
                "events": []
            }
        ]
        
        schema = infer_jstruct_schema_from_json(values, type_name='Match')
        self._validate_jstruct_roundtrip(values, schema, "Complex structure: ")


class TestJSONLRoundTrip(unittest.TestCase):
    """Round-trip tests for JSONL files where each line must validate."""

    @classmethod
    def setUpClass(cls):
        try:
            from json_structure import InstanceValidator, SchemaValidator
            cls.json_structure_available = True
        except ImportError:
            cls.json_structure_available = False

    def test_jsonl_lines_validate_individually(self):
        """Each line in a JSONL file must validate against the inferred schema."""
        if not self.json_structure_available:
            self.skipTest("json-structure SDK not installed")
        
        from json_structure import InstanceValidator, SchemaValidator
        
        # Create JSONL content
        lines = [
            {"id": 1, "name": "Line1", "active": True},
            {"id": 2, "name": "Line2", "active": False},
            {"id": 3, "name": "Line3", "extra": "optional field"}
        ]
        
        # Create temp JSONL file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            for line in lines:
                f.write(json.dumps(line) + '\n')
            jsonl_file = f.name

        try:
            output_file = os.path.join(tempfile.gettempdir(), 'avrotize', 'test_jsonl_roundtrip.jstruct.json')
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            convert_json_to_jstruct(
                input_files=[jsonl_file],
                jstruct_schema_file=output_file,
                type_name='LineRecord'
            )
            
            with open(output_file, 'r') as f:
                schema = json.load(f)
            
            # Schema must be valid
            schema_validator = SchemaValidator()
            schema_errors = schema_validator.validate(schema)
            self.assertEqual(schema_errors, [], f"Schema validation failed: {schema_errors}")
            
            # Each line must validate
            instance_validator = InstanceValidator(schema)
            for i, line in enumerate(lines):
                errors = instance_validator.validate_instance(line)
                self.assertEqual(errors, [], f"JSONL line {i+1} failed validation: {errors}")
        finally:
            os.unlink(jsonl_file)


if __name__ == '__main__':
    unittest.main()
