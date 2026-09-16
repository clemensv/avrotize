"""Tests for jstructtoavro module."""

import io
import json
import os
import tempfile
import unittest

from avrotize.jstructtoavro import JsonStructureToAvro, convert_json_structure_to_avro
from avrotize.avrotojstruct import AvroToJsonStructure
from avrotize.common import generic_type, deduplicate_any_value_record, ANY_VALUE_RECORD, ANY_VALUE_NAME


class TestAnyValueSerialization(unittest.TestCase):
    """Test Avro serialization/deserialization with AnyValue (extensible any type)."""

    def test_any_value_schema_valid(self):
        """Verify the generic_type() union is a valid Avro schema."""
        from fastavro.schema import parse_schema

        schema = {
            'type': 'record',
            'name': 'TestRecord',
            'namespace': 'test',
            'fields': [
                {'name': 'payload', 'type': generic_type()}
            ]
        }
        parsed = parse_schema(schema)
        self.assertIsNotNone(parsed)

    def test_any_value_serialize_primitives(self):
        """Serialize and deserialize primitive values through the any-type union."""
        from fastavro import writer, reader
        from fastavro.schema import parse_schema

        schema = {
            'type': 'record',
            'name': 'Event',
            'namespace': 'test',
            'fields': [
                {'name': 'id', 'type': 'string'},
                {'name': 'payload', 'type': generic_type()}
            ]
        }
        parsed = parse_schema(schema)

        records = [
            {'id': 'evt-1', 'payload': None},
            {'id': 'evt-2', 'payload': True},
            {'id': 'evt-3', 'payload': 42},
            {'id': 'evt-4', 'payload': 3.14},
            {'id': 'evt-5', 'payload': 'hello world'},
            {'id': 'evt-6', 'payload': b'\x00\x01\x02\x03'},
        ]

        buf = io.BytesIO()
        writer(buf, parsed, records)

        buf.seek(0)
        deserialized = list(reader(buf))

        self.assertEqual(len(deserialized), 6)
        self.assertIsNone(deserialized[0]['payload'])
        self.assertTrue(deserialized[1]['payload'])
        self.assertEqual(deserialized[2]['payload'], 42)
        self.assertAlmostEqual(deserialized[3]['payload'], 3.14, places=2)
        self.assertEqual(deserialized[4]['payload'], 'hello world')
        self.assertEqual(deserialized[5]['payload'], b'\x00\x01\x02\x03')

    def test_any_value_serialize_arrays_and_maps(self):
        """Serialize arrays and maps through the any-type union."""
        from fastavro import writer, reader
        from fastavro.schema import parse_schema

        schema = {
            'type': 'record',
            'name': 'Container',
            'namespace': 'test',
            'fields': [
                {'name': 'data', 'type': generic_type()}
            ]
        }
        parsed = parse_schema(schema)

        records = [
            {'data': ['hello', 'world']},  # array of strings
            {'data': {'key1': 'val1', 'key2': 'val2'}},  # map of strings
            {'data': [1, 2, 3]},  # array of ints
        ]

        buf = io.BytesIO()
        writer(buf, parsed, records)

        buf.seek(0)
        deserialized = list(reader(buf))

        self.assertEqual(deserialized[0]['data'], ['hello', 'world'])
        self.assertEqual(deserialized[1]['data'], {'key1': 'val1', 'key2': 'val2'})
        self.assertEqual(deserialized[2]['data'], [1, 2, 3])

    def test_any_value_serialize_extended_record(self):
        """
        Demonstrate schema evolution: AnyValue starts empty, but a v2 schema 
        adds fields. Data written with v2 can be read back with v2 readers.
        v1 readers would ignore the new fields (standard Avro evolution).
        """
        from fastavro import writer, reader
        from fastavro.schema import parse_schema
        import copy

        # v1 schema: AnyValue is empty (no fields)
        schema_v1 = {
            'type': 'record',
            'name': 'Message',
            'namespace': 'test',
            'fields': [
                {'name': 'id', 'type': 'string'},
                {'name': 'payload', 'type': generic_type()}
            ]
        }

        # v2 schema: AnyValue has been extended with fields (schema evolution)
        any_value_v2 = {
            "type": "record",
            "name": "AnyValue",
            "namespace": "avrotize",
            "doc": "Extended AnyValue with structured fields.",
            "fields": [
                {"name": "content_type", "type": ["null", "string"], "default": None},
                {"name": "body", "type": ["null", "string"], "default": None},
                {"name": "timestamp", "type": ["null", "long"], "default": None}
            ]
        }

        schema_v2 = copy.deepcopy(schema_v1)
        # Replace the AnyValue definition in v2 with the extended version
        def replace_any_value(node, replacement):
            if isinstance(node, list):
                for i, item in enumerate(node):
                    if isinstance(item, dict) and item.get('name') == 'AnyValue' and item.get('type') == 'record':
                        node[i] = replacement
                        return True
                    if replace_any_value(item, replacement):
                        return True
            elif isinstance(node, dict):
                for v in node.values():
                    if isinstance(v, (list, dict)):
                        if replace_any_value(v, replacement):
                            return True
            return False

        replace_any_value(schema_v2, any_value_v2)

        parsed_v1 = parse_schema(copy.deepcopy(schema_v1))
        parsed_v2 = parse_schema(copy.deepcopy(schema_v2))

        # Write data with v1 schema (payload is a simple string)
        buf_v1 = io.BytesIO()
        writer(buf_v1, parsed_v1, [
            {'id': 'msg-1', 'payload': 'simple string payload'},
        ])

        # Write data with v2 schema (payload uses extended AnyValue record)
        buf_v2 = io.BytesIO()
        writer(buf_v2, parsed_v2, [
            {'id': 'msg-2', 'payload': {
                'content_type': 'application/json',
                'body': '{"key": "value"}',
                'timestamp': 1717430400000
            }},
        ])

        # Read v2 data back with v2 schema
        buf_v2.seek(0)
        result_v2 = list(reader(buf_v2, reader_schema=parsed_v2))
        self.assertEqual(result_v2[0]['id'], 'msg-2')
        # fastavro may return bytes or str for string fields depending on version
        payload = result_v2[0]['payload']
        ct = payload['content_type']
        body = payload['body']
        if isinstance(ct, bytes):
            ct = ct.decode('utf-8')
        if isinstance(body, bytes):
            body = body.decode('utf-8')
        self.assertEqual(ct, 'application/json')
        self.assertEqual(body, '{"key": "value"}')
        self.assertEqual(payload['timestamp'], 1717430400000)

        # Read v1 data back with v1 schema (payload was a string, not AnyValue)
        buf_v1.seek(0)
        result_v1 = list(reader(buf_v1))
        self.assertEqual(result_v1[0]['id'], 'msg-1')
        self.assertEqual(result_v1[0]['payload'], 'simple string payload')

    def test_any_value_multiple_fields_deduplication(self):
        """Multiple fields using any-type in same record serialize correctly."""
        from fastavro import writer, reader
        from fastavro.schema import parse_schema

        schema = {
            'type': 'record',
            'name': 'MultiAny',
            'namespace': 'test',
            'fields': [
                {'name': 'field_a', 'type': generic_type()},
                {'name': 'field_b', 'type': generic_type()},
                {'name': 'field_c', 'type': generic_type()},
            ]
        }
        deduplicate_any_value_record(schema)
        parsed = parse_schema(schema)

        records = [
            {'field_a': 'text', 'field_b': 123, 'field_c': ['arr', 'val']},
            {'field_a': None, 'field_b': True, 'field_c': {'k': 'v'}},
        ]

        buf = io.BytesIO()
        writer(buf, parsed, records)

        buf.seek(0)
        deserialized = list(reader(buf))

        self.assertEqual(deserialized[0]['field_a'], 'text')
        self.assertEqual(deserialized[0]['field_b'], 123)
        self.assertEqual(deserialized[0]['field_c'], ['arr', 'val'])
        self.assertIsNone(deserialized[1]['field_a'])
        self.assertTrue(deserialized[1]['field_b'])
        self.assertEqual(deserialized[1]['field_c'], {'k': 'v'})

    def test_any_value_nested_arrays_and_maps(self):
        """Nested arrays/maps with AnyValue references serialize correctly."""
        from fastavro import writer, reader
        from fastavro.schema import parse_schema

        schema = {
            'type': 'record',
            'name': 'Nested',
            'namespace': 'test',
            'fields': [
                {'name': 'deep', 'type': generic_type()}
            ]
        }
        parsed = parse_schema(schema)

        # Nested array: array containing maps
        records = [
            {'deep': [{'inner_key': 'inner_val'}]},
            {'deep': {'outer': [1, 2, 3]}},
        ]

        buf = io.BytesIO()
        writer(buf, parsed, records)

        buf.seek(0)
        deserialized = list(reader(buf))

        self.assertEqual(deserialized[0]['deep'], [{'inner_key': 'inner_val'}])
        self.assertEqual(deserialized[1]['deep'], {'outer': [1, 2, 3]})

    def test_per_field_named_records(self):
        """Per-field named AnyValue records enable independent schema evolution."""
        from fastavro import writer, reader
        from fastavro.schema import parse_schema
        import copy

        # Two fields with DIFFERENT named records — can be extended independently
        schema = {
            'type': 'record',
            'name': 'Event',
            'namespace': 'test',
            'fields': [
                {'name': 'id', 'type': 'string'},
                {'name': 'payload', 'type': generic_type(name='PayloadAnyValue')},
                {'name': 'metadata', 'type': generic_type(name='MetadataAnyValue')},
            ]
        }
        parsed = parse_schema(schema)

        records = [
            {'id': 'evt-1', 'payload': 'hello', 'metadata': {'key': 'val'}},
            {'id': 'evt-2', 'payload': 42, 'metadata': None},
        ]

        buf = io.BytesIO()
        writer(buf, parsed, records)
        buf.seek(0)
        deserialized = list(reader(buf))

        self.assertEqual(deserialized[0]['payload'], 'hello')
        self.assertEqual(deserialized[0]['metadata'], {'key': 'val'})
        self.assertEqual(deserialized[1]['payload'], 42)
        self.assertIsNone(deserialized[1]['metadata'])

    def test_per_field_independent_evolution(self):
        """Extend per-field records independently and serialize/deserialize."""
        from fastavro import writer, reader
        from fastavro.schema import parse_schema
        import copy

        # v2 schema: PayloadAnyValue extended with content fields,
        # MetadataAnyValue extended with trace fields
        schema_v2 = {
            'type': 'record',
            'name': 'Event',
            'namespace': 'test',
            'fields': [
                {'name': 'id', 'type': 'string'},
                {'name': 'payload', 'type': generic_type(name='PayloadAnyValue')},
                {'name': 'metadata', 'type': generic_type(name='MetadataAnyValue')},
            ]
        }

        # Replace PayloadAnyValue with extended version
        payload_v2 = {
            "type": "record", "name": "PayloadAnyValue", "namespace": "avrotize",
            "fields": [
                {"name": "body", "type": ["null", "string"], "default": None},
                {"name": "content_type", "type": ["null", "string"], "default": None},
            ]
        }
        # Replace MetadataAnyValue with different extended version
        metadata_v2 = {
            "type": "record", "name": "MetadataAnyValue", "namespace": "avrotize",
            "fields": [
                {"name": "trace_id", "type": ["null", "string"], "default": None},
                {"name": "span_id", "type": ["null", "string"], "default": None},
            ]
        }

        def replace_record(node, name, replacement):
            if isinstance(node, list):
                for i, item in enumerate(node):
                    if isinstance(item, dict) and item.get('name') == name and item.get('type') == 'record':
                        node[i] = replacement
                        return True
                    if replace_record(item, name, replacement):
                        return True
            elif isinstance(node, dict):
                for v in node.values():
                    if isinstance(v, (list, dict)):
                        if replace_record(v, name, replacement):
                            return True
            return False

        replace_record(schema_v2, 'PayloadAnyValue', payload_v2)
        replace_record(schema_v2, 'MetadataAnyValue', metadata_v2)

        parsed_v2 = parse_schema(copy.deepcopy(schema_v2))

        # Write with independently-extended records
        buf = io.BytesIO()
        writer(buf, parsed_v2, [
            {
                'id': 'evt-1',
                'payload': {'body': 'hello world', 'content_type': 'text/plain'},
                'metadata': {'trace_id': 'abc123', 'span_id': 'def456'},
            },
        ])

        buf.seek(0)
        result = list(reader(buf, reader_schema=parsed_v2))
        payload = result[0]['payload']
        metadata = result[0]['metadata']

        # Verify independent fields
        if isinstance(payload.get('body'), bytes):
            payload['body'] = payload['body'].decode('utf-8')
        if isinstance(payload.get('content_type'), bytes):
            payload['content_type'] = payload['content_type'].decode('utf-8')
        if isinstance(metadata.get('trace_id'), bytes):
            metadata['trace_id'] = metadata['trace_id'].decode('utf-8')
        if isinstance(metadata.get('span_id'), bytes):
            metadata['span_id'] = metadata['span_id'].decode('utf-8')

        self.assertEqual(payload['body'], 'hello world')
        self.assertEqual(payload['content_type'], 'text/plain')
        self.assertEqual(metadata['trace_id'], 'abc123')
        self.assertEqual(metadata['span_id'], 'def456')


class TestJsonStructureToAvro(unittest.TestCase):
    """Test cases for JsonStructureToAvro converter."""

    def setUp(self):
        """Set up test fixtures."""
        self.converter = JsonStructureToAvro()
        self.maxDiff = None

    def test_simple_record(self):
        """Test converting a simple record."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Person",
            "definitions": {
                "Person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "age": {"type": "int32"}
                    },
                    "required": ["name", "age"]
                }
            }
        }

        result = self.converter.convert(structure)

        self.assertEqual(result['type'], 'record')
        self.assertEqual(result['name'], 'Person')
        self.assertEqual(len(result['fields']), 2)
        self.assertEqual(result['fields'][0]['name'], 'name')
        self.assertEqual(result['fields'][0]['type'], 'string')
        self.assertEqual(result['fields'][1]['name'], 'age')
        self.assertEqual(result['fields'][1]['type'], 'int')

    def test_optional_fields(self):
        """Test converting record with optional fields."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Person",
            "definitions": {
                "Person": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "age": {"type": "int32"}
                    },
                    "required": ["name"]
                }
            }
        }

        result = self.converter.convert(structure)

        # age should be nullable with null default
        age_field = next(f for f in result['fields'] if f['name'] == 'age')
        self.assertEqual(age_field['type'], ['null', 'int'])
        self.assertIsNone(age_field['default'])

    def test_nested_namespace(self):
        """Test converting record with nested namespace."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/com/example/Person",
            "definitions": {
                "com": {
                    "example": {
                        "Person": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"}
                            },
                            "required": ["name"]
                        }
                    }
                }
            }
        }

        result = self.converter.convert(structure)

        self.assertEqual(result['namespace'], 'com.example')
        self.assertEqual(result['name'], 'Person')

    def test_enum_type(self):
        """Test converting enum type."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Color",
            "definitions": {
                "Color": {
                    "type": "string",
                    "enum": ["RED", "GREEN", "BLUE"]
                }
            }
        }

        result = self.converter.convert(structure)

        self.assertEqual(result['type'], 'enum')
        self.assertEqual(result['name'], 'Color')
        self.assertEqual(result['symbols'], ["RED", "GREEN", "BLUE"])

    def test_array_type(self):
        """Test converting array type."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/StringList",
            "definitions": {
                "StringList": {
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    },
                    "required": ["items"]
                }
            }
        }

        result = self.converter.convert(structure)

        items_field = result['fields'][0]
        self.assertEqual(items_field['type']['type'], 'array')
        self.assertEqual(items_field['type']['items'], 'string')

    def test_map_type(self):
        """Test converting map type."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/StringMap",
            "definitions": {
                "StringMap": {
                    "type": "object",
                    "properties": {
                        "data": {
                            "type": "map",
                            "values": {"type": "string"}
                        }
                    },
                    "required": ["data"]
                }
            }
        }

        result = self.converter.convert(structure)

        data_field = result['fields'][0]
        self.assertEqual(data_field['type']['type'], 'map')
        self.assertEqual(data_field['type']['values'], 'string')

    def test_type_reference(self):
        """Test converting type with references."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Employee",
            "definitions": {
                "Address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city": {"type": "string"}
                    },
                    "required": ["street", "city"]
                },
                "Employee": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "address": {"$ref": "#/definitions/Address"}
                    },
                    "required": ["name", "address"]
                }
            }
        }

        result = self.converter.convert(structure)

        # Should convert both types
        self.assertEqual(len(self.converter.converted_types), 2)
        
        # Result should be a list when there are multiple types
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        
        # Find the Employee record
        employee = next(r for r in result if r['name'] == 'Employee')
        
        # Check reference is resolved
        address_field = next(f for f in employee['fields'] if f['name'] == 'address')
        self.assertEqual(address_field['type'], 'Address')

    def test_logical_types(self):
        """Test converting logical types."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Event",
            "definitions": {
                "Event": {
                    "type": "object",
                    "properties": {
                        "timestamp": {
                            "type": "int64",
                            "logicalType": "timestampMillis"
                        },
                        "uuid": {
                            "type": "string",
                            "logicalType": "uuid"
                        }
                    },
                    "required": ["timestamp", "uuid"]
                }
            }
        }

        result = self.converter.convert(structure)

        # Avrotize Schema: temporal types use a string base with an rfc3339-* logical
        # type so the output is valid standard Avro (issue #335).
        timestamp_field = next(f for f in result['fields'] if f['name'] == 'timestamp')
        self.assertEqual(timestamp_field['type']['type'], 'string')
        self.assertEqual(timestamp_field['type']['logicalType'], 'rfc3339-timestamp-millis')

        uuid_field = next(f for f in result['fields'] if f['name'] == 'uuid')
        self.assertEqual(uuid_field['type']['type'], 'string')
        self.assertEqual(uuid_field['type']['logicalType'], 'uuid')

    # ---- Issue #335: temporal types must emit valid *standard* Avro -----------

    # Reserved Avro logical types that require a non-string base; emitting any of
    # these on a "string" base is invalid standard Avro (the #335 defect).
    _RESERVED_NONSTRING_TEMPORAL = {
        'date', 'time-millis', 'time-micros',
        'timestamp-millis', 'timestamp-micros',
        'local-timestamp-millis', 'local-timestamp-micros',
        'duration',
    }

    @staticmethod
    def _iter_logical(node):
        """Yield (base_type, logicalType) pairs for every annotated node in a schema."""
        if isinstance(node, dict):
            lt = node.get('logicalType')
            if lt is not None:
                yield (node.get('type'), lt)
            for v in node.values():
                yield from TestJsonStructureToAvro._iter_logical(v)
        elif isinstance(node, list):
            for v in node:
                yield from TestJsonStructureToAvro._iter_logical(v)

    def _temporal_avro(self):
        with open('test/struct/temporal-types.struct.json', 'r', encoding='utf-8') as f:
            structure = json.load(f)
        return self.converter.convert(structure)

    def test_temporal_types_emit_rfc3339_logical_types(self):
        """Each JSON Structure temporal type maps to the rfc3339-* string family."""
        result = self._temporal_avro()
        fields = {f['name']: f for f in result['fields']}

        def logical_of(field):
            t = field['type']
            if isinstance(t, list):  # nullable union ["null", {...}]
                t = next(x for x in t if x != 'null')
            return t['type'], t['logicalType']

        self.assertEqual(logical_of(fields['dateField']), ('string', 'rfc3339-date'))
        self.assertEqual(logical_of(fields['timeField']), ('string', 'rfc3339-time-millis'))
        self.assertEqual(logical_of(fields['datetimeField']), ('string', 'rfc3339-timestamp-millis'))
        self.assertEqual(logical_of(fields['timestampField']), ('string', 'rfc3339-timestamp-millis'))
        self.assertEqual(logical_of(fields['durationField']), ('string', 'rfc3339-duration'))

    def test_temporal_output_has_no_reserved_logical_on_string(self):
        """Regression for #335: never emit a reserved Avro logical type on string."""
        result = self._temporal_avro()
        offenders = [
            (base, lt) for base, lt in self._iter_logical(result)
            if base == 'string' and lt in self._RESERVED_NONSTRING_TEMPORAL
        ]
        self.assertEqual(
            offenders, [],
            f"Emitted reserved Avro logical type(s) on a string base: {offenders}")

    def test_issue_335_reproducer_datetime(self):
        """Exact reproducer from issue #335 must not emit string+timestamp-millis."""
        structure = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/repro/dt2",
            "name": "DtRepro2",
            "type": "object",
            "properties": {
                "Id": {"type": "string"},
                "sent": {"type": "datetime", "description": "ISO-8601 timestamp"},
            },
        }
        result = self.converter.convert(structure)
        sent = next(f for f in result['fields'] if f['name'] == 'sent')
        sent_type = sent['type']
        if isinstance(sent_type, list):
            sent_type = next(x for x in sent_type if x != 'null')
        self.assertEqual(sent_type['type'], 'string')
        self.assertNotEqual(sent_type['logicalType'], 'timestamp-millis')
        self.assertEqual(sent_type['logicalType'], 'rfc3339-timestamp-millis')

    def test_temporal_output_is_valid_standard_avro(self):
        """fastavro must parse the emitted schema as valid standard Avro."""
        try:
            import fastavro
        except ImportError:
            self.skipTest("fastavro not installed")
        # Must not raise.
        fastavro.parse_schema(self._temporal_avro())

    def test_temporal_round_trip_to_json_structure(self):
        """s2a -> a2s returns the native JSON Structure temporal types."""
        avro = self._temporal_avro()
        structure = AvroToJsonStructure().convert(avro)
        defs = structure['definitions']['TemporalTypes']['properties']
        self.assertEqual(defs['dateField']['type'], 'date')
        self.assertEqual(defs['timeField']['type'], 'time')
        self.assertEqual(defs['datetimeField']['type'], 'datetime')
        self.assertEqual(defs['timestampField']['type'], 'datetime')
        self.assertEqual(defs['durationField']['type'], 'duration')

    def test_rfc3339_validator_accepts_and_rejects(self):
        """avrovalidator validates rfc3339-* values as RFC 3339 strings."""
        from avrotize.avrovalidator import validate_json_against_avro
        avro = self._temporal_avro()
        valid = {
            "dateField": "2024-06-03",
            "timeField": "12:00:00",
            "datetimeField": "2024-06-03T12:00:00Z",
            "timestampField": "2024-06-03T12:00:00.500Z",
            "durationField": "P1Y2M3DT4H5M6S",
        }
        self.assertEqual(validate_json_against_avro(valid, avro), [])
        bad = dict(valid, datetimeField="not-a-datetime")
        self.assertTrue(validate_json_against_avro(bad, avro))

    def test_logical_type_annotation_maps_to_rfc3339(self):
        """Explicit JSON Structure logicalType annotations also use rfc3339-*."""
        structure = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "t", "name": "E", "type": "object",
            "properties": {
                "a": {"type": "int64", "logicalType": "timestampMicros"},
                "b": {"type": "int32", "logicalType": "date"},
                "c": {"type": "string", "logicalType": "uuid"},
            },
            "required": ["a", "b", "c"],
        }
        result = self.converter.convert(structure)
        fields = {f['name']: f['type'] for f in result['fields']}
        self.assertEqual(fields['a'], {'type': 'string', 'logicalType': 'rfc3339-timestamp-micros'})
        self.assertEqual(fields['b'], {'type': 'string', 'logicalType': 'rfc3339-date'})
        # uuid stays a valid standard-Avro string logical type.
        self.assertEqual(fields['c'], {'type': 'string', 'logicalType': 'uuid'})

    def test_round_trip_conversion(self):
        """Test round-trip conversion: Avro -> Structure -> Avro."""
        # Start with a simple Avro schema
        original_avro = {
            "type": "record",
            "name": "TestRecord",
            "namespace": "com.example",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"}
            ]
        }

        # Convert to structure
        to_struct_converter = AvroToJsonStructure()
        structure = to_struct_converter.convert(original_avro, "com.example")

        # Convert back to Avro
        result = self.converter.convert(structure)

        # Verify key properties
        self.assertEqual(result['type'], 'record')
        self.assertEqual(result['name'], 'TestRecord')
        self.assertEqual(result['namespace'], 'com.example')
        self.assertEqual(len(result['fields']), 3)

    def test_round_trip_preserves_relational_metadata(self):
        """Test that unique/foreignKeys metadata is preserved across A2S/S2A."""
        original_avro = {
            "type": "record",
            "name": "Order",
            "namespace": "com.example",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "customer_id", "type": "long"}
            ],
            "unique": ["id"],
            "foreignKeys": [
                {
                    "name": "fk_orders_customers",
                    "columns": ["customer_id"],
                    "referencedTable": "Customer",
                    "referencedColumns": ["id"],
                    "referencedTableSql": "public.customers"
                }
            ]
        }

        structure = AvroToJsonStructure().convert(original_avro)
        round_tripped = JsonStructureToAvro().convert(structure)

        self.assertEqual(["id"], round_tripped.get("unique"))
        self.assertEqual(1, len(round_tripped.get("foreignKeys", [])))
        fk = round_tripped["foreignKeys"][0]
        self.assertEqual(["customer_id"], fk.get("columns"))
        self.assertEqual("Customer", fk.get("referencedTable"))

    def test_file_conversion(self):
        """Test file-based conversion."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/Simple",
            "definitions": {
                "Simple": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"}
                    },
                    "required": ["value"]
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            struct_file = os.path.join(tmpdir, "test.struct.json")
            avro_file = os.path.join(tmpdir, "test.avsc")

            # Write structure file
            with open(struct_file, 'w', encoding='utf-8') as f:
                json.dump(structure, f)

            # Convert
            convert_json_structure_to_avro(struct_file, avro_file)

            # Verify output file exists and is valid
            self.assertTrue(os.path.exists(avro_file))
            
            with open(avro_file, 'r', encoding='utf-8') as f:
                result = json.load(f)
            
            self.assertEqual(result['name'], 'Simple')

    def test_fixed_type(self):
        """Test converting fixed-length binary type."""
        structure = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "test",
            "$root": "#/definitions/MD5",
            "definitions": {
                "MD5": {
                    "type": "binary",
                    "byteLength": 16,
                    "description": "MD5 hash"
                }
            }
        }

        result = self.converter.convert(structure)

        self.assertEqual(result['type'], 'fixed')
        self.assertEqual(result['name'], 'MD5')
        self.assertEqual(result['size'], 16)
        self.assertEqual(result['doc'], 'MD5 hash')

    def test_regression_basic_types(self):
        """Test basic-types.struct.json against reference output."""
        self._test_against_reference('test/struct/basic-types.struct.json')

    def test_regression_choice_types(self):
        """Test choice-types.struct.json against reference output."""
        self._test_against_reference('test/struct/choice-types.struct.json')

    def test_regression_collections(self):
        """Test collections.struct.json against reference output."""
        self._test_against_reference('test/struct/collections.struct.json')

    def test_regression_complex_scenario(self):
        """Test complex-scenario.struct.json against reference output."""
        self._test_against_reference('test/struct/complex-scenario.struct.json')

    def test_regression_edge_cases(self):
        """Test edge-cases.struct.json against reference output."""
        self._test_against_reference('test/struct/edge-cases.struct.json')

    def test_regression_extensions(self):
        """Test extensions.struct.json against reference output."""
        self._test_against_reference('test/struct/extensions.struct.json')

    def test_regression_nested_objects(self):
        """Test nested-objects.struct.json against reference output."""
        self._test_against_reference('test/struct/nested-objects.struct.json')

    def test_regression_numeric_types(self):
        """Test numeric-types.struct.json against reference output."""
        self._test_against_reference('test/struct/numeric-types.struct.json')

    def test_regression_temporal_types(self):
        """Test temporal-types.struct.json against reference output."""
        self._test_against_reference('test/struct/temporal-types.struct.json')

    def test_regression_validation_constraints(self):
        """Test validation-constraints.struct.json against reference output."""
        self._test_against_reference('test/struct/validation-constraints.struct.json')

    def _test_against_reference(self, struct_file: str):
        """
        Helper method to test a struct file against its reference output.
        
        Args:
            struct_file: Path to the .struct.json file
        """
        # Determine reference file path
        ref_file = struct_file.replace('.struct.json', '.struct-ref.avsc')
        
        if not os.path.exists(ref_file):
            self.skipTest(f"Reference file {ref_file} not found")
        
        # Load the struct file
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure = json.load(f)
        
        # Convert using the converter
        result = self.converter.convert(structure)
        
        # Load the reference output
        with open(ref_file, 'r', encoding='utf-8') as f:
            expected = json.load(f)
        
        # Compare the results
        self.assertEqual(
            result, 
            expected,
            f"Conversion of {struct_file} does not match reference output in {ref_file}"
        )


class TestInlineObjectConversion(unittest.TestCase):
    """Inline JSON Structure ``object`` types must produce valid Avro (issue #336).

    A property-less object previously emitted the bare literal ``"object"``,
    which is not a valid Avro type and also discarded any nested shape. These
    tests pin the corrected behavior: open objects -> Avro ``map``, objects with
    ``properties`` -> nested Avro ``record``, and every emitted schema parses.
    """

    def setUp(self):
        self.converter = JsonStructureToAvro()

    @staticmethod
    def _assert_parses(schema):
        from fastavro.schema import parse_schema
        import copy
        parse_schema(copy.deepcopy(schema))

    def _assert_no_bare_object_type(self, node):
        """Recursively assert no Avro *type position* holds a bare object/array."""
        if isinstance(node, str):
            self.assertNotIn(node, ('object',),
                             "bare 'object' string found in a type position")
        elif isinstance(node, list):
            for item in node:
                self._assert_no_bare_object_type(item)
        elif isinstance(node, dict):
            self.assertNotEqual(node.get('type'), 'object',
                                "dict with type=='object' found (invalid Avro)")
            for value in node.values():
                self._assert_no_bare_object_type(value)

    def test_issue_336_property_less_object_is_valid_map(self):
        """Exact reproducer from issue #336: open object -> map<string>, parseable."""
        repro = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/repro/objlit",
            "name": "ObjLit",
            "type": "object",
            "properties": {
                "Id": {"type": "string"},
                "Dimension": {"type": "object"},
            },
        }
        result = self.converter.convert(repro)
        self._assert_no_bare_object_type(result)
        self._assert_parses(result)

        dimension = next(f for f in result['fields'] if f['name'] == 'Dimension')
        # Optional field => nullable union wrapping the map.
        self.assertIn({'type': 'map', 'values': 'string'}, dimension['type'])

    def test_required_open_object_maps_to_string_map(self):
        """A required open object becomes a bare map<string> (no null wrapper)."""
        structure = {
            "type": "object",
            "name": "Root",
            "properties": {"payload": {"type": "object"}},
            "required": ["payload"],
        }
        result = self.converter.convert(structure)
        payload = next(f for f in result['fields'] if f['name'] == 'payload')
        self.assertEqual(payload['type'], {'type': 'map', 'values': 'string'})
        self._assert_parses(result)

    def test_object_with_properties_becomes_nested_record(self):
        """An inline object WITH properties must keep its shape as a nested record."""
        structure = {
            "type": "object",
            "name": "Root",
            "properties": {
                "dimension": {
                    "type": "object",
                    "properties": {
                        "width": {"type": "int32"},
                        "height": {"type": "int32"},
                    },
                    "required": ["width", "height"],
                }
            },
            "required": ["dimension"],
        }
        result = self.converter.convert(structure)
        self._assert_no_bare_object_type(result)
        self._assert_parses(result)

        dim = next(f for f in result['fields'] if f['name'] == 'dimension')
        self.assertEqual(dim['type']['type'], 'record')
        self.assertEqual(dim['type']['name'], 'DimensionType')
        field_names = {fld['name'] for fld in dim['type']['fields']}
        self.assertEqual(field_names, {'width', 'height'})

    def test_object_additional_properties_typed_map(self):
        """additionalProperties as a type schema controls the map value type."""
        structure = {
            "type": "object",
            "name": "Root",
            "properties": {
                "counts": {
                    "type": "object",
                    "additionalProperties": {"type": "int32"},
                }
            },
            "required": ["counts"],
        }
        result = self.converter.convert(structure)
        counts = next(f for f in result['fields'] if f['name'] == 'counts')
        self.assertEqual(counts['type'], {'type': 'map', 'values': 'int'})
        self._assert_parses(result)

    def test_object_additional_properties_string_typename(self):
        """additionalProperties given as a type *name* string also maps cleanly."""
        structure = {
            "type": "object",
            "name": "Root",
            "properties": {
                "flags": {"type": "object", "additionalProperties": "boolean"}
            },
            "required": ["flags"],
        }
        result = self.converter.convert(structure)
        flags = next(f for f in result['fields'] if f['name'] == 'flags')
        self.assertEqual(flags['type'], {'type': 'map', 'values': 'boolean'})
        self._assert_parses(result)

    def test_array_of_inline_objects(self):
        """Arrays whose items are inline objects-with-properties stay structured."""
        structure = {
            "type": "object",
            "name": "Root",
            "properties": {
                "points": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"x": {"type": "int32"}},
                        "required": ["x"],
                    },
                }
            },
            "required": ["points"],
        }
        result = self.converter.convert(structure)
        self._assert_no_bare_object_type(result)
        self._assert_parses(result)
        points = next(f for f in result['fields'] if f['name'] == 'points')
        self.assertEqual(points['type']['type'], 'array')
        self.assertEqual(points['type']['items']['type'], 'record')

    def test_union_with_bare_object_and_array_members(self):
        """A union listing bare 'object'/'array' members must remain valid Avro."""
        structure = {
            "type": "object",
            "name": "Root",
            "properties": {
                "anything": {"type": ["string", "int32", "array", "object", "null"]}
            },
            "required": ["anything"],
        }
        result = self.converter.convert(structure)
        self._assert_no_bare_object_type(result)
        self._assert_parses(result)
        anything = next(f for f in result['fields'] if f['name'] == 'anything')
        self.assertIn({'type': 'map', 'values': 'string'}, anything['type'])
        self.assertIn({'type': 'array', 'items': 'string'}, anything['type'])

    def test_deeply_nested_inline_objects_preserved(self):
        """Deeply nested inline objects are preserved (no data loss) and parse."""
        structure = {
            "type": "object",
            "name": "Root",
            "properties": {
                "a": {
                    "type": "object",
                    "properties": {
                        "b": {
                            "type": "object",
                            "properties": {"c": {"type": "string"}},
                            "required": ["c"],
                        }
                    },
                    "required": ["b"],
                }
            },
            "required": ["a"],
        }
        result = self.converter.convert(structure)
        self._assert_no_bare_object_type(result)
        self._assert_parses(result)
        a = next(f for f in result['fields'] if f['name'] == 'a')
        b = next(f for f in a['type']['fields'] if f['name'] == 'b')
        c = next(f for f in b['type']['fields'] if f['name'] == 'c')
        self.assertEqual(c['type'], 'string')

    def test_like_named_objects_get_unique_record_names(self):
        """Like-named inline objects must yield unique Avro record names."""
        structure = {
            "type": "object",
            "name": "Root",
            "properties": {
                "outer": {
                    "type": "object",
                    "properties": {
                        "meta": {
                            "type": "object",
                            "properties": {"k": {"type": "string"}},
                            "required": ["k"],
                        }
                    },
                    "required": ["meta"],
                },
                "meta": {
                    "type": "object",
                    "properties": {"v": {"type": "string"}},
                    "required": ["v"],
                },
            },
            "required": ["outer", "meta"],
        }
        result = self.converter.convert(structure)
        # Must parse: duplicate record names would raise a redefinition error.
        self._assert_parses(result)

        names = []

        def collect(node):
            if isinstance(node, dict):
                if node.get('type') == 'record':
                    names.append(node['name'])
                for v in node.values():
                    collect(v)
            elif isinstance(node, list):
                for v in node:
                    collect(v)

        collect(result)
        self.assertEqual(len(names), len(set(names)), f"record names not unique: {names}")

    def test_fastavro_roundtrip_through_nested_record(self):
        """Data written/read through a converted nested-record schema survives."""
        from fastavro import writer, reader
        from fastavro.schema import parse_schema

        structure = {
            "type": "object",
            "name": "Order",
            "properties": {
                "id": {"type": "string"},
                "shipTo": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"},
                        "zip": {"type": "string"},
                    },
                    "required": ["city", "zip"],
                },
                "attributes": {"type": "object"},
            },
            "required": ["id", "shipTo", "attributes"],
        }
        schema = self.converter.convert(structure)
        parsed = parse_schema(schema)

        record = {
            "id": "order-1",
            "shipTo": {"city": "Seattle", "zip": "98101"},
            "attributes": {"gift": "true", "priority": "high"},
        }
        buf = io.BytesIO()
        writer(buf, parsed, [record])
        buf.seek(0)
        out = list(reader(buf))
        self.assertEqual(out[0], record)

    def test_all_struct_fixtures_emit_no_bare_object(self):
        """No committed JSON Structure fixture may emit a bare ``object`` type.

        This is the core #336 invariant across the whole fixture corpus. (Full
        fastavro parseability is asserted in the self-contained tests above;
        a few fixtures exercise an unrelated pre-existing forward-reference in
        multi-type list emission, which is outside the scope of this fix.)
        """
        import glob

        fixtures = sorted(glob.glob('test/struct/*.struct.json'))
        self.assertTrue(fixtures, "no struct fixtures discovered")
        for path in fixtures:
            with self.subTest(fixture=path):
                with open(path, 'r', encoding='utf-8') as fh:
                    structure = json.load(fh)
                result = JsonStructureToAvro().convert(structure)
                self._assert_no_bare_object_type(result)


class TestNameSanitization(unittest.TestCase):
    """Issues #382 / #383: sanitize non-identifier property and enum-symbol names."""

    def setUp(self):
        self.converter = JsonStructureToAvro()

    @staticmethod
    def _assert_parses_strict(schema):
        """Parse with the reference avro lib (strict on enum symbols) and fastavro."""
        import avro.schema as avro_schema
        from fastavro.schema import parse_schema
        import copy
        avro_schema.parse(json.dumps(schema))
        parse_schema(copy.deepcopy(schema))

    def test_hyphenated_field_names_get_sanitized_with_aliases(self):
        """Property keys like 'dr-type' become valid Avro names with the original as alias."""
        structure = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/repro/382",
            "name": "Ev",
            "type": "object",
            "properties": {
                "dr-type": {"type": "string"},
                "kar.id": {"type": "string"},
            },
            "required": ["dr-type", "kar.id"],
        }
        result = self.converter.convert(structure)
        self._assert_parses_strict(result)
        fields = {f['name']: f for f in result['fields']}
        self.assertIn('dr_type', fields)
        self.assertIn('kar_id', fields)
        self.assertEqual(fields['dr_type'].get('aliases'), ['dr-type'])
        self.assertEqual(fields['kar_id'].get('aliases'), ['kar.id'])

    def test_numeric_field_name_prefixed(self):
        """A numeric property key like '1' becomes '_1' with the original aliased."""
        structure = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/repro/382num",
            "name": "Ev",
            "type": "object",
            "properties": {"1": {"type": "int32"}},
            "required": ["1"],
        }
        result = self.converter.convert(structure)
        self._assert_parses_strict(result)
        field = result['fields'][0]
        self.assertEqual(field['name'], '_1')
        self.assertEqual(field.get('aliases'), ['1'])

    def test_clean_field_name_has_no_alias(self):
        """Identifier property keys are emitted verbatim with no spurious alias."""
        structure = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/repro/clean",
            "name": "Ev",
            "type": "object",
            "properties": {"status": {"type": "string"}},
            "required": ["status"],
        }
        result = self.converter.convert(structure)
        self._assert_parses_strict(result)
        field = result['fields'][0]
        self.assertEqual(field['name'], 'status')
        self.assertNotIn('aliases', field)

    def _root_enum(self, values, default=None):
        schema = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "$id": "urn:repro:bad-enum",
            "name": "BadEnum",
            "$root": "#/definitions/BadEnum",
            "definitions": {
                "BadEnum": {"name": "BadEnum", "type": "string", "enum": list(values)}
            },
        }
        if default is not None:
            schema["definitions"]["BadEnum"]["default"] = default
        return schema

    def test_root_enum_symbols_sanitized_and_parseable(self):
        """Issue #383: digit/slash/hyphen enum values become valid, parseable symbols."""
        result = self.converter.convert(self._root_enum(["1", "2", "N/A", "KAR-MQTT"]))
        self._assert_parses_strict(result)
        self.assertEqual(result['type'], 'enum')
        self.assertEqual(result['symbols'], ['_1', '_2', 'N_A', 'KAR_MQTT'])

    def test_enum_original_values_preserved_in_doc(self):
        """The original wire values are recoverable from the enum doc mapping."""
        result = self.converter.convert(self._root_enum(["1", "N/A"]))
        self.assertIn('doc', result)
        # The doc embeds a JSON object mapping sanitized symbol -> original value.
        start = result['doc'].index('{')
        mapping = json.loads(result['doc'][start:])
        self.assertEqual(mapping, {"_1": "1", "N_A": "N/A"})

    def test_enum_symbol_collision_uniqueness(self):
        """Distinct values that sanitize to the same base get unique suffixes."""
        result = self.converter.convert(self._root_enum(["N/A", "N-A", "N.A"]))
        self._assert_parses_strict(result)
        self.assertEqual(result['symbols'], ['N_A', 'N_A_1', 'N_A_2'])
        self.assertEqual(len(set(result['symbols'])), 3)

    def test_enum_default_maps_to_sanitized_symbol(self):
        """A non-identifier default value is remapped to its sanitized symbol."""
        result = self.converter.convert(self._root_enum(["1", "2", "N/A"], default="N/A"))
        self._assert_parses_strict(result)
        self.assertEqual(result['default'], 'N_A')
        self.assertIn(result['default'], result['symbols'])

    def test_sanitized_record_roundtrips_with_fastavro(self):
        """The sanitized schema is usable end-to-end: write then read a record."""
        from fastavro import writer, reader
        from fastavro.schema import parse_schema
        structure = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/repro/rt",
            "name": "Ev",
            "type": "object",
            "properties": {
                "dr-type": {"type": "string"},
                "1": {"type": "int32"},
            },
            "required": ["dr-type", "1"],
        }
        result = self.converter.convert(structure)
        parsed = parse_schema(result)
        buf = io.BytesIO()
        writer(buf, parsed, [{"dr_type": "start", "_1": 7}])
        buf.seek(0)
        out = list(reader(buf))
        self.assertEqual(out, [{"dr_type": "start", "_1": 7}])


if __name__ == '__main__':
    unittest.main()
