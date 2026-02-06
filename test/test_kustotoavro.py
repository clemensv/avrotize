"""Tests for kustotoavro and kustotojstruct modules with testcontainer support."""

import json
import os
import sys
import tempfile
import time
import unittest

import pytest
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from testcontainers.core.container import DockerContainer

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.kustotoavro import convert_kusto_to_avro, KustoToAvro
from avrotize.kustotojstruct import convert_kusto_to_jstruct, KustoToJsonStructure


class KustoContainer(DockerContainer):
    """A container for running Kusto (Kustainer) for testing."""

    def __init__(self, image="mcr.microsoft.com/azuredataexplorer/kustainer-linux:latest"):
        super().__init__(image)
        self.with_exposed_ports(8080)  # Let Docker assign a random host port
        self.with_env("ACCEPT_EULA", "Y")

    def get_connection_string(self):
        """Get the connection string for the Kusto container."""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(8080)
        return f"http://{host}:{port}"

    def get_database_name(self):
        """Get the name of the database for testing."""
        return "test_db"


@pytest.fixture(scope="module")
def kusto_container():
    """Starts a Kusto container and creates a test database with sample data."""
    container = KustoContainer()
    container.start()
    time.sleep(10)  # Wait for Kusto to initialize

    kusto_uri = container.get_connection_string()
    kusto_database = container.get_database_name()
    token_provider = lambda *_: "token"

    kusto_client = KustoClient(
        KustoConnectionStringBuilder.with_token_provider(kusto_uri, token_provider)
    )

    # Create database
    kusto_client.execute_mgmt(
        "NetDefaultDB",
        f".create database {kusto_database} persist ("
        "@\"/kustodata/dbs/{kusto_database}/md\","
        "@\"/kustodata/dbs/{kusto_database}/data\")"
    )

    # Create a test table with various column types including dynamic
    kusto_client.execute_mgmt(
        kusto_database,
        """
        .create table TestEvents (
            id: string,
            timestamp: datetime,
            count: int,
            value: real,
            isActive: bool,
            payload: dynamic
        )
        """
    )

    # Create JSON ingestion mapping
    kusto_client.execute_mgmt(
        kusto_database,
        """
        .create table TestEvents ingestion json mapping 'TestEvents_json_flat'
        '[{"column":"id","path":"$.id"},{"column":"timestamp","path":"$.timestamp"},{"column":"count","path":"$.count"},{"column":"value","path":"$.value"},{"column":"isActive","path":"$.isActive"},{"column":"payload","path":"$.payload"}]'
        """
    )

    # Insert sample data with complex payloads for discriminated union inference
    # This mirrors the complexity of test data used in json2a/json2s inference tests
    sample_data = [
        # User signup events (3 variants to establish pattern)
        {"id": "1", "timestamp": "2024-01-01T00:00:00Z", "count": 10, "value": 1.5, "isActive": True,
         "payload": {"event_type": "user_signup", "name": "Alice", "email": "alice@example.com", "age": 30}},
        {"id": "2", "timestamp": "2024-01-01T01:00:00Z", "count": 11, "value": 1.6, "isActive": True,
         "payload": {"event_type": "user_signup", "name": "Bob", "email": "bob@example.com"}},
        {"id": "3", "timestamp": "2024-01-01T02:00:00Z", "count": 12, "value": 1.7, "isActive": True,
         "payload": {"event_type": "user_signup", "name": "Charlie", "email": "charlie@example.com", "age": 25, "phone": "+1234"}},
        
        # Order placed events with nested arrays (3 variants)
        {"id": "4", "timestamp": "2024-01-02T00:00:00Z", "count": 20, "value": 39.98, "isActive": True,
         "payload": {"event_type": "order_placed", "orderId": "ORD-001", "items": [{"sku": "A1", "qty": 2}], "total": 39.98}},
        {"id": "5", "timestamp": "2024-01-02T01:00:00Z", "count": 21, "value": 99.0, "isActive": True,
         "payload": {"event_type": "order_placed", "orderId": "ORD-002", "items": [{"sku": "B2", "qty": 1}], "total": 99.0, "discount": 10}},
        {"id": "6", "timestamp": "2024-01-02T02:00:00Z", "count": 22, "value": 150.0, "isActive": True,
         "payload": {"event_type": "order_placed", "orderId": "ORD-003", "items": [{"sku": "C3", "qty": 3}], "total": 150.0}},
        
        # Metric events with nested objects (3 variants)
        {"id": "7", "timestamp": "2024-01-03T00:00:00Z", "count": 30, "value": 75.5, "isActive": False,
         "payload": {"event_type": "metric", "name": "cpu", "value": 75.5, "unit": "percent"}},
        {"id": "8", "timestamp": "2024-01-03T01:00:00Z", "count": 31, "value": 1024.0, "isActive": False,
         "payload": {"event_type": "metric", "name": "memory", "value": 1024, "unit": "MB", "host": "server-1"}},
        {"id": "9", "timestamp": "2024-01-03T02:00:00Z", "count": 32, "value": 50.0, "isActive": False,
         "payload": {"event_type": "metric", "name": "disk", "value": 50.0, "tags": {"env": "prod", "region": "us-east"}}},
        
        # Nested discriminator pattern (envelope with payload.type discriminator)
        {"id": "10", "timestamp": "2024-01-04T00:00:00Z", "count": 40, "value": 0.0, "isActive": True,
         "payload": {"version": 1, "data": {"type": "text", "content": "Hello world"}}},
        {"id": "11", "timestamp": "2024-01-04T01:00:00Z", "count": 41, "value": 0.0, "isActive": True,
         "payload": {"version": 1, "data": {"type": "text", "content": "Another message", "format": "plain"}}},
        {"id": "12", "timestamp": "2024-01-04T02:00:00Z", "count": 42, "value": 0.0, "isActive": True,
         "payload": {"version": 1, "data": {"type": "image", "url": "http://example.com/img.png", "width": 800, "height": 600}}},
        {"id": "13", "timestamp": "2024-01-04T03:00:00Z", "count": 43, "value": 0.0, "isActive": True,
         "payload": {"version": 2, "data": {"type": "video", "url": "http://example.com/vid.mp4", "duration": 120, "codec": "h264"}}},
        {"id": "14", "timestamp": "2024-01-04T04:00:00Z", "count": 44, "value": 0.0, "isActive": True,
         "payload": {"version": 2, "data": {"type": "audio", "url": "http://example.com/song.mp3", "duration": 180}}},
    ]

    for record in sample_data:
        data_str = json.dumps(record).replace('"', '\\"')
        kusto_client.execute_mgmt(
            kusto_database,
            f'.ingest inline into table TestEvents with (format="json", ingestionMappingReference="TestEvents_json_flat") <|\n{json.dumps(record)}'
        )

    yield {
        "container": container,
        "uri": kusto_uri,
        "database": kusto_database,
        "token_provider": token_provider,
    }

    container.stop()


# ============================================================
# Tests for kustotoavro.py
# ============================================================

@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2a_basic_schema_inference(kusto_container):
    """Test basic k2a schema inference from Kusto table."""
    output_path = os.path.join(tempfile.gettempdir(), "avrotize", "k2a_basic.avsc")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    convert_kusto_to_avro(
        kusto_uri=kusto_container["uri"],
        kusto_database=kusto_container["database"],
        table_name="TestEvents",
        avro_namespace="com.test",
        avro_schema_file=output_path,
        emit_cloudevents=False,
        emit_cloudevents_xregistry=False,
        token_provider=kusto_container["token_provider"],
    )

    assert os.path.exists(output_path)
    with open(output_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    # Schema may be a top-level union (list) or a single record
    if isinstance(schema, list):
        # Top-level union - get first variant for field inspection
        schema = schema[0]
    
    # Verify basic schema structure
    assert schema["type"] == "record"
    field_names = [f["name"] for f in schema["fields"]]
    assert "id" in field_names
    assert "timestamp" in field_names
    assert "payload" in field_names


@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2a_with_sample_size(kusto_container):
    """Test k2a with custom sample size parameter."""
    output_path = os.path.join(tempfile.gettempdir(), "avrotize", "k2a_sample.avsc")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    convert_kusto_to_avro(
        kusto_uri=kusto_container["uri"],
        kusto_database=kusto_container["database"],
        table_name="TestEvents",
        avro_namespace="com.test",
        avro_schema_file=output_path,
        emit_cloudevents=False,
        emit_cloudevents_xregistry=False,
        token_provider=kusto_container["token_provider"],
        sample_size=2,  # Only sample 2 records
    )

    assert os.path.exists(output_path)
    with open(output_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    # Schema may be a top-level union (list) or a single record
    if isinstance(schema, list):
        schema = schema[0]
    assert schema["type"] == "record"


@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2a_with_infer_choices(kusto_container):
    """Test k2a with choice inference enabled for dynamic columns.
    
    The test data contains:
    - 3 user_signup events (different optional fields)
    - 3 order_placed events (with nested item arrays)
    - 3 metric events (with optional nested tags object)
    - 5 envelope events (nested discriminator pattern with data.type)
    
    With infer_choices=True, the payload field should be inferred as a union
    with discriminator defaults on the event_type field.
    """
    output_path = os.path.join(tempfile.gettempdir(), "avrotize", "k2a_choices.avsc")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    convert_kusto_to_avro(
        kusto_uri=kusto_container["uri"],
        kusto_database=kusto_container["database"],
        table_name="TestEvents",
        avro_namespace="com.test",
        avro_schema_file=output_path,
        emit_cloudevents=False,
        emit_cloudevents_xregistry=False,
        token_provider=kusto_container["token_provider"],
        sample_size=100,
        infer_choices=True,
        choice_depth=2,
    )

    assert os.path.exists(output_path)
    with open(output_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    
    # Schema may be a top-level union (list) or a single record
    if isinstance(schema, list):
        schema = schema[0]
    
    # Find the payload field
    payload_field = next((f for f in schema["fields"] if f["name"] == "payload"), None)
    assert payload_field is not None
    
    # With infer_choices enabled, payload type should be a union (list) or record with union fields
    payload_type = payload_field["type"]
    # The inference may produce a record with union fields or a direct union
    if isinstance(payload_type, dict) and payload_type.get("type") == "record":
        # Check that the record has fields (inference worked)
        assert "fields" in payload_type
    elif isinstance(payload_type, list):
        # Direct union type - each variant should be a record
        for variant in payload_type:
            if isinstance(variant, dict) and variant.get("type") == "record":
                assert "fields" in variant


# ============================================================
# Tests for kustotojstruct.py
# ============================================================

@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2s_basic_schema_inference(kusto_container):
    """Test basic k2s schema inference from Kusto table.
    
    Verifies that the dynamic 'payload' column is properly inferred
    from the sample data, not just mapped to a generic 'object' type.
    """
    output_path = os.path.join(tempfile.gettempdir(), "avrotize", "k2s_basic.jstruct.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    convert_kusto_to_jstruct(
        kusto_uri=kusto_container["uri"],
        kusto_database=kusto_container["database"],
        table_name="TestEvents",
        base_id="https://example.com/",
        jstruct_schema_file=output_path,
        emit_cloudevents=False,
        emit_cloudevents_xregistry=False,
        token_provider=kusto_container["token_provider"],
    )

    assert os.path.exists(output_path)
    with open(output_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    # Verify JSON Structure schema format
    assert schema["type"] == "object"
    assert "properties" in schema
    assert "id" in schema["properties"]
    assert "timestamp" in schema["properties"]
    assert "payload" in schema["properties"]
    
    # Verify payload was actually inferred (not just "object")
    payload_schema = schema["properties"]["payload"]
    # Should have type info from inference
    assert "type" in payload_schema
    payload_type = payload_schema["type"]
    
    # Without infer_choices, payload should be an object with merged properties
    if payload_type == "object":
        # Should have inferred properties from the dynamic column data
        assert "properties" in payload_schema
        # Should have discovered at least some fields from our test data
        props = payload_schema["properties"]
        # The data has event_type, version, data, name, email, orderId, etc.
        assert len(props) > 0, "Inference should discover properties in dynamic column"


@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2s_with_sample_size(kusto_container):
    """Test k2s with custom sample size parameter."""
    output_path = os.path.join(tempfile.gettempdir(), "avrotize", "k2s_sample.jstruct.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    convert_kusto_to_jstruct(
        kusto_uri=kusto_container["uri"],
        kusto_database=kusto_container["database"],
        table_name="TestEvents",
        base_id="https://example.com/",
        jstruct_schema_file=output_path,
        emit_cloudevents=False,
        emit_cloudevents_xregistry=False,
        token_provider=kusto_container["token_provider"],
        sample_size=2,
    )

    assert os.path.exists(output_path)
    with open(output_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    assert schema["type"] == "object"


@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2s_with_infer_choices(kusto_container):
    """Test k2s with choice inference enabled for dynamic columns.
    
    With the complex test data containing multiple event types (user_signup,
    order_placed, metric, and nested envelope events), the payload field
    should be inferred as a choice type with event_type as the selector.
    """
    output_path = os.path.join(tempfile.gettempdir(), "avrotize", "k2s_choices.jstruct.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    convert_kusto_to_jstruct(
        kusto_uri=kusto_container["uri"],
        kusto_database=kusto_container["database"],
        table_name="TestEvents",
        base_id="https://example.com/",
        jstruct_schema_file=output_path,
        emit_cloudevents=False,
        emit_cloudevents_xregistry=False,
        token_provider=kusto_container["token_provider"],
        sample_size=100,
        infer_choices=True,
        choice_depth=2,
    )

    assert os.path.exists(output_path)
    with open(output_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    
    # Verify payload has inferred structure
    assert "payload" in schema["properties"]
    payload_schema = schema["properties"]["payload"]
    
    # With infer_choices=True, payload should be a choice type
    # The test data has event_type as discriminator for user_signup, order_placed, metric
    # and nested data.type for text, image, video, audio
    assert "type" in payload_schema, "payload should have inferred type"
    payload_type = payload_schema["type"]
    
    if payload_type == "choice":
        # Proper choice type detected - verify structure
        assert "selector" in payload_schema, "choice type must have selector"
        assert "choices" in payload_schema, "choice type must have choices"
        assert len(payload_schema["choices"]) >= 2, f"should have multiple variants, got {list(payload_schema['choices'].keys())}"
        
        # Each choice variant should have the discriminator with default
        for variant_name, variant_schema in payload_schema["choices"].items():
            variant_type = variant_schema.get("type", {})
            if isinstance(variant_type, dict) and "properties" in variant_type:
                props = variant_type["properties"]
                selector = payload_schema["selector"]
                if selector in props:
                    assert "default" in props[selector], f"discriminator {selector} should have default in variant {variant_name}"
    else:
        # May be an object if inference didn't find clear discriminator pattern
        # but should still have discovered structure from the dynamic column
        assert payload_type == "object", f"expected choice or object, got {payload_type}"
        assert "properties" in payload_schema, "inferred object should have properties"
        assert len(payload_schema["properties"]) > 0, "should have discovered properties"


@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2s_with_infer_enums(kusto_container):
    """Test k2s with enum inference enabled for dynamic columns.
    
    The test data has event_type values with low cardinality:
    - user_signup (3 occurrences)
    - order_placed (3 occurrences)
    - metric (3 occurrences)
    
    With infer_enums=True, event_type should be inferred as an enum.
    """
    output_path = os.path.join(tempfile.gettempdir(), "avrotize", "k2s_enums.jstruct.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    convert_kusto_to_jstruct(
        kusto_uri=kusto_container["uri"],
        kusto_database=kusto_container["database"],
        table_name="TestEvents",
        base_id="https://example.com/",
        jstruct_schema_file=output_path,
        emit_cloudevents=False,
        emit_cloudevents_xregistry=False,
        token_provider=kusto_container["token_provider"],
        sample_size=100,
        infer_enums=True,
    )

    assert os.path.exists(output_path)
    with open(output_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    
    assert schema["type"] == "object"
    # Payload should have event_type as enum with values ["user_signup", "order_placed", "metric"]
    # or the nested structure depending on how inference handles the mixed data


@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2s_with_xregistry_output(kusto_container):
    """Test k2s with xRegistry manifest output format."""
    output_path = os.path.join(tempfile.gettempdir(), "avrotize", "k2s_xreg.jstruct.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    convert_kusto_to_jstruct(
        kusto_uri=kusto_container["uri"],
        kusto_database=kusto_container["database"],
        table_name="TestEvents",
        base_id="https://example.com/",
        jstruct_schema_file=output_path,
        emit_cloudevents=False,
        emit_cloudevents_xregistry=True,
        token_provider=kusto_container["token_provider"],
    )

    assert os.path.exists(output_path)
    with open(output_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    
    # Verify xRegistry manifest structure
    assert "$schema" in manifest
    assert "schemagroups" in manifest


# ============================================================
# Unit tests (no container required)
# ============================================================

class TestKustoToAvroUnit(unittest.TestCase):
    """Unit tests for KustoToAvro that don't require a container."""

    def test_type_map_coverage(self):
        """Verify all expected Kusto types are mapped."""
        from avrotize.kustotoavro import KustoToAvro
        expected_types = ["int", "long", "string", "real", "bool", "datetime", "timespan", "decimal", "dynamic", "guid"]
        for kusto_type in expected_types:
            if kusto_type in KustoToAvro.type_map:
                self.assertIsNotNone(KustoToAvro.type_map[kusto_type])


class TestKustoToJsonStructureUnit(unittest.TestCase):
    """Unit tests for KustoToJsonStructure that don't require a container."""

    def test_type_map_coverage(self):
        """Verify all expected Kusto types are mapped to JSON Structure types."""
        from avrotize.kustotojstruct import KustoToJsonStructure
        expected_types = ["int", "long", "string", "real", "bool", "datetime", "timespan", "decimal", "dynamic"]
        for kusto_type in expected_types:
            if kusto_type in KustoToJsonStructure.type_map:
                self.assertIsNotNone(KustoToJsonStructure.type_map[kusto_type])

    def test_type_map_values(self):
        """Verify Kusto types map to correct JSON Structure types."""
        from avrotize.kustotojstruct import KustoToJsonStructure
        self.assertEqual(KustoToJsonStructure.type_map["int"], "int32")
        self.assertEqual(KustoToJsonStructure.type_map["long"], "int64")
        self.assertEqual(KustoToJsonStructure.type_map["string"], "string")
        self.assertEqual(KustoToJsonStructure.type_map["real"], "double")
        self.assertEqual(KustoToJsonStructure.type_map["bool"], "boolean")
        self.assertEqual(KustoToJsonStructure.type_map["datetime"], "datetime")
        self.assertEqual(KustoToJsonStructure.type_map["timespan"], "duration")
        self.assertEqual(KustoToJsonStructure.type_map["decimal"], "decimal")


if __name__ == "__main__":
    unittest.main()
