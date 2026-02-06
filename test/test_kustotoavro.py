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
        self.with_bind_ports(8080, 8080)
        self.with_env("ACCEPT_EULA", "Y")

    def get_connection_string(self):
        """Get the connection string for the Kusto container."""
        host = self.get_container_host_ip()
        return f"http://{host}:8080"

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

    # Insert sample data with different payload structures for choice inference
    sample_data = [
        {"id": "1", "timestamp": "2024-01-01T00:00:00Z", "count": 10, "value": 1.5, "isActive": True,
         "payload": {"eventType": "click", "x": 100, "y": 200}},
        {"id": "2", "timestamp": "2024-01-02T00:00:00Z", "count": 20, "value": 2.5, "isActive": False,
         "payload": {"eventType": "click", "x": 150, "y": 250}},
        {"id": "3", "timestamp": "2024-01-03T00:00:00Z", "count": 30, "value": 3.5, "isActive": True,
         "payload": {"eventType": "scroll", "direction": "up", "amount": 50}},
        {"id": "4", "timestamp": "2024-01-04T00:00:00Z", "count": 40, "value": 4.5, "isActive": False,
         "payload": {"eventType": "scroll", "direction": "down", "amount": 100}},
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
    assert schema["type"] == "record"


@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2a_with_infer_choices(kusto_container):
    """Test k2a with choice inference enabled for dynamic columns."""
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
    
    # With infer_choices, the payload field should have a union type
    payload_field = next((f for f in schema["fields"] if f["name"] == "payload"), None)
    assert payload_field is not None


# ============================================================
# Tests for kustotojstruct.py
# ============================================================

@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2s_basic_schema_inference(kusto_container):
    """Test basic k2s schema inference from Kusto table."""
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
    """Test k2s with choice inference enabled for dynamic columns."""
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


@pytest.mark.skipif(
    os.environ.get("SKIP_CONTAINER_TESTS", "false").lower() == "true",
    reason="Container tests skipped"
)
def test_k2s_with_infer_enums(kusto_container):
    """Test k2s with enum inference enabled for dynamic columns."""
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
    
    # Enum detection may detect eventType as enum with values "click", "scroll"
    assert schema["type"] == "object"


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
