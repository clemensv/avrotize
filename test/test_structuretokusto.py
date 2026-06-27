"""Tests for structuretokusto module"""
import json
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from testcontainers.core.container import DockerContainer
from avrotize.structuretokusto import convert_structure_to_kusto_file, convert_structure_to_kusto_db
import unittest
import os
import sys
import tempfile
import time
import re
import pytest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class KustoContainer(DockerContainer):
    """A container for running Kusto for testing"""

    def __init__(self, image="mcr.microsoft.com/azuredataexplorer/kustainer-linux:latest"):
        super().__init__(image)
        self.with_bind_ports(8080, 8080)
        self.with_env("ACCEPT_EULA", "Y")

    def get_connection_string(self):
        """Get the connection string for the Kusto container"""
        host = self.get_container_host_ip()
        return f"http://{host}:8080"

    def get_database_name(self):
        """Get the name of the database for testing"""
        return "avrotize_test"


@pytest.fixture(scope="module")
def kusto_container():
    """Starts a Kusto container for testing"""
    container = KustoContainer()
    container.start()
    time.sleep(5)
    kusto_client = KustoClient(KustoConnectionStringBuilder.with_token_provider(
        container.get_connection_string(), lambda *_: "token"))
    kusto_database = container.get_database_name()
    kusto_client.execute_mgmt("NetDefaultDB",
                              f".create database {kusto_database} persist (" +
                              "@\"/kustodata/dbs/<YourDatabaseName>/md\"," +
                              "@\"/kustodata/dbs/<YourDatabaseName>/data\")")
    yield container
    container.stop()


# pylint: disable=redefined-outer-name


def _parse_kusto_verbatim_string(literal: str) -> str:
    assert literal.startswith('@"') and literal.endswith('"')
    return literal[2:-1].replace('""', '"')


def test_convert_structure_docstrings_escape_kql_literals(tmp_path):
    """Structure-to-Kusto docstrings with embedded JSON are escaped once."""
    original_doc = 'Field whose description contains a nested JSON schema doc: { "doc": "Schema too large to inline. Please refer to the JSON Structure schema for more details." } with \\ and newline\nnext line'
    schema = {
        "$schema": "https://json-structure.org/meta/core/v0/",
        "$id": "https://example.org/schemas/repro",
        "name": "ReproEvent",
        "namespace": "Example.Repro",
        "type": "object",
        "properties": {
            "id": {"type": "string", "description": "Plain id"},
            "tricky": {
                "type": "object",
                "description": original_doc,
                "properties": {"v": {"type": "string"}},
            },
        },
    }
    struct_path = tmp_path / "schema.json"
    kql_path = tmp_path / "out.kql"
    struct_path.write_text(json.dumps(schema), encoding="utf-8")

    convert_structure_to_kusto_file(
        str(struct_path),
        "Example.Repro.ReproEvent",
        str(kql_path),
        False,
        False,
        True,
        "Example.Repro",
    )

    kql = kql_path.read_text(encoding="utf-8")
    assert "\\\\\"" not in kql

    column_match = re.search(r"\[tricky\]: (@\"(?:\"\"|[^\"])*\")", kql)
    assert column_match is not None
    column_doc_json = json.loads(_parse_kusto_verbatim_string(column_match.group(1)))
    assert column_doc_json["description"] == original_doc


def test_convert_address_struct_to_kusto_server(kusto_container):
    """Test converting address.struct.json to Kusto server"""
    cwd = os.getcwd()
    struct_path = os.path.join(cwd, "test", "avsc", "address-ref.struct.json")
    kusto_uri = kusto_container.get_connection_string()
    kusto_database = kusto_container.get_database_name()
    convert_structure_to_kusto_db(struct_path, None, kusto_uri,
                                 kusto_database, True, token_provider=lambda *_: "token")

    my_address_data = """{
        "type": "address",
        "postOfficeBox": "PO Box 1234",
        "extendedAddress": "Suite 100",
        "streetAddress": "123 Main St",
        "locality": "Anytown",
        "region": "WA",
        "postalCode": "98052",
        "countryName": "United States"
    }"""

    kusto_client = KustoClient(
        KustoConnectionStringBuilder.with_token_provider(kusto_uri, lambda *_: "token"))
    # Insert data into the table
    my_address_data = my_address_data.replace("\n", " ").replace("  ", "")
    query = f".ingest inline into table record with (format=\"json\", ingestionMappingReference=\"record_json_flat\" ) <| \n{my_address_data}\n"
    kusto_client.execute_mgmt(kusto_database, query)
    # Query the data from the table
    query = "record | limit 10"
    response = kusto_client.execute_query(kusto_database, query)
    response_rows = response.primary_results[0]
    assert len(response_rows) == 1


def test_convert_address_struct_to_kusto_server_qualified(kusto_container):
    """Test that --qualified-table-names produces a working Kusto table with a
    dotted, bracket-quoted identifier (['ns.name']) end-to-end. The namespace
    is inferred from the JSON Structure definitions/<seg>/<seg>/<type> path."""
    cwd = os.getcwd()
    struct_path = os.path.join(cwd, "test", "avsc", "address-ref.struct.json")
    kusto_uri = kusto_container.get_connection_string()
    kusto_database = kusto_container.get_database_name()
    # The address-ref.struct.json uses definitions/example/com/record, so the
    # derived namespace is "example.com".
    convert_structure_to_kusto_db(
        struct_path,
        None,
        kusto_uri,
        kusto_database,
        emit_cloudevents_columns=True,
        token_provider=lambda *_: "token",
        qualified_table_names=True,
    )

    qualified_table = "example.com.record"
    mapping_name = f"{qualified_table}_json_flat"

    my_address_data = """{
        "type": "address",
        "postOfficeBox": "PO Box 1234",
        "extendedAddress": "Suite 100",
        "streetAddress": "123 Main St",
        "locality": "Anytown",
        "region": "WA",
        "postalCode": "98052",
        "countryName": "United States"
    }""".replace("\n", " ").replace("  ", "")

    kusto_client = KustoClient(
        KustoConnectionStringBuilder.with_token_provider(kusto_uri, lambda *_: "token"))
    ingest_query = (
        f".ingest inline into table ['{qualified_table}'] "
        f"with (format=\"json\", ingestionMappingReference=\"{mapping_name}\") "
        f"<| \n{my_address_data}\n"
    )
    kusto_client.execute_mgmt(kusto_database, ingest_query)

    response = kusto_client.execute_query(
        kusto_database, f"['{qualified_table}'] | limit 10")
    response_rows = response.primary_results[0]
    assert len(response_rows) == 1


def test_convert_address_struct_to_kusto():
    """Test converting address.struct.json to address.kql"""
    convert_case("address", False, False)


def test_convert_address_struct_to_kusto_ce():
    """Test converting address.struct.json to address-ce.kql"""
    convert_case("address", True, False)


def test_convert_address_struct_to_kusto_ce_dt():
    """Test converting address.struct.json to address-ce-dt.kql"""
    convert_case("address", True, True)


def test_convert_structure_with_wrapped_nullable_types_to_kusto():
    """Test wrapped nullable and ref-backed property types."""
    schema = {
        "$schema": "https://json-structure.org/meta/extended/v0/#",
        "$id": "https://example.com/test/wrapped-types",
        "definitions": {
            "StringList": {
                "name": "StringList",
                "type": "array",
                "items": {"type": "string"}
            },
            "WrappedString": {
                "name": "WrappedString",
                "type": "string"
            }
        },
        "type": "object",
        "name": "WrappedTypes",
        "properties": {
            "identifier": {
                "type": "string"
            },
            "title": {
                "type": {"$ref": "#/definitions/WrappedString"}
            },
            "subtitle": {
                "type": ["null", "string"]
            },
            "description_lines": {
                "type": ["null", {"$ref": "#/definitions/StringList"}]
            }
        }
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        struct_path = os.path.join(temp_dir, "wrapped-types.struct.json")
        kql_path = os.path.join(temp_dir, "wrapped-types.kql")

        with open(struct_path, "w", encoding="utf-8") as struct_file:
            json.dump(schema, struct_file)

        convert_structure_to_kusto_file(
            struct_path, "WrappedTypes", kql_path, True, True)

        with open(kql_path, "r", encoding="utf-8") as kql_file:
            kql = kql_file.read()

    assert ".create-merge table [WrappedTypes]" in kql
    assert "[identifier]: string" in kql
    assert "[title]: string" in kql
    assert "[subtitle]: string" in kql
    assert "[description_lines]: dynamic" in kql
    assert "_cloudevents_dispatch | where (specversion == '1.0' and type == 'WrappedTypes')" in kql


def convert_case(file_base_name: str, emit_cloudevents_columns, emit_cloudevents_dispatch_table):
    """Convert a JSON Structure schema to Kusto query language"""
    cwd = os.getcwd()
    struct_path = os.path.join(cwd, "test", "avsc", file_base_name+"-ref.struct.json")
    kql_path = os.path.join(tempfile.gettempdir(
    ), "avrotize", file_base_name+('-ce' if emit_cloudevents_columns else '')+('-dt' if emit_cloudevents_dispatch_table else '')+"-struct.kql")
    kql_ref_path = os.path.join(cwd, "test", "avsc", file_base_name +
                                ('-ce' if emit_cloudevents_columns else '')+('-dt' if emit_cloudevents_dispatch_table else '')+"-struct-ref.kql")
    dir_name = os.path.dirname(kql_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_structure_to_kusto_file(
        struct_path, None, kql_path, emit_cloudevents_columns, emit_cloudevents_dispatch_table)
    
    # Compare with reference file if it exists
    if os.path.exists(kql_ref_path):
        with open(kql_path, 'r', encoding="utf-8") as file1, open(kql_ref_path, 'r', encoding="utf-8") as file2:
            content1 = file1.read()
            content2 = file2.read()
        assert content1 == content2


def test_kql_json_blocks_are_valid():
    """Test that all JSON blocks in generated KQL are valid JSON (no trailing commas)."""
    cwd = os.getcwd()
    struct_path = os.path.join(cwd, "test", "avsc", "address-ref.struct.json")
    kql_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-json-valid.kql")
    dir_name = os.path.dirname(kql_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_structure_to_kusto_file(struct_path, None, kql_path, True, True)

    with open(kql_path, 'r', encoding="utf-8") as f:
        content = f.read()

    # Extract all JSON blocks between ``` fences
    blocks = re.findall(r'```\n(.*?)\n```', content, re.DOTALL)
    assert len(blocks) > 0, "Expected at least one fenced JSON block"
    for i, block in enumerate(blocks):
        block = block.strip()
        if block.startswith('[') or block.startswith('{'):
            try:
                json.loads(block)
            except json.JSONDecodeError as e:
                raise AssertionError(
                    f"Invalid JSON in block {i}: {e}\n---\n{block}\n---")


if __name__ == '__main__':
    test_convert_address_struct_to_kusto()
    test_convert_address_struct_to_kusto_ce()
    test_convert_address_struct_to_kusto_ce_dt()
    test_kql_json_blocks_are_valid()
    print("All tests passed!")
