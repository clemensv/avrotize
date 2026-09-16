from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from testcontainers.core.container import DockerContainer
from avrotize.avrotokusto import convert_avro_to_kusto_db, convert_avro_to_kusto_file
from unittest.mock import patch
import unittest
import json
import re
import os
import sys
import tempfile
from os import path, getcwd
import time

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


# class TestAvroToKusto(unittest.TestCase):
# """Test cases for avrotize.avrotokusto module"""

@pytest.fixture(scope="module")
def kusto_container():
    """Starts a Kusto container for testing"""
    container = KustoContainer()
    container.start()
    kusto_client = KustoClient(KustoConnectionStringBuilder.with_token_provider(
        container.get_connection_string(), lambda *_: "token"))
    kusto_database = container.get_database_name()
    # Wait for the Kusto emulator to be ready (it can take a while to start)
    for attempt in range(30):
        try:
            kusto_client.execute_mgmt("NetDefaultDB", ".show cluster")
            break
        except Exception:
            time.sleep(5)
    else:
        pytest.skip("Kusto emulator did not become ready in time")
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


def test_convert_avro_docstrings_escape_kql_literals(tmp_path):
    """Docstrings with JSON-sensitive characters are escaped once for KQL."""
    original_table_doc = 'Table doc has "quotes", a backslash \\, and a newline\nnext line'
    original_field_doc = 'Field doc embeds JSON: { "doc": "see \\path" } and newline\nnext line'
    schema = {
        "type": "record",
        "name": "EscapedDocs",
        "namespace": "example.docs",
        "doc": original_table_doc,
        "fields": [
            {"name": "id", "type": "string", "doc": original_field_doc}
        ],
    }
    avro_path = tmp_path / "escaped-docs.avsc"
    kql_path = tmp_path / "escaped-docs.kql"
    avro_path.write_text(json.dumps(schema), encoding="utf-8")

    convert_avro_to_kusto_file(str(avro_path), None, str(kql_path), False, False)

    kql = kql_path.read_text(encoding="utf-8")
    double_escaped_quote = "\\\\\""
    assert double_escaped_quote not in kql

    table_match = re.search(r"\.alter table \[EscapedDocs\] docstring (@\"(?:\"\"|[^\"])*\");", kql)
    assert table_match is not None
    table_doc_json = json.loads(_parse_kusto_verbatim_string(table_match.group(1)))
    assert table_doc_json["description"] == original_table_doc

    column_match = re.search(r"\[id\]: (@\"(?:\"\"|[^\"])*\")", kql)
    assert column_match is not None
    column_doc_json = json.loads(_parse_kusto_verbatim_string(column_match.group(1)))
    assert column_doc_json["description"] == original_field_doc


def test_convert_address_avsc_to_kusto_server(kusto_container):
    """Test converting address.avsc to address.kql"""
    cwd = os.getcwd()
    avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
    kusto_uri = kusto_container.get_connection_string()
    kusto_database = kusto_container.get_database_name()
    convert_avro_to_kusto_db(avro_path, None, kusto_uri,
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


def test_convert_address_avsc_to_kusto_server_qualified(kusto_container):
    """Test that --qualified-table-names plus --namespace produces a working Kusto
    table with a dotted, bracket-quoted identifier (['ns.name']) end-to-end."""
    cwd = os.getcwd()
    avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
    kusto_uri = kusto_container.get_connection_string()
    kusto_database = kusto_container.get_database_name()
    namespace = "test.acme"
    convert_avro_to_kusto_db(
        avro_path,
        None,
        kusto_uri,
        kusto_database,
        emit_cloudevents_columns=True,
        token_provider=lambda *_: "token",
        qualified_table_names=True,
        namespace=namespace,
    )

    qualified_table = f"{namespace}.record"
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


def test_convert_address_avsc_to_kusto():
    """Test converting address.avsc to address.kql"""
    convert_case("address", True, True)
    convert_case("address", True, False)
    convert_case("address", False, False)


def test_convert_telemetry_avsc_to_kusto():
    """Test converting address.avsc to address.kql"""
    convert_case("telemetry", True, True)
    convert_case("telemetry", True, False)
    convert_case("telemetry", False, False)


def convert_case(file_base_name: str, emit_cloudevents_columns, emit_cloudevents_dispatch_table):
    """Convert an Avro schema to Kusto query language"""
    cwd = os.getcwd()
    avro_path = os.path.join(cwd, "test", "avsc", file_base_name+".avsc")
    kql_path = os.path.join(tempfile.gettempdir(
    ), "avrotize", file_base_name+('-ce' if emit_cloudevents_columns else '')+('-dt' if emit_cloudevents_dispatch_table else '')+".kql")
    kql_ref_path = os.path.join(cwd, "test", "avsc", file_base_name +
                                ('-ce' if emit_cloudevents_columns else '')+('-dt' if emit_cloudevents_dispatch_table else '')+"-ref.kql")
    dir_name = os.path.dirname(kql_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_avro_to_kusto_file(
        avro_path, None, kql_path, emit_cloudevents_columns, emit_cloudevents_dispatch_table)
    if os.path.exists(kql_ref_path):
        with open(kql_path, 'r', encoding="utf-8") as file1, open(kql_ref_path, 'r', encoding="utf-8") as file2:
            content1 = file1.read()
            content2 = file2.read()
        assert content1 == content2
