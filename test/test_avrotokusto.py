from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from testcontainers.core.container import DockerContainer
from avrotize.avrotokusto import convert_avro_to_kusto_db, convert_avro_to_kusto_file
from unittest.mock import patch
import unittest
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


def convert_case(file_base_name: str, emit_cloud_events_columns, emit_cloudevents_dispatch_table):
    """Convert an Avro schema to Kusto query language"""
    cwd = os.getcwd()
    avro_path = os.path.join(cwd, "test", "avsc", file_base_name+".avsc")
    kql_path = os.path.join(tempfile.gettempdir(
    ), "avrotize", file_base_name+('-ce' if emit_cloud_events_columns else '')+('-dt' if emit_cloudevents_dispatch_table else '')+".kql")
    kql_ref_path = os.path.join(cwd, "test", "avsc", file_base_name +
                                ('-ce' if emit_cloud_events_columns else '')+('-dt' if emit_cloudevents_dispatch_table else '')+"-ref.kql")
    dir_name = os.path.dirname(kql_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_avro_to_kusto_file(
        avro_path, None, kql_path, emit_cloud_events_columns, emit_cloudevents_dispatch_table)
    if os.path.exists(kql_ref_path):
        with open(kql_path, 'r', encoding="utf-8") as file1, open(kql_ref_path, 'r', encoding="utf-8") as file2:
            content1 = file1.read()
            content2 = file2.read()
        assert content1 == content2
