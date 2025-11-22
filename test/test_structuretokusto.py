"""Tests for structuretokusto module"""
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from testcontainers.core.container import DockerContainer
from avrotize.structuretokusto import convert_structure_to_kusto_file, convert_structure_to_kusto_db
import unittest
import os
import sys
import tempfile
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


def test_convert_address_struct_to_kusto():
    """Test converting address.struct.json to address.kql"""
    convert_case("address", False, False)


def test_convert_address_struct_to_kusto_ce():
    """Test converting address.struct.json to address-ce.kql"""
    convert_case("address", True, False)


def test_convert_address_struct_to_kusto_ce_dt():
    """Test converting address.struct.json to address-ce-dt.kql"""
    convert_case("address", True, True)


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


if __name__ == '__main__':
    test_convert_address_struct_to_kusto()
    test_convert_address_struct_to_kusto_ce()
    test_convert_address_struct_to_kusto_ce_dt()
    print("All tests passed!")
