import os
import sys
import tempfile
from os import path, getcwd
import time

import pytest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize.avrotokusto import convert_avro_to_kusto_db, convert_avro_to_kusto_file
from testcontainers.core.container import DockerContainer
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

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
  

#class TestAvroToKusto(unittest.TestCase):
#"""Test cases for avrotize.avrotokusto module"""

@pytest.fixture(scope="module")
def kusto_container():
    """Starts a Kusto container for testing"""
    container = KustoContainer()
    container.start()
    time.sleep(5)
    kusto_client = KustoClient(KustoConnectionStringBuilder.with_az_cli_authentication(container.get_connection_string()))
    kusto_database = container.get_database_name()
    kusto_client.execute_mgmt("NetDefaultDB",
        f".create database {kusto_database} persist (" + \
         "@\"/kustodata/dbs/<YourDatabaseName>/md\"," + \
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
    convert_avro_to_kusto_db(avro_path, None, kusto_uri, kusto_database, True)

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
    
    kusto_client = KustoClient(KustoConnectionStringBuilder.with_az_cli_authentication(kusto_uri))
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
    cwd = os.getcwd()        
    avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
    kql_path = os.path.join(tempfile.gettempdir(), "avrotize", "address.kql")
    dir = os.path.dirname(kql_path)
    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)
    
    convert_avro_to_kusto_file(avro_path, None, kql_path, False)           

def test_convert_telemetry_avsc_to_kusto():
    """Test converting telemetry.avsc to telemetry.kql"""
    cwd = os.getcwd()        
    avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
    kql_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry.kql")
    dir = os.path.dirname(kql_path)
    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)
    
    convert_avro_to_kusto_file(avro_path, None, kql_path, True)
