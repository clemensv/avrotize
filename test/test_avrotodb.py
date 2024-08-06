import os
import re
import shutil
import sys
import tempfile
import unittest
from os import path, getcwd
import json
import pytest
import sqlalchemy
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.mongodb import MongoDbContainer
from testcontainers.mssql import SqlServerContainer
from testcontainers.oracle import OracleDbContainer
from testcontainers.cassandra import CassandraContainer
import pymysql
import psycopg2
import pymongo
import pyodbc
import oracledb
from cassandra.cluster import Cluster, DCAwareRoundRobinPolicy
import asyncio

from avrotize.common import altname

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize.avrotodb import convert_avro_to_sql, convert_avro_to_nosql


class TestAvroToDB(unittest.TestCase):

    def setUp(self):
        self.avro_schema = {
            "type": "record",
            "name": "TestRecord",
            "namespace": "com.example",
            "fields": [
                {"name": "id", "type": "int", "doc": "Primary key"},
                {"name": "name", "type": "string", "doc": "Name of the record"},
                {"name": "email", "type": ["null", "string"], "doc": "Email address"}
            ],
            "unique": ["id"]
        }
        self.schema_json = json.dumps(self.avro_schema)
        with open("test_schema.avsc", "w", encoding="utf-8") as f:
            f.write(self.schema_json)

    def tearDown(self):
        os.remove("test_schema.avsc")

    def test_mysql_schema_creation(self):
        self.run_mysql_schema_creation("address")
        self.run_mysql_schema_creation("northwind")

    def run_mysql_schema_creation(self, avro_name: str):
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{avro_name}.avsc")
        sql_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{avro_name}-mysql.sql")

        with MySqlContainer() as mysql:
            conn = pymysql.connect(host=mysql.get_container_host_ip(),
                                   port=int(mysql.get_exposed_port(3306)),
                                   user=mysql.username,
                                   password=mysql.password,
                                   database=mysql.dbname)
            cursor = conn.cursor()
            cloudevents_columns_count = 5
            convert_avro_to_sql(avro_path, sql_path, "mysql", emit_cloudevents_columns=True)
            with open(sql_path, "r", encoding="utf-8") as sql_file:
                schema_sql = sql_file.read()
                for statement in schema_sql.split(";"):
                    if statement.strip():
                        cursor.execute(statement)
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            with open(avro_path, "r", encoding="utf-8") as avsc_file:
                avro_schema = json.load(avsc_file)
            for table_cols in tables:
                table = table_cols[0]
                if isinstance(avro_schema, list):
                    self.assertTrue(any([table == altname(t, "sql") for t in avro_schema]))
                else:
                    self.assertTrue(table == altname(avro_schema, "sql"))
                cursor.execute(f"DESCRIBE `{table}`;")
                columns = cursor.fetchall()
                if isinstance(avro_schema, list):
                    avro_record = [t for t in avro_schema if table == altname(t, "sql")][0]
                else:
                    avro_record = avro_schema
                self.assertEqual(len(columns), len(avro_record["fields"])+cloudevents_columns_count)  # id, name, email, and CloudEvents columns
        
            cursor.close()
            conn.close()

    def test_postgres_schema_creation(self):
        self.run_postgres_schema_creation("address")
        self.run_postgres_schema_creation("northwind")

    def run_postgres_schema_creation(self, avro_name):
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{avro_name}.avsc")
        sql_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{avro_name}-postgres.sql")
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(host=postgres.get_container_host_ip(),
                                    port=postgres.get_exposed_port(5432),
                                    user=postgres.username,
                                    password=postgres.password,
                                    database=postgres.dbname)
            cursor = conn.cursor()
            cloudevents_columns_count = 5
            convert_avro_to_sql(avro_path, sql_path, "postgres", emit_cloudevents_columns=True)
            with open(sql_path, "r", encoding="utf-8") as sql_file:
                schema_sql = sql_file.read()
                cursor.execute(schema_sql)
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = cursor.fetchall()
            with open(avro_path, "r", encoding="utf-8") as avsc_file:
                avro_schema = json.load(avsc_file)
            for table_cols in tables:
                table = table_cols[0]
                if isinstance(avro_schema, list):
                    self.assertTrue(any([table == altname(t, "sql") for t in avro_schema]))
                else:
                    self.assertTrue(table == altname(avro_schema, "sql"))
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';")
                columns = cursor.fetchall()
                if isinstance(avro_schema, list):
                    avro_record = [t for t in avro_schema if table == altname(t, "sql")][0]
                else:
                    avro_record = avro_schema
                self.assertEqual(len(columns), len(avro_record["fields"])+cloudevents_columns_count)  # id, name, email, and CloudEvents columns
            cursor.close()
            conn.close()

    def test_mongodb_schema_creation(self):
        self.run_mongodb_schema_creation("address")
        self.run_mongodb_schema_creation("northwind")

    def get_fullname(self, avro_schema: dict):
        name = avro_schema.get("name", "")
        namespace = avro_schema.get("namespace", "")
        return namespace + "." + name if namespace else name

    def run_mongodb_schema_creation(self, avro_name):
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{avro_name}.avsc")
        model_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{avro_name}-mongo")
        if os.path.exists(model_path):
            shutil.rmtree(model_path, ignore_errors=True)
        os.makedirs(model_path, exist_ok=True)
        with MongoDbContainer() as mongo:
            client = pymongo.MongoClient(mongo.get_connection_url())
            db = client.test
            convert_avro_to_nosql(avro_path, model_path, "mongodb", emit_cloudevents_columns=True)
            json_files = [f for f in os.listdir(model_path) if f.endswith('.json')]
            for json_file in json_files:
                with open(os.path.join(model_path, json_file), "r", encoding="utf-8") as json_file:
                    schema_json = json.load(json_file)
                    collection_name = list(schema_json.keys())[0]
                    db.create_collection(collection_name, validator=schema_json[collection_name])
            with open(avro_path, "r", encoding="utf-8") as avsc_file:
                avro_schema = json.load(avsc_file)
            collections = db.list_collection_names()
            for collection in collections:
                if isinstance(avro_schema, list):
                    self.assertTrue(any([collection.lower() == self.get_fullname(t).lower() for t in avro_schema]))
                else:
                    self.assertTrue(collection.lower() == self.get_fullname(avro_schema).lower())
            client.close()

    def test_mssql_schema_creation(self):
        """Test schema creation for Microsoft SQL Server database."""
        self.run_mssql_schema_creation("address")
        self.run_mssql_schema_creation("northwind")

    def run_mssql_schema_creation(self, avro_name):
        """Test schema creation for Microsoft SQL Server database."""
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{avro_name}.avsc")
        sql_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{avro_name}-mssql.sql")
        with SqlServerContainer("mcr.microsoft.com/azure-sql-edge:1.0.7") as mssql:
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={mssql.get_container_host_ip()},{mssql.get_exposed_port(1433)};UID={mssql.username};PWD={mssql.password}'
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cloudevents_columns_count = 5
            convert_avro_to_sql(avro_path, sql_path, "sqlserver", emit_cloudevents_columns=True)
            with open(sql_path, "r", encoding="utf-8") as sql_file:
                schema_sql = sql_file.read()
                for statement in schema_sql.split(";"):
                    if statement.strip().strip('\n'):
                        cursor.execute(statement)
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES;")
            tables = [t[0] for t in cursor.fetchall() if not t[0].startswith("spt_") and not t[0].startswith("MS")]
            with open(avro_path, "r", encoding="utf-8") as avsc_file:
                avro_schema = json.load(avsc_file)
            for table in tables:
                if isinstance(avro_schema, list):
                    self.assertTrue(any([table == altname(t, "sql") for t in avro_schema]))
                else:
                    self.assertTrue(table == altname(avro_schema, "sql"))
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';")
                columns = cursor.fetchall()
                if isinstance(avro_schema, list):
                    avro_record = [t for t in avro_schema if table == altname(t, "sql")][0]
                else:
                    avro_record = avro_schema
                self.assertEqual(len(columns), len(avro_record["fields"])+cloudevents_columns_count)  # id, name, email, and CloudEvents columns
            cursor.close()
            conn.close()

    def test_oracle_schema_creation(self):
        self.run_oracle_schema_creation("address")
        self.run_oracle_schema_creation("northwind")

    def run_oracle_schema_creation(self, avro_name):
        """Test schema creation for Oracle database."""
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", f"{avro_name}.avsc")
        sql_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{avro_name}-oracle.sql")
        with OracleDbContainer() as oracle:
            # Create an SQLAlchemy engine
            engine = sqlalchemy.create_engine(oracle.get_connection_url())
            cloudevents_columns_count = 5
            convert_avro_to_sql(avro_path, sql_path, "oracle", emit_cloudevents_columns=True)
            
            with engine.begin() as session:
                with open(sql_path, "r", encoding="utf-8") as sql_file:
                    schema_sql = sql_file.read()
                    for statement in schema_sql.split(";"):
                        if statement.strip().strip('\n'):
                            session.execute(sqlalchemy.text(statement))
                result = session.execute(sqlalchemy.text("SELECT table_name FROM user_tables"))
                tables = [row[0] for row in result.fetchall()]

                with open(avro_path, "r", encoding="utf-8") as avsc_file:
                    avro_schema = json.load(avsc_file)
                for table in tables:
                    if not isinstance(avro_schema, list):
                        avro_schema = [avro_schema]
                    found = next(table == altname(t, "sql").upper() for t in avro_schema)
                    if found:
                        result = session.execute(sqlalchemy.text(f"SELECT column_name FROM user_tab_columns WHERE table_name = '{table}'"))
                        columns = [row[0] for row in result.fetchall()]
                        if isinstance(avro_schema, list):
                            avro_record = [t for t in avro_schema if table == altname(t, "sql").upper()][0]
                        else:
                            avro_record = avro_schema
                        self.assertEqual(len(columns), len(avro_record["fields"]) + cloudevents_columns_count)  # id, name, email, and CloudEvents columns

    def test_cassandra_schema_creation(self):
        self.run_cassandra_schema_creation("address")
        self.run_cassandra_schema_creation("northwind")

    def run_cassandra_schema_creation(self, avro_name):
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{avro_name}.avsc")
        cql_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{avro_name}-cassandra.cql")
        with CassandraContainer() as cassandra:
            cluster = Cluster(contact_points=cassandra.get_contact_points(), 
                              port=cassandra.get_exposed_port(9042), 
                              protocol_version=4,
                              load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=cassandra.get_local_datacenter()))
            session = cluster.connect()
            keyspace = avro_name.lower()
            session.execute(f"CREATE KEYSPACE {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};")
            session.set_keyspace(keyspace)
            convert_avro_to_sql(avro_path, cql_path, "cassandra", emit_cloudevents_columns=True, schema_name=keyspace)
            with open(cql_path, "r", encoding="utf-8") as cql_file:
                cql_script = cql_file.read()
                statements = cql_script.split(";")
                for statement in statements:
                    if statement.strip():
                        session.execute(statement)
            # tables = session.execute(f"SELECT table_name FROM system_schema.tables;")
            # with open(avro_path, "r", encoding="utf-8") as avsc_file:
            #     avro_schema = json.load(avsc_file)
            # for table in tables:
            #     if isinstance(avro_schema, list):
            #         table_name = table.table_name
            #         columns = session.execute(f"SELECT column_name FROM system_schema.columns WHERE table_name = '{table_name}';")
            #         self.assertEqual(len(columns), len(avro_schema[0]["fields"]))  # Number of columns should match the number of fields in the Avro schema
            session.shutdown()
            cluster.shutdown()
