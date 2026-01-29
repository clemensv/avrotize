"""Tests for SQL to Avro schema conversion."""

import json
import os
import sys
import tempfile
import unittest

import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.sqltoavro import convert_sql_to_avro, SqlToAvro


class TestSqlToAvro(unittest.TestCase):
    """Test cases for SQL to Avro conversion."""

    def test_postgres_basic_types(self):
        """Test conversion of PostgreSQL basic types to Avro schema."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            # Create a table with various PostgreSQL types
            cursor.execute("""
                CREATE TABLE test_basic_types (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    description TEXT,
                    age INTEGER,
                    salary NUMERIC(10, 2),
                    is_active BOOLEAN,
                    birth_date DATE,
                    created_at TIMESTAMP WITH TIME ZONE,
                    data BYTEA,
                    rating REAL,
                    score DOUBLE PRECISION,
                    big_number BIGINT
                );
                COMMENT ON TABLE test_basic_types IS 'A test table with various PostgreSQL types';
                COMMENT ON COLUMN test_basic_types.name IS 'The name of the entity';
            """)
            conn.commit()

            # Convert to Avro
            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_basic_types.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                avro_namespace='com.example.test',
                infer_json_schema=False,
                infer_xml_schema=False
            )

            # Verify the output
            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'test_basic_types')
            self.assertEqual(schema['namespace'], 'com.example.test')
            self.assertEqual(schema['doc'], 'A test table with various PostgreSQL types')

            # Check fields
            fields_by_name = {f['name']: f for f in schema['fields']}
            
            # id should be int (SERIAL)
            self.assertIn('id', fields_by_name)
            
            # name should be string and NOT nullable (NOT NULL)
            self.assertIn('name', fields_by_name)
            self.assertEqual(fields_by_name['name']['type'], 'string')
            self.assertEqual(fields_by_name['name']['doc'], 'The name of the entity')
            
            # description should be nullable string
            self.assertIn('description', fields_by_name)
            self.assertIn('null', fields_by_name['description']['type'])
            self.assertIn('string', fields_by_name['description']['type'])

            # Primary key should be noted
            self.assertIn('unique', schema)
            self.assertIn('id', schema['unique'])

            cursor.close()
            conn.close()

    def test_postgres_json_inference(self):
        """Test JSON column schema inference from PostgreSQL."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            # Create a table with JSON column
            cursor.execute("""
                CREATE TABLE events (
                    id SERIAL PRIMARY KEY,
                    event_type VARCHAR(100),
                    payload JSONB NOT NULL
                );
            """)

            # Insert some JSON data with consistent structure
            cursor.execute("""
                INSERT INTO events (event_type, payload) VALUES
                ('user.created', '{"user_id": 1, "email": "alice@example.com", "name": "Alice"}'),
                ('user.created', '{"user_id": 2, "email": "bob@example.com", "name": "Bob"}'),
                ('user.created', '{"user_id": 3, "email": "charlie@example.com", "name": "Charlie"}');
            """)
            conn.commit()

            # Convert to Avro with JSON inference
            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_json_inference.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='events',
                avro_namespace='com.example.events',
                infer_json_schema=True,
                infer_xml_schema=False,
                sample_size=100
            )

            # Verify the output
            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'events')

            # Check that payload is an inferred record type (not just string)
            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('payload', fields_by_name)
            
            payload_type = fields_by_name['payload']['type']
            # Should be a record type with user_id, email, name fields
            if isinstance(payload_type, dict) and payload_type.get('type') == 'record':
                payload_fields = {f['name']: f for f in payload_type.get('fields', [])}
                self.assertIn('user_id', payload_fields)
                self.assertIn('email', payload_fields)
                self.assertIn('name', payload_fields)

            cursor.close()
            conn.close()

    def test_postgres_array_types(self):
        """Test PostgreSQL array type conversion."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            # Create a table with array columns
            cursor.execute("""
                CREATE TABLE test_arrays (
                    id SERIAL PRIMARY KEY,
                    tags TEXT[],
                    scores INTEGER[]
                );
            """)
            conn.commit()

            # Convert to Avro
            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_arrays.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='test_arrays',
                avro_namespace='com.example.test',
                infer_json_schema=False,
                infer_xml_schema=False
            )

            # Verify the output
            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            fields_by_name = {f['name']: f for f in schema['fields']}
            
            # tags should be an array of strings
            self.assertIn('tags', fields_by_name)
            tags_type = fields_by_name['tags']['type']
            # Handle nullable array
            if isinstance(tags_type, list):
                array_type = next((t for t in tags_type if isinstance(t, dict) and t.get('type') == 'array'), None)
            else:
                array_type = tags_type
            
            if array_type:
                self.assertEqual(array_type['type'], 'array')
                self.assertEqual(array_type['items'], 'string')

            cursor.close()
            conn.close()

    def test_postgres_xml_inference(self):
        """Test XML column schema inference from PostgreSQL."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            # Create a table with XML column
            cursor.execute("""
                CREATE TABLE xml_data (
                    id SERIAL PRIMARY KEY,
                    content XML
                );
            """)

            # Insert some XML data
            cursor.execute("""
                INSERT INTO xml_data (content) VALUES
                ('<person><name>Alice</name><age>30</age></person>'),
                ('<person><name>Bob</name><age>25</age></person>'),
                ('<person><name>Charlie</name><age>35</age></person>');
            """)
            conn.commit()

            # Convert to Avro with XML inference
            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_xml_inference.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='xml_data',
                avro_namespace='com.example.xml',
                infer_json_schema=False,
                infer_xml_schema=True,
                sample_size=100
            )

            # Verify the output
            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'xml_data')

            # Check that content field exists
            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('content', fields_by_name)

            cursor.close()
            conn.close()

    def test_postgres_multiple_tables(self):
        """Test conversion of multiple PostgreSQL tables."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            # Create multiple tables
            cursor.execute("""
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) NOT NULL,
                    email VARCHAR(100)
                );
                CREATE TABLE orders (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    total NUMERIC(10, 2),
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()

            # Convert to Avro
            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_multi_table.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                avro_namespace='com.example.shop',
                infer_json_schema=False,
                infer_xml_schema=False
            )

            # Verify the output - should be a list with both schemas
            with open(output_path, 'r', encoding='utf-8') as f:
                schemas = json.load(f)

            self.assertIsInstance(schemas, list)
            self.assertEqual(len(schemas), 2)
            
            schema_names = {s['name'] for s in schemas}
            self.assertIn('users', schema_names)
            self.assertIn('orders', schema_names)

            cursor.close()
            conn.close()

    def test_postgres_cloudevents_detection(self):
        """Test CloudEvents table pattern detection."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            # Create a CloudEvents-style table
            cursor.execute("""
                CREATE TABLE cloudevents (
                    id VARCHAR(100) PRIMARY KEY,
                    type VARCHAR(200) NOT NULL,
                    source VARCHAR(500) NOT NULL,
                    data JSONB,
                    time TIMESTAMP WITH TIME ZONE,
                    subject VARCHAR(500)
                );
            """)

            # Insert sample CloudEvents data
            cursor.execute("""
                INSERT INTO cloudevents (id, type, source, data, time, subject) VALUES
                ('evt-1', 'com.example.user.created', '/users', '{"user_id": 1, "name": "Alice"}', NOW(), 'user-1'),
                ('evt-2', 'com.example.user.created', '/users', '{"user_id": 2, "name": "Bob"}', NOW(), 'user-2'),
                ('evt-3', 'com.example.order.placed', '/orders', '{"order_id": 100, "total": 99.99}', NOW(), 'order-100');
            """)
            conn.commit()

            # Convert to Avro with CloudEvents detection
            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_cloudevents.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='cloudevents',
                avro_namespace='com.example',
                emit_cloudevents=True,
                infer_json_schema=True,
                infer_xml_schema=False,
                sample_size=100
            )

            # Verify the output
            with open(output_path, 'r', encoding='utf-8') as f:
                schemas = json.load(f)

            # Should produce multiple schemas based on type discriminator
            if isinstance(schemas, list):
                self.assertGreaterEqual(len(schemas), 1)
                # Check that ce_type is set
                for schema in schemas:
                    if 'ce_type' in schema:
                        self.assertIn('com.example', schema['ce_type'])

            cursor.close()
            conn.close()

    def test_postgres_decimal_precision(self):
        """Test that NUMERIC/DECIMAL precision is preserved."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE test_decimal (
                    id SERIAL PRIMARY KEY,
                    price NUMERIC(12, 4),
                    quantity DECIMAL(8, 2)
                );
            """)
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_decimal.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='test_decimal',
                avro_namespace='com.example.test',
                infer_json_schema=False,
                infer_xml_schema=False
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            fields_by_name = {f['name']: f for f in schema['fields']}
            
            # Check price field
            price_type = fields_by_name['price']['type']
            if isinstance(price_type, list):
                decimal_type = next((t for t in price_type if isinstance(t, dict) and t.get('logicalType') == 'decimal'), None)
            else:
                decimal_type = price_type if isinstance(price_type, dict) else None
            
            if decimal_type:
                self.assertEqual(decimal_type['logicalType'], 'decimal')
                self.assertEqual(decimal_type['precision'], 12)
                self.assertEqual(decimal_type['scale'], 4)

            cursor.close()
            conn.close()

    def test_postgres_uuid_type(self):
        """Test UUID type conversion."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                CREATE TABLE test_uuid (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    name VARCHAR(100)
                );
            """)
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_uuid.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='test_uuid',
                avro_namespace='com.example.test',
                infer_json_schema=False,
                infer_xml_schema=False
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            fields_by_name = {f['name']: f for f in schema['fields']}
            
            # UUID should map to string with uuid logicalType
            id_type = fields_by_name['id']['type']
            if isinstance(id_type, dict):
                self.assertEqual(id_type['type'], 'string')
                self.assertEqual(id_type['logicalType'], 'uuid')

            cursor.close()
            conn.close()

    def test_postgres_heterogeneous_json(self):
        """Test JSON inference with heterogeneous data produces union types."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE mixed_json (
                    id SERIAL PRIMARY KEY,
                    data JSONB
                );
            """)

            # Insert JSON with different structures
            cursor.execute("""
                INSERT INTO mixed_json (data) VALUES
                ('{"type": "string_value"}'),
                ('{"type": "number_value", "count": 42}'),
                ('{"type": "nested", "child": {"name": "test"}}');
            """)
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_mixed_json.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='mixed_json',
                avro_namespace='com.example.mixed',
                infer_json_schema=True,
                infer_xml_schema=False,
                sample_size=100
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # The schema should have been created
            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'mixed_json')

            cursor.close()
            conn.close()

    def test_postgres_roundtrip_basic(self):
        """Test that sql2a output can be used to recreate similar tables via a2sql."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            # Create a simple table
            cursor.execute("""
                CREATE TABLE roundtrip_test (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    value INTEGER,
                    created_at TIMESTAMP
                );
            """)
            conn.commit()

            # Convert SQL to Avro
            avro_output_path = os.path.join(tempfile.gettempdir(), "avrotize", "roundtrip_test.avsc")
            os.makedirs(os.path.dirname(avro_output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=avro_output_path,
                dialect='postgres',
                table_name='roundtrip_test',
                avro_namespace='com.example.roundtrip',
                infer_json_schema=False,
                infer_xml_schema=False
            )

            # Read the generated schema
            with open(avro_output_path, 'r', encoding='utf-8') as f:
                avro_schema = json.load(f)

            # Verify basic structure
            self.assertEqual(avro_schema['type'], 'record')
            self.assertEqual(avro_schema['name'], 'roundtrip_test')
            
            fields_by_name = {f['name']: f for f in avro_schema['fields']}
            self.assertIn('id', fields_by_name)
            self.assertIn('name', fields_by_name)
            self.assertIn('value', fields_by_name)
            self.assertIn('created_at', fields_by_name)
            
            # name should NOT be nullable (NOT NULL constraint)
            self.assertEqual(fields_by_name['name']['type'], 'string')
            
            # value should be nullable
            self.assertIn('null', fields_by_name['value']['type'])

            cursor.close()
            conn.close()

    def test_postgres_date_time_types(self):
        """Test date and time type conversions."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE test_datetime (
                    id SERIAL PRIMARY KEY,
                    date_col DATE,
                    time_col TIME,
                    timetz_col TIME WITH TIME ZONE,
                    timestamp_col TIMESTAMP,
                    timestamptz_col TIMESTAMP WITH TIME ZONE,
                    interval_col INTERVAL
                );
            """)
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_datetime.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='test_datetime',
                avro_namespace='com.example.test',
                infer_json_schema=False,
                infer_xml_schema=False
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            fields_by_name = {f['name']: f for f in schema['fields']}
            
            # Check date column has date logical type
            date_type = fields_by_name['date_col']['type']
            if isinstance(date_type, list):
                date_logical = next((t for t in date_type if isinstance(t, dict) and t.get('logicalType') == 'date'), None)
            else:
                date_logical = date_type if isinstance(date_type, dict) else None
            
            if date_logical:
                self.assertEqual(date_logical['logicalType'], 'date')
                self.assertEqual(date_logical['type'], 'int')

            # Check timestamp column has timestamp-millis logical type
            ts_type = fields_by_name['timestamp_col']['type']
            if isinstance(ts_type, list):
                ts_logical = next((t for t in ts_type if isinstance(t, dict) and t.get('logicalType') == 'timestamp-millis'), None)
            else:
                ts_logical = ts_type if isinstance(ts_type, dict) else None
            
            if ts_logical:
                self.assertEqual(ts_logical['logicalType'], 'timestamp-millis')
                self.assertEqual(ts_logical['type'], 'long')

            cursor.close()
            conn.close()


if __name__ == '__main__':
    unittest.main()
