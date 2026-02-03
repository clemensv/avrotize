"""Tests for SQL to Avro schema conversion."""

import json
import os
import sys
import tempfile
import unittest

import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

# Optional imports for other databases
try:
    import pymysql
    from testcontainers.mysql import MySqlContainer
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import pymssql
    from testcontainers.mssql import SqlServerContainer
    SQLSERVER_AVAILABLE = True
except ImportError:
    SQLSERVER_AVAILABLE = False

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.sqltoavro import convert_sql_to_avro, SqlToAvro

# Reference schema directory for regression testing
REF_SCHEMA_DIR = os.path.join(os.path.dirname(__file__), 'db')

# Set to True to regenerate reference schemas (run once, then set back to False)
REGENERATE_REFS = os.environ.get('REGENERATE_REFS', 'false').lower() == 'true'


def normalize_schema_for_comparison(schema):
    """Normalize schema for comparison by removing variable parts."""
    if isinstance(schema, dict):
        # Create a copy to avoid modifying the original
        result = {}
        for key, value in schema.items():
            # Skip keys that might vary between runs
            if key in ('namespace',):
                # Keep namespace but normalize it
                result[key] = value
            else:
                result[key] = normalize_schema_for_comparison(value)
        return result
    elif isinstance(schema, list):
        return [normalize_schema_for_comparison(item) for item in schema]
    else:
        return schema


def save_or_compare_schema(test_name: str, schema: dict, dialect: str = 'postgres'):
    """Save schema as reference or compare against existing reference."""
    ref_file = os.path.join(REF_SCHEMA_DIR, f"{dialect}-{test_name}-ref.avsc")
    
    if REGENERATE_REFS or not os.path.exists(ref_file):
        # Save as reference
        os.makedirs(REF_SCHEMA_DIR, exist_ok=True)
        with open(ref_file, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=2)
        print(f"Saved reference schema: {ref_file}")
        return True, None
    else:
        # Compare against reference
        with open(ref_file, 'r', encoding='utf-8') as f:
            ref_schema = json.load(f)
        
        # Normalize both for comparison
        normalized_actual = normalize_schema_for_comparison(schema)
        normalized_ref = normalize_schema_for_comparison(ref_schema)
        
        if normalized_actual == normalized_ref:
            return True, None
        else:
            # Find differences
            diff_msg = f"Schema differs from reference {ref_file}"
            return False, diff_msg


class TestSqlToAvro(unittest.TestCase):
    """Test cases for SQL to Avro conversion."""

    def assert_schema_matches_reference(self, schema: dict, test_name: str, dialect: str = 'postgres'):
        """Assert that schema matches reference, or save as reference if regenerating."""
        success, error_msg = save_or_compare_schema(test_name, schema, dialect)
        if not success:
            self.fail(error_msg)

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

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_basic_types', 'postgres')

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

    def test_postgres_separate_credentials(self):
        """Test that separate --username and --password options work correctly."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            # Create a simple test table
            cursor.execute("""
                CREATE TABLE test_separate_creds (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL
                );
            """)
            conn.commit()

            # Convert to Avro using separate username/password (not in connection string)
            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_separate_creds.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Connection string WITHOUT credentials
            connection_string = f"postgresql://{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                avro_namespace='com.example.test',
                username=postgres.username,  # Separate username
                password=postgres.password   # Separate password
            )

            # Verify the output
            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'test_separate_creds')
            self.assertEqual(schema['namespace'], 'com.example.test')

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

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_json_inference', 'postgres')

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

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_array_types', 'postgres')

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

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_xml_inference', 'postgres')

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

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_decimal_precision', 'postgres')

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

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_uuid_type', 'postgres')

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

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_heterogeneous_json', 'postgres')

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

            # Compare against reference schema
            self.assert_schema_matches_reference(avro_schema, 'test_postgres_roundtrip_basic', 'postgres')

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

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_date_time_types', 'postgres')

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

    def test_postgres_json_sparse_data(self):
        """Test JSON inference with sparse data where optional fields are sometimes omitted."""
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
                CREATE TABLE sparse_json (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL
                );
            """)

            # Insert 30 records with sparse optional fields
            # Fields: name (always), email (80%), phone (50%), address (30%), 
            #         preferences (40%), metadata (20%)
            sparse_records = [
                # Records 1-5: Most fields present
                '{"name": "Alice", "email": "alice@example.com", "phone": "555-0001", "address": {"street": "123 Main St", "city": "NYC"}, "preferences": {"theme": "dark"}, "metadata": {"source": "web"}}',
                '{"name": "Bob", "email": "bob@example.com", "phone": "555-0002", "address": {"street": "456 Oak Ave", "city": "LA"}, "preferences": {"theme": "light"}}',
                '{"name": "Charlie", "email": "charlie@example.com", "phone": "555-0003", "address": {"street": "789 Pine Rd", "city": "Chicago"}}',
                '{"name": "Diana", "email": "diana@example.com", "phone": "555-0004", "preferences": {"theme": "auto", "language": "en"}}',
                '{"name": "Eve", "email": "eve@example.com", "address": {"street": "321 Elm St", "city": "Boston", "zip": "02101"}}',
                # Records 6-10: Fewer fields
                '{"name": "Frank", "email": "frank@example.com", "phone": "555-0006"}',
                '{"name": "Grace", "email": "grace@example.com"}',
                '{"name": "Henry", "phone": "555-0008", "metadata": {"source": "mobile", "version": "2.0"}}',
                '{"name": "Ivy", "email": "ivy@example.com", "preferences": {"notifications": true}}',
                '{"name": "Jack", "email": "jack@example.com", "phone": "555-0010", "address": {"city": "Seattle"}}',
                # Records 11-15: Minimal fields
                '{"name": "Kate"}',
                '{"name": "Leo", "email": "leo@example.com"}',
                '{"name": "Mia", "phone": "555-0013"}',
                '{"name": "Noah", "email": "noah@example.com", "address": {"street": "999 River Rd"}}',
                '{"name": "Olivia", "preferences": {"theme": "dark", "fontSize": 14}}',
                # Records 16-20: Various combinations
                '{"name": "Paul", "email": "paul@example.com", "metadata": {"campaign": "summer2024"}}',
                '{"name": "Quinn", "phone": "555-0017", "address": {"city": "Denver", "state": "CO"}}',
                '{"name": "Rose", "email": "rose@example.com", "phone": "555-0018", "preferences": {"theme": "light"}}',
                '{"name": "Sam", "address": {"street": "111 Lake Dr", "city": "Miami", "zip": "33101"}, "metadata": {"referrer": "google"}}',
                '{"name": "Tina", "email": "tina@example.com"}',
                # Records 21-25: More variations
                '{"name": "Uma", "email": "uma@example.com", "phone": "555-0021", "address": {"street": "222 Hill St", "city": "Portland"}, "preferences": {"theme": "auto"}}',
                '{"name": "Victor", "phone": "555-0022"}',
                '{"name": "Wendy", "email": "wendy@example.com", "preferences": {"language": "es"}}',
                '{"name": "Xavier", "email": "xavier@example.com", "address": {"city": "Austin"}}',
                '{"name": "Yara", "metadata": {"source": "api"}}',
                # Records 26-30: Final set
                '{"name": "Zack", "email": "zack@example.com", "phone": "555-0026", "address": {"street": "333 Beach Blvd", "city": "San Diego", "zip": "92101"}, "preferences": {"theme": "dark", "notifications": false}, "metadata": {"source": "partner", "tier": "premium"}}',
                '{"name": "Amy", "email": "amy@example.com"}',
                '{"name": "Brian", "phone": "555-0028", "preferences": {"fontSize": 16}}',
                '{"name": "Carol", "email": "carol@example.com", "address": {"city": "Phoenix"}}',
                '{"name": "David", "email": "david@example.com", "phone": "555-0030"}',
            ]

            for record in sparse_records:
                cursor.execute("INSERT INTO sparse_json (data) VALUES (%s)", (record,))
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_sparse_json.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='sparse_json',
                avro_namespace='com.example.sparse',
                infer_json_schema=True,
                infer_xml_schema=False,
                sample_size=50
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_json_sparse_data', 'postgres')

            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'sparse_json')

            # Get the data field's inferred type
            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('data', fields_by_name)
            
            data_type = fields_by_name['data']['type']
            # Should be a record with merged fields from all samples
            if isinstance(data_type, dict) and data_type.get('type') == 'record':
                data_fields = {f['name']: f for f in data_type.get('fields', [])}
                # name should always be present (required)
                self.assertIn('name', data_fields)
                # optional fields should also be discovered
                self.assertIn('email', data_fields)
                self.assertIn('phone', data_fields)
                # nested objects should be inferred
                self.assertIn('address', data_fields)
                self.assertIn('preferences', data_fields)
                self.assertIn('metadata', data_fields)
                
                # Check that address is a nested record with merged fields
                addr_type = data_fields['address']['type']
                if isinstance(addr_type, dict) and addr_type.get('type') == 'record':
                    addr_fields = {f['name']: f for f in addr_type.get('fields', [])}
                    # Should have street, city from various records, zip and state from some
                    self.assertIn('city', addr_fields)

            cursor.close()
            conn.close()

    def test_postgres_json_complex_nested_structures(self):
        """Test JSON inference with complex nested structures: arrays, nested objects, mixed types."""
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
                CREATE TABLE complex_json (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL
                );
            """)

            # Insert complex nested structures
            complex_records = [
                # Array of primitives
                '{"name": "test1", "tags": ["alpha", "beta", "gamma"], "scores": [95, 87, 92]}',
                # Array of objects
                '{"name": "test2", "items": [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]}',
                # Deeply nested objects
                '{"name": "test3", "config": {"database": {"host": "localhost", "port": 5432, "credentials": {"user": "admin", "encrypted": true}}}}',
                # Mixed array types (should produce union)
                '{"name": "test4", "mixed_array": [1, "two", 3.0, true]}',
                # Nested arrays
                '{"name": "test5", "matrix": [[1, 2, 3], [4, 5, 6]], "nested_items": [{"children": [{"name": "child1"}, {"name": "child2"}]}]}',
                # Complex event structure
                '{"name": "test6", "events": [{"type": "click", "data": {"x": 100, "y": 200}}, {"type": "scroll", "data": {"offset": 500}}]}',
                # Large nested object
                '{"name": "test7", "organization": {"name": "Acme Corp", "departments": [{"name": "Engineering", "teams": [{"name": "Backend", "members": 10}, {"name": "Frontend", "members": 8}]}, {"name": "Sales", "teams": [{"name": "Enterprise", "members": 5}]}]}}',
                # Object with array of nested objects with arrays
                '{"name": "test8", "catalog": {"categories": [{"name": "Electronics", "products": [{"sku": "E001", "variants": ["red", "blue"]}, {"sku": "E002", "variants": ["small", "large"]}]}]}}',
                # Sparse nested structure
                '{"name": "test9", "config": {"database": {"host": "db.example.com"}}}',
                '{"name": "test10", "items": [{"id": 3, "value": "c", "extra": "data"}]}',
                # More array variations
                '{"name": "test11", "tags": ["delta"], "scores": [100]}',
                '{"name": "test12", "events": [{"type": "keypress", "data": {"key": "Enter"}}]}',
                # Empty arrays (edge case)
                '{"name": "test13", "tags": [], "items": []}',
                # Null values mixed in
                '{"name": "test14", "config": null, "tags": ["epsilon"]}',
                '{"name": "test15", "items": [{"id": 4, "value": null}]}',
            ]

            for record in complex_records:
                cursor.execute("INSERT INTO complex_json (data) VALUES (%s)", (record,))
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_complex_json.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='complex_json',
                avro_namespace='com.example.complex',
                infer_json_schema=True,
                infer_xml_schema=False,
                sample_size=50
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_json_complex_nested_structures', 'postgres')

            self.assertEqual(schema['type'], 'record')
            
            fields_by_name = {f['name']: f for f in schema['fields']}
            data_type = fields_by_name['data']['type']
            
            if isinstance(data_type, dict) and data_type.get('type') == 'record':
                data_fields = {f['name']: f for f in data_type.get('fields', [])}
                
                # Check arrays were detected
                self.assertIn('tags', data_fields)
                tags_type = data_fields['tags']['type']
                # Should be an array type
                self.assertTrue(
                    (isinstance(tags_type, dict) and tags_type.get('type') == 'array') or
                    (isinstance(tags_type, list) and any(isinstance(t, dict) and t.get('type') == 'array' for t in tags_type))
                )
                
                # Check nested objects were detected  
                self.assertIn('config', data_fields)
                self.assertIn('items', data_fields)
                self.assertIn('events', data_fields)

            cursor.close()
            conn.close()

    def test_postgres_json_map_keys_non_identifiers(self):
        """Test JSON inference with dictionary keys that cannot be valid identifiers (should become maps)."""
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
                CREATE TABLE map_json (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL
                );
            """)

            # Insert records with non-identifier keys (UUIDs, URLs, special chars, numbers as keys)
            map_records = [
                # UUID keys (common in event sourcing)
                '{"name": "event_store", "events_by_id": {"550e8400-e29b-41d4-a716-446655440000": {"type": "created", "data": "payload1"}, "6ba7b810-9dad-11d1-80b4-00c04fd430c8": {"type": "updated", "data": "payload2"}}}',
                # URL/path keys (common in routing configs)
                '{"name": "routes", "handlers": {"/api/users": {"method": "GET", "auth": true}, "/api/orders/{id}": {"method": "POST", "auth": true}}}',
                # Keys with special characters
                '{"name": "metrics", "values": {"cpu.usage": 45.2, "memory.free": 1024, "disk.read/s": 100}}',
                # Numeric string keys (common in year-based data)
                '{"name": "annual_report", "data_by_year": {"2022": {"revenue": 1000000}, "2023": {"revenue": 1200000}, "2024": {"revenue": 1500000}}}',
                # Keys starting with numbers
                '{"name": "config", "rules": {"1st_priority": "high", "2nd_priority": "medium", "3rd_priority": "low"}}',
                # Keys with spaces and hyphens
                '{"name": "translations", "messages": {"hello-world": "Hello World", "good bye": "Goodbye", "welcome message": "Welcome!"}}',
                # ISO date keys
                '{"name": "daily_stats", "stats": {"2024-01-15": {"visits": 1000}, "2024-01-16": {"visits": 1100}, "2024-01-17": {"visits": 950}}}',
                # Email keys
                '{"name": "user_prefs", "settings": {"user@example.com": {"theme": "dark"}, "admin@example.com": {"theme": "light"}}}',
                # Mixed identifier and non-identifier keys
                '{"name": "hybrid", "valid_field": "ok", "settings": {"normal_key": 1, "key-with-dash": 2, "key.with.dots": 3}}',
                # Deeply nested maps with non-identifier keys
                '{"name": "nested_maps", "level1": {"key/1": {"level2": {"key.2": {"value": 42}}}}}',
                # Array of maps
                '{"name": "map_array", "items": [{"item-1": "a", "item-2": "b"}, {"item-3": "c", "item-4": "d"}]}',
                # Unicode keys
                '{"name": "i18n", "labels": {"日本語": "Japanese", "中文": "Chinese", "한국어": "Korean"}}',
                # Empty map
                '{"name": "empty", "mapping": {}}',
                # Large map
                '{"name": "large_map", "codes": {"US-CA": "California", "US-NY": "New York", "US-TX": "Texas", "DE-BY": "Bavaria", "DE-BE": "Berlin"}}',
                # Nested structure with maps at multiple levels
                '{"name": "multi_level", "regions": {"us-west": {"zones": {"us-west-1a": {"capacity": 100}, "us-west-1b": {"capacity": 150}}}}}',
            ]

            for record in map_records:
                cursor.execute("INSERT INTO map_json (data) VALUES (%s)", (record,))
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_map_json.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='map_json',
                avro_namespace='com.example.maps',
                infer_json_schema=True,
                infer_xml_schema=False,
                sample_size=50
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_json_map_keys_non_identifiers', 'postgres')

            self.assertEqual(schema['type'], 'record')
            
            # Save schema for inspection
            print(f"Map JSON Schema: {json.dumps(schema, indent=2)}")
            
            fields_by_name = {f['name']: f for f in schema['fields']}
            data_type = fields_by_name['data']['type']
            
            # The schema should have been successfully created
            self.assertIsNotNone(data_type)

            cursor.close()
            conn.close()

    def test_postgres_xml_sparse_data(self):
        """Test XML inference with sparse data where optional elements/attributes are sometimes omitted."""
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
                CREATE TABLE sparse_xml (
                    id SERIAL PRIMARY KEY,
                    content XML
                );
            """)

            # Insert 25 XML records with sparse optional elements and attributes
            sparse_xml_records = [
                # Full records with all elements
                '<person id="1" status="active"><name>Alice</name><email>alice@example.com</email><phone>555-0001</phone><address><street>123 Main St</street><city>NYC</city><zip>10001</zip></address></person>',
                '<person id="2" status="active"><name>Bob</name><email>bob@example.com</email><phone>555-0002</phone><address><street>456 Oak Ave</street><city>LA</city></address></person>',
                # Missing phone
                '<person id="3" status="active"><name>Charlie</name><email>charlie@example.com</email><address><city>Chicago</city></address></person>',
                # Missing address entirely
                '<person id="4"><name>Diana</name><email>diana@example.com</email><phone>555-0004</phone></person>',
                # Minimal - just name
                '<person id="5"><name>Eve</name></person>',
                # With extra optional attribute
                '<person id="6" status="inactive" tier="premium"><name>Frank</name><email>frank@example.com</email></person>',
                # Different address structure
                '<person id="7"><name>Grace</name><address><city>Seattle</city><state>WA</state><country>USA</country></address></person>',
                # With nested preferences
                '<person id="8" status="active"><name>Henry</name><email>henry@example.com</email><preferences><theme>dark</theme><language>en</language></preferences></person>',
                # Minimal with phone only
                '<person id="9"><name>Ivy</name><phone>555-0009</phone></person>',
                # Full address, no contact
                '<person id="10"><name>Jack</name><address><street>999 River Rd</street><city>Denver</city><zip>80201</zip><state>CO</state></address></person>',
                # Just email
                '<person id="11" status="pending"><name>Kate</name><email>kate@example.com</email></person>',
                # With metadata element
                '<person id="12"><name>Leo</name><metadata><source>import</source><created>2024-01-15</created></metadata></person>',
                # Complex nested
                '<person id="13" status="active"><name>Mia</name><email>mia@example.com</email><phone>555-0013</phone><address><street>111 Lake Dr</street><city>Miami</city></address><preferences><notifications>true</notifications></preferences></person>',
                # Minimal
                '<person id="14"><name>Noah</name></person>',
                # With multiple optional attrs
                '<person id="15" status="active" tier="basic" verified="true"><name>Olivia</name><email>olivia@example.com</email></person>',
                # Different preference structure
                '<person id="16"><name>Paul</name><preferences><theme>light</theme><fontSize>14</fontSize><notifications>false</notifications></preferences></person>',
                # Address with apartment
                '<person id="17"><name>Quinn</name><address><street>222 Hill St</street><apartment>4B</apartment><city>Portland</city></address></person>',
                # Just phone and email
                '<person id="18"><name>Rose</name><email>rose@example.com</email><phone>555-0018</phone></person>',
                # With work info
                '<person id="19" status="active"><name>Sam</name><email>sam@example.com</email><work><company>Acme Corp</company><title>Engineer</title></work></person>',
                # Minimal with status
                '<person id="20" status="inactive"><name>Tina</name></person>',
                # Full record
                '<person id="21" status="active" tier="premium"><name>Uma</name><email>uma@example.com</email><phone>555-0021</phone><address><street>333 Beach Blvd</street><city>San Diego</city><zip>92101</zip></address><preferences><theme>auto</theme></preferences></person>',
                # Different metadata
                '<person id="22"><name>Victor</name><metadata><source>api</source><version>2</version></metadata></person>',
                # Sparse address
                '<person id="23"><name>Wendy</name><address><city>Austin</city></address></person>',
                # With notes element
                '<person id="24" status="active"><name>Xavier</name><email>xavier@example.com</email><notes>VIP customer</notes></person>',
                # Final full record
                '<person id="25" status="active" tier="basic" verified="false"><name>Yara</name><email>yara@example.com</email><phone>555-0025</phone><address><street>444 Park Ave</street><city>Phoenix</city><zip>85001</zip></address></person>',
            ]

            for record in sparse_xml_records:
                cursor.execute("INSERT INTO sparse_xml (content) VALUES (%s)", (record,))
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_sparse_xml.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='sparse_xml',
                avro_namespace='com.example.sparsexml',
                infer_json_schema=False,
                infer_xml_schema=True,
                sample_size=50
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_xml_sparse_data', 'postgres')

            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'sparse_xml')

            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('content', fields_by_name)
            
            content_type = fields_by_name['content']['type']
            # Should be a record with merged fields
            if isinstance(content_type, dict) and content_type.get('type') == 'record':
                content_fields = {f['name']: f for f in content_type.get('fields', [])}
                # Should have discovered name (always present)
                self.assertIn('name', content_fields)
                # Should have discovered optional elements
                self.assertIn('email', content_fields)
                # Should have discovered attributes (prefixed with @)
                self.assertIn('@id', content_fields)

            cursor.close()
            conn.close()

    def test_postgres_xml_complex_nested_structures(self):
        """Test XML inference with complex nested structures: nested elements, arrays, attributes."""
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
                CREATE TABLE complex_xml (
                    id SERIAL PRIMARY KEY,
                    content XML
                );
            """)

            # Insert complex XML structures
            complex_xml_records = [
                # Multiple nested levels
                '''<order id="1" status="pending">
                    <customer>
                        <name>John Doe</name>
                        <contact>
                            <email>john@example.com</email>
                            <phone type="mobile">555-0001</phone>
                        </contact>
                    </customer>
                    <items>
                        <item sku="A001" qty="2">
                            <name>Widget</name>
                            <price currency="USD">19.99</price>
                        </item>
                        <item sku="B002" qty="1">
                            <name>Gadget</name>
                            <price currency="USD">49.99</price>
                        </item>
                    </items>
                    <shipping>
                        <address>
                            <street>123 Main St</street>
                            <city>NYC</city>
                        </address>
                        <method priority="standard">Ground</method>
                    </shipping>
                </order>''',
                # Different item structure
                '''<order id="2" status="completed">
                    <customer>
                        <name>Jane Smith</name>
                        <contact>
                            <email>jane@example.com</email>
                        </contact>
                    </customer>
                    <items>
                        <item sku="C003" qty="3">
                            <name>Thingamajig</name>
                            <price currency="EUR">29.99</price>
                            <discount percent="10"/>
                        </item>
                    </items>
                </order>''',
                # Deeply nested
                '''<order id="3" status="processing">
                    <customer>
                        <name>Bob Wilson</name>
                        <loyalty tier="gold" points="5000"/>
                        <contact>
                            <email>bob@example.com</email>
                            <phone type="work">555-0003</phone>
                            <phone type="home">555-0004</phone>
                        </contact>
                    </customer>
                    <items>
                        <item sku="D004" qty="1">
                            <name>Doohickey</name>
                            <price currency="USD">99.99</price>
                            <variants>
                                <variant color="red" size="L"/>
                                <variant color="blue" size="M"/>
                            </variants>
                        </item>
                    </items>
                    <payment>
                        <method>credit</method>
                        <card last4="1234" type="visa"/>
                    </payment>
                </order>''',
                # With notes and metadata
                '''<order id="4" status="pending" priority="high">
                    <customer>
                        <name>Alice Brown</name>
                    </customer>
                    <items>
                        <item sku="E005" qty="5">
                            <name>Whatchamacallit</name>
                            <price currency="USD">9.99</price>
                        </item>
                    </items>
                    <notes>
                        <note author="system" timestamp="2024-01-15T10:30:00Z">Order flagged for review</note>
                        <note author="admin" timestamp="2024-01-15T11:00:00Z">Approved</note>
                    </notes>
                    <metadata>
                        <source>web</source>
                        <campaign>winter-sale</campaign>
                    </metadata>
                </order>''',
                # Minimal structure
                '''<order id="5" status="draft">
                    <customer>
                        <name>Test User</name>
                    </customer>
                    <items>
                        <item sku="F006" qty="1">
                            <name>Sample Item</name>
                            <price currency="USD">0.00</price>
                        </item>
                    </items>
                </order>''',
                # Array of similar elements
                '''<order id="6" status="completed">
                    <customer>
                        <name>Multi Item Customer</name>
                        <contact>
                            <email>multi@example.com</email>
                        </contact>
                    </customer>
                    <items>
                        <item sku="G001" qty="1"><name>Item 1</name><price currency="USD">10.00</price></item>
                        <item sku="G002" qty="2"><name>Item 2</name><price currency="USD">20.00</price></item>
                        <item sku="G003" qty="3"><name>Item 3</name><price currency="USD">30.00</price></item>
                        <item sku="G004" qty="4"><name>Item 4</name><price currency="USD">40.00</price></item>
                    </items>
                    <shipping>
                        <address>
                            <street>456 Oak Ave</street>
                            <city>LA</city>
                            <state>CA</state>
                            <zip>90001</zip>
                        </address>
                    </shipping>
                </order>''',
            ]

            for record in complex_xml_records:
                cursor.execute("INSERT INTO complex_xml (content) VALUES (%s)", (record,))
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_complex_xml.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='complex_xml',
                avro_namespace='com.example.complexxml',
                infer_json_schema=False,
                infer_xml_schema=True,
                sample_size=50
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_xml_complex_nested_structures', 'postgres')

            self.assertEqual(schema['type'], 'record')
            
            # Print schema for debugging
            print(f"Complex XML Schema: {json.dumps(schema, indent=2)}")

            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('content', fields_by_name)
            
            content_type = fields_by_name['content']['type']
            if isinstance(content_type, dict) and content_type.get('type') == 'record':
                content_fields = {f['name']: f for f in content_type.get('fields', [])}
                # Should have discovered main elements
                self.assertIn('customer', content_fields)
                self.assertIn('items', content_fields)
                # Should have discovered attributes
                self.assertIn('@id', content_fields)
                self.assertIn('@status', content_fields)

            cursor.close()
            conn.close()

    def test_postgres_json_mutually_exclusive_structures(self):
        """Test JSON inference with mutually exclusive record structures that should yield union types."""
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
                CREATE TABLE union_json (
                    id SERIAL PRIMARY KEY,
                    event JSONB NOT NULL
                );
            """)

            # Insert records with mutually exclusive structures (event sourcing pattern)
            # These represent different event types that share only 'type' field
            union_records = [
                # UserCreated events
                '{"type": "UserCreated", "userId": "user-001", "email": "alice@example.com", "name": "Alice"}',
                '{"type": "UserCreated", "userId": "user-002", "email": "bob@example.com", "name": "Bob"}',
                '{"type": "UserCreated", "userId": "user-003", "email": "carol@example.com", "name": "Carol"}',
                # OrderPlaced events (completely different structure)
                '{"type": "OrderPlaced", "orderId": "order-001", "items": [{"sku": "PROD-1", "qty": 2}], "total": 99.99}',
                '{"type": "OrderPlaced", "orderId": "order-002", "items": [{"sku": "PROD-2", "qty": 1}], "total": 49.99}',
                '{"type": "OrderPlaced", "orderId": "order-003", "items": [{"sku": "PROD-1", "qty": 5}, {"sku": "PROD-3", "qty": 1}], "total": 299.99}',
                # PaymentProcessed events (another different structure)
                '{"type": "PaymentProcessed", "paymentId": "pay-001", "orderId": "order-001", "amount": 99.99, "method": "credit_card", "last4": "1234"}',
                '{"type": "PaymentProcessed", "paymentId": "pay-002", "orderId": "order-002", "amount": 49.99, "method": "paypal", "paypalEmail": "bob@paypal.com"}',
                # ShipmentCreated events
                '{"type": "ShipmentCreated", "shipmentId": "ship-001", "orderId": "order-001", "carrier": "UPS", "trackingNumber": "1Z999AA10123456784"}',
                '{"type": "ShipmentCreated", "shipmentId": "ship-002", "orderId": "order-002", "carrier": "FedEx", "trackingNumber": "794644790149"}',
                # UserUpdated events (overlaps partially with UserCreated)
                '{"type": "UserUpdated", "userId": "user-001", "changes": {"email": "alice.new@example.com"}}',
                '{"type": "UserUpdated", "userId": "user-002", "changes": {"name": "Robert"}}',
                # InventoryAdjusted events
                '{"type": "InventoryAdjusted", "sku": "PROD-1", "adjustment": -7, "reason": "sold", "newQuantity": 93}',
                '{"type": "InventoryAdjusted", "sku": "PROD-2", "adjustment": 50, "reason": "restock", "newQuantity": 150}',
            ]

            for record in union_records:
                cursor.execute("INSERT INTO union_json (event) VALUES (%s)", (record,))
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_union_json.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='union_json',
                avro_namespace='com.example.events',
                infer_json_schema=True,
                infer_xml_schema=False,
                sample_size=100
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_json_mutually_exclusive_structures', 'postgres')

            print(f"Union JSON Schema: {json.dumps(schema, indent=2)}")

            self.assertEqual(schema['type'], 'record')
            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('event', fields_by_name)

            event_type = fields_by_name['event']['type']
            
            # The event field should either be:
            # 1. A union of record types (if structures couldn't be folded)
            # 2. A single record with all fields as optional (if folded)
            # Either way, we need to verify the schema captures all the different event structures
            
            if isinstance(event_type, list):
                # It's a union - check that we have multiple record types
                record_types = [t for t in event_type if isinstance(t, dict) and t.get('type') == 'record']
                print(f"Found {len(record_types)} record types in union")
                # Should have captured different event structures
                all_fields = set()
                for rec in record_types:
                    for f in rec.get('fields', []):
                        all_fields.add(f['name'])
                # Verify key fields from different event types are present
                self.assertIn('type', all_fields)  # Common field
                
            elif isinstance(event_type, dict) and event_type.get('type') == 'record':
                # Folded into single record - all fields should be present
                event_fields = {f['name']: f for f in event_type.get('fields', [])}
                print(f"Folded record has {len(event_fields)} fields: {list(event_fields.keys())}")
                # Should have the common 'type' field
                self.assertIn('type', event_fields)
                # Should have accumulated fields from different event types
                # At minimum: userId (UserCreated), orderId (OrderPlaced), paymentId (PaymentProcessed)

            cursor.close()
            conn.close()

    def test_postgres_json_varying_primitive_types(self):
        """Test JSON inference with fields that have varying primitive types across records."""
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
                CREATE TABLE varying_types (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL
                );
            """)

            # Insert records where the same field has different types
            # This tests that the inference produces proper union types for primitives
            varying_records = [
                # 'value' field is sometimes string, sometimes int, sometimes float
                '{"name": "item1", "value": "string_value"}',
                '{"name": "item2", "value": 42}',
                '{"name": "item3", "value": 3.14159}',
                '{"name": "item4", "value": "another_string"}',
                '{"name": "item5", "value": 100}',
                '{"name": "item6", "value": null}',  # Also null sometimes
                # 'count' field alternates between int and string representation
                '{"name": "item7", "value": true, "count": 5}',  # value is boolean here
                '{"name": "item8", "value": false, "count": "10"}',
                '{"name": "item9", "value": 0, "count": 15}',  # 0 as int
                '{"name": "item10", "value": "0", "count": "twenty"}',  # "0" as string
                # 'status' field varies between string, int code, and boolean
                '{"name": "item11", "status": "active"}',
                '{"name": "item12", "status": 1}',
                '{"name": "item13", "status": true}',
                '{"name": "item14", "status": "inactive"}',
                '{"name": "item15", "status": 0}',
                '{"name": "item16", "status": false}',
                # 'metadata' field is sometimes object, sometimes array, sometimes string
                '{"name": "item17", "metadata": {"key": "value"}}',
                '{"name": "item18", "metadata": ["tag1", "tag2"]}',
                '{"name": "item19", "metadata": "simple-string-metadata"}',
                '{"name": "item20", "metadata": null}',
                # 'score' field has int, float, and string representations
                '{"name": "item21", "score": 95}',
                '{"name": "item22", "score": 87.5}',
                '{"name": "item23", "score": "A+"}',
                '{"name": "item24", "score": "N/A"}',
                # 'id' field varies (common in loosely-typed APIs)
                '{"name": "item25", "id": 12345}',
                '{"name": "item26", "id": "uuid-550e8400"}',
                '{"name": "item27", "id": "item-27"}',
                '{"name": "item28", "id": 99999}',
            ]

            for record in varying_records:
                cursor.execute("INSERT INTO varying_types (data) VALUES (%s)", (record,))
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_varying_types.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='postgres',
                table_name='varying_types',
                avro_namespace='com.example.varying',
                infer_json_schema=True,
                infer_xml_schema=False,
                sample_size=100
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_postgres_json_varying_primitive_types', 'postgres')

            print(f"Varying Types Schema: {json.dumps(schema, indent=2)}")

            self.assertEqual(schema['type'], 'record')
            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('data', fields_by_name)

            data_type = fields_by_name['data']['type']
            
            def count_types_in_schema(schema_node, depth=0):
                """Recursively count distinct types in a schema."""
                types_found = set()
                if isinstance(schema_node, str):
                    types_found.add(schema_node)
                elif isinstance(schema_node, list):
                    for item in schema_node:
                        types_found.update(count_types_in_schema(item, depth+1))
                elif isinstance(schema_node, dict):
                    if 'type' in schema_node:
                        types_found.add(schema_node['type'])
                        if schema_node['type'] == 'record':
                            for field in schema_node.get('fields', []):
                                types_found.update(count_types_in_schema(field.get('type'), depth+1))
                        elif schema_node['type'] == 'array':
                            types_found.update(count_types_in_schema(schema_node.get('items'), depth+1))
                        elif schema_node['type'] == 'map':
                            types_found.update(count_types_in_schema(schema_node.get('values'), depth+1))
                return types_found

            types_in_schema = count_types_in_schema(data_type)
            print(f"Types found in data schema: {types_in_schema}")

            # The schema should contain multiple primitive types due to varying data
            # At minimum we should see: string, long/int, double, boolean, null
            primitive_types = types_in_schema.intersection({'string', 'long', 'int', 'double', 'float', 'boolean', 'null'})
            print(f"Primitive types found: {primitive_types}")
            
            # We should have at least 3 different primitive types captured
            # (string, numeric (long/double), and likely null or boolean)
            self.assertGreaterEqual(len(primitive_types), 3, 
                f"Expected at least 3 different primitive types, got {primitive_types}")

            cursor.close()
            conn.close()

    def test_postgres_inference_switches(self):
        """Test that --infer-json and --infer-xml switches work correctly."""
        with PostgresContainer() as postgres:
            conn = psycopg2.connect(
                host=postgres.get_container_host_ip(),
                port=postgres.get_exposed_port(5432),
                user=postgres.username,
                password=postgres.password,
                database=postgres.dbname
            )
            cursor = conn.cursor()

            # Create a table with JSON and XML columns
            cursor.execute("""
                CREATE TABLE inference_test (
                    id SERIAL PRIMARY KEY,
                    json_data JSONB,
                    xml_data XML
                );
            """)

            # Insert data to allow inference
            cursor.execute("""
                INSERT INTO inference_test (json_data, xml_data) VALUES
                ('{"name": "test", "count": 42}'::jsonb, '<root><item>value</item></root>'::xml);
            """)
            conn.commit()

            # Test 1: With inference enabled (default)
            output_path_infer = os.path.join(tempfile.gettempdir(), "avrotize", "test_inference_enabled.avsc")
            os.makedirs(os.path.dirname(output_path_infer), exist_ok=True)

            connection_string = f"postgresql://{postgres.username}:{postgres.password}@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}/{postgres.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path_infer,
                dialect='postgres',
                table_name='inference_test',
                avro_namespace='com.example.test',
                infer_json_schema=True,
                infer_xml_schema=True
            )

            with open(output_path_infer, 'r', encoding='utf-8') as f:
                schema_with_inference = json.load(f)

            fields_infer = {f['name']: f for f in schema_with_inference['fields']}
            
            # With inference, json_data should be a record type, not just string
            json_type_infer = fields_infer['json_data']['type']
            if isinstance(json_type_infer, list):
                non_null = [t for t in json_type_infer if t != 'null'][0]
            else:
                non_null = json_type_infer
            self.assertIsInstance(non_null, dict, "JSON column with inference should be a complex type")
            self.assertEqual(non_null.get('type'), 'record', "JSON column with inference should be a record")

            # Test 2: With inference disabled
            output_path_no_infer = os.path.join(tempfile.gettempdir(), "avrotize", "test_inference_disabled.avsc")
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path_no_infer,
                dialect='postgres',
                table_name='inference_test',
                avro_namespace='com.example.test',
                infer_json_schema=False,
                infer_xml_schema=False
            )

            with open(output_path_no_infer, 'r', encoding='utf-8') as f:
                schema_no_inference = json.load(f)

            fields_no_infer = {f['name']: f for f in schema_no_inference['fields']}
            
            # Without inference, json_data should fall back to string type
            json_type_no_infer = fields_no_infer['json_data']['type']
            if isinstance(json_type_no_infer, list):
                non_null = [t for t in json_type_no_infer if t != 'null'][0]
            else:
                non_null = json_type_no_infer
            self.assertEqual(non_null, 'string', "JSON column without inference should be string")
            
            # Without inference, xml_data should be string too
            xml_type_no_infer = fields_no_infer['xml_data']['type']
            if isinstance(xml_type_no_infer, list):
                non_null = [t for t in xml_type_no_infer if t != 'null'][0]
            else:
                non_null = xml_type_no_infer
            self.assertEqual(non_null, 'string', "XML column without inference should be string")

            cursor.close()
            conn.close()


@unittest.skipUnless(MYSQL_AVAILABLE, "pymysql and testcontainers-mysql not installed")
class TestSqlToAvroMySQL(unittest.TestCase):
    """Test cases for MySQL to Avro conversion."""

    def assert_schema_matches_reference(self, schema: dict, test_name: str, dialect: str = 'mysql'):
        """Assert that schema matches reference, or save as reference if regenerating."""
        success, error_msg = save_or_compare_schema(test_name, schema, dialect)
        if not success:
            self.fail(error_msg)

    def test_mysql_basic_types(self):
        """Test conversion of MySQL basic types to Avro schema."""
        with MySqlContainer() as mysql:
            conn = pymysql.connect(
                host=mysql.get_container_host_ip(),
                port=int(mysql.get_exposed_port(3306)),
                user=mysql.username,
                password=mysql.password,
                database=mysql.dbname
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE test_basic_types (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    description TEXT,
                    age INT,
                    salary DECIMAL(10, 2),
                    is_active BOOLEAN,
                    birth_date DATE,
                    created_at DATETIME,
                    data BLOB,
                    rating FLOAT,
                    score DOUBLE,
                    big_number BIGINT
                )
            """)
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_mysql_basic.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"mysql+pymysql://{mysql.username}:{mysql.password}@{mysql.get_container_host_ip()}:{mysql.get_exposed_port(3306)}/{mysql.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='mysql',
                table_name='test_basic_types',
                avro_namespace='com.example.mysql'
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_mysql_basic_types', 'mysql')

            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'test_basic_types')

            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('id', fields_by_name)
            self.assertIn('name', fields_by_name)
            self.assertIn('salary', fields_by_name)
            self.assertIn('is_active', fields_by_name)
            self.assertIn('big_number', fields_by_name)

            cursor.close()
            conn.close()

    def test_mysql_json_inference(self):
        """Test JSON column inference in MySQL."""
        with MySqlContainer() as mysql:
            conn = pymysql.connect(
                host=mysql.get_container_host_ip(),
                port=int(mysql.get_exposed_port(3306)),
                user=mysql.username,
                password=mysql.password,
                database=mysql.dbname
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE json_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    data JSON
                )
            """)

            cursor.execute("""
                INSERT INTO json_data (data) VALUES
                ('{"name": "Alice", "age": 30, "email": "alice@example.com"}'),
                ('{"name": "Bob", "age": 25, "email": "bob@example.com", "phone": "555-1234"}'),
                ('{"name": "Carol", "age": 35}')
            """)
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_mysql_json.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"mysql+pymysql://{mysql.username}:{mysql.password}@{mysql.get_container_host_ip()}:{mysql.get_exposed_port(3306)}/{mysql.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='mysql',
                table_name='json_data',
                avro_namespace='com.example.mysql',
                infer_json_schema=True,
                sample_size=100
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_mysql_json_inference', 'mysql')

            self.assertEqual(schema['type'], 'record')
            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('data', fields_by_name)

            # The data field should be inferred as a record type
            data_type = fields_by_name['data']['type']
            if isinstance(data_type, dict) and data_type.get('type') == 'record':
                data_fields = {f['name']: f for f in data_type.get('fields', [])}
                self.assertIn('name', data_fields)
                self.assertIn('age', data_fields)

            cursor.close()
            conn.close()


@unittest.skipUnless(SQLSERVER_AVAILABLE, "pymssql and testcontainers-mssql not installed")
class TestSqlToAvroSqlServer(unittest.TestCase):
    """Test cases for SQL Server to Avro conversion."""

    def assert_schema_matches_reference(self, schema: dict, test_name: str, dialect: str = 'sqlserver'):
        """Assert that schema matches reference, or save as reference if regenerating."""
        success, error_msg = save_or_compare_schema(test_name, schema, dialect)
        if not success:
            self.fail(error_msg)

    def test_sqlserver_basic_types(self):
        """Test conversion of SQL Server basic types to Avro schema."""
        with SqlServerContainer() as sqlserver:
            conn = pymssql.connect(
                server=sqlserver.get_container_host_ip(),
                port=sqlserver.get_exposed_port(1433),
                user=sqlserver.username,
                password=sqlserver.password,
                database=sqlserver.dbname
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE test_basic_types (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    name NVARCHAR(100) NOT NULL,
                    description NVARCHAR(MAX),
                    age INT,
                    salary DECIMAL(10, 2),
                    is_active BIT,
                    birth_date DATE,
                    created_at DATETIME2,
                    data VARBINARY(MAX),
                    rating REAL,
                    score FLOAT,
                    big_number BIGINT,
                    unique_id UNIQUEIDENTIFIER
                )
            """)
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_sqlserver_basic.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"mssql+pymssql://{sqlserver.username}:{sqlserver.password}@{sqlserver.get_container_host_ip()}:{sqlserver.get_exposed_port(1433)}/{sqlserver.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='sqlserver',
                table_name='test_basic_types',
                avro_namespace='com.example.sqlserver'
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_sqlserver_basic_types', 'sqlserver')

            self.assertEqual(schema['type'], 'record')
            self.assertEqual(schema['name'], 'test_basic_types')

            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('id', fields_by_name)
            self.assertIn('name', fields_by_name)
            self.assertIn('salary', fields_by_name)
            self.assertIn('unique_id', fields_by_name)
            self.assertIn('big_number', fields_by_name)

            cursor.close()
            conn.close()

    def test_sqlserver_nvarchar_max(self):
        """Test SQL Server NVARCHAR(MAX) and XML types."""
        with SqlServerContainer() as sqlserver:
            conn = pymssql.connect(
                server=sqlserver.get_container_host_ip(),
                port=sqlserver.get_exposed_port(1433),
                user=sqlserver.username,
                password=sqlserver.password,
                database=sqlserver.dbname
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE text_types (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    short_text NVARCHAR(50),
                    long_text NVARCHAR(MAX),
                    xml_data XML
                )
            """)

            cursor.execute("""
                INSERT INTO text_types (short_text, long_text, xml_data) VALUES
                ('short', 'This is a longer text', '<root><item>test</item></root>'),
                ('text', 'Another longer text value', '<root><item>data</item></root>')
            """)
            conn.commit()

            output_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_sqlserver_text.avsc")
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            connection_string = f"mssql+pymssql://{sqlserver.username}:{sqlserver.password}@{sqlserver.get_container_host_ip()}:{sqlserver.get_exposed_port(1433)}/{sqlserver.dbname}"
            convert_sql_to_avro(
                connection_string=connection_string,
                avro_schema_file=output_path,
                dialect='sqlserver',
                table_name='text_types',
                avro_namespace='com.example.sqlserver',
                infer_xml_schema=True,
                sample_size=100
            )

            with open(output_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            # Compare against reference schema
            self.assert_schema_matches_reference(schema, 'test_sqlserver_nvarchar_max', 'sqlserver')

            self.assertEqual(schema['type'], 'record')
            fields_by_name = {f['name']: f for f in schema['fields']}
            self.assertIn('short_text', fields_by_name)
            self.assertIn('long_text', fields_by_name)
            self.assertIn('xml_data', fields_by_name)

            cursor.close()
            conn.close()


if __name__ == '__main__':
    unittest.main()
