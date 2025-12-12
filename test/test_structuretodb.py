import os
import json
import tempfile
import unittest
from avrotize.structuretodb import convert_structure_to_sql, convert_structure_to_nosql


class TestStructureToDB(unittest.TestCase):

    def setUp(self):
        self.test_schema = {
            "type": "object",
            "name": "TestRecord",
            "namespace": "com.example",
            "properties": {
                "id": {"type": "int32", "description": "Primary key"},
                "name": {"type": "string", "description": "Name of the record"},
                "email": {"type": "string", "description": "Email address"},
                "age": {"type": "int32"}
            },
            "required": ["id", "name"]
        }
        self.schema_json = json.dumps(self.test_schema)
        self.test_schema_path = os.path.join(tempfile.gettempdir(), "test_schema.struct.json")
        with open(self.test_schema_path, "w", encoding="utf-8") as f:
            f.write(self.schema_json)

    def tearDown(self):
        if os.path.exists(self.test_schema_path):
            os.remove(self.test_schema_path)

    def test_mysql_schema_generation(self):
        """Test MySQL schema generation"""
        sql_path = os.path.join(tempfile.gettempdir(), "test_mysql.sql")
        try:
            convert_structure_to_sql(self.test_schema_path, sql_path, "mysql", emit_cloudevents_columns=False)
            
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            # Verify basic structure
            self.assertIn("CREATE TABLE", sql_content)
            self.assertIn("`TestRecord`", sql_content)  # Table name from schema name
            self.assertIn("`id`", sql_content)
            self.assertIn("`name`", sql_content)
            self.assertIn("INT", sql_content)
            self.assertIn("VARCHAR", sql_content)
            self.assertIn("PRIMARY KEY", sql_content)
        finally:
            if os.path.exists(sql_path):
                os.remove(sql_path)

    def test_postgres_schema_generation(self):
        """Test PostgreSQL schema generation"""
        sql_path = os.path.join(tempfile.gettempdir(), "test_postgres.sql")
        try:
            convert_structure_to_sql(self.test_schema_path, sql_path, "postgres", emit_cloudevents_columns=False)
            
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            # Verify basic structure
            self.assertIn("CREATE TABLE", sql_content)
            self.assertIn('"TestRecord"', sql_content)  # Table name from schema name
            self.assertIn('"id"', sql_content)
            self.assertIn("INTEGER", sql_content)
            self.assertIn("VARCHAR", sql_content)
        finally:
            if os.path.exists(sql_path):
                os.remove(sql_path)

    def test_sqlserver_schema_generation(self):
        """Test SQL Server schema generation"""
        sql_path = os.path.join(tempfile.gettempdir(), "test_sqlserver.sql")
        try:
            convert_structure_to_sql(self.test_schema_path, sql_path, "sqlserver", emit_cloudevents_columns=False)
            
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            # Verify basic structure
            self.assertIn("CREATE TABLE", sql_content)
            self.assertIn("[TestRecord]", sql_content)  # Table name from schema name
            self.assertIn("[id]", sql_content)
            self.assertIn("INT", sql_content)
            self.assertIn("NVARCHAR", sql_content)
        finally:
            if os.path.exists(sql_path):
                os.remove(sql_path)

    def test_cloudevents_columns(self):
        """Test CloudEvents columns are added when requested"""
        sql_path = os.path.join(tempfile.gettempdir(), "test_cloudevents.sql")
        try:
            convert_structure_to_sql(self.test_schema_path, sql_path, "mysql", emit_cloudevents_columns=True)
            
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            # Verify CloudEvents columns
            self.assertIn("___type", sql_content)
            self.assertIn("___source", sql_content)
            self.assertIn("___id", sql_content)
            self.assertIn("___time", sql_content)
            self.assertIn("___subject", sql_content)
        finally:
            if os.path.exists(sql_path):
                os.remove(sql_path)

    def test_cassandra_schema_generation(self):
        """Test Cassandra schema generation"""
        cql_path = os.path.join(tempfile.gettempdir(), "test_cassandra.cql")
        try:
            convert_structure_to_sql(self.test_schema_path, cql_path, "cassandra", emit_cloudevents_columns=False)
            
            with open(cql_path, "r", encoding="utf-8") as f:
                cql_content = f.read()
            
            # Verify basic structure
            self.assertIn("CREATE TABLE", cql_content)
            self.assertIn("TestRecord", cql_content)  # Table name includes namespace for Cassandra
            self.assertIn('"id"', cql_content)
            self.assertIn("int", cql_content)
            self.assertIn("text", cql_content)
            self.assertIn("PRIMARY KEY", cql_content)
        finally:
            if os.path.exists(cql_path):
                os.remove(cql_path)

    def test_mongodb_schema_generation(self):
        """Test MongoDB schema generation"""
        nosql_dir = os.path.join(tempfile.gettempdir(), "test_mongodb")
        try:
            convert_structure_to_nosql(self.test_schema_path, nosql_dir, "mongodb", emit_cloudevents_columns=False)
            
            # Find generated JSON file
            json_files = [f for f in os.listdir(nosql_dir) if f.endswith('.json')]
            self.assertTrue(len(json_files) > 0, "No JSON files generated")
            
            with open(os.path.join(nosql_dir, json_files[0]), "r", encoding="utf-8") as f:
                mongo_content = json.load(f)
            
            # Verify basic structure
            self.assertIn("$jsonSchema", list(mongo_content.values())[0])
            schema = list(mongo_content.values())[0]["$jsonSchema"]
            self.assertIn("properties", schema)
            self.assertIn("id", schema["properties"])
            self.assertIn("name", schema["properties"])
        finally:
            if os.path.exists(nosql_dir):
                import shutil
                shutil.rmtree(nosql_dir)

    def test_type_mappings(self):
        """Test that various JSON Structure types map correctly"""
        complex_schema = {
            "type": "object",
            "name": "ComplexRecord",
            "properties": {
                "bool_field": {"type": "boolean"},
                "int8_field": {"type": "int8"},
                "int16_field": {"type": "int16"},
                "int32_field": {"type": "int32"},
                "int64_field": {"type": "int64"},
                "float_field": {"type": "float"},
                "double_field": {"type": "double"},
                "string_field": {"type": "string"},
                "binary_field": {"type": "binary"},
                "date_field": {"type": "date"},
                "datetime_field": {"type": "datetime"},
                "uuid_field": {"type": "uuid"},
                "array_field": {"type": "array", "items": {"type": "string"}},
                "map_field": {"type": "map", "values": {"type": "int32"}}
            },
            "required": ["int32_field"]
        }
        
        schema_path = os.path.join(tempfile.gettempdir(), "complex_schema.struct.json")
        sql_path = os.path.join(tempfile.gettempdir(), "complex_test.sql")
        
        try:
            with open(schema_path, "w", encoding="utf-8") as f:
                json.dump(complex_schema, f)
            
            convert_structure_to_sql(schema_path, sql_path, "postgres", emit_cloudevents_columns=False)
            
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            # Verify type mappings
            self.assertIn("BOOLEAN", sql_content)
            self.assertIn("INTEGER", sql_content)
            self.assertIn("BIGINT", sql_content)
            self.assertIn("REAL", sql_content)
            self.assertIn("DOUBLE PRECISION", sql_content)
            self.assertIn("VARCHAR", sql_content)
            self.assertIn("BYTEA", sql_content)
            self.assertIn("DATE", sql_content)
            self.assertIn("TIMESTAMP", sql_content)
            self.assertIn("UUID", sql_content)
            self.assertIn("JSONB", sql_content)  # For array and map types
        finally:
            if os.path.exists(schema_path):
                os.remove(schema_path)
            if os.path.exists(sql_path):
                os.remove(sql_path)

    def test_inheritance_with_extends(self):
        """Test that $extends properly merges properties from base types"""
        inheritance_schema = {
            "definitions": {
                "BaseEntity": {
                    "type": "object",
                    "abstract": True,
                    "properties": {
                        "id": {"type": "int32"},
                        "createdAt": {"type": "datetime"}
                    },
                    "required": ["id"]
                }
            },
            "type": "object",
            "name": "DerivedEntity",
            "$extends": "#/definitions/BaseEntity",
            "properties": {
                "name": {"type": "string"},
                "value": {"type": "int32"}
            },
            "required": ["name"]
        }
        
        schema_path = os.path.join(tempfile.gettempdir(), "inheritance_schema.struct.json")
        sql_path = os.path.join(tempfile.gettempdir(), "inheritance_test.sql")
        
        try:
            with open(schema_path, "w", encoding="utf-8") as f:
                json.dump(inheritance_schema, f)
            
            convert_structure_to_sql(schema_path, sql_path, "postgres", emit_cloudevents_columns=False)
            
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            # Verify inheritance comment
            self.assertIn("Inherits from:", sql_content)
            self.assertIn("#/definitions/BaseEntity", sql_content)
            
            # Verify all properties are included (inherited + own)
            self.assertIn('"id"', sql_content)
            self.assertIn('"createdAt"', sql_content)
            self.assertIn('"name"', sql_content)
            self.assertIn('"value"', sql_content)
            
            # Verify all required properties are in PRIMARY KEY
            self.assertIn("PRIMARY KEY", sql_content)
            self.assertIn('"id"', sql_content.split("PRIMARY KEY")[1])
            self.assertIn('"name"', sql_content.split("PRIMARY KEY")[1])
        finally:
            if os.path.exists(schema_path):
                os.remove(schema_path)
            if os.path.exists(sql_path):
                os.remove(sql_path)
    
    def test_abstract_type_not_generated(self):
        """Test that abstract types don't generate tables"""
        schema_with_abstract = {
            "definitions": {
                "AbstractBase": {
                    "type": "object",
                    "name": "AbstractBase",
                    "abstract": True,
                    "properties": {
                        "id": {"type": "int32"}
                    }
                }
            },
            "type": "object",
            "name": "ConcreteType",
            "$extends": "#/definitions/AbstractBase",
            "properties": {
                "data": {"type": "string"}
            }
        }
        
        schema_path = os.path.join(tempfile.gettempdir(), "abstract_schema.struct.json")
        sql_path = os.path.join(tempfile.gettempdir(), "abstract_test.sql")
        
        try:
            with open(schema_path, "w", encoding="utf-8") as f:
                json.dump(schema_with_abstract, f)
            
            convert_structure_to_sql(schema_path, sql_path, "mysql", emit_cloudevents_columns=False)
            
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            # Verify only ConcreteType table is created, not AbstractBase
            self.assertIn("`ConcreteType`", sql_content)
            self.assertNotIn("`AbstractBase`", sql_content)
            
            # Verify ConcreteType has properties from both base and derived
            self.assertIn("`id`", sql_content)
            self.assertIn("`data`", sql_content)
        finally:
            if os.path.exists(schema_path):
                os.remove(schema_path)
            if os.path.exists(sql_path):
                os.remove(sql_path)
    
    def test_tuple_type_generation(self):
        """Test that tuple types generate tables"""
        tuple_schema = {
            "type": "tuple",
            "name": "CoordinatePair",
            "properties": {
                "x": {"type": "double"},
                "y": {"type": "double"}
            },
            "required": ["x", "y"]
        }
        
        schema_path = os.path.join(tempfile.gettempdir(), "tuple_schema.struct.json")
        sql_path = os.path.join(tempfile.gettempdir(), "tuple_test.sql")
        
        try:
            with open(schema_path, "w", encoding="utf-8") as f:
                json.dump(tuple_schema, f)
            
            convert_structure_to_sql(schema_path, sql_path, "postgres", emit_cloudevents_columns=False)
            
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            
            # Verify tuple generates a table
            self.assertIn("CREATE TABLE", sql_content)
            self.assertIn('"CoordinatePair"', sql_content)
            self.assertIn('"x"', sql_content)
            self.assertIn('"y"', sql_content)
        finally:
            if os.path.exists(schema_path):
                os.remove(schema_path)
            if os.path.exists(sql_path):
                os.remove(sql_path)

    def test_string_maxlength_annotation(self):
        """Test that maxLength annotation is applied to string columns"""
        schema_with_maxlength = {
            "type": "object",
            "name": "Product",
            "properties": {
                "id": {"type": "int32"},
                "name": {"type": "string", "maxLength": 100},
                "description": {"type": "string", "maxLength": 2000},
                "sku": {"type": "string"}  # No maxLength, should use default
            },
            "required": ["id"]
        }
        
        schema_path = os.path.join(tempfile.gettempdir(), "maxlength_schema.struct.json")
        
        try:
            with open(schema_path, "w", encoding="utf-8") as f:
                json.dump(schema_with_maxlength, f)
            
            # Test SQL Server (uses NVARCHAR)
            sql_path = os.path.join(tempfile.gettempdir(), "maxlength_sqlserver.sql")
            convert_structure_to_sql(schema_path, sql_path, "sqlserver", emit_cloudevents_columns=False)
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            self.assertIn("NVARCHAR(100)", sql_content)
            self.assertIn("NVARCHAR(2000)", sql_content)
            self.assertIn("NVARCHAR(512)", sql_content)  # Default for sku
            os.remove(sql_path)
            
            # Test PostgreSQL (uses VARCHAR)
            sql_path = os.path.join(tempfile.gettempdir(), "maxlength_postgres.sql")
            convert_structure_to_sql(schema_path, sql_path, "postgres", emit_cloudevents_columns=False)
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            self.assertIn("VARCHAR(100)", sql_content)
            self.assertIn("VARCHAR(2000)", sql_content)
            self.assertIn("VARCHAR(512)", sql_content)  # Default for sku
            os.remove(sql_path)
            
            # Test MySQL (uses VARCHAR)
            sql_path = os.path.join(tempfile.gettempdir(), "maxlength_mysql.sql")
            convert_structure_to_sql(schema_path, sql_path, "mysql", emit_cloudevents_columns=False)
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            self.assertIn("VARCHAR(100)", sql_content)
            self.assertIn("VARCHAR(2000)", sql_content)
            os.remove(sql_path)
            
            # Test Oracle (uses VARCHAR2)
            sql_path = os.path.join(tempfile.gettempdir(), "maxlength_oracle.sql")
            convert_structure_to_sql(schema_path, sql_path, "oracle", emit_cloudevents_columns=False)
            with open(sql_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            self.assertIn("VARCHAR2(100)", sql_content)
            self.assertIn("VARCHAR2(2000)", sql_content)
            os.remove(sql_path)
        finally:
            if os.path.exists(schema_path):
                os.remove(schema_path)


if __name__ == '__main__':
    unittest.main()
