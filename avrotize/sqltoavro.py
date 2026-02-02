"""Converts SQL database schemas to Avro schema format."""

import copy
import json
import os
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple, cast
from urllib.parse import urlparse, parse_qs

from avrotize.common import avro_name, get_tree_hash
from avrotize.constants import AVRO_VERSION

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | float | None


class SqlToAvro:
    """Converts SQL database schemas to Avro schema format."""

    def __init__(
        self,
        connection_string: str,
        database: str | None,
        table_name: str | None,
        avro_namespace: str,
        avro_schema_path: str,
        dialect: str,
        emit_cloudevents: bool,
        emit_cloudevents_xregistry: bool,
        sample_size: int = 100,
        infer_json_schema: bool = True,
        infer_xml_schema: bool = True
    ):
        """Initializes the SqlToAvro class with database connection parameters.

        Args:
            connection_string: Database connection string (e.g., postgresql://user:pass@host:port/dbname)
            database: Database name (overrides connection string if provided)
            table_name: Specific table to convert (None for all tables)
            avro_namespace: Namespace for generated Avro schemas
            avro_schema_path: Output path for the Avro schema file
            dialect: SQL dialect (postgres, mysql, sqlserver, oracle, sqlite)
            emit_cloudevents: Whether to emit CloudEvents declarations
            emit_cloudevents_xregistry: Whether to emit xRegistry manifest format
            sample_size: Number of rows to sample for JSON/XML inference
            infer_json_schema: Whether to infer schema for JSON columns
            infer_xml_schema: Whether to infer schema for XML columns
        """
        self.connection_string = connection_string
        self.dialect = dialect.lower()
        self.single_table_name = table_name
        self.avro_namespace = avro_namespace
        self.avro_schema_path = avro_schema_path
        self.emit_xregistry = emit_cloudevents_xregistry
        self.emit_cloudevents = emit_cloudevents or emit_cloudevents_xregistry
        self.sample_size = sample_size
        self.infer_json_schema = infer_json_schema
        self.infer_xml_schema = infer_xml_schema
        self.generated_types: List[str] = []

        if self.emit_xregistry and not self.avro_namespace:
            raise ValueError(
                "The avro_namespace must be specified when emit_cloudevents_xregistry is True")

        # Parse connection string and establish connection
        self.connection = self._connect(connection_string, database)
        self.database = database or self._extract_database_from_connection_string(connection_string)

    def _extract_database_from_connection_string(self, connection_string: str) -> str:
        """Extracts database name from connection string."""
        parsed = urlparse(connection_string)
        if parsed.path:
            return parsed.path.lstrip('/')
        return ''

    def _connect(self, connection_string: str, database: str | None):
        """Establishes database connection based on dialect.
        
        Connection strings can include SSL/TLS and authentication options:
        
        PostgreSQL:
          - Standard: postgresql://user:pass@host:port/dbname
          - SSL: postgresql://user:pass@host:port/dbname?sslmode=require
          - SSL modes: disable, allow, prefer, require, verify-ca, verify-full
          
        MySQL:
          - Standard: mysql://user:pass@host:port/dbname
          - SSL: mysql://user:pass@host:port/dbname?ssl=true
          - SSL with cert: mysql://...?ssl_ca=/path/to/ca.pem
          
        SQL Server:
          - Standard: mssql://user:pass@host:port/dbname
          - Windows Auth: mssql://@host:port/dbname (no user/pass = integrated)
          - Encrypt: mssql://...?encrypt=true
          - Trust cert: mssql://...?trustServerCertificate=true
          
        Oracle:
          - Standard: oracle://user:pass@host:port/service_name
          - Wallet: Uses TNS names or wallet configuration
        """
        parsed = urlparse(connection_string)
        query_params = dict(parse_qs(parsed.query)) if parsed.query else {}
        # Flatten single-value lists
        query_params = {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}
        
        if self.dialect == 'postgres':
            try:
                import psycopg2
            except ImportError:
                raise ImportError(
                    "psycopg2 is required for PostgreSQL support. "
                    "Install with: pip install psycopg2-binary"
                )
            # psycopg2 handles the full connection string including sslmode
            return psycopg2.connect(connection_string)
        elif self.dialect == 'mysql':
            try:
                import pymysql
            except ImportError:
                raise ImportError(
                    "pymysql is required for MySQL support. "
                    "Install with: pip install pymysql"
                )
            
            connect_kwargs = {
                'host': parsed.hostname or 'localhost',
                'port': parsed.port or 3306,
                'user': parsed.username,
                'password': parsed.password,
                'database': database or parsed.path.lstrip('/')
            }
            
            # SSL/TLS configuration
            ssl_config = {}
            if query_params.get('ssl') in ('true', 'True', '1', True):
                ssl_config['ssl'] = True
            if 'ssl_ca' in query_params:
                ssl_config['ssl'] = {'ca': query_params['ssl_ca']}
            if 'ssl_cert' in query_params:
                ssl_config.setdefault('ssl', {})
                if isinstance(ssl_config['ssl'], dict):
                    ssl_config['ssl']['cert'] = query_params['ssl_cert']
            if 'ssl_key' in query_params:
                ssl_config.setdefault('ssl', {})
                if isinstance(ssl_config['ssl'], dict):
                    ssl_config['ssl']['key'] = query_params['ssl_key']
            
            if ssl_config:
                connect_kwargs.update(ssl_config)
            
            return pymysql.connect(**connect_kwargs)
        elif self.dialect == 'sqlserver':
            try:
                import pymssql
                use_pymssql = True
            except ImportError:
                use_pymssql = False
                try:
                    import pyodbc
                except ImportError:
                    raise ImportError(
                        "pymssql or pyodbc is required for SQL Server support. "
                        "Install with: pip install pymssql or pip install pyodbc"
                    )
            
            if not use_pymssql:
                # pyodbc - pass connection string directly (supports all ODBC options)
                return pyodbc.connect(connection_string)
            
            # pymssql - parse and build connection
            connect_kwargs = {
                'server': parsed.hostname or 'localhost',
                'port': str(parsed.port or 1433),
                'database': database or parsed.path.lstrip('/')
            }
            
            # Check for integrated/Windows authentication (no username)
            if parsed.username:
                connect_kwargs['user'] = parsed.username
                connect_kwargs['password'] = parsed.password or ''
            # If no username, pymssql will attempt Windows auth
            
            # TLS/encryption options
            if query_params.get('encrypt') in ('true', 'True', '1', True):
                connect_kwargs['tds_version'] = '7.4'  # Ensures TLS
            if query_params.get('trustServerCertificate') in ('true', 'True', '1', True):
                # pymssql doesn't directly support this, but we note it
                pass
            
            return pymssql.connect(**connect_kwargs)
        elif self.dialect == 'oracle':
            try:
                import oracledb
            except ImportError:
                raise ImportError(
                    "oracledb is required for Oracle support. "
                    "Install with: pip install oracledb"
                )
            # oracledb supports various connection methods including wallets
            return oracledb.connect(connection_string)
        elif self.dialect == 'sqlite':
            import sqlite3
            # For SQLite, connection_string is the file path
            return sqlite3.connect(connection_string)
        else:
            raise ValueError(f"Unsupported SQL dialect: {self.dialect}")

    def close(self):
        """Closes the database connection."""
        if self.connection:
            self.connection.close()

    # -------------------------------------------------------------------------
    # Type Mapping
    # -------------------------------------------------------------------------

    # PostgreSQL type mapping
    postgres_type_map: Dict[str, JsonNode] = {
        # Numeric types
        'smallint': 'int',
        'int2': 'int',
        'integer': 'int',
        'int': 'int',
        'int4': 'int',
        'bigint': 'long',
        'int8': 'long',
        'real': 'float',
        'float4': 'float',
        'double precision': 'double',
        'float8': 'double',
        'smallserial': 'int',
        'serial': 'int',
        'bigserial': 'long',
        # Boolean
        'boolean': 'boolean',
        'bool': 'boolean',
        # Character types
        'character varying': 'string',
        'varchar': 'string',
        'character': 'string',
        'char': 'string',
        'bpchar': 'string',
        'text': 'string',
        'name': 'string',
        # Binary
        'bytea': 'bytes',
        # Date/Time types
        'date': {'type': 'int', 'logicalType': 'date'},
        'time': {'type': 'int', 'logicalType': 'time-millis'},
        'time with time zone': {'type': 'int', 'logicalType': 'time-millis'},
        'time without time zone': {'type': 'int', 'logicalType': 'time-millis'},
        'timetz': {'type': 'int', 'logicalType': 'time-millis'},
        'timestamp': {'type': 'long', 'logicalType': 'timestamp-millis'},
        'timestamp with time zone': {'type': 'long', 'logicalType': 'timestamp-millis'},
        'timestamp without time zone': {'type': 'long', 'logicalType': 'timestamp-millis'},
        'timestamptz': {'type': 'long', 'logicalType': 'timestamp-millis'},
        'interval': {'type': 'fixed', 'size': 12, 'name': 'duration', 'logicalType': 'duration'},
        # UUID
        'uuid': {'type': 'string', 'logicalType': 'uuid'},
        # JSON types (will be inferred if enabled)
        'json': 'string',
        'jsonb': 'string',
        # XML (will be inferred if enabled)
        'xml': 'string',
        # Network types
        'inet': 'string',
        'cidr': 'string',
        'macaddr': 'string',
        'macaddr8': 'string',
        # Geometric types (stored as string representation)
        'point': 'string',
        'line': 'string',
        'lseg': 'string',
        'box': 'string',
        'path': 'string',
        'polygon': 'string',
        'circle': 'string',
        # Other
        'money': {'type': 'bytes', 'logicalType': 'decimal', 'precision': 19, 'scale': 2},
        'bit': 'string',
        'bit varying': 'string',
        'varbit': 'string',
        'tsvector': 'string',
        'tsquery': 'string',
        'oid': 'long',
    }

    mysql_type_map: Dict[str, JsonNode] = {
        'tinyint': 'int',
        'smallint': 'int',
        'mediumint': 'int',
        'int': 'int',
        'integer': 'int',
        'bigint': 'long',
        'float': 'float',
        'double': 'double',
        'decimal': {'type': 'bytes', 'logicalType': 'decimal', 'precision': 38, 'scale': 10},
        'numeric': {'type': 'bytes', 'logicalType': 'decimal', 'precision': 38, 'scale': 10},
        'bit': 'boolean',
        'boolean': 'boolean',
        'bool': 'boolean',
        'char': 'string',
        'varchar': 'string',
        'tinytext': 'string',
        'text': 'string',
        'mediumtext': 'string',
        'longtext': 'string',
        'binary': 'bytes',
        'varbinary': 'bytes',
        'tinyblob': 'bytes',
        'blob': 'bytes',
        'mediumblob': 'bytes',
        'longblob': 'bytes',
        'date': {'type': 'int', 'logicalType': 'date'},
        'time': {'type': 'int', 'logicalType': 'time-millis'},
        'datetime': {'type': 'long', 'logicalType': 'timestamp-millis'},
        'timestamp': {'type': 'long', 'logicalType': 'timestamp-millis'},
        'year': 'int',
        'json': 'string',
        'enum': 'string',
        'set': 'string',
    }

    sqlserver_type_map: Dict[str, JsonNode] = {
        'bit': 'boolean',
        'tinyint': 'int',
        'smallint': 'int',
        'int': 'int',
        'bigint': 'long',
        'float': 'double',
        'real': 'float',
        'decimal': {'type': 'bytes', 'logicalType': 'decimal', 'precision': 38, 'scale': 10},
        'numeric': {'type': 'bytes', 'logicalType': 'decimal', 'precision': 38, 'scale': 10},
        'money': {'type': 'bytes', 'logicalType': 'decimal', 'precision': 19, 'scale': 4},
        'smallmoney': {'type': 'bytes', 'logicalType': 'decimal', 'precision': 10, 'scale': 4},
        'char': 'string',
        'varchar': 'string',
        'nchar': 'string',
        'nvarchar': 'string',
        'text': 'string',
        'ntext': 'string',
        'binary': 'bytes',
        'varbinary': 'bytes',
        'image': 'bytes',
        'date': {'type': 'int', 'logicalType': 'date'},
        'time': {'type': 'int', 'logicalType': 'time-millis'},
        'datetime': {'type': 'long', 'logicalType': 'timestamp-millis'},
        'datetime2': {'type': 'long', 'logicalType': 'timestamp-millis'},
        'smalldatetime': {'type': 'long', 'logicalType': 'timestamp-millis'},
        'datetimeoffset': {'type': 'long', 'logicalType': 'timestamp-millis'},
        'uniqueidentifier': {'type': 'string', 'logicalType': 'uuid'},
        'xml': 'string',
        'sql_variant': 'string',
        'hierarchyid': 'string',
        'geometry': 'bytes',
        'geography': 'bytes',
    }

    def get_type_map(self) -> Dict[str, JsonNode]:
        """Returns the type map for the current dialect."""
        if self.dialect == 'postgres':
            return self.postgres_type_map
        elif self.dialect == 'mysql':
            return self.mysql_type_map
        elif self.dialect == 'sqlserver':
            return self.sqlserver_type_map
        else:
            # Default to postgres map for now
            return self.postgres_type_map

    # -------------------------------------------------------------------------
    # Schema Extraction (PostgreSQL)
    # -------------------------------------------------------------------------

    def fetch_tables(self) -> List[Dict[str, str]]:
        """Fetches list of tables from the database."""
        cursor = self.connection.cursor()
        
        if self.dialect == 'postgres':
            query = """
                SELECT table_name, table_schema
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                  AND table_type = 'BASE TABLE'
            """
            if self.single_table_name:
                query += f" AND table_name = '{self.single_table_name}'"
            query += " ORDER BY table_schema, table_name"
        elif self.dialect == 'mysql':
            query = """
                SELECT table_name, table_schema
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_type = 'BASE TABLE'
            """
            if self.single_table_name:
                query += f" AND table_name = '{self.single_table_name}'"
        elif self.dialect == 'sqlserver':
            query = """
                SELECT t.name AS table_name, s.name AS table_schema
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.type = 'U'
            """
            if self.single_table_name:
                query += f" AND t.name = '{self.single_table_name}'"
            query += " ORDER BY s.name, t.name"
        else:
            raise NotImplementedError(f"fetch_tables not implemented for {self.dialect}")

        cursor.execute(query)
        tables = [{'table_name': row[0], 'table_schema': row[1]} for row in cursor.fetchall()]
        cursor.close()
        return tables

    def fetch_table_columns(self, table_name: str, table_schema: str = 'public') -> List[Dict[str, Any]]:
        """Fetches column information for a table."""
        cursor = self.connection.cursor()

        if self.dialect == 'postgres':
            query = """
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.udt_name,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.ordinal_position,
                    col_description(
                        (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass::oid,
                        c.ordinal_position
                    ) as column_comment
                FROM information_schema.columns c
                WHERE c.table_name = %s AND c.table_schema = %s
                ORDER BY c.ordinal_position
            """
            cursor.execute(query, (table_name, table_schema))
        elif self.dialect == 'mysql':
            query = """
                SELECT 
                    column_name,
                    data_type,
                    column_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal_position,
                    column_comment
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = %s
                ORDER BY ordinal_position
            """
            cursor.execute(query, (table_name, table_schema))
        elif self.dialect == 'sqlserver':
            query = """
                SELECT 
                    c.name AS column_name,
                    t.name AS data_type,
                    t.name AS udt_name,
                    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
                    dc.definition AS column_default,
                    c.max_length AS character_maximum_length,
                    c.precision AS numeric_precision,
                    c.scale AS numeric_scale,
                    c.column_id AS ordinal_position,
                    ep.value AS column_comment
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                INNER JOIN sys.tables tb ON c.object_id = tb.object_id
                INNER JOIN sys.schemas s ON tb.schema_id = s.schema_id
                LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
                LEFT JOIN sys.extended_properties ep 
                    ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.name = 'MS_Description'
                WHERE tb.name = %s AND s.name = %s
                ORDER BY c.column_id
            """
            cursor.execute(query, (table_name, table_schema))
        else:
            raise NotImplementedError(f"fetch_table_columns not implemented for {self.dialect}")

        columns = []
        for row in cursor.fetchall():
            columns.append({
                'column_name': row[0],
                'data_type': row[1],
                'udt_name': row[2],
                'is_nullable': row[3] == 'YES',
                'column_default': row[4],
                'character_maximum_length': row[5],
                'numeric_precision': row[6],
                'numeric_scale': row[7],
                'ordinal_position': row[8],
                'column_comment': row[9] if len(row) > 9 else None
            })
        cursor.close()
        return columns

    def fetch_primary_keys(self, table_name: str, table_schema: str = 'public') -> List[str]:
        """Fetches primary key columns for a table."""
        cursor = self.connection.cursor()

        if self.dialect == 'postgres':
            query = """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.table_name = %s 
                  AND tc.table_schema = %s
                  AND tc.constraint_type = 'PRIMARY KEY'
                ORDER BY kcu.ordinal_position
            """
            cursor.execute(query, (table_name, table_schema))
        elif self.dialect == 'mysql':
            query = """
                SELECT column_name
                FROM information_schema.key_column_usage
                WHERE table_name = %s 
                  AND table_schema = %s
                  AND constraint_name = 'PRIMARY'
                ORDER BY ordinal_position
            """
            cursor.execute(query, (table_name, table_schema))
        elif self.dialect == 'sqlserver':
            query = """
                SELECT c.name AS column_name
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN sys.tables t ON i.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE i.is_primary_key = 1 AND t.name = %s AND s.name = %s
                ORDER BY ic.key_ordinal
            """
            cursor.execute(query, (table_name, table_schema))
        else:
            cursor.close()
            return []

        pk_columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return pk_columns

    def fetch_table_comment(self, table_name: str, table_schema: str = 'public') -> str | None:
        """Fetches table comment/description."""
        cursor = self.connection.cursor()

        if self.dialect == 'postgres':
            query = """
                SELECT obj_description(
                    (quote_ident(%s) || '.' || quote_ident(%s))::regclass::oid,
                    'pg_class'
                )
            """
            cursor.execute(query, (table_schema, table_name))
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else None
        elif self.dialect == 'mysql':
            query = """
                SELECT table_comment
                FROM information_schema.tables
                WHERE table_name = %s AND table_schema = %s
            """
            cursor.execute(query, (table_name, table_schema))
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result and result[0] else None
        elif self.dialect == 'sqlserver':
            query = """
                SELECT ep.value
                FROM sys.extended_properties ep
                INNER JOIN sys.tables t ON ep.major_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE ep.minor_id = 0 AND ep.name = 'MS_Description'
                  AND t.name = %s AND s.name = %s
            """
            cursor.execute(query, (table_name, table_schema))
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result and result[0] else None
        
        cursor.close()
        return None

    # -------------------------------------------------------------------------
    # JSON/XML Schema Inference (following k2a patterns)
    # -------------------------------------------------------------------------

    def fold_record_types(self, base_record: dict, new_record: dict) -> Tuple[bool, dict]:
        """Merges two record types following k2a pattern."""
        base_fields = copy.deepcopy(base_record).get("fields", [])
        new_fields = new_record.get("fields", [])

        for field in new_fields:
            base_field = next(
                (f for f in base_fields if f["name"] == field["name"]), None)
            if not base_field:
                base_fields.append(field)
            else:
                if isinstance(base_field["type"], str) and base_field["type"] != field["type"]:
                    return False, new_record
                elif isinstance(base_field["type"], dict) and isinstance(field["type"], dict):
                    if base_field["type"].get("type") == field["type"].get("type"):
                        result, record_type = self.fold_record_types(
                            base_field["type"], field["type"])
                        if not result:
                            return False, new_record
                        base_field["type"] = record_type
        base_record["fields"] = base_fields
        return True, base_record

    def python_type_to_avro_type(self, type_name: str, python_value: Any) -> JsonNode:
        """Maps Python types to Avro types (following k2a pattern)."""
        simple_types = {
            int: "long",  # Use long for safety with large integers
            float: "double",
            str: "string",
            bool: "boolean",
            bytes: "bytes"
        }
        
        if python_value is None:
            return "null"
        
        if isinstance(python_value, dict):
            type_name_name = avro_name(type_name.rsplit('.', 1)[-1])
            type_name_namespace = (type_name.rsplit('.', 1)[0]) + "Types" if '.' in type_name else ''
            type_namespace = self.avro_namespace + ('.' if self.avro_namespace and type_name_namespace else '') + type_name_namespace
            record: Dict[str, JsonNode] = {
                "type": "record",
                "name": type_name_name,
            }
            if type_namespace:
                record["namespace"] = type_namespace
            fields: List[JsonNode] = []
            for key, value in python_value.items():
                original_key = key
                key = avro_name(key)
                field: Dict[str, JsonNode] = {
                    "name": key,
                    "type": self.python_type_to_avro_type(f"{type_name}.{key}", value)
                }
                if original_key != key:
                    field["altnames"] = {"sql": original_key}
                fields.append(field)
            record["fields"] = fields
            return record
        
        if isinstance(python_value, list):
            if len(python_value) > 0:
                item_types = self.consolidated_type_list(type_name, python_value)
            else:
                item_types = ["string"]
            if len(item_types) == 1:
                return {"type": "array", "items": item_types[0]}
            else:
                return {"type": "array", "items": item_types}
        
        return simple_types.get(type(python_value), "string")

    def consolidated_type_list(self, type_name: str, python_values: list) -> List[JsonNode]:
        """Consolidates a list of types (following k2a pattern)."""
        list_types = [self.python_type_to_avro_type(type_name, item) for item in python_values]

        # Eliminate duplicates using tree hashing
        tree_hashes = {}
        for item in list_types:
            tree_hash = get_tree_hash(item)
            if tree_hash.hash_value not in tree_hashes:
                tree_hashes[tree_hash.hash_value] = item
        list_types = list(tree_hashes.values())

        # Try to fold record types together
        unique_types = []
        prior_record = None
        for item in list_types:
            if isinstance(item, dict) and item.get("type") == "record":
                if prior_record is None:
                    prior_record = item
                else:
                    folded, record = self.fold_record_types(prior_record, item)
                    if not folded:
                        unique_types.append(item)
                    else:
                        prior_record = record
            else:
                unique_types.append(item)
        if prior_record is not None:
            unique_types.append(prior_record)

        # Consolidate array and map types
        array_types = [item["items"] for item in unique_types 
                       if isinstance(item, dict) and item.get("type") == "array"]
        map_types = [item["values"] for item in unique_types 
                     if isinstance(item, dict) and item.get("type") == "map"]
        list_types = [item for item in unique_types 
                      if not isinstance(item, dict) or item.get("type") not in ["array", "map"]]

        item_types: List[JsonNode] = []
        for item2 in array_types:
            if isinstance(item2, list):
                item_types.extend(item2)
            else:
                item_types.append(item2)
        if len(item_types) > 0:
            list_types.append({"type": "array", "items": item_types})

        value_types: List[JsonNode] = []
        for item3 in map_types:
            if isinstance(item3, list):
                value_types.extend(item3)
            else:
                value_types.append(item3)
        if len(value_types) > 0:
            list_types.append({"type": "map", "values": value_types})

        return list_types

    def infer_json_column_schema(
        self,
        table_name: str,
        table_schema: str,
        column_name: str,
        type_column: str | None = None,
        type_value: str | None = None
    ) -> JsonNode:
        """Infers Avro schema for a JSON/JSONB column by sampling data."""
        cursor = self.connection.cursor()

        if self.dialect == 'postgres':
            if type_column and type_value:
                query = f"""
                    SELECT "{column_name}"::text
                    FROM "{table_schema}"."{table_name}"
                    WHERE "{type_column}" = %s AND "{column_name}" IS NOT NULL
                    LIMIT {self.sample_size}
                """
                cursor.execute(query, (type_value,))
            else:
                query = f"""
                    SELECT "{column_name}"::text
                    FROM "{table_schema}"."{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    LIMIT {self.sample_size}
                """
                cursor.execute(query)
        elif self.dialect == 'mysql':
            if type_column and type_value:
                query = f"""
                    SELECT `{column_name}`
                    FROM `{table_schema}`.`{table_name}`
                    WHERE `{type_column}` = %s AND `{column_name}` IS NOT NULL
                    LIMIT {self.sample_size}
                """
                cursor.execute(query, (type_value,))
            else:
                query = f"""
                    SELECT `{column_name}`
                    FROM `{table_schema}`.`{table_name}`
                    WHERE `{column_name}` IS NOT NULL
                    LIMIT {self.sample_size}
                """
                cursor.execute(query)
        else:
            cursor.close()
            return "string"

        values = []
        for row in cursor.fetchall():
            if row[0]:
                try:
                    parsed = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                    values.append(parsed)
                except (json.JSONDecodeError, TypeError):
                    pass
        cursor.close()

        if not values:
            return "string"

        type_name = type_value if type_value else f"{table_name}.{column_name}"
        unique_types = self.consolidated_type_list(type_name, values)

        if len(unique_types) > 1:
            return unique_types
        elif len(unique_types) == 1:
            return unique_types[0]
        else:
            return "string"

    def infer_xml_column_schema(
        self,
        table_name: str,
        table_schema: str,
        column_name: str
    ) -> JsonNode:
        """Infers Avro schema for an XML column by sampling data."""
        cursor = self.connection.cursor()

        if self.dialect == 'postgres':
            query = f"""
                SELECT "{column_name}"::text
                FROM "{table_schema}"."{table_name}"
                WHERE "{column_name}" IS NOT NULL
                LIMIT {self.sample_size}
            """
            cursor.execute(query)
        else:
            cursor.close()
            return "string"

        xml_structures: List[Dict[str, Any]] = []
        for row in cursor.fetchall():
            if row[0]:
                try:
                    structure = self._parse_xml_to_dict(row[0])
                    if structure:
                        xml_structures.append(structure)
                except ET.ParseError:
                    pass
        cursor.close()

        if not xml_structures:
            return "string"

        type_name = f"{table_name}.{column_name}"
        unique_types = self.consolidated_type_list(type_name, xml_structures)

        if len(unique_types) > 1:
            return unique_types
        elif len(unique_types) == 1:
            return unique_types[0]
        else:
            return "string"

    def _parse_xml_to_dict(self, xml_string: str) -> Dict[str, Any] | None:
        """Parses XML string to a dictionary structure for schema inference."""
        try:
            root = ET.fromstring(xml_string)
            return self._element_to_dict(root)
        except ET.ParseError:
            return None

    def _element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Converts an XML element to a dictionary."""
        result: Dict[str, Any] = {}

        # Handle attributes
        for attr_name, attr_value in element.attrib.items():
            # Strip namespace from attribute name
            attr_name = attr_name.split('}')[-1] if '}' in attr_name else attr_name
            result[f"@{attr_name}"] = attr_value

        # Handle text content
        if element.text and element.text.strip():
            if len(element) == 0 and not element.attrib:
                return element.text.strip()  # type: ignore
            result["#text"] = element.text.strip()

        # Handle child elements
        for child in element:
            child_tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
            child_dict = self._element_to_dict(child)

            if child_tag in result:
                # Convert to list if multiple children with same tag
                if not isinstance(result[child_tag], list):
                    result[child_tag] = [result[child_tag]]
                result[child_tag].append(child_dict)
            else:
                result[child_tag] = child_dict

        return result

    # -------------------------------------------------------------------------
    # Type Conversion
    # -------------------------------------------------------------------------

    def map_sql_type_to_avro_type(
        self,
        column: Dict[str, Any],
        table_name: str,
        table_schema: str,
        type_column: str | None = None,
        type_value: str | None = None
    ) -> JsonNode:
        """Maps a SQL column type to Avro type."""
        data_type = column['data_type'].lower()
        udt_name = (column.get('udt_name') or '').lower()

        # Check for JSON types that need inference
        if self.infer_json_schema and data_type in ('json', 'jsonb'):
            inferred = self.infer_json_column_schema(
                table_name, table_schema, column['column_name'], type_column, type_value
            )
            return inferred

        # Check for XML types that need inference
        if self.infer_xml_schema and data_type == 'xml':
            inferred = self.infer_xml_column_schema(
                table_name, table_schema, column['column_name']
            )
            return inferred

        # Handle ARRAY types
        if data_type == 'array' or (udt_name and udt_name.startswith('_')):
            element_type = udt_name[1:] if udt_name.startswith('_') else 'text'
            type_map = self.get_type_map()
            element_avro_type = type_map.get(element_type, 'string')
            return {"type": "array", "items": element_avro_type}

        # Handle NUMERIC/DECIMAL with precision
        if data_type in ('numeric', 'decimal'):
            precision = column.get('numeric_precision') or 38
            scale = column.get('numeric_scale') or 10
            return {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": precision,
                "scale": scale
            }

        # Handle MySQL bit(n) where n > 1 should be array of booleans
        if self.dialect == 'mysql' and data_type == 'bit':
            # character_maximum_length holds the bit width for MySQL bit type
            bit_width = column.get('character_maximum_length') or 1
            if bit_width > 1:
                return {"type": "array", "items": "boolean"}
            # bit(1) is commonly used as boolean
            return "boolean"

        # Look up in type map
        type_map = self.get_type_map()
        
        # Try exact match first
        if data_type in type_map:
            return copy.deepcopy(type_map[data_type])
        
        # Try udt_name
        if udt_name and udt_name in type_map:
            return copy.deepcopy(type_map[udt_name])

        # Try matching without "USER-DEFINED" and composite types
        if data_type == 'user-defined':
            return "string"  # Default for user-defined types

        # Default fallback
        return "string"

    # -------------------------------------------------------------------------
    # Schema Generation
    # -------------------------------------------------------------------------

    def table_to_avro_schema(
        self,
        table_name: str,
        table_schema: str = 'public'
    ) -> JsonNode:
        """Converts a SQL table to Avro schema."""
        columns = self.fetch_table_columns(table_name, table_schema)
        primary_keys = self.fetch_primary_keys(table_name, table_schema)
        table_comment = self.fetch_table_comment(table_name, table_schema)

        # Check for CloudEvents pattern
        column_names = set(col['column_name'].lower() for col in columns)
        is_cloudevent = False
        type_values: List[str | None] = []
        type_column: str | None = None

        if self.emit_cloudevents:
            is_cloudevent = all(c in column_names for c in ['type', 'source', 'data', 'id'])
            if is_cloudevent:
                type_column = next(
                    (col['column_name'] for col in columns if col['column_name'].lower() == 'type'),
                    None
                )
                if type_column:
                    type_values = self._fetch_distinct_type_values(table_name, table_schema, type_column)

        if not type_values:
            type_values = [None]

        schemas: List[JsonNode] = []

        for type_value in type_values:
            if type_value and isinstance(type_value, str):
                type_name_name = avro_name(type_value.rsplit('.', 1)[-1])
                type_name_namespace = type_value.rsplit('.', 1)[0] if '.' in type_value else ''
                type_namespace = self.avro_namespace + ('.' if self.avro_namespace and type_name_namespace else '') + type_name_namespace
            else:
                type_name_name = avro_name(table_name)
                type_namespace = self.avro_namespace

            if is_cloudevent and type_column:
                # For CloudEvents, focus on the 'data' column
                data_column = next(
                    (col for col in columns if col['column_name'].lower() == 'data'),
                    None
                )
                if data_column:
                    data_schema = self.map_sql_type_to_avro_type(
                        data_column, table_name, table_schema, type_column, type_value
                    )
                    if isinstance(data_schema, dict):
                        data_schema = [data_schema]
                    if isinstance(data_schema, list):
                        for schema in data_schema:
                            if not isinstance(schema, dict) or schema.get("type") != "record":
                                schema = self._wrap_schema_in_root_record(schema, type_name_name, type_namespace)
                            if self.emit_xregistry:
                                ce_attribs: Dict[str, JsonNode] = {}
                                for col in columns:
                                    if col['column_name'].lower() != 'data':
                                        ce_attribs[col['column_name'].lower()] = "string"
                                if isinstance(schema, dict):
                                    schema["ce_attribs"] = ce_attribs
                            self._apply_schema_attributes(schema, table_name, table_schema, type_value, type_namespace, table_comment)
                            schemas.append(schema)
            else:
                # Normal table conversion
                fields: List[JsonNode] = []
                for column in columns:
                    avro_type = self.map_sql_type_to_avro_type(
                        column, table_name, table_schema, type_column, type_value
                    )
                    
                    # Make nullable if column allows NULL
                    if column['is_nullable'] and avro_type != "null":
                        if isinstance(avro_type, list):
                            if "null" not in avro_type:
                                avro_type = ["null"] + avro_type
                        else:
                            avro_type = ["null", avro_type]

                    field: Dict[str, JsonNode] = {
                        "name": avro_name(column['column_name']),
                        "type": avro_type
                    }
                    
                    # Add original name as altname if different
                    if avro_name(column['column_name']) != column['column_name']:
                        field["altnames"] = {"sql": column['column_name']}
                    
                    # Add column comment as doc
                    if column.get('column_comment'):
                        field["doc"] = column['column_comment']
                    
                    fields.append(field)

                schema: Dict[str, JsonNode] = {
                    "type": "record",
                    "name": type_name_name,
                    "fields": fields
                }

                # Add primary keys as 'unique' annotation
                if primary_keys:
                    schema["unique"] = [avro_name(pk) for pk in primary_keys]

                self._apply_schema_attributes(schema, table_name, table_schema, type_value, type_namespace, table_comment)
                schemas.append(schema)

        return schemas if len(schemas) > 1 else schemas[0]

    def _fetch_distinct_type_values(self, table_name: str, table_schema: str, type_column: str) -> List[str]:
        """Fetches distinct values from a type discriminator column."""
        cursor = self.connection.cursor()
        
        if self.dialect == 'postgres':
            query = f"""
                SELECT DISTINCT "{type_column}"
                FROM "{table_schema}"."{table_name}"
                WHERE "{type_column}" IS NOT NULL
                LIMIT 1000
            """
        elif self.dialect == 'mysql':
            query = f"""
                SELECT DISTINCT `{type_column}`
                FROM `{table_schema}`.`{table_name}`
                WHERE `{type_column}` IS NOT NULL
                LIMIT 1000
            """
        else:
            cursor.close()
            return []

        cursor.execute(query)
        values = [row[0] for row in cursor.fetchall() if row[0]]
        cursor.close()
        return values

    def _wrap_schema_in_root_record(self, schema: JsonNode, type_name: str, type_namespace: str) -> Dict[str, JsonNode]:
        """Wraps a schema in a root record."""
        record: Dict[str, JsonNode] = {
            "type": "record",
            "name": type_name,
            "fields": [
                {
                    "name": "data",
                    "type": schema,
                    "root": True
                }
            ]
        }
        if type_namespace:
            record["namespace"] = type_namespace
        return record

    def _apply_schema_attributes(
        self,
        schema: JsonNode,
        table_name: str,
        table_schema: str,
        type_value: str | None,
        type_namespace: str,
        table_comment: str | None
    ):
        """Applies schema attributes to the schema."""
        if isinstance(schema, dict):
            schema["altnames"] = {"sql": f"{table_schema}.{table_name}" if table_schema != 'public' else table_name}
            if self.emit_cloudevents and type_value:
                schema["ce_type"] = type_value
            if type_namespace:
                schema["namespace"] = type_namespace
            if table_comment:
                schema["doc"] = table_comment

    def make_type_names_unique(self, item_types: list):
        """Makes the type names unique (following k2a pattern)."""
        for item in item_types:
            if isinstance(item, dict) and item.get("type") == "array":
                if isinstance(item.get("items"), dict) and item["items"].get("type") == "record":
                    self.make_type_names_unique([item["items"]])
                elif isinstance(item.get("items"), list):
                    self.make_type_names_unique(item["items"])
            if isinstance(item, dict) and item.get("type") == "map":
                if isinstance(item.get("values"), dict) and item["values"].get("type") == "record":
                    self.make_type_names_unique([item["values"]])
                elif isinstance(item.get("values"), list):
                    self.make_type_names_unique(item["values"])
            elif isinstance(item, dict) and item.get("type") == "record":
                namespace = item.get("namespace", '')
                type_name = base_name = item["name"]
                record_name = f"{namespace}.{type_name}" if namespace else type_name
                if record_name in self.generated_types:
                    i = 0
                    while record_name in self.generated_types:
                        i += 1
                        type_name = f"{base_name}{i}"
                        record_name = f"{namespace}.{type_name}" if namespace else type_name
                    self.generated_types.append(record_name)
                else:
                    self.generated_types.append(record_name)
                item["name"] = type_name
                for field in item.get("fields", []):
                    if isinstance(field.get("type"), dict):
                        if field["type"].get("type") in ["record", "array", "map"]:
                            self.make_type_names_unique([field["type"]])
                    elif isinstance(field.get("type"), list):
                        self.make_type_names_unique(field["type"])

    # -------------------------------------------------------------------------
    # Main Processing
    # -------------------------------------------------------------------------

    def process_all_tables(self):
        """Processes all tables in the database and generates Avro schema."""
        union_schema: List[JsonNode] = []
        tables = self.fetch_tables()

        for table_info in tables:
            table_name = table_info['table_name']
            table_schema = table_info['table_schema']
            print(f"Processing table: {table_schema}.{table_name}")

            avro_schema = self.table_to_avro_schema(table_name, table_schema)
            if isinstance(avro_schema, list):
                union_schema.extend(avro_schema)
            else:
                union_schema.append(avro_schema)

        output = None
        if self.emit_xregistry:
            xregistry_messages = {}
            xregistry_schemas = {}
            groupname = self.avro_namespace
            for schema in union_schema:
                self.generated_types = []
                self.make_type_names_unique([schema])
                ce_attribs: Dict[str, JsonNode] = {}
                if isinstance(schema, dict) and "ce_attribs" in schema:
                    ce_attribs = cast(Dict[str, JsonNode], schema.get("ce_attribs", {}))
                    del schema["ce_attribs"]
                if isinstance(schema, dict):
                    schemaid = schema.get('ce_type', f"{self.avro_namespace}.{schema['name']}")
                    schema_name = str(schemaid).rsplit('.', 1)[-1]
                    xregistry_schemas[schemaid] = {
                        "id": schemaid,
                        "name": schema_name,
                        "format": f"Avro/{AVRO_VERSION}",
                        "defaultversionid": "1",
                        "versions": {
                            "1": {
                                "id": "1",
                                "format": f"Avro/{AVRO_VERSION}",
                                "schema": schema
                            }
                        }
                    }
                    xregistry_messages[schemaid] = {
                        "id": schemaid,
                        "name": schema_name,
                        "envelope": "CloudEvents/1.0",
                        "envelopemetadata": {
                            "type": {"value": schemaid},
                            "source": {"value": "{source}"},
                        },
                        "schemaformat": f"Avro/{AVRO_VERSION}",
                        "schemauri": f"#/schemagroups/{groupname}/schemas/{schemaid}"
                    }
                    for key, value in ce_attribs.items():
                        if key in ("type", "source", "id", "specversion"):
                            continue
                        xregistry_messages[schemaid]["envelopemetadata"][key] = {
                            "type": value,
                            "required": True
                        }
            output = {
                "messagegroups": {
                    groupname: {
                        "id": groupname,
                        "messages": xregistry_messages
                    }
                },
                "schemagroups": {
                    groupname: {
                        "id": groupname,
                        "schemas": xregistry_schemas
                    }
                }
            }
        else:
            self.generated_types = []
            self.make_type_names_unique(union_schema)
            output = union_schema if len(union_schema) > 1 else union_schema[0] if union_schema else []

        # Create the output directory if needed
        base_dir = os.path.dirname(self.avro_schema_path)
        if base_dir and not os.path.exists(base_dir):
            os.makedirs(base_dir)

        with open(self.avro_schema_path, 'w', encoding='utf-8') as avro_file:
            json.dump(output, avro_file, indent=4)

        self.close()


def convert_sql_to_avro(
    connection_string: str,
    avro_schema_file: str,
    dialect: str = 'postgres',
    database: str | None = None,
    table_name: str | None = None,
    avro_namespace: str | None = None,
    emit_cloudevents: bool = False,
    emit_cloudevents_xregistry: bool = False,
    sample_size: int = 100,
    infer_json_schema: bool = True,
    infer_xml_schema: bool = True
):
    """Converts SQL database schemas to Avro schema format.

    Args:
        connection_string: Database connection string
        avro_schema_file: Output path for the Avro schema file
        dialect: SQL dialect (postgres, mysql, sqlserver, oracle, sqlite)
        database: Database name (overrides connection string if provided)
        table_name: Specific table to convert (None for all tables)
        avro_namespace: Namespace for generated Avro schemas
        emit_cloudevents: Whether to emit CloudEvents declarations
        emit_cloudevents_xregistry: Whether to emit xRegistry manifest format
        sample_size: Number of rows to sample for JSON/XML inference
        infer_json_schema: Whether to infer schema for JSON columns
        infer_xml_schema: Whether to infer schema for XML columns
    """
    if not connection_string:
        raise ValueError("connection_string is required")

    if not avro_namespace:
        avro_namespace = database or 'database'

    converter = SqlToAvro(
        connection_string=connection_string,
        database=database,
        table_name=table_name,
        avro_namespace=avro_namespace,
        avro_schema_path=avro_schema_file,
        dialect=dialect,
        emit_cloudevents=emit_cloudevents,
        emit_cloudevents_xregistry=emit_cloudevents_xregistry,
        sample_size=sample_size,
        infer_json_schema=infer_json_schema,
        infer_xml_schema=infer_xml_schema
    )

    return converter.process_all_tables()
