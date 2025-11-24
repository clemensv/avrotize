""" 
Convert JSON Structure schema to SQL schema for various databases.
"""

import json
import sys
import os
from typing import Dict, List, Optional, Any, cast, Set

from avrotize.common import altname

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | float | None


class SchemaConverter:
    """Helper class to manage schema conversion with support for $ref, $extends, and definitions"""
    
    def __init__(self, schema_list: List[Dict[str, JsonNode]]):
        self.schema_list = schema_list if isinstance(schema_list, list) else [schema_list]
        self.schema_registry: Dict[str, Dict] = {}
        self.definitions: Dict[str, Dict] = {}
        self.generated_tables: Set[str] = set()
        
        # Register all schemas with $id
        for schema in self.schema_list:
            self.register_schema_ids(schema)
            # Extract definitions from schema
            if isinstance(schema, dict) and 'definitions' in schema:
                self.definitions.update(schema['definitions'])
    
    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """Recursively registers schemas with $id keywords"""
        if not isinstance(schema, dict):
            return
        
        if '$id' in schema:
            schema_id = schema['$id']
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                from urllib.parse import urljoin
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            base_uri = schema_id
        
        # Register definitions
        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)
        
        # Register properties
        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)
    
    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition"""
        # Check if it's an absolute URI reference
        if not ref.startswith('#/'):
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None
        
        # Handle fragment-only references (internal to document)
        path = ref[2:].split('/')
        schema = context_schema if context_schema else (self.schema_list[0] if self.schema_list else {})
        
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        
        return schema
    
    def get_all_properties(self, schema: Dict) -> Dict[str, Any]:
        """Gets all properties including inherited ones from $extends"""
        properties = {}
        
        # First, recursively get base class properties if $extends exists
        if '$extends' in schema:
            base_ref = schema['$extends']
            base_schema = self.resolve_ref(base_ref, schema)
            if base_schema:
                # Recursively get base properties
                properties.update(self.get_all_properties(base_schema))
        
        # Then add/override with current schema properties
        if 'properties' in schema:
            properties.update(schema['properties'])
        
        return properties
    
    def get_all_required(self, schema: Dict) -> List[str]:
        """Gets all required properties including inherited ones from $extends"""
        required = []
        
        # First, recursively get base class required if $extends exists
        if '$extends' in schema:
            base_ref = schema['$extends']
            base_schema = self.resolve_ref(base_ref, schema)
            if base_schema:
                required.extend(self.get_all_required(base_schema))
        
        # Then add current schema required
        if 'required' in schema:
            schema_required = schema['required']
            if isinstance(schema_required, list):
                required.extend(schema_required)
        
        return list(set(required))  # Remove duplicates
    
    def should_generate_table(self, schema: Dict) -> bool:
        """Determines if a table should be generated for this schema"""
        # Only generate tables for object and tuple types
        schema_type = schema.get('type')
        if schema_type not in ['object', 'tuple']:
            return False
        
        # Don't generate tables for abstract types (they're only for inheritance)
        if schema.get('abstract', False):
            return False
        
        return True


def convert_structure_to_sql(structure_schema_path: str, dbscript_file_path: str, db_dialect: str, 
                            emit_cloudevents_columns: bool = False, schema_name: str = ''):
    """
    Converts a JSON Structure schema to database schema for the specified DB dialect.

    Args:
        structure_schema_path (str): Path to the JSON Structure schema file.
        dbscript_file_path (str): Path to the output SQL file.
        db_dialect (str): SQL/DB dialect. Supported: 'sqlserver', 'postgres', 'mysql', 'mariadb', 
                          'sqlite', 'oracle', 'db2', 'sqlanywhere', 'bigquery', 'snowflake', 
                          'redshift', 'cassandra', 'mongodb', 'dynamodb', 'elasticsearch', 
                          'couchdb', 'neo4j', 'firebase', 'cosmosdb', 'hbase'.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.
        schema_name (str): Schema name (optional).

    Raises:
        ValueError: If the SQL dialect is unsupported.
    """
    if db_dialect not in ["sqlserver", "postgres", "mysql", "mariadb", "sqlite", "oracle", "db2",
                          "sqlanywhere", "bigquery", "snowflake", "redshift", "cassandra", "mongodb",
                          "dynamodb", "elasticsearch", "couchdb", "neo4j", "firebase", "cosmosdb", "hbase"]:
        print(f"Unsupported SQL dialect: {db_dialect}")
        sys.exit(1)

    if not structure_schema_path:
        print("Please specify the structure schema file")
        sys.exit(1)
        
    with open(structure_schema_path, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)
    if not isinstance(schema_list, list):
        schema_list = [schema_list]
    
    # Create converter with support for $ref and $extends
    converter = SchemaConverter(schema_list)
    
    tables_sql = []
    # Process all schemas and generate tables only for object/tuple types
    for schema in schema_list:
        if not isinstance(schema, dict):
            continue
        
        # Only generate tables for object and tuple types (not abstract)
        if converter.should_generate_table(schema):
            tables_sql.extend(generate_sql(
                schema, db_dialect, emit_cloudevents_columns, converter, schema_name))
        
        # Also process definitions if present
        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict) and converter.should_generate_table(def_schema):
                    # Add name if not present
                    if 'name' not in def_schema:
                        def_schema['name'] = def_name
                    tables_sql.extend(generate_sql(
                        def_schema, db_dialect, emit_cloudevents_columns, converter, schema_name))
    
    with open(dbscript_file_path, "w", encoding="utf-8") as sql_file:
        sql_file.write("\n".join(tables_sql))


def generate_sql(
        schema: Dict[str, JsonNode],
        sql_dialect: str,
        emit_cloudevents_columns: bool,
        converter: SchemaConverter,
        schema_name: str = '') -> List[str]:
    """
    Generates SQL schema statements for the given JSON Structure schema.

    Args:
        schema (dict): JSON Structure schema.
        sql_dialect (str): SQL dialect.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.
        converter (SchemaConverter): Schema converter with $ref/$extends support.
        schema_name (str): Schema name (optional).

    Returns:
        list: List of SQL statements.
    """
    if sql_dialect in ["sqlserver", "postgres", "mysql", "mariadb", "sqlite", "oracle", "db2", "sqlanywhere",
                       "bigquery", "snowflake", "redshift"]:
        return generate_relational_sql(schema, sql_dialect, emit_cloudevents_columns, converter, schema_name)
    elif sql_dialect == "cassandra":
        return generate_cassandra_schema(schema, emit_cloudevents_columns, converter, schema_name)
    else:
        raise ValueError(f"Unsupported SQL dialect: {sql_dialect}")


def generate_relational_sql(
        schema: Dict[str, JsonNode],
        sql_dialect: str,
        emit_cloudevents_columns: bool,
        converter: SchemaConverter,
        schema_name: str = '') -> List[str]:
    """
    Generates relational SQL schema statements for the given JSON Structure schema.
    Handles inheritance from $extends by including all properties from base types.
    """
    namespace = str(schema.get("namespace", "")).replace('.', '_')
    plain_table_name = altname(schema, 'sql') or f"{namespace}_{schema.get('name', 'table')}"
    table_name = escape_name(plain_table_name, sql_dialect)
    
    # Get all properties including inherited ones
    properties: Dict[str, Any] = converter.get_all_properties(schema)
    required_props: List[str] = converter.get_all_required(schema)

    table_comments = generate_table_comments_json(schema)
    column_comments = generate_column_comments_json(properties, converter.schema_list)

    sql = []
    
    # Add comment about inheritance if $extends is present
    if '$extends' in schema:
        base_ref = schema['$extends']
        sql.append(f"-- Inherits from: {base_ref}")
    
    sql.append(f"CREATE TABLE {table_name} (")
    
    for prop_name, prop_schema in properties.items():
        column_name = escape_name(altname(prop_schema, 'sql') if isinstance(prop_schema, dict) and 'sql' in prop_schema.get('$altnames', {}) else prop_name, sql_dialect)
        column_type = structure_type_to_sql_type(prop_schema, sql_dialect)
        column_definition = f"{column_name} {column_type}"
        
        if isinstance(prop_schema, dict) and prop_schema.get("unique", False):
            column_definition += f" {unique_clause(sql_dialect)}"
        if sql_dialect == "mysql" and prop_name in column_comments:
            cmt = column_comments[str(prop_name)].replace("'", "''")
            column_definition += f" COMMENT '{cmt}'"
        sql.append(f"    {column_definition},")

    if emit_cloudevents_columns:
        sql.extend([
            f"    {escape_name('___type', sql_dialect)} {structure_type_to_sql_type('string', sql_dialect)} NOT NULL,",
            f"    {escape_name('___source', sql_dialect)} {structure_type_to_sql_type('string', sql_dialect)} NOT NULL,",
            f"    {escape_name('___id', sql_dialect)} {structure_type_to_sql_type('string', sql_dialect)} NOT NULL,",
            f"    {escape_name('___time', sql_dialect)} {structure_type_to_sql_type('datetime', sql_dialect)} NULL,",
            f"    {escape_name('___subject', sql_dialect)} {structure_type_to_sql_type('string', sql_dialect)} NULL,"
        ])

    # Handle unique keys from schema
    unique_keys = []
    for prop_name in properties.keys():
        if prop_name in required_props:
            unique_keys.append(prop_name)
    
    if unique_keys:
        unique_column_names = []
        if sql_dialect in ["mysql", "mariadb"]:
            for prop_name in unique_keys:
                prop_schema = properties[prop_name]
                column_type = structure_type_to_sql_type(prop_schema, sql_dialect)
                col_name = altname(prop_schema, 'sql') if isinstance(prop_schema, dict) and 'sql' in prop_schema.get('$altnames', {}) else prop_name
                if column_type in ["BLOB", "TEXT"]:
                    unique_column_names.append(escape_name(col_name + "(20)", sql_dialect))
                else:
                    unique_column_names.append(escape_name(col_name, sql_dialect))
        else:
            unique_column_names = [escape_name(
                altname(properties[prop_name], 'sql') if isinstance(properties[prop_name], dict) and 'sql' in properties[prop_name].get('$altnames', {}) else prop_name, 
                sql_dialect) for prop_name in unique_keys]
        
        sql.append(f"    {primary_key_clause(sql_dialect)} ({', '.join(unique_column_names)})")
    else:
        sql[-1] = sql[-1][:-1]  # Remove the last comma
    sql.append(");")
    sql.append("")

    if sql_dialect != "mysql":
        sql.extend(generate_table_comment_sql(sql_dialect, table_comments, plain_table_name))
        sql.extend(generate_column_comment_sql(sql_dialect, column_comments, plain_table_name))

    return sql


def structure_type_to_sql_type(structure_type: Any, dialect: str) -> str:
    """
    Maps a JSON Structure type to a SQL type for the specified dialect.
    """
    type_map = {
        "sqlserver": {
            "null": "NULL",
            "boolean": "BIT",
            "string": "NVARCHAR(512)",
            "int8": "TINYINT",
            "uint8": "TINYINT",
            "int16": "SMALLINT",
            "uint16": "SMALLINT",
            "int32": "INT",
            "uint32": "INT",
            "int64": "BIGINT",
            "uint64": "BIGINT",
            "int128": "DECIMAL(38,0)",
            "uint128": "DECIMAL(38,0)",
            "integer": "INT",
            "float8": "REAL",
            "float": "FLOAT",
            "double": "FLOAT",
            "number": "FLOAT",
            "decimal": "DECIMAL(18,6)",
            "binary": "VARBINARY(MAX)",
            "date": "DATE",
            "time": "TIME",
            "datetime": "DATETIME2",
            "timestamp": "DATETIME2",
            "duration": "BIGINT",
            "uuid": "UNIQUEIDENTIFIER",
            "uri": "NVARCHAR(2048)",
            "jsonpointer": "NVARCHAR(512)",
            "any": "NVARCHAR(MAX)",
            "array": "NVARCHAR(MAX)",
            "set": "NVARCHAR(MAX)",
            "map": "NVARCHAR(MAX)",
            "object": "NVARCHAR(MAX)",
            "choice": "NVARCHAR(MAX)",
            "tuple": "NVARCHAR(MAX)"
        },
        "postgres": {
            "null": "NULL",
            "boolean": "BOOLEAN",
            "string": "VARCHAR(512)",
            "int8": "SMALLINT",
            "uint8": "SMALLINT",
            "int16": "SMALLINT",
            "uint16": "INTEGER",
            "int32": "INTEGER",
            "uint32": "BIGINT",
            "int64": "BIGINT",
            "uint64": "NUMERIC(20)",
            "int128": "NUMERIC(39)",
            "uint128": "NUMERIC(39)",
            "integer": "INTEGER",
            "float8": "REAL",
            "float": "REAL",
            "double": "DOUBLE PRECISION",
            "number": "DOUBLE PRECISION",
            "decimal": "NUMERIC(18,6)",
            "binary": "BYTEA",
            "date": "DATE",
            "time": "TIME",
            "datetime": "TIMESTAMP",
            "timestamp": "TIMESTAMP",
            "duration": "INTERVAL",
            "uuid": "UUID",
            "uri": "VARCHAR(2048)",
            "jsonpointer": "VARCHAR(512)",
            "any": "JSONB",
            "array": "JSONB",
            "set": "JSONB",
            "map": "JSONB",
            "object": "JSONB",
            "choice": "JSONB",
            "tuple": "JSONB"
        },
        "mysql": {
            "null": "NULL",
            "boolean": "BOOLEAN",
            "string": "VARCHAR(512)",
            "int8": "TINYINT",
            "uint8": "TINYINT UNSIGNED",
            "int16": "SMALLINT",
            "uint16": "SMALLINT UNSIGNED",
            "int32": "INT",
            "uint32": "INT UNSIGNED",
            "int64": "BIGINT",
            "uint64": "BIGINT UNSIGNED",
            "int128": "DECIMAL(38,0)",
            "uint128": "DECIMAL(38,0)",
            "integer": "INT",
            "float8": "FLOAT",
            "float": "FLOAT",
            "double": "DOUBLE",
            "number": "DOUBLE",
            "decimal": "DECIMAL(18,6)",
            "binary": "BLOB",
            "date": "DATE",
            "time": "TIME",
            "datetime": "DATETIME",
            "timestamp": "TIMESTAMP",
            "duration": "BIGINT",
            "uuid": "CHAR(36)",
            "uri": "VARCHAR(2048)",
            "jsonpointer": "VARCHAR(512)",
            "any": "JSON",
            "array": "JSON",
            "set": "JSON",
            "map": "JSON",
            "object": "JSON",
            "choice": "JSON",
            "tuple": "JSON"
        }
    }
    
    # Handle mariadb, sqlite, oracle, etc. by copying mysql/postgres patterns
    type_map["mariadb"] = type_map["mysql"].copy()
    type_map["sqlite"] = {k: v.replace("JSON", "TEXT").replace("BYTEA", "BLOB").replace("BIGINT", "INTEGER") for k, v in type_map["postgres"].items()}
    type_map["oracle"] = {k: v.replace("VARCHAR", "VARCHAR2").replace("JSONB", "CLOB").replace("BYTEA", "BLOB") for k, v in type_map["postgres"].items()}
    type_map["db2"] = type_map["postgres"].copy()
    type_map["sqlanywhere"] = type_map["sqlserver"].copy()
    type_map["bigquery"] = {k: v.replace("VARCHAR", "STRING").replace("JSONB", "STRING").replace("BYTEA", "BYTES") for k, v in type_map["postgres"].items()}
    type_map["snowflake"] = {k: v.replace("JSONB", "VARIANT").replace("BYTEA", "BINARY") for k, v in type_map["postgres"].items()}
    type_map["redshift"] = type_map["postgres"].copy()

    # Handle type resolution
    if isinstance(structure_type, str):
        return type_map.get(dialect, type_map["postgres"]).get(structure_type, type_map[dialect]["string"])
    
    if isinstance(structure_type, list):
        # Union type - filter out null
        non_null_types = [t for t in structure_type if t != "null"]
        if len(non_null_types) == 1:
            return structure_type_to_sql_type(non_null_types[0], dialect)
        return type_map[dialect]["any"]
    
    if isinstance(structure_type, dict):
        struct_type = structure_type.get("type", "string")
        if struct_type in ["array", "set", "map", "object", "choice", "tuple"]:
            return type_map[dialect][struct_type]
        return structure_type_to_sql_type(struct_type, dialect)
    
    return type_map.get(dialect, type_map["postgres"])["string"]


def generate_cassandra_schema(schema: Dict[str, JsonNode], emit_cloudevents_columns: bool, converter: SchemaConverter, schema_name: str) -> List[str]:
    """
    Generates Cassandra schema statements for the given JSON Structure schema.
    Handles inheritance from $extends by including all properties from base types.
    """
    namespace = cast(str, schema.get("namespace", "")).replace(".", "_")
    table_name = altname(schema, 'sql') or schema.get("name", "table")
    table_name = escape_name(
        f"{namespace}_{table_name}" if namespace else table_name, "cassandra")
    if schema_name:
        table_name = f"{schema_name}.{table_name}"
    
    # Get all properties including inherited ones
    properties: Dict[str, Any] = converter.get_all_properties(schema)
    required_props: List[str] = converter.get_all_required(schema)

    cql = []
    
    # Add comment about inheritance if $extends is present
    if '$extends' in schema:
        base_ref = schema['$extends']
        cql.append(f"-- Inherits from: {base_ref}")
    
    cql.append(f"CREATE TABLE {table_name} (")
    for prop_name, prop_schema in properties.items():
        column_name = escape_name(
            altname(prop_schema, 'sql') if isinstance(prop_schema, dict) and 'sql' in prop_schema.get('$altnames', {}) else prop_name, 
            'cassandra')
        column_type = structure_type_to_cassandra_type(prop_schema)
        cql.append(f"    {column_name} {column_type},")
    
    if emit_cloudevents_columns:
        cql.extend([
            f"    {escape_name('cloudevents_type', 'cassandra')} text,",
            f"    {escape_name('cloudevents_source', 'cassandra')} text,",
            f"    {escape_name('cloudevents_id', 'cassandra')} text,",
            f"    {escape_name('cloudevents_time', 'cassandra')} timestamp,",
            f"    {escape_name('cloudevents_subject', 'cassandra')} text,"
        ])
    
    # Determine primary key
    primary_keys = [prop_name for prop_name in required_props if prop_name in properties]
    if primary_keys:
        pk_columns = [escape_name(
            altname(properties[prop_name], 'sql') if isinstance(properties[prop_name], dict) and 'sql' in properties[prop_name].get('$altnames', {}) else prop_name, 
            "cassandra") for prop_name in primary_keys]
        cql.append(f"    PRIMARY KEY ({', '.join(pk_columns)})")
    elif emit_cloudevents_columns:
        cql.append(f"    PRIMARY KEY ({escape_name('cloudevents_id', 'cassandra')})")
    else:
        # Use first column as primary key
        if properties:
            first_prop = list(properties.keys())[0]
            cql.append(f"    PRIMARY KEY ({escape_name(first_prop, 'cassandra')})")
    
    cql.append(");")
    return cql


def structure_type_to_cassandra_type(structure_type: Any) -> str:
    """
    Converts a JSON Structure type to Cassandra type.
    """
    type_map = {
        "null": "text",
        "boolean": "boolean",
        "string": "text",
        "int8": "tinyint",
        "uint8": "tinyint",
        "int16": "smallint",
        "uint16": "smallint",
        "int32": "int",
        "uint32": "int",
        "int64": "bigint",
        "uint64": "bigint",
        "int128": "varint",
        "uint128": "varint",
        "integer": "int",
        "float8": "float",
        "float": "float",
        "double": "double",
        "number": "double",
        "decimal": "decimal",
        "binary": "blob",
        "date": "date",
        "time": "time",
        "datetime": "timestamp",
        "timestamp": "timestamp",
        "duration": "bigint",
        "uuid": "uuid",
        "uri": "text",
        "jsonpointer": "text",
        "any": "text",
        "array": "text",
        "set": "text",
        "map": "text",
        "object": "text",
        "choice": "text",
        "tuple": "text"
    }
    
    if isinstance(structure_type, str):
        return type_map.get(structure_type, "text")
    
    if isinstance(structure_type, list):
        non_null_types = [t for t in structure_type if t != "null"]
        if len(non_null_types) == 1:
            return structure_type_to_cassandra_type(non_null_types[0])
        return "text"
    
    if isinstance(structure_type, dict):
        return type_map.get(structure_type.get("type", "string"), "text")
    
    return "text"


def escape_name(name: str, dialect: str) -> str:
    """
    Escapes a name (table or column) for the given SQL dialect.
    """
    if dialect in ["sqlserver", "sqlanywhere"]:
        return f"[{name}]"
    elif dialect in ["postgres", "sqlite", "bigquery", "snowflake", "redshift", "cassandra"]:
        return f'"{name}"'
    elif dialect in ["mysql", "mariadb"]:
        return f"`{name}`"
    elif dialect in ["oracle", "db2"]:
        return f'"{name.upper()}"'
    else:
        return name


def unique_clause(dialect: str) -> str:
    """
    Returns the UNIQUE clause for the given SQL dialect.
    """
    return "UNIQUE"


def primary_key_clause(dialect: str) -> str:
    """
    Returns the PRIMARY KEY clause for the given SQL dialect.
    """
    return "PRIMARY KEY"


def generate_table_comments_json(schema: Dict[str, JsonNode]) -> Dict[str, str]:
    """
    Generates table-level comments as JSON.
    """
    comments = {}
    if "description" in schema:
        comments["doc"] = str(schema["description"])
    elif "doc" in schema:
        comments["doc"] = str(schema["doc"])
    return comments


def generate_column_comments_json(properties: Dict[str, Any], schema_list: List[Dict[str, JsonNode]]) -> Dict[str, str]:
    """
    Generates column-level comments as JSON.
    """
    comments = {}
    for prop_name, prop_schema in properties.items():
        column_comment = {}
        if isinstance(prop_schema, dict):
            if "description" in prop_schema:
                column_comment["doc"] = prop_schema["description"]
            elif "doc" in prop_schema:
                column_comment["doc"] = prop_schema["doc"]
            
            if "type" in prop_schema and prop_schema["type"] in ["array", "set", "map", "object", "choice", "tuple"]:
                column_comment["schema"] = prop_schema
        
        if column_comment:
            comments[str(prop_name)] = json.dumps(column_comment)
    return comments


def generate_table_comment_sql(dialect: str, table_comments: Dict[str, str], table_name: str) -> List[str]:
    """
    Generates SQL statements for table-level comments.
    """
    comments = []
    if "doc" in table_comments:
        doc_string = table_comments["doc"].replace("'", "''")
        if dialect == "sqlserver":
            comments.append(
                f"EXEC sp_addextendedproperty 'MS_Description', '{doc_string}', 'SCHEMA', 'dbo', 'TABLE', '{table_name}';")
        elif dialect in ["postgres", "oracle"]:
            comments.append(
                f"COMMENT ON TABLE {escape_name(table_name, dialect)} IS '{doc_string}';")
        elif dialect == "sqlite":
            comments.append(
                f"-- COMMENT ON TABLE {escape_name(table_name, dialect)} IS '{doc_string}';")
    return comments


def generate_column_comment_sql(dialect: str, column_comments: Dict[str, str], table_name: str) -> List[str]:
    """
    Generates SQL statements for column-level comments.
    """
    comments = []
    for column_name, comment in column_comments.items():
        comment_data = json.loads(comment)
        doc = comment_data.get("doc", "")
        doc = doc.replace("'", "''")
        schema = comment_data.get("schema", "")
        if dialect == "sqlserver":
            if doc:
                comments.append(
                    f"EXEC sp_addextendedproperty 'MS_Description', '{doc}', 'SCHEMA', 'dbo', 'TABLE', '{table_name}', 'COLUMN', '{column_name}';")
            if schema:
                comments.append(
                    f"EXEC sp_addextendedproperty 'MS_Schema', '{json.dumps(schema)}', 'SCHEMA', 'dbo', 'TABLE', '{table_name}', 'COLUMN', '{column_name}';")
        else:
            comment = comment.replace("'", "''")
            comments.append(
                f"COMMENT ON COLUMN {escape_name(table_name, dialect)}.{escape_name(column_name, dialect)} IS '{comment}';")
    return comments


def convert_structure_to_nosql(structure_schema_path: str, nosql_file_path: str, nosql_dialect: str, 
                              emit_cloudevents_columns: bool = False):
    """
    Converts a JSON Structure schema to NoSQL schema for the specified NoSQL dialect.
    Only generates schemas for object and tuple types (not abstract types).
    """
    if not structure_schema_path:
        print("Please specify the structure schema file")
        sys.exit(1)
    
    with open(structure_schema_path, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)
    if not isinstance(schema_list, list):
        schema_list = [schema_list]
    
    # Create converter with support for $ref and $extends
    converter = SchemaConverter(schema_list)
    
    dirname = nosql_file_path
    if not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)

    # Process all schemas
    for schema in schema_list:
        if not isinstance(schema, dict):
            continue
        
        # Only generate for object and tuple types (not abstract)
        if converter.should_generate_table(schema):
            model = generate_nosql(schema, nosql_dialect, emit_cloudevents_columns, converter)
            file_name = os.path.join(
                nosql_file_path, get_file_name(schema, get_nosql_file_extension(nosql_dialect)))
            with open(file_name, "w", encoding="utf-8") as nosql_file:
                if isinstance(model, list):
                    nosql_file.write("\n".join(model))
                else:
                    nosql_file.write(model)
        
        # Also process definitions if present
        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict) and converter.should_generate_table(def_schema):
                    # Add name if not present
                    if 'name' not in def_schema:
                        def_schema['name'] = def_name
                    model = generate_nosql(def_schema, nosql_dialect, emit_cloudevents_columns, converter)
                    file_name = os.path.join(
                        nosql_file_path, get_file_name(def_schema, get_nosql_file_extension(nosql_dialect)))
                    with open(file_name, "w", encoding="utf-8") as nosql_file:
                        if isinstance(model, list):
                            nosql_file.write("\n".join(model))
                        else:
                            nosql_file.write(model)


def get_nosql_file_extension(nosql_dialect: str) -> str:
    """
    Returns the file extension for the given NoSQL dialect.
    """
    if nosql_dialect == "neo4j":
        return "cypher"
    else:
        return "json"


def generate_nosql(schema: Dict[str, Any], nosql_dialect: str, emit_cloudevents_columns: bool, 
                  converter: SchemaConverter) -> str:
    """
    Generates NoSQL schema statements for the given JSON Structure schema.
    """
    if nosql_dialect == "mongodb":
        return generate_mongodb_schema(schema, emit_cloudevents_columns, converter)
    elif nosql_dialect == "dynamodb":
        return generate_dynamodb_schema(schema, emit_cloudevents_columns, converter)
    elif nosql_dialect == "elasticsearch":
        return generate_elasticsearch_schema(schema, emit_cloudevents_columns, converter)
    elif nosql_dialect == "couchdb":
        return generate_couchdb_schema(schema, emit_cloudevents_columns, converter)
    elif nosql_dialect == "neo4j":
        return generate_neo4j_schema(schema, emit_cloudevents_columns, converter)
    elif nosql_dialect == "firebase":
        return generate_firebase_schema(schema, emit_cloudevents_columns, converter)
    elif nosql_dialect == "cosmosdb":
        return generate_cosmosdb_schema(schema, emit_cloudevents_columns, converter)
    elif nosql_dialect == "hbase":
        return generate_hbase_schema(schema, emit_cloudevents_columns, converter)
    else:
        raise ValueError(f"Unsupported NoSQL dialect: {nosql_dialect}")


def generate_mongodb_schema(schema: Dict[str, Any], emit_cloudevents_columns: bool, converter: SchemaConverter) -> str:
    """
    Generates MongoDB schema statements for the given JSON Structure schema.
    """
    namespace = schema.get("namespace", "")
    collection_name = altname(schema, 'sql') or f"{namespace}_{schema.get('name', 'collection')}"
    properties = converter.get_all_properties(schema)
    required_props = converter.get_all_required(schema)

    mongodb_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [],
            "properties": {}
        }
    }

    for prop_name, prop_schema in properties.items():
        column_type = structure_type_to_mongodb_type(prop_schema)
        mongodb_schema["$jsonSchema"]["properties"][prop_name] = column_type
        if prop_name in required_props:
            mongodb_schema["$jsonSchema"]["required"].append(prop_name)
        if isinstance(prop_schema, dict):
            if prop_schema.get("description"):
                mongodb_schema["$jsonSchema"]["properties"][prop_name]["description"] = prop_schema["description"]

    if emit_cloudevents_columns:
        mongodb_schema["$jsonSchema"]["properties"].update({
            "___type": {"bsonType": "string"},
            "___source": {"bsonType": "string"},
            "___id": {"bsonType": "string"},
            "___time": {"bsonType": "date"},
            "___subject": {"bsonType": "string"}
        })
        mongodb_schema["$jsonSchema"]["required"].extend(["___type", "___source", "___id"])

    return json.dumps({collection_name: mongodb_schema}, indent=4)


def structure_type_to_mongodb_type(structure_type: Any) -> Dict[str, str]:
    """
    Converts a JSON Structure type to MongoDB type.
    """
    type_map = {
        "null": {"bsonType": "null"},
        "boolean": {"bsonType": "bool"},
        "string": {"bsonType": "string"},
        "int8": {"bsonType": "int"},
        "uint8": {"bsonType": "int"},
        "int16": {"bsonType": "int"},
        "uint16": {"bsonType": "int"},
        "int32": {"bsonType": "int"},
        "uint32": {"bsonType": "long"},
        "int64": {"bsonType": "long"},
        "uint64": {"bsonType": "long"},
        "int128": {"bsonType": "decimal"},
        "uint128": {"bsonType": "decimal"},
        "integer": {"bsonType": "int"},
        "float": {"bsonType": "double"},
        "double": {"bsonType": "double"},
        "number": {"bsonType": "double"},
        "decimal": {"bsonType": "decimal"},
        "binary": {"bsonType": "binData"},
        "date": {"bsonType": "date"},
        "datetime": {"bsonType": "date"},
        "timestamp": {"bsonType": "date"},
        "uuid": {"bsonType": "string"},
        "uri": {"bsonType": "string"},
        "array": {"bsonType": "array"},
        "map": {"bsonType": "object"},
        "object": {"bsonType": "object"},
        "any": {"bsonType": "object"}
    }
    
    if isinstance(structure_type, str):
        return type_map.get(structure_type, {"bsonType": "string"})
    
    if isinstance(structure_type, list):
        non_null_types = [t for t in structure_type if t != "null"]
        if len(non_null_types) == 1:
            return structure_type_to_mongodb_type(non_null_types[0])
        return {"bsonType": "object"}
    
    if isinstance(structure_type, dict):
        return type_map.get(structure_type.get("type", "string"), {"bsonType": "string"})
    
    return {"bsonType": "string"}


def generate_dynamodb_schema(schema: Dict[str, Any], emit_cloudevents_columns: bool, converter: SchemaConverter) -> str:
    """
    Generates DynamoDB schema statements for the given JSON Structure schema.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    table_name = altname(schema, 'sql') or f"{namespace}_{schema.get('name', 'table')}"
    properties = converter.get_all_properties(schema)

    dynamodb_schema = {
        "TableName": table_name,
        "KeySchema": [],
        "AttributeDefinitions": [],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5
        }
    }

    # Add first property as hash key
    if properties:
        first_prop = list(properties.keys())[0]
        first_prop_schema = properties[first_prop]
        attr_type = structure_type_to_dynamodb_type(first_prop_schema)
        dynamodb_schema["AttributeDefinitions"].append(
            {"AttributeName": first_prop, "AttributeType": attr_type})
        dynamodb_schema["KeySchema"].append(
            {"AttributeName": first_prop, "KeyType": "HASH"})

    if emit_cloudevents_columns:
        dynamodb_schema["AttributeDefinitions"].append(
            {"AttributeName": "___id", "AttributeType": "S"})
        if not dynamodb_schema["KeySchema"]:
            dynamodb_schema["KeySchema"].append(
                {"AttributeName": "___id", "KeyType": "HASH"})

    return json.dumps(dynamodb_schema, indent=4)


def structure_type_to_dynamodb_type(structure_type: Any) -> str:
    """
    Converts a JSON Structure type to DynamoDB type.
    """
    type_map = {
        "boolean": "N",
        "string": "S",
        "int8": "N", "uint8": "N",
        "int16": "N", "uint16": "N",
        "int32": "N", "uint32": "N",
        "int64": "N", "uint64": "N",
        "integer": "N",
        "float": "N", "double": "N", "number": "N",
        "binary": "B",
        "uuid": "S"
    }
    
    if isinstance(structure_type, str):
        return type_map.get(structure_type, "S")
    
    if isinstance(structure_type, dict):
        return type_map.get(structure_type.get("type", "string"), "S")
    
    return "S"


def generate_elasticsearch_schema(schema: Dict[str, Any], emit_cloudevents_columns: bool, converter: SchemaConverter) -> str:
    """
    Generates Elasticsearch schema.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    index_name = altname(schema, 'sql') or f"{namespace}_{schema.get('name', 'index')}"
    properties = converter.get_all_properties(schema)

    es_mapping = {
        "mappings": {
            "properties": {}
        }
    }

    for prop_name, prop_schema in properties.items():
        es_mapping["mappings"]["properties"][prop_name] = structure_type_to_elasticsearch_type(prop_schema)

    if emit_cloudevents_columns:
        es_mapping["mappings"]["properties"].update({
            "___type": {"type": "keyword"},
            "___source": {"type": "keyword"},
            "___id": {"type": "keyword"},
            "___time": {"type": "date"},
            "___subject": {"type": "keyword"}
        })

    return json.dumps({index_name: es_mapping}, indent=4)


def structure_type_to_elasticsearch_type(structure_type: Any) -> Dict[str, str]:
    """
    Converts a JSON Structure type to Elasticsearch type.
    """
    type_map = {
        "boolean": {"type": "boolean"},
        "string": {"type": "text"},
        "int8": {"type": "byte"}, "uint8": {"type": "byte"},
        "int16": {"type": "short"}, "uint16": {"type": "short"},
        "int32": {"type": "integer"}, "uint32": {"type": "integer"},
        "int64": {"type": "long"}, "uint64": {"type": "long"},
        "integer": {"type": "integer"},
        "float": {"type": "float"}, "double": {"type": "double"},
        "number": {"type": "double"},
        "binary": {"type": "binary"},
        "date": {"type": "date"},
        "datetime": {"type": "date"},
        "uuid": {"type": "keyword"}
    }
    
    if isinstance(structure_type, str):
        return type_map.get(structure_type, {"type": "text"})
    
    if isinstance(structure_type, dict):
        return type_map.get(structure_type.get("type", "string"), {"type": "text"})
    
    return {"type": "text"}


def generate_couchdb_schema(schema: Dict[str, Any], emit_cloudevents_columns: bool, converter: SchemaConverter) -> str:
    """
    Generates CouchDB schema.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    db_name = altname(schema, 'sql') or f"{namespace}_{schema.get('name', 'db')}"
    properties = converter.get_all_properties(schema)

    couchdb_schema = {
        "type": "object",
        "properties": {}
    }

    for prop_name, prop_schema in properties.items():
        couchdb_schema["properties"][prop_name] = {"type": "string"}

    if emit_cloudevents_columns:
        couchdb_schema["properties"].update({
            "___type": {"type": "string"},
            "___source": {"type": "string"},
            "___id": {"type": "string"},
            "___time": {"type": "string"},
            "___subject": {"type": "string"}
        })

    return json.dumps({db_name: couchdb_schema}, indent=4)


def generate_neo4j_schema(schema: Dict[str, Any], emit_cloudevents_columns: bool, converter: SchemaConverter) -> str:
    """
    Generates Neo4j schema.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    label_name = altname(schema, 'sql') or f"{namespace}_{schema.get('name', 'node')}"
    properties = converter.get_all_properties(schema)

    cypher = []
    cypher.append(f"CREATE (:{label_name} {{")
    
    prop_lines = []
    for prop_name in properties.keys():
        prop_lines.append(f"    {prop_name}: 'value'")
    
    if emit_cloudevents_columns:
        prop_lines.extend([
            "    ___type: 'value'",
            "    ___source: 'value'",
            "    ___id: 'value'",
            "    ___time: 'value'",
            "    ___subject: 'value'"
        ])
    
    cypher.append(",\n".join(prop_lines))
    cypher.append("});")
    return "\n".join(cypher)


def generate_firebase_schema(schema: Dict[str, Any], emit_cloudevents_columns: bool, converter: SchemaConverter) -> str:
    """
    Generates Firebase schema.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    collection_name = altname(schema, 'sql') or f"{namespace}_{schema.get('name', 'collection')}"
    properties = converter.get_all_properties(schema)

    firebase_schema = {
        "fields": {}
    }

    for prop_name in properties.keys():
        firebase_schema["fields"][prop_name] = {"type": "string"}

    if emit_cloudevents_columns:
        firebase_schema["fields"].update({
            "___type": {"type": "string"},
            "___source": {"type": "string"},
            "___id": {"type": "string"},
            "___time": {"type": "timestamp"},
            "___subject": {"type": "string"}
        })

    return json.dumps({collection_name: firebase_schema}, indent=4)


def generate_cosmosdb_schema(schema: Dict[str, Any], emit_cloudevents_columns: bool, converter: SchemaConverter) -> str:
    """
    Generates CosmosDB schema.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    collection_name = altname(schema, 'sql') or f"{namespace}_{schema.get('name', 'collection')}"
    properties = converter.get_all_properties(schema)

    cosmosdb_schema = {
        "id": collection_name,
        "partitionKey": {
            "paths": [],
            "kind": "Hash"
        },
        "fields": {}
    }

    for prop_name in properties.keys():
        cosmosdb_schema["fields"][prop_name] = {"type": "string"}
        cosmosdb_schema["partitionKey"]["paths"].append(f"/{prop_name}")

    if emit_cloudevents_columns:
        cosmosdb_schema["fields"].update({
            "___type": {"type": "string"},
            "___id": {"type": "string"}
        })
        cosmosdb_schema["partitionKey"]["paths"].append("/___id")

    return json.dumps(cosmosdb_schema, indent=4)


def generate_hbase_schema(schema: Dict[str, Any], emit_cloudevents_columns: bool, converter: SchemaConverter) -> str:
    """
    Generates HBase schema.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    table_name = altname(schema, 'sql') or f"{namespace}_{schema.get('name', 'table')}"
    properties = converter.get_all_properties(schema)

    hbase_schema = {
        "table": table_name,
        "column_families": []
    }

    for prop_name in properties.keys():
        hbase_schema["column_families"].append({
            "name": prop_name,
            "column_family": "string"
        })

    if emit_cloudevents_columns:
        hbase_schema["column_families"].extend([
            {"name": "___type", "column_family": "string"},
            {"name": "___id", "column_family": "string"}
        ])

    return json.dumps(hbase_schema, indent=4)


def get_file_name(schema: Dict[str, Any], extension: str) -> str:
    """
    Generates a file name based on the schema.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    name = schema.get("name", "schema")
    return (namespace + '.' + name + '.' + extension) if namespace else (name + '.' + extension)
