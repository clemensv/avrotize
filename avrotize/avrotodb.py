""" 
Convert Avro schema to SQL schema for various databases.
"""

import json
import sys
import os
from typing import Dict, List, cast
from avrotize.common import altname, camel

JsonNode = Dict[str,
                'JsonNode'] | List['JsonNode'] | str | bool | int | float | None


def convert_avro_to_sql(avro_schema_path, dbscript_file_path, db_dialect, emit_cloudevents_columns=False, schema_name: str = ''):
    """
    Converts an Avro schema to database schema for the specified DB dialect.

    Args:
        avro_schema_path (str): Path to the Avro schema file.
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

    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list):
        tables_sql = []
        for schema in schema_list:
            tables_sql.extend(generate_sql(
                schema, db_dialect, emit_cloudevents_columns, schema_list, schema_name))
        with open(dbscript_file_path, "w", encoding="utf-8") as sql_file:
            sql_file.write("\n".join(tables_sql))
    else:
        tables_sql = generate_sql(
            schema, db_dialect, emit_cloudevents_columns, schema_list, schema_name)
        with open(dbscript_file_path, "w", encoding="utf-8") as sql_file:
            sql_file.write("\n".join(tables_sql))


def generate_sql(
        schema: Dict[str, JsonNode],
        sql_dialect: str,
        emit_cloudevents_columns: bool,
        schema_list: List[Dict[str, JsonNode]],
        schema_name: str = '') -> List[str]:
    """
    Generates SQL schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        sql_dialect (str): SQL dialect.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.
        schema_list (list): List of all schemas.
        schema_name (str): Schema name (optional).

    Returns:
        list: List of SQL statements.
    """
    if sql_dialect in ["sqlserver", "postgres", "mysql", "mariadb", "sqlite", "oracle", "db2", "sqlanywhere",
                       "bigquery", "snowflake", "redshift"]:
        return generate_relational_sql(schema, sql_dialect, emit_cloudevents_columns, schema_list, schema_name)
    elif sql_dialect == "cassandra":
        return generate_cassandra_schema(schema, emit_cloudevents_columns, schema_name)
    else:
        raise ValueError(f"Unsupported SQL dialect: {sql_dialect}")


def generate_relational_sql(
        schema: Dict[str, JsonNode],
        sql_dialect: str,
        emit_cloudevents_columns: bool,
        schema_list: List[Dict[str, JsonNode]],
        schema_name: str = '') -> List[str]:
    """
    Generates relational SQL schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        sql_dialect (str): SQL dialect.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.
        schema_list (list): List of all schemas.
        schema_name (str): Schema name (optional).

    Returns:
        list: List of SQL statements.
    """
    namespace = str(schema.get("namespace", "")).replace('.', '_')
    plain_table_name = altname(
        schema, 'sql') or f"{namespace}_{schema['name']}"
    table_name = escape_name(plain_table_name, sql_dialect)
    fields: List[Dict[str, JsonNode]] = cast(
        List[Dict[str, JsonNode]], schema["fields"])
    unique_record_keys: List[str] = cast(List[str], schema.get("unique", []))

    table_comments = generate_table_comments_json(schema)
    column_comments = generate_column_comments_json(fields, schema_list)

    sql = []
    sql.append(f"CREATE TABLE {table_name} (")
    for field in fields:
        column_name = escape_name(
            altname(field, 'sql') or field["name"], sql_dialect)
        column_type = avro_type_to_sql_type(field["type"], sql_dialect)
        column_definition = f"{column_name} {column_type}"
        if field.get("unique", False):
            column_definition += f" {unique_clause(sql_dialect)}"
        if sql_dialect == "mysql" and field["name"] in column_comments:
            cmt = column_comments[str(field['name'])].replace("'", "''")
            column_definition += f" COMMENT '{cmt}'"
        sql.append(f"    {column_definition},")

    if emit_cloudevents_columns:
        sql.extend([
            f"    {escape_name('___type', sql_dialect)} {avro_type_to_sql_type('string', sql_dialect)} NOT NULL,",
            f"    {escape_name('___source', sql_dialect)} {avro_type_to_sql_type('string', sql_dialect)} NOT NULL,",
            f"    {escape_name('___id', sql_dialect)} {avro_type_to_sql_type('string', sql_dialect)} NOT NULL,",
            f"    {escape_name('___time', sql_dialect)} {avro_type_to_sql_type('timestamp', sql_dialect)} NULL,",
            f"    {escape_name('___subject', sql_dialect)} {avro_type_to_sql_type('string', sql_dialect)} NULL,"
        ])

    if unique_record_keys:
        unique_column_altnames = []
        if sql_dialect in ["mysql", "mariadb"]:
            for field in fields:
                if field["name"] in unique_record_keys:
                    column_type = avro_type_to_sql_type(
                        field["type"], sql_dialect)
                    if column_type in ["BLOB", "TEXT"]:
                        unique_column_altnames.append(escape_name(
                            altname(field, 'sql') + "(20)", sql_dialect))
                    else:
                        unique_column_altnames.append(
                            escape_name(altname(field, 'sql'), sql_dialect))
        else:
            unique_column_altnames = [escape_name(altname(
                field, 'sql'), sql_dialect) for field in fields if field["name"] in unique_record_keys]

        sql.append(
            f"    {primary_key_clause(sql_dialect)} ({', '.join(unique_column_altnames)})")
    else:
        sql[-1] = sql[-1][:-1]  # Remove the last comma
    sql.append(");")
    sql.append("")

    if sql_dialect != "mysql":
        sql.extend(generate_table_comment_sql(
            sql_dialect, table_comments, plain_table_name))
        sql.extend(generate_column_comment_sql(
            sql_dialect, column_comments, plain_table_name))

    return sql


def generate_table_comments_json(schema: Dict[str, JsonNode]) -> Dict[str, str]:
    """
    Generates table-level comments as JSON.

    Args:
        schema (dict): Avro schema.

    Returns:
        dict: Table-level comments as JSON.
    """
    comments = {}
    if "doc" in schema:
        comments["doc"] = str(schema["doc"])
    return comments


def generate_column_comments_json(fields: List[Dict[str, JsonNode]], schema_list: List[Dict[str, JsonNode]]) -> Dict[str, str]:
    """
    Generates column-level comments as JSON.

    Args:
        fields (list): List of fields.
        schema_list (list): List of all schemas.

    Returns:
        dict: Column-level comments as JSON.
    """
    comments = {}
    for field in fields:
        column_comment = {}
        if "doc" in field:
            column_comment["doc"] = field["doc"]
        column_type = field["type"]
        if isinstance(column_type, list):
            column_type = [x for x in column_type if x != "null"]
            if len(column_type) > 1:
                column_comment["schema"] = {
                    "type": "union", "types": column_type}
                column_type = "union"
            else:
                column_type = column_type[0]
        if isinstance(column_type, dict) and column_type["type"] in ["array", "map", "record", "enum", "fixed"]:
            column_comment["schema"] = column_type
        elif isinstance(schema_list, list):
            column_schema = next(
                (x for x in schema_list if x["name"] == column_type), None)
            if column_schema:
                column_comment["schema"] = column_schema
        comments[str(field["name"])] = json.dumps(column_comment)
    return comments


def generate_table_comment_sql(dialect: str, table_comments: Dict[str, str], table_name: str) -> List[str]:
    """
    Generates SQL statements for table-level comments.

    Args:
        dialect (str): SQL dialect.
        table_comments (dict): Table-level comments as JSON.
        table_name (str): Table name.

    Returns:
        list: SQL statements for table-level comments.
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
        # Add more dialect-specific clauses if needed
    return comments


def generate_column_comment_sql(dialect: str, column_comments: Dict[str, str], table_name: str) -> List[str]:
    """
    Generates SQL statements for column-level comments.

    Args:
        dialect (str): SQL dialect.
        column_comments (dict): Column-level comments as JSON.
        table_name (str): Table name.

    Returns:
        list: SQL statements for column-level comments.
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


def escape_name(name, dialect):
    """
    Escapes a name (table or column) for the given SQL dialect.

    Args:
        name (str): The name to escape.
        dialect (str): The SQL dialect.

    Returns:
        str: The escaped name.
    """
    if dialect in ["sqlserver", "sqlanywhere"]:
        return f"[{name}]"
    elif dialect in ["postgres", "sqlite", "bigquery", "snowflake", "redshift"]:
        return f'"{name}"'
    elif dialect in ["mysql", "mariadb"]:
        return f"`{name}`"
    elif dialect in ["oracle", "db2"]:
        return f'"{name.upper()}"'
    elif dialect == "cassandra":
        return f'"{name}"'
    elif dialect in ["mongodb", "dynamodb", "elasticsearch", "couchdb", "neo4j", "firebase", "cosmosdb", "hbase"]:
        return name
    else:
        return name


def avro_type_to_sql_type(avro_type, dialect):
    """
    Maps an Avro type to a SQL type for the specified dialect.

    Args:
        avro_type (str or dict): The Avro type.
        dialect (str): The SQL dialect.

    Returns:
        str: The corresponding SQL type.
    """
    avro_to_sql_type_map = {
        "sqlserver": {
            "null": "NULL",
            "boolean": "BIT",
            "int": "INT",
            "long": "BIGINT",
            "float": "FLOAT",
            "double": "FLOAT",
            "bytes": "VARBINARY(MAX)",
            "string": "NVARCHAR(512)",
            "array": "NVARCHAR(MAX)",
            "map": "NVARCHAR(MAX)",
            "record": "NVARCHAR(MAX)",
            "union": "NVARCHAR(MAX)"
        },
        "postgres": {
            "null": "NULL",
            "boolean": "BOOLEAN",
            "int": "INTEGER",
            "long": "BIGINT",
            "float": "REAL",
            "double": "DOUBLE PRECISION",
            "bytes": "BYTEA",
            "string": "VARCHAR(512)",
            "array": "JSONB",
            "map": "JSONB",
            "record": "JSONB",
            "union": "JSONB"
        },
        "mysql": {
            "null": "NULL",
            "boolean": "BOOLEAN",
            "int": "INT",
            "long": "BIGINT",
            "float": "FLOAT",
            "double": "DOUBLE",
            "bytes": "BLOB",
            "string": "VARCHAR(512)",
            "array": "JSON",
            "map": "JSON",
            "record": "JSON",
            "union": "JSON"
        },
        "mariadb": {
            "null": "NULL",
            "boolean": "BOOLEAN",
            "int": "INT",
            "long": "BIGINT",
            "float": "FLOAT",
            "double": "DOUBLE",
            "bytes": "BLOB",
            "string": "VARCHAR(512)",
            "array": "JSON",
            "map": "JSON",
            "record": "JSON",
            "union": "JSON"
        },
        "sqlite": {
            "null": "NULL",
            "boolean": "BOOLEAN",
            "int": "INTEGER",
            "long": "INTEGER",
            "float": "REAL",
            "double": "REAL",
            "bytes": "BLOB",
            "string": "VARCHAR(512)",
            "array": "TEXT",
            "map": "TEXT",
            "record": "TEXT",
            "union": "TEXT"
        },
        "oracle": {
            "null": "NULL",
            "boolean": "NUMBER(1)",
            "int": "NUMBER(10)",
            "long": "NUMBER(19)",
            "float": "FLOAT(126)",
            "double": "FLOAT(126)",
            "bytes": "BLOB",
            "string": "VARCHAR(512)",
            "array": "CLOB",
            "map": "CLOB",
            "record": "CLOB",
            "union": "CLOB"
        },
        "db2": {
            "null": "NULL",
            "boolean": "BOOLEAN",
            "int": "INTEGER",
            "long": "BIGINT",
            "float": "REAL",
            "double": "DOUBLE",
            "bytes": "BLOB",
            "string": "VARCHAR(512)",
            "array": "CLOB",
            "map": "CLOB",
            "record": "CLOB",
            "union": "CLOB"
        },
        "sqlanywhere": {
            "null": "NULL",
            "boolean": "BIT",
            "int": "INTEGER",
            "long": "BIGINT",
            "float": "FLOAT",
            "double": "FLOAT",
            "bytes": "LONG BINARY",
            "string": "VARCHAR(512)",
            "array": "LONG VARCHAR",
            "map": "LONG VARCHAR",
            "record": "LONG VARCHAR",
            "union": "LONG VARCHAR"
        },
        "bigquery": {
            "null": "NULL",
            "boolean": "BOOL",
            "int": "INT64",
            "long": "INT64",
            "float": "FLOAT64",
            "double": "FLOAT64",
            "bytes": "BYTES",
            "string": "STRING",
            "array": "STRING",
            "map": "STRING",
            "record": "STRING",
            "union": "STRING"
        },
        "snowflake": {
            "null": "NULL",
            "boolean": "BOOLEAN",
            "int": "NUMBER",
            "long": "NUMBER",
            "float": "FLOAT",
            "double": "FLOAT",
            "bytes": "BINARY",
            "string": "STRING",
            "array": "VARIANT",
            "map": "VARIANT",
            "record": "VARIANT",
            "union": "VARIANT"
        },
        "redshift": {
            "null": "NULL",
            "boolean": "BOOLEAN",
            "int": "INTEGER",
            "long": "BIGINT",
            "float": "REAL",
            "double": "DOUBLE PRECISION",
            "bytes": "VARBYTE",
            "string": "VARCHAR(256)",
            "array": "VARCHAR(65535)",
            "map": "VARCHAR(65535)",
            "record": "VARCHAR(65535)",
            "union": "VARCHAR(65535)"
        }
    }

    if isinstance(avro_type, list):
        avro_type = [x for x in avro_type if x != "null"]
        if len(avro_type) > 1:
            return avro_to_sql_type_map[dialect]["union"]
        avro_type = avro_type[0]

    if isinstance(avro_type, dict):
        avro_type = avro_type.get("type", "string")

    return avro_to_sql_type_map[dialect].get(avro_type, avro_to_sql_type_map[dialect]["string"])


def unique_clause(dialect):
    """
    Returns the UNIQUE clause for the given SQL dialect.

    Args:
        dialect (str): The SQL dialect.

    Returns:
        str: The UNIQUE clause.
    """
    return "UNIQUE"


def primary_key_clause(dialect):
    """
    Returns the PRIMARY KEY clause for the given SQL dialect.

    Args:
        dialect (str): The SQL dialect.

    Returns:
        str: The PRIMARY KEY clause.
    """
    return "PRIMARY KEY"


def generate_cassandra_schema(schema: Dict[str, JsonNode], emit_cloudevents_columns: bool, schema_name: str) -> List[str]:
    """
    Generates Cassandra schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.
        schema_name (str): Schema name (optional).

    Returns:
        list: List of Cassandra schema statements.
    """
    namespace = cast(str, schema.get("namespace", "")).replace(".", "_")
    table_name = altname(schema, 'sql')
    table_name = escape_name(
        f"{namespace}_{table_name}" if namespace else table_name, "cassandra")
    table_name = f"{schema_name}.{table_name}" if schema_name else table_name
    # Cassandra table name length limit
    table_name = compact_table_name(table_name, 48)
    fields: List[Dict[str, JsonNode]] = cast(
        List[Dict[str, JsonNode]], schema["fields"])
    unique_record_keys: List[str] = cast(List[str], schema.get("unique", []))

    cql = []
    cql.append(f"CREATE TABLE {table_name} (")
    for field in fields:
        column_name = escape_name(
            altname(field, 'sql') or field["name"], 'cassandra')
        column_type = convert_avro_type_to_cassandra_type(field["type"])
        cql.append(f"    {column_name} {column_type},")
    if emit_cloudevents_columns:
        cql.extend([
            f"    {escape_name('cloudevents_type', 'cassandra')} text,",
            f"    {escape_name('cloudevents_source', 'cassandra')} text,",
            f"    {escape_name('cloudevents_id', 'cassandra')} text,",
            f"    {escape_name('cloudevents_time', 'cassandra')} timestamp,",
            f"    {escape_name('cloudevents_subject', 'cassandra')} text,"
        ])
    if unique_record_keys:
        unique_columns = [escape_name(field_name, "cassandra")
                          for field_name in unique_record_keys]
        unique_column_altnames = [altname(
            field, 'sql') for field in fields if field["name"] in unique_record_keys]
        cql.append(f"    PRIMARY KEY ({', '.join(unique_columns)})")
    elif emit_cloudevents_columns:
        cql.append(
            f"    PRIMARY KEY ({escape_name('cloudevents_id', 'cassandra')})")
    else:
        all_columns = [escape_name(
            altname(field, 'sql') or field, 'cassandra') for field in fields]
        cql.append(f"    PRIMARY KEY ({', '.join(all_columns)})")
    cql.append(");")
    return cql


def convert_avro_type_to_cassandra_type(avro_type):
    """
    Converts an Avro type to Cassandra type.

    Args:
        avro_type (str or dict): The Avro type.

    Returns:
        str: The corresponding Cassandra type.
    """
    avro_to_cassandra_type_map = {
        "null": "NULL",
        "boolean": "boolean",
        "int": "int",
        "long": "bigint",
        "float": "float",
        "double": "double",
        "bytes": "blob",
        "string": "text",
        "array": "text",
        "map": "text",
        "record": "text",
        "union": "text"
    }

    if isinstance(avro_type, list):
        avro_type = [x for x in avro_type if x != "null"]
        if len(avro_type) > 1:
            return avro_to_cassandra_type_map["union"]
        avro_type = avro_type[0]

    if isinstance(avro_type, dict):
        avro_type = avro_type.get("type", "string")

    return avro_to_cassandra_type_map.get(avro_type, "text")


def compact_table_name(table_name, max_length):
    """
    Compacts the table name to fit the specified maximum length.

    Args:
        table_name (str): Table name.
        max_length (int): Maximum length of the table name.

    Returns:
        str: Compacted table name.
    """
    if len(table_name) > max_length:
        # Drop vowels, one by one, seek from end
        vowels = "aeiou"
        while len(table_name) > max_length:
            count = 0
            for i in range(len(table_name) - 1, -1, -1):
                if table_name[i].lower() in vowels:
                    count += 1
                    table_name = table_name[:i] + table_name[i + 1:]
                    break
            if count == 0:
                break
    return table_name[:max_length]


def convert_avro_to_nosql(avro_schema_path, nosql_file_path, nosql_dialect, emit_cloudevents_columns=False):
    """
    Converts an Avro schema to NoSQL schema for the specified NoSQL dialect.

    Args:
        avro_schema_path (str): Path to the Avro schema file.
        nosql_file_path (str): Path to the output NoSQL schema file.
        nosql_dialect (str): NoSQL dialect (e.g., 'mongodb', 'dynamodb', 'cassandra').
        emit_cloudevents_columns (bool): Whether to include cloud events columns.

    Raises:
        ValueError: If the NoSQL dialect is unsupported.
    """
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)
    dirname = nosql_file_path
    if not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)

    if isinstance(schema, list):
        for schema in schema_list:
            model = generate_nosql(schema, nosql_dialect,
                                   emit_cloudevents_columns, schema_list)
            file_name = os.path.join(
                nosql_file_path, get_file_name(schema, get_nosql_file_extension(nosql_dialect)))
            with open(file_name, "w", encoding="utf-8") as nosql_file:
                if isinstance(model, list):
                    nosql_file.write("\n".join(model))
                else:
                    nosql_file.write(model)
    else:
        model = generate_nosql(schema, nosql_dialect,
                               emit_cloudevents_columns, schema_list)
        file_name = os.path.join(
            nosql_file_path, get_file_name(schema_list, get_nosql_file_extension(nosql_dialect)))
        with open(file_name, "w", encoding="utf-8") as nosql_file:
            nosql_file.write(model)

def get_nosql_file_extension(nosql_dialect):
    """
    Returns the file extension for the given NoSQL dialect.

    Args:
        nosql_dialect (str): NoSQL dialect.

    Returns:
        str: File extension.
    """
    if nosql_dialect == "neo4j":
        return "cypher"
    else:
        return "json"


def generate_nosql(schema, nosql_dialect, emit_cloudevents_columns, schema_list):
    """
    Generates NoSQL schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        nosql_dialect (str): NoSQL dialect.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.
        schema_list (list): List of all schemas.

    Returns:
        list: List of NoSQL schema statements.
    """
    if nosql_dialect == "mongodb":
        return generate_mongodb_schema(schema, emit_cloudevents_columns)
    elif nosql_dialect == "dynamodb":
        return generate_dynamodb_schema(schema, emit_cloudevents_columns)
    elif nosql_dialect == "elasticsearch":
        return generate_elasticsearch_schema(schema, emit_cloudevents_columns)
    elif nosql_dialect == "couchdb":
        return generate_couchdb_schema(schema, emit_cloudevents_columns)
    elif nosql_dialect == "neo4j":
        return generate_neo4j_schema(schema, emit_cloudevents_columns)
    elif nosql_dialect == "firebase":
        return generate_firebase_schema(schema, emit_cloudevents_columns)
    elif nosql_dialect == "cosmosdb":
        return generate_cosmosdb_schema(schema, emit_cloudevents_columns)
    elif nosql_dialect == "hbase":
        return generate_hbase_schema(schema, emit_cloudevents_columns)
    else:
        raise ValueError(f"Unsupported NoSQL dialect: {nosql_dialect}")


def generate_mongodb_schema(schema, emit_cloudevents_columns):
    """
    Generates MongoDB schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.

    Returns:
        list: List of MongoDB schema statements.
    """
    namespace = schema.get("namespace", "")
    collection_name = altname(schema, 'sql') or f"{namespace}_{schema['name']}"
    collection_name = namespace + "." + \
        collection_name if namespace else collection_name
    collection_name = '.'.join(camel(t) for t in collection_name.split('.'))
    fields = schema["fields"]
    unique_record_keys = schema.get("unique", [])

    mongodb_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [],
            "properties": {}
        }
    }

    for field in fields:
        column_name = altname(field, 'sql') or field["name"]
        column_type = convert_avro_type_to_mongodb_type(field["type"])
        mongodb_schema["$jsonSchema"]["properties"][column_name] = column_type
        if "null" not in field["type"]:
            mongodb_schema["$jsonSchema"]["required"].append(column_name)
        if field.get("unique", False):
            mongodb_schema["$jsonSchema"]["properties"][column_name]["unique"] = True
        if field.get("doc", ""):
            mongodb_schema["$jsonSchema"]["properties"][column_name]["description"] = field["doc"]

    if emit_cloudevents_columns:
        mongodb_schema["$jsonSchema"]["properties"].update({
            "___type": {"bsonType": "string"},
            "___source": {"bsonType": "string"},
            "___id": {"bsonType": "string"},
            "___time": {"bsonType": "date"},
            "___subject": {"bsonType": "string"}
        })
        mongodb_schema["$jsonSchema"]["required"].extend(
            ["___type", "___source", "___id"])

    return json.dumps({collection_name: mongodb_schema}, indent=4)


def convert_avro_type_to_mongodb_type(avro_type):
    """
    Converts an Avro type to MongoDB type.

    Args:
        avro_type (str or dict): The Avro type.

    Returns:
        dict: The corresponding MongoDB type.
    """
    avro_to_mongodb_type_map = {
        "null": {"bsonType": "null"},
        "boolean": {"bsonType": "bool"},
        "int": {"bsonType": "int"},
        "long": {"bsonType": "long"},
        "float": {"bsonType": "double"},
        "double": {"bsonType": "double"},
        "bytes": {"bsonType": "binData"},
        "string": {"bsonType": "string"},
        "array": {"bsonType": "array"},
        "map": {"bsonType": "object"},
        "record": {"bsonType": "object"},
        "union": {"bsonType": "object"}
    }

    if isinstance(avro_type, list):
        avro_type = [x for x in avro_type if x != "null"]
        if len(avro_type) > 1:
            return avro_to_mongodb_type_map["union"]
        avro_type = avro_type[0]

    if isinstance(avro_type, dict):
        avro_type = avro_type.get("type", "string")

    return avro_to_mongodb_type_map.get(avro_type, {"bsonType": "string"})


def generate_dynamodb_schema(schema, emit_cloudevents_columns):
    """
    Generates DynamoDB schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.

    Returns:
        list: List of DynamoDB schema statements.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    table_name = altname(schema, 'sql') or f"{namespace}_{schema['name']}"
    fields = schema["fields"]
    unique_record_keys = schema.get("unique", [])

    dynamodb_schema = {
        "TableName": table_name,
        "KeySchema": [],
        "AttributeDefinitions": [],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5
        }
    }

    for field in fields:
        column_name = altname(field, 'sql') or field["name"]
        column_type = convert_avro_type_to_dynamodb_type(field["type"])
        dynamodb_schema["AttributeDefinitions"].append(
            {"AttributeName": column_name, "AttributeType": column_type})
        if not dynamodb_schema["KeySchema"]:
            dynamodb_schema["KeySchema"].append(
                {"AttributeName": column_name, "KeyType": "HASH"})
        if field.get("unique", False):
            dynamodb_schema["AttributeDefinitions"].append(
                {"AttributeName": column_name, "AttributeType": column_type})
            dynamodb_schema["KeySchema"].append(
                {"AttributeName": column_name, "KeyType": "RANGE"})

    if emit_cloudevents_columns:
        dynamodb_schema["AttributeDefinitions"].extend([
            {"AttributeName": "___type", "AttributeType": "S"},
            {"AttributeName": "___source", "AttributeType": "S"},
            {"AttributeName": "___id", "AttributeType": "S"},
            {"AttributeName": "___time", "AttributeType": "S"},
            {"AttributeName": "___subject", "AttributeType": "S"}
        ])
        dynamodb_schema["KeySchema"].append(
            {"AttributeName": "___id", "KeyType": "HASH"})

    return json.dumps(dynamodb_schema, indent=4)


def convert_avro_type_to_dynamodb_type(avro_type):
    """
    Converts an Avro type to DynamoDB type.

    Args:
        avro_type (str or dict): The Avro type.

    Returns:
        str: The corresponding DynamoDB type.
    """
    avro_to_dynamodb_type_map = {
        "null": "NULL",
        "boolean": "BOOL",
        "int": "N",
        "long": "N",
        "float": "N",
        "double": "N",
        "bytes": "B",
        "string": "S",
        "array": "S",
        "map": "S",
        "record": "S",
        "union": "S"
    }

    if isinstance(avro_type, list):
        avro_type = [x for x in avro_type if x != "null"]
        if len(avro_type) > 1:
            return avro_to_dynamodb_type_map["union"]
        avro_type = avro_type[0]

    if isinstance(avro_type, dict):
        avro_type = avro_type.get("type", "string")

    return avro_to_dynamodb_type_map.get(avro_type, "S")


def generate_elasticsearch_schema(schema, emit_cloudevents_columns):
    """
    Generates Elasticsearch schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.

    Returns:
        list: List of Elasticsearch schema statements.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    index_name = altname(schema, 'sql') or f"{namespace}_{schema['name']}"
    fields = schema["fields"]

    es_mapping = {
        "mappings": {
            "properties": {}
        }
    }

    for field in fields:
        column_name = altname(field, 'sql') or field["name"]
        column_type = convert_avro_type_to_elasticsearch_type(field["type"])
        es_mapping["mappings"]["properties"][column_name] = column_type

    if emit_cloudevents_columns:
        es_mapping["mappings"]["properties"].update({
            "___type": {"type": "keyword"},
            "___source": {"type": "keyword"},
            "___id": {"type": "keyword"},
            "___time": {"type": "date"},
            "___subject": {"type": "keyword"}
        })

    return json.dumps({index_name: es_mapping}, indent=4)


def convert_avro_type_to_elasticsearch_type(avro_type):
    """
    Converts an Avro type to Elasticsearch type.

    Args:
        avro_type (str or dict): The Avro type.

    Returns:
        dict: The corresponding Elasticsearch type.
    """
    avro_to_elasticsearch_type_map = {
        "null": {"type": "null"},
        "boolean": {"type": "boolean"},
        "int": {"type": "integer"},
        "long": {"type": "long"},
        "float": {"type": "float"},
        "double": {"type": "double"},
        "bytes": {"type": "binary"},
        "string": {"type": "text"},
        "array": {"type": "text"},
        "map": {"type": "text"},
        "record": {"type": "text"},
        "union": {"type": "text"}
    }

    if isinstance(avro_type, list):
        avro_type = [x for x in avro_type if x != "null"]
        if len(avro_type) > 1:
            return avro_to_elasticsearch_type_map["union"]
        avro_type = avro_type[0]

    if isinstance(avro_type, dict):
        avro_type = avro_type.get("type", "string")

    return avro_to_elasticsearch_type_map.get(avro_type, {"type": "text"})


def generate_couchdb_schema(schema, emit_cloudevents_columns):
    """
    Generates CouchDB schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.

    Returns:
        list: List of CouchDB schema statements.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    db_name = altname(schema, 'sql') or f"{namespace}_{schema['name']}"
    fields = schema["fields"]

    couchdb_schema = {
        "type": "object",
        "properties": {}
    }

    for field in fields:
        column_name = altname(field, 'sql') or field["name"]
        column_type = convert_avro_type_to_couchdb_type(field["type"])
        couchdb_schema["properties"][column_name] = column_type

    if emit_cloudevents_columns:
        couchdb_schema["properties"].update({
            "___type": {"type": "string"},
            "___source": {"type": "string"},
            "___id": {"type": "string"},
            "___time": {"type": "string"},
            "___subject": {"type": "string"}
        })

    return json.dumps({db_name: couchdb_schema}, indent=4)


def convert_avro_type_to_couchdb_type(avro_type):
    """
    Converts an Avro type to CouchDB type.

    Args:
        avro_type (str or dict): The Avro type.

    Returns:
        dict: The corresponding CouchDB type.
    """
    avro_to_couchdb_type_map = {
        "null": {"type": "null"},
        "boolean": {"type": "boolean"},
        "int": {"type": "integer"},
        "long": {"type": "integer"},
        "float": {"type": "number"},
        "double": {"type": "number"},
        "bytes": {"type": "string"},
        "string": {"type": "string"},
        "array": {"type": "string"},
        "map": {"type": "string"},
        "record": {"type": "string"},
        "union": {"type": "string"}
    }

    if isinstance(avro_type, list):
        avro_type = [x for x in avro_type if x != "null"]
        if len(avro_type) > 1:
            return avro_to_couchdb_type_map["union"]
        avro_type = avro_type[0]

    if isinstance(avro_type, dict):
        avro_type = avro_type.get("type", "string")

    return avro_to_couchdb_type_map.get(avro_type, {"type": "string"})


def generate_neo4j_schema(schema, emit_cloudevents_columns):
    """
    Generates Neo4j schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.

    Returns:
        list: List of Neo4j schema statements.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    label_name = altname(schema, 'sql') or f"{namespace}_{schema['name']}"
    fields = schema["fields"]

    cypher = []
    cypher.append(f"CREATE (:{label_name} {{")
    for field in fields:
        column_name = altname(field, 'sql') or field["name"]
        column_type = convert_avro_type_to_neo4j_type(field["type"])
        cypher.append(f"    {column_name}: {column_type},")
    if emit_cloudevents_columns:
        cypher.extend([
            f"    {escape_name('___type', 'neo4j')}: 'string',",
            f"    {escape_name('___source', 'neo4j')}: 'string',",
            f"    {escape_name('___id', 'neo4j')}: 'string',",
            f"    {escape_name('___time', 'neo4j')}: 'datetime',",
            f"    {escape_name('___subject', 'neo4j')}: 'string'"
        ])
    cypher[-1] = cypher[-1][:-1]  # Remove the last comma
    cypher.append("});")
    return cypher


def convert_avro_type_to_neo4j_type(avro_type):
    """
    Converts an Avro type to Neo4j type.

    Args:
        avro_type (str or dict): The Avro type.

    Returns:
        str: The corresponding Neo4j type.
    """
    avro_to_neo4j_type_map = {
        "null": "NULL",
        "boolean": "boolean",
        "int": "integer",
        "long": "long",
        "float": "float",
        "double": "float",
        "bytes": "string",
        "string": "string",
        "array": "string",
        "map": "string",
        "record": "string",
        "union": "string"
    }

    if isinstance(avro_type, list):
        avro_type = [x for x in avro_type if x != "null"]
        if len(avro_type) > 1:
            return avro_to_neo4j_type_map["union"]
        avro_type = avro_type[0]

    if isinstance(avro_type, dict):
        avro_type = avro_type.get("type", "string")

    return avro_to_neo4j_type_map.get(avro_type, "string")


def generate_firebase_schema(schema, emit_cloudevents_columns):
    """
    Generates Firebase schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.

    Returns:
        list: List of Firebase schema statements.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    collection_name = altname(schema, 'sql') or f"{namespace}_{schema['name']}"
    fields = schema["fields"]

    firebase_schema = {
        "fields": {}
    }

    for field in fields:
        column_name = altname(field, 'sql') or field["name"]
        column_type = convert_avro_type_to_firebase_type(field["type"])
        firebase_schema["fields"][column_name] = column_type

    if emit_cloudevents_columns:
        firebase_schema["fields"].update({
            "___type": {"type": "string"},
            "___source": {"type": "string"},
            "___id": {"type": "string"},
            "___time": {"type": "timestamp"},
            "___subject": {"type": "string"}
        })

    return json.dumps({collection_name: firebase_schema}, indent=4)


def convert_avro_type_to_firebase_type(avro_type):
    """
    Converts an Avro type to Firebase type.

    Args:
        avro_type (str or dict): The Avro type.

    Returns:
        dict: The corresponding Firebase type.
    """
    avro_to_firebase_type_map = {
        "null": {"type": "null"},
        "boolean": {"type": "boolean"},
        "int": {"type": "integer"},
        "long": {"type": "integer"},
        "float": {"type": "number"},
        "double": {"type": "number"},
        "bytes": {"type": "string"},
        "string": {"type": "string"},
        "array": {"type": "string"},
        "map": {"type": "string"},
        "record": {"type": "string"},
        "union": {"type": "string"}
    }

    if isinstance(avro_type, list):
        avro_type = [x for x in avro_type if x != "null"]
        if len(avro_type) > 1:
            return avro_to_firebase_type_map["union"]
        avro_type = avro_type[0]

    if isinstance(avro_type, dict):
        avro_type = avro_type.get("type", "string")

    return avro_to_firebase_type_map.get(avro_type, {"type": "string"})


def generate_cosmosdb_schema(schema, emit_cloudevents_columns):
    """
    Generates CosmosDB schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.

    Returns:
        list: List of CosmosDB schema statements.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    collection_name = altname(schema, 'sql') or f"{namespace}_{schema['name']}"
    fields = schema["fields"]

    cosmosdb_schema = {
        "id": collection_name,
        "partitionKey": {
            "paths": [],
            "kind": "Hash"
        },
        "uniqueKeyPolicy": {
            "uniqueKeys": []
        },
        "fields": {}
    }

    for field in fields:
        column_name = altname(field, 'sql') or field["name"]
        column_type = convert_avro_type_to_cosmosdb_type(field["type"])
        cosmosdb_schema["fields"][column_name] = column_type
        cosmosdb_schema["partitionKey"]["paths"].append(f"/{column_name}")

    if emit_cloudevents_columns:
        cosmosdb_schema["fields"].update({
            "___type": {"type": "string"},
            "___source": {"type": "string"},
            "___id": {"type": "string"},
            "___time": {"type": "string"},
            "___subject": {"type": "string"}
        })
        cosmosdb_schema["partitionKey"]["paths"].append("/___id")

    return json.dumps(cosmosdb_schema, indent=4)


def convert_avro_type_to_cosmosdb_type(avro_type):
    """
    Converts an Avro type to CosmosDB type.

    Args:
        avro_type (str or dict): The Avro type.

    Returns:
        dict: The corresponding CosmosDB type.
    """
    avro_to_cosmosdb_type_map = {
        "null": {"type": "null"},
        "boolean": {"type": "boolean"},
        "int": {"type": "number"},
        "long": {"type": "number"},
        "float": {"type": "number"},
        "double": {"type": "number"},
        "bytes": {"type": "string"},
        "string": {"type": "string"},
        "array": {"type": "string"},
        "map": {"type": "string"},
        "record": {"type": "string"},
        "union": {"type": "string"}
    }

    if isinstance(avro_type, list):
        avro_type = [x for x in avro_type if x != "null"]
        if len(avro_type) > 1:
            return avro_to_cosmosdb_type_map["union"]
        avro_type = avro_type[0]

    if isinstance(avro_type, dict):
        avro_type = avro_type.get("type", "string")

    return avro_to_cosmosdb_type_map.get(avro_type, {"type": "string"})


def generate_hbase_schema(schema, emit_cloudevents_columns):
    """
    Generates HBase schema statements for the given Avro schema.

    Args:
        schema (dict): Avro schema.
        emit_cloudevents_columns (bool): Whether to include cloud events columns.

    Returns:
        list: List of HBase schema statements.
    """
    namespace = schema.get("namespace", "").replace('.', '_')
    table_name = altname(schema, 'sql') or f"{namespace}_{schema['name']}"
    fields = schema["fields"]

    hbase_schema = {
        "table": table_name,
        "column_families": []
    }

    for field in fields:
        column_name = altname(field, 'sql') or field["name"]
        column_family = convert_avro_type_to_hbase_column_family(field["type"])
        hbase_schema["column_families"].append(
            {"name": column_name, "column_family": column_family})

    if emit_cloudevents_columns:
        hbase_schema["column_families"].extend([
            {"name": "___type", "column_family": "string"},
            {"name": "___source", "column_family": "string"},
            {"name": "___id", "column_family": "string"},
            {"name": "___time", "column_family": "string"},
            {"name": "___subject", "column_family": "string"}
        ])

    return json.dumps(hbase_schema, indent=4)


def convert_avro_type_to_hbase_column_family(avro_type):
    """
    Converts an Avro type to HBase column family.

    Args:
        avro_type (str or dict): The Avro type.

    Returns:
        str: The corresponding HBase column family.
    """
    avro_to_hbase_column_family_map = {
        "null": "string",
        "boolean": "boolean",
        "int": "integer",
        "long": "integer",
        "float": "number",
        "double": "number",
        "bytes": "string",
        "string": "string",
        "array": "string",
        "map": "string",
        "record": "string",
        "union": "string"
    }

    if isinstance(avro_type, list):
        avro_type = [x for x in avro_type if x != "null"]
        if len(avro_type) > 1:
            return avro_to_hbase_column_family_map["union"]
        avro_type = avro_type[0]

    if isinstance(avro_type, dict):
        avro_type = avro_type.get("type", "string")

    return avro_to_hbase_column_family_map.get(avro_type, "string")


def get_file_name(avro_schema: dict, extension: str) -> str:
    """
    Generates a file name based on the Avro schema.

    Args:
        avro_schema (dict): Avro schema.
        extension (str): File extension.

    Returns:
        str: Generated file name.
    """
    namespace = avro_schema.get("namespace", "").replace('.', '_')
    name = avro_schema.get("name", "")
    return (namespace + '.' + name + '.' + extension) if namespace else (name + '.' + extension)
