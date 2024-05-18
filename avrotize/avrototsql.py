import json
import sys

def convert_avro_to_tsql(avro_schema_path, avro_record_type, tsql_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the T-SQL statements
    tsql = []

    # Append the create table statement with the column names and types
    tsql.append(f"CREATE TABLE [{table_name}] (")
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_tsql_type(field["type"])
        tsql.append(f"    [{column_name}] {column_type},")
    if emit_cloud_events_columns:
        tsql.append("    [__type] VARCHAR(MAX) NOT NULL,")
        tsql.append("    [__source] VARCHAR(MAX) NOT NULL,")
        tsql.append("    [__id] VARCHAR(MAX) NOT NULL,")
        tsql.append("    [__time] DATETIME NULL,")
        tsql.append("    [__subject] VARCHAR(MAX) NULL")
    tsql.append(");")
    tsql.append("")

    # Add the doc string as table metadata
    if "doc" in schema:
        doc_string = schema["doc"]
        tsql.append(f"EXEC sys.sp_addextendedproperty @name = N'MS_Description', @value = N'{doc_string}', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'{table_name}';")
        tsql.append("")

    doc_string_statement = []
    for field in fields:
        column_name = field["name"]
        if "doc" in field:
            doc = field["doc"]
            doc_string_statement.append(f"EXEC sys.sp_addextendedproperty @name = N'MS_Description', @value = N'{doc}', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'{table_name}', @level2type = N'COLUMN', @level2name = N'{column_name}';")
        # determine if the column type is complex, if it is, add a 'MS_Avro_Schema' extended property
        column_type = field["type"]
        # if the column_type is a union with null, then it is a nullable field, just get the type
        if isinstance(column_type, list) and len(column_type) == 2 and "null" in column_type:
            column_type = [x for x in column_type if x != "null"][0]        
        if "type" in column_type and column_type["type"] in ["array", "map", "record", "enum", "fixed"]:
            doc_string_statement.append(f"EXEC sys.sp_addextendedproperty @name = N'MS_Avro_Schema', @value = N'{json.dumps(column_type)}', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'{table_name}', @level2type = N'COLUMN', @level2name = N'{column_name}';")
        elif isinstance(schema_list, list):
            # if the column type refers to a complex type, it may in the schema list
            column_schema = next((x for x in schema_list if x["name"] == column_type), None)
            if column_schema:
                doc_string_statement.append(f"EXEC sys.sp_addextendedproperty @name = N'MS_Avro_Schema', @value = N'{json.dumps(column_schema)}', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'{table_name}', @level2type = N'COLUMN', @level2name = N'{column_name}';")


    if doc_string_statement and emit_cloud_events_columns:
        doc_string_statement.extend([
            f"EXEC sys.sp_addextendedproperty @name = N'MS_Description', @value = N'Event type', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'{table_name}', @level2type = N'COLUMN', @level2name = N'__type';",
            f"EXEC sys.sp_addextendedproperty @name = N'MS_Description', @value = N'Context origin/source of the event', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'{table_name}', @level2type = N'COLUMN', @level2name = N'__source';",
            f"EXEC sys.sp_addextendedproperty @name = N'MS_Description', @value = N'Event identifier', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'{table_name}', @level2type = N'COLUMN', @level2name = N'__id';",
            f"EXEC sys.sp_addextendedproperty @name = N'MS_Description', @value = N'Event generation time', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'{table_name}', @level2type = N'COLUMN', @level2name = N'__time';",
            f"EXEC sys.sp_addextendedproperty @name = N'MS_Description', @value = N'Context subject of the event', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'{table_name}', @level2type = N'COLUMN', @level2name = N'__subject';"
        ])
    if doc_string_statement:
        tsql.extend(doc_string_statement)

    with open(tsql_file_path, "w", encoding="utf-8") as tsql_file:
        tsql_file.write("\n".join(tsql))

def convert_avro_to_postgres(avro_schema_path, avro_record_type, postgres_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the PostgreSQL statements
    postgres = []

    # Append the create table statement with the column names and types
    postgres.append(f"CREATE TABLE {table_name} (")
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_postgres_type(field["type"])
        postgres.append(f"    {column_name} {column_type},")
    if emit_cloud_events_columns:
        postgres.append("    __type VARCHAR NOT NULL,")
        postgres.append("    __source VARCHAR NOT NULL,")
        postgres.append("    __id VARCHAR NOT NULL,")
        postgres.append("    __time TIMESTAMP NULL,")
        postgres.append("    __subject VARCHAR NULL")
    postgres.append(");")
    postgres.append("")

    # Add the doc string as table metadata
    if "doc" in schema:
        doc_string = schema["doc"]
        postgres.append(f"COMMENT ON TABLE {table_name} IS '{doc_string}';")
        postgres.append("")

    doc_string_statement = []
    for field in fields:
        column_name = field["name"]
        if "doc" in field:
            doc = field["doc"]
            doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{doc}';")
        # determine if the column type is complex, if it is, add a 'Avro_Schema' comment
        column_type = field["type"]
        if isinstance(column_type, list) and len(column_type) == 2 and "null" in column_type:
            column_type = [x for x in column_type if x != "null"][0]
        if "type" in column_type and column_type["type"] in ["array", "map", "record", "enum", "fixed"]:
            doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{json.dumps(column_type)}';")
        elif isinstance(schema_list, list):
            column_schema = next((x for x in schema_list if x["name"] == column_type), None)
            if column_schema:
                doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{json.dumps(column_schema)}';")

    if doc_string_statement and emit_cloud_events_columns:
        doc_string_statement.extend([
            f"COMMENT ON COLUMN {table_name}.__type IS 'Event type';",
            f"COMMENT ON COLUMN {table_name}.__source IS 'Context origin/source of the event';",
            f"COMMENT ON COLUMN {table_name}.__id IS 'Event identifier';",
            f"COMMENT ON COLUMN {table_name}.__time IS 'Event generation time';",
            f"COMMENT ON COLUMN {table_name}.__subject IS 'Context subject of the event';"
        ])
    if doc_string_statement:
        postgres.extend(doc_string_statement)

    with open(postgres_file_path, "w", encoding="utf-8") as postgres_file:
        postgres_file.write("\n".join(postgres))

def convert_avro_to_mysql(avro_schema_path, avro_record_type, mysql_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the MySQL statements
    mysql = []

    # Append the create table statement with the column names and types
    mysql.append(f"CREATE TABLE `{table_name}` (")
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_mysql_type(field["type"])
        mysql.append(f"    `{column_name}` {column_type},")
    if emit_cloud_events_columns:
        mysql.append("    `__type` VARCHAR(255) NOT NULL,")
        mysql.append("    `__source` VARCHAR(255) NOT NULL,")
        mysql.append("    `__id` VARCHAR(255) NOT NULL,")
        mysql.append("    `__time` TIMESTAMP NULL,")
        mysql.append("    `__subject` VARCHAR(255) NULL")
    mysql.append(") ENGINE=InnoDB;")
    mysql.append("")

    # Add the doc string as table metadata
    if "doc" in schema:
        doc_string = schema["doc"]
        mysql.append(f"ALTER TABLE `{table_name}` COMMENT = '{doc_string}';")
        mysql.append("")

    doc_string_statement = []
    for field in fields:
        column_name = field["name"]
        if "doc" in field:
            doc = field["doc"]
            doc_string_statement.append(f"ALTER TABLE `{table_name}` MODIFY `{column_name}` COMMENT '{doc}';")
        # determine if the column type is complex, if it is, add a 'Avro_Schema' comment
        column_type = field["type"]
        if isinstance(column_type, list) and len(column_type) == 2 and "null" in column_type:
            column_type = [x for x in column_type if x != "null"][0]
        if "type" in column_type and column_type["type"] in ["array", "map", "record", "enum", "fixed"]:
            doc_string_statement.append(f"ALTER TABLE `{table_name}` MODIFY `{column_name}` COMMENT '{json.dumps(column_type)}';")
        elif isinstance(schema_list, list):
            column_schema = next((x for x in schema_list if x["name"] == column_type), None)
            if column_schema:
                doc_string_statement.append(f"ALTER TABLE `{table_name}` MODIFY `{column_name}` COMMENT '{json.dumps(column_schema)}';")

    if doc_string_statement and emit_cloud_events_columns:
        doc_string_statement.extend([
            f"ALTER TABLE `{table_name}` MODIFY `__type` COMMENT 'Event type';",
            f"ALTER TABLE `{table_name}` MODIFY `__source` COMMENT 'Context origin/source of the event';",
            f"ALTER TABLE `{table_name}` MODIFY `__id` COMMENT 'Event identifier';",
            f"ALTER TABLE `{table_name}` MODIFY `__time` COMMENT 'Event generation time';",
            f"ALTER TABLE `{table_name}` MODIFY `__subject` COMMENT 'Context subject of the event';"
        ])
    if doc_string_statement:
        mysql.extend(doc_string_statement)

    with open(mysql_file_path, "w", encoding="utf-8") as mysql_file:
        mysql_file.write("\n".join(mysql))

def map_scalar_type(type, nullable=True):
    if type == "null":
        return "NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"
    elif type == "int":
        return "INT" + ("" if nullable else " NOT") + " NULL"
    elif type == "long":
        return "BIGINT" + ("" if nullable else " NOT") + " NULL"
    elif type == "float":
        return "REAL" + ("" if nullable else " NOT") + " NULL"
    elif type == "double":
        return "FLOAT" + ("" if nullable else " NOT") + " NULL"
    elif type == "boolean":
        return "BIT" + ("" if nullable else " NOT") + " NULL"
    elif type == "bytes":
        return "NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"
    elif type == "string":
        return "NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"
    else:
        return "NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"

def convert_avro_type_to_tsql_type(avroType, nullable=False):
    if isinstance(avroType, list):
        # If the type is an array, then it is a union type. Look whether it's a pair of a scalar type and null:
        itemCount = len(avroType)
        if itemCount > 2:
            return " NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"
        if itemCount == 1:
            return convert_avro_type_to_tsql_type(avroType[0], False)
        else:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_tsql_type(second, True)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_tsql_type(first, True)
            else: 
                return "NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        nullable = avroType.get("nullable", True)
        if type == "enum":
            return "NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"
        elif type == "fixed":
            return "NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return "UNIQUEIDENTIFIER" + ("" if nullable else " NOT") + " NULL"
            return "NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return "DECIMAL" + ("" if nullable else " NOT") + " NULL"
            return "NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return "DATETIME" + ("" if nullable else " NOT") + " NULL"
            if logicalType in ["time-millis", "time-micros"]:
                return "TIME" + ("" if nullable else " NOT") + " NULL"
            return "BIGINT" + ("" if nullable else " NOT") + " NULL"
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return "DATE" + ("" if nullable else " NOT") + " NULL"
            return "INT" + ("" if nullable else " NOT") + " NULL"
        else:
            return map_scalar_type(type) + ("" if nullable else " NOT") + " NULL"
    elif isinstance(avroType, str):
        return map_scalar_type(avroType)  + ("" if nullable else " NOT") + " NULL"
    return "NVARCHAR(MAX)" + ("" if nullable else " NOT") + " NULL"

def convert_avro_type_to_postgres_type(avroType, nullable=False):
    if isinstance(avroType, list):
        itemCount = len(avroType)
        if itemCount > 2:
            return " TEXT" + (" NULL" if nullable else " NOT NULL")
        if itemCount == 1:
            return convert_avro_type_to_postgres_type(avroType[0], False)
        else:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_postgres_type(second, True)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_postgres_type(first, True)
            else:
                return "TEXT" + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        nullable = avroType.get("nullable", True)
        if type == "enum":
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "fixed":
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return "UUID" + (" NULL" if nullable else " NOT NULL")
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return "DECIMAL" + (" NULL" if nullable else " NOT NULL")
            return "BYTEA" + (" NULL" if nullable else " NOT NULL")
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return "TIMESTAMP" + (" NULL" if nullable else " NOT NULL")
            if logicalType in ["time-millis", "time-micros"]:
                return "TIME" + (" NULL" if nullable else " NOT NULL")
            return "BIGINT" + (" NULL" if nullable else " NOT NULL")
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return "DATE" + (" NULL" if nullable else " NOT NULL")
            return "INT" + (" NULL" if nullable else " NOT NULL")
        else:
            return map_scalar_type_postgres(type) + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, str):
        return map_scalar_type_postgres(avroType) + (" NULL" if nullable else " NOT NULL")
    return "TEXT" + (" NULL" if nullable else " NOT NULL")

def convert_avro_type_to_mysql_type(avroType, nullable=False):
    if isinstance(avroType, list):
        itemCount = len(avroType)
        if itemCount > 2:
            return " TEXT" + (" NULL" if nullable else " NOT NULL")
        if itemCount == 1:
            return convert_avro_type_to_mysql_type(avroType[0], False)
        else:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_mysql_type(second, True)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_mysql_type(first, True)
            else:
                return "TEXT" + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        nullable = avroType.get("nullable", True)
        if type == "enum":
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "fixed":
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return "CHAR(36)" + (" NULL" if nullable else " NOT NULL")
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return "DECIMAL" + (" NULL" if nullable else " NOT NULL")
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return "TIMESTAMP" + (" NULL" if nullable else " NOT NULL")
            if logicalType in ["time-millis", "time-micros"]:
                return "TIME" + (" NULL" if nullable else " NOT NULL")
            return "BIGINT" + (" NULL" if nullable else " NOT NULL")
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return "DATE" + (" NULL" if nullable else " NOT NULL")
            return "INT" + (" NULL" if nullable else " NOT NULL")
        else:
            return map_scalar_type_mysql(type) + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, str):
        return map_scalar_type_mysql(avroType) + (" NULL" if nullable else " NOT NULL")
    return "TEXT" + (" NULL" if nullable else " NOT NULL")

def map_scalar_type_postgres(type):
    if type == "null":
        return "TEXT"
    elif type == "int":
        return "INT"
    elif type == "long":
        return "BIGINT"
    elif type == "float":
        return "REAL"
    elif type == "double":
        return "DOUBLE PRECISION"
    elif type == "boolean":
        return "BOOLEAN"
    elif type == "bytes":
        return "BYTEA"
    elif type == "string":
        return "TEXT"
    else:
        return "TEXT"

def map_scalar_type_mysql(type):
    if type == "null":
        return "TEXT"
    elif type == "int":
        return "INT"
    elif type == "long":
        return "BIGINT"
    elif type == "float":
        return "FLOAT"
    elif type == "double":
        return "DOUBLE"
    elif type == "boolean":
        return "TINYINT(1)"
    elif type == "bytes":
        return "BLOB"
    elif type == "string":
        return "TEXT"
    else:
        return "TEXT"


def convert_avro_to_oracle(avro_schema_path, avro_record_type, oracle_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the Oracle SQL statements
    oracle = []

    # Append the create table statement with the column names and types
    oracle.append(f"CREATE TABLE {table_name} (")
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_oracle_type(field["type"])
        oracle.append(f"    {column_name} {column_type},")
    if emit_cloud_events_columns:
        oracle.append("    __type VARCHAR2(4000) NOT NULL,")
        oracle.append("    __source VARCHAR2(4000) NOT NULL,")
        oracle.append("    __id VARCHAR2(4000) NOT NULL,")
        oracle.append("    __time TIMESTAMP NULL,")
        oracle.append("    __subject VARCHAR2(4000) NULL")
    oracle.append(");")
    oracle.append("")

    # Add the doc string as table metadata
    if "doc" in schema:
        doc_string = schema["doc"]
        oracle.append(f"COMMENT ON TABLE {table_name} IS '{doc_string}';")
        oracle.append("")

    doc_string_statement = []
    for field in fields:
        column_name = field["name"]
        if "doc" in field:
            doc = field["doc"]
            doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{doc}';")
        column_type = field["type"]
        if isinstance(column_type, list) and len(column_type) == 2 and "null" in column_type:
            column_type = [x for x in column_type if x != "null"][0]
        if "type" in column_type and column_type["type"] in ["array", "map", "record", "enum", "fixed"]:
            doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{json.dumps(column_type)}';")
        elif isinstance(schema_list, list):
            column_schema = next((x for x in schema_list if x["name"] == column_type), None)
            if column_schema:
                doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{json.dumps(column_schema)}';")

    if doc_string_statement and emit_cloud_events_columns:
        doc_string_statement.extend([
            f"COMMENT ON COLUMN {table_name}.__type IS 'Event type';",
            f"COMMENT ON COLUMN {table_name}.__source IS 'Context origin/source of the event';",
            f"COMMENT ON COLUMN {table_name}.__id IS 'Event identifier';",
            f"COMMENT ON COLUMN {table_name}.__time IS 'Event generation time';",
            f"COMMENT ON COLUMN {table_name}.__subject IS 'Context subject of the event';"
        ])
    if doc_string_statement:
        oracle.extend(doc_string_statement)

    with open(oracle_file_path, "w", encoding="utf-8") as oracle_file:
        oracle_file.write("\n".join(oracle))

def convert_avro_type_to_oracle_type(avroType, nullable=False):
    if isinstance(avroType, list):
        itemCount = len(avroType)
        if itemCount > 2:
            return " VARCHAR2(4000)" + (" NULL" if nullable else " NOT NULL")
        if itemCount == 1:
            return convert_avro_type_to_oracle_type(avroType[0], False)
        else:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_oracle_type(second, True)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_oracle_type(first, True)
            else:
                return "VARCHAR2(4000)" + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        nullable = avroType.get("nullable", True)
        if type == "enum":
            return "VARCHAR2(4000)" + (" NULL" if nullable else " NOT NULL")
        elif type == "fixed":
            return "RAW(2000)" + (" NULL" if nullable else " NOT NULL")
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return "RAW(16)" + (" NULL" if nullable else " NOT NULL")
            return "VARCHAR2(4000)" + (" NULL" if nullable else " NOT NULL")
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return "NUMBER" + (" NULL" if nullable else " NOT NULL")
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return "TIMESTAMP" + (" NULL" if nullable else " NOT NULL")
            if logicalType in ["time-millis", "time-micros"]:
                return "TIMESTAMP" + (" NULL" if nullable else " NOT NULL")
            return "NUMBER(19)" + (" NULL" if nullable else " NOT NULL")
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return "DATE" + (" NULL" if nullable else " NOT NULL")
            return "NUMBER(10)" + (" NULL" if nullable else " NOT NULL")
        else:
            return map_scalar_type_oracle(type) + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, str):
        return map_scalar_type_oracle(avroType) + (" NULL" if nullable else " NOT NULL")
    return "VARCHAR2(4000)" + (" NULL" if nullable else " NOT NULL")

def map_scalar_type_oracle(type):
    if type == "null":
        return "VARCHAR2(4000)"
    elif type == "int":
        return "NUMBER(10)"
    elif type == "long":
        return "NUMBER(19)"
    elif type == "float":
        return "BINARY_FLOAT"
    elif type == "double":
        return "BINARY_DOUBLE"
    elif type == "boolean":
        return "NUMBER(1)"
    elif type == "bytes":
        return "BLOB"
    elif type == "string":
        return "VARCHAR2(4000)"
    else:
        return "VARCHAR2(4000)"


def convert_avro_to_sqlite(avro_schema_path, avro_record_type, sqlite_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the SQLite statements
    sqlite = []

    # Append the create table statement with the column names and types
    sqlite.append(f"CREATE TABLE {table_name} (")
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_sqlite_type(field["type"])
        sqlite.append(f"    {column_name} {column_type},")
    if emit_cloud_events_columns:
        sqlite.append("    __type TEXT NOT NULL,")
        sqlite.append("    __source TEXT NOT NULL,")
        sqlite.append("    __id TEXT NOT NULL,")
        sqlite.append("    __time TEXT NULL,")
        sqlite.append("    __subject TEXT NULL")
    sqlite.append(");")
    sqlite.append("")

    # Add the doc string as table metadata
    if "doc" in schema:
        doc_string = schema["doc"]
        sqlite.append(f"-- {doc_string}")
        sqlite.append("")

    doc_string_statement = []
    for field in fields:
        column_name = field["name"]
        if "doc" in field:
            doc = field["doc"]
            doc_string_statement.append(f"-- {table_name}.{column_name}: {doc}")
        column_type = field["type"]
        if isinstance(column_type, list) and len(column_type) == 2 and "null" in column_type:
            column_type = [x for x in column_type if x != "null"][0]
        if "type" in column_type and column_type["type"] in ["array", "map", "record", "enum", "fixed"]:
            doc_string_statement.append(f"-- {table_name}.{column_name}: {json.dumps(column_type)}")
        elif isinstance(schema_list, list):
            column_schema = next((x for x in schema_list if x["name"] == column_type), None)
            if column_schema:
                doc_string_statement.append(f"-- {table_name}.{column_name}: {json.dumps(column_schema)}")

    if doc_string_statement and emit_cloud_events_columns:
        doc_string_statement.extend([
            f"-- {table_name}.__type: Event type",
            f"-- {table_name}.__source: Context origin/source of the event",
            f"-- {table_name}.__id: Event identifier",
            f"-- {table_name}.__time: Event generation time",
            f"-- {table_name}.__subject: Context subject of the event"
        ])
    if doc_string_statement:
        sqlite.extend(doc_string_statement)

    with open(sqlite_file_path, "w", encoding="utf-8") as sqlite_file:
        sqlite_file.write("\n".join(sqlite))

def convert_avro_type_to_sqlite_type(avroType, nullable=False):
    if isinstance(avroType, list):
        itemCount = len(avroType)
        if itemCount > 2:
            return " TEXT" + (" NULL" if nullable else " NOT NULL")
        if itemCount == 1:
            return convert_avro_type_to_sqlite_type(avroType[0], False)
        else:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_sqlite_type(second, True)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_sqlite_type(first, True)
            else:
                return "TEXT" + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        nullable = avroType.get("nullable", True)
        if type == "enum":
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "fixed":
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return "TEXT" + (" NULL" if nullable else " NOT NULL")
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return "NUMERIC" + (" NULL" if nullable else " NOT NULL")
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return "TEXT" + (" NULL" if nullable else " NOT NULL")
            if logicalType in ["time-millis", "time-micros"]:
                return "TEXT" + (" NULL" if nullable else " NOT NULL")
            return "INTEGER" + (" NULL" if nullable else " NOT NULL")
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return "TEXT" + (" NULL" if nullable else " NOT NULL")
            return "INTEGER" + (" NULL" if nullable else " NOT NULL")
        else:
            return map_scalar_type_sqlite(type) + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, str):
        return map_scalar_type_sqlite(avroType) + (" NULL" if nullable else " NOT NULL")
    return "TEXT" + (" NULL" if nullable else " NOT NULL")

def map_scalar_type_sqlite(type):
    if type == "null":
        return "TEXT"
    elif type == "int":
        return "INTEGER"
    elif type == "long":
        return "INTEGER"
    elif type == "float":
        return "REAL"
    elif type == "double":
        return "REAL"
    elif type == "boolean":
        return "INTEGER"
    elif type == "bytes":
        return "BLOB"
    elif type == "string":
        return "TEXT"
    else:
        return "TEXT"

def convert_avro_to_db2(avro_schema_path, avro_record_type, db2_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the DB2 SQL statements
    db2 = []

    # Append the create table statement with the column names and types
    db2.append(f"CREATE TABLE {table_name} (")
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_db2_type(field["type"])
        db2.append(f"    {column_name} {column_type},")
    if emit_cloud_events_columns:
        db2.append("    __type VARCHAR(255) NOT NULL,")
        db2.append("    __source VARCHAR(255) NOT NULL,")
        db2.append("    __id VARCHAR(255) NOT NULL,")
        db2.append("    __time TIMESTAMP NULL,")
        db2.append("    __subject VARCHAR(255) NULL")
    db2.append(");")
    db2.append("")

    # Add the doc string as table metadata
    if "doc" in schema:
        doc_string = schema["doc"]
        db2.append(f"COMMENT ON TABLE {table_name} IS '{doc_string}';")
        db2.append("")

    doc_string_statement = []
    for field in fields:
        column_name = field["name"]
        if "doc" in field:
            doc = field["doc"]
            doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{doc}';")
        column_type = field["type"]
        if isinstance(column_type, list) and len(column_type) == 2 and "null" in column_type:
            column_type = [x for x in column_type if x != "null"][0]
        if "type" in column_type and column_type["type"] in ["array", "map", "record", "enum", "fixed"]:
            doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{json.dumps(column_type)}';")
        elif isinstance(schema_list, list):
            column_schema = next((x for x in schema_list if x["name"] == column_type), None)
            if column_schema:
                doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{json.dumps(column_schema)}';")

    if doc_string_statement and emit_cloud_events_columns:
        doc_string_statement.extend([
            f"COMMENT ON COLUMN {table_name}.__type IS 'Event type';",
            f"COMMENT ON COLUMN {table_name}.__source IS 'Context origin/source of the event';",
            f"COMMENT ON COLUMN {table_name}.__id IS 'Event identifier';",
            f"COMMENT ON COLUMN {table_name}.__time IS 'Event generation time';",
            f"COMMENT ON COLUMN {table_name}.__subject IS 'Context subject of the event';"
        ])
    if doc_string_statement:
        db2.extend(doc_string_statement)

    with open(db2_file_path, "w", encoding="utf-8") as db2_file:
        db2_file.write("\n".join(db2))

def convert_avro_type_to_db2_type(avroType, nullable=False):
    if isinstance(avroType, list):
        itemCount = len(avroType)
        if itemCount > 2:
            return " VARCHAR(255)" + (" NULL" if nullable else " NOT NULL")
        if itemCount == 1:
            return convert_avro_type_to_db2_type(avroType[0], False)
        else:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_db2_type(second, True)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_db2_type(first, True)
            else:
                return "VARCHAR(255)" + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        nullable = avroType.get("nullable", True)
        if type == "enum":
            return "VARCHAR(255)" + (" NULL" if nullable else " NOT NULL")
        elif type == "fixed":
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return "CHAR(36)" + (" NULL" if nullable else " NOT NULL")
            return "VARCHAR(255)" + (" NULL" if nullable else " NOT NULL")
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return "DECIMAL" + (" NULL" if nullable else " NOT NULL")
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return "TIMESTAMP" + (" NULL" if nullable else " NOT NULL")
            if logicalType in ["time-millis", "time-micros"]:
                return "TIME" + (" NULL" if nullable else " NOT NULL")
            return "BIGINT" + (" NULL" if nullable else " NOT NULL")
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return "DATE" + (" NULL" if nullable else " NOT NULL")
            return "INTEGER" + (" NULL" if nullable else " NOT NULL")
        else:
            return map_scalar_type_db2(type) + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, str):
        return map_scalar_type_db2(avroType) + (" NULL" if nullable else " NOT NULL")
    return "VARCHAR(255)" + (" NULL" if nullable else " NOT NULL")

def map_scalar_type_db2(type):
    if type == "null":
        return "VARCHAR(255)"
    elif type == "int":
        return "INTEGER"
    elif type == "long":
        return "BIGINT"
    elif type == "float":
        return "REAL"
    elif type == "double":
        return "DOUBLE"
    elif type == "boolean":
        return "SMALLINT"
    elif type == "bytes":
        return "BLOB"
    elif type == "string":
        return "VARCHAR(255)"
    else:
        return "VARCHAR(255)"

def convert_avro_to_cassandra(avro_schema_path, avro_record_type, cassandra_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the CQL statements
    cql = []

    # Append the create table statement with the column names and types
    cql.append(f"CREATE TABLE {table_name} (")
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_cassandra_type(field["type"])
        cql.append(f"    {column_name} {column_type},")
    if emit_cloud_events_columns:
        cql.append("    __type TEXT,")
        cql.append("    __source TEXT,")
        cql.append("    __id TEXT,")
        cql.append("    __time TIMESTAMP,")
        cql.append("    __subject TEXT")
    cql.append("    PRIMARY KEY (__id)")
    cql.append(");")
    cql.append("")

    # Add the doc string as table metadata
    if "doc" in schema:
        doc_string = schema["doc"]
        cql.append(f"-- {doc_string}")
        cql.append("")

    doc_string_statement = []
    for field in fields:
        column_name = field["name"]
        if "doc" in field:
            doc = field["doc"]
            doc_string_statement.append(f"-- {table_name}.{column_name}: {doc}")
        column_type = field["type"]
        if isinstance(column_type, list) and len(column_type) == 2 and "null" in column_type:
            column_type = [x for x in column_type if x != "null"][0]
        if "type" in column_type and column_type["type"] in ["array", "map", "record", "enum", "fixed"]:
            doc_string_statement.append(f"-- {table_name}.{column_name}: {json.dumps(column_type)}")
        elif isinstance(schema_list, list):
            column_schema = next((x for x in schema_list if x["name"] == column_type), None)
            if column_schema:
                doc_string_statement.append(f"-- {table_name}.{column_name}: {json.dumps(column_schema)}")

    if doc_string_statement and emit_cloud_events_columns:
        doc_string_statement.extend([
            f"-- {table_name}.__type: Event type",
            f"-- {table_name}.__source: Context origin/source of the event",
            f"-- {table_name}.__id: Event identifier",
            f"-- {table_name}.__time: Event generation time",
            f"-- {table_name}.__subject: Context subject of the event"
        ])
    if doc_string_statement:
        cql.extend(doc_string_statement)

    with open(cassandra_file_path, "w", encoding="utf-8") as cassandra_file:
        cassandra_file.write("\n".join(cql))

def convert_avro_type_to_cassandra_type(avroType, nullable=False):
    if isinstance(avroType, list):
        itemCount = len(avroType)
        if itemCount > 2:
            return " TEXT" + (" NULL" if nullable else " NOT NULL")
        if itemCount == 1:
            return convert_avro_type_to_cassandra_type(avroType[0], False)
        else:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_cassandra_type(second, True)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_cassandra_type(first, True)
            else:
                return "TEXT" + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        nullable = avroType.get("nullable", True)
        if type == "enum":
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "fixed":
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return "UUID" + (" NULL" if nullable else " NOT NULL")
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return "DECIMAL" + (" NULL" if nullable else " NOT NULL")
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return "TIMESTAMP" + (" NULL" if nullable else " NOT NULL")
            if logicalType in ["time-millis", "time-micros"]:
                return "TIME" + (" NULL" if nullable else " NOT NULL")
            return "BIGINT" + (" NULL" if nullable else " NOT NULL")
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return "DATE" + (" NULL" if nullable else " NOT NULL")
            return "INT" + (" NULL" if nullable else " NOT NULL")
        else:
            return map_scalar_type_cassandra(type) + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, str):
        return map_scalar_type_cassandra(avroType) + (" NULL" if nullable else " NOT NULL")
    return "TEXT" + (" NULL" if nullable else " NOT NULL")

def map_scalar_type_cassandra(type):
    if type == "null":
        return "TEXT"
    elif type == "int":
        return "INT"
    elif type == "long":
        return "BIGINT"
    elif type == "float":
        return "FLOAT"
    elif type == "double":
        return "DOUBLE"
    elif type == "boolean":
        return "BOOLEAN"
    elif type == "bytes":
        return "BLOB"
    elif type == "string":
        return "TEXT"
    else:
        return "TEXT"


def convert_avro_to_mariadb(avro_schema_path, avro_record_type, mariadb_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the MariaDB SQL statements
    mariadb = []

    # Append the create table statement with the column names and types
    mariadb.append(f"CREATE TABLE `{table_name}` (")
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_mariadb_type(field["type"])
        mariadb.append(f"    `{column_name}` {column_type},")
    if emit_cloud_events_columns:
        mariadb.append("    `__type` VARCHAR(255) NOT NULL,")
        mariadb.append("    `__source` VARCHAR(255) NOT NULL,")
        mariadb.append("    `__id` VARCHAR(255) NOT NULL,")
        mariadb.append("    `__time` TIMESTAMP NULL,")
        mariadb.append("    `__subject` VARCHAR(255) NULL")
    mariadb.append(") ENGINE=InnoDB;")
    mariadb.append("")

    # Add the doc string as table metadata
    if "doc" in schema:
        doc_string = schema["doc"]
        mariadb.append(f"ALTER TABLE `{table_name}` COMMENT = '{doc_string}';")
        mariadb.append("")

    doc_string_statement = []
    for field in fields:
        column_name = field["name"]
        if "doc" in field:
            doc = field["doc"]
            doc_string_statement.append(f"ALTER TABLE `{table_name}` MODIFY `{column_name}` COMMENT '{doc}';")
        column_type = field["type"]
        if isinstance(column_type, list) and len(column_type) == 2 and "null" in column_type:
            column_type = [x for x in column_type if x != "null"][0]
        if "type" in column_type and column_type["type"] in ["array", "map", "record", "enum", "fixed"]:
            doc_string_statement.append(f"ALTER TABLE `{table_name}` MODIFY `{column_name}` COMMENT '{json.dumps(column_type)}';")
        elif isinstance(schema_list, list):
            column_schema = next((x for x in schema_list if x["name"] == column_type), None)
            if column_schema:
                doc_string_statement.append(f"ALTER TABLE `{table_name}` MODIFY `{column_name}` COMMENT '{json.dumps(column_schema)}';")

    if doc_string_statement and emit_cloud_events_columns:
        doc_string_statement.extend([
            f"ALTER TABLE `{table_name}` MODIFY `__type` COMMENT 'Event type';",
            f"ALTER TABLE `{table_name}` MODIFY `__source` COMMENT 'Context origin/source of the event';",
            f"ALTER TABLE `{table_name}` MODIFY `__id` COMMENT 'Event identifier';",
            f"ALTER TABLE `{table_name}` MODIFY `__time` COMMENT 'Event generation time';",
            f"ALTER TABLE `{table_name}` MODIFY `__subject` COMMENT 'Context subject of the event';"
        ])
    if doc_string_statement:
        mariadb.extend(doc_string_statement)

    with open(mariadb_file_path, "w", encoding="utf-8") as mariadb_file:
        mariadb_file.write("\n".join(mariadb))

def convert_avro_type_to_mariadb_type(avroType, nullable=False):
    if isinstance(avroType, list):
        itemCount = len(avroType)
        if itemCount > 2:
            return " TEXT" + (" NULL" if nullable else " NOT NULL")
        if itemCount == 1:
            return convert_avro_type_to_mariadb_type(avroType[0], False)
        else:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_mariadb_type(second, True)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_mariadb_type(first, True)
            else:
                return "TEXT" + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        nullable = avroType.get("nullable", True)
        if type == "enum":
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "fixed":
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return "CHAR(36)" + (" NULL" if nullable else " NOT NULL")
            return "TEXT" + (" NULL" if nullable else " NOT NULL")
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return "DECIMAL" + (" NULL" if nullable else " NOT NULL")
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return "TIMESTAMP" + (" NULL" if nullable else " NOT NULL")
            if logicalType in ["time-millis", "time-micros"]:
                return "TIME" + (" NULL" if nullable else " NOT NULL")
            return "BIGINT" + (" NULL" if nullable else " NOT NULL")
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return "DATE" + (" NULL" if nullable else " NOT NULL")
            return "INT" + (" NULL" if nullable else " NOT NULL")
        else:
            return map_scalar_type_mariadb(type) + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, str):
        return map_scalar_type_mariadb(avroType) + (" NULL" if nullable else " NOT NULL")
    return "TEXT" + (" NULL" if nullable else " NOT NULL")

def map_scalar_type_mariadb(type):
    if type == "null":
        return "TEXT"
    elif type == "int":
        return "INT"
    elif type == "long":
        return "BIGINT"
    elif type == "float":
        return "FLOAT"
    elif type == "double":
        return "DOUBLE"
    elif type == "boolean":
        return "TINYINT(1)"
    elif type == "bytes":
        return "BLOB"
    elif type == "string":
        return "TEXT"
    else:
        return "TEXT"

def convert_avro_to_mongodb(avro_schema_path, avro_record_type, mongodb_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    collection_name = schema["name"]
    fields = schema["fields"]

    # Create a dictionary to store the MongoDB schema
    mongodb_schema = {
        "bsonType": "object",
        "required": [],
        "properties": {}
    }

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_mongodb_type(field["type"])
        mongodb_schema["properties"][column_name] = column_type
        if "null" not in field["type"]:
            mongodb_schema["required"].append(column_name)
    
    if emit_cloud_events_columns:
        mongodb_schema["properties"].update({
            "__type": {"bsonType": "string"},
            "__source": {"bsonType": "string"},
            "__id": {"bsonType": "string"},
            "__time": {"bsonType": "date"},
            "__subject": {"bsonType": "string"}
        })
        mongodb_schema["required"].extend(["__type", "__source", "__id"])

    with open(mongodb_file_path, "w", encoding="utf-8") as mongodb_file:
        json.dump({collection_name: mongodb_schema}, mongodb_file, indent=4)

def convert_avro_type_to_mongodb_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "record":
            return {
                "bsonType": "object",
                "properties": {field["name"]: convert_avro_type_to_mongodb_type(field["type"]) for field in avroType["fields"]}
            }
        elif type == "array":
            return {"bsonType": "array", "items": convert_avro_type_to_mongodb_type(avroType["items"])}
        elif type == "map":
            return {"bsonType": "object", "additionalProperties": convert_avro_type_to_mongodb_type(avroType["values"])}
        elif type == "enum":
            return {"bsonType": "string"}
        elif type == "fixed":
            return {"bsonType": "binData"}
        elif type == "string":
            return {"bsonType": "string"}
        elif type == "bytes":
            return {"bsonType": "binData"}
        elif type == "int":
            return {"bsonType": "int"}
        elif type == "long":
            return {"bsonType": "long"}
        elif type == "float" or type == "double":
            return {"bsonType": "double"}
        elif type == "boolean":
            return {"bsonType": "bool"}
        elif type == "date":
            return {"bsonType": "date"}
    elif isinstance(avroType, str):
        return map_scalar_type_mongodb(avroType)
    return {"bsonType": "string"}

def map_scalar_type_mongodb(type):
    if type == "null":
        return {"bsonType": "null"}
    elif type == "int":
        return {"bsonType": "int"}
    elif type == "long":
        return {"bsonType": "long"}
    elif type == "float" or type == "double":
        return {"bsonType": "double"}
    elif type == "boolean":
        return {"bsonType": "bool"}
    elif type == "bytes":
        return {"bsonType": "binData"}
    elif type == "string":
        return {"bsonType": "string"}
    else:
        return {"bsonType": "string"}

def convert_avro_to_dynamodb(avro_schema_path, avro_record_type, dynamodb_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a dictionary to store the DynamoDB table schema
    dynamodb_schema = {
        "TableName": table_name,
        "AttributeDefinitions": [],
        "KeySchema": [],
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5
        }
    }

    attribute_definitions = []
    key_schema = []

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_dynamodb_type(field["type"])
        attribute_definitions.append({"AttributeName": column_name, "AttributeType": column_type})
        # Assume the first field is the primary key for simplicity
        if not key_schema:
            key_schema.append({"AttributeName": column_name, "KeyType": "HASH"})
    
    if emit_cloud_events_columns:
        attribute_definitions.extend([
            {"AttributeName": "__type", "AttributeType": "S"},
            {"AttributeName": "__source", "AttributeType": "S"},
            {"AttributeName": "__id", "AttributeType": "S"},
            {"AttributeName": "__time", "AttributeType": "S"},
            {"AttributeName": "__subject", "AttributeType": "S"}
        ])
        key_schema.append({"AttributeName": "__id", "KeyType": "HASH"})

    dynamodb_schema["AttributeDefinitions"] = attribute_definitions
    dynamodb_schema["KeySchema"] = key_schema

    with open(dynamodb_file_path, "w", encoding="utf-8") as dynamodb_file:
        json.dump(dynamodb_schema, dynamodb_file, indent=4)

def convert_avro_type_to_dynamodb_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "string":
            return "S"
        elif type == "bytes":
            return "B"
        elif type == "int" or type == "long":
            return "N"
        elif type == "float" or type == "double":
            return "N"
        elif type == "boolean":
            return "BOOL"
    elif isinstance(avroType, str):
        return map_scalar_type_dynamodb(avroType)
    return "S"

def map_scalar_type_dynamodb(type):
    if type == "null":
        return "S"
    elif type == "int" or type == "long" or type == "float" or type == "double":
        return "N"
    elif type == "boolean":
        return "BOOL"
    elif type == "bytes":
        return "B"
    elif type == "string":
        return "S"
    else:
        return "S"

def convert_avro_to_sqlanywhere(avro_schema_path, avro_record_type, sqlanywhere_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    table_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the SQL Anywhere SQL statements
    sqlanywhere = []

    # Append the create table statement with the column names and types
    sqlanywhere.append(f"CREATE TABLE {table_name} (")
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_sqlanywhere_type(field["type"])
        sqlanywhere.append(f"    {column_name} {column_type},")
    if emit_cloud_events_columns:
        sqlanywhere.append("    __type VARCHAR(255) NOT NULL,")
        sqlanywhere.append("    __source VARCHAR(255) NOT NULL,")
        sqlanywhere.append("    __id VARCHAR(255) NOT NULL,")
        sqlanywhere.append("    __time TIMESTAMP NULL,")
        sqlanywhere.append("    __subject VARCHAR(255) NULL")
    sqlanywhere.append(");")
    sqlanywhere.append("")

    # Add the doc string as table metadata
    if "doc" in schema:
        doc_string = schema["doc"]
        sqlanywhere.append(f"COMMENT ON TABLE {table_name} IS '{doc_string}';")
        sqlanywhere.append("")

    doc_string_statement = []
    for field in fields:
        column_name = field["name"]
        if "doc" in field:
            doc = field["doc"]
            doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{doc}';")
        column_type = field["type"]
        if isinstance(column_type, list) and len(column_type) == 2 and "null" in column_type:
            column_type = [x for x in column_type if x != "null"][0]
        if "type" in column_type and column_type["type"] in ["array", "map", "record", "enum", "fixed"]:
            doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{json.dumps(column_type)}';")
        elif isinstance(schema_list, list):
            column_schema = next((x for x in schema_list if x["name"] == column_type), None)
            if column_schema:
                doc_string_statement.append(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{json.dumps(column_schema)}';")

    if doc_string_statement and emit_cloud_events_columns:
        doc_string_statement.extend([
            f"COMMENT ON COLUMN {table_name}.__type IS 'Event type';",
            f"COMMENT ON COLUMN {table_name}.__source IS 'Context origin/source of the event';",
            f"COMMENT ON COLUMN {table_name}.__id IS 'Event identifier';",
            f"COMMENT ON COLUMN {table_name}.__time IS 'Event generation time';",
            f"COMMENT ON COLUMN {table_name}.__subject IS 'Context subject of the event';"
        ])
    if doc_string_statement:
        sqlanywhere.extend(doc_string_statement)

    with open(sqlanywhere_file_path, "w", encoding="utf-8") as sqlanywhere_file:
        sqlanywhere_file.write("\n".join(sqlanywhere))

def convert_avro_type_to_sqlanywhere_type(avroType, nullable=False):
    if isinstance(avroType, list):
        itemCount = len(avroType)
        if itemCount > 2:
            return " VARCHAR(255)" + (" NULL" if nullable else " NOT NULL")
        if itemCount == 1:
            return convert_avro_type_to_sqlanywhere_type(avroType[0], False)
        else:
            first = avroType[0]
            second = avroType[1]
            if isinstance(first, str) and first == "null":
                return convert_avro_type_to_sqlanywhere_type(second, True)
            elif isinstance(second, str) and second == "null":
                return convert_avro_type_to_sqlanywhere_type(first, True)
            else:
                return "VARCHAR(255)" + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, dict):
        type = avroType.get("type")
        nullable = avroType.get("nullable", True)
        if type == "enum":
            return "VARCHAR(255)" + (" NULL" if nullable else " NOT NULL")
        elif type == "fixed":
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "string":
            logicalType = avroType.get("logicalType")
            if logicalType == "uuid":
                return "CHAR(36)" + (" NULL" if nullable else " NOT NULL")
            return "VARCHAR(255)" + (" NULL" if nullable else " NOT NULL")
        elif type == "bytes":
            logicalType = avroType.get("logicalType")
            if logicalType == "decimal":
                return "DECIMAL" + (" NULL" if nullable else " NOT NULL")
            return "BLOB" + (" NULL" if nullable else " NOT NULL")
        elif type == "long":
            logicalType = avroType.get("logicalType")
            if logicalType in ["timestamp-millis", "timestamp-micros"]:
                return "TIMESTAMP" + (" NULL" if nullable else " NOT NULL")
            if logicalType in ["time-millis", "time-micros"]:
                return "TIME" + (" NULL" if nullable else " NOT NULL")
            return "BIGINT" + (" NULL" if nullable else " NOT NULL")
        elif type == "int":
            logicalType = avroType.get("logicalType")
            if logicalType == "date":
                return "DATE" + (" NULL" if nullable else " NOT NULL")
            return "INTEGER" + (" NULL" if nullable else " NOT NULL")
        else:
            return map_scalar_type_sqlanywhere(type) + (" NULL" if nullable else " NOT NULL")
    elif isinstance(avroType, str):
        return map_scalar_type_sqlanywhere(avroType) + (" NULL" if nullable else " NOT NULL")
    return "VARCHAR(255)" + (" NULL" if nullable else " NOT NULL")

def map_scalar_type_sqlanywhere(type):
    if type == "null":
        return "VARCHAR(255)"
    elif type == "int":
        return "INTEGER"
    elif type == "long":
        return "BIGINT"
    elif type == "float":
        return "FLOAT"
    elif type == "double":
        return "DOUBLE"
    elif type == "boolean":
        return "BIT"
    elif type == "bytes":
        return "BLOB"
    elif type == "string":
        return "VARCHAR(255)"
    else:
        return "VARCHAR(255)"       

def convert_avro_to_influxdb(avro_schema_path, avro_record_type, influxdb_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    # Parse the schema as a JSON object
    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    # Get the name and fields of the top-level record
    measurement_name = schema["name"]
    fields = schema["fields"]

    # Create a list to store the InfluxDB schema
    influxdb_schema = {
        "measurement": measurement_name,
        "fields": {},
        "tags": [],
        "time": "timestamp"
    }

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_influxdb_type(field["type"])
        influxdb_schema["fields"][column_name] = column_type
    
    if emit_cloud_events_columns:
        influxdb_schema["fields"].update({
            "__type": "string",
            "__source": "string",
            "__id": "string",
            "__time": "timestamp",
            "__subject": "string"
        })

    with open(influxdb_file_path, "w", encoding="utf-8") as influxdb_file:
        json.dump(influxdb_schema, influxdb_file, indent=4)

def convert_avro_type_to_influxdb_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "string"
        elif type == "bytes":
            return "string"
        elif type == "int" or type == "long":
            return "integer"
        elif type == "float" or type == "double":
            return "float"
        elif type == "boolean":
            return "boolean"
    elif isinstance(avroType, str):
        return map_scalar_type_influxdb(avroType)
    return "string"

def map_scalar_type_influxdb(type):
    if type == "null":
        return "string"
    elif type == "int":
        return "integer"
    elif type == "long":
        return "integer"
    elif type == "float":
        return "float"
    elif type == "double":
        return "float"
    elif type == "boolean":
        return "boolean"
    elif type == "bytes":
        return "string"
    elif type == "string":
        return "string"
    else:
        return "string"

def convert_avro_to_redis(avro_schema_path, avro_record_type, redis_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    table_name = schema["name"]
    fields = schema["fields"]

    redis_commands = []

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_redis_type(field["type"])
        redis_commands.append(f"SET {table_name}:{column_name} {column_type}")

    if emit_cloud_events_columns:
        redis_commands.extend([
            f"SET {table_name}:__type string",
            f"SET {table_name}:__source string",
            f"SET {table_name}:__id string",
            f"SET {table_name}:__time string",
            f"SET {table_name}:__subject string"
        ])

    with open(redis_file_path, "w", encoding="utf-8") as redis_file:
        redis_file.write("\n".join(redis_commands))

def convert_avro_type_to_redis_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "string"
        elif type == "bytes":
            return "string"
        elif type == "int" or type == "long":
            return "integer"
        elif type == "float" or type == "double":
            return "float"
        elif type == "boolean":
            return "boolean"
    elif isinstance(avroType, str):
        return map_scalar_type_redis(avroType)
    return "string"

def map_scalar_type_redis(type):
    if type == "null":
        return "string"
    elif type == "int":
        return "integer"
    elif type == "long":
        return "integer"
    elif type == "float":
        return "float"
    elif type == "double":
        return "float"
    elif type == "boolean":
        return "boolean"
    elif type == "bytes":
        return "string"
    elif type == "string":
        return "string"
    else:
        return "string"


def convert_avro_to_elasticsearch(avro_schema_path, avro_record_type, es_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    index_name = schema["name"]
    fields = schema["fields"]

    es_mapping = {
        "mappings": {
            "properties": {}
        }
    }

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_elasticsearch_type(field["type"])
        es_mapping["mappings"]["properties"][column_name] = {"type": column_type}

    if emit_cloud_events_columns:
        es_mapping["mappings"]["properties"].update({
            "__type": {"type": "text"},
            "__source": {"type": "text"},
            "__id": {"type": "keyword"},
            "__time": {"type": "date"},
            "__subject": {"type": "text"}
        })

    with open(es_file_path, "w", encoding="utf-8") as es_file:
        json.dump({index_name: es_mapping}, es_file, indent=4)

def convert_avro_type_to_elasticsearch_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "text"
        elif type == "bytes":
            return "binary"
        elif type == "int":
            return "integer"
        elif type == "long":
            return "long"
        elif type == "float":
            return "float"
        elif type == "double":
            return "double"
        elif type == "boolean":
            return "boolean"
        elif type == "timestamp-millis" or type == "timestamp-micros":
            return "date"
    elif isinstance(avroType, str):
        return map_scalar_type_elasticsearch(avroType)
    return "text"

def map_scalar_type_elasticsearch(type):
    if type == "null":
        return "text"
    elif type == "int":
        return "integer"
    elif type == "long":
        return "long"
    elif type == "float":
        return "float"
    elif type == "double":
        return "double"
    elif type == "boolean":
        return "boolean"
    elif type == "bytes":
        return "binary"
    elif type == "string":
        return "text"
    else:
        return "text"

def convert_avro_to_couchdb(avro_schema_path, avro_record_type, couchdb_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    doc_name = schema["name"]
    fields = schema["fields"]

    couchdb_schema = {
        "_id": f"_design/{doc_name}",
        "views": {
            "all": {
                "map": f"function(doc) {{ emit(doc._id, doc); }}"
            }
        },
        "indexes": {
            "all_fields": {
                "analyzer": "standard",
                "index": f"function(doc) {{ index('default', doc); }}"
            }
        },
        "schema": {}
    }

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_couchdb_type(field["type"])
        couchdb_schema["schema"][column_name] = column_type

    if emit_cloud_events_columns:
        couchdb_schema["schema"].update({
            "__type": "string",
            "__source": "string",
            "__id": "string",
            "__time": "string",
            "__subject": "string"
        })

    with open(couchdb_file_path, "w", encoding="utf-8") as couchdb_file:
        json.dump(couchdb_schema, couchdb_file, indent=4)

def convert_avro_type_to_couchdb_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "string"
        elif type == "bytes":
            return "string"
        elif type == "int":
            return "integer"
        elif type == "long":
            return "integer"
        elif type == "float":
            return "number"
        elif type == "double":
            return "number"
        elif type == "boolean":
            return "boolean"
    elif isinstance(avroType, str):
        return map_scalar_type_couchdb(avroType)
    return "string"

def map_scalar_type_couchdb(type):
    if type == "null":
        return "string"
    elif type == "int":
        return "integer"
    elif type == "long":
        return "integer"
    elif type == "float":
        return "number"
    elif type == "double":
        return "number"
    elif type == "boolean":
        return "boolean"
    elif type == "bytes":
        return "string"
    elif type == "string":
        return "string"
    else:
        return "string"


def convert_avro_to_neo4j(avro_schema_path, avro_record_type, neo4j_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    node_label = schema["name"]
    fields = schema["fields"]

    neo4j_commands = []

    # Create the Cypher query to create nodes with properties
    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_neo4j_type(field["type"])
        neo4j_commands.append(f"CREATE (n:{node_label} {{{column_name}: {column_type}}})")

    if emit_cloud_events_columns:
        neo4j_commands.append(f"CREATE (n:{node_label} {{__type: 'string', __source: 'string', __id: 'string', __time: 'datetime', __subject: 'string'}})")

    with open(neo4j_file_path, "w", encoding="utf-8") as neo4j_file:
        neo4j_file.write("\n".join(neo4j_commands))

def convert_avro_type_to_neo4j_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "'string'"
        elif type == "bytes":
            return "'string'"
        elif type == "int" or type == "long":
            return "int"
        elif type == "float" or type == "double":
            return "float"
        elif type == "boolean":
            return "boolean"
        elif type == "timestamp-millis" or type == "timestamp-micros":
            return "datetime"
    elif isinstance(avroType, str):
        return map_scalar_type_neo4j(avroType)
    return "'string'"

def map_scalar_type_neo4j(type):
    if type == "null":
        return "'string'"
    elif type == "int":
        return "int"
    elif type == "long":
        return "int"
    elif type == "float":
        return "float"
    elif type == "double":
        return "float"
    elif type == "boolean":
        return "boolean"
    elif type == "bytes":
        return "'string'"
    elif type == "string":
        return "'string'"
    else:
        return "'string'"

def convert_avro_to_bigquery(avro_schema_path, avro_record_type, bigquery_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    table_name = schema["name"]
    fields = schema["fields"]

    bigquery_schema = []

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_bigquery_type(field["type"])
        bigquery_schema.append({
            "name": column_name,
            "type": column_type,
            "mode": "NULLABLE" if "null" in field["type"] else "REQUIRED"
        })

    if emit_cloud_events_columns:
        bigquery_schema.extend([
            {"name": "__type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "__source", "type": "STRING", "mode": "REQUIRED"},
            {"name": "__id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "__time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "__subject", "type": "STRING", "mode": "NULLABLE"}
        ])

    with open(bigquery_file_path, "w", encoding="utf-8") as bigquery_file:
        json.dump({"table_name": table_name, "schema": bigquery_schema}, bigquery_file, indent=4)

def convert_avro_type_to_bigquery_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "STRING"
        elif type == "bytes":
            return "BYTES"
        elif type == "int":
            return "INT64"
        elif type == "long":
            return "INT64"
        elif type == "float":
            return "FLOAT64"
        elif type == "double":
            return "FLOAT64"
        elif type == "boolean":
            return "BOOL"
        elif type == "timestamp-millis" or type == "timestamp-micros":
            return "TIMESTAMP"
    elif isinstance(avroType, str):
        return map_scalar_type_bigquery(avroType)
    return "STRING"

def map_scalar_type_bigquery(type):
    if type == "null":
        return "STRING"
    elif type == "int":
        return "INT64"
    elif type == "long":
        return "INT64"
    elif type == "float":
        return "FLOAT64"
    elif type == "double":
        return "FLOAT64"
    elif type == "boolean":
        return "BOOL"
    elif type == "bytes":
        return "BYTES"
    elif type == "string":
        return "STRING"
    else:
        return "STRING"


def convert_avro_to_snowflake(avro_schema_path, avro_record_type, snowflake_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    table_name = schema["name"]
    fields = schema["fields"]

    snowflake_schema = []

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_snowflake_type(field["type"])
        snowflake_schema.append(f"{column_name} {column_type}")

    if emit_cloud_events_columns:
        snowflake_schema.extend([
            "__type STRING NOT NULL",
            "__source STRING NOT NULL",
            "__id STRING NOT NULL",
            "__time TIMESTAMP_NTZ NULL",
            "__subject STRING NULL"
        ])

    with open(snowflake_file_path, "w", encoding="utf-8") as snowflake_file:
        snowflake_file.write(f"CREATE TABLE {table_name} (\n")
        snowflake_file.write(",\n".join(snowflake_schema))
        snowflake_file.write("\n);")

def convert_avro_type_to_snowflake_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "STRING"
        elif type == "bytes":
            return "BINARY"
        elif type == "int":
            return "NUMBER"
        elif type == "long":
            return "NUMBER"
        elif type == "float":
            return "FLOAT"
        elif type == "double":
            return "FLOAT"
        elif type == "boolean":
            return "BOOLEAN"
        elif type == "timestamp-millis" or type == "timestamp-micros":
            return "TIMESTAMP_NTZ"
    elif isinstance(avroType, str):
        return map_scalar_type_snowflake(avroType)
    return "STRING"

def map_scalar_type_snowflake(type):
    if type == "null":
        return "STRING"
    elif type == "int":
        return "NUMBER"
    elif type == "long":
        return "NUMBER"
    elif type == "float":
        return "FLOAT"
    elif type == "double":
        return "FLOAT"
    elif type == "boolean":
        return "BOOLEAN"
    elif type == "bytes":
        return "BINARY"
    elif type == "string":
        return "STRING"
    else:
        return "STRING"


def convert_avro_to_firebase(avro_schema_path, avro_record_type, firebase_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    collection_name = schema["name"]
    fields = schema["fields"]

    firebase_schema = {}

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_firebase_type(field["type"])
        firebase_schema[column_name] = column_type

    if emit_cloud_events_columns:
        firebase_schema.update({
            "__type": "string",
            "__source": "string",
            "__id": "string",
            "__time": "timestamp",
            "__subject": "string"
        })

    with open(firebase_file_path, "w", encoding="utf-8") as firebase_file:
        json.dump({collection_name: firebase_schema}, firebase_file, indent=4)

def convert_avro_type_to_firebase_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "string"
        elif type == "bytes":
            return "string"
        elif type == "int":
            return "integer"
        elif type == "long":
            return "integer"
        elif type == "float":
            return "float"
        elif type == "double":
            return "double"
        elif type == "boolean":
            return "boolean"
        elif type == "timestamp-millis" or type == "timestamp-micros":
            return "timestamp"
    elif isinstance(avroType, str):
        return map_scalar_type_firebase(avroType)
    return "string"

def map_scalar_type_firebase(type):
    if type == "null":
        return "string"
    elif type == "int":
        return "integer"
    elif type == "long":
        return "integer"
    elif type == "float":
        return "float"
    elif type == "double":
        return "double"
    elif type == "boolean":
        return "boolean"
    elif type == "bytes":
        return "string"
    elif type == "string":
        return "string"
    else:
        return "string"


def convert_avro_to_redshift(avro_schema_path, avro_record_type, redshift_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    table_name = schema["name"]
    fields = schema["fields"]

    redshift_schema = []

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_redshift_type(field["type"])
        redshift_schema.append(f"{column_name} {column_type}")

    if emit_cloud_events_columns:
        redshift_schema.extend([
            "__type VARCHAR(256) NOT NULL",
            "__source VARCHAR(256) NOT NULL",
            "__id VARCHAR(256) NOT NULL",
            "__time TIMESTAMP NULL",
            "__subject VARCHAR(256) NULL"
        ])

    with open(redshift_file_path, "w", encoding="utf-8") as redshift_file:
        redshift_file.write(f"CREATE TABLE {table_name} (\n")
        redshift_file.write(",\n".join(redshift_schema))
        redshift_file.write("\n);")

def convert_avro_type_to_redshift_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "VARCHAR(256)"
        elif type == "bytes":
            return "VARBYTE"
        elif type == "int":
            return "INTEGER"
        elif type == "long":
            return "BIGINT"
        elif type == "float":
            return "FLOAT4"
        elif type == "double":
            return "FLOAT8"
        elif type == "boolean":
            return "BOOLEAN"
        elif type == "timestamp-millis" or type == "timestamp-micros":
            return "TIMESTAMP"
    elif isinstance(avroType, str):
        return map_scalar_type_redshift(avroType)
    return "VARCHAR(256)"

def map_scalar_type_redshift(type):
    if type == "null":
        return "VARCHAR(256)"
    elif type == "int":
        return "INTEGER"
    elif type == "long":
        return "BIGINT"
    elif type == "float":
        return "FLOAT4"
    elif type == "double":
        return "FLOAT8"
    elif type == "boolean":
        return "BOOLEAN"
    elif type == "bytes":
        return "VARBYTE"
    elif type == "string":
        return "VARCHAR(256)"
    else:
        return "VARCHAR(256)"

def convert_avro_to_cosmosdb(avro_schema_path, avro_record_type, cosmosdb_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    collection_name = schema["name"]
    fields = schema["fields"]

    cosmosdb_schema = {
        "id": collection_name,
        "fields": {}
    }

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_cosmosdb_type(field["type"])
        cosmosdb_schema["fields"][column_name] = column_type

    if emit_cloud_events_columns:
        cosmosdb_schema["fields"].update({
            "__type": "string",
            "__source": "string",
            "__id": "string",
            "__time": "string",
            "__subject": "string"
        })

    with open(cosmosdb_file_path, "w", encoding="utf-8") as cosmosdb_file:
        json.dump({collection_name: cosmosdb_schema}, cosmosdb_file, indent=4)

def convert_avro_type_to_cosmosdb_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "string"
        elif type == "bytes":
            return "string"
        elif type == "int":
            return "integer"
        elif type == "long":
            return "integer"
        elif type == "float":
            return "number"
        elif type == "double":
            return "number"
        elif type == "boolean":
            return "boolean"
    elif isinstance(avroType, str):
        return map_scalar_type_cosmosdb(avroType)
    return "string"

def map_scalar_type_cosmosdb(type):
    if type == "null":
        return "string"
    elif type == "int":
        return "integer"
    elif type == "long":
        return "integer"
    elif type == "float":
        return "number"
    elif type == "double":
        return "number"
    elif type == "boolean":
        return "boolean"
    elif type == "bytes":
        return "string"
    elif type == "string":
        return "string"
    else:
        return "string"

def convert_avro_to_hbase(avro_schema_path, avro_record_type, hbase_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r", encoding="utf-8") as f:
        schema_json = f.read()

    schema_list = schema = json.loads(schema_json)

    if isinstance(schema, list) and avro_record_type:
        schema = next((x for x in schema if x["name"] == avro_record_type), None)
        if schema is None:
            print(f"No record type {avro_record_type} found in the Avro schema")
            sys.exit(1)
    elif not isinstance(schema, dict):
        print("Expected a single Avro schema as a JSON object, or a list of schema records")
        sys.exit(1)

    table_name = schema["name"]
    fields = schema["fields"]

    hbase_schema = []

    for field in fields:
        column_name = field["name"]
        column_type = convert_avro_type_to_hbase_type(field["type"])
        hbase_schema.append(f"add 'cf:{column_name}', {column_type}")

    if emit_cloud_events_columns:
        hbase_schema.extend([
            "add 'cf:__type', 'string'",
            "add 'cf:__source', 'string'",
            "add 'cf:__id', 'string'",
            "add 'cf:__time', 'string'",
            "add 'cf:__subject', 'string'"
        ])

    with open(hbase_file_path, "w", encoding="utf-8") as hbase_file:
        hbase_file.write(f"create '{table_name}', 'cf'\n")
        hbase_file.write("\n".join(hbase_schema))

def convert_avro_type_to_hbase_type(avroType):
    if isinstance(avroType, list):
        if "null" in avroType:
            avroType = [x for x in avroType if x != "null"][0]
        else:
            avroType = avroType[0]

    if isinstance(avroType, dict):
        type = avroType.get("type")
        if type == "enum" or type == "fixed" or type == "string":
            return "string"
        elif type == "bytes":
            return "string"
        elif type == "int":
            return "integer"
        elif type == "long":
            return "integer"
        elif type == "float":
            return "float"
        elif type == "double":
            return "double"
        elif type == "boolean":
            return "boolean"
    elif isinstance(avroType, str):
        return map_scalar_type_hbase(avroType)
    return "string"

def map_scalar_type_hbase(type):
    if type == "null":
        return "string"
    elif type == "int":
        return "integer"
    elif type == "long":
        return "integer"
    elif type == "float":
        return "float"
    elif type == "double":
        return "double"
    elif type == "boolean":
        return "boolean"
    elif type == "bytes":
        return "string"
    elif type == "string":
        return "string"
    else:
        return "string"




