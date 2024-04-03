import json
import sys

def convert_avro_to_tsql(avro_schema_path, avro_record_type, tsql_file_path, emit_cloud_events_columns=False):
    schema_file = avro_schema_path
    if not schema_file:
        print("Please specify the avro schema file")
        sys.exit(1)
    with open(schema_file, "r") as f:
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

    with open(tsql_file_path, "w") as tsql_file:
        tsql_file.write("\n".join(tsql))

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

def map_scalar_type(type):
    if type == "null":
        return "NVARCHAR(MAX)"
    elif type == "int":
        return "INT"
    elif type == "long":
        return "BIGINT"
    elif type == "float":
        return "REAL"
    elif type == "double":
        return "FLOAT"
    elif type == "boolean":
        return "BIT"
    elif type == "bytes":
        return "NVARCHAR(MAX)"
    elif type == "string":
        return "NVARCHAR(MAX)"
    else:
        return "NVARCHAR(MAX)"
