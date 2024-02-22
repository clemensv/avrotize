# Avrotize

> This tool is under very active development. Don't use it.

Avrotize is a command-line tool that allows you to convert between different
schema formats. It is designed to be easy to use and flexible, supporting a
variety of use cases.

Supported conversions to Avro Schema:
- JSON Schema
- XML Schema (XSD)
- Protocol Buffers
- ASN.1

Supported conversions from Avro Schema:
- Kusto Data Table Definition (KQL)
- T-SQL Table Definition (SQL)
- Apache Parquet files
- Protocol Buffers

Mind that many conversions are lossy and will not transfer all information to
the target schema. This is very much by design. The key point of this tool is to
use a "sane" schema format (Avro Schema) as the pivot point to and from which
other schema formats are converted. The tool tries to preserve the most
important information of the source schema format, but not all.

The conversion issues are documented below.

## Installation

You can install Avrotize from PyPI:

```bash
pip install avrotize
```

## Usage

Avrotize provides several commands for converting between different schema formats.

### Convert Proto schema to Avro schema

```bash
avrotize p2a --proto <path_to_proto_file> --avsc <path_to_avro_schema_file>
```

Conversion notes:
* Protobuf allows any scalar type as key in a map, Avro does not. When converting
  from Proto to Avro, the type information for the map keys is ignored.
* The tool embeds all 'well-known' Protobuf 3.0 types in Avro format and injects
  them as needed when the respective types are included. Only the Timestamp type is 
  mapped to the Avro logical type 'timestamp-millis'. The rest of the well-known
  Protobuf types are kept as Avro record types with the same field names and types.
* The field numbers in message types are not yet mapped to the positions of the
  fields in Avro records. The fields in Avro are ordered as they appear in the
  Proto schema.

### Convert Avro schema to Proto schema

```bash
avrotize a2p --avsc <path_to_avro_schema_file> --proto <path_to_proto_directory>
```

Conversion notes:

- Avro namespaces are resolved into distinct proto package definitions. The tool will
  create a new `.proto` file with the package definition and an `import` statement for
  each namespace found in the Avro schema.
- Avro type unions `[]` are converted to `oneof` expressions in Proto. Avro allows for 
  maps and arrays in the type union, whereas Proto only supports scalar types and 
  message type references. The tool will therefore emit message types containing
  a single array or map field for any such case and add it to the containing type,
  and will also recursively resolve further unions in the array and map values.
- The sequence of fields in a message follows the sequence of fields in the Avro
  record. When type unions need to be resolved into `oneof` expressions, the alternative
  fields need to be assigned field numbers, which will shift the field numbers for any
  subsequent fields.


### Convert JSON schema to Avro schema

```bash
avrotize j2a --jsons <path_to_json_schema_file> --avsc <path_to_avro_schema_file> [--namespace <avro_schema_namespace>]
```

JSON Schema is a very flexible schema format and extremely permissive. That
results in many valid JSON schema documents for which it is difficult to
translate all definitions into Avro Schema.

Conversion notes:
* All field constraints and validations associated with the JSON Schema are
  ignored in the translation to Avro. Avro does not support the same level of
  validation as JSON Schema.
* Very large schemas with many cross references (`$ref`) throughout the schema may
  have circular references that cannot be fully resolved in Avro Schema.
* JSON type unions as well as `allOf`, `anyOf`, and `oneOf` expressions that are
  shared and referenced by a `$ref` expression are mapped to a record type in
  Avro with a field `value` of the type union.  
* JSON enums are converted to the Avro `enum` type. Numeric values are not
  supported by Avro and the tool will ignore them. Numeric string values are
  prefixed with an underscore and the result is sanitized to be a valid Avro
  enum name.
* Untyped object properties (without `type` attribute) are mapped to an Avro
  union that allows scalar values or two levels of array and/or map nesting.
* Conditional schema validation is not translated to Avro. The tool will ignore
  all `if`/`then`/`else`, `dependentRequired`, and `dependentSchemas` keywords
  and the resulting Avro schema will not enforce the conditional validation.
* JSON Schema allows for arbitrary property names, Avro does not. When converting
  from JSON to Avro, the property names in objects are sanitized by replacing 
  any non-alphanumeric characters with underscores and prefixing the result with an 
  underscore. This may lead to name conflicts and the tool will simply append a 
  unique index to the name to avoid naming conflicts.
* All `patternProperties` are converted into a fields holding arrays of records.
* All external references (`$ref`) are resolved and embedded in the Avro schema.
  The tool does not support maintaining external references to other schemas. To
  perform a conversion, all external $ref references have to be resolvable by
  the tool.
* When a JSON schema file does not define a top-level type, the tool will look for 
  a `definitions` section and emit all definitions as a union of the types defined.
  This also works with Swagger and OpenAPI files.

### Convert XML Schema (XSD) to Avro schema

```bash
avrotize x2a --xsd <path_to_xsd_file> --avsc <path_to_avro_schema_file> [--namespace <avro_schema_namespace>]
```

Conversion notes:
* All XML Schema elements are mapped to Avro record types with fields, whereby
  both elements and attributes become fields in the record.
* `simpleType` declarations and all type constraints are ignored. Avro does not
  support the same level of validation as XML Schema.


### Convert Avro schema to Kusto table declaration

```bash
avrotize a2k --avsc <path_to_avro_schema_file> --kusto <path_to_kusto_kql_file> [--record-type <record_type>]
```

Conversion notes:
* Only the Avro `record` type can be mapped to a Kusto table. If the Avro schema
  contains other types (like `enum` or `array`), the tool will ignore them.
* Only the first `record` type in the Avro schema is converted to a Kusto table.
  If the Avro schema contains other `record` types, they will be ignored. The
  `--record-type` option can be used to specify which `record` type to convert.
* The fields of the record are mapped to columns in the Kusto table. Fields that
  are records or arrays or maps are mapped to columns of type `dynamic` in the
  Kusto table.


### Convert Avro schema to T-SQL table definition

```bash
avrotize a2tsql --avsc <path_to_avro_schema_file> --tsql <path_to_sql_file> [--record-type <record_type>]
```

Conversion notes:
* Only the Avro `record` type can be mapped to a T-SQL table. If the Avro schema
  contains other types (like `enum` or `array`), the tool will ignore them.
* Only the first `record` type in the Avro schema is converted to a T-SQL table.
  If the Avro schema contains other `record` types, they will be ignored. The
  `--record-type` option can be used to specify which `record` type to convert.
* The fields of the record are mapped to columns in the T-SQL table. Fields that
  are records or arrays or maps are mapped to columns of type `varchar(max)` in
  the T-SQL table and it's assumed for them to hold JSON data.
* The emitted script sets extended properties to the columns with the Avro schema
  definition of the field in a JSON format. This allows for easy introspection of
  the serialized Avro schema in the field definition.

## Convert Avro schema to empty Parquet file

```bash
avrotize a2pq --avsc <path_to_avro_schema_file> --parquet <path_to_parquet_schema_file>
```

Conversion notes:
* The emitted Parquet file contains only the schema, no data rows.

## Convert ASN.1 schema to Avro schema

```bash
avrotize asn2a --asn <path_to_asn1_schema_file>  --avsc <path_to_avro_schema_file>
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Avrotize is released under the Apache License. See the LICENSE file for more details.
