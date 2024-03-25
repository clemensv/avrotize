# Avrotize

Avrotize is a command-line tool for converting data structure definitions between
different schema formats, using [Apache Avro](https://avro.apache.org/) Schema
as the integration schema model.

You can use the tool to convert between Avro Schema and other schema formats
like JSON Schema, XML Schema (XSD), Protocol Buffers (Protobuf), ASN.1, and
database schema formats like Kusto Data Table Definition (KQL) and T-SQL Table
Definition (SQL). That means you can also convert from JSON Schema to Protobuf
going via Avro Schema.

The tool does not convert data (instances of schemas), only the data structure
definitions.

Mind that the primary objective of the tool is the conversion of schemas that
describe data structures used in applications, databases, and message systems.
While the project's internal tests do cover a lot of ground, it is nevertheless
not a primary goal of the tool to convert every complex document schema like
those used for devops pipeline or system configuration files.

## Why?

Data structure definitions are an essential part of data exchange,
serialization, and storage. They define the shape and type of data, and they are
foundational for tooling and libraries for working with the data. Nearly all
data schema languages are coupled to a specific data exchange or storage format,
locking the definitions to that format.

Avrotize is designed as a tool to "unlock" data definitions from JSON Schema or
XML Schema and make them usable in other contexts. The intent is also to lay a
foundation for transcoding data from one format to another, by translating the
schema definitions as accurately as possible into the schema model of the target
format's schema. The transcoding of the data itself requires separate tools
that are beyond the scope of this project.

The use of the term "data structure definition" and not "data object definition"
is quite intentional. The focus of the tool is on data structures that can be
used for messaging and eventing payloads, for data serialization, and for
database tables, with the goal that those structures can be mapped cleanly from
and to common programming language types.

Therefore, Avrotize intentionally ignores common techniques to model
object-oriented inheritance. For instance, when converting from JSON Schema, all
content from `allOf` expressions is merged into a single record type rather than
trying to model the inheritance tree in Avro.

## Why Avro Schema?

Apache Avro Schema is the "pivot point" for this tool. All schemas are converted
from and to Avro Schema.

Avro Schema ...

- provides a simple, clean, and concise way to define data structures. It is
  quite easy to understand and use.
- is self-contained by design without having or requiring external references.
  Avro Schema can express complex data structure hierarchies spanning multiple
  namespace boundaries all in a single file, which neither JSON Schema nor
  XML Schema nor Protobuf can do.
- can be resolved by code generators and other tools "top-down" since it
  enforces dependencies to be ordered such that no forward-referencing occurs.
- emerged out of the Apache Hadoop ecosystem and is widely used for
  serialization and storage of data and for data exchange between systems.
- supports native and logical types that cover the needs of many business and
  technical use cases.
- can describe the popular JSON data encoding very well and in a way that always
  maps cleanly to a wide range of programming languages and systems. In
  contrast, it's quite easy to inadvertently define a JSON Schema that is very
  difficult to map to a programming language structure.
- is itself expressed as JSON. That makes it easy to parse and generate, which
  is not the case for Protobuf or ASN.1, which require bespoke parsers.

Avro Schema does not support all the bells and whistles of XML Schema or JSON
Schema, but that is a feature, not a bug, as it ensures the portability of the
schemas across different systems and infrastructures. Specifically, Avro Schema
does not support many of the data validation features found in JSON Schema or
XML Schema. There are no `pattern`, `format`, `minimum`, `maximum`, or `required`
keywords in Avro Schema, and Avro does not support conditional validation.

In a system where data originates as XML or JSON described by a validating XML
Schema or JSON Schema, the assumption we make here is that data will be
validated using its native schema language first, and then the Avro Schema will
be used for transformation or transfer or storage.

## Adding CloudEvents columns for database tables

When converting Avro Schema to Kusto Data Table Definition (KQL), T-SQL Table
Definition (SQL), or Parquet Schema, the tool can add special columns for
[CloudEvents](https://cloudevents.io) attributes. CNCF CloudEvents is a
specification for describing event data in a common way.

The rationale for adding such columns to database tables is that messages and
events commonly separate event metadata from the payload data, while that
information is merged when events are projected into a database. The metadata
often carries important context information about the event that is not
contained in the payload itself. Therefore, the tool can add those columns to
the database tables for easy alignment of the message context with the payload
when building event stores.

## Installation

You can install Avrotize from PyPI, [having installed Python 3.11 or later](https://www.python.org/downloads/):

```bash
pip install avrotize
```

## Usage

Avrotize provides several commands for converting schema formats via Avro Schema.

Converting to Avro Schema:

- [`avrotize p2a`´](#convert-proto-schema-to-avro-schema) - Convert Protobuf (2 or 3) schema to Avro schema.
- [`avrotize j2a`](#convert-json-schema-to-avro-schema) - Convert JSON schema to Avro schema.
- [`avrotize x2a`](#convert-xml-schema-xsd-to-avro-schema) - Convert XML schema to Avro schema.
- [`avrotize asn2a`](#convert-asn1-schema-to-avro-schema) - Convert ASN.1 to Avro schema.

Converting from Avro Schema:

- [`avrotize a2p`](#convert-avro-schema-to-proto-schema) - Convert Avro schema to Protobuf 3 schema.
- [`avrotize a2j`](#convert-avro-schema-to-json-schema) - Convert Avro schema to JSON schema.
- [`avrotize a2x`](#convert-avro-schema-to-xml-schema) - Convert Avro schema to XML schema.
- [`avrotize a2k`](#convert-avro-schema-to-kusto-table-declaration) - Convert Avro schema to Kusto table definition.
- [`avrotize a2tsql`](#convert-avro-schema-to-t-sql-table-definition) - Convert Avro schema to T-SQL table definition.
- [`avrotize a2pq`](#convert-avro-schema-to-empty-parquet-file) - Convert Avro schema to empty Parquet file.

### Convert Proto schema to Avro schema

```bash
avrotize p2a --proto <path_to_proto_file> --avsc <path_to_avro_schema_file>
```

Parameters:

- `--proto`: The path to the Protobuf schema file to be converted.
- `--avsc`: The path to the Avro schema file to write the conversion result to.

Conversion notes:

- Proto 2 and Proto 3 syntax are supported.
- Proto package names are mapped to Avro namespaces. The tool does resolve imports
  and consolidates all imported types into a single Avro schema file.
- The tool embeds all 'well-known' Protobuf 3.0 types in Avro format and injects
  them as needed when the respective types are imported. Only the `Timestamp` type is
  mapped to the Avro logical type 'timestamp-millis'. The rest of the well-known
  Protobuf types are kept as Avro record types with the same field names and types.
- Protobuf allows any scalar type as key in a `map`, Avro does not. When converting
  from Proto to Avro, the type information for the map keys is ignored.
- The field numbers in message types are not mapped to the positions of the
  fields in Avro records. The fields in Avro are ordered as they appear in the
  Proto schema. Consequently, the Avro schema also ignores the `extensions` and
  `reserved` keywords in the Proto schema.
- The `optional` keyword results in an Avro field being nullable (union with the
  `null` type), while the `required` keyword results in a non-nullable field.
  The `repeated` keyword results in an Avro field being an array of the field
  type.
- The `oneof` keyword in Proto is mapped to an Avro union type. 
- All `options` in the Proto schema are ignored.


### Convert Avro schema to Proto schema

```bash
avrotize a2p --avsc <path_to_avro_schema_file> --proto <path_to_proto_directory>
```

Parameters:

- `--avsc`: The path to the Avro schema file to be converted.
- `--proto`: The path to the Protobuf schema directory to write the conversion result to.

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

Parameters:

- `--jsons`: The path to the JSON schema file to be converted.
- `--avsc`: The path to the Avro schema file to write the conversion result to.
- `--namespace`: (optional) The namespace to use in the Avro schema if the JSON
  schema does not define a namespace.

Conversion notes:

- [JSON Schema Handling in Avrotize](jsonschema.md)

### Convert Avro schema to JSON schema

```bash
avrotize a2j --avsc <path_to_avro_schema_file> --json <path_to_json_schema_file>
```

Parameters:

- `--avsc`: The path to the Avro schema file to be converted.
- `--json`: The path to the JSON schema file to write the conversion result to.

Conversion notes:

- [JSON Schema Handling in Avrotize](jsonschema.md)

### Convert XML Schema (XSD) to Avro schema

```bash
avrotize x2a --xsd <path_to_xsd_file> --avsc <path_to_avro_schema_file> [--namespace <avro_schema_namespace>]
```

Parameters:

- `--xsd`: The path to the XML schema file to be converted.
- `--avsc`: The path to the Avro schema file to write the conversion result to.
- `--namespace`: (optional) The namespace to use in the Avro schema if the XML
  schema does not define a namespace.

Conversion notes:

- All XML Schema constructs are mapped to Avro record types with fields, whereby
  **both**, elements and attributes, become fields in the record. XML is therefore
  flattened into fields and this aspect of the structure is not preserved.
- Avro does not support `xsd:any` as Avro does not support arbitrary typing and
  must always use a named type. The tool will map `xsd:any` to a field `any`
  typed as a union that allows scalar values or two levels of array and/or map
  nesting.
- `simpleType` declarations that define enums are mapped to `enum` types in Avro.
  All other facets are ignored and simple types are mapped to the corresponding
  Avro type.
- `complexType` declarations that have simple content where a base type is augmented
  with attributes is mapped to a record type in Avro. Any other facets defined on
  the complex type are ignored.
- If the schema defines a single root element, the tool will emit a single Avro
  record type. If the schema defines multiple root elements, the tool will emit a
  union of record types, each corresponding to a root element.

## Convert Avro schema to XML schema

```bash
avrotize a2x --avsc <path_to_avro_schema_file> --xsd <path_to_xsd_schema_file>
```

Parameters:

- `--avsc`: The path to the Avro schema file to be converted.
- `--xsd`: The path to the XML schema file to write the conversion result to.

Conversion notes:

- Avro record types are mapped to XML Schema complex types with elements.
- Avro enum types are mapped to XML Schema simple types with restrictions.
- Avro logical types are mapped to XML Schema simple types with restrictions
  where required.
- Avro unions are mapped to standalone XSD simple type definitions with a
  union restriction if all union types are primitives.
- Avro unions with complex types are resolved into distinct types for
  each option that are then joined with a choice.

## Convert ASN.1 schema to Avro schema

```bash
avrotize asn2a --asn <path_to_asn1_schema_file>[,<path_to_asn1_schema_file>,...]  --avsc <path_to_avro_schema_file>
```

Parameters:

- `--asn`: The path to the ASN.1 schema file to be converted. The tool supports
  multiple files in a comma-separated list.
- `--avsc`: The path to the Avro schema file to write the conversion result to.

Conversion notes:

- All ASN.1 types are mapped to Avro record types, enums, and unions. Avro does
  not support the same level of nesting of types as ASN.1, the tool will map
  the types to the best fit.
- The tool will map the following ASN.1 types to Avro types:
  - `SEQUENCE` and `SET` are mapped to Avro record types.
  - `CHOICE` is mapped to an Avro record types with all fields being optional. While
     the `CHOICE` type technically corresponds to an Avro union, the ASN.1 type
     has different named fields for each option, which is not a feature of Avro unions.
  - `OBJECT IDENTIFIER` is mapped to an Avro string type.
  - `ENUMERATED` is mapped to an Avro enum type.
  - `SEQUENCE OF` and `SET OF` are mapped to Avro array type.
  - `BIT STRING` is mapped to Avro bytes type.
  - `OCTET STRING` is mapped to Avro bytes type.
  - `INTEGER` is mapped to Avro long type.
  - `REAL` is mapped to Avro double type.
  - `BOOLEAN` is mapped to Avro boolean type.
  - `NULL` is mapped to Avro null type.
  - `UTF8String`, `PrintableString`, `IA5String`, `BMPString`, `NumericString`, `TeletexString`,
    `VideotexString`, `GraphicString`, `VisibleString`, `GeneralString`, `UniversalString`,
    `CharacterString`, `T61String` are all mapped to Avro string type.
  - All other ASN.1 types are mapped to Avro string type.
- The ability to parse ASN.1 schema files is limited and the tool may not be able
  to parse all ASN.1 files. The tool is based on the Python asn1tools package and
  is limited to that package's capabilities.

### Convert Avro schema to Kusto table declaration

```bash
avrotize a2k --avsc <path_to_avro_schema_file> --kusto <path_to_kusto_kql_file> [--record-type <record_type>] [--emit-cloudevents-columns]
```

Parameters:

- `--avsc`: The path to the Avro schema file to be converted.
- `--kusto`: The path to the Kusto KQL file to write the conversion result to.
- `--record-type`: (optional) The name of the Avro record type to convert to a Kusto table.
- `--emit-cloudevents-columns`: (optional) If set, the tool will add
  [CloudEvents](https://cloudevents.io) attribute columns to the table: `__id`,
  `__source`, `__subject`, `__type`, and `__time`.

Conversion notes:

- Only the Avro `record` type can be mapped to a Kusto table. If the Avro schema
  contains other types (like `enum` or `array`), the tool will ignore them.
- Only the first `record` type in the Avro schema is converted to a Kusto table.
  If the Avro schema contains other `record` types, they will be ignored. The
  `--record-type` option can be used to specify which `record` type to convert.
- The fields of the record are mapped to columns in the Kusto table. Fields that
  are records or arrays or maps are mapped to columns of type `dynamic` in the
  Kusto table.

### Convert Avro schema to T-SQL table definition

```bash
avrotize a2tsql --avsc <path_to_avro_schema_file> --tsql <path_to_sql_file> [--record-type <record_type>] [--emit-cloudevents-columns]
```

Parameters:

- `--avsc`: The path to the Avro schema file to be converted.
- `--tsql`: The path to the T-SQL file to write the conversion result to.
- `--record-type`: (optional) The name of the Avro record type to convert to a T-SQL table.
- `--emit-cloudevents-columns`: (optional) If set, the tool will add
  [CloudEvents](https://cloudevents.io) attribute columns to the table: `__id`,
  `__source`, `__subject`, `__type`, and `__time`.

Conversion notes:

- Only the Avro `record` type can be mapped to a T-SQL table. If the Avro schema
  contains other types (like `enum` or `array`), the tool will ignore them.
- Only the first `record` type in the Avro schema is converted to a T-SQL table.
  If the Avro schema contains other `record` types, they will be ignored. The
  `--record-type` option can be used to specify which `record` type to convert.
- The fields of the record are mapped to columns in the T-SQL table. Fields of
  type `record` or `array` or `map` are mapped to columns of type `varchar(max)` in
  the T-SQL table and it's assumed for them to hold JSON data.
- The emitted script sets extended properties to the columns with the Avro schema
  definition of the field in a JSON format. This allows for easy introspection of
  the serialized Avro schema in the field definition.

## Convert Avro schema to empty Parquet file

```bash
avrotize a2pq --avsc <path_to_avro_schema_file> --parquet <path_to_parquet_schema_file> [--record-type <record-type-from-avro>] [--emit-cloudevents-columns]
```

Parameters:

- `--avsc`: The path to the Avro schema file to be converted.
- `--parquet`: The path to the Parquet schema file to write the conversion result to.
- `--record-type`: (optional) The name of the Avro record type to convert to a Parquet schema.
- `--emit-cloudevents-columns`: (optional) If set, the tool will add
  [CloudEvents](https://cloudevents.io) attribute columns to the Parquet schema:
  `__id`, `__source`, `__subject`, `__type`, and `__time`.

Conversion notes:

- The emitted Parquet file contains only the schema, no data rows.
- The tool only supports writing Parquet files for Avro schema that describe a
  single `record` type. If the Avro schema contains a top-level union, the
  `--record-type` option must be used to specify which record type to emit.
- The fields of the record are mapped to columns in the Parquet file. Array and
  record fields are mapped to Parquet nested types. Avro type unions are mapped
  to structures, not to Parquet unions since those are not supported by the
  PyArrow library used here.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Avrotize is released under the Apache License. See the LICENSE file for more details.
