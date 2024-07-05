# Avrotize

Avrotize is a ["Rosetta Stone"](https://en.wikipedia.org/wiki/Rosetta_Stone) for data structure definitions, allowing you to convert between numerous data and database schema formats and to generate code for different programming languages.

It is, for instance, a well-documented and predictable converter and code generator for data structures originally defined in JSON Schema (of arbitrary complexity).

The tool leans on the Apache Avro-derived [Avrotize Schema](specs/avrotize-schema.md) as its schema model.

- Programming languages: Python, C#, Java, TypeScript, JavaScript, Rust, Go, C++
- SQL Databases: MySQL, MariaDB, PostgreSQL, SQL Server, Oracle, SQLite, BigQuery, Snowflake, Redshift, DB2
- Other databases: KQL/Kusto, MongoDB, Cassandra, Redis, Elasticsearch, DynamoDB, CosmosDB
- Data schema formats: Avro, JSON Schema, XML Schema (XSD), Protocol Buffers 2 and 3, ASN.1, Apache Parquet

## Installation

You can install Avrotize from PyPI, [having installed Python 3.10 or later](https://www.python.org/downloads/):

```bash
pip install avrotize
```

## Usage

Avrotize provides several commands for converting schema formats via Avrotize Schema.

Converting to Avrotize Schema:

- [`avrotize p2a`](#convert-proto-schema-to-avrotize-schema) - Convert Protobuf (2 or 3) schema to Avrotize Schema.
- [`avrotize j2a`](#convert-json-schema-to-avrotize-schema) - Convert JSON schema to Avrotize Schema.
- [`avrotize x2a`](#convert-xml-schema-xsd-to-avrotize-schema) - Convert XML schema to Avrotize Schema.
- [`avrotize asn2a`](#convert-asn1-schema-to-avrotize-schema) - Convert ASN.1 to Avrotize Schema.
- [`avrotize k2a`](#convert-kusto-table-definition-to-avrotize-schema) - Convert Kusto table definitions to Avrotize Schema.
- [`avrotize pq2a`](#convert-parquet-schema-to-avrotize-schema) - Convert Parquet schema to Avrotize Schema.
- [`avrotize csv2a`](#convert-csv-file-to-avrotize-schema) - Convert CSV file to Avrotize Schema.
- [`avrotize kstruct2a`](#convert-kafka-connect-schema-to-avrotize-schema) - Convert Kafka Connect Schema to Avrotize Schema.

Converting from Avrotize Schema:

- [`avrotize a2p`](#convert-avrotize-schema-to-proto-schema) - Convert Avrotize Schema to Protobuf 3 schema.
- [`avrotize a2j`](#convert-avrotize-schema-to-json-schema) - Convert Avrotize Schema to JSON schema.
- [`avrotize a2x`](#convert-avrotize-schema-to-xml-schema) - Convert Avrotize Schema to XML schema.
- [`avrotize a2k`](#convert-avrotize-schema-to-kusto-table-declaration) - Convert Avrotize Schema to Kusto table definition.
- [`avrotize a2sql`](#convert-avrotize-schema-to-sql-table-definition) - Convert Avrotize Schema to SQL table definition.
- [`avrotize a2pq`](#convert-avrotize-schema-to-empty-parquet-file) - Convert Avrotize Schema to Parquet or Iceberg schema.
- [`avrotize a2ib`](#convert-avrotize-schema-to-iceberg-schema) - Convert Avrotize Schema to Iceberg schema.
- [`avrotize a2mongo`](#convert-avrotize-schema-to-mongodb-schema) - Convert Avrotize Schema to MongoDB schema.
- [`avrotize a2cassandra`](#convert-avrotize-schema-to-cassandra-schema) - Convert Avrotize Schema to Cassandra schema.
- [`avrotize a2es`](#convert-avrotize-schema-to-elasticsearch-schema) - Convert Avrotize Schema to Elasticsearch schema.
- [`avrotize a2dynamodb`](#convert-avrotize-schema-to-dynamodb-schema) - Convert Avrotize Schema to DynamoDB schema.
- [`avrotize a2cosmos`](#convert-avrotize-schema-to-cosmosdb-schema) - Convert Avrotize Schema to CosmosDB schema.
- [`avrotize a2couchdb`](#convert-avrotize-schema-to-couchdb-schema) - Convert Avrotize Schema to CouchDB schema.
- [`avrotize a2firebase`](#convert-avrotize-schema-to-firebase-schema) - Convert Avrotize Schema to Firebase schema.
- [`avrotize a2hbase`](#convert-avrotize-schema-to-hbase-schema) - Convert Avrotize Schema to HBase schema.
- [`avrotize a2neo4j`](#convert-avrotize-schema-to-neo4j-schema) - Convert Avrotize Schema to Neo4j schema.
- [`avrotize a2dp`](#convert-avrotize-schema-to-datapackage-schema) - Convert Avrotize Schema to Datapackage schema.
- [`avrotize a2md`](#convert-avrotize-schema-to-markdown-documentation) - Convert Avrotize Schema to Markdown documentation.

Generate code from Avrotize Schema:

- [`avrotize a2cs`](#convert-avrotize-schema-to-c-classes) - Generate C# code from Avrotize Schema.
- [`avrotize a2java`](#convert-avrotize-schema-to-java-classes) - Generate Java code from Avrotize Schema.
- [`avrotize a2py`](#convert-avrotize-schema-to-python-classes) - Generate Python code from Avrotize Schema.
- [`avrotize a2ts`](#convert-avrotize-schema-to-typescript-classes) - Generate TypeScript code from Avrotize Schema.
- [`avrotize a2js`](#convert-avrotize-schema-to-javascript-classes) - Generate JavaScript code from Avrotize Schema.
- [`avrotize a2cpp`](#convert-avrotize-schema-to-c-classes) - Generate C++ code from Avrotize Schema.
- [`avrotize a2go`](#convert-avrotize-schema-to-go-classes) - Generate Go code from Avrotize Schema.
- [`avrotize a2rust`](#convert-avrotize-schema-to-rust-classes) - Generate Rust code from Avrotize Schema.

Other commands:

- [`avrotize pcf`](#create-the-parsing-canonical-form-pcf-of-an-avrotize-schema) - Create the Parsing Canonical Form (PCF) of an Avrotize Schema.

## Overview

You can use Avrotize to convert between Avro/Avrotize Schema and other schema formats like JSON Schema, XML Schema (XSD), Protocol Buffers (Protobuf), ASN.1, and database schema formats like Kusto Data Table Definition (KQL) and SQL Table Definition. That means you can also convert from JSON Schema to Protobuf going via Avrotize Schema.

You can also generate C#, Java, TypeScript, JavaScript, and Python code from Avrotize Schema documents. The difference to the native Avro tools is that Avrotize can emit data classes without Avro library dependencies and, optionally, with annotations for JSON serialization libraries like Jackson or System.Text.Json.

The tool does not convert data (instances of schemas), only the data structure definitions.

Mind that the primary objective of the tool is the conversion of schemas that describe data structures used in applications, databases, and message systems. While the project's internal tests do cover a lot of ground, it is nevertheless not a primary goal of the tool to convert every complex document schema like those used for devops pipeline or system configuration files.

## Why?

Data structure definitions are an essential part of data exchange, serialization, and storage. They define the shape and type of data, and they are foundational for tooling and libraries for working with the data. Nearly all data schema languages are coupled to a specific data exchange or storage format, locking the definitions to that format.

Avrotize is designed as a tool to "unlock" data definitions from JSON Schema or XML Schema and make them usable in other contexts. The intent is also to lay a foundation for transcoding data from one format to another, by translating the schema definitions as accurately as possible into the schema model of the target format's schema. The transcoding of the data itself requires separate tools that are beyond the scope of this project.

The use of the term "data structure definition" and not "data object definition" is quite intentional. The focus of the tool is on data structures that can be used for messaging and eventing payloads, for data serialization, and for database tables, with the goal that those structures can be mapped cleanly from and to common programming language types.

Therefore, Avrotize intentionally ignores common techniques to model object-oriented inheritance. For instance, when converting from JSON Schema, all content from `allOf` expressions is merged into a single record type rather than trying to model the inheritance tree in Avro.

## Avrotize Schema

Avrotize Schema is a schema model that is a full superset of the popular Apache Avro Schema model. Avrotize Schema is the "pivot point" for this tool. All schemas are converted from and to Avrotize Schema.

Since Avrotize Schema is a superset of Avro Schema and uses its extensibility features, every Avrotize Schema is also a valid Avro Schema and vice versa.

Why did we pick Avro Schema as the foundational schema model?

Avro Schema ...

- provides a simple, clean, and concise way to define data structures. It is quite easy to understand and use.
- is self-contained by design without having or requiring external references. Avro Schema can express complex data structure hierarchies spanning multiple namespace boundaries all in a single file, which neither JSON Schema nor XML Schema nor Protobuf can do.
- can be resolved by code generators and other tools "top-down" since it enforces dependencies to be ordered such that no forward-referencing occurs.
- emerged out of the Apache Hadoop ecosystem and is widely used for serialization and storage of data and for data exchange between systems.
- supports native and logical types that cover the needs of many business and technical use cases.
- can describe the popular JSON data encoding very well and in a way that always maps cleanly to a wide range of programming languages and systems. In contrast, it's quite easy to inadvertently define a JSON Schema that is very difficult to map to a programming language structure.
- is itself expressed as JSON. That makes it easy to parse and generate, which is not the case for Protobuf or ASN.1, which require bespoke parsers.

> It needs to be noted here that while Avro Schema is great for defining data structures, and data classes generated from Avro Schema using this tool or other tools can be used to with the most popular JSON serialization libraries, the Apache Avro project's own JSON encoding has fairly grave interoperability issues with common usage of JSON. Avrotize defines an alternate JSON encoding

 in [`avrojson.md`](specs/avrojson.md).

Avro Schema does not support all the bells and whistles of XML Schema or JSON Schema, but that is a feature, not a bug, as it ensures the portability of the schemas across different systems and infrastructures. Specifically, Avro Schema does not support many of the data validation features found in JSON Schema or XML Schema. There are no `pattern`, `format`, `minimum`, `maximum`, or `required` keywords in Avro Schema, and Avro does not support conditional validation.

In a system where data originates as XML or JSON described by a validating XML Schema or JSON Schema, the assumption we make here is that data will be validated using its native schema language first, and then the Avro Schema will be used for transformation or transfer or storage.

## Adding CloudEvents columns for database tables

When converting Avrotize Schema to Kusto Data Table Definition (KQL), SQL Table Definition, or Parquet Schema, the tool can add special columns for [CloudEvents](https://cloudevents.io) attributes. CNCF CloudEvents is a specification for describing event data in a common way.

The rationale for adding such columns to database tables is that messages and events commonly separate event metadata from the payload data, while that information is merged when events are projected into a database. The metadata often carries important context information about the event that is not contained in the payload itself. Therefore, the tool can add those columns to the database tables for easy alignment of the message context with the payload when building event stores.

### Convert Proto schema to Avrotize Schema

```bash
avrotize p2a <path_to_proto_file> [--out <path_to_avro_schema_file>]
```

Parameters:

- `<path_to_proto_file>`: The path to the Protobuf schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Avrotize Schema file to write the conversion result to. If omitted, the output is directed to stdout.

Conversion notes:

- Proto 2 and Proto 3 syntax are supported.
- Proto package names are mapped to Avro namespaces. The tool does resolve imports and consolidates all imported types into a single Avrotize Schema file.
- The tool embeds all 'well-known' Protobuf 3.0 types in Avro format and injects them as needed when the respective types are imported. Only the `Timestamp` type is mapped to the Avro logical type 'timestamp-millis'. The rest of the well-known Protobuf types are kept as Avro record types with the same field names and types.
- Protobuf allows any scalar type as key in a `map`, Avro does not. When converting from Proto to Avro, the type information for the map keys is ignored.
- The field numbers in message types are not mapped to the positions of the fields in Avro records. The fields in Avro are ordered as they appear in the Proto schema. Consequently, the Avrotize Schema also ignores the `extensions` and `reserved` keywords in the Proto schema.
- The `optional` keyword results in an Avro field being nullable (union with the `null` type), while the `required` keyword results in a non-nullable field. The `repeated` keyword results in an Avro field being an array of the field type.
- The `oneof` keyword in Proto is mapped to an Avro union type.
- All `options` in the Proto schema are ignored.

### Convert Avrotize Schema to Proto schema

```bash
avrotize a2p <path_to_avro_schema_file> [--out <path_to_proto_directory>] [--naming <naming_mode>] [--allow-optional]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Protobuf schema directory to write the conversion result to. If omitted, the output is directed to stdout.
- `--naming`: (optional) Type naming convention. Choices are `snake`, `camel`, `pascal`.
- `--allow-optional`: (optional) Enable support for 'optional' fields.

Conversion notes:

- Avro namespaces are resolved into distinct proto package definitions. The tool will create a new `.proto` file with the package definition and an `import` statement for each namespace found in the Avrotize Schema.
- Avro type unions `[]` are converted to `oneof` expressions in Proto. Avro allows for maps and arrays in the type union, whereas Proto only supports scalar types and message type references. The tool will therefore emit message types containing a single array or map field for any such case and add it to the containing type, and will also recursively resolve further unions in the array and map values.
- The sequence of fields in a message follows the sequence of fields in the Avro record. When type unions need to be resolved into `oneof` expressions, the alternative fields need to be assigned field numbers, which will shift the field numbers for any subsequent fields.

### Convert JSON schema to Avrotize Schema

```bash
avrotize j2a <path_to_json_schema_file> [--out <path_to_avro_schema_file>] [--namespace <avro_schema_namespace>] [--split-top-level-records]
```

Parameters:

- `<path_to_json_schema_file>`: The path to the JSON schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Avrotize Schema file to write the conversion result to. If omitted, the output is directed to stdout.
- `--namespace`: (optional) The namespace to use in the Avrotize Schema if the JSON schema does not define a namespace.
- `--split-top-level-records`: (optional) Split top-level records into separate files.

Conversion notes:

- [JSON Schema Handling in Avrotize](specs/jsonschema.md)

### Convert Avrotize Schema to JSON schema

```bash
avrotize a2j <path_to_avro_schema_file> [--out <path_to_json_schema_file>] [--naming <naming_mode>]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the JSON schema file to write the conversion result to. If omitted, the output is directed to stdout.
- `--naming`: (optional) Type naming convention. Choices are `snake`, `camel`, `pascal`, `default`.

Conversion notes:

- [JSON Schema Handling in Avrotize](specs/jsonschema.md)

### Convert XML Schema (XSD) to Avrotize Schema

```bash
avrotize x2a <path_to_xsd_file> [--out <path_to_avro_schema_file>] [--namespace <avro_schema_namespace>]
```

Parameters:

- `<path_to_xsd_file>`: The path to the XML schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Avrotize Schema file to write the conversion result to. If omitted, the output is directed to stdout.
- `--namespace`: (optional) The namespace to use in the Avrotize Schema if the XML schema does not define a namespace.

Conversion notes:

- All XML Schema constructs are mapped to Avro record types with fields, whereby **both**, elements and attributes, become fields in the record. XML is therefore flattened into fields and this aspect of the structure is not preserved.
- Avro does not support `xsd:any` as Avro does not support arbitrary typing and must always use a named type. The tool will map `xsd:any` to a field `any` typed as a union that allows scalar values or two levels of array and/or map nesting.
- `simpleType` declarations that define enums are mapped to `enum` types in Avro. All other facets are ignored and simple types are mapped to the corresponding Avro type.
- `complexType` declarations that have simple content where a base type is augmented with attributes is mapped to a record type in Avro. Any other facets defined on the complex type are ignored.
- If the schema defines a single root element, the tool will emit a single Avro record type. If the schema defines multiple root elements, the tool will emit a union of record types, each corresponding to a root element.
- All fields in the resulting Avrotize Schema are annotated with an `xmlkind` extension attribute that indicates whether the field was an `element` or an `attribute` in the XML schema.

### Convert Avrotize Schema to XML schema

```bash
avrotize a2x <path_to_avro_schema_file> [--out <path_to_xsd_schema_file>] [--namespace <target_namespace>]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the XML schema file to write the conversion result to. If omitted, the output is directed to stdout.
- `--namespace`: (optional) Target namespace for the XSD schema.

Conversion notes:

- Avro record types are mapped to XML Schema complex types with elements.
- Avro enum types are mapped to XML Schema simple types with restrictions.
- Avro logical types are mapped to XML Schema simple types with restrictions where required.
- Avro unions are mapped to standalone XSD simple type definitions with a union restriction if all union types are primitives.
- Avro unions with complex types are resolved into distinct types for each option that are

 then joined with a choice.

### Convert ASN.1 schema to Avrotize Schema

```bash
avrotize asn2a <path_to_asn1_schema_file>[,<path_to_asn1_schema_file>,...] [--out <path_to_avro_schema_file>]
```

Parameters:

- `<path_to_asn1_schema_file>`: The path to the ASN.1 schema file to be converted. The tool supports multiple files in a comma-separated list. If omitted, the file is read from stdin.
- `--out`: The path to the Avrotize Schema file to write the conversion result to. If omitted, the output is directed to stdout.

Conversion notes:

- All ASN.1 types are mapped to Avro record types, enums, and unions. Avro does not support the same level of nesting of types as ASN.1, the tool will map the types to the best fit.
- The tool will map the following ASN.1 types to Avro types:
  - `SEQUENCE` and `SET` are mapped to Avro record types.
  - `CHOICE` is mapped to an Avro record types with all fields being optional. While the `CHOICE` type technically corresponds to an Avro union, the ASN.1 type has different named fields for each option, which is not a feature of Avro unions.
  - `OBJECT IDENTIFIER` is mapped to an Avro string type.
  - `ENUMERATED` is mapped to an Avro enum type.
  - `SEQUENCE OF` and `SET OF` are mapped to Avro array type.
  - `BIT STRING` is mapped to Avro bytes type.
  - `OCTET STRING` is mapped to Avro bytes type.
  - `INTEGER` is mapped to Avro long type.
  - `REAL` is mapped to Avro double type.
  - `BOOLEAN` is mapped to Avro boolean type.
  - `NULL` is mapped to Avro null type.
  - `UTF8String`, `PrintableString`, `IA5String`, `BMPString`, `NumericString`, `TeletexString`, `VideotexString`, `GraphicString`, `VisibleString`, `GeneralString`, `UniversalString`, `CharacterString`, `T61String` are all mapped to Avro string type.
  - All other ASN.1 types are mapped to Avro string type.
- The ability to parse ASN.1 schema files is limited and the tool may not be able to parse all ASN.1 files. The tool is based on the Python asn1tools package and is limited to that package's capabilities.

### Convert Kusto table definition to Avrotize Schema

```bash
avrotize k2a --kusto-uri <kusto_cluster_uri> --kusto-database <kusto_database> [--out <path_to_avro_schema_file>] [--emit-cloudevents-xregistry]
```

Parameters:

- `--kusto-uri`: The URI of the Kusto cluster to connect to.
- `--kusto-database`: The name of the Kusto database to read the table definitions from.
- `--out`: The path to the Avrotize Schema file to write the conversion result to. If omitted, the output is directed to stdout.
- `--emit-cloudevents-xregistry`: (optional) See discussion below.

Conversion notes:

- The tool directly connects to the Kusto cluster and reads the table definitions from the specified database. The tool will convert all tables in the database to Avro record types, returned in a top-level type union.
- Connecting to the Kusto cluster leans on the same authentication mechanisms as the Azure CLI. The tool will use the same authentication context as the Azure CLI if it is installed and authenticated.
- The tool will map the Kusto column types to Avro types as follows:
  - `bool` is mapped to Avro boolean type.
  - `datetime` is mapped to Avro long type with logical type `timestamp-millis`.
  - `decimal` is mapped to a logical Avro type with the `logicalType` set to `decimal` and the `precision` and `scale` set to the values of the `decimal` type in Kusto.
  - `guid` is mapped to Avro string type.
  - `int` is mapped to Avro int type.
  - `long` is mapped to Avro long type.
  - `real` is mapped to Avro double type.
  - `string` is mapped to Avro string type.
  - `timespan` is mapped to a logical Avro type with the `logicalType` set to `duration`.
- For `dynamic` columns, the tool will sample the data in the table to determine the structure of the dynamic column. The tool will map the dynamic column to an Avro record type with fields that correspond to the fields found in the dynamic column. If the dynamic column contains nested dynamic columns, the tool will recursively map those to Avro record types. If records with conflicting structures are found in the dynamic column, the tool will emit a union of record types for the dynamic column.
- If the `--emit-cloudevents-xregistry` option is set, the tool will emit an [xRegistry](http://xregistry.io) registry manifest file with a CloudEvent message definition for each table in the Kusto database and a separate Avro Schema for each table in the embedded schema registry. If one or more tables are found to contain CloudEvent data (as indicated by the presence of the CloudEvents attribute columns), the tool will inspect the content of the `type` (or `__type` or `__type`) columns to determine which CloudEvent types have been stored in the table and will emit a CloudEvent definition and schema for each unique type.

### Convert Avrotize Schema to Kusto table declaration

```bash
avrotize a2k <path_to_avro_schema_file> [--out <path_to_kusto_kql_file>] [--record-type <record_type>] [--emit-cloudevents-columns] [--emit-cloudevents-dispatch]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Kusto KQL file to write the conversion result to. If omitted, the output is directed to stdout.
- `--record-type`: (optional) The name of the Avro record type to convert to a Kusto table.
- `--emit-cloudevents-columns`: (optional) If set, the tool will add [CloudEvents](https://cloudevents.io) attribute columns to the table: `___id`, `___source`, `___subject`, `___type`, and `___time`.
- `--emit-cloudevents-dispatch`: (optional) If set, the tool will add a table named `_cloudevents_dispatch` to the script or database, which serves as an ingestion and dispatch table for CloudEvents. The table has columns for the core CloudEvents attributes and a `data` column that holds the CloudEvents data. For each table in the Avrotize Schema, the tool will create an update policy that maps events whose `type` attribute matches the Avro type name to the respective table.

Conversion notes:

- Only the Avro `record` type can be mapped to a Kusto table. If the Avrotize Schema contains other types (like `enum` or `array`), the tool will ignore them.
- Only the first `record` type in the Avrotize Schema is converted to a Kusto table. If the Avrotize Schema contains other `record` types, they will be ignored. The `--record-type` option can be used to specify which `record` type to convert.
- The fields of the record are mapped to columns in the Kusto table. Fields that are records or arrays or maps are mapped to columns of type `dynamic` in the Kusto table.

### Convert Avrotize Schema to SQL Schema

```bash
avrotize a2sql [input] --out <path_to_sql_script> --dialect <dialect>
```

Parameters:

- `input`: The path to the Avrotize schema file to be converted (or read from stdin if omitted).
- `--out`: The path to the SQL script file to write the conversion result to.
- `--dialect`: The SQL dialect (database type) to target. Supported dialects include:
  - `mysql`, `mariadb`, `postgres`, `sqlserver`, `oracle`, `sqlite`, `bigquery`, `snowflake`, `redshift`, `db2`
- `--emit-cloudevents-columns`: (Optional) Add CloudEvents columns to the SQL table.

For detailed conversion rules and type mappings for each SQL dialect, refer to the [SQL Conversion Notes](sqlcodegen.md) document.

### Convert Avrotize Schema to MongoDB schema

```bash
avrotize a2mongo <path_to_avro_schema_file> [--out <path_to_mongodb_schema>] [--emit-cloudevents-columns]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the MongoDB schema file to write the conversion result to.
- `--emit-cloudevents-columns`: (optional) If set, the tool will add [CloudEvents](https://cloudevents.io) attribute columns to the MongoDB schema.

Conversion notes:

- The fields of the Avro record type are mapped to fields in the MongoDB schema. Fields that are records or arrays or maps are mapped to fields of type `object`.
- The emitted MongoDB schema file is a JSON file that can be used with MongoDB's `mongoimport` tool to create a collection with the specified schema.

Here are the "Convert ..." sections for the newly added commands:

### Convert Avrotize schema to Cassandra schema

```bash
avrotize a2cassandra [input] --out <output_directory> [--emit-cloudevents-columns]
```

- `input`: Path to the Avrotize schema file (or read from stdin if omitted).
- `--out`: Output path for the Cassandra schema (required).
- `--emit-cloudevents-columns`: Add CloudEvents columns to the Cassandra schema (optional, default: false).

Refer to the detailed conversion notes for Cassandra in the [NoSQL Conversion Notes](nosqlcodegen.md).

### Convert Avrotize schema to DynamoDB schema

```bash
avrotize a2dynamodb [input] --out <output_directory> [--emit-cloudevents-columns]
```

- `input`: Path to the Avrotize schema file (or read from stdin if omitted).
- `--out`: Output path for the DynamoDB schema (required).
- `--emit-cloudevents-columns`: Add CloudEvents columns to the DynamoDB schema (optional, default: false).

Refer to the detailed conversion notes for DynamoDB in the [NoSQL Conversion Notes](nosqlcodegen.md).

### Convert Avrotize schema to Elasticsearch schema

```bash
avrotize a2es [input] --out <output_directory> [--emit-cloudevents-columns]
```

- `input`: Path to the Avrotize schema file (or read from stdin if omitted).
- `--out`: Output path for the Elasticsearch schema (required).
- `--emit-cloudevents-columns`: Add CloudEvents columns to the Elasticsearch schema (optional, default: false).

Refer to the detailed conversion notes for Elasticsearch in the [NoSQL Conversion Notes](nosqlcodegen.md).

### Convert Avrotize schema to CouchDB schema

```bash
avrotize a2couchdb [input] --out <output_directory> [--emit-cloudevents-columns]
```

- `input`: Path to the Avrotize schema file (or read from stdin if omitted).
- `--out`: Output path for the CouchDB schema (required).
- `--emit-cloudevents-columns`: Add CloudEvents columns to the CouchDB schema (optional, default: false).

Refer to the detailed conversion notes for CouchDB in the [NoSQL Conversion Notes](nosqlcodegen.md).

### Convert Avrotize schema to Neo4j schema

```bash
avrotize a2neo4j [input] --out <output_directory> [--emit-cloudevents-columns]
```

- `input`: Path to the Avrotize schema file (or read from stdin if omitted).
- `--out`: Output path for the Neo4j schema (required).
- `--emit-cloudevents-columns`: Add CloudEvents columns to the Neo4j schema (optional, default: false).

Refer to the detailed conversion notes for Neo4j in the [NoSQL Conversion Notes](nosqlcodegen.md).

### Convert Avrotize schema to Firebase schema

```bash
avrotize a2firebase [input] --out <output_directory> [--emit-cloudevents-columns]
```

- `input`: Path to the Avrotize schema file (or read from stdin if omitted).
- `--out`: Output path for the Firebase schema (required).
- `--emit-cloudevents-columns`: Add CloudEvents columns to the Firebase schema (optional, default: false).

Refer to the detailed conversion notes for Firebase in the [NoSQL Conversion Notes](nosqlcodegen.md).

### Convert Avrotize schema to CosmosDB schema

```bash
avrotize a2cosmos [input] --out <output_directory> [--emit-cloudevents-columns]
```

- `input`: Path to the Avrotize schema file (or read from stdin if omitted).
- `--out`: Output path for the CosmosDB schema (required).
- `--emit-cloudevents-columns`: Add CloudEvents columns to the CosmosDB schema (optional, default: false).

Refer to the detailed conversion notes for CosmosDB in the [NoSQL Conversion Notes](nosqlcodegen.md).

### Convert Avrotize schema to HBase schema

```bash
avrotize a2hbase [input] --out <output_directory> [--emit-cloudevents-columns]
```

- `input`: Path to the Avrotize schema file (or read from stdin if omitted).
- `--out`: Output path for the HBase schema (required).
- `--emit-cloudevents-columns`: Add CloudEvents columns to the HBase schema (optional, default: false).

Refer to the detailed conversion notes for HBase in the [NoSQL Conversion Notes](nosqlcodegen.md).

### Convert Avrotize Schema to empty Parquet file

```bash
avrotize a2pq <path_to_avro_schema_file> [--out <path_to_parquet_schema_file>] [--record-type <record-type-from-avro>] [--emit-cloudevents-columns]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Parquet schema file to write the conversion result to. If omitted, the output is directed to stdout.
- `--record-type`: (optional) The name of the Avro record type to convert to a Parquet schema.
- `--emit-cloudevents-columns`: (optional) If set, the tool will add [CloudEvents](https://cloudevents.io) attribute columns to the Parquet schema: `__id`, `__source`, `__subject`, `__type`, and `__time`.

Conversion notes:

- The emitted Parquet file contains only the schema, no data rows.
- The tool only supports writing Parquet files for Avrotize Schema that describe a single `record` type. If the Avrotize Schema contains a top-level union, the `--record-type` option must be used to specify which record type to emit.
- The fields of the record are mapped to columns in the Parquet file. Array and record fields are mapped to Parquet nested types. Avro type unions are mapped to structures, not to Parquet unions since those are not supported by the PyArrow library used here.

### Convert Avrotize Schema to Iceberg schema

```bash
avrotize a2ib <path_to_avro_schema_file> [--out <path_to_iceberg_schema_file>] [--record-type <record-type-from-avro>] [--emit-cloudevents-columns]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Iceberg schema file to write the conversion result to. If omitted, the output is directed to stdout.
- `--record-type`: (optional) The name of the Avro record type to convert to an Iceberg schema.
- `--emit-cloudevents-columns`: (optional) If set, the tool will add [CloudEvents](https://cloudevents.io) attribute columns to the Iceberg schema: `__id`, `__source`, `__subject`, `__type`, and `__time`.

Conversion notes:

- The emitted Iceberg file contains only the schema, no data rows.
- The tool only supports writing Iceberg files for Avrotize Schema that describe a single `record` type. If the Avrotize Schema contains a top-level union, the `--record-type` option must be used to specify which record type to emit.
- The fields of the record are mapped to columns in the Iceberg file. Array and record fields are mapped to Iceberg nested types. Avro type unions are mapped to structures, not to Iceberg unions since those are not supported by the PyArrow library used here.

### Convert Parquet schema to Avrotize Schema

```bash
avrotize pq2a <path_to_parquet_file> [--out <path_to_avro_schema_file>] [--namespace <avro_schema_namespace>]
```

Parameters:

- `<path_to_parquet_file>`: The path to the Parquet file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Avrotize Schema file to write the conversion result to. If omitted, the output is directed to stdout.
- `--namespace`: (optional) The namespace to use in the Avrotize Schema if the Parquet file does not define a namespace.

Conversion notes:

- The tool reads the schema from the Parquet file and converts it to Avrotize Schema. The data in the Parquet file is not read or converted.
- The fields of the Parquet schema are mapped to fields in the Avrotize Schema. Nested fields are mapped to nested records in the Avrotize Schema.

### Convert CSV file to Avrotize Schema

```bash
avrotize csv2a <path_to_csv_file> [--out <path_to_avro_schema_file>] [--namespace <avro_schema_namespace>]
```

Parameters:

- `<path_to_csv_file>`: The path to the CSV file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Avrotize Schema file to write the conversion result to. If omitted, the output is directed to stdout.
- `--namespace`: (optional) The namespace to use in the Avrotize Schema if the CSV file does not define a namespace.

Conversion notes:

- The tool reads the CSV file and converts it to Avrotize Schema. The first row of the CSV file is assumed to be the header row, containing the field names.
- The fields of the CSV file are mapped to fields in the Avrotize Schema. The tool infers the types of the fields from the data in the CSV file.

### Convert Kafka Connect Schema to Avrotize Schema

```bash
avrotize kstruct2a [input] --out <path_to_avro_schema_file>
```

Parameters:

- `input`: The path to the Kafka Struct file to be converted (or read from stdin if omitted).
- `--out`: The path to the Avrotize Schema file to write the conversion result to.
- `--kstruct`: Deprecated: The path to the Kafka Struct file (for backward compatibility).

Conversion notes:

- The tool converts the Kafka Struct definition to an Avrotize Schema, mapping Kafka data types to their Avro equivalents.
- Kafka Structs are typically used to define data structures for Kafka Connect and other Kafka-based applications. This command facilitates interoperability by enabling the conversion of these definitions into Avro, which can be further used with various serialization and schema registry tools.

### Convert Avrotize Schema to C# classes

```bash
avrotize a2cs <path_to_avro_schema_file> [--out <path_to_csharp_dir>] [--namespace <csharp_namespace>] [--avro-annotation] [--system_text_json_annotation] [--newtonsoft-json-annotation] [--pascal-properties]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the directory to write the C# classes to. Required.
- `--namespace`: (optional) The namespace to use in the C# classes.
- `--avro-annotation`: (optional) Use Avro annotations.
- `--system_text_json_annotation`: (optional) Use System.Text.Json annotations.
- `--newtonsoft-json-annotation`: (optional) Use Newtonsoft.Json annotations.
- `--pascal-properties`: (optional) Use PascalCase properties.

Conversion notes:

- The tool generates C# classes from the Avrotize Schema. Each record type in the Avrotize Schema is converted to a C# class.
- The fields of the record are mapped to properties in the C# class. Nested records are mapped to nested classes in the C# class.
- The tool supports adding annotations to the properties in the C# class. The `--avro-annotation` option adds Avro annotations, the `--system_text_json_annotation` option adds System.Text.Json annotations, and the `--newtonsoft-json-annotation` option adds Newtonsoft.Json annotations.
- The `--pascal-properties` option changes the naming convention of the properties to PascalCase.

### Convert Avrotize Schema to Java classes

```bash
avrotize a2java <path_to_avro_schema_file> [--out <path_to_java_dir>] [--package <java_package>] [--avro-annotation] [--jackson-annotation] [--pascal-properties]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the directory to write the Java classes to. Required.
- `--package`: (optional) The package to use in the Java classes.
- `--avro-annotation`: (optional) Use Avro annotations.
- `--jackson-annotation`: (optional) Use Jackson annotations.
- `--pascal-properties`: (optional) Use PascalCase properties.

Conversion notes:

- The tool generates Java classes from the Avrotize Schema. Each record type in the Avrotize Schema is converted to a Java class.
- The fields of the record are mapped to properties in the Java class. Nested records are mapped to nested classes in the Java class.
- The tool supports adding annotations to the properties in the Java class. The `--avro-annotation` option adds Avro annotations, and the `--jackson-annotation` option adds Jackson annotations.
- The `--pascal-properties` option changes the naming convention of the properties to PascalCase.

### Convert Avrotize Schema to Python classes

```bash
avrotize a2py <path_to_avro_schema_file> [--out <path_to_python_dir>] [--package <python_package>] [--dataclasses-json-annotation] [--avro-annotation]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the directory to write the Python classes to. Required.
- `--package`: (optional) The package to use in the Python classes.
- `--dataclasses-json-annotation`: (optional) Use dataclasses-json annotations.
- `--avro-annotation`: (optional) Use Avro annotations.

Conversion notes:

- The tool generates Python classes from the Avrotize Schema. Each record type in the Avrotize Schema is converted to a Python class.
- The fields of the record are mapped to properties in the Python class. Nested records are mapped to nested classes in the Python class.
- The tool supports adding annotations to the properties in the Python class. The `--dataclasses-json-annotation` option adds dataclasses-json annotations, and the `--avro-annotation` option adds Avro annotations.

### Convert Avrotize Schema to TypeScript classes

```bash
avrotize a2ts <path_to_avro_schema_file> [--out <path_to_typescript_dir>] [--package <typescript_package>] [--avro-annotation] [--typedjson-annotation]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the directory to write the TypeScript classes to. Required.
- `--package`: (optional) The package to use in the TypeScript classes.
- `--avro-annotation`: (optional) Use Avro annotations.
- `--typedjson-annotation`: (optional) Use TypedJSON annotations.

Conversion notes:

- The tool generates TypeScript classes from the Avrotize Schema. Each record type in the Avrotize Schema is converted to a TypeScript class.
- The fields of the record are mapped to properties in the TypeScript class. Nested records are mapped to nested classes in the TypeScript class.
- The tool supports adding annotations to the properties in the TypeScript class. The `--avro-annotation` option adds Avro annotations, and the `--typedjson-annotation` option adds TypedJSON annotations.

### Convert Avrotize Schema to JavaScript classes

```bash
avrotize a2js <path_to_avro_schema_file> [--out <path_to_javascript_dir>] [--package <javascript_package>] [--avro-annotation]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the directory to write the JavaScript classes to. Required.
- `--package`: (optional) The package to use in the JavaScript classes.
- `--avro-annotation`: (optional) Use Avro annotations.

Conversion notes:

- The tool generates JavaScript classes from the Avrotize Schema. Each record type in the Avrotize Schema is converted to a JavaScript class.
- The fields of the record are mapped to properties in the JavaScript class. Nested records are mapped to nested classes in the JavaScript class.
- The tool supports adding annotations to the properties in the JavaScript class. The `--avro-annotation` option adds Avro annotations.

### Convert Avrotize Schema to C++ classes

```bash
avrotize a2cpp <path_to_avro_schema_file> [--out <path_to_cpp_dir>] [--namespace <cpp_namespace>] [--avro-annotation] [--json-annotation]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the directory to write the C++ classes to. Required.
- `--namespace`: (optional) The namespace to use in the C++ classes.
- `--avro-annotation`: (optional) Use Avro annotations.
- `--json-annotation`: (optional) Use JSON annotations.

Conversion notes:

- The tool generates C++ classes from the Avrotize Schema. Each record type in the Av

rotize Schema is converted to a C++ class.

- The fields of the record are mapped to properties in the C++ class. Nested records are mapped to nested classes in the C++ class.
- The tool supports adding annotations to the properties in the C++ class. The `--avro-annotation` option adds Avro annotations, and the `--json-annotation` option adds JSON annotations.

### Convert Avrotize Schema to Go classes

```bash
avrotize a2go <path_to_avro_schema_file> [--out <path_to_go_dir>] [--package <go_package>] [--avro-annotation] [--json-annotation] [--package-site <go_package_site>] [--package-username <go_package_username>]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the directory to write the Go classes to. Required.
- `--package`: (optional) The package to use in the Go classes.
- `--package-site`: (optional) The package site to use in the Go classes.
- `--package-username`: (optional) The package username to use in the Go classes.
- `--avro-annotation`: (optional) Use Avro annotations.
- `--json-annotation`: (optional) Use JSON annotations.

Conversion notes:

- The tool generates Go classes from the Avrotize Schema. Each record type in the Avrotize Schema is converted to a Go class.
- The fields of the record are mapped to properties in the Go class. Nested records are mapped to nested classes in the Go class.
- The tool supports adding annotations to the properties in the Go class. The `--avro-annotation` option adds Avro annotations, and the `--json-annotation` option adds JSON annotations.

### Convert Avrotize Schema to Rust classes

```bash
avrotize a2rust <path_to_avro_schema_file> [--out <path_to_rust_dir>] [--package <rust_package>] [--avro-annotation] [--serde-annotation]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the directory to write the Rust classes to. Required.
- `--package`: (optional) The package to use in the Rust classes.
- `--avro-annotation`: (optional) Use Avro annotations.
- `--serde-annotation`: (optional) Use Serde annotations.

Conversion notes:

- The tool generates Rust classes from the Avrotize Schema. Each record type in the Avrotize Schema is converted to a Rust class.
- The fields of the record are mapped to properties in the Rust class. Nested records are mapped to nested classes in the Rust class.
- The tool supports adding annotations to the properties in the Rust class. The `--avro-annotation` option adds Avro annotations, and the `--serde-annotation` option adds Serde annotations.

### Convert Avrotize Schema to Datapackage schema

```bash
avrotize a2dp <path_to_avro_schema_file> [--out <path_to_datapackage_file>] [--record-type <record-type-from-avro>]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Datapackage schema file to write the conversion result to. If omitted, the output is directed to stdout.
- `--record-type`: (optional) The name of the Avro record type to convert to a Datapackage schema.

Conversion notes:

- The tool generates a Datapackage schema from the Avrotize Schema. Each record type in the Avrotize Schema is converted to a Datapackage resource.
- The fields of the record are mapped to fields in the Datapackage resource. Nested records are mapped to nested resources in the Datapackage.

### Convert Avrotize Schema to Markdown documentation

```bash
avrotize a2md <path_to_avro_schema_file> [--out <path_to_markdown_file>]
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.
- `--out`: The path to the Markdown file to write the conversion result to. If omitted, the output is directed to stdout.

Conversion notes:

- The tool generates Markdown documentation from the Avrotize Schema. Each record type in the Avrotize Schema is converted to a Markdown section.
- The fields of the record are documented in a table in the Markdown section. Nested records are documented in nested sections in the Markdown file.

### Create the Parsing Canonical Form (PCF) of an Avrotize schema

```bash
avrotize pcf <path_to_avro_schema_file>
```

Parameters:

- `<path_to_avro_schema_file>`: The path to the Avrotize Schema file to be converted. If omitted, the file is read from stdin.

Conversion notes:

- The tool generates the Parsing Canonical Form (PCF) of the Avrotize Schema. The PCF is a normalized form of the schema that is used for schema comparison and compatibility checking.
- The PCF is a JSON object that is written to stdout.

This document provides an overview of the usage and functionality of Avrotize. For more detailed information, please refer to the [Avrotize Schema documentation](specs/avrotize-schema.md) and the individual command help messages.
