# NoSQL Conversion Notes

This document provides detailed information about the conversions from Avrotize schemas to various NoSQL database schemas.

## Cassandra

**Command**: `a2cassandra`

**Description**: Converts an Avrotize schema to a Cassandra schema.

### Conversion Details
- **Primary Key**: The first field in the schema is used as the primary key. Additional unique fields are added as clustering keys.
- **CloudEvents Columns**: If enabled, additional columns for CloudEvents are added to the schema.
- **Column Types**: Avro types are mapped to corresponding Cassandra types:
  - `null`: `NULL`
  - `boolean`: `boolean`
  - `int`: `int`
  - `long`: `bigint`
  - `float`: `float`
  - `double`: `double`
  - `bytes`: `blob`
  - `string`: `text`
  - `array`, `map`, `record`, `union`: `text`

## DynamoDB

**Command**: `a2dynamodb`

**Description**: Converts an Avrotize schema to a DynamoDB schema.

### Conversion Details
- **Primary Key**: The first field in the schema is used as the partition key. Additional unique fields are added as sort keys.
- **CloudEvents Columns**: If enabled, additional columns for CloudEvents are added to the schema.
- **Column Types**: Avro types are mapped to corresponding DynamoDB types:
  - `null`: `NULL`
  - `boolean`: `BOOL`
  - `int`, `long`, `float`, `double`: `N`
  - `bytes`: `B`
  - `string`, `array`, `map`, `record`, `union`: `S`

## Elasticsearch

**Command**: `a2elasticsearch`

**Description**: Converts an Avrotize schema to an Elasticsearch schema.

### Conversion Details
- **CloudEvents Columns**: If enabled, additional fields for CloudEvents are added to the schema.
- **Field Types**: Avro types are mapped to corresponding Elasticsearch types:
  - `null`: `null`
  - `boolean`: `boolean`
  - `int`: `integer`
  - `long`: `long`
  - `float`: `float`
  - `double`: `double`
  - `bytes`: `binary`
  - `string`: `text`
  - `array`, `map`, `record`, `union`: `text`

## CouchDB

**Command**: `a2couchdb`

**Description**: Converts an Avrotize schema to a CouchDB schema.

### Conversion Details
- **CloudEvents Columns**: If enabled, additional fields for CloudEvents are added to the schema.
- **Field Types**: Avro types are mapped to corresponding CouchDB types:
  - `null`: `null`
  - `boolean`: `boolean`
  - `int`, `long`: `integer`
  - `float`, `double`: `number`
  - `bytes`, `string`, `array`, `map`, `record`, `union`: `string`

## Neo4j

**Command**: `a2neo4j`

**Description**: Converts an Avrotize schema to a Neo4j schema.

### Conversion Details
- **CloudEvents Columns**: If enabled, additional fields for CloudEvents are added to the schema.
- **Field Types**: Avro types are mapped to corresponding Neo4j types:
  - `null`: `NULL`
  - `boolean`, `int`, `long`, `float`, `double`: `number`
  - `bytes`, `string`, `array`, `map`, `record`, `union`: `string`

## Firebase

**Command**: `a2firebase`

**Description**: Converts an Avrotize schema to a Firebase schema.

### Conversion Details
- **CloudEvents Columns**: If enabled, additional fields for CloudEvents are added to the schema.
- **Field Types**: Avro types are mapped to corresponding Firebase types:
  - `null`: `null`
  - `boolean`: `boolean`
  - `int`, `long`: `integer`
  - `float`, `double`: `number`
  - `bytes`, `string`, `array`, `map`, `record`, `union`: `string`

## CosmosDB

**Command**: `a2cosmosdb`

**Description**: Converts an Avrotize schema to a CosmosDB schema.

### Conversion Details
- **Partition Key**: The first field in the schema is used as the partition key. Additional unique fields are added as sort keys.
- **CloudEvents Columns**: If enabled, additional fields for CloudEvents are added to the schema.
- **Field Types**: Avro types are mapped to corresponding CosmosDB types:
  - `null`: `null`
  - `boolean`: `boolean`
  - `int`: `number`
  - `long`: `number`
  - `float`, `double`: `number`
  - `bytes`, `string`, `array`, `map`, `record`, `union`: `string`

## HBase

**Command**: `a2hbase`

**Description**: Converts an Avrotize schema to an HBase schema.

### Conversion Details
- **CloudEvents Columns**: If enabled, additional columns for CloudEvents are added to the schema.
- **Column Families**: Each field in the Avro schema is converted to a column family in HBase.
- **Column Types**: Avro types are mapped to corresponding HBase column families:
  - `null`: `string`
  - `boolean`: `boolean`
  - `int`, `long`: `integer`
  - `float`, `double`: `number`
  - `bytes`, `string`, `array`, `map`, `record`, `union`: `string`

This document serves as a reference for understanding the specific mappings and additional configurations available when converting Avrotize schemas to NoSQL database schemas.