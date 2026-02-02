# SQL to Avro Type Mappings

This document describes how the `sql2a` command maps SQL database types to Avro types for each supported dialect.

## Overview

The `sql2a` command connects to a live SQL database and converts table schemas to Avrotize Schema format. Type mappings are dialect-specific, as each database system has its own type system.

## PostgreSQL Type Mappings

PostgreSQL has a rich type system including arrays, JSON, and geometric types.

### Numeric Types

| PostgreSQL Type | Avro Type | Notes |
|-----------------|-----------|-------|
| `smallint`, `int2` | `int` | 16-bit signed integer |
| `integer`, `int`, `int4` | `int` | 32-bit signed integer |
| `bigint`, `int8` | `long` | 64-bit signed integer |
| `smallserial` | `int` | Auto-incrementing 16-bit |
| `serial` | `int` | Auto-incrementing 32-bit |
| `bigserial` | `long` | Auto-incrementing 64-bit |
| `real`, `float4` | `float` | 32-bit floating point |
| `double precision`, `float8` | `double` | 64-bit floating point |
| `numeric`, `decimal` | `bytes` | Logical type `decimal` with precision/scale |
| `money` | `bytes` | Logical type `decimal` (precision: 19, scale: 2) |

### Boolean Type

| PostgreSQL Type | Avro Type |
|-----------------|-----------|
| `boolean`, `bool` | `boolean` |

### Character Types

| PostgreSQL Type | Avro Type | Notes |
|-----------------|-----------|-------|
| `character varying`, `varchar` | `string` | Variable-length string |
| `character`, `char`, `bpchar` | `string` | Fixed-length string |
| `text` | `string` | Unlimited length string |
| `name` | `string` | Internal identifier type (63 bytes) |

### Binary Types

| PostgreSQL Type | Avro Type |
|-----------------|-----------|
| `bytea` | `bytes` |

### Date/Time Types

| PostgreSQL Type | Avro Type | Logical Type |
|-----------------|-----------|--------------|
| `date` | `int` | `date` |
| `time`, `time without time zone` | `int` | `time-millis` |
| `time with time zone`, `timetz` | `int` | `time-millis` |
| `timestamp`, `timestamp without time zone` | `long` | `timestamp-millis` |
| `timestamp with time zone`, `timestamptz` | `long` | `timestamp-millis` |
| `interval` | `fixed` (12 bytes) | `duration` |

### UUID Type

| PostgreSQL Type | Avro Type | Logical Type |
|-----------------|-----------|--------------|
| `uuid` | `string` | `uuid` |

### JSON Types

| PostgreSQL Type | Default Avro Type | With Inference |
|-----------------|-------------------|----------------|
| `json` | `string` | Inferred record/union |
| `jsonb` | `string` | Inferred record/union |

When `--infer-json` is enabled (default), the tool samples data from JSON/JSONB columns to infer the schema structure. Fields that appear in some but not all records are accumulated into a single record type. If field types conflict, a union of record types is generated.

### XML Type

| PostgreSQL Type | Default Avro Type | With Inference |
|-----------------|-------------------|----------------|
| `xml` | `string` | Inferred record |

When `--infer-xml` is enabled (default), the tool samples XML data to infer element and attribute structure.

### Network Types

| PostgreSQL Type | Avro Type | Notes |
|-----------------|-----------|-------|
| `inet` | `string` | IPv4 or IPv6 address |
| `cidr` | `string` | IPv4 or IPv6 network |
| `macaddr` | `string` | MAC address (6 bytes) |
| `macaddr8` | `string` | MAC address (8 bytes) |

### Geometric Types

| PostgreSQL Type | Avro Type | Notes |
|-----------------|-----------|-------|
| `point` | `string` | Stored as text representation |
| `line` | `string` | |
| `lseg` | `string` | Line segment |
| `box` | `string` | Rectangular box |
| `path` | `string` | Open or closed path |
| `polygon` | `string` | |
| `circle` | `string` | |

### Other Types

| PostgreSQL Type | Avro Type | Notes |
|-----------------|-----------|-------|
| `bit`, `bit varying`, `varbit` | `string` | Bit strings |
| `tsvector` | `string` | Full-text search vector |
| `tsquery` | `string` | Full-text search query |
| `oid` | `long` | Object identifier |

### Array Types

PostgreSQL array types (e.g., `integer[]`, `text[]`) are mapped to Avro arrays with the appropriate item type:

```json
{
  "type": "array",
  "items": "int"
}
```

---

## MySQL Type Mappings

### Numeric Types

| MySQL Type | Avro Type | Notes |
|------------|-----------|-------|
| `tinyint` | `int` | 8-bit signed |
| `smallint` | `int` | 16-bit signed |
| `mediumint` | `int` | 24-bit signed |
| `int`, `integer` | `int` | 32-bit signed |
| `bigint` | `long` | 64-bit signed |
| `float` | `float` | 32-bit floating point |
| `double` | `double` | 64-bit floating point |
| `decimal`, `numeric` | `bytes` | Logical type `decimal` (precision: 38, scale: 10) |

### Boolean Type

| MySQL Type | Avro Type | Notes |
|------------|-----------|-------|
| `bit(1)` | `boolean` | Single bit, commonly used as boolean |
| `bit(n)` (n > 1) | `bytes` | Multi-bit field stored as bytes |
| `boolean`, `bool` | `boolean` | Alias for `tinyint(1)` |

### Character Types

| MySQL Type | Avro Type |
|------------|-----------|
| `char` | `string` |
| `varchar` | `string` |
| `tinytext` | `string` |
| `text` | `string` |
| `mediumtext` | `string` |
| `longtext` | `string` |

### Binary Types

| MySQL Type | Avro Type |
|------------|-----------|
| `binary` | `bytes` |
| `varbinary` | `bytes` |
| `tinyblob` | `bytes` |
| `blob` | `bytes` |
| `mediumblob` | `bytes` |
| `longblob` | `bytes` |

### Date/Time Types

| MySQL Type | Avro Type | Logical Type |
|------------|-----------|--------------|
| `date` | `int` | `date` |
| `time` | `int` | `time-millis` |
| `datetime` | `long` | `timestamp-millis` |
| `timestamp` | `long` | `timestamp-millis` |
| `year` | `int` | Plain integer (1901-2155) |

### JSON Type

| MySQL Type | Default Avro Type | With Inference |
|------------|-------------------|----------------|
| `json` | `string` | Inferred record/union |

### Other Types

| MySQL Type | Avro Type | Notes |
|------------|-----------|-------|
| `enum` | `string` | Stored as string value |
| `set` | `string` | Stored as comma-separated string |

---

## SQL Server Type Mappings

### Numeric Types

| SQL Server Type | Avro Type | Notes |
|-----------------|-----------|-------|
| `bit` | `boolean` | 0 or 1 |
| `tinyint` | `int` | 8-bit unsigned (0-255) |
| `smallint` | `int` | 16-bit signed |
| `int` | `int` | 32-bit signed |
| `bigint` | `long` | 64-bit signed |
| `real` | `float` | 32-bit floating point |
| `float` | `double` | 64-bit floating point |
| `decimal`, `numeric` | `bytes` | Logical type `decimal` (precision: 38, scale: 10) |
| `money` | `bytes` | Logical type `decimal` (precision: 19, scale: 4) |
| `smallmoney` | `bytes` | Logical type `decimal` (precision: 10, scale: 4) |

### Character Types

| SQL Server Type | Avro Type | Notes |
|-----------------|-----------|-------|
| `char` | `string` | Fixed-length non-Unicode |
| `varchar` | `string` | Variable-length non-Unicode |
| `nchar` | `string` | Fixed-length Unicode |
| `nvarchar` | `string` | Variable-length Unicode |
| `text` | `string` | Legacy large text (deprecated) |
| `ntext` | `string` | Legacy large Unicode text (deprecated) |

### Binary Types

| SQL Server Type | Avro Type |
|-----------------|-----------|
| `binary` | `bytes` |
| `varbinary` | `bytes` |
| `image` | `bytes` |

### Date/Time Types

| SQL Server Type | Avro Type | Logical Type |
|-----------------|-----------|--------------|
| `date` | `int` | `date` |
| `time` | `int` | `time-millis` |
| `datetime` | `long` | `timestamp-millis` |
| `datetime2` | `long` | `timestamp-millis` |
| `smalldatetime` | `long` | `timestamp-millis` |
| `datetimeoffset` | `long` | `timestamp-millis` |

### UUID Type

| SQL Server Type | Avro Type | Logical Type |
|-----------------|-----------|--------------|
| `uniqueidentifier` | `string` | `uuid` |

### XML Type

| SQL Server Type | Default Avro Type | With Inference |
|-----------------|-------------------|----------------|
| `xml` | `string` | Inferred record |

### Other Types

| SQL Server Type | Avro Type | Notes |
|-----------------|-----------|-------|
| `sql_variant` | `string` | Stored as string representation |
| `hierarchyid` | `string` | Stored as string representation |
| `geometry` | `bytes` | Binary spatial data |
| `geography` | `bytes` | Binary spatial data |

---

## Oracle Type Mappings

Oracle support uses the `oracledb` driver and maps types following similar patterns:

| Oracle Type | Avro Type | Notes |
|-------------|-----------|-------|
| `NUMBER` | `bytes` | Logical type `decimal` |
| `FLOAT` | `double` | |
| `BINARY_FLOAT` | `float` | |
| `BINARY_DOUBLE` | `double` | |
| `VARCHAR2` | `string` | |
| `NVARCHAR2` | `string` | |
| `CHAR` | `string` | |
| `NCHAR` | `string` | |
| `CLOB` | `string` | |
| `NCLOB` | `string` | |
| `BLOB` | `bytes` | |
| `RAW` | `bytes` | |
| `DATE` | `long` | `timestamp-millis` |
| `TIMESTAMP` | `long` | `timestamp-millis` |
| `TIMESTAMP WITH TIME ZONE` | `long` | `timestamp-millis` |

---

## SQLite Type Mappings

SQLite uses dynamic typing with type affinity. The tool maps based on declared type:

| SQLite Affinity | Avro Type | Notes |
|-----------------|-----------|-------|
| `INTEGER` | `long` | 64-bit signed |
| `REAL` | `double` | 64-bit floating point |
| `TEXT` | `string` | |
| `BLOB` | `bytes` | |
| `NUMERIC` | `string` | May contain various formats |

---

## JSON/XML Schema Inference

When `--infer-json` or `--infer-xml` is enabled, the tool samples data to determine structure.

### JSON Inference Algorithm

1. Sample up to `--sample-size` rows (default: 100)
2. Parse each JSON value and determine its Python type
3. Map Python types to Avro types:
   - `dict` → `record`
   - `list` → `array`
   - `str` → `string`
   - `int` → `long`
   - `float` → `double`
   - `bool` → `boolean`
   - `None` → `null`
4. Fold compatible record types together (accumulate fields)
5. Generate unions when field types conflict

### Handling Sparse Data

When JSON fields appear in some records but not others, the inference engine folds all discovered fields into a single record type. For example:

```json
{"name": "Alice", "email": "alice@example.com"}
{"name": "Bob", "phone": "555-1234"}
{"name": "Carol", "email": "carol@example.com", "phone": "555-5678"}
```

Results in a record with all three fields: `name`, `email`, `phone`.

### Handling Type Conflicts

When the same field has different types across records:

```json
{"value": 42}
{"value": "forty-two"}
{"value": 3.14}
```

The tool generates a union of record types, each with the field typed differently.

### Non-Identifier Keys → Maps

When JSON object keys cannot be valid Avro field names (UUIDs, URLs, special characters), the tool generates a `map<string, T>` type instead of a record:

```json
{"events": {"550e8400-e29b-41d4-a716-446655440000": {"type": "created"}}}
```

Results in:

```json
{
  "name": "events",
  "type": {
    "type": "map",
    "values": {"type": "record", ...}
  }
}
```

---

## Nullability

Column nullability is mapped as follows:

- `NOT NULL` columns → direct Avro type (e.g., `"type": "string"`)
- Nullable columns → union with null (e.g., `"type": ["null", "string"]`)

---

## Primary Keys

Primary key columns are recorded in the schema's `unique` attribute:

```json
{
  "type": "record",
  "name": "users",
  "fields": [...],
  "unique": ["id"]
}
```

---

## Table and Column Comments

Where supported by the database, table and column comments are preserved as Avro `doc` attributes:

```json
{
  "name": "email",
  "type": "string",
  "doc": "User's primary email address"
}
```
