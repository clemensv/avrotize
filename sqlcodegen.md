## SQL Conversion Notes

### Overview

This document provides detailed conversion notes for converting Avro schemas to SQL schemas for various databases. Each SQL dialect has specific considerations and mappings for data types, constraints, and other schema elements.

### Supported SQL Dialects

The following SQL dialects are supported for conversion:
- SQL Server
- PostgreSQL
- MySQL
- MariaDB
- SQLite
- Oracle
- DB2
- SQL Anywhere
- BigQuery
- Snowflake
- Redshift
- Cassandra (NoSQL)

### General Conversion Rules

1. **Data Type Mapping**: Avro types are mapped to corresponding SQL types based on the target SQL dialect.
2. **Primary Key and Unique Constraints**: Fields specified as unique or primary keys in the Avro schema are enforced in the SQL schema.
3. **Comments**: Documentation comments from the Avro schema are included as table and column comments where supported.
4. **CloudEvents Columns**: Optionally, CloudEvents columns can be added to the schema.

### SQL Dialect Specifics

#### SQL Server

- Data Types:
  - `null`: `NULL`
  - `boolean`: `BIT`
  - `int`: `INT`
  - `long`: `BIGINT`
  - `float`: `FLOAT`
  - `double`: `FLOAT`
  - `bytes`: `VARBINARY(MAX)`
  - `string`: `NVARCHAR(512)`

- Constraints:
  - Primary keys and unique constraints are supported.
  - Extended properties are used for comments.

#### PostgreSQL

- Data Types:
  - `null`: `NULL`
  - `boolean`: `BOOLEAN`
  - `int`: `INTEGER`
  - `long`: `BIGINT`
  - `float`: `REAL`
  - `double`: `DOUBLE PRECISION`
  - `bytes`: `BYTEA`
  - `string`: `VARCHAR(512)`
  - `array`, `map`, `record`, `union`: `JSONB`

- Constraints:
  - Primary keys and unique constraints are supported.
  - Standard SQL comments are used for table and column comments.

#### MySQL and MariaDB

- Data Types:
  - `null`: `NULL`
  - `boolean`: `BOOLEAN`
  - `int`: `INT`
  - `long`: `BIGINT`
  - `float`: `FLOAT`
  - `double`: `DOUBLE`
  - `bytes`: `BLOB`
  - `string`: `VARCHAR(512)`
  - `array`, `map`, `record`, `union`: `JSON`

- Constraints:
  - Primary keys and unique constraints are supported.
  - Column comments are included using the `COMMENT` keyword.

#### SQLite

- Data Types:
  - `null`: `NULL`
  - `boolean`: `BOOLEAN`
  - `int`: `INTEGER`
  - `long`: `INTEGER`
  - `float`: `REAL`
  - `double`: `REAL`
  - `bytes`: `BLOB`
  - `string`: `VARCHAR(512)`
  - `array`, `map`, `record`, `union`: `TEXT`

- Constraints:
  - Primary keys and unique constraints are supported.
  - Comments are included as SQL comments.

#### Oracle

- Data Types:
  - `null`: `NULL`
  - `boolean`: `NUMBER(1)`
  - `int`: `NUMBER(10)`
  - `long`: `NUMBER(19)`
  - `float`: `FLOAT(126)`
  - `double`: `FLOAT(126)`
  - `bytes`: `BLOB`
  - `string`: `VARCHAR(512)`
  - `array`, `map`, `record`, `union`: `CLOB`

- Constraints:
  - Primary keys and unique constraints are supported.
  - Standard SQL comments are used for table and column comments.

#### DB2

- Data Types:
  - `null`: `NULL`
  - `boolean`: `BOOLEAN`
  - `int`: `INTEGER`
  - `long`: `BIGINT`
  - `float`: `REAL`
  - `double`: `DOUBLE`
  - `bytes`: `BLOB`
  - `string`: `VARCHAR(512)`
  - `array`, `map`, `record`, `union`: `CLOB`

- Constraints:
  - Primary keys and unique constraints are supported.
  - Standard SQL comments are used for table and column comments.

#### SQL Anywhere

- Data Types:
  - `null`: `NULL`
  - `boolean`: `BIT`
  - `int`: `INTEGER`
  - `long`: `BIGINT`
  - `float`: `FLOAT`
  - `double`: `FLOAT`
  - `bytes`: `LONG BINARY`
  - `string`: `VARCHAR(512)`
  - `array`, `map`, `record`, `union`: `LONG VARCHAR`

- Constraints:
  - Primary keys and unique constraints are supported.
  - Extended properties are used for comments.

#### BigQuery

- Data Types:
  - `null`: `NULL`
  - `boolean`: `BOOL`
  - `int`: `INT64`
  - `long`: `INT64`
  - `float`: `FLOAT64`
  - `double`: `FLOAT64`
  - `bytes`: `BYTES`
  - `string`: `STRING`
  - `array`, `map`, `record`, `union`: `STRING`

- Constraints:
  - Primary keys and unique constraints are not supported natively.
  - Standard SQL comments are used for table and column comments.

#### Snowflake

- Data Types:
  - `null`: `NULL`
  - `boolean`: `BOOLEAN`
  - `int`: `NUMBER`
  - `long`: `NUMBER`
  - `float`: `FLOAT`
  - `double`: `FLOAT`
  - `bytes`: `BINARY`
  - `string`: `STRING`
  - `array`, `map`, `record`, `union`: `VARIANT`

- Constraints:
  - Primary keys and unique constraints are supported.
  - Standard SQL comments are used for table and column comments.

#### Redshift

- Data Types:
  - `null`: `NULL`
  - `boolean`: `BOOLEAN`
  - `int`: `INTEGER`
  - `long`: `BIGINT`
  - `float`: `REAL`
  - `double`: `DOUBLE PRECISION`
  - `bytes`: `VARBYTE`
  - `string`: `VARCHAR(256)`
  - `array`, `map`, `record`, `union`: `VARCHAR(65535)`

- Constraints:
  - Primary keys and unique constraints are supported.
  - Standard SQL comments are used for table and column comments.

#### Cassandra (NoSQL)

- Data Types:
  - `null`: `NULL`
  - `boolean`: `boolean`
  - `int`: `int`
  - `long`: `bigint`
  - `float`: `float`
  - `double`: `double`
  - `bytes`: `blob`
  - `string`: `text`
  - `array`, `map`, `record`, `union`: `text`

- Constraints:
  - Primary keys are supported.
  - Unique constraints are not natively supported.
  - Comments are not natively supported.

For further details on the conversion logic and examples for each SQL dialect, refer to the main documentation.
