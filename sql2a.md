# sql2a: SQL Schema to Avro Implementation Plan

## Overview

The `sql2a` command transforms SQL database schemas into Avro schemas, serving as the reverse of the existing `a2sql` command. The most urgent requirement is PostgreSQL support, including intelligent schema inference for JSON and XML columns – analogous to how `k2a` (Kusto to Avro) infers schema for `dynamic` columns by sampling live data.

## Context and Existing Patterns

### Reference Implementations

The codebase already contains two relevant reference implementations:

1. **[kustotoavro.py](avrotize/kustotoavro.py)** – Connects to a live Kusto database, extracts table schemas, and infers Avro types for `dynamic` columns by sampling actual data. This is the primary architectural template.

2. **[avrotodb.py](avrotize/avrotodb.py)** – Converts Avro schemas *to* SQL. Contains comprehensive type mappings for all supported dialects (PostgreSQL, MySQL, SQL Server, Oracle, etc.) that can be reversed.

### Key Architectural Decisions from `k2a`

The Kusto-to-Avro converter demonstrates several patterns we should replicate:

- **Class-based design** with a main converter class (`KustoToAvro`)
- **Live database connection** using SDK authentication (Azure CLI credentials)
- **Schema sampling** for dynamic/semi-structured columns
- **Type consolidation** when multiple JSON structures appear in the same column
- **CloudEvents support** for event-driven architectures
- **xRegistry manifest generation** for schema registries

## Proposed Architecture

### Module Structure

```
avrotize/
├── sqltoavro.py           # Main converter implementation
├── sqltoavro/
│   ├── __init__.py
│   ├── postgres.py        # PostgreSQL-specific logic
│   ├── mysql.py           # MySQL-specific logic (future)
│   ├── sqlserver.py       # SQL Server-specific logic (future)
│   └── inference.py       # JSON/XML schema inference engine
```

### Class Design

```python
class SqlToAvro:
    """Converts SQL database schemas to Avro schema format."""
    
    def __init__(
        self,
        connection_string: str,
        database: str,
        table_name: str | None,
        avro_namespace: str,
        avro_schema_path: str,
        dialect: str,
        emit_cloudevents: bool,
        emit_xregistry: bool,
        sample_size: int = 100,
        infer_json_schema: bool = True,
        infer_xml_schema: bool = True
    ):
        ...
```

## Type Mapping: PostgreSQL → Avro

### Base Type Mappings

| PostgreSQL Type | Avro Type | Notes |
|-----------------|-----------|-------|
| `smallint`, `int2` | `int` | |
| `integer`, `int4` | `int` | |
| `bigint`, `int8` | `long` | |
| `real`, `float4` | `float` | |
| `double precision`, `float8` | `double` | |
| `numeric`, `decimal` | `{"type": "bytes", "logicalType": "decimal", "precision": P, "scale": S}` | Extract precision/scale from column definition |
| `boolean`, `bool` | `boolean` | |
| `char`, `varchar`, `text` | `string` | |
| `bytea` | `bytes` | |
| `date` | `{"type": "int", "logicalType": "date"}` | |
| `time` | `{"type": "int", "logicalType": "time-millis"}` | |
| `timestamp` | `{"type": "long", "logicalType": "timestamp-millis"}` | |
| `timestamptz` | `{"type": "long", "logicalType": "timestamp-millis"}` | Store as UTC |
| `interval` | `{"type": "fixed", "size": 12, "logicalType": "duration"}` | |
| `uuid` | `{"type": "string", "logicalType": "uuid"}` | |
| `json` | *inferred* or `string` | See JSON inference section |
| `jsonb` | *inferred* or `string` | See JSON inference section |
| `xml` | *inferred* or `string` | See XML inference section |
| `array` | `{"type": "array", "items": ...}` | Recursive mapping |
| `composite` | `{"type": "record", ...}` | Map fields recursively |

### Semi-Structured Column Inference

For `json`, `jsonb`, and `xml` columns, we sample actual data to infer the schema. This mirrors the `k2a` approach for Kusto's `dynamic` type.

#### JSON/JSONB Inference Strategy

1. **Sample rows** from the table (configurable, default 100)
2. **Parse JSON values** from the column
3. **Build type union** across all samples using the existing `consolidated_type_list()` pattern from `kustotoavro.py`
4. **Merge compatible record types** using the `fold_record_types()` algorithm
5. **Generate Avro schema** with proper union types for fields that vary

```python
def infer_json_column_schema(
    self, 
    table_name: str, 
    column_name: str,
    type_discriminator_column: str | None = None,
    type_discriminator_value: str | None = None
) -> JsonNode:
    """
    Infers Avro schema for a JSON/JSONB column by sampling data.
    
    Optionally constrains sampling by a type discriminator column,
    useful for CloudEvents-style tables where 'type' indicates structure.
    """
    query = f"""
        SELECT "{column_name}" 
        FROM "{table_name}"
        {'WHERE "' + type_discriminator_column + '" = %s' if type_discriminator_column else ''}
        LIMIT {self.sample_size}
    """
    # ... fetch and consolidate types
```

#### XML Inference Strategy

For XML columns, we have two options:

1. **Schema-based inference** – If the XML has an associated XSD (e.g., declared via `xmlns:xsi` or stored separately), use `xsdtoavro.py` to convert it.

2. **Sample-based inference** – Parse XML samples, extract element/attribute structure, and generate an Avro record type. This is more complex than JSON due to:
   - Mixed content (text + elements)
   - Attributes vs. elements
   - Namespaces
   - Repeated elements → arrays

```python
def infer_xml_column_schema(
    self, 
    table_name: str, 
    column_name: str
) -> JsonNode:
    """
    Infers Avro schema for an XML column by sampling data.
    
    Uses a simplified XML-to-record conversion:
    - Elements become fields
    - Attributes become fields with 'xml_attribute' annotation
    - Repeated elements become arrays
    - Text content becomes a '_text' field
    """
```

## PostgreSQL Schema Extraction

### Information Schema Queries

```sql
-- Get all tables
SELECT table_name, table_schema
FROM information_schema.tables 
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND table_type = 'BASE TABLE';

-- Get columns for a table
SELECT 
    column_name,
    data_type,
    udt_name,
    is_nullable,
    column_default,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    datetime_precision
FROM information_schema.columns
WHERE table_name = %s AND table_schema = %s
ORDER BY ordinal_position;

-- Get primary key columns
SELECT kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = %s 
  AND tc.constraint_type = 'PRIMARY KEY';

-- Get column comments
SELECT col_description(
    (quote_ident(table_schema) || '.' || quote_ident(table_name))::regclass,
    ordinal_position
) as column_comment
FROM information_schema.columns
WHERE table_name = %s;

-- Get composite type structure (for user-defined types)
SELECT 
    a.attname as column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = %s::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;

-- Get array element type
SELECT 
    e.data_type as element_type,
    e.udt_name as element_udt_name
FROM information_schema.element_types e
WHERE e.object_catalog = current_database()
  AND e.object_schema = %s
  AND e.object_name = %s
  AND e.collection_type_identifier = %s;
```

## Command-Line Interface

### Commands Entry (commands.json)

```json
{
    "command": "sql2a",
    "description": "Convert SQL schema to Avrotize schema",
    "group": "1_Schemas",
    "function": {
        "name": "avrotize.sqltoavro.convert_sql_to_avro",
        "args": {
            "connection_string": "args.connection_string",
            "database": "args.database",
            "table_name": "args.table_name",
            "avro_namespace": "args.namespace",
            "avro_schema_file": "output_file_path",
            "dialect": "args.dialect",
            "emit_cloudevents": "args.emit_cloudevents",
            "emit_xregistry": "args.emit_xregistry",
            "sample_size": "args.sample_size",
            "infer_json_schema": "args.infer_json",
            "infer_xml_schema": "args.infer_xml"
        }
    },
    "extensions": [],
    "args": [
        {
            "name": "input",
            "type": "str",
            "nargs": "?",
            "help": "Not used (database connection is live)",
            "required": false
        },
        {
            "name": "--out",
            "type": "str",
            "help": "Path to the Avrotize schema file",
            "required": false
        },
        {
            "name": "--connection-string",
            "type": "str",
            "help": "Database connection string (e.g., postgresql://user:pass@host:port/dbname)",
            "required": true
        },
        {
            "name": "--database",
            "type": "str",
            "help": "Database name (if not in connection string)",
            "required": false
        },
        {
            "name": "--table-name",
            "type": "str",
            "help": "Specific table name (omit for all tables)",
            "required": false
        },
        {
            "name": "--namespace",
            "type": "str",
            "help": "Namespace for the Avrotize schema",
            "required": false
        },
        {
            "name": "--dialect",
            "type": "str",
            "help": "SQL dialect",
            "choices": ["postgres", "mysql", "sqlserver", "oracle", "sqlite"],
            "default": "postgres",
            "required": false
        },
        {
            "name": "--emit-cloudevents",
            "type": "bool",
            "help": "Emit CloudEvents declarations for each table",
            "required": false
        },
        {
            "name": "--emit-xregistry",
            "type": "bool",
            "help": "Emit an xRegistry manifest instead of a single Avrotize schema",
            "required": false
        },
        {
            "name": "--sample-size",
            "type": "int",
            "help": "Number of rows to sample for JSON/XML inference (default: 100)",
            "default": 100,
            "required": false
        },
        {
            "name": "--infer-json",
            "type": "bool",
            "help": "Infer schema for JSON/JSONB columns (default: true)",
            "default": true,
            "required": false
        },
        {
            "name": "--infer-xml",
            "type": "bool",
            "help": "Infer schema for XML columns (default: true)",
            "default": true,
            "required": false
        }
    ],
    "suggested_output_file_path": "{database}.avsc",
    "prompts": [
        {
            "name": "--namespace",
            "message": "Enter the namespace for the Avro schema",
            "type": "str",
            "required": false
        },
        {
            "name": "--dialect",
            "message": "Select the SQL dialect",
            "choices": ["postgres", "mysql", "sqlserver", "oracle", "sqlite"],
            "default": "postgres",
            "required": true
        }
    ]
}
```

## Implementation Phases

### Phase 1: Core PostgreSQL Support (MVP)

**Deliverables:**
- `sqltoavro.py` with `SqlToAvro` class
- Basic PostgreSQL type mapping (all scalar types)
- Table schema extraction via `information_schema`
- Primary key detection → Avro `unique` annotation
- Column comments → Avro `doc` fields
- Simple JSON column passthrough (as `string`)

**Acceptance Criteria:**
- Can convert a PostgreSQL database with basic types to valid Avro schema
- Unit tests with `testcontainers.postgres` (following `test_avrotodb.py` pattern)

### Phase 2: JSON/JSONB Schema Inference

**Deliverables:**
- `inference.py` module for JSON type consolidation
- Reuse `consolidated_type_list()` and `fold_record_types()` patterns from `kustotoavro.py`
- Support for type discriminator columns (CloudEvents `type` pattern)

**Acceptance Criteria:**
- JSON columns produce structured Avro record types when data is homogeneous
- Heterogeneous JSON produces union types
- Empty/null JSON columns fall back to `string`

### Phase 3: XML Schema Inference

**Deliverables:**
- XML parsing and structure inference in `inference.py`
- Optional XSD-based inference using existing `xsdtoavro.py`
- Attribute vs. element distinction via Avro annotations

**Acceptance Criteria:**
- XML columns with consistent structure produce Avro records
- Attributes marked with `altnames.xml_attribute` annotation

### Phase 4: CloudEvents and xRegistry Support

**Deliverables:**
- CloudEvents field detection (similar to `k2a`)
- xRegistry manifest generation
- Type discriminator-based schema splitting

**Acceptance Criteria:**
- Tables with `type`, `source`, `id`, `data` columns recognized as CloudEvents
- xRegistry output matches existing `k2a` format

### Phase 5: Additional Dialects

**Deliverables:**
- MySQL support in `sqltoavro/mysql.py`
- SQL Server support in `sqltoavro/sqlserver.py`
- Oracle support in `sqltoavro/oracle.py`

**Acceptance Criteria:**
- Same functionality as PostgreSQL for each dialect
- Dialect-specific type mappings (e.g., MySQL `JSON`, SQL Server `NVARCHAR(MAX)` for JSON)

## Dependencies

### Required (add to pyproject.toml dependencies)

```toml
"psycopg2-binary>=2.9.9",  # PostgreSQL driver (already in dev deps, promote to main)
```

### Optional (for future dialects)

```toml
"pymysql>=1.1.1",         # MySQL (already in dev deps)
"pyodbc>=5.1.0",          # SQL Server (already in dev deps)  
"oracledb>=2.3.0",        # Oracle (already in dev deps)
```

For Phase 1, I recommend keeping PostgreSQL as the only required dependency and making others optional/installable via extras:

```toml
[project.optional-dependencies]
postgres = ["psycopg2-binary>=2.9.9"]
mysql = ["pymysql>=1.1.1"]
sqlserver = ["pyodbc>=5.1.0"]
oracle = ["oracledb>=2.3.0"]
all-sql = ["psycopg2-binary>=2.9.9", "pymysql>=1.1.1", "pyodbc>=5.1.0", "oracledb>=2.3.0"]
```

## Testing Strategy

### Unit Tests

Follow the established pattern from [test_avrotodb.py](test/test_avrotodb.py):

```python
class TestSqlToAvro(unittest.TestCase):
    
    def test_postgres_basic_types(self):
        """Test conversion of all PostgreSQL scalar types."""
        with PostgresContainer() as postgres:
            # Create test table with all types
            # Run sql2a
            # Validate Avro schema
            
    def test_postgres_json_inference(self):
        """Test JSON column schema inference."""
        with PostgresContainer() as postgres:
            # Insert JSON data with known structure
            # Run sql2a with inference
            # Validate inferred record type
            
    def test_postgres_xml_inference(self):
        """Test XML column schema inference."""
        ...
```

### Integration Tests

Test round-trip conversion: SQL → Avro → SQL should produce equivalent schemas.

```python
def test_roundtrip_postgres(self):
    """Test that sql2a -> a2sql produces equivalent schema."""
    # 1. Create original table in PostgreSQL
    # 2. Run sql2a to get Avro
    # 3. Run a2sql --dialect=postgres to get SQL
    # 4. Create table from generated SQL
    # 5. Compare column types
```

## Open Questions

1. **Connection String Security**: Should we support environment variable substitution (e.g., `${DB_PASSWORD}`) or a separate credentials file? The Kusto implementation uses Azure CLI authentication – we could add similar Azure Database for PostgreSQL support.

2. **Schema Filtering**: Should we support schema/namespace filtering (e.g., only convert tables in `public` schema)? 

3. **View Support**: Should we extract schemas from views, or only base tables?

4. **Materialized Views**: PostgreSQL-specific – include these?

5. **Partitioned Tables**: How should we handle PostgreSQL table partitioning? Likely treat as single table.

6. **Foreign Keys**: Should foreign key relationships be captured in Avro annotations for documentation purposes?

## Appendix: Example Usage

```bash
# Basic conversion
avrotize sql2a --connection-string "postgresql://user:pass@localhost:5432/mydb" \
    --namespace "com.example" \
    --out ./schemas/mydb.avsc

# Single table with JSON inference
avrotize sql2a --connection-string "postgresql://user:pass@localhost:5432/mydb" \
    --table-name "events" \
    --namespace "com.example.events" \
    --sample-size 500 \
    --out ./schemas/events.avsc

# CloudEvents table to xRegistry
avrotize sql2a --connection-string "postgresql://user:pass@localhost:5432/mydb" \
    --table-name "cloudevents_log" \
    --namespace "com.example" \
    --emit-xregistry \
    --out ./xregistry/cloudevents.json
```
