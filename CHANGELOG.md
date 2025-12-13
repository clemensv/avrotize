# Changelog

All notable changes to Avrotize are documented in this file.

## [3.0.0] - 2025-12-13

### 🚀 Major New Feature: JSON Structure Schema Support

This release introduces **JSON Structure**, a new intermediate schema format that provides richer type semantics than Avro while remaining JSON-based. JSON Structure schemas use the `.struct.json` extension and support:

- **Discriminated unions** with `$discriminator` for polymorphic types
- **Add-in system** with `$offers`/`$uses` for schema composition
- **Validation keywords** from JSON Schema (minLength, maxLength, pattern, etc.)
- **Inheritance** via `$extends` for type hierarchies
- **Direct code generation** without requiring Avro as an intermediate format

All `s2*` commands convert FROM JSON Structure schemas. All `*2s` commands convert TO JSON Structure schemas.

### New Commands

#### Schema Conversions
| Command | Description |
|---------|-------------|
| `s2a` | JSON Structure → Avro schema |
| `a2s` | Avro schema → JSON Structure |
| `j2s` | JSON Schema → JSON Structure |
| `s2j` | JSON Structure → JSON Schema |
| `s2x` | JSON Structure → XSD schema |
| `s2cddl` | JSON Structure → CDDL schema |
| `cddl2s` | CDDL → JSON Structure |
| `oas2s` | OpenAPI 3.x → JSON Structure |
| `s2p` | JSON Structure → Protocol Buffers |
| `s2dp` | JSON Structure → Data Package |
| `a2dp` | Avro → Data Package |
| `s2ib` | JSON Structure → Iceberg schema |

#### Code Generation
| Command | Description |
|---------|-------------|
| `s2cs` | JSON Structure → C# classes |
| `s2java` | JSON Structure → Java classes |
| `s2py` | JSON Structure → Python classes |
| `s2go` | JSON Structure → Go structs |
| `s2ts` | JSON Structure → TypeScript classes |
| `s2js` | JSON Structure → JavaScript classes |
| `s2rust` | JSON Structure → Rust structs |
| `s2cpp` | JSON Structure → C++ classes |

#### Database Schema Generation
| Command | Description |
|---------|-------------|
| `s2sql` | JSON Structure → SQL schema |
| `s2cassandra` | JSON Structure → Cassandra schema |
| `s2k` | JSON Structure → Kusto (KQL) schema |
| `a2cassandra` | Avro → Cassandra schema |
| `a2dynamodb` | Avro → DynamoDB schema |
| `a2es` | Avro → Elasticsearch schema |
| `a2couchdb` | Avro → CouchDB schema |
| `a2neo4j` | Avro → Neo4j schema |
| `a2firebase` | Avro → Firebase schema |
| `a2cosmos` | Avro → CosmosDB schema |
| `a2hbase` | Avro → HBase schema |

#### Utility Commands
| Command | Description |
|---------|-------------|
| `s2md` | JSON Structure → Markdown documentation |
| `s2graphql` | JSON Structure → GraphQL schema |
| `s2csv` | JSON Structure → CSV schema |
| `a2csv` | Avro → CSV schema |
| `a2graphql` | Avro → GraphQL schema |

---

## [2.22.x] - 2025-12-12

### Fixed
- **Rust**: Fix `is_json_match` for `Option<T>` types and `serde_json::Value` fields
- **Rust**: Fix serde trait implementation for union enums and enum/union test code
- **Rust**: Conditional imports and proper chrono/uuid type handling
- **Java**: Correct test values for Instant, LocalDate, UUID and other time types
- **Python**: Fix decimal import, stdlib shadowing, uuid import issues
- **Go**: Handle reserved word package names and missing time import
- **SQL**: Add `maxLength` annotation support for string columns
- **TypeScript**: Lowercase `base_package` in import paths
- Fix structure converter import paths
- Fix remaining robustness test bugs

### Added
- **Java**: `createTestInstance()` method for generated classes
- Jackson JSR-310 support for Java date/time serialization

---

## [2.21.x] - 2025-12-10/11

### Added
- **TypeScript**: `createInstance()` method for JSON Structure converter
- Comprehensive test coverage for JSON Structure code generators
- **Go**: Time import when `time.Time` or `time.Duration` fields are present
- `convert_schema` method and `convert_structure_schema_to_typescript` export for JsonNode schema input
- **C#**: MessagePack serialization support
- **C#**: Protobuf-net support for code generation
- **JavaScript**: Conversion from JSON Structure with Jinja template-based code generation
- **OpenAPI**: OpenAPI 3.x to JSON-Structure converter
- Comprehensive code coverage for generated C# tests

### Fixed
- **Go**: Fix `time.Time` and `time.Duration` helper values (was using nil for value types)
- **TypeScript**: Fix enum `@jsonMember`, C# enum test casing, and schema list support
- **TypeScript**: Fix case mismatch in `generate_index` export paths
- **TypeScript**: ESM avro-js import compatibility
- **TypeScript**: Fix `createInstance()` for enums
- **C#**: Fix test generation for classes with union properties

### Dependencies
- MessagePack: 2.5.187 → 3.1.4
- jackson-annotations bump

---

## [2.20.x] - 2025-12-07/08

### Added
- **TypeScript**: `createInstance()` test helper generation
- Central dependency management with Dependabot integration

### Dependencies
Major dependency updates via Dependabot:
- apache-avro (Cargo): 0.21.0
- jest: 30.2.0
- actions/setup-node: 6
- actions/setup-java: 5
- actions/download-artifact: 6
- jackson-bom (Maven): 2.20.1
- junit-jupiter: 6.0.1
- Microsoft.NET.Test.Sdk: 18.0.1
- NUnit: 4.4.0
- NUnit3TestAdapter: 5.2.0
- Newtonsoft.Json: 13.0.4
- goavro/v2: 2.14.1
- confluent-kafka-go/v2: 2.12.0
- testify: 1.11.1
- pytest-cov: 7.0.0
- Various GitHub Actions updates

---

## [2.19.0] - 2025-12-01

### Added
- `s2cddl` command: Convert JSON Structure to CDDL schema
- `cddl2s` command: Convert CDDL schemas to JSON Structure
- `s2java` and `s2go` commands to CLI
- **Java**: Conversion from JSON Structure to Java classes with 100% spec coverage
- GitHub Pages workflow and gallery build script

### Fixed
- **C#**: Ensure serialization output conforms to JSON Structure schema
- **Java**: Enum generation to use SCREAMING_CASE for constants

---

## [2.18.x] - 2025-11-26/27

### Added
- **GraphQL**: JSON Structure to GraphQL conversion
- **SQL/NoSQL**: `structuretodb` for JSON structure to SQL and NoSQL conversion with inheritance support
- **Kusto**: `structuretokusto` for JSON to Kusto conversion with advanced feature support
- **Go**: Conversion from JSON Structure to Go structs
- **Markdown**: `structuretomd` for JSON Structure to Markdown conversion
- **Rust**: JSON structure to Rust conversion implementation
- **CSV**: `structuretocsv` for JSON Structure to CSV conversion
- **Data Package**: JSON Structure to Data Package conversion with 100% spec coverage
- **C++**: JSON Structure to C++ conversion (`s2cpp`)
- **Iceberg**: Conversion from JSON Structure to Iceberg schema
- **XSD**: `structuretoxsd` for JSON Structure to XSD conversion
- **Protocol Buffers**: JSON Structure to Protocol Buffers conversion with 100% spec coverage
- **TypeScript**: `structuretots` for JSON Structure to TypeScript conversion
- **Python**: JSON Structure to Python (`s2py`) converter with full feature parity
- `$offers`/`$uses` add-in system with dictionary-backed view pattern
- JSON Structure to Avro Schema converter with improved discriminated unions
- `structurize` package configuration for PyPI publishing
- Parallel test groups for faster CI

### Fixed
- **Java**: Union serialization, null handling, and comprehensive test generation
- Properly handle logical types based on base type
- Fix `to_byte_array()` to handle +gzip compression suffix in content types

---

## [2.16.x] - 2025-11-14 to 2025-11-21

### Added
- CI workflow for testing across Python 3.10-3.14
- `proto_root` parameter for buf-style protobuf import resolution

### Fixed
- Case-sensitive package name mismatch in pyproject.toml causing Linux build failures
- Proto3 enum parsing failure due to unhandled None tokens
- Support fixed type name references in Avro to JSON Schema conversion
- **C#**: Remove pragma suppressions and initialize non-nullable reference types

---

## [2.15.0] - 2025-11-04

### Changed
- Update dependencies and enable all tests

---

## [2.14.0] - 2025-11-02

### Fixed
- **TypeScript**: Remove JSON escaping and add avro-js type definitions
- **C#**: Remove spurious 'true' condition in IsJsonMatch methods
- Correct enum handling and hash code generation for value equality

---

## [2.13.x] - 2025-10-27 to 2025-10-30

### Added
- **C#**: Value equality for generated classes

---

## [2.12.0] - 2025-10-26

### Changed
- **C#**: Remove FluentAssertions dependency from test templates

---

## [2.11.0] - 2025-10-26

### Added
- **C#**: Optional `project_name` parameter for code generation

---

## [2.10.0] - 2025-10-24

### Fixed
- **Python**: Correct `__init__.py` generation to export PascalCase class names

---

## [2.9.0] - 2025-09-09

### Added
- `j2s` command: JSON Schema to JSON Structure
- `s2j` command: JSON Structure to JSON Schema

### Fixed
- Handle cases where fixed or bytes type does not have logicalType
- Fix import paths for json_structure_schema_validator in submodule

---

## [2.8.0] - 2025-09-06

### Added
- `a2s` command: Avro to JSON Structure

### Fixed
- **Java**: Capitalize enum values when converting Avro to Java
- **Java**: Handle restricted words in package names and fully qualified names

---

## [2.7.0] - 2025-09-02

### Fixed
- Kusto fixes
- Don't trim whitespace around commas in constructor params list

### Dependencies
- Bump dependencies

---

## [2.6.0] - 2025-08-28

### Added
- **C#**: XML serialization support (System.Xml.Serialization)

### Fixed
- Skip enums when processing a union

---

## [2.5.x] - 2025-08-20

### Fixed
- **TypeScript**: Union handling improvements
- **TypeScript**: Array annotation fix

---

## [2.4.x] - 2025-08-15

### Fixed
- TypedJSON annotations
- Nested structures handling

---

## [2.3.0] - 2025-08-10

### Changed
- **TypeScript**: Generator refactor
- Upgrade node components
- Python primitive type updates

### Fixed
- SQL Server test fix
- KQL test fixed, enum ordinals extension

---

## [2.2.x] - 2024-09-12 to 2024-09-23

### Added
- `k2a` "table_name" filter option
- `p2a` `--message-type` option
- JSONS altnames support

### Fixed
- Correctly handle optional fields in postinit
- Ensure KQL docstrings fit into limits
- Python min version downgrade to 3.10
- Python `__init__.py` and enum handling
- Dependency resolution
- Avro serialization fixups for List
- **C#**: Enum fixup

---

## [2.1.x] - 2024-06-25 to 2024-08-23

### Added
- **C#**: Unit test support
- **Python**: Unit tests and templatization
- XSD doc annotation
- VSCE (VS Code Extension) command groups
- NoSQL options for database conversion

### Fixed
- Proto fixes
- VSCE manifest and doc updates

---

## [2.0.0] - 2024-06-03

Initial v2.0.0 release with Avro-centric schema conversion tools.

### Commands in v2.0.0
| Command | Description |
|---------|-------------|
| `a2cpp` | Avro → C++ |
| `a2cs` | Avro → C# |
| `a2go` | Avro → Go |
| `a2j` | Avro → JSON Schema |
| `a2java` | Avro → Java |
| `a2js` | Avro → JSON |
| `a2k` | Avro → Kusto |
| `a2md` | Avro → Markdown |
| `a2mongo` | Avro → MongoDB |
| `a2p` | Avro → Protobuf |
| `a2pq` | Avro → Parquet |
| `a2py` | Avro → Python |
| `a2rust` | Avro → Rust |
| `a2sql` | Avro → SQL |
| `a2ts` | Avro → TypeScript |
| `a2x` | Avro → XSD |
| `a2ib` | Avro → Iceberg |
| `a2dp` | Avro → Data Package |
| `asn2a` | ASN.1 → Avro |
| `csv2a` | CSV → Avro |
| `j2a` | JSON Schema → Avro |
| `k2a` | Kusto → Avro |
| `kstruct2a` | Kusto Struct → Avro |
| `p2a` | Protobuf → Avro |
| `pcf` | Parsing Canonical Form |
| `pq2a` | Parquet → Avro |
| `x2a` | XSD → Avro |
