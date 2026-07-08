All notable changes to Avrotize are documented in this file.

## [Unreleased]

### Added

- **RAML 1.0 Data Types support** ([#261](https://github.com/clemensv/avrotize/issues/261)): Added `raml2a`, `a2raml`, `raml2s`, and `s2raml` converters for RAML Data Types via Avrotize Schema and JSON Structure. Phase 1 intentionally excludes full RAML API resource/method conversion, traits, resourceTypes, security schemes, annotations, and external `!include` expansion.
- **Cap'n Proto schema support** ([#257](https://github.com/clemensv/avrotize/issues/257)): Added `capnp2a`, `a2capnp`, `capnp2s`, and `s2capnp` conversions with documented primitive, union, group, ordinal, file id, import, and skipped-declaration mapping limitations.
- **Smithy IDL data-shape conversion** ([#259](https://github.com/clemensv/avrotize/issues/259)): Added `smithy2a`, `a2smithy`, `smithy2s`, and `s2smithy` commands for Smithy 2.0 IDL data shapes, including structures, unions, enums/intEnums, lists, maps, scalars, documentation/default/required traits, and JSON Structure bridging through Avrotize Schema. Service, operation, resource, and protocol modeling are explicitly out of phase-1 scope and are skipped gracefully.
- **Apache Thrift IDL conversion support** ([#255](https://github.com/clemensv/avrotize/issues/255)): Added Thrift IDL to/from Avrotize Schema and JSON Structure bridge commands (`thrift2a`, `a2thrift`, `thrift2s`, `s2thrift`) with documented mapping limitations for services, constants, includes, unions, sets, and non-string map keys.
- **`s2pq` — JSON Structure to Parquet schema conversion** (issue #48): converts a
  JSON Structure schema to an empty Parquet file whose footer carries the derived
  schema (mirrors the existing `a2pq` and `s2ib` converters). Supports
  `--record-type`, `--emit-cloudevents-columns`, and `--format parquet|schema`.
  JSON Structure primitive, temporal, numeric (`decimal` precision/scale),
  collection (`array`/`set`/`map`/`tuple`), `object` (incl. `$extends`), and
  `choice` types are mapped to PyArrow types; property-less objects become
  `map<string, string>`; required properties become non-nullable columns.
- **JSON Type Definition (JTD, RFC 8927) support** ([#260](https://github.com/clemensv/avrotize/issues/260)): Added `jtd2a`, `a2jtd`, `jtd2s`, and `s2jtd` commands for converting JTD to/from Avrotize Schema and JSON Structure. The implementation supports JTD type, enum, elements, values, properties/optionalProperties, discriminator/mapping, definitions/ref, and nullable forms, with documented Avro mapping limitations.
- **SurrealDB schema conversion** ([#270](https://github.com/clemensv/avrotize/issues/270)): Added `surreal2a` and `a2surreal` converters for SurrealQL `DEFINE TABLE` / `DEFINE FIELD` schemas, including nested dotted fields, arrays, sets, nullable `option<T>` fields, and Avro logical types for datetime, decimal, and UUID values.
- **CUE schema subset support** ([#258](https://github.com/clemensv/avrotize/issues/258)): Added `cue2a`, `a2cue`, `cue2s`, and `s2cue` converters for a documented practical CUE schema/type-definition subset, including Avro and JSON Structure bridge conversions.
- **FlatBuffers schema support** ([#256](https://github.com/clemensv/avrotize/issues/256)): Added `fbs2a`, `a2fbs`, `fbs2s`, and `s2fbs` conversions for FlatBuffers `.fbs` schemas, including namespace, table, struct, enum, union, vector, root type, required-field, and default-value handling. Documented mapping limitations for fixed-layout structs, enum integer values, unsigned 64-bit integers, `[ubyte]` vectors, and unsupported Avro/JSON Structure metadata.
- **C# `--openapi-generator-compat` output mode** ([#152](https://github.com/clemensv/avrotize/issues/152)): `a2cs` and `s2cs` can now emit OpenAPI Generator-compatible C# models with `Option<T>` optional properties and matching `System.Text.Json` converters.

## [3.6.1] - 2026-07-08

### Fixed

- **`s2a` now sanitizes non-identifier property and enum names (issues #382,
  #383, #385)**: JSON Structure permits property keys and enum values that are
  not valid Avro names (e.g. `dr-type`, `kar.id`, `1`, `N/A`, `KAR-MQTT`).
  Previously `s2a` emitted these verbatim, producing schemas that violate the
  Avro name grammar `[A-Za-z_][A-Za-z0-9_]*` — enum symbols were rejected even by
  the reference `avro.schema.parse`, and hyphenated field names are rejected by
  strict consumers (Java, Confluent Schema Registry). Property keys are now
  sanitized to valid Avro field names with the original key preserved as an Avro
  `aliases` entry; enum values are sanitized to valid symbols with collisions
  resolved by numeric suffix and the original wire values preserved in the enum
  `doc`. Tagged-union discriminator enums are sanitized the same way.
- **`s2k` now quotes non-identifier column names and JSONPaths (issue #382)**:
  Kusto column definitions and ingestion-mapping JSONPaths for non-identifier
  property keys are now bracket/quote-escaped (`['dr-type']`, `$['dr-type']`)
  instead of the previously invalid bare forms (`[dr-type]`, `$.dr-type`). Plain
  identifier columns are unchanged.

## [3.6.0] - 2026-06-29

### Added

- **JSON Structure `altnames`/`altenums` wire serialization (issue #384)**: the
  direct structure-to-language generators (Python, C#, Java, TypeScript, Go,
  Rust, C++) now honor the `JSONStructureAlternateNames` extension on the JSON
  wire. A spec-valid identifier property carrying `altnames: {"json": "wire-key"}`
  keeps the identifier as the language member name while serializing/deserializing
  the alternate `json` key (e.g. member `dr_type` ↔ wire `dr-type`). Renames apply
  recursively to nested objects, array items, and map values. Enums honor
  `altenums: {"json": {...}}`, mapping each member to its wire value with identity
  fallback for unmapped symbols (C++ emits `NLOHMANN_JSON_SERIALIZE_ENUM`). A
  rename is emitted only when the wire name differs from the identifier. Other
  purposes (`description`, `lang:*`, custom) never affect the wire.

## [3.5.12] - 2026-06-19

### Fixed

- **Kusto docstring escaping (issue #317)**: `a2k` and `s2k` now emit table
  and column docstrings as single-encoded KQL string literals, avoiding invalid
  double-escaped quote sequences in generated `.kql`.
- **`s2a` temporal types now emit valid standard Avro (#335)**: JSON Structure
  temporal types (`date`, `time`, `datetime`, `timestamp`, `duration`) previously
  mapped to a `string` base annotated with a reserved Avro logical type (e.g.
  `{"type":"string","logicalType":"timestamp-millis"}`), which is invalid Avro
  and is rejected by strict validators (e.g. Microsoft Fabric EventSchemaSet).
  They now use a new `rfc3339-*` logical-type family (e.g.
  `{"type":"string","logicalType":"rfc3339-timestamp-millis"}`). Because
  `rfc3339-*` names are not reserved Avro logical types, the output is valid
  standard Avro while the RFC 3339 textual value is preserved. `avrotojstruct`,
  `avrotojsons`, and the Avrotize validator recognize the new names (the legacy
  `string` + reserved-name form is still accepted on input). See
  `specs/avrotize-schema.md` §3.8.11.
- **s2a inline `object` produced invalid Avro / lost nested data (issue #336)**:
  A JSON Structure property typed as `{"type": "object"}` previously emitted the
  bare literal `"object"`, which is not a valid Avro type (parsers reject it,
  Microsoft Fabric `EventSchemaSet` validation fails). Objects *with* `properties`
  additionally lost their entire nested shape. Inline objects now convert
  correctly:
  - open / property-less object -> `{"type": "map", "values": "string"}`
    (honoring `additionalProperties` value type when present);
  - object with `properties` -> a nested Avro `record` (shape preserved, with
    unique record names derived from the enclosing property);
  - bare `"object"`/`"array"` union members map to their valid open Avro forms.
- **j2a dropped JSON Schema `format` logical types (issue #337)**: Converting a
  JSON Schema / OpenAPI field with `"format": "date-time"` produced a bare Avro
  `int` (32-bit — silent millisecond-timestamp overflow) and discarded the
  logical type. The post-processing step that inlines basic types now preserves
  logical-type annotations, so:
  - `date-time` -> `{"type": "long", "logicalType": "timestamp-millis"}`
  - `date` -> `{"type": "int", "logicalType": "date"}`
  - `time` -> `{"type": "int", "logicalType": "time-millis"}`
  - `uuid` -> `{"type": "string", "logicalType": "uuid"}`
  These are retained for required, optional (nullable), array-item, and nested
  field positions.
- **a2ts inline-enum test imports point to a non-existent path (issue #338)**:
  When a record has an inline enum field, j2a correctly scopes the enum into a
  `<Record>_types` sub-namespace, but the generated Jest test file imported the
  enum from a flattened path (e.g. `../src/ns/Type.js`) that does not exist and
  fails to compile. The TypeScript generator now records each class's
  fully-qualified import names and the test generator reuses them, so test
  imports resolve to the real source file. Added j2a regression tests asserting
  inline enums on same-named properties across sibling schemas produce distinct
  enums with their own symbols, and an a2ts test asserting every generated test
  import resolves to an existing file.
- **Avro to TypeScript contextual field names (issue #339)**: `a2ts` now preserves
  TypeScript contextual keywords such as `type`, `namespace`, `module`, and
  `readonly` when they are used as Avro field names instead of renaming them
  with a trailing underscore.
- **Avro to Python enum symbols (issue #354)**: `a2p` now sanitizes enum member
  names that are numeric, digit-prefixed, hyphenated, reserved words, or otherwise
  invalid Python identifiers while preserving the original Avro symbol strings as
  enum values.
- **Avro to Python dataclasses JSON date encoding (issue #355)**:
  `datetime.date` fields generated from Avro `date` logical types now use ISO
  `dataclasses-json` encoder/decoder wiring and a Marshmallow `fields.Date`,
  matching existing `datetime.datetime` timestamp handling.## [3.5.9] - 2026-06-04
### Changed

- **Extensible `AnyValue` record for the Avro "any" type**: The generic type
  (`generic_type()`) is now modeled as a union of all Avro primitives plus an
  empty record `avrotize.AnyValue` and recursive `array`/`map` types.  The
  `AnyValue` record can be extended via standard Avro schema evolution (adding
  fields with defaults), giving consumers a structured upgrade path instead of
  the previous brute-force 2-level nested primitives union.  All code
  generators (Java, C#, C++, Go, TypeScript, JavaScript, Python, Rust, Proto,
  JSON Schema, XSD, JSON Structure) map `AnyValue` to the language-native "any"
  type.
- **Union ordering for serialization correctness**: `AnyValue` name references
  are placed after `array`/`map` types in inner unions so that serializers
  (fastavro) resolve dict values to map before the empty record.
- **Backward compatibility**: `is_generic_avro_type()` detects both the new
  AnyValue-based format and the legacy 2-level nested primitives union.

### Fixed

- **int64/uint64/int128/uint128/decimal JSON string serialization (issue #346)**:
  All language code generators now serialize these types as JSON strings (not
  numbers) per the JSON Structure Core spec, since IEEE-754 doubles cannot
  represent the full 64-bit+ integer range without precision loss.
  - Python: `encoder`/`decoder` in `dataclasses_json.config`; `to_serializer_dict`/`from_serializer_dict` string conversion
  - Java: `@JsonFormat(shape = JsonFormat.Shape.STRING)` annotation
  - Go: `json:",string"` struct tag
  - Rust: Custom `serde` `string_as_number`/`option_string_as_number` modules
  - TypeScript: Custom `serializer`/`deserializer` in `@jsonMember` decorator
  - C++: Custom `to_json`/`from_json` friend functions with `std::to_string`
  - C#: Already handled via `JsonStructureConverters`
- **Rust code generator**: `serde_json` is now always included in `Cargo.toml`
  dependencies (required for `serde_json::Value` used by `AnyValue`).
- **Java code generator**: Skip generating per-type `Object` constructor in
  union classes to avoid a duplicate-constructor compilation error when
  `AnyValue` is a union member.
- **gzip compression test**: Set `PYTHONIOENCODING=utf-8` in subprocess
  environment to handle Unicode characters on Windows cp1252 consoles.
- **MCP CLI test**: Updated prompt format for `get_conversion` tool to match
  current Copilot CLI conventions.

## [3.5.7] - 2026-05-23

### Added

- **TMSL (Tabular Model Scripting Language) support** ([#254](https://github.com/clemensv/avrotize/pull/254)): New `a2tsml` and `s2tsml` converters that emit Microsoft Analysis Services TMSL scripts from Avrotize and JSON Structure schemas. New `validate-tmsl` command performs local structural validation against the documented TMSL object model — no Analysis Services instance required.
- **SQL primary-key / foreign-key metadata round-trip**: `sql2a` now captures foreign-key relationships from the source database and emits them as Avro `unique` and `foreignKeys` annotations. These flow through `a2s` / `s2a` and into `a2tsml` as TMSL `isKey` markers and `relationships` entries, preserving the relational model end-to-end.
- **Structure to Avro: PK/FK propagation** in `jstructtoavro` so the metadata survives a Structure→Avro→TMSL round-trip.
- **Avro to JSON Structure: PK/FK annotations** — `avrotojstruct` now emits `x-avrotize-unique` and `x-avrotize-foreignKeys` on record schemas when the source Avro carries `unique` / `foreignKeys` metadata.

## [3.5.6] - 2026-05-22

### Fixed

- **Structure to Python: integer enum members produced invalid Python** ([#315](https://github.com/clemensv/avrotize/issues/315)): Integer-valued JSON Structure enums (e.g. `"enum": [0, 1, 2]`) previously generated a class body with bare numeric literals like `0 = '0'` — a `SyntaxError` on import. `StructureToPython.generate_enum` now detects all-numeric enums and emits an `IntEnum` with member names prefixed `VALUE_<n>` (or `VALUE_NEG_<n>` for negatives), assigning the original integer literal as the value. String enum members whose values aren't valid identifiers (digit-prefixed, hyphenated, etc.) are also sanitized with the `VALUE_` prefix. Mirrors the `Value{value}` pattern already used by the C# converter.

## [3.5.5] - 2026-05-22

### Fixed

- **Structure to Python: `$root` + `definitions` wrapper silently dropped** ([#314](https://github.com/clemensv/avrotize/issues/314)): `convert_structure_schema_to_python` previously produced only `pyproject.toml` (no `src/` tree) for the canonical JSON Structure shape where the actual type lives at a JSON pointer like `#/definitions/de/wsv/pegelonline/Station`. The converter now resolves `$root` pointers, derives the namespace from the pointer path, and recursively walks the `definitions` tree so every named type is emitted. A defensive warning is logged when a top-level schema matches none of the recognized shapes, so future silent drops are visible.

## [3.5.4] - 2026-05-20

### Changed

- **Kusto to JSON Structure xRegistry output**: When `--emit-xregistry` is set, `k2j` now emits a proper xRegistry document with both `schemagroups` (versioned schemas) and `messagegroups`. CloudEvents-style tables (those with a `type` column whose docstring lists allowed values) get a `CloudEvents/1.0` envelope with required `id`/`source`/`time` and a fixed `type`; non-CloudEvents tables get a plain message group keyed by schema id. Inferrer `root` artifacts are dropped before attributes are applied.

### Dependencies

- Bumped 20 dependencies via Dependabot across Python (`avro`, `fastavro`, `confluent-kafka`, `mypy`, `pylint`, `jsonschema`, `xmlschema`, `pyiceberg`, `oracledb`, `azure-kusto-data`), Maven (`jackson-bom`, `jackson-core`, `jackson-databind`), NuGet (`System.Text.Json`, `System.Memory.Data`, `NUnit`, `Microsoft.NET.Test.Sdk`, `Dahomey.Cbor`), and GitHub Actions (`s4u/setup-maven-action`, `pypa/gh-action-pypi-publish`).

## [3.5.3] - 2026-05-19

### Added

- **`a2k` and `s2k`: `--qualified-table-names` option**: Generates Kusto table names using the entity's fully qualified namespace (e.g. `['com.example.Order']`). Table references, materialized views, and update-policy targets are bracket-quoted; mapping names use the dotted string as-is.
- **`a2k` and `s2k`: `--namespace` option**: Sets or overrides the namespace used for table-name qualification. For JSON Structure inputs, namespaces are derived from the `definitions/<seg>/<seg>/<type>` path when not explicitly provided.

### Notes

- Default output (without `--qualified-table-names`) is byte-identical to prior versions; reference-file tests are preserved.

## [3.5.2] - 2026-04-08

### Fixed

- **Structure to Kusto wrapped types**: Normalized wrapped `type` expressions before Kusto type mapping so nullable unions and ref-backed wrappers like `{"type": {"$ref": ...}}` resolve to the expected scalar or dynamic column types instead of falling back incorrectly.

## [3.5.1] - 2026-03-25

### Fixed

- **Java codegen: Jackson version mismatch**: `jackson-core` 2.21.0 does not exist on Maven Central — Jackson published it as `2.21` (without `.0`). Fixed the dependency version in `dependencies/java/jdk21/pom.xml` so generated Java projects resolve correctly.

## [3.5.0] - 2026-03-25

### Changed

- **C# code generation targets .NET 10**: Updated `TargetFramework` from `net9.0` to `net10.0` in all C# project and test project templates (avrotocsharp and structuretocsharp).
- **C# dependency manifest**: Created `net100` dependency directory with .NET 10 compatible package versions; all C# version lookups in `constants.py` now read from `net100`.
- **NUnit3TestAdapter**: Bumped from 6.1.0 to 6.2.0 to resolve .NET 10 SDK incompatibility (`Microsoft.Testing.Platform.MSBuild 2.0.2` rejects VSTest on .NET 10).
- **Test project templates**: NUnit3TestAdapter and Microsoft.NET.Test.Sdk references moved to `<PrivateAssets>all</PrivateAssets>` and relocated from the main project template to the test project template.
- **Dependabot**: Updated to monitor `net100` dependency directory.

## [3.4.4] - 2026-03-24

### Changed

- **MCP server performance**: Replaced `FastMCP` with a lightweight built-in JSON-RPC 2.0 stdio implementation, eliminating the heavy `mcp` library import (~2-3 s for uvicorn, starlette, httpx, pydantic). MCP server first-response time drops from ~1.7 s to ~0.2 s on Windows.
- **MCP server invocation**: VS Code extension and config files now launch the MCP server via `python -m avrotize.mcp_server`, bypassing the Windows `.exe` console-scripts wrapper overhead (~1.5 s).
- **MCP quickstart docs**: Updated `copilot-vscode-mcp-quickstart.md` to use the faster `python -m avrotize.mcp_server` invocation.

### Added

- **`avrotize-mcp` console script**: New direct entry point for the MCP server (`avrotize-mcp`), kept as a convenience alias.
- **Copilot VS Code MCP quickstart**: Added `copilot-vscode-mcp-quickstart.md` with step-by-step instructions for using Avrotize MCP from GitHub Copilot Chat.

### Fixed

- **C# test fixture NuGet versions**: Updated `System.Text.Json` and `System.Memory.Data` versions in C# test fixture `.csproj` files to match the dependency manifest, fixing CI build failures from NuGet downgrade errors.
- **Kusto emulator test reliability**: Replaced fixed `time.sleep(5)` with a retry loop (up to 60 s) for Kusto emulator container readiness, fixing transient CI failures.
- **Java code generation**: Jackson annotations version now read from the dependency manifest instead of being hardcoded.

### Dependencies

- Bumped `actions/download-artifact` from 7 to 8
- Bumped `actions/upload-artifact` from 6 to 7
- Bumped `com.fasterxml.jackson.core:jackson-core` from 2.18.2 to 2.21.0
- Bumped `com.fasterxml.jackson:jackson-bom` from 2.21.0 to 2.21.2
- Bumped `com.fasterxml.jackson.core:jackson-databind` from 2.21.0 to 2.21.2
- Bumped `org.apache.maven.plugins:maven-surefire-plugin` from 3.5.4 to 3.5.5
- Bumped `Dahomey.Cbor` from 1.26.0 to 1.26.1
- Bumped `Microsoft.NET.Test.Sdk` from 18.0.1 to 18.3.0
- Bumped `NUnit` from 4.4.0 to 4.5.1
- Bumped `System.Memory.Data` from 10.0.2 to 10.0.5
- Bumped `System.Text.Json` from 10.0.3 to 10.0.5
- Bumped `typescript` from 5.9.3 to 6.0.2

## [3.4.3] - 2026-02-20

### Fixed

- **MCP `json2s` input routing**: `run_conversion` now correctly maps `input_path` and `input_content` into positional command inputs (including `nargs` forms), fixing failures for conversions expecting a primary positional input.
- **VS Code extension Python fallback**: Extension runtime now aggressively discovers usable local Python 3.10+ installations (including common Windows install locations) and falls back to `python -m avrotize` when `avrotize` is not on `PATH`.

### Changed

- **Extension generator parity**: Updated `tools/editvscodeext.py` to emit the same Python discovery and fallback behavior so deploy-time regeneration preserves runtime fixes.

## [3.4.2] - 2026-02-20

### Added

- **MCP registry ownership marker**: Added `mcp-name: io.github.clemensv/avrotize` to `README.md` so PyPI package metadata satisfies MCP Registry ownership validation.

## [3.4.1] - 2026-02-20

### Added

- **VS Code extension MCP registration**: The Avrotize extension now explicitly contributes and registers itself as an MCP server provider (`avrotize.local-mcp`).
- **Extension MCP integration tests**: Added extension tests to verify MCP provider contribution metadata and stdio server definition output for `avrotize mcp`.
- **CI coverage for extension tests**: Added a `vscode-extension-test` workflow job to execute VS Code extension tests in CI.

### Changed

- **Extension installation path**: Extension-managed install/update now uses `avrotize[mcp]` to ensure MCP support is installed together with the CLI.
- **VS Code extension documentation**: Updated extension README installation instructions to include the MCP extra.

## [3.4.0] - 2026-02-20

### Added

- **Local MCP server mode (`avrotize mcp`)**: Avrotize can now run as a local MCP server over stdio.
- **MCP tool surface**: Added MCP tools for conversion discovery and execution:
  - `describe_capabilities`
  - `list_conversions`
  - `get_conversion`
  - `run_conversion`
- **Catalog metadata for MCP registration**:
  - Added official MCP Registry manifest at `server.json`
  - Added generic cross-catalog manifest at `mcp-server.json`
  - Added Microsoft/GitHub catalog listing template at `catalogs/microsoft-github-mcp.md`
- **CI validation for MCP registry manifest**: Added workflow `.github/workflows/validate-mcp-server-json.yml` to validate `server.json` on PRs.

### Changed

- **Command metadata**: Added `mcp` command in `avrotize/commands.json`.
- **CLI routing robustness**: Updated `avrotize/avrotize.py` input handling to support commands without positional input.
- **Packaging**: Added optional `mcp` extra in `pyproject.toml` (`mcp>=1.26.0`).
- **Documentation**: Expanded README with MCP usage, installation, and catalog registration guidance.

### Fixed

- **C# fixture dependency alignment**: Updated test fixtures to `System.Text.Json 10.0.3` to avoid downgrade conflicts and keep CI green.

### Dependencies

- Bumped `System.Text.Json` from `10.0.2` to `10.0.3`
- Bumped `org.junit.jupiter:junit-jupiter`
- Bumped `org.junit.jupiter:junit-jupiter-api`
- Bumped `org.junit.jupiter:junit-jupiter-engine`
- Bumped `com.fasterxml.jackson.core:jackson-databind`

## [3.3.2] - 2026-02-06

### Fixed

- **Choice discriminator validation**: Discriminator values must now be identifier-like strings (e.g., `PlayerTracking`, `goal_event`) rather than arbitrary values
  - Excludes numeric values (`"1"`, `"15"`, `"2024"`)
  - Excludes date/season patterns (`"2020/2021"`)
  - Excludes booleans and other non-identifier strings
  - Reduces false positives when inferring discriminated unions from JSON data

## [3.3.1] - 2026-02-06

### Fixed

- **CI workflow checkout conflict**: Fixed duplicate Authorization header error in GitHub Actions by disabling redundant checkout in `s4u/setup-maven-action`

- **Schema inference required field threshold**: Required fields now use strict threshold across all values, ensuring consistent detection

- **Array validation**: Arrays are now validated as single instances when schema type is array

- **Top-level array handling in json2s/json2a**: Top-level arrays are now correctly treated as array types instead of flattened instances

- **Choice discriminator detection**: Boolean-like values are now excluded from discriminator detection

- **Nullable type handling**: Improved nullable type handling with round-trip validation tests

- **Discriminator value casing**: Original casing of discriminator values is now preserved in choice inference

### Added

- **Dynamic field schema inference for k2a**: Added Kusto-to-Avro dynamic field schema inference
- **New k2s command**: Added Kusto-to-JSON-Structure command

### Dependencies

- Bumped `Apache.Avro` to 1.12.1
- Bumped `System.Text.Json` to 10.0.2
- Bumped `System.Memory.Data` to 10.0.2
- Bumped `s4u/setup-maven-action` to 1.19.0
- Bumped Jackson BOM to 2.21.0
- Bumped Maven compiler plugin to 3.15.0

## [3.3.0] - 2026-02-05

### Added

- **Recursive choice inference with `--choice-depth`**: New option to detect discriminated unions in nested object properties
  - `--choice-depth 1` (default): Only detect choices at root level
  - `--choice-depth 2+`: Recursively detect choices in nested object properties
  - Enables schema inference for complex nested data like event payloads with polymorphic fields

- **Response file support (`@file`)**: All commands now support response files for long argument lists
  - Use `@filelist.txt` to pass file paths from a text file (one per line)
  - Essential for processing hundreds of input files without hitting shell limits

- **Enum type inference with `--infer-enums`**: New option to detect enum types from repeated string values
  - Detects low-cardinality string fields as enums (max 50 unique values, <10% ratio)
  - Excludes ID-like fields, timestamps, and numeric strings from enum detection
  - Requires at least 2 unique values (single-value enums are not useful)

- **Datetime/date/time pattern detection**: String fields with ISO 8601 patterns are now typed correctly
  - Detects `datetime` for ISO 8601 timestamps (e.g., `2024-01-15T10:30:00Z`)
  - Detects `date` for date-only values (e.g., `2024-01-15`)
  - Detects `time` for time-only values (e.g., `10:30:00`)
  - Works on both single values and multi-value analysis

### Fixed

- **Nested array item schema merging**: Arrays within objects now merge items from all parent instances
  - Previously, only the first parent object's array items were analyzed
  - Now flattens and combines items from all parent objects before inferring schema
  - Fields appearing in some array items (e.g., ball tracking vs player tracking frames) are correctly included as optional
  - Required fields are those present in ALL array items across all parent objects

- **Multi-value type inference**: Property types now inferred from first non-null value, not first document
  - Fixes incorrect `type: null` inference when first document has missing fields

- **Choice variant field typing**: Variant-specific fields now correctly typed by scanning all documents

- **Discriminator field handling**: Discriminator fields no longer incorrectly marked as required in base types

- **Sparse data detection**: Improved correlation analysis distinguishes optional fields from variant-specific fields

## [3.2.2] - 2026-02-05

### Fixed

- **C# inline union code generation for `s2cs`**: Fixed code generation for inline unions with wrapped `$ref` format
  - `s2cs` now correctly handles `{"type": {"$ref": "..."}}` format emitted by `json2s --infer-choices`
  - Discriminator properties in base classes now marked with `[JsonIgnore]` to avoid conflict with `JsonPolymorphic` metadata
  - Derived class constructors now use correct property name (snake_case from base, not PascalCase)
  - Generated C# classes correctly deserialize and reserialize JSON data via `System.Text.Json`

### Added

- **`test_inline_union_json_roundtrip` test**: Comprehensive test verifying end-to-end flow:
  json2s infers schema → s2cs generates C# → C# compiles and handles JSON correctly

## [3.2.1] - 2026-02-04

### Fixed

- **JSON Structure `choice` type output**: Fixed inline union format to be spec-compliant
  - `choices` now uses map format (object) instead of array, per JSON Structure spec
  - Choice keys match actual discriminator values from source data (not PascalCase type names)
  - Inline unions now include `$extends` (abstract base type), `selector` (discriminator field), and `definitions`
  - Variant-specific fields are now correctly typed by scanning all documents (not just first sample)
  - Discriminator field no longer marked as required in base type (handled by selector mechanism)

- **Integer range detection**: Large integers beyond int32 range now infer as `double` instead of `integer`
  - Per JSON Structure spec, `int64` requires string encoding which doesn't match native JSON numbers
  - This allows source instances to validate correctly against inferred schemas

- **SDK validation in tests**: All json2s tests now use the json-structure SDK to validate both schemas and instances

## [3.2.0] - 2026-02-04

### Added

- **`--infer-choices` flag**: New option for `json2a`, `json2s`, and `sql2a` commands to automatically detect discriminated unions in JSON data during schema inference
  - Detects discriminator fields that correlate with schema variants (e.g., `event_type`, `kind`, `$type`, `__typename`)
  - Emits Avro unions with discriminator field defaults per variant type
  - Emits JSON Structure `choice` types with discriminator field defaults
  - Supports nested discriminators for envelope patterns (e.g., CloudEvents with typed payload, Kafka message envelopes)
  - Uses Jaccard similarity clustering on field signatures with two-pass refinement
  - Includes unique-ID penalty to avoid false positives on identifier fields

- **Choice Inference Module** (`choice_inference.py`): New algorithm for detecting discriminated unions
  - Clusters documents by field signature similarity
  - Identifies fields whose values correlate strongly with cluster membership
  - Detects nested discriminators in object fields (up to 2 levels deep)
  - Distinguishes sparse data (optional fields) from distinct variant types

### Changed

- **Schema inference classes**: `AvroSchemaInferrer` and `JsonStructureSchemaInferrer` now accept optional `infer_choices` parameter
- **SQL to Avro**: `sql2a` passes `infer_choices` to internal inferrer for JSON/XML column schema inference

## [3.1.0] - 2026-02-03

### Added

- **`validate` command**: New command to validate JSON instances against Avro or JSON Structure schemas
  - Validates JSON files against Avro schemas (`.avsc`) per the Avrotize Schema specification
  - Validates JSON files against JSON Structure schemas (`.jstruct.json`) via json-structure SDK
  - Supports single JSON objects, JSON arrays, and JSONL (newline-delimited JSON) formats
  - Auto-detects schema type from file extension
  - Supports `--quiet` mode for CI/CD pipelines (exit code 0 if valid, 1 if invalid)
  - Full Avro type support: primitives, records, enums, arrays, maps, fixed, unions
  - Logical type validation: decimal, uuid, date, time, timestamp, duration (both int/long and string base types)
  - Altnames/altsymbols support for JSON field and symbol mapping

- **Avro JSON Validator** (`avrovalidator.py`): New module implementing JSON validation against Avro schemas
  - Validates all Avro primitive types with proper range checking (int32, int64)
  - Validates complex types: record, enum, array, map, fixed
  - Validates logical types with RFC 3339 patterns for string-encoded dates/times
  - Supports field `altnames` and enum `altsymbols` for JSON encoding

### Fixed

- **Schema inference for sparse data**: Fields missing in some JSON/XML records are now correctly made nullable with `["null", type]` unions and `"default": null`
- **Type merging in inference**: Different primitive types (string vs null, int vs string) now correctly merge into union types instead of causing folding failures
- **Record folding**: `fold_record_types()` now tracks field presence across all records and makes partial fields optional
- **Shared inference logic**: JSON/XML schema inference now uses shared `SchemaInferrer` base class used by `json2a`, `json2s`, `xml2a`, `xml2s`, and `sql2a` commands

### Changed

- **SQL to Avro inference**: `sql2a` now uses the shared `AvroSchemaInferrer` via composition for JSON/XML column inference, ensuring consistent behavior across all inference commands

## [3.0.2] - 2025-12-14

### Fixed

- **Redshift SUPER type**: Use `SUPER` type for Redshift instead of `JSONB` (Redshift doesn't support JSONB) in `s2db` command
- **Iceberg schema output**: Output spec-compliant JSON schema format per Appendix C instead of binary format

### Added

- **Iceberg --format option**: New `--format` flag for Iceberg converters (`a2ib`, `s2ib`) to choose between:
  - `arrow` (default): Arrow IPC schema format for backward compatibility
  - `schema`: Human-readable JSON schema format per Iceberg Table Spec Appendix C
- **pyiceberg validation tests**: Comprehensive tests verifying Iceberg schema output can be parsed by `pyiceberg`

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
