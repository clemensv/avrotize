#!/usr/bin/env python3
"""
Build script for generating gallery content for the GitHub Pages site.

This script runs various avrotize conversions and generates the gallery pages
with file trees and source content for the documentation site.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

# Directories - these are relative to the script location
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent

# Gallery output directory (in the project for local testing)
GALLERY_DIR = PROJECT_ROOT / "gallery"

# Test data directory (source schemas)
TEST_DIR = PROJECT_ROOT / "test"

# Temporary output directory
TMP_DIR = PROJECT_ROOT / "tmp" / "gallery"

# Gallery source schemas (curated examples)
GALLERY_SOURCES = GALLERY_DIR / "sources"

# Conversion definitions - one example per conversion feature
GALLERY_ITEMS = [
    # ============================================================
    # SOURCE FORMAT -> AVRO (Input conversions)
    # ============================================================
    {
        "id": "jsonschema-to-avro",
        "title": "JSON Schema -> Avro",
        "description": "E-commerce Order with nested types, refs, and polymorphic payment options",
        "source_file": "order.jsons",
        "source_path": GALLERY_SOURCES / "order.jsons",
        "source_language": "json",
        "conversions": [{"cmd": "j2a", "args": ["--out", "{out}/order.avsc"]}]
    },
    {
        "id": "xsd-to-avro",
        "title": "XSD -> Avro",
        "description": "ISO 20022 banking standard (Account Opening) to Avro Schema",
        "source_file": "acmt.003.001.08.xsd",
        "source_path": TEST_DIR / "xsd" / "acmt.003.001.08.xsd",
        "source_language": "xml",
        "conversions": [{"cmd": "x2a", "args": ["--out", "{out}/acmt.avsc"]}]
    },
    {
        "id": "proto-to-avro",
        "title": "Protobuf -> Avro",
        "description": "Chat messaging with oneof unions and nested messages",
        "source_file": "messaging.proto",
        "source_path": GALLERY_SOURCES / "messaging.proto",
        "source_language": "protobuf",
        "conversions": [{"cmd": "p2a", "args": ["--out", "{out}/messaging.avsc"]}]
    },
    {
        "id": "asn1-to-avro",
        "title": "ASN.1 -> Avro",
        "description": "Movie database with sequences, enums, and optional fields",
        "source_file": "movie.asn",
        "source_path": TEST_DIR / "asn1" / "movie.asn",
        "source_language": "asn1",
        "conversions": [{"cmd": "asn2a", "args": ["--out", "{out}/movie.avsc"]}]
    },
    {
        "id": "parquet-to-avro",
        "title": "Parquet -> Avro",
        "description": "Extract schema from Parquet file to Avro Schema",
        "source_file": "address.parquet",
        "source_path": TEST_DIR / "parquet" / "address.parquet",
        "source_language": "binary",
        "conversions": [{"cmd": "pq2a", "args": ["--out", "{out}/address.avsc"]}]
    },
    {
        "id": "kstruct-to-avro",
        "title": "Kafka Struct -> Avro",
        "description": "Kafka Connect Struct schema to Avro Schema",
        "source_file": "cardata.json",
        "source_path": TEST_DIR / "kstruct" / "cardata.json",
        "source_language": "json",
        "conversions": [{"cmd": "kstruct2a", "args": ["--out", "{out}/cardata.avsc"]}]
    },
    {
        "id": "struct-to-avro",
        "title": "JSON Structure -> Avro",
        "description": "Inventory management schema to Avro Schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2a", "args": ["--out", "{out}/inventory.avsc"]}]
    },
    {
        "id": "csv-to-avro",
        "title": "CSV -> Avro",
        "description": "Infer Avro schema from CSV data file",
        "source_file": "addresses.csv",
        "source_path": GALLERY_SOURCES / "addresses.csv",
        "source_language": "csv",
        "conversions": [{"cmd": "csv2a", "args": ["--out", "{out}/addresses.avsc"]}]
    },
    # NOTE: kusto-to-avro (k2a) requires a live Kusto cluster connection, 
    # so it cannot be demonstrated in the static gallery.
    
    # ============================================================
    # AVRO -> SCHEMA FORMATS
    # ============================================================
    {
        "id": "avro-to-proto",
        "title": "Avro -> Protobuf",
        "description": "Convert Avro telemetry schema to Protocol Buffers",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2p", "args": ["--out", "{out}/telemetry.proto"]}]
    },
    {
        "id": "avro-to-jsonschema",
        "title": "Avro -> JSON Schema",
        "description": "Convert Avro schema to JSON Schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2j", "args": ["--out", "{out}/telemetry.json"]}]
    },
    {
        "id": "avro-to-xsd",
        "title": "Avro -> XSD",
        "description": "Convert Avro schema to XML Schema Definition",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2x", "args": ["--out", "{out}/telemetry.xsd"]}]
    },
    {
        "id": "avro-to-structure",
        "title": "Avro -> JSON Structure",
        "description": "Convert Avro schema to JSON Structure format",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2s", "args": ["--out", "{out}/telemetry.struct.json"]}]
    },
    {
        "id": "avro-to-datapackage",
        "title": "Avro -> Datapackage",
        "description": "Convert Avro schema to Frictionless Datapackage",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2dp", "args": ["--out", "{out}/telemetry.datapackage.json"]}]
    },
    
    # ============================================================
    # AVRO -> CODE GENERATION
    # One example per annotation option, plus a combined example
    # ============================================================
    
    # --- Python ---
    {
        "id": "avro-to-python-avro",
        "title": "Avro -> Python (Avro)",
        "description": "Python dataclasses with Apache Avro serialization support",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2py", "args": ["--out", "{out}/python", "--avro-annotation"]}]
    },
    {
        "id": "avro-to-python-json",
        "title": "Avro -> Python (JSON)",
        "description": "Python dataclasses with dataclasses-json for JSON serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2py", "args": ["--out", "{out}/python", "--dataclasses-json-annotation"]}]
    },
    {
        "id": "avro-to-python-all",
        "title": "Avro -> Python (All)",
        "description": "Python dataclasses with Avro and dataclasses-json annotations",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2py", "args": ["--out", "{out}/python", "--avro-annotation", "--dataclasses-json-annotation"]}]
    },
    
    # --- C# ---
    {
        "id": "avro-to-csharp-avro",
        "title": "Avro -> C# (Avro)",
        "description": "C# classes with Apache Avro binary serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cs", "args": ["--out", "{out}/csharp", "--avro-annotation"]}]
    },
    {
        "id": "avro-to-csharp-stjson",
        "title": "Avro -> C# (System.Text.Json)",
        "description": "C# classes with System.Text.Json annotations",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cs", "args": ["--out", "{out}/csharp", "--system-text-json-annotation", "--pascal-properties"]}]
    },
    {
        "id": "avro-to-csharp-newtonsoft",
        "title": "Avro -> C# (Newtonsoft)",
        "description": "C# classes with Newtonsoft.Json annotations",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cs", "args": ["--out", "{out}/csharp", "--newtonsoft-json-annotation", "--pascal-properties"]}]
    },
    {
        "id": "avro-to-csharp-protobuf",
        "title": "Avro -> C# (protobuf-net)",
        "description": "C# classes with protobuf-net binary serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cs", "args": ["--out", "{out}/csharp", "--protobuf-net-annotation"]}]
    },
    {
        "id": "avro-to-csharp-xml",
        "title": "Avro -> C# (System.Xml)",
        "description": "C# classes with System.Xml serialization attributes",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cs", "args": ["--out", "{out}/csharp", "--system-xml-annotation"]}]
    },
    {
        "id": "avro-to-csharp-msgpack",
        "title": "Avro -> C# (MessagePack)",
        "description": "C# classes with MessagePack binary serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cs", "args": ["--out", "{out}/csharp", "--msgpack-annotation"]}]
    },
    {
        "id": "avro-to-csharp-cbor",
        "title": "Avro -> C# (CBOR)",
        "description": "C# classes with Dahomey.Cbor serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cs", "args": ["--out", "{out}/csharp", "--cbor-annotation"]}]
    },
    {
        "id": "avro-to-csharp-all",
        "title": "Avro -> C# (All)",
        "description": "C# classes with Avro, System.Text.Json, XML, MessagePack, CBOR, and protobuf-net",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cs", "args": ["--out", "{out}/csharp", "--avro-annotation", "--system-text-json-annotation", "--system-xml-annotation", "--msgpack-annotation", "--cbor-annotation", "--protobuf-net-annotation", "--pascal-properties"]}]
    },
    
    # --- Java ---
    {
        "id": "avro-to-java-avro",
        "title": "Avro -> Java (Avro)",
        "description": "Java POJOs with Apache Avro binary serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2java", "args": ["--out", "{out}/java", "--avro-annotation"]}]
    },
    {
        "id": "avro-to-java-jackson",
        "title": "Avro -> Java (Jackson)",
        "description": "Java POJOs with Jackson JSON annotations",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2java", "args": ["--out", "{out}/java", "--jackson-annotation", "--pascal-properties"]}]
    },
    {
        "id": "avro-to-java-all",
        "title": "Avro -> Java (All)",
        "description": "Java POJOs with Avro and Jackson annotations",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2java", "args": ["--out", "{out}/java", "--avro-annotation", "--jackson-annotation", "--pascal-properties"]}]
    },
    
    # --- TypeScript ---
    {
        "id": "avro-to-typescript-avro",
        "title": "Avro -> TypeScript (Avro)",
        "description": "TypeScript interfaces with Apache Avro serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2ts", "args": ["--out", "{out}/typescript", "--avro-annotation"]}]
    },
    {
        "id": "avro-to-typescript-typedjson",
        "title": "Avro -> TypeScript (TypedJSON)",
        "description": "TypeScript interfaces with TypedJSON runtime type info",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2ts", "args": ["--out", "{out}/typescript", "--typedjson-annotation"]}]
    },
    {
        "id": "avro-to-typescript-all",
        "title": "Avro -> TypeScript (All)",
        "description": "TypeScript interfaces with Avro and TypedJSON annotations",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2ts", "args": ["--out", "{out}/typescript", "--avro-annotation", "--typedjson-annotation"]}]
    },
    
    # --- JavaScript ---
    {
        "id": "avro-to-javascript",
        "title": "Avro -> JavaScript",
        "description": "JavaScript classes (no annotations)",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2js", "args": ["--out", "{out}/javascript"]}]
    },
    {
        "id": "avro-to-javascript-avro",
        "title": "Avro -> JavaScript (Avro)",
        "description": "JavaScript classes with Apache Avro serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2js", "args": ["--out", "{out}/javascript", "--avro-annotation"]}]
    },
    
    # --- Rust ---
    {
        "id": "avro-to-rust-avro",
        "title": "Avro -> Rust (Avro)",
        "description": "Rust structs with Apache Avro binary serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2rust", "args": ["--out", "{out}/rust", "--avro-annotation"]}]
    },
    {
        "id": "avro-to-rust-json",
        "title": "Avro -> Rust (Serde JSON)",
        "description": "Rust structs with serde JSON serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2rust", "args": ["--out", "{out}/rust", "--json-annotation"]}]
    },
    {
        "id": "avro-to-rust-all",
        "title": "Avro -> Rust (All)",
        "description": "Rust structs with Avro and serde JSON support",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2rust", "args": ["--out", "{out}/rust", "--avro-annotation", "--json-annotation"]}]
    },
    
    # --- Go ---
    {
        "id": "avro-to-go-avro",
        "title": "Avro -> Go (Avro)",
        "description": "Go structs with Apache Avro binary serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2go", "args": ["--out", "{out}/go", "--avro-annotation"]}]
    },
    {
        "id": "avro-to-go-json",
        "title": "Avro -> Go (JSON)",
        "description": "Go structs with JSON struct tags",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2go", "args": ["--out", "{out}/go", "--json-annotation"]}]
    },
    {
        "id": "avro-to-go-all",
        "title": "Avro -> Go (All)",
        "description": "Go structs with Avro and JSON tags",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2go", "args": ["--out", "{out}/go", "--avro-annotation", "--json-annotation"]}]
    },
    
    # --- C++ ---
    {
        "id": "avro-to-cpp",
        "title": "Avro -> C++",
        "description": "C++ classes (no annotations)",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cpp", "args": ["--out", "{out}/cpp"]}]
    },
    {
        "id": "avro-to-cpp-avro",
        "title": "Avro -> C++ (Avro)",
        "description": "C++ classes with Apache Avro binary serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cpp", "args": ["--out", "{out}/cpp", "--avro-annotation"]}]
    },
    {
        "id": "avro-to-cpp-json",
        "title": "Avro -> C++ (JSON)",
        "description": "C++ classes with nlohmann/json serialization",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cpp", "args": ["--out", "{out}/cpp", "--json-annotation"]}]
    },
    {
        "id": "avro-to-cpp-all",
        "title": "Avro -> C++ (All)",
        "description": "C++ classes with Avro and JSON support",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cpp", "args": ["--out", "{out}/cpp", "--avro-annotation", "--json-annotation"]}]
    },
    
    # ============================================================
    # AVRO -> DATABASE SCHEMAS
    # ============================================================
    {
        "id": "avro-to-sql-postgres",
        "title": "Avro -> PostgreSQL",
        "description": "IoT telemetry to PostgreSQL table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "postgres", "--out", "{out}/telemetry.sql"]}]
    },
    {
        "id": "avro-to-kusto",
        "title": "Avro -> Kusto",
        "description": "IoT telemetry to Azure Data Explorer (Kusto) table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2k", "args": ["--out", "{out}/telemetry.kql"]}]
    },
    {
        "id": "avro-to-parquet",
        "title": "Avro -> Parquet",
        "description": "IoT telemetry to Parquet schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2pq", "args": ["--out", "{out}/telemetry.parquet.json"]}]
    },
    {
        "id": "avro-to-iceberg",
        "title": "Avro -> Iceberg",
        "description": "IoT telemetry to Apache Iceberg schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2ib", "args": ["--out", "{out}/telemetry.iceberg.json"]}]
    },
    {
        "id": "avro-to-mongodb",
        "title": "Avro -> MongoDB",
        "description": "IoT telemetry to MongoDB JSON Schema validation",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2mongo", "args": ["--out", "{out}/telemetry.mongodb.json"]}]
    },
    {
        "id": "avro-to-cassandra",
        "title": "Avro -> Cassandra",
        "description": "IoT telemetry to Cassandra CQL schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cassandra", "args": ["--out", "{out}/telemetry.cql"]}]
    },
    {
        "id": "avro-to-dynamodb",
        "title": "Avro -> DynamoDB",
        "description": "IoT telemetry to AWS DynamoDB schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2dynamodb", "args": ["--out", "{out}/telemetry.dynamodb.json"]}]
    },
    {
        "id": "avro-to-elasticsearch",
        "title": "Avro -> Elasticsearch",
        "description": "IoT telemetry to Elasticsearch mapping",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2es", "args": ["--out", "{out}/telemetry.es.json"]}]
    },
    {
        "id": "avro-to-cosmosdb",
        "title": "Avro -> CosmosDB",
        "description": "IoT telemetry to Azure CosmosDB schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2cosmos", "args": ["--out", "{out}/telemetry.cosmos.json"]}]
    },
    {
        "id": "avro-to-neo4j",
        "title": "Avro -> Neo4j",
        "description": "IoT telemetry to Neo4j Cypher schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2neo4j", "args": ["--out", "{out}/telemetry.cypher"]}]
    },
    {
        "id": "avro-to-firebase",
        "title": "Avro -> Firebase",
        "description": "IoT telemetry to Firebase security rules schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2firebase", "args": ["--out", "{out}/telemetry.firebase.json"]}]
    },
    {
        "id": "avro-to-couchdb",
        "title": "Avro -> CouchDB",
        "description": "IoT telemetry to CouchDB validation schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2couchdb", "args": ["--out", "{out}/telemetry.couchdb.json"]}]
    },
    {
        "id": "avro-to-hbase",
        "title": "Avro -> HBase",
        "description": "IoT telemetry to HBase schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2hbase", "args": ["--out", "{out}/telemetry.hbase.json"]}]
    },
    
    # ============================================================
    # AVRO -> DOCUMENTATION
    # ============================================================
    {
        "id": "avro-to-markdown",
        "title": "Avro -> Markdown",
        "description": "IoT telemetry to Markdown documentation",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2md", "args": ["--out", "{out}/telemetry.md"]}]
    },
    
    # ============================================================
    # JSON STRUCTURE -> SCHEMA FORMATS
    # ============================================================
    {
        "id": "struct-to-jsonschema",
        "title": "Structure -> JSON Schema",
        "description": "Inventory management to JSON Schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2j", "args": ["--out", "{out}/inventory.schema.json"]}]
    },
    {
        "id": "struct-to-xsd",
        "title": "Structure -> XSD",
        "description": "Inventory management to XML Schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2x", "args": ["--out", "{out}/inventory.xsd"]}]
    },
    {
        "id": "struct-to-graphql",
        "title": "Structure -> GraphQL",
        "description": "Inventory management to GraphQL type definitions",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2graphql", "args": ["--out", "{out}/schema.graphql"]}]
    },
    {
        "id": "struct-to-proto",
        "title": "Structure -> Protobuf",
        "description": "Inventory management to Protocol Buffers",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2p", "args": ["--out", "{out}/inventory.proto"]}]
    },
    {
        "id": "struct-to-datapackage",
        "title": "Structure -> Datapackage",
        "description": "Inventory management to Frictionless Datapackage",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2dp", "args": ["--out", "{out}/inventory.datapackage.json"]}]
    },
    
    # ============================================================
    # JSON STRUCTURE -> CODE GENERATION
    # One example per annotation option, plus a combined example
    # ============================================================
    
    # --- Python ---
    {
        "id": "struct-to-python-avro",
        "title": "Structure -> Python (Avro)",
        "description": "Python dataclasses with Apache Avro serialization support",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2py", "args": ["--out", "{out}/python", "--avro-annotation"]}]
    },
    {
        "id": "struct-to-python-json",
        "title": "Structure -> Python (JSON)",
        "description": "Python dataclasses with dataclasses-json for JSON serialization",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2py", "args": ["--out", "{out}/python", "--dataclasses-json-annotation"]}]
    },
    {
        "id": "struct-to-python-all",
        "title": "Structure -> Python (All)",
        "description": "Python dataclasses with Avro and dataclasses-json annotations",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2py", "args": ["--out", "{out}/python", "--avro-annotation", "--dataclasses-json-annotation"]}]
    },
    
    # --- C# ---
    {
        "id": "struct-to-csharp-avro",
        "title": "Structure -> C# (Avro)",
        "description": "C# classes with Apache Avro binary serialization",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2cs", "args": ["--out", "{out}/csharp", "--avro-annotation"]}]
    },
    {
        "id": "struct-to-csharp-stjson",
        "title": "Structure -> C# (System.Text.Json)",
        "description": "C# classes with System.Text.Json annotations",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2cs", "args": ["--out", "{out}/csharp", "--system-text-json-annotation", "--pascal-properties"]}]
    },
    {
        "id": "struct-to-csharp-newtonsoft",
        "title": "Structure -> C# (Newtonsoft)",
        "description": "C# classes with Newtonsoft.Json annotations",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2cs", "args": ["--out", "{out}/csharp", "--newtonsoft-json-annotation", "--pascal-properties"]}]
    },
    {
        "id": "struct-to-csharp-xml",
        "title": "Structure -> C# (System.Xml)",
        "description": "C# classes with System.Xml serialization attributes",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2cs", "args": ["--out", "{out}/csharp", "--system-xml-annotation"]}]
    },
    {
        "id": "struct-to-csharp-all",
        "title": "Structure -> C# (All)",
        "description": "C# classes with Avro, System.Text.Json, and XML annotations",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2cs", "args": ["--out", "{out}/csharp", "--avro-annotation", "--system-text-json-annotation", "--system-xml-annotation", "--pascal-properties"]}]
    },
    
    # --- Java ---
    {
        "id": "struct-to-java",
        "title": "Structure -> Java",
        "description": "Java POJOs (no annotations)",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2java", "args": ["--out", "{out}/java"]}]
    },
    {
        "id": "struct-to-java-jackson",
        "title": "Structure -> Java (Jackson)",
        "description": "Java POJOs with Jackson JSON annotations",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2java", "args": ["--out", "{out}/java", "--jackson-annotation", "--pascal-properties"]}]
    },
    
    # --- TypeScript ---
    {
        "id": "struct-to-typescript",
        "title": "Structure -> TypeScript",
        "description": "TypeScript interfaces (no annotations)",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2ts", "args": ["--out", "{out}/typescript"]}]
    },
    {
        "id": "struct-to-typescript-avro",
        "title": "Structure -> TypeScript (Avro)",
        "description": "TypeScript interfaces with Apache Avro serialization",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2ts", "args": ["--out", "{out}/typescript", "--avro-annotation"]}]
    },
    {
        "id": "struct-to-typescript-typedjson",
        "title": "Structure -> TypeScript (TypedJSON)",
        "description": "TypeScript interfaces with TypedJSON runtime type info",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2ts", "args": ["--out", "{out}/typescript", "--typedjson-annotation"]}]
    },
    {
        "id": "struct-to-typescript-all",
        "title": "Structure -> TypeScript (All)",
        "description": "TypeScript interfaces with Avro and TypedJSON annotations",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2ts", "args": ["--out", "{out}/typescript", "--avro-annotation", "--typedjson-annotation"]}]
    },
    
    # --- JavaScript ---
    {
        "id": "struct-to-javascript",
        "title": "Structure -> JavaScript",
        "description": "JavaScript classes (no annotations)",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2js", "args": ["--out", "{out}/javascript"]}]
    },
    {
        "id": "struct-to-javascript-avro",
        "title": "Structure -> JavaScript (Avro)",
        "description": "JavaScript classes with Apache Avro serialization",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2js", "args": ["--out", "{out}/javascript", "--avro-annotation"]}]
    },
    
    # --- Rust ---
    {
        "id": "struct-to-rust",
        "title": "Structure -> Rust",
        "description": "Rust structs (no annotations)",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2rust", "args": ["--out", "{out}/rust"]}]
    },
    {
        "id": "struct-to-rust-json",
        "title": "Structure -> Rust (Serde JSON)",
        "description": "Rust structs with serde JSON serialization",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2rust", "args": ["--out", "{out}/rust", "--json-annotation"]}]
    },
    
    # --- Go ---
    {
        "id": "struct-to-go",
        "title": "Structure -> Go",
        "description": "Go structs (no annotations)",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2go", "args": ["--out", "{out}/go"]}]
    },
    {
        "id": "struct-to-go-avro",
        "title": "Structure -> Go (Avro)",
        "description": "Go structs with Apache Avro binary serialization",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2go", "args": ["--out", "{out}/go", "--avro-annotation"]}]
    },
    {
        "id": "struct-to-go-json",
        "title": "Structure -> Go (JSON)",
        "description": "Go structs with JSON struct tags",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2go", "args": ["--out", "{out}/go", "--json-annotation"]}]
    },
    {
        "id": "struct-to-go-all",
        "title": "Structure -> Go (All)",
        "description": "Go structs with Avro and JSON tags",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2go", "args": ["--out", "{out}/go", "--avro-annotation", "--json-annotation"]}]
    },
    
    # --- C++ ---
    {
        "id": "struct-to-cpp",
        "title": "Structure -> C++",
        "description": "C++ classes (no annotations)",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2cpp", "args": ["--out", "{out}/cpp"]}]
    },
    {
        "id": "struct-to-cpp-json",
        "title": "Structure -> C++ (JSON)",
        "description": "C++ classes with nlohmann/json serialization",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2cpp", "args": ["--out", "{out}/cpp", "--json-annotation"]}]
    },
    
    # ============================================================
    # JSON STRUCTURE -> DATABASE/DATA FORMATS
    # ============================================================
    {
        "id": "struct-to-sql",
        "title": "Structure -> PostgreSQL",
        "description": "Inventory management to PostgreSQL table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "postgres", "--out", "{out}/inventory.sql"]}]
    },
    {
        "id": "struct-to-cassandra",
        "title": "Structure -> Cassandra",
        "description": "Inventory management to Cassandra CQL schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2cassandra", "args": ["--out", "{out}/inventory.cql"]}]
    },
    {
        "id": "struct-to-iceberg",
        "title": "Structure -> Iceberg",
        "description": "Inventory management to Apache Iceberg schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2ib", "args": ["--out", "{out}/inventory.iceberg.json"]}]
    },
    
    # ============================================================
    # JSON STRUCTURE -> DOCUMENTATION
    # ============================================================
    {
        "id": "struct-to-markdown",
        "title": "Structure -> Markdown",
        "description": "Inventory management to Markdown documentation",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2md", "args": ["--out", "{out}/inventory.md"]}]
    },
    
    # ============================================================
    # JSON SCHEMA -> JSON STRUCTURE
    # ============================================================
    {
        "id": "jsonschema-to-struct",
        "title": "JSON Schema -> Structure",
        "description": "E-commerce Order to JSON Structure format",
        "source_file": "order.jsons",
        "source_path": GALLERY_SOURCES / "order.jsons",
        "source_language": "json",
        "conversions": [{"cmd": "j2s", "args": ["--out", "{out}/order.struct.json"]}]
    },
    
    # ============================================================
    # ADDITIONAL SQL DIALECTS
    # ============================================================
    {
        "id": "avro-to-sql-mysql",
        "title": "Avro -> MySQL",
        "description": "IoT telemetry to MySQL table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "mysql", "--out", "{out}/telemetry_mysql.sql"]}]
    },
    {
        "id": "avro-to-sql-sqlserver",
        "title": "Avro -> SQL Server",
        "description": "IoT telemetry to SQL Server table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "sqlserver", "--out", "{out}/telemetry_mssql.sql"]}]
    },
    {
        "id": "avro-to-sql-sqlite",
        "title": "Avro -> SQLite",
        "description": "IoT telemetry to SQLite table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "sqlite", "--out", "{out}/telemetry_sqlite.sql"]}]
    },
    {
        "id": "avro-to-sql-oracle",
        "title": "Avro -> Oracle",
        "description": "IoT telemetry to Oracle database table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "oracle", "--out", "{out}/telemetry_oracle.sql"]}]
    },
    {
        "id": "avro-to-sql-mariadb",
        "title": "Avro -> MariaDB",
        "description": "IoT telemetry to MariaDB table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "mariadb", "--out", "{out}/telemetry_maria.sql"]}]
    },
    
    # ============================================================
    # CSV OUTPUT
    # ============================================================
    {
        "id": "struct-to-csv",
        "title": "Structure -> CSV Template",
        "description": "Generate CSV template from JSON Structure schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2csv", "args": ["--out", "{out}/inventory_template.csv"]}]
    },
    
    # ============================================================
    # STRUCTURE -> KUSTO
    # ============================================================
    {
        "id": "struct-to-kusto",
        "title": "Structure -> Kusto",
        "description": "Inventory management to Azure Data Explorer table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2k", "args": ["--out", "{out}/inventory.kql"]}]
    },
    
    # ============================================================
    # MISSING SQL DIALECTS - a2sql
    # ============================================================
    {
        "id": "avro-to-sql-bigquery",
        "title": "Avro -> BigQuery",
        "description": "IoT telemetry to Google BigQuery table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "bigquery", "--out", "{out}/telemetry_bigquery.sql"]}]
    },
    {
        "id": "avro-to-sql-snowflake",
        "title": "Avro -> Snowflake",
        "description": "IoT telemetry to Snowflake table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "snowflake", "--out", "{out}/telemetry_snowflake.sql"]}]
    },
    {
        "id": "avro-to-sql-redshift",
        "title": "Avro -> Redshift",
        "description": "IoT telemetry to AWS Redshift table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "redshift", "--out", "{out}/telemetry_redshift.sql"]}]
    },
    {
        "id": "avro-to-sql-db2",
        "title": "Avro -> DB2",
        "description": "IoT telemetry to IBM DB2 table schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "db2", "--out", "{out}/telemetry_db2.sql"]}]
    },
    
    # ============================================================
    # MISSING SQL DIALECTS - s2sql
    # ============================================================
    {
        "id": "struct-to-sql-mysql",
        "title": "Structure -> MySQL",
        "description": "Inventory management to MySQL table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "mysql", "--out", "{out}/inventory_mysql.sql"]}]
    },
    {
        "id": "struct-to-sql-sqlserver",
        "title": "Structure -> SQL Server",
        "description": "Inventory management to SQL Server table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "sqlserver", "--out", "{out}/inventory_mssql.sql"]}]
    },
    {
        "id": "struct-to-sql-sqlite",
        "title": "Structure -> SQLite",
        "description": "Inventory management to SQLite table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "sqlite", "--out", "{out}/inventory_sqlite.sql"]}]
    },
    {
        "id": "struct-to-sql-oracle",
        "title": "Structure -> Oracle",
        "description": "Inventory management to Oracle database table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "oracle", "--out", "{out}/inventory_oracle.sql"]}]
    },
    {
        "id": "struct-to-sql-mariadb",
        "title": "Structure -> MariaDB",
        "description": "Inventory management to MariaDB table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "mariadb", "--out", "{out}/inventory_maria.sql"]}]
    },
    {
        "id": "struct-to-sql-bigquery",
        "title": "Structure -> BigQuery",
        "description": "Inventory management to Google BigQuery table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "bigquery", "--out", "{out}/inventory_bigquery.sql"]}]
    },
    {
        "id": "struct-to-sql-snowflake",
        "title": "Structure -> Snowflake",
        "description": "Inventory management to Snowflake table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "snowflake", "--out", "{out}/inventory_snowflake.sql"]}]
    },
    {
        "id": "struct-to-sql-redshift",
        "title": "Structure -> Redshift",
        "description": "Inventory management to AWS Redshift table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "redshift", "--out", "{out}/inventory_redshift.sql"]}]
    },
    {
        "id": "struct-to-sql-db2",
        "title": "Structure -> DB2",
        "description": "Inventory management to IBM DB2 table schema",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "db2", "--out", "{out}/inventory_db2.sql"]}]
    },
    
    # ============================================================
    # MISSING COMMANDS
    # ============================================================
    {
        "id": "avro-to-csv",
        "title": "Avro -> CSV Template",
        "description": "Generate CSV template from Avro schema",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2csv", "args": ["--out", "{out}/telemetry_template.csv"]}]
    },
    {
        "id": "avro-to-graphql",
        "title": "Avro -> GraphQL",
        "description": "IoT telemetry to GraphQL type definitions",
        "source_file": "telemetry.avsc",
        "source_path": GALLERY_SOURCES / "telemetry.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2graphql", "args": ["--out", "{out}/telemetry.graphql"]}]
    },
    {
        "id": "struct-to-javascript",
        "title": "Structure -> JavaScript",
        "description": "JavaScript classes with Avro serialization support",
        "source_file": "inventory.struct.json",
        "source_path": GALLERY_SOURCES / "inventory.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2js", "args": ["--out", "{out}/javascript", "--avro-annotation"]}]
    },
    
    # ============================================================
    # CLOUDEVENTS DATABASE EXAMPLES (Multi-table with CE columns)
    # ============================================================
    {
        "id": "avro-to-sql-postgres-cloudevents",
        "title": "Avro -> PostgreSQL (CloudEvents)",
        "description": "E-commerce multi-table schema with CloudEvents envelope columns",
        "source_file": "ecommerce.avsc",
        "source_path": GALLERY_SOURCES / "ecommerce.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2sql", "args": ["--dialect", "postgres", "--emit-cloudevents-columns", "--out", "{out}/ecommerce_ce.sql"]}]
    },
    {
        "id": "avro-to-kusto-cloudevents",
        "title": "Avro -> Kusto (CloudEvents)",
        "description": "E-commerce multi-table schema with CloudEvents envelope columns",
        "source_file": "ecommerce.avsc",
        "source_path": GALLERY_SOURCES / "ecommerce.avsc",
        "source_language": "json",
        "conversions": [{"cmd": "a2k", "args": ["--emit-cloudevents-columns", "--out", "{out}/ecommerce_ce.kql"]}]
    },
    {
        "id": "struct-to-sql-postgres-cloudevents",
        "title": "Structure -> PostgreSQL (CloudEvents)",
        "description": "E-commerce multi-table schema with CloudEvents envelope columns",
        "source_file": "ecommerce.struct.json",
        "source_path": GALLERY_SOURCES / "ecommerce.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2sql", "args": ["--dialect", "postgres", "--emit-cloudevents-columns", "--out", "{out}/ecommerce_ce.sql"]}]
    },
    {
        "id": "struct-to-kusto-cloudevents",
        "title": "Structure -> Kusto (CloudEvents)",
        "description": "E-commerce multi-table schema with CloudEvents envelope columns",
        "source_file": "ecommerce.struct.json",
        "source_path": GALLERY_SOURCES / "ecommerce.struct.json",
        "source_language": "json",
        "conversions": [{"cmd": "s2k", "args": ["--emit-cloudevents-columns", "--out", "{out}/ecommerce_ce.kql"]}]
    },
]


def run_avrotize(cmd: str, input_file: Path | str, args: list[str], cwd: Path) -> bool:
    """Run an avrotize command."""
    full_cmd = ["avrotize", cmd, str(input_file)] + args
    print(f"  Running: {' '.join(full_cmd)}")
    try:
        result = subprocess.run(full_cmd, cwd=cwd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print(f"  Error: {result.stderr}")
            return False
        return True
    except subprocess.TimeoutExpired:
        print("  Error: Command timed out")
        return False
    except Exception as e:
        print(f"  Error: {e}")
        return False


def build_file_tree(directory: Path, base_path: Path) -> list[dict[str, Any]]:
    """Build a file tree structure from a directory."""
    tree = []
    
    if not directory.exists():
        return tree
    
    items = sorted(directory.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    
    for item in items:
        rel_path = item.relative_to(base_path)
        
        if item.is_dir():
            children = build_file_tree(item, base_path)
            if children:  # Only include non-empty directories
                tree.append({
                    "name": item.name,
                    "type": "folder",
                    "path": str(rel_path),
                    "children": children
                })
        else:
            tree.append({
                "name": item.name,
                "type": "file",
                "path": str(rel_path)
            })
    
    return tree


def get_language_for_extension(ext: str) -> str:
    """Get Prism.js language identifier for a file extension."""
    lang_map = {
        ".json": "json",
        ".avsc": "json",
        ".struct.json": "json",
        ".py": "python",
        ".cs": "csharp",
        ".java": "java",
        ".ts": "typescript",
        ".js": "javascript",
        ".go": "go",
        ".rs": "rust",
        ".cpp": "cpp",
        ".hpp": "cpp",
        ".h": "c",
        ".proto": "protobuf",
        ".sql": "sql",
        ".kql": "kusto",
        ".cql": "sql",
        ".xsd": "xml",
        ".xml": "xml",
        ".md": "markdown",
        ".graphql": "graphql",
        ".gql": "graphql",
        ".csv": "csv",
        ".asn": "asn1",
        ".cypher": "cypher",
        ".parquet": "plaintext",
    }
    return lang_map.get(ext.lower(), "plaintext")


def escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;"))


# Binary file extensions (cannot be displayed as text)
BINARY_EXTENSIONS = {".parquet", ".iceberg"}


def get_file_type(ext: str) -> str:
    """Get file type for display handling: binary, markdown, or text."""
    ext_lower = ext.lower()
    if ext_lower in BINARY_EXTENSIONS:
        return "binary"
    if ext_lower == ".md":
        return "markdown"
    return "text"


def render_file_tree_html(tree: list[dict], base_url: str, indent: int = 0) -> str:
    """Render file tree as HTML."""
    html_parts = []
    
    for item in tree:
        if item["type"] == "folder":
            # Get folder icon based on folder name (for special folders)
            folder_icon = get_folder_icon(item["name"])
            html_parts.append(f'''
<div class="tree-item folder expanded" style="padding-left: {indent * 16}px;">
  <span class="tree-icon">{folder_icon}</span>
  <span class="tree-name">{escape_html(item["name"])}</span>
</div>
<div class="tree-children">
{render_file_tree_html(item["children"], base_url, indent + 1)}
</div>''')
        else:
            ext = Path(item["name"]).suffix
            icon = get_file_icon(ext)
            file_url = f"{base_url}/{item['path']}"
            file_type = get_file_type(ext)
            html_parts.append(f'''
<div class="tree-item file" data-path="{file_url}" data-lang="{get_language_for_extension(ext)}" data-filetype="{file_type}" style="padding-left: {indent * 16}px;">
  <span class="tree-icon">{icon}</span>
  <span class="tree-name">{escape_html(item["name"])}</span>
</div>''')
    
    return "".join(html_parts)


def get_folder_icon(folder_name: str) -> str:
    """Get Devicon class or emoji for a folder based on its name."""
    folder_lower = folder_name.lower()
    
    # Map folder names to devicon classes
    folder_devicons = {
        "python": '<i class="devicon-python-plain colored"></i>',
        "csharp": '<i class="devicon-csharp-plain colored"></i>',
        "java": '<i class="devicon-java-plain colored"></i>',
        "typescript": '<i class="devicon-typescript-plain colored"></i>',
        "javascript": '<i class="devicon-javascript-plain colored"></i>',
        "go": '<i class="devicon-go-plain colored"></i>',
        "rust": '<i class="devicon-rust-original"></i>',
        "cpp": '<i class="devicon-cplusplus-plain colored"></i>',
    }
    
    return folder_devicons.get(folder_lower, "📁")


def get_file_icon(ext: str) -> str:
    """Get Devicon class or fallback emoji for a file extension."""
    # Map file extensions to devicon classes (using 'colored' variant for visual appeal)
    devicon_map = {
        # Programming languages
        ".py": "devicon-python-plain colored",
        ".cs": "devicon-csharp-plain colored",
        ".java": "devicon-java-plain colored",
        ".ts": "devicon-typescript-plain colored",
        ".js": "devicon-javascript-plain colored",
        ".go": "devicon-go-plain colored",
        ".rs": "devicon-rust-original",
        ".cpp": "devicon-cplusplus-plain colored",
        ".hpp": "devicon-cplusplus-plain colored",
        ".h": "devicon-c-plain colored",
        ".rb": "devicon-ruby-plain colored",
        
        # Schema formats
        ".json": "devicon-json-plain colored",
        ".avsc": "devicon-apachekafka-original colored",  # Avro is Kafka-adjacent
        ".proto": "devicon-grpc-plain",
        ".xsd": "devicon-xml-plain",
        ".xml": "devicon-xml-plain",
        ".graphql": "devicon-graphql-plain colored",
        ".gql": "devicon-graphql-plain colored",
        
        # Database
        ".sql": "devicon-azuresqldatabase-plain colored",
        ".kql": "devicon-azure-plain colored",  # Kusto is Azure
        ".cql": "devicon-cassandra-plain colored",
        ".cypher": "devicon-neo4j-plain colored",
        
        # Data formats
        ".csv": "devicon-pandas-plain",  # CSV often used with pandas
        ".parquet": "devicon-apachespark-plain colored",  # Parquet is Spark-adjacent
        ".md": "devicon-markdown-original",
        ".yaml": "devicon-yaml-plain colored",
        ".yml": "devicon-yaml-plain colored",
        
        # Config/Build
        ".toml": "devicon-rust-original",  # TOML associated with Rust
        ".gradle": "devicon-gradle-plain colored",
        ".mod": "devicon-go-plain colored",  # Go modules
        ".csproj": "devicon-dotnetcore-plain colored",
        ".sln": "devicon-visualstudio-plain colored",
    }
    
    devicon_class = devicon_map.get(ext.lower())
    if devicon_class:
        return f'<i class="{devicon_class}"></i>'
    
    # Fallback emojis for extensions without devicons
    emoji_map = {
        ".asn": "📜",
        ".iceberg": "🧊",
        ".datapackage": "📦",
        ".struct.json": "🔧",
    }
    return emoji_map.get(ext.lower(), "📄")


def generate_gallery_page(item: dict, output_dir: Path, files_base_url: str) -> None:
    """Generate a gallery page for an item."""
    import base64
    
    page_dir = GALLERY_DIR / item["id"]
    page_dir.mkdir(parents=True, exist_ok=True)
    
    # Read source content (skip binary files)
    source_path = item.get("source_path")
    source_language = item.get("source_language", "")
    
    if source_language == "binary":
        # Binary files cannot be displayed as text
        source_content = ""
    elif source_path and source_path.exists():
        try:
            source_content = source_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            source_content = ""
    elif (output_dir / item["source_file"]).exists():
        try:
            source_content = (output_dir / item["source_file"]).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            source_content = ""
    else:
        source_content = "# Source file not found"
    
    # Build file tree
    file_tree = build_file_tree(output_dir, output_dir)
    file_tree_html = render_file_tree_html(file_tree, files_base_url)
    
    # Determine if source is binary
    source_is_binary = source_language == "binary"
    source_file_url = f"{files_base_url}/{item['source_file']}" if source_is_binary else ""
    
    # Base64 encode source content for safe embedding in front matter
    source_content_b64 = base64.b64encode(source_content.encode('utf-8')).decode('ascii') if source_content else ""
    
    # Generate the page - only file tree HTML in content, data in front matter
    page_content = f'''---
layout: gallery-viewer
title: "{item['title']}"
description: "{item['description']}"
source_file: "{item['source_file']}"
source_language: "{item['source_language']}"
source_is_binary: {str(source_is_binary).lower()}
source_file_url: "{source_file_url}"
source_content_b64: "{source_content_b64}"
files_base_url: "{files_base_url}"
permalink: /gallery/{item['id']}/
---

{file_tree_html}
'''
    
    (page_dir / "index.html").write_text(page_content, encoding="utf-8")
    print(f"  Generated page: {page_dir / 'index.html'}")


def generate_gallery_index(successful_items: list[dict]) -> None:
    """Generate the main gallery index page listing all conversions."""
    
    # Icon mapping for formats/languages (using verified Devicon icons)
    FORMAT_ICONS = {
        # Languages
        "python": '<i class="devicon-python-plain colored"></i>',
        "c#": '<i class="devicon-csharp-plain colored"></i>',
        "csharp": '<i class="devicon-csharp-plain colored"></i>',
        "java": '<i class="devicon-java-plain colored"></i>',
        "typescript": '<i class="devicon-typescript-plain colored"></i>',
        "javascript": '<i class="devicon-javascript-plain colored"></i>',
        "go": '<i class="devicon-go-original-wordmark colored"></i>',
        "rust": '<i class="devicon-rust-original"></i>',
        "c++": '<i class="devicon-cplusplus-plain colored"></i>',
        "cpp": '<i class="devicon-cplusplus-plain colored"></i>',
        # Schema formats
        "avro": '<i class="devicon-apachekafka-original colored" title="Avro"></i>',
        "json schema": '<i class="devicon-json-plain colored"></i>',
        "jsonschema": '<i class="devicon-json-plain colored"></i>',
        "protobuf": '<i class="devicon-grpc-plain colored"></i>',
        "proto": '<i class="devicon-grpc-plain colored"></i>',
        "xsd": '<i class="devicon-xml-plain colored"></i>',
        "xml": '<i class="devicon-xml-plain colored"></i>',
        "graphql": '<i class="devicon-graphql-plain colored"></i>',
        # Databases
        "postgresql": '<i class="devicon-postgresql-plain colored"></i>',
        "postgres": '<i class="devicon-postgresql-plain colored"></i>',
        "mysql": '<i class="devicon-mysql-original colored"></i>',
        "mariadb": '<i class="devicon-mariadb-original colored"></i>',
        "sqlite": '<i class="devicon-sqlite-plain colored"></i>',
        "mongodb": '<i class="devicon-mongodb-plain colored"></i>',
        "cassandra": '<i class="devicon-cassandra-plain colored"></i>',
        "redis": '<i class="devicon-redis-plain colored"></i>',
        "elasticsearch": '<i class="devicon-elasticsearch-plain colored"></i>',
        "neo4j": '<i class="devicon-neo4j-plain colored"></i>',
        "couchdb": '<i class="devicon-couchdb-plain colored"></i>',
        "hbase": '<i class="devicon-hadoop-plain colored"></i>',
        # Cloud/Platforms  
        "azure": '<i class="devicon-azure-plain colored"></i>',
        "cosmosdb": '<i class="devicon-cosmosdb-plain colored"></i>',
        "dynamodb": '<i class="devicon-dynamodb-plain colored"></i>',
        "firebase": '<i class="devicon-firebase-plain colored"></i>',
        "kusto": '<i class="devicon-azure-plain colored"></i>',
        # Data formats
        "parquet": '<i class="devicon-apachespark-original colored" title="Parquet"></i>',
        "csv": '<i class="devicon-pandas-plain colored" title="CSV"></i>',
        "markdown": '<i class="devicon-markdown-original"></i>',
        "iceberg": '<i class="devicon-apachespark-original colored" title="Iceberg"></i>',
        # Fallbacks
        "structure": '<i class="devicon-json-plain colored"></i>',
        "json structure": '<i class="devicon-json-plain colored"></i>',
        "datapackage": '<i class="devicon-json-plain colored"></i>',
        "sql": '<i class="devicon-azuresqldatabase-plain colored"></i>',
        "asn.1": '<i class="devicon-linux-plain" title="ASN.1"></i>',
        "asn1": '<i class="devicon-linux-plain" title="ASN.1"></i>',
    }
    
    # Command to README anchor mapping
    CMD_README_ANCHORS = {
        # Avrotize commands
        "j2a": "convert-json-schema-to-avro-schema",
        "x2a": "convert-xsd-to-avro-schema",
        "p2a": "convert-proto-to-avro-schema",
        "asn2a": "convert-asn1-to-avro-schema",
        "pq2a": "convert-parquet-to-avro-schema",
        "kstruct2a": "convert-kafka-connect-struct-schema-to-avro-schema",
        "csv2a": "convert-csv-to-avro-schema",
        "k2a": "convert-kusto-to-avro-schema",
        "s2a": "convert-json-structure-to-avro-schema",
        "a2p": "convert-avro-schema-to-proto",
        "a2j": "convert-avro-schema-to-json-schema",
        "a2x": "convert-avro-schema-to-xsd",
        "a2pq": "convert-avro-schema-to-parquet",
        "a2k": "convert-avro-schema-to-kusto",
        "a2ib": "convert-avro-schema-to-iceberg",
        "a2dp": "convert-avro-schema-to-datapackage",
        "a2md": "convert-avro-schema-to-markdown",
        "a2py": "convert-avro-schema-to-python",
        "a2cs": "convert-avro-schema-to-c",
        "a2java": "convert-avro-schema-to-java",
        "a2ts": "convert-avro-schema-to-typescript",
        "a2js": "convert-avro-schema-to-javascript",
        "a2go": "convert-avro-schema-to-go",
        "a2rust": "convert-avro-schema-to-rust",
        "a2cpp": "convert-avro-schema-to-c-1",
        "a2sql": "convert-avro-schema-to-sql",
        "a2nosql": "convert-avro-schema-to-nosql",
        "a2gql": "convert-avro-schema-to-graphql",
        "a2graphql": "convert-avro-schema-to-graphql",
        "a2csv": "convert-avro-schema-to-csv",
        "a2s": "convert-avro-schema-to-json-structure",
        # Structurize commands
        "j2s": "convert-json-schema-to-json-structure",
        "s2j": "convert-json-structure-to-json-schema",
        "s2x": "convert-json-structure-to-xsd",
        "s2p": "convert-json-structure-to-proto",
        "s2gql": "convert-json-structure-to-graphql",
        "s2graphql": "convert-json-structure-to-graphql",
        "s2dp": "convert-json-structure-to-datapackage",
        "s2ib": "convert-json-structure-to-iceberg",
        "s2csv": "convert-json-structure-to-csv",
        "s2k": "convert-json-structure-to-kusto",
        "s2md": "convert-json-structure-to-markdown",
        "s2py": "convert-json-structure-to-python",
        "s2cs": "convert-json-structure-to-c",
        "s2java": "convert-json-structure-to-java",
        "s2ts": "convert-json-structure-to-typescript",
        "s2js": "convert-json-structure-to-javascript",
        "s2go": "convert-json-structure-to-go",
        "s2rust": "convert-json-structure-to-rust",
        "s2cpp": "convert-json-structure-to-c-1",
        "s2sql": "convert-json-structure-to-sql",
        "s2nosql": "convert-json-structure-to-nosql",
    }
    
    def get_icon(format_name: str) -> str:
        """Get icon HTML for a format."""
        key = format_name.lower().strip()
        # Check for SQL variants
        if "sql" in key:
            if "server" in key or "sqlserver" in key:
                return '<i class="devicon-microsoftsqlserver-plain colored"></i>'
            if "oracle" in key:
                return '<i class="devicon-oracle-original colored"></i>'
            if "mysql" in key:
                return '<i class="devicon-mysql-original colored"></i>'
            if "mariadb" in key:
                return '<i class="devicon-mariadb-original colored"></i>'
            if "postgres" in key:
                return '<i class="devicon-postgresql-plain colored"></i>'
            if "sqlite" in key:
                return '<i class="devicon-sqlite-plain colored"></i>'
            # Generic SQL
            return '<i class="devicon-azuresqldatabase-plain colored"></i>'
        if "oracle" in key:
            return '<i class="devicon-oracle-original colored"></i>'
        return FORMAT_ICONS.get(key, '')
    
    def get_command(item: dict) -> str:
        """Get the primary command from an item."""
        conversions = item.get("conversions", [])
        if conversions:
            return conversions[-1].get("cmd", "")
        return ""
    
    def get_readme_url(cmd: str, is_structurize: bool) -> str:
        """Get the README URL for a command."""
        anchor = CMD_README_ANCHORS.get(cmd, "")
        if anchor:
            if is_structurize:
                return f"https://github.com/clemensv/avrotize/blob/master/structurize/README.md#{anchor}"
            return f"https://github.com/clemensv/avrotize/blob/master/README.md#{anchor}"
        return "https://github.com/clemensv/avrotize"
    
    # Categorize items into subcategories
    # Avrotize categories
    avro_input = []           # X -> Avro (schema conversions to Avro)
    avro_schema_output = []   # Avro -> schema formats
    avro_code_output = []     # Avro -> code generation
    avro_db_output = []       # Avro -> database/data formats
    avro_doc_output = []      # Avro -> documentation
    
    # Structurize categories  
    struct_input = []         # X -> Struct
    struct_schema_output = [] # Struct -> schema formats
    struct_code_output = []   # Struct -> code generation
    struct_db_output = []     # Struct -> database/data formats
    struct_doc_output = []    # Struct -> documentation
    
    # Code generation commands
    code_gen_cmds = {'a2py', 'a2cs', 'a2java', 'a2ts', 'a2js', 'a2go', 'a2rust', 'a2cpp',
                     's2py', 's2cs', 's2java', 's2ts', 's2js', 's2go', 's2rust', 's2cpp'}
    
    # Database/data format commands
    db_cmds = {'a2sql', 'a2k', 'a2pq', 'a2ib', 'a2mongo', 'a2cassandra', 'a2dynamodb', 
               'a2es', 'a2cosmos', 'a2neo4j', 'a2firebase', 'a2couchdb', 'a2hbase',
               's2sql', 's2k', 's2ib', 's2cassandra', 's2csv', 'a2csv'}
    
    # Documentation commands
    doc_cmds = {'a2md', 's2md'}
    
    for item in successful_items:
        item_id = item["id"]
        cmd = get_command(item)
        
        if item_id.endswith("-to-avro"):
            avro_input.append(item)
        elif item_id.startswith("avro-to-"):
            if cmd in code_gen_cmds:
                avro_code_output.append(item)
            elif cmd in db_cmds or any(db in item_id for db in ['sql', 'kusto', 'parquet', 'iceberg', 'mongo', 'cassandra', 'dynamodb', 'elasticsearch', 'cosmos', 'neo4j', 'firebase', 'couchdb', 'hbase', 'csv']):
                avro_db_output.append(item)
            elif cmd in doc_cmds or 'markdown' in item_id:
                avro_doc_output.append(item)
            else:
                avro_schema_output.append(item)
            # a2s also appears as input for Structurize section
            if cmd == 'a2s' or 'structure' in item_id:
                struct_input.append(item)
        elif item_id.endswith("-to-struct") or item_id.endswith("-to-structure"):
            struct_input.append(item)
        elif item_id.startswith("struct-to-"):
            if cmd in code_gen_cmds:
                struct_code_output.append(item)
            elif cmd in db_cmds or any(db in item_id for db in ['sql', 'kusto', 'iceberg', 'cassandra', 'csv']):
                struct_db_output.append(item)
            elif cmd in doc_cmds or 'markdown' in item_id:
                struct_doc_output.append(item)
            else:
                struct_schema_output.append(item)
    
    def build_example_command(item: dict, is_structurize: bool) -> str:
        """Build a full example command string."""
        conversions = item.get("conversions", [])
        if not conversions:
            return ""
        
        last_conv = conversions[-1]
        cmd = last_conv.get("cmd", "")
        args = last_conv.get("args", [])
        source_file = item.get("source_file", "input")
        
        # Build argument string
        arg_str = ""
        for i, arg in enumerate(args):
            if arg.startswith("--"):
                # It's an option, check next arg for value
                if i + 1 < len(args) and not args[i + 1].startswith("--"):
                    val = args[i + 1]
                    # Simplify output path for display
                    if "{out}" in val:
                        val = val.replace("{out}/", "").replace("{out}", "")
                    arg_str += f" {arg} {val}"
                else:
                    arg_str += f" {arg}"
        
        # Build the full command
        tool = "structurize" if is_structurize else "avrotize"
        return f"{tool} {cmd} {source_file}{arg_str}"
    
    def render_card(item: dict, is_structurize: bool = False) -> str:
        """Render a single gallery card."""
        title = item["title"]
        desc = item["description"]
        item_id = item["id"]
        cmd = get_command(item)
        readme_url = get_readme_url(cmd, is_structurize)
        example_cmd = build_example_command(item, is_structurize)
        
        # Parse formats from title (e.g., "JSON Schema -> Avro" or "Avro -> Python")
        parts = title.split(" -> ")
        formats = []
        for p in parts:
            icon = get_icon(p)
            if icon:
                formats.append(f'<span class="format-tag">{icon} {p}</span>')
            else:
                formats.append(f'<span class="format-tag">{p}</span>')
        format_html = '<span class="format-arrow">-></span>'.join(formats)
        
        return f'''    <div class="gallery-card" onclick="window.location.href='{{{{ '/gallery/{item_id}/' | relative_url }}}}'" style="cursor: pointer;">
      <div class="gallery-card-header">
        <div class="gallery-card-title">{title}</div>
        <div class="gallery-card-subtitle">{desc}</div>
      </div>
      <div class="gallery-card-body">
        <div class="gallery-card-formats">
          {format_html}
        </div>
        <div class="gallery-card-command">
          <code class="command-code">{example_cmd}</code>
          <a href="{readme_url}" class="command-docs-link" target="_blank" title="View documentation" onclick="event.stopPropagation();">docs</a>
        </div>
        <a href="{{{{ '/gallery/{item_id}/' | relative_url }}}}" class="gallery-card-link">View example -></a>
      </div>
    </div>'''
    
    def render_section(section_id: str, title: str, subtitle: str, items: list[dict], is_structurize: bool = False) -> str:
        """Render a section with cards."""
        if not items:
            return ""
        
        cards = "\n\n".join(render_card(item, is_structurize) for item in items)
        return f'''
  <div id="{section_id}" class="gallery-section">
    <div class="section-header">
      <h3>{title}</h3>
      <p>{subtitle}</p>
    </div>
    
    <div class="gallery-grid">
{cards}
    </div>
  </div>'''
    
    # Build page content with tabs
    page_content = '''---
layout: default
title: Gallery
description: See real conversion examples with full output
permalink: /gallery/
---

<section class="hero" style="padding: var(--spacing-xl) var(--spacing-xl);">
  <div class="hero-content">
    <h1>Conversion Gallery</h1>
    <p class="hero-subtitle">
      Explore real-world schema conversions. Click any card to see the source schema, 
      browse the output files, and view the generated code with syntax highlighting.
    </p>
  </div>
</section>

<section class="gallery-index">
  <!-- Tab Navigation -->
  <div class="gallery-tabs">
    <button class="gallery-tab active" data-tab="avrotize" id="tab-avrotize">
      <span class="tab-title">Avrotize</span>
      <span class="tab-subtitle">Avro Schema as pivot</span>
    </button>
    <button class="gallery-tab" data-tab="structurize" id="tab-structurize">
      <span class="tab-title">Structurize</span>
      <span class="tab-subtitle">JSON Structure as pivot</span>
    </button>
  </div>

  <!-- Avrotize Tab Content -->
  <div id="avrotize-content" class="gallery-tab-content active">
'''
    
    # Add Avrotize sections - subdivided by category
    page_content += render_section(
        "avro-input",
        "Input Formats → Avro",
        "Convert various schema formats to Avro Schema",
        avro_input,
        is_structurize=False
    )
    
    page_content += render_section(
        "avro-schema-output",
        "Avro → Schema Formats",
        "Convert Avro Schema to other schema formats",
        avro_schema_output,
        is_structurize=False
    )
    
    page_content += render_section(
        "avro-code-output", 
        "Avro → Code Generation",
        "Generate typed code from Avro Schema",
        avro_code_output,
        is_structurize=False
    )
    
    page_content += render_section(
        "avro-db-output", 
        "Avro → Database & Data Formats",
        "Generate database schemas and data format definitions",
        avro_db_output,
        is_structurize=False
    )
    
    page_content += render_section(
        "avro-doc-output", 
        "Avro → Documentation",
        "Generate documentation from Avro Schema",
        avro_doc_output,
        is_structurize=False
    )
    
    page_content += '''
  </div>

  <!-- Structurize Tab Content -->
  <div id="structurize-content" class="gallery-tab-content">
'''
    
    # Add Structurize sections - subdivided by category
    page_content += render_section(
        "struct-input",
        "Input Formats → JSON Structure", 
        "Convert various schema formats to JSON Structure",
        struct_input,
        is_structurize=True
    )
    
    page_content += render_section(
        "struct-schema-output",
        "JSON Structure → Schema Formats",
        "Convert JSON Structure to other schema formats",
        struct_schema_output,
        is_structurize=True
    )
    
    page_content += render_section(
        "struct-code-output",
        "JSON Structure → Code Generation",
        "Generate typed code from JSON Structure", 
        struct_code_output,
        is_structurize=True
    )
    
    page_content += render_section(
        "struct-db-output",
        "JSON Structure → Database & Data Formats",
        "Generate database schemas and data format definitions",
        struct_db_output,
        is_structurize=True
    )
    
    page_content += render_section(
        "struct-doc-output",
        "JSON Structure → Documentation",
        "Generate documentation from JSON Structure", 
        struct_doc_output,
        is_structurize=True
    )
    
    page_content += '''
  </div>
</section>

<script>
document.addEventListener('DOMContentLoaded', function() {
  const tabs = document.querySelectorAll('.gallery-tab');
  const contents = document.querySelectorAll('.gallery-tab-content');
  
  function switchTab(tabName) {
    const targetId = tabName + '-content';
    
    // Update active tab
    tabs.forEach(t => t.classList.remove('active'));
    const activeTab = document.querySelector(`[data-tab="${tabName}"]`);
    if (activeTab) activeTab.classList.add('active');
    
    // Update visible content
    contents.forEach(c => c.classList.remove('active'));
    const targetContent = document.getElementById(targetId);
    if (targetContent) targetContent.classList.add('active');
    
    // Update URL hash without scrolling
    history.replaceState(null, null, '#' + tabName);
  }
  
  // Handle tab clicks
  tabs.forEach(tab => {
    tab.addEventListener('click', function() {
      switchTab(this.dataset.tab);
    });
  });
  
  // Handle initial hash on page load
  const hash = window.location.hash.slice(1); // Remove #
  if (hash === 'structurize' || hash === 'avrotize') {
    switchTab(hash);
  }
  
  // Handle hash changes (e.g., back/forward navigation)
  window.addEventListener('hashchange', function() {
    const newHash = window.location.hash.slice(1);
    if (newHash === 'structurize' || newHash === 'avrotize') {
      switchTab(newHash);
    }
  });
});
</script>
'''
    
    # Write to pages/gallery.html
    pages_dir = PROJECT_ROOT / "pages"
    pages_dir.mkdir(parents=True, exist_ok=True)
    output_path = pages_dir / "gallery.html"
    output_path.write_text(page_content, encoding="utf-8")
    print(f"\nGenerated gallery index: {output_path}")

def _ensure_test_files_from_master() -> None:
    """Ensure test files are available from master branch."""
    # Files needed from master branch test directory
    needed_files = [
        ("test/xsd/acmt.003.001.08.xsd", TEST_DIR / "xsd" / "acmt.003.001.08.xsd"),
        ("test/asn1/movie.asn", TEST_DIR / "asn1" / "movie.asn"),
        ("test/parquet/address.parquet", TEST_DIR / "parquet" / "address.parquet"),
        ("test/kstruct/cardata.json", TEST_DIR / "kstruct" / "cardata.json"),
    ]
    
    for git_path, local_path in needed_files:
        if not local_path.exists():
            print(f"  Fetching {git_path} from master...")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                result = subprocess.run(
                    ["git", "show", f"master:{git_path}"],
                    cwd=PROJECT_ROOT,
                    capture_output=True,
                    check=True
                )
                # Handle binary files (like parquet)
                if git_path.endswith(".parquet"):
                    local_path.write_bytes(result.stdout)
                else:
                    local_path.write_bytes(result.stdout)
                print(f"    Fetched: {local_path}")
            except subprocess.CalledProcessError as e:
                print(f"    Failed to fetch {git_path}: {e.stderr.decode() if e.stderr else e}")
            except Exception as e:
                print(f"    Error fetching {git_path}: {e}")


def build_gallery() -> None:
    """Build all gallery items."""
    print("Building gallery content...")
    print(f"  Project root: {PROJECT_ROOT}")
    print(f"  Test directory: {TEST_DIR}")
    print(f"  Gallery directory: {GALLERY_DIR}")
    print(f"  Temp directory: {TMP_DIR}")
    
    # Ensure gallery directories exist
    GALLERY_DIR.mkdir(parents=True, exist_ok=True)
    
    # Checkout test files from master branch if not present
    if not TEST_DIR.exists():
        TEST_DIR.mkdir(parents=True, exist_ok=True)
    _ensure_test_files_from_master()
    
    # Track successful items for index generation
    successful_items = []
    
    for item in GALLERY_ITEMS:
        print(f"\nProcessing: {item['title']}")
        
        # Check if source file exists
        source_path = item.get("source_path")
        if source_path and not source_path.exists():
            print(f"  Source file not found: {source_path}")
            print(f"  Skipping item")
            continue
        
        # Create temporary output directory
        output_dir = TMP_DIR / item["id"]
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)
        
        # Run setup commands if any
        setup_commands = item.get("setup", [])
        success = True
        for setup in setup_commands:
            input_file = setup.get("input", item["source_path"])
            args = [arg.replace("{out}", str(output_dir)) for arg in setup.get("args", [])]
            if not run_avrotize(setup["cmd"], input_file, args, output_dir):
                print(f"  Setup failed, skipping item")
                success = False
                break
        
        if not success:
            continue
        
        # Run conversion commands
        source_input = item["source_path"]
        for conv in item["conversions"]:
            if "input" in conv:
                source_input = Path(conv["input"].replace("{out}", str(output_dir)))
            
            args = [arg.replace("{out}", str(output_dir)) for arg in conv.get("args", [])]
            
            if not run_avrotize(conv["cmd"], source_input, args, output_dir):
                print(f"  Conversion failed, skipping item")
                success = False
                break
        
        if success:
            # Files base URL for the generated page - use 'files' subdirectory (not _data which Jekyll ignores)
            files_base_url = f"/gallery/files/{item['id']}"
            
            # Generate the gallery page
            generate_gallery_page(item, output_dir, files_base_url)
            successful_items.append(item)
    
    # Generate the main gallery index page
    generate_gallery_index(successful_items)
    
    print("\nGallery build complete!")
    print(f"Generated {len(successful_items)} gallery pages in: {GALLERY_DIR}")
    print(f"Output files are in: {TMP_DIR}")


if __name__ == "__main__":
    build_gallery()

