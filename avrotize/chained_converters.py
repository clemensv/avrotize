"""
Chained converters that implement s2* and *2s commands by converting through Avrotize schema as temporary files.
This module provides conversion functions that chain existing *2a and a2* conversions via temporary Avrotize schemas.
"""

import os
import tempfile
from pathlib import Path
from typing import Optional
import typing # Added import

# Import existing converter functions
from .jstructtoavro import convert_json_structure_to_avro
from .avrotojstruct import convert_avro_to_json_structure
from .prototoavro import convert_proto_to_avro
from .avrotoproto import convert_avro_to_proto
from .jsonstoavro import convert_jsons_to_avro
from .avrotojsons import convert_avro_to_json_schema
from .xsdtoavro import convert_xsd_to_avro
from .avrotoxsd import convert_avro_to_xsd
from .parquettoavro import convert_parquet_to_avro
from .avrotoparquet import convert_avro_to_parquet
from .asn1toavro import convert_asn1_to_avro
from .kustotoavro import convert_kusto_to_avro
from .avrotokusto import convert_avro_to_kusto_file
from .csvtoavro import convert_csv_to_avro
from .kstructtoavro import convert_kafka_struct_to_avro_schema
from .avrotodb import convert_avro_to_sql, convert_avro_to_nosql
from .avrotoiceberg import convert_avro_to_iceberg
from .avrotojava import convert_avro_to_java
from .avrotocsharp import convert_avro_to_csharp
from .avrotopython import convert_avro_to_python
from .avrotots import convert_avro_to_typescript
from .avrotojs import convert_avro_to_javascript
from .avrotocpp import convert_avro_to_cpp
from .avrotogo import convert_avro_to_go
from .avrotorust import convert_avro_to_rust
from .avrotodatapackage import convert_avro_to_datapackage
from .avrotomd import convert_avro_to_markdown


def _create_temp_avro_file() -> str:
    """Create a temporary Avrotize schema file and return its path."""
    temp_fd, temp_path = tempfile.mkstemp(suffix='.avsc', prefix='avrotize_temp_')
    os.close(temp_fd)  # Close the file descriptor, but keep the file
    return temp_path


def _cleanup_temp_file(temp_path: str) -> None:
    """Remove a temporary file if it exists."""
    try:
        if os.path.exists(temp_path):
            os.remove(temp_path)
    except OSError:
        pass  # Ignore cleanup errors


# S2* commands (JSON Structure to *)

def convert_structure_to_proto(json_structure_file: str, proto_file_path: str, 
                              naming_mode: str = "pascal", allow_optional: bool = False) -> None:
    """Convert JSON Structure to Proto schema via Avrotize schema."""
    import tempfile
    import shutil
    
    temp_avro = _create_temp_avro_file()
    temp_dir = None
    try:
        # Step 1: Structure to Avrotize
        print(f"DEBUG: s2p: Converting {json_structure_file} to {temp_avro}")
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        if os.path.exists(temp_avro) and os.path.getsize(temp_avro) > 0:
            print(f"DEBUG: s2p: Temporary Avro file {temp_avro} created successfully and is not empty.")
            with open(temp_avro, 'r', encoding='utf-8') as f:
                print(f"DEBUG: s2p: Content of {temp_avro}:\\n{f.read()}")
        else:
            print(f"DEBUG: s2p: Temporary Avro file {temp_avro} was NOT created or is empty.")
            if os.path.exists(temp_avro):
                print(f"DEBUG: s2p: Size of {temp_avro}: {os.path.getsize(temp_avro)}")
            else:
                print(f"DEBUG: s2p: File {temp_avro} does not exist.")

        # Step 2: Avrotize to Proto
        print(f"DEBUG: s2p: Converting {temp_avro} to {proto_file_path}")
        
        # Ensure naming_mode is one of the literal types expected by convert_avro_to_proto
        actual_naming_mode: typing.Literal['snake', 'pascal', 'camel']
        if naming_mode == 'snake':
            actual_naming_mode = 'snake'
        elif naming_mode == 'pascal':
            actual_naming_mode = 'pascal'
        elif naming_mode == 'camel':
            actual_naming_mode = 'camel'
        else:
            print(f"DEBUG: s2p: Invalid naming_mode '{naming_mode}', defaulting to 'pascal'.")
            actual_naming_mode = "pascal"
        
        # convert_avro_to_proto expects a directory and creates files named {package}.proto
        # So we need to create a temp directory and then move the generated file
        temp_dir = tempfile.mkdtemp()
        convert_avro_to_proto(temp_avro, temp_dir, naming_mode=actual_naming_mode, allow_optional=allow_optional)
        
        # Find the generated proto file in temp_dir and move it to the expected location
        generated_files = [f for f in os.listdir(temp_dir) if f.endswith('.proto')]
        if generated_files:
            source_file = os.path.join(temp_dir, generated_files[0])
            shutil.move(source_file, proto_file_path)
            print(f"DEBUG: s2p: Moved {source_file} to {proto_file_path}")
        else:
            print(f"DEBUG: s2p: No .proto files generated in {temp_dir}")
        
        if os.path.exists(proto_file_path) and os.path.getsize(proto_file_path) > 0:
            print(f"DEBUG: s2p: Proto file {proto_file_path} created successfully and is not empty.")
        else:
            print(f"DEBUG: s2p: Proto file {proto_file_path} was NOT created or is empty.")
            if os.path.exists(proto_file_path):
                print(f"DEBUG: s2p: Size of {proto_file_path}: {os.path.getsize(proto_file_path)}")
            else:
                print(f"DEBUG: s2p: File {proto_file_path} does not exist.")
    finally:
        _cleanup_temp_file(temp_avro)
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


def convert_structure_to_json_schema(json_structure_file: str, json_schema_file: str, 
                                   naming_mode: str = "default") -> None:
    """Convert JSON Structure to JSON schema via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to JSON schema
        convert_avro_to_json_schema(temp_avro, json_schema_file, naming_mode)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_xsd(json_structure_file: str, xml_file_path: str, 
                           target_namespace: Optional[str] = None) -> None:
    """Convert JSON Structure to XSD schema via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to XSD
        target_namespace_str = target_namespace if target_namespace is not None else ""
        convert_avro_to_xsd(temp_avro, xml_file_path, target_namespace_str)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_kusto(json_structure_file: str, kusto_file_path: str, 
                             avro_record_type: Optional[str] = None) -> None:
    """Convert JSON Structure to Kusto table schema via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to Kusto
        convert_avro_to_kusto_file(temp_avro, avro_record_type, kusto_file_path)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_sql(json_structure_file: str, dbscript_file_path: str, 
                           db_dialect: str, emit_cloudevents_columns: bool = False) -> None:
    """Convert JSON Structure to SQL schema via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to SQL
        convert_avro_to_sql(temp_avro, dbscript_file_path, db_dialect, emit_cloudevents_columns)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_nosql(json_structure_file: str, nosql_file_path: str, 
                             nosql_dialect: str, emit_cloudevents_columns: bool = False) -> None:
    """Convert JSON Structure to NoSQL schema via Avrotize schema."""
    import tempfile
    import shutil
    import os
    
    temp_avro = _create_temp_avro_file()
    temp_dir = None
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)        # Step 2: Avrotize to NoSQL
        # convert_avro_to_nosql expects a directory path, so create temp directory
        temp_dir = tempfile.mkdtemp()
        try:
            convert_avro_to_nosql(temp_avro, temp_dir, nosql_dialect, emit_cloudevents_columns)
        except TypeError as e:
            if "write() argument must be str, not list" in str(e):
                # Handle Neo4j list output issue by post-processing
                import json
                with open(temp_avro, "r", encoding="utf-8") as f:
                    schema = json.loads(f.read())
                from avrotize.avrotodb import generate_nosql, get_file_name, get_nosql_file_extension
                model = generate_nosql(schema, nosql_dialect, emit_cloudevents_columns, schema)
                file_name = os.path.join(temp_dir, get_file_name(schema, get_nosql_file_extension(nosql_dialect)))
                with open(file_name, "w", encoding="utf-8") as nosql_file:
                    if isinstance(model, list):
                        nosql_file.write("\n".join(model))
                    else:
                        nosql_file.write(model)
            else:
                raise
        
        # Find the generated file in temp_dir and move it to the expected location
        for file_name in os.listdir(temp_dir):
            source_file = os.path.join(temp_dir, file_name)
            if os.path.isfile(source_file):
                shutil.move(source_file, nosql_file_path)
                break
    finally:
        _cleanup_temp_file(temp_avro)
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


def convert_structure_to_parquet(json_structure_file: str, parquet_file_path: str, 
                               avro_record_type: Optional[str] = None, 
                               emit_cloudevents_columns: bool = False) -> None:
    """Convert JSON Structure to Parquet schema via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
          # Step 2: Avrotize to Parquet
        convert_avro_to_parquet(temp_avro, avro_record_type, parquet_file_path, emit_cloudevents_columns)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_iceberg(json_structure_file: str, output_path: str, 
                               avro_record_type: Optional[str] = None, 
                               emit_cloudevents_columns: bool = False) -> None:
    """Convert JSON Structure to Iceberg schema via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        # Step 2: Avrotize to Iceberg
        convert_avro_to_iceberg(temp_avro, avro_record_type, output_path, emit_cloudevents_columns)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_java(json_structure_file: str, java_file_path: str, 
                            package_name: Optional[str] = None, avro_annotation: bool = False,
                            jackson_annotation: bool = False, pascal_properties: bool = False) -> None:
    """Convert JSON Structure to Java classes via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to Java
        pkg_name = package_name if package_name is not None else "generated.java"
        convert_avro_to_java(temp_avro, java_file_path, pkg_name, avro_annotation, jackson_annotation, pascal_properties)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_csharp(json_structure_file: str, cs_file_path: str, 
                              avro_annotation: bool = False, system_text_json_annotation: bool = False,
                              newtonsoft_json_annotation: bool = False, system_xml_annotation: bool = False,
                              pascal_properties: bool = False, base_namespace: Optional[str] = None) -> None:
    """Convert JSON Structure to C# classes via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to C#
        base_namespace_str = base_namespace if base_namespace is not None else "Generated.Models"
        convert_avro_to_csharp(temp_avro, cs_file_path, 
                               base_namespace_str, # 3rd param for underlying
                               system_text_json_annotation, # 4th
                               newtonsoft_json_annotation, # 5th
                               system_xml_annotation, # 6th
                               pascal_properties, # 7th
                               avro_annotation) # 8th param for underlying
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_python(json_structure_file: str, py_file_path: str, 
                               package_name: Optional[str] = None, dataclasses_json_annotation: bool = False,
                               avro_annotation: bool = False) -> None:
    """Convert JSON Structure to Python classes via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to Python
        pkg_name = package_name if package_name is not None else "generated_python"
        convert_avro_to_python(temp_avro, py_file_path, pkg_name, dataclasses_json_annotation, avro_annotation)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_typescript(json_structure_file: str, js_dir_path: str, 
                                  package_name: Optional[str] = None, avro_annotation: bool = False,
                                  typedjson_annotation: bool = False) -> None:
    """Convert JSON Structure to TypeScript classes via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to TypeScript
        pkg_name = package_name if package_name is not None else "generated_ts"
        convert_avro_to_typescript(temp_avro, js_dir_path, pkg_name, avro_annotation, typedjson_annotation)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_javascript(json_structure_file: str, js_dir_path: str, 
                                   package_name: Optional[str] = None, avro_annotation: bool = False) -> None:
    """Convert JSON Structure to JavaScript classes via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to JavaScript
        pkg_name = package_name if package_name is not None else "generated_js"
        convert_avro_to_javascript(temp_avro, js_dir_path, pkg_name, avro_annotation)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_cpp(json_structure_file: str, output_dir: str, 
                           namespace: Optional[str] = None, avro_annotation: bool = False,
                           json_annotation: bool = False) -> None:
    """Convert JSON Structure to C++ classes via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to C++
        ns = namespace if namespace is not None else "generated_cpp"
        convert_avro_to_cpp(temp_avro, output_dir, ns, avro_annotation, json_annotation)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_go(json_structure_file: str, go_file_path: str, 
                          package_name: Optional[str] = None, avro_annotation: bool = False,
                          json_annotation: bool = False, package_site: Optional[str] = None,
                          package_username: Optional[str] = None) -> None:
    """Convert JSON Structure to Go classes via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to Go - provide default values for None parameters
        pkg_name = package_name if package_name is not None else ''
        site = package_site if package_site is not None else 'github.com'
        username = package_username if package_username is not None else 'username'
        convert_avro_to_go(temp_avro, go_file_path, pkg_name, avro_annotation, json_annotation, site, username)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_rust(json_structure_file: str, rust_file_path: str, 
                            package_name: Optional[str] = None,
                            avro_annotation: bool = False, serde_annotation: bool = False) -> None:
    """Convert JSON Structure to Rust structs via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        print(f"DEBUG: s2rust: Converting {json_structure_file} to {temp_avro}")
        convert_json_structure_to_avro(json_structure_file, temp_avro)

        if os.path.exists(temp_avro) and os.path.getsize(temp_avro) > 0:
            print(f"DEBUG: s2rust: Temporary Avro file {temp_avro} created successfully and is not empty.")
            with open(temp_avro, 'r', encoding='utf-8') as f:
                print(f"DEBUG: s2rust: Content of {temp_avro}:\\n{f.read()}")
        else:
            print(f"DEBUG: s2rust: Temporary Avro file {temp_avro} was NOT created or is empty.")
            if os.path.exists(temp_avro):
                print(f"DEBUG: s2rust: Size of {temp_avro}: {os.path.getsize(temp_avro)}")
            else:
                print(f"DEBUG: s2rust: File {temp_avro} does not exist.")
        
        # Step 2: Avrotize to Rust
        # Note: convert_avro_to_rust expects a directory path, not a file path
        rust_dir = rust_file_path if os.path.isdir(rust_file_path) else os.path.dirname(rust_file_path)
        if not rust_dir:
            rust_dir = os.path.dirname(rust_file_path) if rust_file_path else "."
        if not os.path.exists(rust_dir):
            os.makedirs(rust_dir, exist_ok=True)
            
        print(f"DEBUG: s2rust: Converting {temp_avro} to {rust_dir}")
        convert_avro_to_rust(temp_avro, rust_dir, 
                             package_name=package_name if package_name is not None else "model", 
                             avro_annotation=avro_annotation, 
                             serde_annotation=serde_annotation)
          # Check if Rust files were created in the directory
        rust_files = []
        src_dir = os.path.join(rust_dir, 'src')
        if os.path.exists(src_dir):
            rust_files = [f for f in os.listdir(src_dir) if f.endswith('.rs')]
        cargo_file = os.path.join(rust_dir, 'Cargo.toml')
        
        if rust_files or os.path.exists(cargo_file):
            print(f"DEBUG: s2rust: Rust project created successfully in {rust_dir}")
            print(f"DEBUG: s2rust: Generated files: {rust_files}")
            if os.path.exists(cargo_file):
                print(f"DEBUG: s2rust: Cargo.toml created")
        else:
            print(f"DEBUG: s2rust: Rust project was NOT created or is empty in {rust_dir}")
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_datapackage(json_structure_file: str, datapackage_path: str, 
                                    avro_record_type: Optional[str] = None) -> None:
    """Convert JSON Structure to Datapackage schema via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
          # Step 2: Avrotize to Datapackage
        convert_avro_to_datapackage(temp_avro, avro_record_type, datapackage_path)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_structure_to_markdown(json_structure_file: str, markdown_path: str) -> None:
    """Convert JSON Structure to Markdown documentation via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Structure to Avrotize
        convert_json_structure_to_avro(json_structure_file, temp_avro)
        
        # Step 2: Avrotize to Markdown
        convert_avro_to_markdown(temp_avro, markdown_path)
    finally:
        _cleanup_temp_file(temp_avro)


# *2S commands (* to JSON Structure)

def convert_proto_to_structure(proto_file_path: str, json_structure_file: str, 
                              namespace: Optional[str] = None, message_type: Optional[str] = None,
                              naming_mode: str = "default", avro_encoding: bool = False) -> None:
    """Convert Proto schema to JSON Structure via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Proto to Avrotize
        ns = namespace if namespace is not None else ""
        mt = message_type if message_type is not None else ""
        convert_proto_to_avro(proto_file_path, temp_avro, ns, mt)
        
        # Step 2: Avrotize to Structure
        convert_avro_to_json_structure(temp_avro, json_structure_file, naming_mode, avro_encoding)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_json_schema_to_structure(json_schema_file_path: str, json_structure_file: str, 
                                   namespace: Optional[str] = None, split_top_level_records: bool = False,
                                   naming_mode: str = "default", avro_encoding: bool = False,
                                   root_class_name: Optional[str] = None, utility_namespace: Optional[str] = None) -> None:
    """Convert JSON schema to JSON Structure via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: JSON schema to Avrotize
        ns = namespace if namespace is not None else "model"
        util_ns = utility_namespace if utility_namespace is not None else "utils"
        root_name = root_class_name if root_class_name is not None else "Root"
        convert_jsons_to_avro(json_schema_file_path, temp_avro, ns, util_ns, root_name, split_top_level_records)
        
        # Step 2: Avrotize to Structure
        convert_avro_to_json_structure(temp_avro, json_structure_file, naming_mode, avro_encoding)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_xsd_to_structure(xsd_path: str, json_structure_file: str, 
                           namespace: Optional[str] = None, naming_mode: str = "default", 
                           avro_encoding: bool = False) -> None:
    """Convert XSD schema to JSON Structure via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: XSD to Avrotize
        convert_xsd_to_avro(xsd_path, temp_avro, namespace)
        
        # Step 2: Avrotize to Structure
        convert_avro_to_json_structure(temp_avro, json_structure_file, naming_mode, avro_encoding)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_parquet_to_structure(parquet_file_path: str, json_structure_file: str, 
                                namespace: Optional[str] = None, naming_mode: str = "default", 
                                avro_encoding: bool = False) -> None:
    """Convert Parquet schema to JSON Structure via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Parquet to Avrotize
        ns = namespace if namespace is not None else "model"
        convert_parquet_to_avro(parquet_file_path, temp_avro, ns)
        
        # Step 2: Avrotize to Structure
        convert_avro_to_json_structure(temp_avro, json_structure_file, naming_mode, avro_encoding)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_asn1_to_structure(asn1_spec_list: str, json_structure_file: str, 
                            naming_mode: str = "default", avro_encoding: bool = False) -> None:
    """Convert ASN.1 schema to JSON Structure via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: ASN.1 to Avrotize
        # Assuming asn1_spec_list is a single file path string as per type hint
        specs = [asn1_spec_list]
        convert_asn1_to_avro(specs, temp_avro)
        
        # Step 2: Avrotize to Structure
        convert_avro_to_json_structure(temp_avro, json_structure_file, naming_mode, avro_encoding)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_kusto_to_structure(kusto_uri: str, kusto_database: str, json_structure_file: str,
                             table_name: Optional[str] = None, avro_namespace: Optional[str] = None,
                             emit_cloudevents: bool = False, emit_cloudevents_xregistry: bool = False,
                             naming_mode: str = "default", avro_encoding: bool = False) -> None:
    """Convert Kusto schema to JSON Structure via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Kusto to Avrotize
        avro_ns = avro_namespace if avro_namespace is not None else "model"
        convert_kusto_to_avro(kusto_uri, kusto_database, table_name, avro_ns, temp_avro, 
                            emit_cloudevents, emit_cloudevents_xregistry)
        
        # Step 2: Avrotize to Structure
        convert_avro_to_json_structure(temp_avro, json_structure_file, naming_mode, avro_encoding)
    finally:
        _cleanup_temp_file(temp_avro)


def convert_csv_to_structure(csv_file_path: str, json_structure_file: str, 
                           namespace: Optional[str] = None, naming_mode: str = "default", 
                           avro_encoding: bool = False) -> None:
    """Convert CSV file to JSON Structure via Avrotize schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: CSV to Avrotize
        ns = namespace if namespace is not None else "model"
        convert_csv_to_avro(csv_file_path, temp_avro, ns)
        
        # Step 2: Avrotize to Structure
        convert_avro_to_json_structure(temp_avro, json_structure_file, naming_mode, avro_encoding)
    finally:
        _cleanup_temp_file(temp_avro)

def convert_kafka_struct_to_structure(kafka_schema_file_path: str, json_structure_file: str,
                                   naming_mode: str = "default", avro_encoding: bool = False) -> None:
    """Convert Kafka Struct schema to JSON Structure via Avro schema."""
    temp_avro = _create_temp_avro_file()
    try:
        # Step 1: Kafka Struct to Avro
        convert_kafka_struct_to_avro_schema(kafka_schema_file_path, temp_avro)
        
        # Step 2: Avro to JSON Structure
        convert_avro_to_json_structure(temp_avro, json_structure_file, naming_mode, avro_encoding)
    finally:
        _cleanup_temp_file(temp_avro)

__all__ = [
    # S2*
    "convert_structure_to_proto",
    "convert_structure_to_json_schema",
    "convert_structure_to_xsd",
    "convert_structure_to_kusto",
    "convert_structure_to_sql",
    "convert_structure_to_nosql",
    "convert_structure_to_parquet",
    "convert_structure_to_iceberg",
    "convert_structure_to_java",
    "convert_structure_to_csharp",
    "convert_structure_to_python",
    "convert_structure_to_typescript",
    "convert_structure_to_javascript",
    "convert_structure_to_cpp",
    "convert_structure_to_go",
    "convert_structure_to_rust",
    "convert_structure_to_datapackage",
    "convert_structure_to_markdown",
    # *2S
    "convert_proto_to_structure",
    "convert_json_schema_to_structure",
    "convert_xsd_to_structure",
    "convert_parquet_to_structure",
    "convert_asn1_to_structure",
    "convert_kusto_to_structure",
    "convert_csv_to_structure",
    "convert_kafka_struct_to_structure", # Added here
]
