import argparse
import tempfile
import sys
import os
from avrotize.asn1toavro import convert_asn1_to_avro
from avrotize.avrotocpp import convert_avro_to_cpp
from avrotize.avrotocsharp import convert_avro_to_csharp
from avrotize.avrotogo import convert_avro_to_go
from avrotize.avrotojava import convert_avro_to_java
from avrotize.avrotojs import convert_avro_to_javascript
from avrotize.avrotojsons import convert_avro_to_json_schema
from avrotize.avrotokusto import convert_avro_to_kusto_db, convert_avro_to_kusto_file
from avrotize.avrotools import pcf_schema, transform_to_pcf
from avrotize.avrotoparquet import convert_avro_to_parquet
from avrotize.avrotoproto import convert_avro_to_proto
from avrotize.avrotopython import convert_avro_to_python
from avrotize.avrotorust import convert_avro_to_rust
from avrotize.avrotots import convert_avro_to_typescript
from avrotize.avrototsql import convert_avro_to_sql
from avrotize.avrotoxsd import convert_avro_to_xsd
from avrotize.jsonstoavro import convert_jsons_to_avro
from avrotize.kstructtoavro import convert_kafka_struct_to_avro_schema
from avrotize.kustotoavro import convert_kusto_to_avro
from avrotize.parquettoavro import convert_parquet_to_avro
from avrotize.prototoavro import convert_proto_to_avro
from avrotize.xsdtoavro import convert_xsd_to_avro

def main():
    parser = argparse.ArgumentParser(description='Convert a variety of schema formats to Avrotize schema and vice versa.')

    subparsers = parser.add_subparsers(dest='command')

    pcf_parser = subparsers.add_parser('pcf', help='Create the Parsing Canonical Form (PCF) of an Avrotize schema')
    pcf_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    pcf_parser.add_argument('--out', '--pcf', type=str, help='Path to the PCF schema file', required=False)
    pcf_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')

    fingerprint_parser = subparsers.add_parser('fingerprint', help='Create the fingerprint of an Avrotize schema')
    fingerprint_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    fingerprint_parser.add_argument('--algorithm', type=str, help='Hash algorithm', choices=['sha256', 'md5', 'rabin'], required=False, default='rabin')
    fingerprint_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')

    a2p_parser = subparsers.add_parser('p2a', help='Convert Proto schema to Avrotize schema')
    a2p_parser.add_argument('input', type=str, nargs='?', help='Path of the proto file (or read from stdin if omitted)')
    a2p_parser.add_argument('--out', '--avsc', type=str, help='Path to the Avrotize schema file', required=False)
    a2p_parser.add_argument('--proto', type=str, help='Deprecated: Path of the proto file (for backcompat)')

    p2a_parser = subparsers.add_parser('a2p', help='Convert Proto schema to Avrotize schema')
    p2a_parser.add_argument('input', type=str, nargs='?', help='Path to the Proto file (or read from stdin if omitted)')
    p2a_parser.add_argument('--out', '--proto', type=str, help='Path to the Avrotize schema file', required=True)
    p2a_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Proto file (for backcompat)')
    p2a_parser.add_argument('--naming', type=str, help='Type naming convention', choices=['snake', 'camel', 'pascal'], required=False, default='pascal')
    p2a_parser.add_argument('--allow-optional', action='store_true', help='Enable support for "optional" fields', default=False)

    j2a_parser = subparsers.add_parser('j2a', help='Convert JSON schema to Avrotize schema')
    j2a_parser.add_argument('input', type=str, nargs='?', help='Path to the JSON schema file (or read from stdin if omitted)')
    j2a_parser.add_argument('--out', '--avsc', type=str, help='Path to the Avrotize schema file', required=False)
    j2a_parser.add_argument('--jsons', type=str, help='Deprecated: Path to the JSON schema file (for backcompat)')
    j2a_parser.add_argument('--namespace', type=str, help='Namespace for the Avrotize schema', required=False)
    j2a_parser.add_argument('--split-top-level-records', action='store_true', help='Split top-level records into separate files', default=False)

    a2j_parser = subparsers.add_parser('a2j', help='Convert Avrotize schema to JSON schema')
    a2j_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2j_parser.add_argument('--out', '--json', type=str, help='Path to the JSON schema file', required=False)
    a2j_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')

    x2a_parser = subparsers.add_parser('x2a', help='Convert XSD schema to Avrotize schema')
    x2a_parser.add_argument('input', type=str, nargs='?', help='Path to the XSD schema file (or read from stdin if omitted)')
    x2a_parser.add_argument('--out', '--avsc', type=str, help='Path to the Avrotize schema file', required=False)
    x2a_parser.add_argument('--xsd', type=str, help='Deprecated: Path to the XSD schema file (for backcompat)')
    x2a_parser.add_argument('--namespace', type=str, help='Namespace for the Avrotize schema', required=True)

    a2x_parser = subparsers.add_parser('a2x', help='Convert Avrotize schema to XSD schema')
    a2x_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2x_parser.add_argument('--out', '--xsd', type=str, help='Path to the XSD schema file', required=False)
    a2x_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')

    a2k_parser = subparsers.add_parser('a2k', help='Convert Avrotize schema to Kusto table schemas')
    a2k_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2k_parser.add_argument('--out', '--kusto', type=str, help='Path to the Kusto table', required=False)
    a2k_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2k_parser.add_argument('--kusto-uri', type=str, help='Kusto Cluster URI to apply the generated schema to.', required=False)
    a2k_parser.add_argument('--kusto-database', type=str, help='Kusto database name to apply the generated schema to', required=False)
    a2k_parser.add_argument('--record-type', type=str, help='Record type in the Avrotize schema', required=False)
    a2k_parser.add_argument('--emit-cloudevents-columns', action='store_true', help='Add CloudEvents columns to the Kusto table', default=False)
    a2k_parser.add_argument('--emit-cloudevents-dispatch', action='store_true', help='Emit a _cloudevents_dispatch ingestion table and update policies for each generated table', required=False)

    k2a_parser = subparsers.add_parser('k2a', help='Convert Kusto schema to Avrotize schema')
    k2a_parser.add_argument('input', type=str, nargs='?', help='Kusto URI (or read from stdin if omitted)')
    k2a_parser.add_argument('--out', '--avsc', type=str, help='Path to the Avrotize schema file', required=False)
    k2a_parser.add_argument('--kusto-uri', type=str, help='Deprecated: Kusto URI (for backcompat)')
    k2a_parser.add_argument('--kusto-database', type=str, help='Kusto database', required=True)
    k2a_parser.add_argument('--namespace', type=str, help='Namespace for the Avrotize schema', required=False)
    k2a_parser.add_argument('--emit-cloudevents', action='store_true', help='Emit CloudEvents declarations for each table', required=False)
    k2a_parser.add_argument('--emit-xregistry', action='store_true', help='Emit an xRegistry manifest with CloudEvents declarations for each table instead of a single Avrotize schema', required=False)

    a2sql_parser = subparsers.add_parser('a2sql', help='Convert Avrotize schema to SQL schema')
    a2sql_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2sql_parser.add_argument('--out', '--sql', type=str, help='Path to the SQL table', required=False)
    a2sql_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2sql_parser.add_argument('--dialect', type=str, help='SQL dialect (database type)', choices=['mysql', 'mariadb', 'postgres', 'sqlserver', 'oracle', 'sqlite', 'bigquery', 'snowflake', 'redshift', 'db2'], required=True)
    a2sql_parser.add_argument('--record-type', type=str, help='Record type in the Avrotize schema', required=False)
    a2sql_parser.add_argument('--emit-cloudevents-columns', action='store_true', help='Add CloudEvents columns to the SQL table', default=False)

    a2mongo_parser = subparsers.add_parser('a2mongo', help='Convert Avrotize schema to MongoDB schema')
    a2mongo_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2mongo_parser.add_argument('--out', '--mongo', type=str, help='Path to the MongoDB schema', required=False)
    a2mongo_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2mongo_parser.add_argument('--record-type', type=str, help='Record type in the Avrotize schema', required=False)
    a2mongo_parser.add_argument('--emit-cloudevents-columns', action='store_true', help='Add CloudEvents columns to the MongoDB schema', default=False)

    a2pq_parser = subparsers.add_parser('a2pq', help='Convert Avrotize schema to Parquet schema')
    a2pq_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2pq_parser.add_argument('--out', '--parquet', type=str, help='Path to the Parquet file', required=False)
    a2pq_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2pq_parser.add_argument('--record-type', type=str, help='Record type in the Avrotize schema', required=False)
    a2pq_parser.add_argument('--emit-cloudevents-columns', action='store_true', help='Add CloudEvents columns to the Parquet file', default=False)

    pq2a_parser = subparsers.add_parser('pq2a', help='Convert Parquet schema to Avrotize schema')
    pq2a_parser.add_argument('input', type=str, nargs='?', help='Path to the Parquet file (or read from stdin if omitted)')
    pq2a_parser.add_argument('--out', '--avsc', type=str, help='Path to the Avrotize schema file', required=False)
    pq2a_parser.add_argument('--parquet', type=str, help='Deprecated: Path to the Parquet file (for backcompat)')
    pq2a_parser.add_argument('--namespace', type=str, help='Namespace for the Avrotize schema', required=False)

    asn2a_parser = subparsers.add_parser('asn2a', help='Convert ASN.1 schema to Avrotize schema')
    asn2a_parser.add_argument('input', type=str, nargs='?', help='Path(s) to the ASN.1 schema file(s) (or read from stdin if omitted)')
    asn2a_parser.add_argument('--out', '--avsc', type=str, help='Path to the Avrotize schema file', required=False)
    asn2a_parser.add_argument('--asn', type=str, nargs='+', help='Deprecated: Path(s) to the ASN.1 schema file(s) (for backcompat)')

    kstruct2a_parser = subparsers.add_parser('kstruct2a', help='Convert Kafka Struct to Avrotize schema')
    kstruct2a_parser.add_argument('input', type=str, nargs='?', help='Path to the Kafka Struct file (or read from stdin if omitted)')
    kstruct2a_parser.add_argument('--out', '--avsc', type=str, help='Path to the Avrotize schema file', required=False)
    kstruct2a_parser.add_argument('--kstruct', type=str, help='Deprecated: Path to the Kafka Struct file (for backcompat)')

    a2csharp_parser = subparsers.add_parser('a2csharp', help='Convert Avrotize schema to C# classes')
    a2csharp_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2csharp_parser.add_argument('--out', '--csharp', type=str, help='Output path for the C# classes', required=True)
    a2csharp_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2csharp_parser.add_argument('--avro-annotation', action='store_true', help='Use Avro annotations', default=False)
    a2csharp_parser.add_argument('--system-text-json-annotation', action='store_true', help='Use System.Text.Json annotations', default=False)
    a2csharp_parser.add_argument('--newtonsoft-json-annotation', action='store_true', help='Use Newtonsoft.Json annotations', default=False)
    a2csharp_parser.add_argument('--pascal-properties', action='store_true', help='Use PascalCase properties', default=False)
    a2csharp_parser.add_argument('--namespace', type=str, help='C# namespace', required=False)

    a2java_parser = subparsers.add_parser('a2java', help='Convert Avrotize schema to Java classes')
    a2java_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2java_parser.add_argument('--out', '--java', type=str, help='Output path for the Java classes', required=True)
    a2java_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2java_parser.add_argument('--package', type=str, help='Java package name', required=True)
    a2java_parser.add_argument('--avro-annotation', action='store_true', help='Use Avro annotations', default=False)
    a2java_parser.add_argument('--jackson-annotation', action='store_true', help='Use Jackson annotations', default=False)
    a2java_parser.add_argument('--pascal-properties', action='store_true', help='Use PascalCase properties', default=False)

    a2py_parser = subparsers.add_parser('a2py', help='Convert Avrotize schema to Python classes')
    a2py_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2py_parser.add_argument('--out', '--python', type=str, help='Output path for the Python classes', required=True)
    a2py_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2py_parser.add_argument('--package', type=str, help='Python package name', required=False)
    a2py_parser.add_argument('--dataclasses-json-annotation', action='store_true', help='Use dataclasses-json annotations', default=False)

    a2ts_parser = subparsers.add_parser('a2ts', help='Convert Avrotize schema to TypeScript classes')
    a2ts_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2ts_parser.add_argument('--out', '--ts', type=str, help='Output path for the TypeScript classes', required=True)
    a2ts_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2ts_parser.add_argument('--package', type=str, help='TypeScript package name', required=False)
    a2ts_parser.add_argument('--avro-annotation', action='store_true', help='Use Avro annotations', default=False)
    a2ts_parser.add_argument('--typedjson-annotation', action='store_true', help='Use TypedJSON annotations', default=False)

    a2js_parser = subparsers.add_parser('a2js', help='Convert Avrotize schema to JavaScript classes')
    a2js_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2js_parser.add_argument('--out', '--js', type=str, help='Output path for the JavaScript classes', required=True)
    a2js_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2js_parser.add_argument('--package', type=str, help='JavaScript package name', required=False)
    a2js_parser.add_argument('--avro-annotation', action='store_true', help='Use Avro annotations', default=False)

    a2cpp_parser = subparsers.add_parser('a2cpp', help='Convert Avrotize schema to C++ classes')
    a2cpp_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2cpp_parser.add_argument('--out', '--cpp', type=str, help='Output path for the C++ classes', required=True)
    a2cpp_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2cpp_parser.add_argument('--package', type=str, help='C++ namespace', required=False)
    a2cpp_parser.add_argument('--avro-annotation', action='store_true', help='Use Avro annotations', default=False)
    a2cpp_parser.add_argument('--json-annotation', action='store_true', help='Use JSON annotations', default=False)

    a2go_parser = subparsers.add_parser('a2go', help='Convert Avrotize schema to Go classes')
    a2go_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2go_parser.add_argument('--out', '--go', type=str, help='Output path for the Go classes', required=True)
    a2go_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2go_parser.add_argument('--package', type=str, help='Go package name', required=False)
    a2go_parser.add_argument('--avro-annotation', action='store_true', help='Use Avro annotations', default=False)
    a2go_parser.add_argument('--json-annotation', action='store_true', help='Use JSON annotations', default=False)

    a2rust_parser = subparsers.add_parser('a2rust', help='Convert Avrotize schema to Rust classes')
    a2rust_parser.add_argument('input', type=str, nargs='?', help='Path to the Avrotize schema file (or read from stdin if omitted)')
    a2rust_parser.add_argument('--out', '--rust', type=str, help='Output path for the Rust classes', required=True)
    a2rust_parser.add_argument('--avsc', type=str, help='Deprecated: Path to the Avrotize schema file (for backcompat)')
    a2rust_parser.add_argument('--package', type=str, help='Rust package name', required=False)
    a2rust_parser.add_argument('--avro-annotation', action='store_true', help='Use Avro annotations', default=False)
    a2rust_parser.add_argument('--json-annotation', action='store_true', help='Use JSON annotations', default=False)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    input_file_path = args.input or getattr(args, 'avsc', None) or getattr(args, 'proto', None) or getattr(args, 'jsons', None) or getattr(args, 'xsd', None) or getattr(args, 'kusto-uri', None) or getattr(args, 'parquet', None) or getattr(args, 'asn', None) or getattr(args, 'kstruct', None)
    temp_input = None

    if not (args.command == "k2a" and args.kusto_uri):
        if input_file_path is None and sys.stdin.isatty():
            temp_input = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
            input_file_path = temp_input.name
            # read to EOF
            s = sys.stdin.read()
            while s:
                temp_input.write(s)
                s = sys.stdin.read()
            temp_input.flush()
            temp_input.close()

    suppress_print = False
    output_file_path = args.out
    temp_output = None
    if output_file_path is None:
        suppress_print = True
        temp_output = tempfile.NamedTemporaryFile(delete=False)
        output_file_path = temp_output.name
    
    def printmsg(s):
        if not suppress_print:
            print(s)
    try:
        if args.command == 'pcf':
            printmsg(f'Creating Parsing Canonical Form (PCF) of Avrotize schema {input_file_path}')
            with open(input_file_path, 'r', encoding='utf-8') as f:
                schema_json = f.read()
                pcf = transform_to_pcf(schema_json)
                with open(output_file_path, 'w', encoding='utf-8') as f:
                    f.write(pcf)
        elif args.command == 'fingerprint':
            algorithm = args.algorithm
            with open(input_file_path, 'r', encoding='utf-8') as f:
                schema_json = f.read()
                pcf = pcf_schema(schema_json)
                if algorithm == 'rabin':
                    printmsg(pcf.rabin)
                elif algorithm == 'md5':
                    printmsg(pcf.md5)
                elif algorithm == 'sha256':
                    printmsg(pcf.sha256)
        elif args.command == 'p2a':
            printmsg(f'Converting Protobuf {input_file_path} to Avro {output_file_path}')
            convert_proto_to_avro(input_file_path, output_file_path)
        elif args.command == 'a2p':
            naming = args.naming
            allow_optional = args.allow_optional
            printmsg(f'Converting Avro {output_file_path} to Proto {input_file_path}')
            convert_avro_to_proto(output_file_path, input_file_path, naming_mode=naming, allow_optional=allow_optional)
        elif args.command == 'j2a':
            namespace = args.namespace
            split_top_level_records = args.split_top_level_records
            printmsg(f'Converting JSON {input_file_path} to Avro {output_file_path}')
            convert_jsons_to_avro(input_file_path, output_file_path, namespace=namespace, split_top_level_records=split_top_level_records)
        elif args.command == 'a2j':
            printmsg(f'Converting Avro {input_file_path} to JSON {output_file_path}')
            convert_avro_to_json_schema(input_file_path, output_file_path)
        elif args.command == 'x2a':
            namespace = args.namespace
            printmsg(f'Converting XSD {input_file_path} to Avro {output_file_path}')
            convert_xsd_to_avro(input_file_path, output_file_path, namespace=namespace)
        elif args.command == 'a2x':
            printmsg(f'Converting Avro {input_file_path} to XSD {output_file_path}')
            convert_avro_to_xsd(input_file_path, output_file_path)
        elif args.command == 'a2k':
            avro_record_type = args.record_type
            kusto_uri = args.kusto_uri
            kusto_database = args.kusto_database
            if not output_file_path and not kusto_uri:
                printmsg("Please specify either the Kusto table schema file (--out) or the Kusto URI (--kusto-uri and --kusto-database)")
                exit(1)
            if kusto_uri and not kusto_database:
                printmsg("Please specify the Kusto database name --kusto-database")
                exit(1)
            if kusto_database and not kusto_uri:
                printmsg("Please specify the Kusto URI --kusto-uri")
                exit(1)
            emit_cloudevents_columns = args.emit_cloudevents_columns
            emit_cloudevents_dispatch = args.emit_cloudevents_dispatch
            printmsg(f'Converting Avro {input_file_path} to Kusto {output_file_path}')
            if output_file_path:
                convert_avro_to_kusto_file(input_file_path, avro_record_type, output_file_path, emit_cloudevents_columns, emit_cloudevents_dispatch)
            if kusto_uri and kusto_database:
                convert_avro_to_kusto_db(input_file_path, avro_record_type, kusto_uri, kusto_database, emit_cloudevents_columns, emit_cloudevents_dispatch)
        elif args.command == 'k2a':
            kusto_uri = args.kusto_uri
            kusto_database = args.kusto_database
            namespace = args.namespace
            emit_cloudevents = args.emit_cloudevents
            emit_cloudevents_xregistry = args.emit_xregistry
            printmsg(f'Converting Kusto {input_file_path} to Avro {output_file_path}')
            convert_kusto_to_avro(kusto_uri, kusto_database, namespace, output_file_path, emit_cloudevents, emit_cloudevents_xregistry)
        elif args.command == 'a2sql':
            avro_record_type = args.record_type
            dialect = args.dialect
            emit_cloudevents_columns = args.emit_cloudevents_columns
            printmsg(f'Converting Avro {input_file_path} to {dialect} {output_file_path}')
            convert_avro_to_sql(input_file_path, output_file_path, dialect, emit_cloudevents_columns)
        elif args.command == 'a2mongo':
            avro_record_type = args.record_type
            emit_cloudevents_columns = args.emit_cloudevents_columns
            printmsg(f'Converting Avro {input_file_path} to MongoDB {output_file_path}')
            convert_avro_to_sql(input_file_path, avro_record_type, "mongodb", emit_cloudevents_columns)
        elif args.command == 'a2pq':
            avro_record_type = args.record_type
            emit_cloudevents_columns = args.emit_cloudevents_columns
            printmsg(f'Converting Avro {input_file_path} to Parquet {output_file_path}')
            convert_avro_to_parquet(input_file_path, avro_record_type, output_file_path, emit_cloudevents_columns)
        elif args.command == 'pq2a':
            namespace = args.namespace
            printmsg(f'Converting Parquet {input_file_path} to Avro {output_file_path}')
            convert_parquet_to_avro(input_file_path, output_file_path, namespace=namespace)
        elif args.command == 'asn2a':
            asn_schema_file_list = input_file_path.split(',')
            printmsg(f'Converting ASN.1 {asn_schema_file_list} to Avro {output_file_path}')
            convert_asn1_to_avro(asn_schema_file_list, output_file_path)
        elif args.command == 'kstruct2a':
            printmsg(f'Converting Kafka Struct {input_file_path} to Avro {output_file_path}')
            convert_kafka_struct_to_avro_schema(input_file_path, output_file_path)
        elif args.command == 'a2csharp':
            avro_annotation = args.avro_annotation
            system_text_json_annotation = args.system_text_json_annotation
            newtonsoft_json_annotation = args.newtonsoft_json_annotation
            pascal_properties = args.pascal_properties
            namespace = args.namespace
            printmsg(f'Converting Avro {input_file_path} to C# {output_file_path}')
            convert_avro_to_csharp(input_file_path, output_file_path, base_namespace=namespace, avro_annotation=avro_annotation, system_text_json_annotation=system_text_json_annotation, newtonsoft_json_annotation=newtonsoft_json_annotation, pascal_properties=pascal_properties)
        elif args.command == 'a2java':
            package = args.package
            avro_annotation = args.avro_annotation
            jackson_annotation = args.jackson_annotation
            pascal_properties = args.pascal_properties
            printmsg(f'Converting Avro {input_file_path} to Java {output_file_path}')
            convert_avro_to_java(input_file_path, output_file_path, package_name=package, avro_annotation=avro_annotation, jackson_annotation=jackson_annotation, pascal_properties=pascal_properties)
        elif args.command == 'a2py':
            package = args.package
            dataclasses_json_annotation = args.dataclasses_json_annotation
            printmsg(f'Converting Avro {input_file_path} to Python {output_file_path}')
            convert_avro_to_python(input_file_path, output_file_path, package_name=package, dataclasses_json_annotation=dataclasses_json_annotation)
        elif args.command == 'a2ts':
            package = args.package
            avro_annotation = args.avro_annotation
            typedjson_annotation = args.typedjson_annotation
            printmsg(f'Converting Avro {input_file_path} to TypeScript {output_file_path}')
            convert_avro_to_typescript(input_file_path, output_file_path, package_name=package, avro_annotation=avro_annotation, typedjson_annotation=typedjson_annotation)
        elif args.command == 'a2js':
            package = args.package
            avro_annotation = args.avro_annotation
            printmsg(f'Converting Avro {input_file_path} to JavaScript {output_file_path}')
            convert_avro_to_javascript(input_file_path, output_file_path, package_name=package, avro_annotation=avro_annotation)
        elif args.command == 'a2cpp':
            package = args.package
            avro_annotation = args.avro_annotation
            json_annotation = args.json_annotation
            printmsg(f'Converting Avro {input_file_path} to C++ {output_file_path}')
            convert_avro_to_cpp(input_file_path, output_file_path, base_namespace=package, avro_annotation=avro_annotation, json_annotation=json_annotation)
        elif args.command == 'a2go':
            package = args.package
            avro_annotation = args.avro_annotation
            json_annotation = args.json_annotation
            printmsg(f'Converting Avro {input_file_path} to Go {output_file_path}')
            convert_avro_to_go(input_file_path, output_file_path, base_namespace=package, avro_annotation=avro_annotation, json_annotation=json_annotation)
        elif args.command == 'a2rust':
            package = args.package
            avro_annotation = args.avro_annotation
            json_annotation = args.json_annotation
            printmsg(f'Converting Avro {input_file_path} to Rust {output_file_path}')
            convert_avro_to_rust(input_file_path, output_file_path, base_namespace=package, avro_annotation=avro_annotation, json_annotation=json_annotation)

        if temp_output:
            with open(output_file_path, 'r', encoding='utf-8') as f:
                sys.stdout.write(f.read())
            temp_output.close()

    except Exception as e:
        print("Error: ", str(e))
        exit(1)
    finally:
        if temp_input:
            try:
                os.remove(temp_input.name)
            except OSError as e:
                print(f"Error: Could not delete temporary input file {temp_input.name}. {e}")

if __name__ == "__main__":
    main()
