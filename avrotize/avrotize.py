import argparse
from avrotize.asn1toavro import convert_asn1_to_avro
from avrotize.avrotojsons import convert_avro_to_json_schema
from avrotize.avrotokusto import convert_avro_to_kusto
from avrotize.avrotoparquet import convert_avro_to_parquet

from avrotize.avrotoproto import convert_avro_to_proto
from avrotize.avrototsql import convert_avro_to_tsql
from avrotize.jsonstoavro import convert_jsons_to_avro
from avrotize.kstructtoavro import convert_kafka_struct_to_avro_schema
from avrotize.prototoavro import convert_proto_to_avro
from avrotize.xsdtoavro import convert_xsd_to_avro


def main():
    parser = argparse.ArgumentParser(description='Convert a variety of schema formats to Avro Schema and vice versa.')

    subparsers = parser.add_subparsers(dest='command')
    a2p_parser = subparsers.add_parser('p2a', help='Convert Avro schema to Proto schema')
    a2p_parser.add_argument('--proto', type=str, help='Path of the proto file', required=True)
    a2p_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)

    p2a_parser = subparsers.add_parser('a2p', help='Convert Proto schema to Avro schema')
    p2a_parser.add_argument('--proto', type=str, help='Path to the Proto file', required=True)
    p2a_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)
    p2a_parser.add_argument('--naming', type=str, help='Type naming convention', choices=['snake', 'camel', 'pascal'], required=False, default='pascal')
    p2a_parser.add_argument('--allow-optional', action='store_true', help='Enable support for "optional" fields', default=False)

    j2a_parser = subparsers.add_parser('j2a', help='Convert JSON schema to Avro schema')
    j2a_parser.add_argument('--jsons', type=str, help='Path to the JSON schema file', required=True)
    j2a_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)
    j2a_parser.add_argument('--namespace', type=str, help='Namespace for the Avro schema', required=False)
    j2a_parser.add_argument('--split-top-level-records', action='store_true', help='Split top-level records into separate files', default=False)

    a2j_parser = subparsers.add_parser('a2j', help='Convert Avro schema to JSON schema')
    a2j_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)
    a2j_parser.add_argument('--json', type=str, help='Path to the JSON schema file', required=True)

    x2a_parser = subparsers.add_parser('x2a', help='Convert XSD schema to Avro schema')
    x2a_parser.add_argument('--xsd', type=str, help='Path to the XSD schema file', required=True)
    x2a_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)
    x2a_parser.add_argument('--namespace', type=str, help='Namespace for the Avro schema', required=False)

    a2x_parser = subparsers.add_parser('a2x', help='Convert Avro schema to XSD schema')
    a2x_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)
    a2x_parser.add_argument('--xsd', type=str, help='Path to the XSD schema file', required=True)

    a2k_parser = subparsers.add_parser('a2k', help='Convert Avro schema to Kusto schema')
    a2k_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)
    a2k_parser.add_argument('--kusto', type=str, help='Path to the Kusto table', required=True)
    a2k_parser.add_argument('--record-type', type=str, help='Record type in the Avro schema', required=False)
    a2k_parser.add_argument('--emit-cloudevents-columns', action='store_true', help='Add CloudEvents columns to the Kusto table', default=False)

    a2tsql_parser = subparsers.add_parser('a2tsql', help='Convert Avro schema to T-SQL schema')
    a2tsql_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)
    a2tsql_parser.add_argument('--tsql', type=str, help='Path to the T-SQL table', required=True)
    a2tsql_parser.add_argument('--record-type', type=str, help='Record type in the Avro schema', required=False)
    a2tsql_parser.add_argument('--emit-cloudevents-columns', action='store_true', help='Add CloudEvents columns to the T-SQL table', default=False)

    a2pq_parser = subparsers.add_parser('a2pq', help='Convert Avro schema to Parquet schema')
    a2pq_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)
    a2pq_parser.add_argument('--parquet', type=str, help='Path to the Parquet file', required=True)
    a2pq_parser.add_argument('--record-type', type=str, help='Record type in the Avro schema', required=False)
    a2pq_parser.add_argument('--emit-cloudevents-columns', action='store_true', help='Add CloudEvents columns to the Parquet file', default=False)

    asn2a_parser = subparsers.add_parser('asn2a', help='Convert ASN.1 schema to Avro schema')
    asn2a_parser.add_argument('--asn', type=str, nargs='+', help='Path(s) to the ASN.1 schema file(s)', required=True)
    asn2a_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)

    kstruct2a_parser = subparsers.add_parser('kstruct2a', help='Convert Kafka Struct to Avro schema')
    kstruct2a_parser.add_argument('--kstruct', type=str, help='Path to the Kafka Struct file', required=True)
    kstruct2a_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
    elif args.command == 'p2a':
        avro_schema_path = args.avsc
        proto_file_path = args.proto
        print(f'Converting Protobuf {proto_file_path} to Avro {avro_schema_path}')
        convert_proto_to_avro(proto_file_path, avro_schema_path)
    elif args.command == 'a2p':
        proto_schema_path = args.proto
        avro_file_path = args.avsc
        naming = args.naming
        allow_optional = args.allow_optional
        print(f'Converting Avro {avro_file_path} to Proto {proto_schema_path}')
        convert_avro_to_proto(avro_file_path, proto_schema_path, naming_mode=naming, allow_optional=allow_optional)
    elif args.command == 'j2a':
        json_schema_file_path = args.jsons
        avro_schema_path = args.avsc
        namespace = args.namespace
        split_top_level_records = args.split_top_level_records
        print(f'Converting JSON {json_schema_file_path} to Avro {avro_schema_path}')
        convert_jsons_to_avro(json_schema_file_path, avro_schema_path, namespace=namespace, split_top_level_records=split_top_level_records)
    elif args.command == 'a2j':
        avro_schema_path = args.avsc
        json_schema_file_path = args.json
        print(f'Converting Avro {avro_schema_path} to JSON {json_schema_file_path}')
        convert_avro_to_json_schema(avro_schema_path, json_schema_file_path) 
    elif args.command == 'x2a':
        xsd_schema_file_path = args.xsd
        avro_schema_path = args.avsc
        namespace = args.namespace
        print(f'Converting XSD {xsd_schema_file_path} to Avro {avro_schema_path}')
        convert_xsd_to_avro(xsd_schema_file_path, avro_schema_path, namespace=namespace)
    elif args.command == 'a2x':
        avro_schema_path = args.avsc
        xsd_schema_file_path = args.xsd
        print(f'Converting Avro {avro_schema_path} to XSD {xsd_schema_file_path}')
        convert_xsd_to_avro(avro_schema_path, xsd_schema_file_path)
    elif args.command == 'a2k':
        avro_schema_path = args.avsc
        avro_record_type = args.record_type
        kusto_file_path = args.kusto
        emit_cloud_events_columns = args.emit_cloudevents_columns
        print(f'Converting Avro {avro_schema_path} to Kusto {kusto_file_path}')
        convert_avro_to_kusto(avro_schema_path, avro_record_type, kusto_file_path, emit_cloud_events_columns)
    elif args.command == 'a2tsql':
        avro_schema_path = args.avsc
        avro_record_type = args.record_type
        tsql_file_path = args.tsql
        emit_cloud_events_columns = args.emit_cloudevents_columns
        print(f'Converting Avro {avro_schema_path} to T-SQL {tsql_file_path}')
        convert_avro_to_tsql(avro_schema_path, avro_record_type, tsql_file_path, emit_cloud_events_columns)
    elif args.command == 'a2pq':
        avro_schema_path = args.avsc
        avro_record_type = args.record_type
        parquet_file_path = args.parquet
        emit_cloud_events_columns = args.emit_cloudevents_columns
        print(f'Converting Avro {avro_schema_path} to Parquet {parquet_file_path}')
        convert_avro_to_parquet(avro_schema_path, avro_record_type, parquet_file_path, emit_cloud_events_columns)  
    elif args.command == 'asn2a':
        asn_schema_file_list = args.asn
        if len(asn_schema_file_list) == 1:
            asn_schema_file_list = asn_schema_file_list[0].split(',')
        avro_schema_path = args.avsc
        print(f'Converting ASN.1 {asn_schema_file_list} to Avro {avro_schema_path}')
        convert_asn1_to_avro(asn_schema_file_list, avro_schema_path)
    elif args.command == 'kstruct2a':
        kstruct_file_path = args.kstruct
        avro_schema_path = args.avsc
        print(f'Converting Kafka Struct {kstruct_file_path} to Avro {avro_schema_path}')
        convert_kafka_struct_to_avro_schema(kstruct_file_path, avro_schema_path)
    
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Error: ", str(e))
        exit(1)
