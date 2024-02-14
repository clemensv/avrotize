import argparse

from avrotize.avrotoproto import convert_avro_to_proto
from avrotize.jsonstoavro import convert_jsons_to_avro
from avrotize.prototoavro import convert_proto_to_avro


def main():
    parser = argparse.ArgumentParser(description='Convert Proto schema to Avro Schema')

    subparsers = parser.add_subparsers(dest='command')
    a2p_parser = subparsers.add_parser('p2a', help='Convert Avro schema to Proto schema')
    a2p_parser.add_argument('--proto', type=str, help='Path of the proto file', required=True)
    a2p_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)

    p2a_parser = subparsers.add_parser('a2p', help='Convert Proto schema to Avro schema')
    p2a_parser.add_argument('--proto', type=str, help='Path to the Proto file', required=True)
    p2a_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)

    j2a_parser = subparsers.add_parser('j2a', help='Convert JSON schema to Avro schema')
    j2a_parser.add_argument('--jsons', type=str, help='Path to the JSON schema file', required=True)
    j2a_parser.add_argument('--avsc', type=str, help='Path to the Avro schema file', required=True)
    j2a_parser.add_argument('--namespace', type=str, help='Namespace for the Avro schema', required=False)

    args = parser.parse_args()

    if args.command == 'p2a':
        avro_schema_path = args.avsc
        proto_file_path = args.proto
        convert_proto_to_avro(proto_file_path, avro_schema_path)
    elif args.command == 'a2p':
        proto_schema_path = args.proto
        avro_file_path = args.avsc
        convert_avro_to_proto(avro_file_path, proto_schema_path)
    elif args.command == 'j2a':
        json_schema_file_path = args.jsons
        avro_schema_path = args.avsc
        namespace = args.namespace
        convert_jsons_to_avro(json_schema_file_path, avro_schema_path, namespace=namespace)

if __name__ == "__main__":
    main()
