# Avrotize

Avrotize is a command-line tool that allows you to convert between different
schema formats: Avro, Proto, XML Schema and JSON Schema. It is designed to be
easy to use and flexible, supporting a variety of use cases.

## Installation

You can install Avrotize from PyPI:

```bash
pip install avrotize
```

## Usage

Avrotize provides several commands for converting between different schema formats.

### Convert Proto schema to Avro schema

```bash
avrotize p2a --proto <path_to_proto_file> --avsc <path_to_avro_schema_file>
```

### Convert Avro schema to Proto schema

```bash
avrotize a2p --proto <path_to_proto_file> --avsc <path_to_avro_schema_file>
```

### Convert JSON schema to Avro schema

```bash
avrotize j2a --jsons <path_to_json_schema_file> --avsc <path_to_avro_schema_file> [--namespace <avro_schema_namespace>]
```

### Convert XML Schema (XSD) to Avro schema

```bash
avrotize x2a --xsd <path_to_xsd_file> --avsc <path_to_avro_schema_file> [--namespace <avro_schema_namespace>]
```

### Convert Avro schema to Kusto table declaration

```bash
avrotize a2k --avsc <path_to_avro_schema_file> --kusto <path_to_kusto_kql_file> [--record-type <record_type>]
```

### Convert Avro schema to T-SQL table definition

```bash
avrotize a2tsql --avsc <path_to_avro_schema_file> --tsql <path_to_sql_file> [--record-type <record_type>]
```


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Avrotize is released under the Apache License. See the LICENSE file for more details.
