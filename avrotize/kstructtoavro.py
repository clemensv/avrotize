"""

Convert a Kafka schema to an Avro schema.

"""

import json


def kafka_type_to_avro_type(kafka_field):
    """Convert a Kafka field type to an Avro field type."""
    kafka_to_avro_types = {
        'int32': 'int',
        'int64': 'long',
        'string': 'string',
        'boolean': 'boolean',
        'bytes': 'bytes',
        'array': 'array',
        'map': 'map',
        'struct': 'record'
    }

    if kafka_field['type'] in kafka_to_avro_types:
        return kafka_to_avro_types[kafka_field['type']]
    elif isinstance(kafka_field['type'], dict):  # Nested struct
        return convert_schema(kafka_field['type'])
    else:
        raise ValueError(f"Unsupported Kafka type: {kafka_field['type']}")


def convert_field(field):
    """Convert a Kafka field to an Avro field."""
    avro_field = {
        'name': field['field'],
        'type': []
    }

    if field['optional']:
        avro_field['type'].append('null')

    kafka_field_type = kafka_type_to_avro_type(field)

    if field['type'] == 'array':
        item_type = kafka_type_to_avro_type(field['items'])
        avro_field['type'].append({'type': 'array', 'items': item_type})
    elif field['type'] == 'map':
        value_type = kafka_type_to_avro_type(field['values'])
        avro_field['type'].append({'type': 'map', 'values': value_type})
    elif field['type'] == 'struct':
        avro_field['type'].append(convert_schema(field))
    else:
        avro_field['type'].append(kafka_field_type)

    if len(avro_field['type']) == 1:
        avro_field['type'] = avro_field['type'][0]

    return avro_field


def convert_schema(kafka_schema):
    """Convert a Kafka schema to an Avro schema."""
    avro_schema = {
        'type': 'record',
        'name': kafka_schema.get('name', 'MyRecord'),
        'fields': []
    }

    for field in kafka_schema['fields']:
        avro_schema['fields'].append(convert_field(field))

    return avro_schema


def convert_kafka_struct_to_avro_schema(kafka_schema_file_path, avro_file_path):
    """Read a Kafka schema from a file, convert it to an Avro schema, and save it to another file."""

    if not kafka_schema_file_path:
        raise ValueError("Kafka schema file path is required.")

    # Open and read the Kafka schema file
    with open(kafka_schema_file_path, 'r', encoding='utf-8') as kafka_schema_file:
        kafka_schema_data = json.load(kafka_schema_file)

    # Assuming the whole file content is the schema
    if isinstance(kafka_schema_data, dict) and 'schema' in kafka_schema_data:
        kafka_schema = kafka_schema_data['schema']
    else:
        kafka_schema = kafka_schema_data
    avro_schema = convert_schema(kafka_schema)

    # Write the converted Avro schema to a file
    with open(avro_file_path, 'w', encoding='utf-8') as avro_file:
        json.dump(avro_schema, avro_file, indent=4)
