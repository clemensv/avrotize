import json
from avrotize.dependency_resolver import sort_messages_by_dependencies


def json_schema_primitive_to_avro_type(json_primitive: str, format: str, enum: list, field_name: str, dependencies: list) -> str:
    """Convert a JSON-schema primitive type to Avro primitive type."""
    avro_primitive_map = {
        'string': 'string',
        'integer': 'int',
        'number': 'float',
        'boolean': 'boolean',
    }
    if json_primitive in avro_primitive_map:
        avro_primitive = avro_primitive_map[json_primitive]
    else:
        dependencies.append(json_primitive)
        avro_primitive = json_primitive

    if format:
        if format in ('date-time', 'date'):
            avro_primitive = {'type': 'int', 'logicalType': 'date'}
        elif format in ('time'):
            avro_primitive = {'type': 'int', 'logicalType': 'time-millis'}
        elif format in ('duration'):
            avro_primitive = {'type': 'fixed', 'size': 12, 'logicalType': 'duration'}
        elif format in ('uuid'):
            avro_primitive = {'type': 'string', 'logicalType': 'uuid'}
    
    if enum:
        # replace white space with underscore
        enum = [e.replace(" ", "_") for e in enum]
        avro_primitive = {"type": "enum", "symbols": enum, "name": field_name + "_enum"}


    return avro_primitive
    

def merge_schemas(schemas):
    """Merge multiple Avro type schemas into one."""
    merged_schema = {}
    for schema in schemas:
        if 'type' not in merged_schema:
            merged_schema = schema
        else:
            if schema['type'] != merged_schema['type']:
                raise ValueError("Schema of different types cannot be merged")
            merged_schema['name'] = merged_schema['name'] + schema['name']
            merged_schema['fields'].extend(schema['fields'])
    return merged_schema


def json_type_to_avro_type(json_type: str | dict, field_name: str, namespace : str, dependencies: list) -> dict:
    """Convert a JSON type to Avro type."""
    if isinstance(json_type, dict):
        t = json_type.get('type', 'object')
        if t == 'array':
            avro_type = {"type": "array", "items": json_type_to_avro_type(json_type['items'], field_name, namespace, dependencies)}
        elif 'oneOf' in json_type:
            avro_type = [json_type_to_avro_type(one_type, field_name, namespace, dependencies) for one_type in json_type['oneOf']]
        elif 'allOf' in json_type:
            avro_type = merge_schemas([json_type_to_avro_type(schema, field_name, namespace, dependencies) for schema in json_type['allOf']])
        elif t == 'object':
            avro_type = json_schema_object_to_avro_record(field_name, json_type, namespace)
        else:
            avro_type = json_schema_primitive_to_avro_type(t, None, None, field_name, dependencies)
    else:
        avro_type = json_schema_primitive_to_avro_type(json_type, None, None, field_name, dependencies)
    return avro_type

def json_schema_object_to_avro_record(name: str, json_object: dict, namespace) -> dict:
    """Convert a JSON schema object declaration to an Avro record."""

    dependencies = []
    title = json_object.get('title')
    avro_record = {
        'type': 'record', 
        'name': title if title else name if name else 'record',
        'namespace': namespace,
        'fields': []
    }

    required_fields = json_object.get('required', [])
    if 'properties' in json_object:
        for field_name, field in json_object['properties'].items():
            avro_field_type = json_type_to_avro_type(field, field_name, namespace, dependencies)
                
            if avro_field_type is None:
                raise ValueError(f"avro_field_type is None for field {field_name}")
            
            if not field_name in required_fields:
                avro_field = {"name": field_name, "type": ["null", avro_field_type]}
            else:
                avro_field = {"name": field_name, "type": avro_field_type}
            
            if field.get('description'):
                avro_field['doc'] = field['description']

            avro_record["fields"].append(avro_field)
        if len(dependencies) > 0:
            avro_record['dependencies'] = dependencies        
    else:
        avro_record = {
            "type": "map",
            "values": [
                        "null",
                        "boolean",
                        "double",
                        "string"
                ]
            }       

    if 'description' in json_object:
        avro_record['doc'] = json_object['description']
    return avro_record
    

def jsons_to_avro(json_schema: dict | list, namespace: str) -> list:
    """Convert a JSON-schema to an Avro-schema."""
    avro_schema = []

    # check whether this is indeed a swagger file and then grab the definitions section
    if 'swagger' in json_schema:
        json_schema = json_schema.get('definitions', {})
        if not json_schema:
            raise ValueError('No definitions found in swagger file')
        for schema_name, schema in json_schema.items():
            avro_schema_item = json_schema_object_to_avro_record(schema_name, schema, namespace)
            avro_schema_item['name'] = schema_name
            avro_schema.append(avro_schema_item)
        return sort_messages_by_dependencies(avro_schema)
    else:
        if not isinstance(json_schema, list):
            json_schema = [json_schema]

        for schema in json_schema:
            if schema['type'] == 'object':
                avro_schema.append(json_schema_object_to_avro_record(None, schema, namespace))
        return sort_messages_by_dependencies(avro_schema)
    
def convert_jsons_to_avro(json_schema_file_path: str, avro_schema_path: str, namespace: str = None) -> list:
    """Convert JSON schema file to Avro schema file."""
    with open(json_schema_file_path, 'r') as schema_file:
        json_schema = json.load(schema_file)
    avro_schema = jsons_to_avro(json_schema, namespace)
    with open(avro_schema_path, 'w') as avro_file:
        json.dump(avro_schema, avro_file, indent=4)
    return avro_schema