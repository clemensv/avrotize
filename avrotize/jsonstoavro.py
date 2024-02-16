import json
import os
import re
import jsonpointer
import requests
from avrotize.dependency_resolver import inline_dependencies_of, sort_messages_by_dependencies
from urllib.parse import ParseResult, urljoin, urlparse


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

imported_types = {}

def fetch_content(url: str | ParseResult):
    # Parse the URL to determine the scheme
    if isinstance(url, str):
        parsed_url = urlparse(url)
    else:
        parsed_url = url
    scheme = parsed_url.scheme

    # Handle HTTP and HTTPS URLs
    if scheme in ['http', 'https']:
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raises an HTTPError if the response status code is 4XX/5XX
            return response.text
        except requests.RequestException as e:
            return f'Error fetching {url}: {e}'

    # Handle file URLs
    elif scheme == 'file':
        # Remove the leading 'file://' from the path for compatibility
        file_path = parsed_url.netloc
        # On Windows, a file URL might start with a '/' but it's not part of the actual path
        if os.name == 'nt' and file_path.startswith('/'):
            file_path = file_path[1:]
        try:
            with open(file_path, 'r') as file:
                return file.read()
        except Exception as e:
            return f'Error reading file at {file_path}: {e}'

    else:
        return f'Unsupported URL scheme: {scheme}'

def resolve_reference(json_type: dict, base_uri: str, json_doc: dict):
    """Resolve a JSON Pointer reference or a JSON $ref reference."""
    ref = json_type['$ref']
    content = None
    url = urlparse(ref)
    if url.scheme:
        content = fetch_content(ref)
    elif url.path:
        file_uri = urljoin(base_uri, url.path)
        content = fetch_content(file_uri)
    if content:
        try:
            json_schema = json.loads(content)
            # resolve the JSON Pointer reference, if any
            if url.fragment:
                json_schema = jsonpointer.resolve_pointer(json_schema, url.fragment)
            imported_types[ref] = json_schema
            return json_schema
        except json.JSONDecodeError:
            raise Exception(f'Error decoding JSON from {ref}')
    
    if url.fragment:
        json_pointer = url.fragment
        ref_schema = jsonpointer.resolve_pointer(json_doc, json_pointer)
        if ref_schema:
            imported_types[ref] = ref_schema
            return ref_schema
    return json_type
        
        

def json_type_to_avro_type(json_type: str | dict, field_name: str, namespace : str, dependencies: list, json_schema: dict, base_uri: str, avro_schema: list) -> dict:
    """Convert a JSON type to Avro type."""
    if isinstance(json_type, dict):
        if '$ref' in json_type:
            ref = json_type['$ref']
            if ref in imported_types:
                return imported_types[ref]
            else:
                new_base_uri = urljoin(base_uri, json_type['$ref'])
                resolved_json_type = resolve_reference(json_type, base_uri, json_schema)
                if len(json_type) == 1: 
                    # it's a standalone reference, so will import the type into the schema 
                    # and reference it like it was in the same file
                    parsed_ref = urlparse(ref)
                    if parsed_ref.fragment:
                        field_name = parsed_ref.fragment.split('/')[-1]
                    avro_type = json_type_to_avro_type(resolved_json_type, field_name, namespace, dependencies, json_schema, new_base_uri, avro_schema)
                    existing_type = next((t for t in avro_schema if t.get('name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace') ), None)
                    if not existing_type:
                        avro_schema.append(avro_type)
                    imported_types[ref] = avro_type['name']
                    dependencies.append(avro_type['name'])
                    avro_type = avro_type['name']
                else:
                    # it's a reference within a definition, so we will turn this into an inline type
                    json_type.update(resolved_json_type)
                    del json_type['$ref']
                    avro_type = json_type_to_avro_type(json_type, field_name, namespace, dependencies, json_schema, new_base_uri, avro_schema)
                    imported_types[ref] = avro_type['name']
                return avro_type

        t = json_type.get('type', 'object')
        if t == 'array':
            avro_type = {"type": "array", "items": json_type_to_avro_type(json_type['items'], field_name, namespace, dependencies, json_schema, base_uri, avro_schema)}
        elif 'oneOf' in json_type:
            avro_type = [json_type_to_avro_type(one_type, field_name, namespace, dependencies, json_schema, base_uri, avro_schema) for one_type in json_type['oneOf']]
        elif 'allOf' in json_type:
            avro_type = merge_schemas([json_type_to_avro_type(schema, field_name, namespace, dependencies, json_schema, base_uri, avro_schema) for schema in json_type['allOf']])
        elif 'anyOf' in json_type:
            avro_type = merge_schemas([json_type_to_avro_type(schema, field_name, namespace, dependencies, json_schema, base_uri, avro_schema) for schema in json_type['anyOf']])
        elif t == 'object':
            avro_type = json_schema_object_to_avro_record(field_name, json_type, namespace, json_schema, base_uri, avro_schema)
        else:
            avro_type = json_schema_primitive_to_avro_type(t, None, None, field_name, dependencies)
    else:
        avro_type = json_schema_primitive_to_avro_type(json_type, None, None, field_name, dependencies)
    
    if 'name' in avro_type:
        existing_type = next((t for t in avro_schema if t.get('name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace') ), None)
        if existing_type:
            return existing_type.get('name')
        
    return avro_type

def json_schema_object_to_avro_record(name: str, json_object: dict, namespace: str, json_schema: dict, base_uri: str, avro_schema: list) -> dict:
    """Convert a JSON schema object declaration to an Avro record."""

    dependencies = []
    title = json_object.get('title')
    if title:
        title = title.replace(" ", "_")
    avro_record = {
        'type': 'record', 
        'name': title if title else name if name else 'record',
        'namespace': namespace,
        'fields': []
    }

    required_fields = json_object.get('required', [])
    if 'properties' in json_object:
        for field_name, field in json_object['properties'].items():
            avro_field_type = json_type_to_avro_type(field, field_name, namespace, dependencies, json_schema, base_uri, avro_schema)
                
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

        if 'additionalProperties' in json_object and isinstance(json_object['additionalProperties'], dict):
            additional_props = json_object['additionalProperties']
            avro_record['fields'].append({"name": "additionalProperties", "type": "map", "values": json_type_to_avro_type(additional_props, "additionalProperties", namespace, dependencies, json_schema, base_uri, avro_schema)})
        elif 'patternProperties' in json_object and isinstance(json_object['patternProperties'], dict):
            pattern_props = json_object['patternProperties']
            prop_types = []
            for pattern, props in pattern_props.items():
                pattern = re.sub(r'[^a-zA-Z0-9_]', '_', pattern)
                prop_types.append(json_type_to_avro_type(props, pattern, namespace, dependencies, json_schema, base_uri, avro_schema))
            avro_record = { 
                "type": "map", 
                "name": title if title else name if name else 'record', 
                "namespace": namespace,
                "values": prop_types[0] if len(prop_types) == 1 else prop_types
            }
    else:
        if 'additionalProperties' in json_object and isinstance(json_object['additionalProperties'], dict):
            additional_props = json_object['additionalProperties']
            avro_record = { 
                "type": "map", 
                "name": title if title else name if name else 'record', 
                "namespace": namespace,
                "values": json_type_to_avro_type(additional_props, "additionalProperties", namespace, dependencies, json_schema, base_uri, avro_schema)
            }
        elif 'patternProperties' in json_object and isinstance(json_object['patternProperties'], dict):
            pattern_props = json_object['patternProperties']
            prop_types = []
            for pattern, props in pattern_props.items():
                pattern = re.sub(r'[^a-zA-Z0-9_]', '_', pattern)
                prop_types.append(json_type_to_avro_type(props, pattern, namespace, dependencies, json_schema, base_uri, avro_schema))
            avro_record = { 
                "type": "map", 
                "name": title if title else name if name else 'record', 
                "namespace": namespace,
                "values": prop_types[0] if len(prop_types) == 1 else prop_types
            }
        else:
            avro_record = {
                "type": "map",
                "name": title if title else name if name else "empty",
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
    

def jsons_to_avro(json_schema: dict | list, namespace: str, base_uri: str) -> list:
    """Convert a JSON-schema to an Avro-schema."""
    avro_schema = []

    parsed_url = urlparse(base_uri)
    if parsed_url.fragment:
        json_pointer = parsed_url.fragment
        schema_name = parsed_url.fragment.split('/')[-1]
        schema = jsonpointer.resolve_pointer(json_schema, json_pointer)
        avro_schema_item = json_schema_object_to_avro_record(schema_name, schema, namespace, json_schema, base_uri, avro_schema)
        avro_schema.append(avro_schema_item)
        inline_dependencies_of(avro_schema, avro_schema_item)
        return avro_schema
    elif 'swagger' in json_schema:
        json_schema_defs = json_schema.get('definitions', {})
        if not json_schema_defs:
            raise ValueError('No definitions found in swagger file')
        for schema_name, schema in json_schema_defs.items():
            avro_schema_item = json_schema_object_to_avro_record(schema_name, schema, namespace, json_schema, base_uri, avro_schema)
            existing_type = next((t for t in avro_schema if t.get('name') == avro_schema_item['name'] and t.get('namespace') == avro_schema_item.get('namespace') ), None)
            if not existing_type:
                avro_schema_item['name'] = schema_name
                avro_schema.append(avro_schema_item)
        return sort_messages_by_dependencies(avro_schema)
    else:
        if not isinstance(json_schema, list):
            json_schema = [json_schema]

        for schema in json_schema:
            if schema['type'] == 'object':
                avro_type = json_schema_object_to_avro_record(None, schema, namespace, json_schema, base_uri, avro_schema)
                existing_type = next((t for t in avro_schema if t.get('name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace') ), None)
                if not existing_type:
                    avro_schema.append(avro_type)
        return sort_messages_by_dependencies(avro_schema)
    
def convert_jsons_to_avro(json_schema_file_path: str, avro_schema_path: str, namespace: str = None) -> list:
    """Convert JSON schema file to Avro schema file."""
    # turn the file path into a file URI if it's not a URI already
    parsed_url = urlparse(json_schema_file_path)
    if not parsed_url.hostname and not parsed_url.scheme == "file":
        json_schema_file_path = 'file://' + json_schema_file_path
        parsed_url = urlparse(json_schema_file_path)
    content = fetch_content(parsed_url.geturl())
    json_schema = json.loads(content)
    # drop the file name from the parsed URL to get the base URI
    avro_schema = jsons_to_avro(json_schema, namespace, parsed_url.geturl())
    if len(avro_schema) == 1:
        avro_schema = avro_schema[0]
    with open(avro_schema_path, 'w') as avro_file:
        json.dump(avro_schema, avro_file, indent=4)
    return avro_schema