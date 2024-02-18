import json
import os
import re
import jsonpointer
import requests
import copy
from avrotize.dependency_resolver import inline_dependencies_of, sort_messages_by_dependencies
from urllib.parse import ParseResult, urljoin, urlparse

def avro_name(name):
    """Convert a name into an Avro name."""
    return name.replace(" ", "_").replace(".", "_").replace("-", "_").replace(":", "_")

def merge_schemas(schemas_arg: list, avro_schemas: list, type_name: str = None) -> str | list | dict:
    """Merge multiple Avro type schemas into one."""
    merged_schema = {}
    schemas = []
    for schema_entry in schemas_arg:
        if isinstance(schema_entry, list):
            if len(schema_entry) == 0:
                continue
            remaining_list = copy.deepcopy(schemas_arg)
            remaining_list.remove(schema_entry)
            for schema in schema_entry:
                current_list = copy.deepcopy(remaining_list)
                current_list.append(copy.deepcopy(schema) if isinstance(schema, dict) or isinstance(schema,list) else schema)
                schemas.append(merge_schemas(current_list, avro_schemas, type_name))
            break
        elif isinstance(schema_entry, dict):
            if len(schema_entry) == 0:
                continue
            schemas.append(copy.deepcopy(schema_entry))
        else:
            schemas.append(copy.deepcopy(schema_entry) if isinstance(schema_entry,list) else schema_entry)

    if len(schemas) == 1 and isinstance(schemas[0], str):
        return schemas[0]

    if type_name:
        merged_schema["name"] = type_name
    
    for schema in schemas:
        if 'dependencies' in schema:
            deps = merged_schema.get('dependencies', [])
            deps.extend(schema['dependencies'])
            merged_schema['dependencies'] = deps
        if (isinstance(schema, list) or isinstance(schema, dict)) and len(schema) == 0:
            continue
        if isinstance(schema, str):
            sch = next((s for s in avro_schemas if s.get('name') == schema), None)
            if sch:
                merged_schema.update(sch)
            else:
                merged_schema['type'] = schema
        elif 'type' not in schema or 'type' not in merged_schema:
            merged_schema.update(schema)
        else:
            if 'type' in merged_schema and schema['type'] != merged_schema['type']:
                merged_schema['type'] = [schema['type'],merged_schema['type']]
            if not type_name:
                merged_schema['name'] = avro_name(merged_schema.get('name','') + schema.get('name',''))
            if 'fields' in schema:
                if 'fields' in merged_schema:
                    for field in schema['fields']:
                        if field not in merged_schema['fields']:
                            merged_schema['fields'].append(field)
                        else:
                            merged_schema_field = next(f for f in merged_schema['fields'] if f.get('name') == field.get('name'))
                            if merged_schema_field["type"] != field["type"]:
                                merged_schema_field["type"] = [field["type"],merged_schema_field["type"]]
                            if "doc" in field and "doc" not in merged_schema_field:
                                merged_schema_field["doc"] = field["doc"]
                else:
                    merged_schema['fields'] = schema['fields']
                            
    return merged_schema


def generic_type() -> dict:
    simple_type_union = ["null", "boolean", "int", "long", "float", "double", "bytes", "string"]
    l2 = simple_type_union.copy()
    l2.extend([
        {
            "type": "array",
            "items": simple_type_union
        },
        {
            "type": "map",
            "values": simple_type_union
        }])
    l1 = simple_type_union.copy()
    l1.extend([
        {
            "type": "array",
            "items": l2
        },
        {
            "type": "map",
            "values": l2
        }])
    return l1

def ensure_type(type: dict | str | list) -> dict | str:
    if isinstance(type, str) or isinstance(type, list) or 'type' in type:
        return type
    
    type['type'] = generic_type()
    return type


def json_schema_primitive_to_avro_type(json_primitive: str | list, format: str, enum: list, field_name: str, dependencies: list) -> str:
    """Convert a JSON-schema primitive type to Avro primitive type."""

    if isinstance(json_primitive, list):
        union = []
        for item in json_primitive:
            avro_primitive = json_schema_primitive_to_avro_type(item, format, enum, field_name, dependencies)
            union.append(avro_primitive)
        return union

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
        avro_primitive = {"type": "enum", "symbols": enum, "name": avro_name(field_name + "_enum")}


    return avro_primitive
    


# this maps URIs to resolved Avro types
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
            return json_schema
        except json.JSONDecodeError:
            raise Exception(f'Error decoding JSON from {ref}')
    
    if url.fragment:
        json_pointer = url.fragment
        ref_schema = jsonpointer.resolve_pointer(json_doc, json_pointer)
        if ref_schema:
            return ref_schema
    return json_type
        
        

def json_type_to_avro_type(json_type: str | dict, record_name: str, field_name: str, namespace : str, dependencies: list, json_schema: dict, base_uri: str, avro_schema: list, record_stack: list) -> dict:
    """Convert a JSON type to Avro type."""

    avro_type = {}
    qualified_name =  avro_name(json_type.get('title',f'{record_name}_{field_name}' if record_name else field_name))

    if isinstance(json_type, dict):

        if 'properties' in json_type and not 'type' in json_type:
            json_type['type'] = 'object'

        if 'description' in json_type:
            avro_type['doc'] = json_type['description']

        if 'title' in json_type:
            avro_type['name'] = avro_name(json_type['title'])

        # first, pull in any referenced definitions and merge with this schema
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
                        type_name = avro_name(parsed_ref.fragment.split('/')[-1])
                        if record_name != type_name:
                            field_name = type_name
                    avro_type = json_type_to_avro_type(resolved_json_type, record_name, field_name, namespace, dependencies, json_schema, new_base_uri, avro_schema, record_stack)
                    imported_types[ref] = avro_type
                    if isinstance(avro_type, dict) and 'name' in avro_type:
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
                    avro_type = json_type_to_avro_type(json_type, record_name, field_name, namespace, dependencies, json_schema, new_base_uri, avro_schema, record_stack)
                    if 'name' in json_type:
                        imported_types[ref] = avro_type['name']
                    else:
                        imported_types[ref] = avro_type
                
        # if 'const' is present, make this an enum
        if 'const' in json_type:
            const = json_type['const']
            avro_type = merge_schemas([avro_type, {"type": "enum", "symbols": [const], "name": avro_name(avro_type.get('name', qualified_name))  }], avro_schema, avro_type.get('name', qualified_name))

        json_object_type = json_type.get('type')
        if json_object_type:
            if json_object_type == 'array':
                if 'items' in json_type:
                    avro_type = merge_schemas([avro_type, {"type": "array", "items": json_type_to_avro_type(json_type['items'], record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)}], avro_schema, avro_type.get('name', qualified_name))
                else:
                    avro_type = merge_schemas([avro_type, {"type": "array", "items": generic_type()}], avro_schema, avro_type.get('name', qualified_name))
            elif json_object_type == 'object' or 'object' in json_object_type:
                avro_type = merge_schemas([avro_type, json_schema_object_to_avro_record(field_name, json_type, namespace, json_schema, base_uri, avro_schema, record_stack)], avro_schema, avro_type.get('name', qualified_name))
            else:
                avro_type = json_schema_primitive_to_avro_type(json_object_type, None, None, field_name, dependencies)
            

        if 'oneOf' in json_type:
            oneof_types = []
            count = 1
            fname = field_name
            for one_type in json_type['oneOf']:
                fname = field_name + "_" + str(count)
                # capitalize the first character
                fname = avro_name(fname[0].capitalize() + fname[1:])
                one_avro_type = json_type_to_avro_type(one_type, record_name, fname, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
                one_avro_type = merge_schemas([avro_type, one_avro_type], avro_schema, one_avro_type.get('name', fname) if isinstance(one_avro_type, dict) else fname)
                if isinstance(one_avro_type, dict):
                    one_avro_type['name'] = fname
                oneof_types.append(one_avro_type)
                count += 1
            avro_type = oneof_types
        
        if 'anyOf' in json_type:
            avro_type = [
               json_type_to_avro_type(one_type, record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack) for one_type in json_type['anyOf']
            ]

        if 'allOf' in json_type:
            schemas = [json_type_to_avro_type(schema, record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack) for schema in json_type['allOf']]
            if len(avro_type) > 0:
                schemas.append(avro_type)
            avro_type = merge_schemas(schemas, avro_schema)       
 
    else:
        avro_type = merge_schemas([avro_type,json_schema_primitive_to_avro_type(json_type, None, None, field_name, dependencies)], avro_schema, avro_type.get('name', qualified_name))
    
    if 'name' in avro_type:
        existing_type = next((t for t in avro_schema if t.get('name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace') ), None)
        if existing_type:
            existing_type_name = existing_type.get('name')
            if not existing_type_name in dependencies:
                dependencies.append(existing_type_name)
            return existing_type_name
    
    return avro_type

      

def json_schema_object_to_avro_record(name: str, json_object: dict, namespace: str, json_schema: dict, base_uri: str, avro_schema: list, record_stack: list) -> dict:
    """Convert a JSON schema object declaration to an Avro record."""

    dependencies = []
    title = json_object.get('title')
    if title:
        title = title.replace(" ", "_")
    avro_record = {
        'type': 'record', 
        'name': avro_name(title if title else name if name else 'record'),
        'namespace': namespace,
        'fields': []
    }

    # we need to prevent circular dependencies, so we will maintain a stack of the in-progress 
    # records and will resolve the cycle as we go
    record_name = avro_record['name']
    if record_name in record_stack:
        # to break the cycle, we will use a containment type that references 
        # the record that is being defined
        ref_name = avro_name("_".join(record_stack) + "_" + record_name + "_ref")
        return {
                "type": "record",
                "fields": [
                    {
                        "name": record_name,
                        "type": record_name
                    }
                ]
            }
    record_stack.append(avro_record['name'])

    required_fields = json_object.get('required', [])
    if 'properties' in json_object:
        for field_name, field in json_object['properties'].items():
            # skip fields with an bad or empty type
            if not isinstance(field, dict):
                continue
            avro_field_type = ensure_type(json_type_to_avro_type(field, record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack))
            if 'dependencies' in avro_field_type:
                dependencies.extend(avro_field_type['dependencies'])
                del avro_field_type['dependencies']
                
            if avro_field_type is None:
                raise ValueError(f"avro_field_type is None for field {field_name}")
            
            if 'type' in avro_field_type and not avro_field_type['type'] in ["array", "map", "record", "enum", "fixed"]:
                avro_field_type = avro_field_type['type']               
            
            if not field_name in required_fields:
                avro_field = {"name": avro_name(field_name), "type": ["null", avro_field_type]}
            else:
                avro_field = {"name": avro_name(field_name), "type": avro_field_type}
            
            if field.get('description'):
                avro_field['doc'] = field['description']

            avro_record["fields"].append(avro_field)
        if len(dependencies) > 0:
            avro_record['dependencies'] = dependencies        

        if 'additionalProperties' in json_object and isinstance(json_object['additionalProperties'], dict):
            additional_props = json_object['additionalProperties']
            avro_record['fields'].append(
                {
                    "name": "additionalProperties", 
                    "type": { 
                        "type": "map", 
                        "values": ensure_type(json_type_to_avro_type(additional_props, record_name, "additionalProperties", namespace, dependencies, json_schema, base_uri, avro_schema, record_stack))
                        }
                })
        elif 'patternProperties' in json_object and isinstance(json_object['patternProperties'], dict):
            pattern_props = json_object['patternProperties']
            prop_types = []
            for pattern, props in pattern_props.items():
                pattern = re.sub(r'[^a-zA-Z0-9_]', '_', pattern)
                prop_types.append(ensure_type(json_type_to_avro_type(props, record_name, pattern, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)))
            avro_record = { 
                "type": "map", 
                "name": avro_name(title if title else name if name else 'record'), 
                "namespace": namespace,
                "values": prop_types[0] if len(prop_types) == 1 else prop_types
            }
    else:
        if 'additionalProperties' in json_object and isinstance(json_object['additionalProperties'], dict):
            additional_props = json_object['additionalProperties']
            avro_record = { 
                "type": "map", 
                "name": avro_name(title if title else name if name else 'record'), 
                "namespace": namespace,
                "values": ensure_type(json_type_to_avro_type(additional_props, record_name, "additionalProperties", namespace, dependencies, json_schema, base_uri, avro_schema, record_stack))
            }
        elif 'patternProperties' in json_object and isinstance(json_object['patternProperties'], dict):
            pattern_props = json_object['patternProperties']
            prop_types = []
            for pattern, props in pattern_props.items():
                pattern = re.sub(r'[^a-zA-Z0-9_]', '_', pattern)
                prop_types.append(ensure_type(json_type_to_avro_type(props, record_name, pattern, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)))
            avro_record = { 
                "type": "map", 
                "name": avro_name(title if title else name if name else 'record'), 
                "namespace": namespace,
                "values": prop_types[0] if len(prop_types) == 1 else prop_types
            }
        elif 'type' in json_object and (json_object['type'] == 'object' or 'object' in json_object['type']) and \
             not 'allOf' in json_object and not 'oneOf' in json_object and not 'anyOf' in json_object:
            avro_record = {
                "type": "map",
                "name": avro_name(title if title else name if name else "empty"),
                "values": generic_type()
                }       
        else:
            avro_record = {
                "name": avro_name(title if title else name if name else "empty"),
            }

    if 'description' in json_object:
        avro_record['doc'] = json_object['description']

    record_stack.pop()
    return avro_record
    

def jsons_to_avro(json_schema: dict | list, namespace: str, base_uri: str) -> list:
    """Convert a JSON-schema to an Avro-schema."""
    avro_schema = []
    record_stack = []

    parsed_url = urlparse(base_uri)
    if 'swagger' in json_schema or ('definitions' in json_schema and not 'type' in json_schema):
        json_schema_defs = json_schema.get('definitions', {})
        if not json_schema_defs:
            raise ValueError('No definitions found in swagger file')
        for schema_name, schema in json_schema_defs.items():
            if 'type' in schema or 'allOf' in schema or 'oneOf' in schema or 'anyOf' in schema or 'properties' in schema:
                process_definition(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema)
            else:
                process_definition_list(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema.copy())
    else:
        schema_name = 'record'
        if 'type' in json_schema or 'allOf' in json_schema or 'oneOf' in json_schema or 'anyOf' in json_schema or 'properties' in json_schema:
            process_definition(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema)
        else:
            process_definition_list(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema)
    
    avro_schema = sort_messages_by_dependencies(avro_schema)
    
    if parsed_url.fragment:
        imported_types.clear()
        fragment_schema = []
        json_pointer = parsed_url.fragment
        schema_name = parsed_url.fragment.split('/')[-1]
        schema = jsonpointer.resolve_pointer(json_schema, json_pointer)
        avro_schema_item = json_schema_object_to_avro_record(schema_name, schema, namespace, json_schema, base_uri, fragment_schema, record_stack)
        inline_dependencies_of(avro_schema, avro_schema_item)
        return avro_schema_item
    
    return avro_schema

def process_definition_list(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema_list):
    for sub_schema_name, schema in json_schema_list.items():
        if not isinstance(schema, dict) and not isinstance(schema, list):
            continue
        composite_name = avro_name(schema_name+'_'+sub_schema_name) if schema_name else avro_name(sub_schema_name)
        if 'type' in schema or 'allOf' in schema or 'oneOf' in schema or 'anyOf' in schema or 'properties' in schema:
            process_definition(json_schema, namespace, base_uri, avro_schema, record_stack, composite_name, schema)
            continue
        process_definition_list(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema)

def process_definition(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema):
    avro_schema_item = json_schema_object_to_avro_record(schema_name, schema, namespace, json_schema, base_uri, avro_schema, record_stack)
    existing_type = next((t for t in avro_schema if t.get('name') == avro_schema_item['name'] and t.get('namespace') == avro_schema_item.get('namespace') ), None)
    if not existing_type:
        avro_schema_item['name'] = avro_name(schema_name)
        avro_schema.append(avro_schema_item)
    

def id_to_avro_namespace(id: str) -> str:
    """Convert a XSD namespace to Avro Namespace."""
    parsed_url = urlparse(id)
    # strip the file extension 
    path = parsed_url.path.rsplit('.')[0]
    path_segments = path.strip('/').split('/')
    reversed_path_segments = reversed(path_segments)
    namespace_prefix = '.'.join(reversed_path_segments)
    namespace_suffix = parsed_url.hostname
    namespace = f"{namespace_prefix}.{namespace_suffix}"
    return namespace

def convert_jsons_to_avro(json_schema_file_path: str, avro_schema_path: str, namespace: str = None) -> list:
    """Convert JSON schema file to Avro schema file."""
    # turn the file path into a file URI if it's not a URI already
    parsed_url = urlparse(json_schema_file_path)
    if not parsed_url.hostname and not parsed_url.scheme == "file":
        json_schema_file_path = 'file://' + json_schema_file_path
        parsed_url = urlparse(json_schema_file_path)
    content = fetch_content(parsed_url.geturl())
    json_schema = json.loads(content)

    if not namespace:
        namespace = parsed_url.geturl().replace('\\','/').split('/')[-1].split('.')[0]
        # get the $id if present 
        if '$id' in json_schema:
            namespace = id_to_avro_namespace(json_schema['$id'])
    namespace = avro_name(namespace)
    # drop the file name from the parsed URL to get the base URI
    avro_schema = jsons_to_avro(json_schema, namespace, parsed_url.geturl())
    if len(avro_schema) == 1:
        avro_schema = avro_schema[0]

    # create the directory for the Avro schema file if it doesn't exist
    os.makedirs(os.path.dirname(avro_schema_path), exist_ok=True)
    with open(avro_schema_path, 'w') as avro_file:
        json.dump(avro_schema, avro_file, indent=4)
    return avro_schema