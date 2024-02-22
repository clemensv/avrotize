import json
import os
import re
from typing import Any, Dict, List
import jsonpointer
import requests
import copy
from avrotize.common import avro_name, generic_type
from avrotize.dependency_resolver import inline_dependencies_of, sort_messages_by_dependencies
from urllib.parse import ParseResult, urljoin, urlparse

class JsonToAvroConverter:

    def __init__(self) -> None:
        self.imported_types: Dict[Any, Any] = {}
        self.root_namespace = "example.com"

    def is_empty_type(self, avro_type):
        """Check if the Avro type is an empty type."""
        if len(avro_type) == 0:
            return True
        if isinstance(avro_type, list):
            return all(self.is_empty_type(t) for t in avro_type)
        if isinstance(avro_type, dict):
            if not 'type' in avro_type:
                return True
        return False

    def merge_schemas(self, schemas_arg: list, avro_schemas: list, type_name: str | None = None) -> str | list | dict:
        """Merge multiple Avro type schemas into one."""
        merged_schema: Dict[str, list, dict] = {}
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
                    schemas.append(self.merge_schemas(current_list, avro_schemas, type_name))
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
                deps: List[str] = merged_schema.get('dependencies', [])
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


    def ensure_type(self, type: dict | str | list) -> dict | str | list:
        if isinstance(type, str) or isinstance(type, list) or 'type' in type:
            return type
        
        type['type'] = generic_type()
        return type


    def json_schema_primitive_to_avro_type(self, json_primitive: str | list, format: str | None, enum: list | None, field_name: str, dependencies: list) -> str | dict[str,Any] | list:
        """Convert a JSON-schema primitive type to Avro primitive type."""

        if isinstance(json_primitive, list):
            if enum:
                json_primitive = "string" 
            else:
                union = []
                for item in json_primitive:
                    avro_primitive = self.json_schema_primitive_to_avro_type(item, format, enum, field_name, dependencies)
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
            enum = [avro_name(e) for e in enum if isinstance(e, str) and e != ""] 
            # purge duplicates
            enum = list(dict.fromkeys(enum))
            if len(enum) > 0:
                avro_primitive = {"type": "enum", "symbols": enum, "name": avro_name(field_name + "_enum")}
            else:
                avro_primitive = "string"
        return avro_primitive
        



    def fetch_content(self, url: str | ParseResult):
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
            if not file_path:
                file_path = parsed_url.path
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

    def resolve_reference(self, json_type: dict, base_uri: str, json_doc: dict):
        """Resolve a JSON Pointer reference or a JSON $ref reference."""
        ref = json_type['$ref']
        content = None
        url = urlparse(ref)
        if url.scheme:
            content = self.fetch_content(ref)
        elif url.path:
            file_uri = urljoin(base_uri, url.path)
            content = self.fetch_content(file_uri)
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
            
            

    def json_type_to_avro_type(self, json_type: str | dict, record_name: str, field_name: str, namespace : str, dependencies: list, json_schema: dict, base_uri: str, avro_schema: list, record_stack: list) -> dict | list | str:
        """Convert a JSON type to Avro type."""

        avro_type: list | dict | str = {}
        local_name = avro_name(field_name if field_name else record_name)
        qualified_name = namespace + "." + local_name    

        if isinstance(json_type, dict):

            if 'if' in json_type or 'then' in json_type or 'else' in json_type or 'dependentSchemas' in json_type or 'dependentRequired' in json_type:
                print('WARNING: Conditional schema is not supported and will be ignored.')
                if 'if' in json_type:
                    del json_type['if']
                if 'then' in json_type:
                    del json_type['then']
                if 'else' in json_type:
                    del json_type['else']
                if 'dependentSchemas' in json_type:
                    del json_type['dependentSchemas']
                if 'dependentRequired' in json_type:
                    del json_type['dependentRequired']

            base_type = json_type.copy()
            if 'oneOf' in base_type:
                del base_type['oneOf']
            if 'anyOf' in base_type:
                del base_type['anyOf']
            if 'allOf' in base_type:
                del base_type['allOf']
            json_types = []            

            if 'allOf' in json_type:
                merged_type = copy.deepcopy(base_type)
                allof = json_type['allOf']
                for allof_option in allof:
                    merged_type = {**allof_option, **merged_type}
                json_types.append(merged_type)

            if 'oneOf' in json_type:
                oneof = json_type['oneOf']
                if len(json_types) == 0:
                    for oneof_option in oneof:
                        json_types.append({**base_type, **oneof_option})
                else:
                    new_json_types = []
                    for oneof_option in oneof:
                        for json_type_option in json_types:
                            json_type_option = {**json_type_option, **oneof_option}
                            new_json_types.append(json_type_option)
                    json_types = new_json_types
            
            if 'anyOf' in json_type:
                anyof = json_type['anyOf']
                if len(json_types) == 0:
                    for anyof_option in anyof:
                        json_types.append({**base_type, **anyof_option})
                else:
                    new_json_types = []
                    for anyof_option in anyof:
                        for json_type_option in json_types:
                            json_type_option = {**json_type_option, **anyof_option}
                            new_json_types.append(json_type_option)
                    json_types = new_json_types

            if len(json_types) > 0:
                if len(json_types) == 1:
                    avro_type = self.json_type_to_avro_type(json_types[0], record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
                    if isinstance(avro_type, dict) and self.is_empty_type(avro_type):
                        avro_type['type'] = generic_type()
                    return avro_type
                else:
                    subtypes = []
                    count = 1
                    type_deps = []
                    for json_type_option in json_types:
                        # we only set the field_name if this is not a type reference
                        sub_field_name = avro_name(local_name + "_" + str(count)) if not isinstance(json_type_option, dict) or not '$ref' in json_type_option else None
                        avro_subtype = self.json_type_to_avro_type(json_type_option, record_name, sub_field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
                        if isinstance(avro_subtype, dict) and 'dependencies' in avro_subtype:
                            type_deps.extend(avro_subtype['dependencies'])
                            del avro_subtype['dependencies']
                        if not self.is_empty_type(avro_subtype):
                            if isinstance(avro_subtype, list):
                                subtypes.extend(avro_subtype)
                            else:
                                subtypes.append(avro_subtype)
                        count += 1
                    if len(subtypes) == 1:
                        return subtypes[0]
                    return subtypes

            if 'properties' in json_type and not 'type' in json_type:
                json_type['type'] = 'object'

            if 'description' in json_type and isinstance(avro_type, dict):
                avro_type['doc'] = json_type['description']

            if 'title' in json_type and isinstance(avro_type, dict):
                avro_type['name'] = avro_name(json_type['title'])

            # first, pull in any referenced definitions and merge with this schema
            if '$ref' in json_type:
                ref = json_type['$ref']
                if ref in self.imported_types:
                    type_ref = self.imported_types[ref]
                    if isinstance(type_ref, str):
                        dependencies.append(type_ref)
                    return type_ref
                else:
                    new_base_uri = urljoin(base_uri, json_type['$ref'])
                    resolved_json_type = self.resolve_reference(json_type, base_uri, json_schema)
                    if self.is_empty_type(json_type): 
                        # it's a standalone reference, so will import the type into the schema 
                        # and reference it like it was in the same file
                        type_name = record_name
                        type_namespace = namespace
                        parsed_ref = urlparse(ref)
                        if parsed_ref.fragment:
                            type_name = avro_name(parsed_ref.fragment.split('/')[-1])
                            sub_namespace = '.'.join(parsed_ref.fragment.split('/')[1:-1])
                            type_namespace = self.root_namespace + '.' + sub_namespace if len(sub_namespace) > 0 else self.root_namespace
                        
                        # registering in imported_types ahead of resolving to prevent circular references
                        self.imported_types[ref] = type_name
                        # resolve type
                        deps: List[str] = []
                        avro_type = self.json_type_to_avro_type(resolved_json_type, type_name, field_name, type_namespace, deps, json_schema, new_base_uri, avro_schema, record_stack)
                        if isinstance(avro_type, list) or (not isinstance(avro_type, dict) or not avro_type.get('type') == "record"):
                            if isinstance(avro_type, dict) and not 'type' in avro_type:
                                print(f"WARNING: no type definition for {ref} in record {record_name}: {json.dumps(avro_type)}")
                                avro_type = generic_type()

                            avro_type = {
                                "type": "record",
                                "name": type_name,
                                "namespace": type_namespace,
                                "fields": [
                                    {
                                        "name": "value",
                                        "type": avro_type
                                    }
                                ]
                            }

                        if isinstance(avro_type, dict) and len(deps) > 0:
                            avro_type['dependencies'] = deps
                        self.imported_types[ref] = copy.deepcopy(avro_type)
                        if isinstance(avro_type, dict) and 'name' in avro_type:
                            existing_type = next((t for t in avro_schema if t.get('name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace') ), None)
                            if not existing_type:
                                avro_schema.append(avro_type)
                            full_name = avro_type.get('namespace','')+'.'+avro_type['name'] if 'namespace' in avro_type else avro_type['name']
                            self.imported_types[ref] = full_name
                            dependencies.append(full_name)
                            avro_type = full_name                    
                    else:
                        # it's a reference within a definition, so we will turn this into an inline type
                        json_type.update(resolved_json_type)
                        del json_type['$ref']
                        avro_type = self.json_type_to_avro_type(json_type, record_name, field_name, namespace, dependencies, json_schema, new_base_uri, avro_schema, record_stack)
                        if isinstance(avro_type, dict) and 'name' in avro_type:
                            self.imported_types[ref] = avro_type['name']
                        else:
                            self.imported_types[ref] = avro_type
                    
            # if 'const' is present, make this an enum
            if 'const' in json_type:
                const = json_type['const']
                avro_type = self.merge_schemas([avro_type, {"type": "enum", "symbols": [const], "name": avro_name(local_name)}], avro_schema, local_name)

            json_object_type = json_type.get('type')
            if json_object_type:
                if json_object_type == 'array':
                    if 'items' in json_type:
                        avro_type = self.merge_schemas([avro_type, {"type": "array", "items": self.json_type_to_avro_type(json_type['items'], record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)}], avro_schema, avro_type.get('name', local_name))
                    else:
                        avro_type = self.merge_schemas([avro_type, {"type": "array", "items": generic_type()}], avro_schema, avro_type.get('name', local_name))
                elif json_object_type == 'object' or 'object' in json_object_type:
                    record_stack.append(record_name)
                    avro_type = self.merge_schemas([avro_type, self.json_schema_object_to_avro_record(local_name, json_type, namespace, json_schema, base_uri, avro_schema, record_stack)], avro_schema, avro_type.get('name', local_name))
                    if isinstance(avro_type, dict) and 'dependencies' in avro_type:
                        dependencies.extend(avro_type['dependencies'])
                        del avro_type['dependencies']
                    record_stack.pop()
                else:
                    local_name = field_name = field_name if field_name else local_name+"_value"
                    avro_type = self.json_schema_primitive_to_avro_type(json_object_type, json_type.get('format'), json_type.get('enum'), field_name, dependencies)
            elif 'enum' in json_type:
                local_name = field_name = field_name if field_name else local_name
                enum_namespace = namespace + '.' + record_stack[-1] + field_name 
                avro_type = self.merge_schemas([avro_type,self.json_schema_primitive_to_avro_type("string", json_type.get('format'), json_type.get('enum'), field_name, dependencies)], avro_schema, avro_type.get('name', local_name))
                if isinstance(avro_type, dict):
                    avro_type['namespace'] = enum_namespace
        else:
            avro_type = self.merge_schemas([avro_type,self.json_schema_primitive_to_avro_type(json_type, json_type.get('format'), json_type.get('enum'), field_name, dependencies)], avro_schema, avro_type.get('name', local_name))
        
        if isinstance(avro_type, dict) and 'name' in avro_type:
            if not 'namespace' in avro_type:
                avro_type['namespace'] = namespace
            existing_type = next((t for t in avro_schema if t.get('name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace') ), None)
            if existing_type:
                existing_type_name = existing_type.get('namespace') + '.' + existing_type.get('name')
                if not existing_type_name in dependencies:
                    dependencies.append(existing_type_name)
                return existing_type_name
            avro_type['name'] = local_name
        
        return avro_type

        

    def json_schema_object_to_avro_record(self, name: str, json_object: dict, namespace: str, json_schema: dict, base_uri: str, avro_schema: list, record_stack: list) -> dict:
        """Convert a JSON schema object declaration to an Avro record."""

        dependencies = []

        # handle top-level allOf, anyOf, oneOf
        if isinstance(json_object, dict) and ('allOf' in json_object or 'oneOf' in json_object or 'anyOf' in json_object):
            type = self.json_type_to_avro_type(json_object, name, "value", namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
            if isinstance(type, list):
                type = {
                            "type": "record",
                            "name": name,
                            "namespace": namespace,
                            "fields": [
                                {
                                    "name": "value",
                                    "type": type
                                }
                            ]
                        }
            if dependencies and isinstance(type, dict):
                type['dependencies'] = dependencies
            return type    
        
        title = json_object.get('title')
        record_name = avro_name(name if name else title if title else None)
        if record_name == None:
            raise ValueError(f"Cannot determine record name for json_object {json_object}")
        if len(record_stack) > 0:
            namespace = namespace + '.' + record_stack[-1]
        avro_record = {
            'type': 'record', 
            'name': record_name,
            'namespace': namespace,
            'fields': []
        }
        # we need to prevent circular dependencies, so we will maintain a stack of the in-progress 
        # records and will resolve the cycle as we go
        if record_name in record_stack:
            # to break the cycle, we will use a containment type that references 
            # the record that is being defined
            ref_name = avro_name(record_name + "_ref")
            return {
                    "type": "record",
                    "name": ref_name,
                    "namespace": namespace,
                    "fields": [
                        {
                            "name": record_name,
                            "type": record_name
                        }
                    ]
                }
        record_stack.append(record_name)

        required_fields = json_object.get('required', [])
        if 'properties' in json_object:
            for field_name, field in json_object['properties'].items():
                # skip fields with an bad or empty type
                if not isinstance(field, dict):
                    continue
                avro_field_type = self.ensure_type(self.json_type_to_avro_type(field, record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack))
                if 'dependencies' in avro_field_type:
                    dependencies.extend(avro_field_type['dependencies'])
                    del avro_field_type['dependencies']
                    
                if avro_field_type is None:
                    raise ValueError(f"avro_field_type is None for field {field_name}")
                
                if isinstance(avro_field_type,dict) and 'type' in avro_field_type and not avro_field_type['type'] in ["array", "map", "record", "enum", "fixed"]:
                    avro_field_type = avro_field_type['type']               
                
                if not field_name in required_fields and not 'null' in avro_field_type:
                    if isinstance(avro_field_type, list):
                        avro_field_type.append('null')
                        avro_field = {"name": avro_name(field_name), "type": avro_field_type}
                    else:
                        avro_field = {"name": avro_name(field_name), "type": ["null", avro_field_type]}
                else:
                    avro_field = {"name": avro_name(field_name), "type": avro_field_type}
                
                if field.get('description'):
                    avro_field['doc'] = field['description']

                avro_record["fields"].append(avro_field)
            
            if 'additionalProperties' in json_object and isinstance(json_object['additionalProperties'], dict):
                additional_props = json_object['additionalProperties']
                values_type = self.json_type_to_avro_type(additional_props, record_name, record_name + "_extensions", namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
                if self.is_empty_type(values_type):
                    values_type = generic_type()
                avro_record['fields'].append(
                    {
                        "name": record_name, 
                        "namespace": namespace,
                        "type": { 
                            "type": "map", 
                            "values": values_type
                            }
                    })
            elif 'patternProperties' in json_object and isinstance(json_object['patternProperties'], dict):
                pattern_props = json_object['patternProperties']
                for pattern_name, props in pattern_props.items():
                    pattern_name = re.sub(r'[^a-zA-Z0-9_]', '_', pattern_name)
                    if pattern_name == "":
                        pattern_name = "extensions"
                    prop_type = self.ensure_type(self.json_type_to_avro_type(props, record_name, pattern_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack))
                    if self.is_empty_type(prop_type):
                        raise ValueError(f"prop_type for pattern name {pattern_name} and record_name {record_name} is empty")
                    avro_record['fields'].append(
                        {
                            "name": avro_name(pattern_name), 
                            "namespace": namespace,
                            "type": {
                                "type": "map",
                                "values": copy.deepcopy(prop_type)
                            }
                        })
        else:
            if 'additionalProperties' in json_object and isinstance(json_object['additionalProperties'], dict):
                additional_props = json_object['additionalProperties']
                values_type = self.json_type_to_avro_type(additional_props, record_name, record_name + "_extensions", namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
                if self.is_empty_type(values_type):
                    values_type = generic_type()
                avro_record['fields'].append(
                    {
                        "name": "values", 
                        "type": { 
                            "type": "map", 
                            "values": copy.deepcopy(values_type)
                        }
                    })
            elif 'patternProperties' in json_object and isinstance(json_object['patternProperties'], dict):
                pattern_props = json_object['patternProperties']
                for pattern_name, props in pattern_props.items():
                    pattern_name = re.sub(r'[^a-zA-Z0-9_]', '_', pattern_name)
                    if pattern_name == "":
                        pattern_name = "extensions"
                    type = self.ensure_type(self.json_type_to_avro_type(props, record_name, pattern_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack))
                    avro_record['fields'].append(
                        {
                            "name": avro_name(pattern_name), 
                            "namespace": namespace,
                            "type": {
                                "type": "map",
                                "values": copy.deepcopy(type)
                            }
                        })
            elif 'type' in json_object and (json_object['type'] == 'object' or 'object' in json_object['type']) and \
                not 'allOf' in json_object and not 'oneOf' in json_object and not 'anyOf' in json_object:
                avro_record['fields'].append(
                    {
                        "name": "values",
                        "type": {
                            "type": "map",
                            "values": generic_type()
                        }
                    })
            elif 'type' in json_object and (json_object['type'] == 'array' or 'array' in json_object['type']) and \
                not 'allOf' in json_object and not 'oneOf' in json_object and not 'anyOf' in json_object:
                if 'items' in json_object:
                    avro_type = self.json_type_to_avro_type(json_object['items'], record_name, 'values', namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
                else:
                    avro_type = generic_type()
                avro_record['fields'].append(
                    {
                        "name": "values",
                        "type": {
                            "type": "array", 
                            "items": copy.deepcopy(avro_type)
                            }
                    })
            else:
                avro_record = {
                    "name": record_name,
                    "namespace": namespace,
                }
        
        if 'description' in json_object:
            avro_record['doc'] = json_object['description']
        if len(dependencies) > 0:
            # dedupe the list
            dependencies = list(set(dependencies))    
            avro_record['dependencies'] = dependencies        

        record_stack.pop()
        return avro_record
        

    def jsons_to_avro(self, json_schema: dict | list, namespace: str, base_uri: str) -> list:
        """Convert a JSON-schema to an Avro-schema."""
        avro_schema = []
        record_stack = []

        parsed_url = urlparse(base_uri)
        schema_name = 'record'
        if 'type' in json_schema or 'allOf' in json_schema or 'oneOf' in json_schema or 'anyOf' in json_schema or 'properties' in json_schema:
            self.process_definition(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema)
        elif 'swagger' in json_schema or ('definitions' in json_schema and not 'type' in json_schema):
            json_schema_defs = json_schema.get('definitions', {})
            if not json_schema_defs:
                raise ValueError('No definitions found in swagger file')
            for schema_name, schema in json_schema_defs.items():
                if 'type' in schema or 'allOf' in schema or 'oneOf' in schema or 'anyOf' in schema or 'properties' in schema:
                    self.process_definition(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema)
                else:
                    self.process_definition_list(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema.copy())
        elif isinstance(json_schema, list):
            self.process_definition_list(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema)
        else:
            raise ValueError('No schema found in input file')
        
        avro_schema = sort_messages_by_dependencies(avro_schema)
        
        if parsed_url.fragment:
            self.imported_types.clear()
            fragment_schema = []
            json_pointer = parsed_url.fragment
            schema_name = parsed_url.fragment.split('/')[-1]
            schema = jsonpointer.resolve_pointer(json_schema, json_pointer)
            avro_schema_item = self.json_schema_object_to_avro_record(schema_name, schema, namespace, json_schema, base_uri, fragment_schema, record_stack)
            inline_dependencies_of(avro_schema, avro_schema_item)
            return avro_schema_item
        
        return avro_schema

    def process_definition_list(self, json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema_list):
        for sub_schema_name, schema in json_schema_list.items():
            if not isinstance(schema, dict) and not isinstance(schema, list):
                continue
            if 'type' in schema or 'allOf' in schema or 'oneOf' in schema or 'anyOf' in schema or 'properties' in schema:
                self.process_definition(json_schema, namespace, base_uri, avro_schema, record_stack, sub_schema_name, schema)
                continue
            self.process_definition_list(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema)

    def process_definition(self, json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema):
        """ Process a schema definition. """
        avro_schema_item_list = self.json_schema_object_to_avro_record(schema_name, schema, namespace, json_schema, base_uri, avro_schema, record_stack)
        if not isinstance(avro_schema_item_list, list) and not isinstance(avro_schema_item_list, dict):
            return
        # the call above usually returns a single record, but we pretend it's normally a list to handle allOf/anyOf/oneOf cases
        if not isinstance(avro_schema_item_list, list):
            avro_schema_item_list = [avro_schema_item_list]
        for avro_schema_item in avro_schema_item_list:
            avro_schema_item['name'] = avro_name(schema_name)
            existing_type = next((t for t in avro_schema if t.get('name') == avro_schema_item['name'] and t.get('namespace') == avro_schema_item.get('namespace') ), None)
            if not existing_type:
                avro_schema.append(avro_schema_item)
        

    def id_to_avro_namespace(self, id: str) -> str:
        """Convert a XSD namespace to Avro Namespace."""
        parsed_url = urlparse(id)
        # strip the file extension 
        path = parsed_url.path.rsplit('.')[0]
        path_segments = path.strip('/').replace('-', '_').split('/')
        reversed_path_segments = reversed(path_segments)
        namespace_suffix = '.'.join(reversed_path_segments)
        namespace_prefix = '.'.join(reversed(parsed_url.hostname.split('.')))
        namespace = f"{namespace_prefix}.{namespace_suffix}"
        return namespace

    def convert_jsons_to_avro(self, json_schema_file_path: str, avro_schema_path: str, namespace: str = None) -> list:
        """Convert JSON schema file to Avro schema file."""
        # turn the file path into a file URI if it's not a URI already
        parsed_url = urlparse(json_schema_file_path)
        if not parsed_url.hostname and not parsed_url.scheme == "file":
            json_schema_file_path = 'file://' + json_schema_file_path
            parsed_url = urlparse(json_schema_file_path)
        content = self.fetch_content(parsed_url.geturl())
        json_schema = json.loads(content)

        if not namespace:
            namespace = parsed_url.geturl().replace('\\','/').replace('-','_').split('/')[-1].split('.')[0]
            # get the $id if present 
            if '$id' in json_schema:
                namespace = self.id_to_avro_namespace(json_schema['$id'])
        self.root_namespace = namespace
        
        # drop the file name from the parsed URL to get the base URI
        avro_schema = self.jsons_to_avro(json_schema, namespace, parsed_url.geturl())
        if len(avro_schema) == 1:
            avro_schema = avro_schema[0]

        # create the directory for the Avro schema file if it doesn't exist
        dir = os.path.dirname(avro_schema_path)
        if dir != '' and not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        with open(avro_schema_path, 'w') as avro_file:
            json.dump(avro_schema, avro_file, indent=4)
        return avro_schema
    

def convert_jsons_to_avro(json_schema_file_path: str, avro_schema_path: str, namespace: str = None) -> list:
    """Convert JSON schema file to Avro schema file."""
    converter = JsonToAvroConverter()
    return converter.convert_jsons_to_avro(json_schema_file_path, avro_schema_path, namespace)