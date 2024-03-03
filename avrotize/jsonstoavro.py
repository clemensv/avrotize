import json
import os
import re
import sys
import traceback
from typing import Any, Dict, List, Tuple
import jsonpointer
from jsonpointer import JsonPointerException
import requests
import copy
from avrotize.common import avro_name, generic_type
from avrotize.dependency_resolver import inline_dependencies_of, sort_messages_by_dependencies
from urllib.parse import ParseResult, urlparse, unquote

class JsonToAvroConverter:
    """
    Converts JSON schema to Avro schema.

    Attributes:
    imported_types: A dictionary of imported type schemas.
    root_namespace: The namespace for the root schema.
    max_recursion_depth: The maximum recursion depth.
    types_with_unmerged_types: A list of types with unmerged types.
    content_cache: A dictionary for caching fetched URLs.
    utility_namespace: The namespace for utility types.
    maximize_compatiblity: A flag to maximize compatibility.

    """

    def __init__(self) -> None:
        self.imported_types: Dict[Any, Any] = {}
        self.root_namespace = 'example.com'
        self.max_recursion_depth = 40
        self.types_with_unmerged_types: List[dict] = []
        self.content_cache: Dict[str,str] = {}
        self.utility_namespace = 'utility.vasters.com'
        self.maximize_compatibility = False

    def is_empty_type(self, avro_type):
        """
        Check if the Avro type is an empty type.

        Parameters:
        avro_type (any): The Avro type to check.

        Returns:
        bool: True if the Avro type is empty, False otherwise.
        """
        if len(avro_type) == 0:
            return True
        if isinstance(avro_type, list):
            return all(self.is_empty_type(t) for t in avro_type)
        if isinstance(avro_type, dict):
            if not 'type' in avro_type:
                return True
            if (avro_type['type'] == 'record' and (not 'fields' in avro_type or len(avro_type['fields']) == 0)) or \
               (avro_type['type'] == 'enum' and (not 'symbols' in avro_type or len(avro_type['symbols']) == 0)) or \
               (avro_type['type'] == 'array' and (not 'items' in avro_type or not avro_type['items'])) or \
               (avro_type['type'] == 'map' and (not 'values' in avro_type or not avro_type['values'])):                
                return True
        return False
    
    def is_empty_json_type(self, json_type):
        """
        Check if the JSON type is an empty type.

        Parameters:
        json_type (any): The JSON type to check.

        Returns:
        bool: True if the JSON type is empty, False otherwise.
        """
        if len(json_type) == 0:
            return True
        if isinstance(json_type, list):
            return all(self.is_empty_json_type(t) for t in json_type)
        if isinstance(json_type, dict):
            if not 'type' in json_type:
                return True
        return False

    def flatten_union(self, type_list: list) -> list:
        """
        Flatten the list of types in a union into a single list.

        Args:
            type_list (list): The list of types in a union.

        Returns:
            list: The flattened list of types.

        """
        flat_list = []
        for t in type_list:
            if isinstance(t, list):
                inner = self.flatten_union(t)
                for u in inner:
                    if not u in flat_list:
                        flat_list.append(u)
            elif not t in flat_list:
                flat_list.append(t)
        # remove duplicates
        flat_list_1 = []
        for t in flat_list:
            if not t in flat_list_1:
                flat_list_1.append(t)
        return flat_list_1
    
    def merge_avro_schemas(self, schemas: list, avro_schemas: list, type_name: str | None = None, deps: List[str] = []) -> str | list | dict:
        """Merge multiple Avro type schemas into one."""

        def split_merge(schema1, schema2, schema_list,offset):
            """ return the continuing schema merges of incompatible schemas """
            remaining_schemas = schema_list[offset + 1:] if len(schema_list) > offset else []
            if isinstance(schema2, dict) and 'dependencies' in schema2:
                deps.extend(schema2['dependencies'])
                del schema2['dependencies']
            if isinstance(schema1, dict) and 'dependencies' in schema1:
                deps.extend(schema1['dependencies'])
                del schema1['dependencies']
            schema1_merged = self.merge_avro_schemas([schema2] + remaining_schemas, avro_schemas, type_name, deps)
            schema2_merged = self.merge_avro_schemas([schema1] + remaining_schemas, avro_schemas, type_name, deps)
            if not self.is_empty_type(schema1_merged) and not self.is_empty_type(schema2_merged):
                return self.flatten_union([schema1_merged, schema2_merged])
            else:	
                if not self.is_empty_type(schema1_merged):
                    return schema1_merged
                if not self.is_empty_type(schema2_merged):
                    return schema2_merged
                # if both are empty, we'll return an empty record
                return {'type': 'record', 'fields': []}
        
        merged_schema: dict = {}
        if len(schemas) == 1:
            return schemas[0]
        if type_name:
            merged_schema['name'] = type_name
        for i, schema in enumerate(schemas):
            schema = copy.deepcopy(schema)
            if isinstance(schema, dict) and 'dependencies' in schema:
                deps1: List[str] = merged_schema.get('dependencies', [])
                deps1.extend(schema['dependencies'])
                merged_schema['dependencies'] = deps1
            if (isinstance(schema, list) or isinstance(schema, dict)) and len(schema) == 0:
                continue
            if isinstance(schema, str):
                sch = next((s for s in avro_schemas if s.get('name') == schema), None)
                if sch:
                    merged_schema.update(sch)
                else:
                    merged_schema['type'] = schema
            elif isinstance(schema, list):
                # the incoming schema is a list, so it's a union
                if not 'type' in merged_schema:
                    merged_schema['type'] = schema
                else:
                    if isinstance(merged_schema['type'], list):
                        merged_schema['type'].extend(schema) 
                    else:
                        if isinstance(merged_schema['type'], str):
                            if merged_schema['type'] == 'record' or merged_schema['type'] == 'enum' or merged_schema['type'] == 'fixed' \
                               or merged_schema['type'] == 'map' or merged_schema['type'] == 'array':
                                return split_merge(merged_schema, schema, schemas, i)
                            else:
                                merged_schema['type'] = [merged_schema['type']]    
                        else:
                            merged_schema['type'].extend(schema)                                            
            elif schema and 'type' not in schema or 'type' not in merged_schema:
                merged_schema.update(schema)
            elif schema:
                if 'type' in merged_schema and schema['type'] != merged_schema['type']:
                    return split_merge(merged_schema, schema, schemas, i)           
                if not type_name:
                    merged_schema['name'] = avro_name(merged_schema.get('name', '') + schema.get('name', ''))
                if 'fields' in schema:
                    if 'fields' in merged_schema:
                        for field in schema['fields']:
                            if field not in merged_schema['fields']:
                                merged_schema['fields'].append(field)
                            else:
                                merged_schema_field = next(f for f in merged_schema['fields'] if f.get('name') == field.get('name'))
                                if merged_schema_field['type'] != field['type']:
                                    merged_schema_field['type'] = [field['type'],merged_schema_field['type']]
                                if 'doc' in field and 'doc' not in merged_schema_field:
                                    merged_schema_field['doc'] = field['doc']
                    else:
                        merged_schema['fields'] = schema['fields']                                
        return merged_schema

    def merge_json_schemas(self, json_schemas: list[dict], intersect: bool = False) -> dict:
            """
            Merge multiple JSON schemas into one.

            Args:
                json_schemas (list[dict]): A list of JSON schemas to be merged.
                intersect (bool, optional): If True, only keep the intersection of the required fields. Defaults to False.

            Returns:
                dict: The merged JSON schema.

            """
            merged_type:dict = {}

            for json_schema in json_schemas:
                if 'type' not in json_schema or 'type' not in merged_type:
                    for key in json_schema:
                        if not key in merged_type:
                            merged_type[key] = copy.deepcopy(json_schema[key])
                        else:
                            if key == 'required':
                                merged_type[key] = list(set(merged_type[key]).union(set(json_schema[key])))
                            if key == 'name' or key == 'title' or key == 'description':
                                merged_type[key] = merged_type[key] + json_schema[key]
                            elif isinstance(merged_type[key], dict):
                                merged_type[key] =  merged_type[key].update(copy.deepcopy(json_schema[key]))
                            elif isinstance(merged_type[key], list) and isinstance(json_schema[key], list):
                                for item in json_schema[key]:
                                    if item not in merged_type[key]:
                                        merged_type[key].append(item)
                            else:
                                if merged_type[key] == None:
                                    merged_type[key] = json_schema[key]
                                else:
                                    merged_type[key] = [merged_type[key], copy.deepcopy(json_schema[key])]
                else:
                    if 'type' in merged_type and json_schema['type'] != merged_type['type']:
                        if isinstance(merged_type['type'], str):
                            merged_type['type'] = [merged_type['type']]
                        merged_type['type'].append(json_schema['type'])                    
                    if 'required' in json_schema and 'required' in merged_type:
                        merged_type['required'] = list(set(merged_type['required']).union(set(json_schema['required'])))
                    merged_type['name'] = merged_type.get('name','') + json_schema.get('name','')
                    if 'properties' in json_schema and 'properties' in merged_type:
                        merged_type['properties'].update(json_schema['properties'])
                    if 'enum' in json_schema and 'enum' in merged_type:
                        merged_type['enum'] = list(set(merged_type['enum']).union(set(json_schema['enum'])))
                    if 'format' in json_schema and 'format' in merged_type:
                        merged_type['format'] = merged_type['format'] + json_schema['format']

            if intersect:
                # only keep the intersection of the required fields
                if 'required' in merged_type:
                    new_required = merged_type['required']
                    for json_schema in json_schemas:
                        new_required = list(set(new_required).intersection(set(json_schema.get('required',[]))))
                    merged_type['required'] = new_required

            return merged_type
        

    def ensure_type(self, type: dict | str | list) -> dict | str | list:
        """
        Ensures that the given type is valid by adding a 'type' field if it is missing.
        
        Args:
            type (dict | str | list): The type to ensure.
            
        Returns:
            dict | str | list: The ensured type.
        """
        if isinstance(type, str) or isinstance(type, list) or 'type' in type:
            return type
        
        type['type'] = generic_type()
        return type


    def json_schema_primitive_to_avro_type(self, json_primitive: str | list, format: str | None, enum: list | None, record_name: str, field_name: str, namespace: str, dependencies: list) -> str | dict[str,Any] | list:
        """
        Convert a JSON-schema primitive type to Avro primitive type.

        Args:
            json_primitive (str | list): The JSON-schema primitive type to be converted.
            format (str | None): The format of the JSON primitive type, if applicable.
            enum (list | None): The list of enum values, if applicable.
            record_name (str): The name of the record.
            field_name (str): The name of the field.
            namespace (str): The namespace of the Avro type.
            dependencies (list): The list of dependencies.

        Returns:
            str | dict[str,Any] | list: The converted Avro primitive type.

        """
        if isinstance(json_primitive, list):
            if enum:
                json_primitive = 'string' 
            else:
                union = []
                for item in json_primitive:
                    enum2 = item.get('enum') if isinstance(item, dict) else None
                    format2 = item.get('format') if isinstance(item, dict) else None
                    avro_primitive = self.json_schema_primitive_to_avro_type(item, format2, enum2, record_name, field_name, namespace+'.'+record_name+'.'+field_name, dependencies)
                    union.append(avro_primitive)
                return union

        if json_primitive == 'string':
            avro_primitive = 'string'
        elif json_primitive == 'integer':
            avro_primitive = 'int'
        elif json_primitive == 'number':
            avro_primitive = 'float'
        elif json_primitive == 'boolean':
            avro_primitive = 'boolean'
        elif not format:
            if isinstance(json_primitive, str):
                dependencies.append(json_primitive)
            avro_primitive = json_primitive

        # if you've got { 'type': 'string', 'format': ['date-time', 'duration'] }, I'm sorry
        if format and isinstance(format, str):
            if format in ('date-time', 'date'):
                avro_primitive = {'type': 'int', 'logicalType': 'date'}
            elif format in ('time'):
                avro_primitive = {'type': 'int', 'logicalType': 'time-millis'}
            elif format in ('duration'):
                avro_primitive = {'type': 'fixed', 'size': 12, 'logicalType': 'duration'}
            elif format in ('uuid'):
                avro_primitive = {'type': 'string', 'logicalType': 'uuid'}
        
        return avro_primitive

    
    def fetch_content(self, url: str | ParseResult):
        """
        Fetches the content from the specified URL.

        Args:
            url (str or ParseResult): The URL to fetch the content from.

        Returns:
            str: The fetched content.

        Raises:
            requests.RequestException: If there is an error while making the HTTP request.
            Exception: If there is an error while reading the file.

        """
        # Parse the URL to determine the scheme
        if isinstance(url, str):
            parsed_url = urlparse(url)
        else:
            parsed_url = url

        if parsed_url.geturl() in self.content_cache:
            return self.content_cache[parsed_url.geturl()]
        scheme = parsed_url.scheme

        # Handle HTTP and HTTPS URLs
        if scheme in ['http', 'https']:
            try:
                response = requests.get(url if isinstance(url, str) else parsed_url.geturl())
                response.raise_for_status()  # Raises an HTTPError if the response status code is 4XX/5XX
                self.content_cache[parsed_url.geturl()] = response.text
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
               with open(file_path, 'r', encoding='utf-8') as file:
                    text = file.read()
                    self.content_cache[parsed_url.geturl()] = text
                    return text
            except Exception as e:
                return f'Error reading file at {file_path}: {e}'

        else:
            return f'Unsupported URL scheme: {scheme}'

    def resolve_reference(self, json_type: dict, base_uri: str, json_doc: dict) -> Tuple[dict, dict]:
        """
        Resolve a JSON Pointer reference or a JSON $ref reference.

        Args:
            json_type (dict): The JSON type containing the reference.
            base_uri (str): The base URI of the JSON document.
            json_doc (dict): The JSON document containing the reference.

        Returns:
            Tuple[dict, dict]: A tuple containing the resolved JSON schema and the original JSON schema document.

        Raises:
            Exception: If there is an error decoding JSON from the reference.
            Exception: If there is an error resolving the JSON Pointer reference.

        """
        try:
            ref = json_type['$ref']
            content = None
            url = urlparse(ref)
            if url.scheme:
                content = self.fetch_content(ref)
            elif url.path:
                file_uri = self.compose_uri(base_uri, url)
                content = self.fetch_content(file_uri)
            if content:
                try:
                    json_schema_doc = json_schema = json.loads(content)
                    # resolve the JSON Pointer reference, if any
                    if url.fragment:
                        json_schema = jsonpointer.resolve_pointer(json_schema, url.fragment)
                    return json_schema, json_schema_doc
                except json.JSONDecodeError:
                    raise Exception(f'Error decoding JSON from {ref}')
            
            if url.fragment:
                json_pointer = unquote(url.fragment)
                ref_schema = jsonpointer.resolve_pointer(json_doc, json_pointer)
                if ref_schema:
                    return ref_schema, json_doc
        except JsonPointerException as e:
            raise Exception(f'Error resolving JSON Pointer reference for {base_uri}')
        return json_type, json_doc

    def compose_uri(self, base_uri, url):
        if isinstance(url, str):
            url = urlparse(url)
            if url.scheme:
                return url.geturl()
        if not url.path and not url.netloc:
            return base_uri
        if base_uri.startswith('file'):
            parsed_file_uri = urlparse(base_uri)
            dir = os.path.dirname(parsed_file_uri.netloc if parsed_file_uri.netloc else parsed_file_uri.path)
            filename = os.path.join(dir, url.path)
            file_uri = f'file://{filename}'
        else:
            file_uri = os.path.join(os.path.dirname(url), url.path)
        return file_uri           
    
    def get_field_type_name(self, field: dict) -> str:
        if isinstance(field['type'], str):
            return field['type']
        elif isinstance(field['type'], list):
            names = []
            for field_type in field['type']:
                if isinstance(field_type, str):
                    names.append(field_type)
                elif isinstance(field_type, dict):
                    names.append(self.get_field_type_name(field_type))
                else:
                    names.append('union')
            return ', '.join(names)
        elif isinstance(field['type'], dict) and 'type' in field['type']:
            return field['type']['type']
        return 'union'
            

    def json_type_to_avro_type(self, json_type: str | dict, record_name: str, field_name: str, namespace : str, dependencies: list, json_schema: dict, base_uri: str, avro_schema: list, record_stack: list, recursion_depth = 1) -> dict | list | str:
        """Convert a JSON type to Avro type."""

        try:
            if recursion_depth >= self.max_recursion_depth:
                print(f'WARNING: Maximum recursion depth reached for {record_name} at field {field_name}')
                return generic_type()
            
            avro_type: list | dict | str = {}
            local_name = avro_name(field_name if field_name else record_name)
            qualified_name = namespace + '.' + local_name    
            hasAnyOf = isinstance(json_type, dict) and 'anyOf' in json_type

            if isinstance(json_type, dict):

                json_object_type = json_type.get('type')
                if isinstance(json_object_type, list):
                    # if the 'type' is a list, we map it back to a string
                    # if the list has only one item or if the list has two items 
                    # and one of them is 'null'
                    # otherwise, we will construct and inject a oneOf type
                    # and split the type
                    if len(json_object_type) == 1:
                        json_object_type = json_object_type[0]
                    elif len(json_object_type) == 2 and 'null' in json_object_type:
                        if json_object_type[0] == 'null':
                            json_object_type = json_object_type[1]
                        else:
                            json_object_type = json_object_type[0]
                    else:
                        oneof = []
                        for option in json_object_type:
                            if not option == 'null':
                                oneof.append({
                                    'type': option
                                })
                        if len(oneof) > 0:
                            del json_type['type']
                            json_type['oneOf'] = oneof                  
                        

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
                    # if the json type is an allOf, we merge all types into one
                    # this may be lossy if aspects of the types overlap but differ
                    type_list = [copy.deepcopy(base_type)]
                    for allof_option in json_type['allOf']:
                        if isinstance(allof_option, dict) and '$ref' in allof_option:
                            resolved_json_type, resolved_schema = self.resolve_reference(allof_option, base_uri, json_schema)
                            del allof_option['$ref']
                            allof_option = self.merge_json_schemas([allof_option, resolved_json_type])
                        type_list.append(copy.deepcopy(allof_option))
                    merged_type = self.merge_json_schemas(type_list, intersect=False)
                    json_types.append(merged_type)

                if 'oneOf' in json_type:
                    # if the json type is a oneOf, we create a type union of all types
                    if len(json_types) == 0:
                        type_to_process = copy.deepcopy(base_type)
                    else:
                        type_to_process = copy.deepcopy(json_types.pop())
                    json_types = []
                    oneof = json_type['oneOf']
                    if len(json_types) == 0:
                        for oneof_option in oneof:
                            if isinstance(oneof_option, dict) and 'type' in oneof_option and 'type' in type_to_process and not type_to_process.get('type') == oneof_option.get('type'):
                                # we can't merge these due to conflicting types, so we pass the option-type on as-is
                                json_types.append(oneof_option)
                            else:
                                json_types.append(self.merge_json_schemas([type_to_process, oneof_option], intersect=True))  
                    else:
                        new_json_types = []
                        for oneof_option in oneof:
                            for json_type_option in json_types:
                                json_type_option = self.merge_json_schemas([json_type_option, oneof_option], intersect=True)
                                new_json_types.append(json_type_option)
                        json_types = new_json_types
                
                if 'anyOf' in json_type:
                    types_to_process = json_types.copy() if len(json_types) > 0 else [copy.deepcopy(base_type)]
                    json_types = []
                    for type_to_process in types_to_process:
                        type_list = [copy.deepcopy(type_to_process)]
                        # anyOf is a list of types where any number from 1 to all 
                        # may match the data. Trouble with anyOf is that it doesn't
                        # really have a semantic interpretation in the context of Avro.
                        for anyof_option in json_type['anyOf']:
                            if isinstance(anyof_option, dict) and '$ref' in anyof_option:
                                # if we have a ref, we can't merge into the base type, so we pass it on as-is.
                                # into the JSON type list
                                json_types.append(copy.deepcopy(anyof_option))
                            else:
                                type_list.append(copy.deepcopy(anyof_option))
                        merged_type = self.merge_json_schemas(type_list, intersect=True)
                        json_types.append(merged_type)

                if len(json_types) > 0:
                    if len(json_types) == 1:
                        avro_type = self.json_type_to_avro_type(json_types[0], record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack, recursion_depth + 1)
                        if isinstance(avro_type, dict) and self.is_empty_type(avro_type) and not 'allOf' in json_type:
                            avro_type['type'] = generic_type()
                        return avro_type
                    else:
                        try:    
                            record_stack.append(field_name if field_name else record_name)
                            subtypes = []
                            count = 1
                            type_deps: List[str] = []
                            for json_type_option in json_types:
                                
                                if isinstance(json_type_option, dict) and '$ref' in json_type_option:
                                    ref = json_type_option['$ref']
                                    if ref in self.imported_types:
                                        avro_subtype = self.imported_types[ref]
                                        subtypes.append(avro_subtype)
                                        type_deps.append(avro_subtype)
                                        continue

                                subtype_deps: List[str] = []
                                sub_field_name = avro_name(local_name + '_' + str(count)) if not isinstance(json_type_option, dict) or not '$ref' in json_type_option else None
                                avro_subtype = self.json_type_to_avro_type(json_type_option, record_name, sub_field_name, namespace, subtype_deps, json_schema, base_uri, avro_schema, record_stack, recursion_depth + 1)
                                if not avro_subtype:
                                    continue
                                if isinstance(avro_subtype, dict) and 'name' in avro_subtype and 'type' in avro_subtype and (avro_subtype['type'] == 'record' or avro_subtype['type'] == 'enum'):
                                    # we have a standalone record or enum so we need to add it to the schema at the top-level
                                    # and reference it as a dependency from the parent type if it's not already been added.
                                    existing_type = next((t for t in avro_schema if t.get('name') == avro_subtype['name'] and t.get('namespace') == avro_subtype.get('namespace') ), None)
                                    if not existing_type:
                                        if subtype_deps:
                                            if not 'dependencies' in avro_subtype:
                                                avro_subtype['dependencies'] = subtype_deps
                                            else:
                                                avro_subtype['dependencies'].extend(subtype_deps)                                    
                                        if self.is_empty_type(avro_subtype):
                                            print(f'WARN: Standalone type {avro_subtype["name"]} is empty')
                                        if avro_subtype['type'] != 'enum' and avro_subtype['type'] != 'record' and avro_subtype['type'] != 'fixed':
                                            raise ValueError(f'WARN: Standalone type {avro_subtype["name"]} is not a record or enum or fixed type')
                                        avro_schema.append(avro_subtype)
                                    full_name = avro_subtype.get('namespace','')+'.'+avro_subtype['name'] if 'namespace' in avro_subtype else avro_subtype['name']
                                    subtype_deps = [full_name]
                                    avro_subtype = full_name
                                if isinstance(avro_subtype, dict) and 'dependencies' in avro_subtype:
                                    subtype_deps.extend(avro_subtype['dependencies'])
                                    del avro_subtype['dependencies']
                                if len(subtype_deps) > 0:
                                    type_deps.extend(subtype_deps)
                                if not self.is_empty_type(avro_subtype):
                                    if isinstance(avro_subtype, list):
                                        subtypes.extend(copy.deepcopy(avro_subtype))
                                    else:
                                        subtypes.append(copy.deepcopy(avro_subtype))
                                count += 1
                            if len(type_deps) > 0:
                                dependencies.extend(type_deps)
                            if len(subtypes) == 1:
                                return subtypes[0]
                        finally:
                            record_stack.pop()

                        if hasAnyOf:
                            # if all subtypes are strings, they are either primitive types or type references
                            # which means there's nothing to merge, so we'll return the list of types
                            if all([isinstance(st, str) for st in subtypes]):
                                return subtypes

                            # we now has a list of types that may match the data, but this would be
                            # an Avro union which is mutually exclusive. We will merge this list 
                            # into a record type in postprocessing when all types are available
                            if not isinstance(avro_type, dict):
                                avro_type = {}
                            avro_type['unmerged_types'] = subtypes
                            avro_type['type'] = 'record'                 
                            avro_type['name'] = avro_name(local_name)
                            avro_type['namespace'] = namespace
                            avro_type['fields'] = []
                            if 'description' in json_type:
                                avro_type['doc'] = json_type['description']
                            json_type = {}
                        else:
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
                        # reference was already resolved, so we can resolve the reference simply by returning the type
                        type_ref = copy.deepcopy(self.imported_types[ref])
                        if isinstance(type_ref, str):
                            dependencies.append(type_ref)
                        return type_ref
                    else:
                        new_base_uri = self.compose_uri(base_uri, json_type['$ref'])
                        resolved_json_type, resolved_schema = self.resolve_reference(json_type, base_uri, json_schema)
                        if self.is_empty_json_type(json_type): 
                            # it's a standalone reference, so will import the type into the schema 
                            # and reference it like it was in the same file
                            type_name = record_name
                            type_namespace = namespace
                            parsed_ref = urlparse(ref)
                            if parsed_ref.fragment:
                                type_name = avro_name(parsed_ref.fragment.split('/')[-1])
                                sub_namespace = '.'.join(parsed_ref.fragment.split('/')[2:-1])
                                type_namespace = self.root_namespace + '.' + sub_namespace if len(sub_namespace) > 0 else self.root_namespace
                            
                            # registering in imported_types ahead of resolving to prevent circular references.
                            # we only cache the type if it's forseeable that it is usable as a standalone type
                            # which means that it must be either a record or an enum or a fixed type when converted
                            # to Avro. That means we look for the presence of 'type', 'properties', 'allOf', 'anyOf',
                            # and 'enum' in the resolved type.
                            if resolved_json_type and (('type' in resolved_json_type and resolved_json_type['type'] == 'object') or 'properties' in resolved_json_type or 'enum' in resolved_json_type or \
                                                    'allOf' in resolved_json_type or 'anyOf' in resolved_json_type):
                                self.imported_types[ref] = type_namespace + '.' + type_name
                            # resolve type
                            deps: List[str] = []
                            resolved_avro_type: dict | list | str | None = self.json_type_to_avro_type(resolved_json_type, type_name, '', type_namespace, deps, resolved_schema, new_base_uri, avro_schema, [], recursion_depth + 1)
                            if isinstance(resolved_avro_type, str):
                                dependencies.extend(deps)
                                return resolved_avro_type
                            if isinstance(resolved_avro_type, list) or (not isinstance(resolved_avro_type, dict) or (not resolved_avro_type.get('type') == 'record' and not resolved_avro_type.get('type') == 'enum')):
                                if isinstance(resolved_avro_type, dict) and not 'type' in resolved_avro_type:
                                    if isinstance(avro_type, dict):
                                        # the resolved type didn't have a type and avro_type is a dict, 
                                        # so we assume it's a mixin into the type we found
                                        avro_type.update(resolved_avro_type)
                                        resolved_avro_type = None
                                    else:
                                        # no 'type' definition for this field and we can't mix into the avro type, 
                                        # so we fallback to a generic type
                                        print(f"WARNING: no 'type' definition for {ref} in record {record_name}: {json.dumps(resolved_avro_type)}")
                                        resolved_avro_type = generic_type()
                                elif isinstance(avro_type,str) and resolved_avro_type:
                                    # this is a plain type reference
                                    avro_type = resolved_avro_type
                                    self.imported_types[ref] = avro_type
                                    resolved_avro_type = None                        
                                if resolved_avro_type:
                                    # this is not a record type that can stand on its own,
                                    # so we remove the cached type entry 
                                    # and pass it on as an inline type
                                    if ref in self.imported_types:
                                        del self.imported_types[ref]
                                    avro_type = self.merge_avro_schemas([avro_type, resolved_avro_type], avro_schema, local_name)
                            else:
                                avro_type = resolved_avro_type
                                self.imported_types[ref] = copy.deepcopy(avro_type)

                            if len(deps) > 0:
                                if isinstance(avro_type, dict):
                                    avro_type['dependencies'] = deps
                                else:
                                    dependencies.extend(deps)
                            
                            if isinstance(avro_type, dict) and 'name' in avro_type and 'type' in avro_type and (avro_type['type'] == 'record' or avro_type['type'] == 'enum'):
                                self.register_type(avro_schema, avro_type)
                                full_name = avro_type.get('namespace','')+'.'+avro_type['name'] if 'namespace' in avro_type else avro_type['name']
                                if ref in self.imported_types:
                                    # update the import reference to the resolved type if it's cached
                                    self.imported_types[ref] = full_name
                                dependencies.append(full_name)
                                avro_type = full_name                    
                        else:
                            del json_type['$ref']
                            # it's a reference within a definition, so we will turn this into an inline type
                            if isinstance(resolved_json_type, dict) and 'type' in resolved_json_type and json_type.get('type') and not json_type['type'] == resolved_json_type['type']:
                                # the types conflict, so we can't merge them
                                type1 = self.json_type_to_avro_type(json_type, record_name, field_name, namespace, dependencies, resolved_schema, new_base_uri, avro_schema, record_stack, recursion_depth + 1)
                                type2 = self.json_type_to_avro_type(resolved_json_type, record_name, field_name, namespace, dependencies, resolved_schema, new_base_uri, avro_schema, record_stack, recursion_depth + 1)
                                # if either of the types are empty, use just the other one
                                if not self.is_empty_type(type1) and not self.is_empty_type(type2):
                                    return self.flatten_union([type1, type2])
                                if not self.is_empty_type(type1):
                                    avro_type = type1
                                    if isinstance(avro_type, list):
                                        return avro_type
                                if not self.is_empty_type(type2):
                                    avro_type = type2   
                                    if isinstance(avro_type, list):
                                        return avro_type                             
                                json_type = {}
                            else:
                                json_type = self.merge_json_schemas([json_type, resolved_json_type])
                                avro_type = self.json_type_to_avro_type(json_type, record_name, field_name, namespace, dependencies, resolved_schema, new_base_uri, avro_schema, record_stack, recursion_depth + 1)
                                json_type = {}
                            if ref in self.imported_types:
                                # update the import reference to the resolved type if it's cached
                                if isinstance(avro_type, dict) and 'name' in avro_type:
                                    self.imported_types[ref] = avro_type['name']
                                else:
                                    self.imported_types[ref] = avro_type
                            
                # if 'const' is present, make this an enum
                if 'const' in json_type:
                    const = json_type['const']
                    const_enum = {
                        'type': 'enum', 
                        'symbols': [const], 
                        'name': avro_name(local_name), 
                        'namespace': namespace
                        }
                    avro_type = self.merge_avro_schemas([avro_type, const_enum], avro_schema, local_name)

                if json_object_type or 'enum' in json_type:
                    if json_object_type == 'array':
                        if isinstance(json_type, dict) and 'items' in json_type:
                            avro_type = self.merge_avro_schemas([avro_type, {'type': 'array', 'items': self.json_type_to_avro_type(json_type['items'], record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack, recursion_depth + 1)}], avro_schema, avro_type.get('name', local_name) if isinstance(avro_type,dict) else local_name)
                        else:
                            avro_type = self.merge_avro_schemas([avro_type, {'type': 'array', 'items': generic_type()}], avro_schema, avro_type.get('name', local_name) if isinstance(avro_type,dict) else local_name)
                    elif json_object_type and (json_object_type == 'object' or 'object' in json_object_type):
                        avro_record_type = self.json_schema_object_to_avro_record(local_name, json_type, namespace, json_schema, base_uri, avro_schema, record_stack)
                        if isinstance(avro_record_type, list):
                            for record_entry in avro_record_type:
                                if isinstance(record_entry, dict) and 'dependencies' in record_entry:
                                    dependencies.extend(record_entry['dependencies'])
                                    del record_entry['dependencies']
                        avro_type = self.merge_avro_schemas([avro_type, avro_record_type], avro_schema, avro_type.get('name', local_name) if isinstance(avro_type,dict) else local_name)
                        if isinstance(avro_type, dict) and 'dependencies' in avro_type:
                            dependencies.extend(avro_type['dependencies'])
                            del avro_type['dependencies']
                    elif 'enum' in json_type: 
                        enum = [avro_name(e) for e in json_type['enum'] if isinstance(e, str) and e != ''] 
                        enum = list(dict.fromkeys(enum))
                        if len(enum) > 0:
                            avro_type = {
                                'type': 'enum', 
                                'symbols': enum, 
                                'name': local_name, 
                                'namespace': namespace + '.' + record_name
                                }
                    else:
                        avro_type = self.json_schema_primitive_to_avro_type(json_object_type, json_type.get('format'), json_type.get('enum'), record_name, field_name, namespace, dependencies)    
            else:
                if isinstance(json_type, dict):
                    avro_type = self.merge_avro_schemas([avro_type,self.json_schema_primitive_to_avro_type(json_type, json_type.get('format'), json_type.get('enum'), record_name, field_name, namespace, dependencies)], avro_schema, avro_type.get('name', local_name) if isinstance(avro_type,dict) else local_name)
                else:
                    avro_type = self.merge_avro_schemas([avro_type,self.json_schema_primitive_to_avro_type(json_type, None, None, record_name, field_name, namespace, dependencies)], avro_schema, avro_type.get('name', local_name) if isinstance(avro_type,dict) else local_name)
            
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
            
            # post-check on the avro type: if the type is a dict, and the 'type' is not
            # a record, enum, fixed, array, or map, we will just return the basic type
            # and push its dependencies up the stack
            if isinstance(avro_type, dict) and 'type' in avro_type and not avro_type['type'] in ['array', 'map', 'record', 'enum', 'fixed']:
                if 'dependencies' in avro_type:
                    dependencies.extend(avro_type['dependencies'])
                avro_type = avro_type['type']
                        
            if isinstance(avro_type, dict) and 'unmerged_types' in avro_type:
                self.types_with_unmerged_types.append(avro_type)
                
            return avro_type
        except RecursionError as e:
            print(f"Recursion error while processing {namespace}:{record_name}:{field_name} with recursion depth {recursion_depth}")
            raise e

    def register_type(self, avro_schema, avro_type) -> bool:
        existing_type = next((t for t in avro_schema if t.get('name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace') ), None)
        if not existing_type:
            if self.is_empty_type(avro_type) and not 'unmerged_types' in avro_type:
                print(f'WARN: Standalone type {avro_type["name"]} is empty')
            if avro_type['type'] != 'enum' and avro_type['type'] != 'record' and avro_type['type'] != 'fixed':
                raise ValueError(f'WARN: Standalone type {avro_type["name"]} is not a record or enum or fixed type')
            avro_schema.append(avro_type)
            return True
        return False
        
    def json_schema_object_to_avro_record(self, name: str, json_object: dict, namespace: str, json_schema: dict, base_uri: str, avro_schema: list, record_stack: list) -> dict | list | None:
        """Convert a JSON schema object declaration to an Avro record."""
        dependencies: List[str] = []
        # handle top-level allOf, anyOf, oneOf
        if isinstance(json_object, dict) and ('allOf' in json_object or 'oneOf' in json_object or 'anyOf' in json_object):
            # we will merge allOf, oneOf, anyOf into a union record type
            type = self.json_type_to_avro_type(json_object, name, 'value', namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
            if isinstance(type, list) or isinstance(type, str):
                # we should have a union type
                type = {
                            'type': 'record',
                            'name': avro_name(name),
                            'namespace': self.utility_namespace,
                            'fields': [
                                {
                                    'name': 'value',
                                    'type': type
                                }
                            ]
                        }
            elif isinstance(type, dict) and 'type' in type and type['type'] != 'record':
                # merge the type into a record type if it's not a record type
                new_type = {
                            'type': 'record',
                            'name': avro_name(type.get('name',name)+'_wrapper'),
                            'namespace': self.utility_namespace,
                            'fields': [
                                {
                                    'name': 'value',
                                    'type': type['type']
                                }
                            ],
                            'dependencies': type.get('dependencies', [])
                        }
                type = new_type
            # add dependencies to the record    
            if dependencies and isinstance(type, dict):
                if 'dependencies' in type:
                    dependencies.extend(type['dependencies'])
                    del type['dependencies']
                type['dependencies'] = dependencies
            # return the union type
            return type    
        
        if isinstance(json_object, dict) and 'enum' in json_object:
            # this is an enum
            avro_enum = {
                'type': 'enum',
                'name': avro_name(name),
                'namespace': namespace,
                'symbols': [avro_name(e) for e in json_object['enum']]
            }
            if 'description' in json_object:
                avro_enum['doc'] = json_object['description']
            return avro_enum
        
        if isinstance(json_object, dict) and 'type' in json_object and json_object['type'] == 'array':
            # this is an array, which can't be standalone in Avro, so we will wraps it into a record
            # and include the type as an inline
            deps: List[str] =[]
            avro_array = {
                'type': 'record',
                'name': avro_name(name)+'_array',
                'namespace': self.utility_namespace,
                'fields': [
                    {
                        'name': 'items',
                        'type': self.json_type_to_avro_type(json_object, name, avro_name(name), namespace, deps, json_schema, base_uri, avro_schema, record_stack)
                    }
                ]
            }
            if 'description' in json_object:
                avro_array['doc'] = json_object['description']
            if len(deps) > 0:
                avro_array['dependencies'] = deps
            return avro_array       
        
        
        # at this point, we have to assume that we have a JSON schema object
        title = json_object.get('title')
        record_name = avro_name(name if name else title if title else None)
        if record_name == None:
            raise ValueError(f"Cannot determine record name for json_object {json_object}")
        if len(record_stack) > 0:
            # if we have a record stack, we need to add the current name to
            # the namespace since nested types are disambiguated by their namespace
            namespace = namespace + '.' + record_stack[-1]
        # at this point we have a record type
        avro_record = {
            'type': 'record', 
            'name': avro_name(record_name),
            'namespace': namespace,
            'fields': []
        }
        # we need to prevent circular dependencies, so we will maintain a stack of the in-progress 
        # records and will resolve the cycle as we go. if this record is already in the stack, we will
        # just return a reference to a record that contains this record
        if record_name in record_stack:
            # to break the cycle, we will use a containment type that references 
            # the record that is being defined
            ref_name = avro_name(record_name + '_ref')
            return {
                    'type': 'record',
                    'name': avro_name(ref_name),
                    'namespace': namespace,
                    'fields': [
                        {
                            'name': record_name,
                            'type': record_name
                        }
                    ]
                }
        try:
            # enter the record stack scope for this record
            record_stack.append(record_name)
            # collect the required fields so we can make those fields non-null
            required_fields = json_object.get('required', [])

            field_refs = []
            if 'properties' in json_object and isinstance(json_object['properties'], dict):
                # add the properties as fields
                for field_name, field in json_object['properties'].items():
                    # skip fields with an bad or empty type
                    if not isinstance(field, dict):
                        continue
                    field_name = avro_name(field_name)
                    # convert the JSON-type field to an Avro-type field
                    avro_field_ref_type = avro_field_type = self.ensure_type(self.json_type_to_avro_type(field, record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack))
                    if isinstance(avro_field_type, dict):
                        if 'dependencies' in avro_field_type:
                            # move field type dependencies to the record
                            dependencies.extend(avro_field_type['dependencies'])
                            del avro_field_type['dependencies']
                        # if the first call gave us a global type that got added to the schema, this call will give us a reference
                        if 'type' in avro_field_type and (avro_field_type['type'] == 'record' or avro_field_type['type'] == 'enum'):
                            avro_field_ref_type = avro_field_type['namespace']+'.'+avro_field_type['name']
                        
                    if avro_field_type is None:
                        # None type is a problem
                        raise ValueError(f"avro_field_type is None for field {field_name}")
                    
                    if isinstance(avro_field_type,dict) and 'type' in avro_field_type and not avro_field_type['type'] in ['array', 'map', 'record', 'enum', 'fixed']:
                        # if the field type is a basic type, inline it
                        avro_field_type = avro_field_type['type']               
                                    
                    if not field_name in required_fields and not 'null' in avro_field_type:
                        # make the field nullable, if it's required and if it's not already nullable
                        if isinstance(avro_field_type, list):
                            avro_field_type.append('null')
                            avro_field = {'name': avro_name(field_name), 'type': avro_field_type}
                        else:
                            avro_field = {'name': avro_name(field_name), 'type': ['null', avro_field_type]}
                    else:
                        # make the field non-null
                        avro_field = {'name': avro_name(field_name), 'type': avro_field_type}

                    if not field_name in required_fields and not 'null' in avro_field_ref_type:
                        # make the field nullable, if it's required and if it's not already nullable
                        if isinstance(avro_field_ref_type, list):
                            avro_field_ref_type.append('null')
                            avro_field_ref = {'name': avro_name(field_name), 'type': avro_field_ref_type}
                        else:
                            avro_field_ref = {'name': avro_name(field_name), 'type': ['null', avro_field_ref_type]}
                    else:
                        # make the field non-null
                        avro_field_ref = {'name': avro_name(field_name), 'type': avro_field_ref_type}
                    
                    if field.get('description'):
                        # add the field's description
                        avro_field['doc'] = field['description']
                        avro_field_ref['doc'] = field['description']

                    # add the field to the record
                    avro_record['fields'].append(avro_field)
                    field_refs.append(avro_field_ref)
            elif not 'additionalProperties' in json_object and not 'patternProperties' in json_object:
                if 'type' in json_object and (json_object['type'] == 'object' or 'object' in json_object['type']) and \
                    not 'allOf' in json_object and not 'oneOf' in json_object and not 'anyOf' in json_object:
                    # we don't have any fields, but we have an object type, so we create a map
                    avro_record = {
                        'type': 'map',
                        'name': avro_record['name'],
                        'values': generic_type(),
                        'doc': avro_record.get('doc','')
                    }        
                elif 'type' in json_object and (json_object['type'] == 'array' or 'array' in json_object['type']) and \
                    not 'allOf' in json_object and not 'oneOf' in json_object and not 'anyOf' in json_object:
                    # we don't have any fields, but we have an array type, so we create a record with an 'items' field
                    if 'items' in json_object:
                        avro_type = self.json_type_to_avro_type(json_object['items'], record_name, 'values', namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
                    else:
                        avro_type = generic_type()
                    avro_record = {
                        'type': 'array',
                        'name': avro_record['name'],
                        'items': avro_type,
                        'doc': avro_record.get('doc','')
                    }
                else:
                    if 'type' in json_object:
                        return json_object['type']
                    else:
                        return generic_type()

            extension_types = []            
            prop_docs = ''
            if 'patternProperties' in json_object and isinstance(json_object['patternProperties'], dict):
                # pattern properties are represented as a record with field names that are the patterns
                pattern_props = json_object['patternProperties']
                for pattern_name, props in pattern_props.items():
                    prop_type = self.ensure_type(self.json_type_to_avro_type(props, record_name, pattern_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack))
                    if self.is_empty_type(prop_type):
                        prop_type = generic_type()
                    prop_docs += f"Name pattern '{pattern_name}': [{self.get_field_type_name({'type':prop_type})}]. "
                    extension_types.append(prop_type)            
            
            if 'additionalProperties' in json_object and isinstance(json_object['additionalProperties'], dict):
                # additional properties are represented as a map of string to the type of the value
                additional_props = json_object['additionalProperties']
                values_type = self.json_type_to_avro_type(additional_props, record_name, record_name + '_extensions', namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
                if self.is_empty_type(values_type):
                    values_type = generic_type()
                prop_docs += f"Extra properties: [{self.get_field_type_name({'type':values_type})}]. "	
                extension_types.append(values_type)

            avro_alternate_record = None
            if extension_types:
                # Since Avro Schema does not allow fields with dynamic names
                # to appear alongside regular fields, we will union the types of all properties with the 
                # type of the additionalProperties and document this in the record's description
                field_types = [field['type'] for field in field_refs]
                field_type_names = [[field['name'], self.get_field_type_name(field)] for field in field_refs]
                field_type_name_list: str = ', '.join([f"'{field[0]}': [{field[1]}]" for field in field_type_names])
                field_types.extend(extension_types)
                field_types = self.flatten_union(field_types)
                if len(field_types) == 1:
                    field_types = field_types[0]
                doc = f"Mixed dynamic: {field_type_name_list}. " if field_type_names else ''
                doc += prop_docs
                avro_alternate_record = {
                    'type': 'map',
                    'name': avro_record['name'] + '_map',
                    'values': field_types,
                    'doc': doc,
                    'dependencies': [namespace + '.' + record_name]
                }
            
            if 'description' in json_object:
                avro_record['doc'] = json_object['description']
            if len(dependencies) > 0:
                # dedupe the list
                dependencies = list(set(dependencies))    
                avro_record['dependencies'] = dependencies        
        finally:
            record_stack.pop()
        if avro_alternate_record:
            if self.is_empty_type(avro_record):
                # there's no substantive content in the record, so we will return the alternate record
                return avro_alternate_record
            return [avro_record, avro_alternate_record]
        return avro_record
        
    
    def find_schema_node(self, test, avro_schema, recursion_stack = []):
        """Find the first schema node in the avro_schema matching the test"""
        for recursion_item in recursion_stack:
            if avro_schema is recursion_item:
                raise ValueError('Cyclical reference detected in schema')
            if len(recursion_stack) > self.max_recursion_depth:
                raise ValueError('Maximum recursion depth exceeded in schema')
        try:
            recursion_stack.append(avro_schema)
            if isinstance(avro_schema, dict):
                test_node = test(avro_schema)
                if test_node:
                    return avro_schema
                for k, v in avro_schema.items():
                    if isinstance(v, (dict,list)):
                        node = self.find_schema_node(test, v, recursion_stack)
                        if node:
                            return node
            elif isinstance(avro_schema, list):
                for item in avro_schema:
                    node = self.find_schema_node(test, item, recursion_stack)
                    if node:
                        return node
            return None
        finally:
            recursion_stack.pop()
    
    def set_schema_node(self, test, replacement, avro_schema):
        """Set the first schema node in the avro_schema matching the test to the replacement"""
        if isinstance(avro_schema, dict):
            test_node = test(avro_schema)
            if test_node:
                avro_schema.clear()
                avro_schema.update(replacement)
                return
            for k, v in avro_schema.items():
                if isinstance(v, (dict,list)):
                    self.set_schema_node(test, replacement, v)
        elif isinstance(avro_schema, list):
            for item in avro_schema:
                self.set_schema_node(test, replacement, item)
                

    def postprocess_schema(self, avro_schema: list) -> None:
        """ Post-process the Avro Schema for cases wheer we need a second pass """
        result: dict

        if len(self.types_with_unmerged_types)>0:
            types_with_unmerged_types = self.types_with_unmerged_types
            self.types_with_unmerged_types = []
            for ref_type in types_with_unmerged_types:
                # find ref_type anywhere in the avro_schema graph, matching
                # on name and namespace. 
                find_fn = lambda t: 'name' in t and t['name'] == ref_type['name'] and 'namespace' in t and t['namespace'] == ref_type['namespace']
                type = self.find_schema_node(find_fn, avro_schema)
                if not type:
                    continue
                # resolve the unmerged types
                local_name = type.get('name')
                if not isinstance(type, dict):
                    continue
                unmerged_types = type.get('unmerged_types', [])
                if len(unmerged_types) == 0:
                    if 'unmerged_types' in type:
                        del type['unmerged_types']
                    continue
                base_type = copy.deepcopy(type)
                if 'unmerged_types' in base_type:
                    del base_type['unmerged_types']
                mergeable_types = [base_type]
                deps: List[str] = []
                if 'dependencies' in type:
                    deps.extend(type['dependencies'])
                    del type['dependencies']
                for item in unmerged_types:
                    if isinstance(item,str):
                        found_avro_type = next((t for t in avro_schema if t.get('namespace')+'.'+t.get('name') == item ), None)
                        if not found_avro_type:
                            continue
                    elif isinstance(item,dict):
                        found_avro_type = item
                        if 'dependencies' in found_avro_type:
                            deps.extend(found_avro_type['dependencies'])
                            del found_avro_type['dependencies']
                    if isinstance(found_avro_type,dict):
                        candidate = found_avro_type
                        if 'unmerged_types' in candidate:
                            del candidate['unmerged_types']
                        mergeable_types.append(candidate)
                merge_result = self.merge_avro_schemas(mergeable_types, avro_schema, local_name, deps)
                if isinstance(merge_result,dict):
                    merge_result['dependencies'] = deps
                    if 'unmerged_types' in merge_result:
                        del merge_result['unmerged_types']
                if isinstance(merge_result, list):
                    # unmerged field containers have fields.
                    type['name'] = type['name'] + '_item'
                    type['fields'] = [{'name': 'value', 'type': merge_result }]
                    merge_result = type
                self.set_schema_node(find_fn, merge_result, avro_schema)
    
    def process_definition_list(self, json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema_list):
        """Process a schema definition list."""
        for sub_schema_name, schema in json_schema_list.items():
            if not isinstance(schema, dict) and not isinstance(schema, list):
                # skip items that are not schema definitions or lists
                continue
            if 'type' in schema or 'allOf' in schema or 'oneOf' in schema or 'anyOf' in schema or 'properties' in schema or 'enum' in schema or '$ref' in schema or 'additionalProperties' in schema or 'patternProperties' in schema:
                # this is a schema definition
                self.process_definition(json_schema, namespace, base_uri, avro_schema, record_stack, sub_schema_name, schema)
                continue
            # it's a schema definition list
            self.process_definition_list(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema)

    def process_definition(self, json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema):
        """ Process a schema definition. """
        avro_schema_item_list = self.json_schema_object_to_avro_record(schema_name, schema, namespace, json_schema, base_uri, avro_schema, record_stack)
        if not isinstance(avro_schema_item_list, list) and not isinstance(avro_schema_item_list, dict):
            # skip if the record couldn't be resolved
            return
        # the call above usually returns a single record, but we pretend it's normally a list to handle allOf/anyOf/oneOf cases
        if not isinstance(avro_schema_item_list, list):
            # is not a list, so we'll wrap it in a list
            avro_schema_item_list = [avro_schema_item_list]
        for avro_schema_item in avro_schema_item_list:
            # add the item to the schema if it's not already there
            if isinstance(avro_schema_item, str):
                continue
            if isinstance(avro_schema_item,dict) and not 'name' in avro_schema_item:
                avro_schema_item['name'] = avro_name(schema_name)
            existing_type = next((t for t in avro_schema if t.get('name') == avro_schema_item['name'] and t.get('namespace') == avro_schema_item.get('namespace') ), None)
            if not existing_type:
                if (not self.is_empty_type(avro_schema_item) or 'unmerged_types' in avro_schema_item) and \
                    (avro_schema_item.get('type') == 'record' or avro_schema_item.get('type') == 'enum' or avro_schema_item.get('type') == 'fixed'):
                    # we only register record/enum as type. the other defs are mix-ins
                    self.register_type(avro_schema, avro_schema_item)              
        

    def id_to_avro_namespace(self, id: str) -> str:
        """Convert a XSD namespace to Avro Namespace."""
        parsed_url = urlparse(id)
        # strip the file extension 
        path = parsed_url.path.rsplit('.')[0]
        path_segments = path.strip('/').replace('-', '_').split('/')
        reversed_path_segments = reversed(path_segments)
        namespace_suffix = '.'.join(reversed_path_segments)
        if parsed_url.hostname:
            namespace_prefix = '.'.join(reversed(parsed_url.hostname.split('.')))
        namespace = f"{namespace_prefix}.{namespace_suffix}"
        return namespace
    

    def jsons_to_avro(self, json_schema: dict | list, namespace: str, base_uri: str) -> list | dict:
        """Convert a JSON-schema to an Avro-schema."""
        avro_schema: List[dict] = []
        record_stack: List[str] = []

        parsed_url = urlparse(base_uri)
        schema_name = 'document'

        if isinstance(json_schema, dict) and ('definitions' in json_schema or '$defs' in json_schema):
            # this is a swagger file or has a 'definitions' block
            json_schema_defs = json_schema.get('definitions', json_schema.get('$defs', []))
            for def_schema_name, schema in json_schema_defs.items():
                if 'type' in schema or 'allOf' in schema or 'oneOf' in schema or 'anyOf' in schema or 'properties' in schema or 'enum' in schema or '$ref' in schema or 'additionalProperties' in schema or 'patternProperties' in schema:
                    # this is a schema definition
                    self.process_definition(json_schema, namespace, base_uri, avro_schema, record_stack, def_schema_name, schema)
                else:
                    # it's a schema definition list
                    self.process_definition_list(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema.copy())
        elif isinstance(json_schema, list):
            # this is a schema definition list
            self.process_definition_list(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema)

        if isinstance(json_schema, dict) and 'type' in json_schema or 'allOf' in json_schema or 'oneOf' in json_schema or 'anyOf' in json_schema or 'properties' in json_schema:
            # this is a schema definition
            if isinstance(json_schema, dict) and '$ref' in json_schema:
                #if there is a $ref at the root level, resolve the reference and merge it with the current schema
                ref = json_schema['$ref']
                if ref:
                    ref_schema, json_doc = self.resolve_reference(json_schema, base_uri, json_schema)
                    json_schema = self.merge_json_schemas([json_schema, ref_schema], intersect=False)
            self.process_definition(json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema)
        
        # postprocessing pass
        self.postprocess_schema(avro_schema)
        # sort the records by their dependencies
        avro_schema = sort_messages_by_dependencies(avro_schema)       
        
        if parsed_url.fragment and isinstance(json_schema, dict):
            # if the fragment is present in the URL, it's a reference to a schema definition
            # so we will resolve that reference and return a type
            self.imported_types.clear()
            fragment_schema: List[dict] = []
            json_pointer = parsed_url.fragment
            schema_name = parsed_url.fragment.split('/')[-1]
            schema = jsonpointer.resolve_pointer(json_schema, json_pointer)
            avro_schema_item = self.json_schema_object_to_avro_record(schema_name, schema, namespace, json_schema, base_uri, fragment_schema, record_stack)
            if avro_schema_item:
                # we roll all the types into this record as the top level type
                inline_dependencies_of(avro_schema, avro_schema_item)
                return avro_schema_item
        
        return avro_schema


    def convert_jsons_to_avro(self, json_schema_file_path: str, avro_schema_path: str, namespace: str | None = None, utility_namespace: str | None = None) -> list | dict:
        """Convert JSON schema file to Avro schema file."""
        # turn the file path into a file URI if it's not a URI already
        parsed_url = urlparse(json_schema_file_path)
        if not parsed_url.hostname and not parsed_url.scheme == 'file':
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
        if utility_namespace:
            self.utility_namespace = utility_namespace
        else:
            self.utility_namespace = self.root_namespace + '.utility'
        
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
    

def convert_jsons_to_avro(json_schema_file_path: str, avro_schema_path: str, namespace: str = '', utility_namespace = '', maximize_compatibility = False) -> list | dict:
    """Convert JSON schema file to Avro schema file."""
    try:
        converter = JsonToAvroConverter()
        converter.maximize_compatibility = maximize_compatibility
        return converter.convert_jsons_to_avro(json_schema_file_path, avro_schema_path, namespace, utility_namespace)
    except Exception as e:
        print(f'Error converting JSON {json_schema_file_path} to Avro: {e.args[0]}')
        return []