""" JSON to Avro schema converter. """

# pylint: disable=too-many-lines, line-too-long, too-many-branches, too-many-statements, too-many-locals, too-many-nested-blocks, too-many-arguments, too-many-instance-attributes, too-many-public-methods, too-many-boolean-expressions

import json
import os
import copy
import urllib
from urllib.parse import ParseResult, urlparse, unquote
from typing import Any, Dict, List, Tuple
import jsonpointer
from jsonpointer import JsonPointerException
import requests

from avrotize.common import avro_name, avro_namespace, find_schema_node, generic_type, set_schema_node
from avrotize.dependency_resolver import inline_dependencies_of, sort_messages_by_dependencies

primitive_types = ['null', 'string', 'int',
                   'long', 'float', 'double', 'boolean', 'bytes']


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
        self.content_cache: Dict[str, str] = {}
        self.utility_namespace = 'utility.vasters.com'
        self.split_top_level_records = False
        self.root_class_name = 'document'

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
        # consolidate array type instances
        array_type = None
        map_type = None
        flat_list_1 = []
        for t in flat_list:
            if isinstance(t, dict) and 'type' in t and t['type'] == 'array' and 'items' in t:
                if not array_type:
                    array_type = t
                    flat_list_1.append(t)
                else:
                    array_type = self.merge_avro_schemas([array_type, t], [])
            elif isinstance(t, dict) and 'type' in t and t['type'] == 'map' and 'values' in t:
                if not map_type:
                    map_type = t
                    flat_list_1.append(t)
                else:
                    map_type = self.merge_avro_schemas([map_type, t], [])
            elif not t in flat_list_1:
                flat_list_1.append(t)
        return flat_list_1

    # pylint: disable=dangerous-default-value
    def merge_avro_schemas(self, schemas: list, avro_schemas: list, type_name: str | None = None, deps: List[str] = []) -> str | list | dict:
        """Merge multiple Avro type schemas into one."""

        def split_merge(schema1, schema2, schema_list, offset):
            """ return the continuing schema merges of incompatible schemas """
            remaining_schemas = schema_list[offset +
                                            1:] if len(schema_list) > offset else []
            if isinstance(schema2, dict) and 'dependencies' in schema2:
                deps.extend(schema2['dependencies'])
                del schema2['dependencies']
            if isinstance(schema1, dict) and 'dependencies' in schema1:
                deps.extend(schema1['dependencies'])
                del schema1['dependencies']
            schema1_merged = self.merge_avro_schemas(
                [schema2] + remaining_schemas, avro_schemas, type_name, deps)
            schema2_merged = self.merge_avro_schemas(
                [schema1] + remaining_schemas, avro_schemas, type_name, deps)
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
            self.set_avro_type_value(merged_schema, 'name', type_name)
        for i, schema in enumerate(schemas):
            schema = copy.deepcopy(schema)
            if isinstance(schema, dict) and 'dependencies' in schema:
                deps1: List[str] = merged_schema.get('dependencies', [])
                deps1.extend(schema['dependencies'])
                merged_schema['dependencies'] = deps1
            if (isinstance(schema, list) or isinstance(schema, dict)) and len(schema) == 0:
                continue
            if isinstance(schema, str):
                sch = next(
                    (s for s in avro_schemas if s.get('name') == schema), None)
                if sch:
                    merged_schema.update(sch)
                else:
                    merged_schema['type'] = schema
            elif isinstance(schema, list):
                # the incoming schema is a list, so it's a union
                if 'type' not in merged_schema:
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
            elif schema and ('type' not in schema or 'type' not in merged_schema):
                merged_schema.update(schema)
            elif schema:
                if 'type' in merged_schema and schema['type'] != merged_schema['type']:
                    return split_merge(merged_schema, schema, schemas, i)
                if not type_name:
                    self.set_avro_type_value(merged_schema, 'name', avro_name(
                        merged_schema.get('name', '') + schema.get('name', '')))
                if 'fields' in schema:
                    if 'fields' in merged_schema:
                        for field in schema['fields']:
                            if field not in merged_schema['fields']:
                                merged_schema['fields'].append(field)
                            else:
                                merged_schema_field = next(
                                    f for f in merged_schema['fields'] if f.get('name') == field.get('name'))
                                if merged_schema_field['type'] != field['type']:
                                    merged_schema_field['type'] = [
                                        field['type'], merged_schema_field['type']]
                                if 'doc' in field and 'doc' not in merged_schema_field:
                                    merged_schema_field['doc'] = field['doc']
                    else:
                        merged_schema['fields'] = schema['fields']
        if self.is_avro_complex_type(merged_schema) and 'namespace' in merged_schema:
            if merged_schema['type'] in ['array', 'map']:
                del merged_schema['namespace']
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

        def merge_structures(schema1: dict, schema2: dict) -> dict | list:
            """ merge two JSON dicts recursively """
            if 'type' in schema1 and 'type' in schema2 and schema1['type'] != schema2['type']:
                return [schema1, schema2]
            schema1 = copy.deepcopy(schema1)
            for key in schema2:
                if key not in schema1:
                    schema1[key] = schema2[key]
                elif isinstance(schema1[key], dict) and isinstance(schema2[key], dict):
                    schema1[key] = merge_structures(schema1[key], schema2[key])
                elif isinstance(schema1[key], list) and isinstance(schema2[key], list):
                    schema1[key].extend(schema2[key])
                elif schema1[key] == schema2[key]:
                    continue
                else:
                    if isinstance(schema1[key], list):
                        if schema2[key] not in schema1[key]:
                            schema1[key].append(schema2[key])
                    else:
                        schema1[key] = [schema1[key], schema2[key]]
            return schema1

        merged_type: dict = {}

        for json_schema in json_schemas:
            if 'type' not in json_schema or 'type' not in merged_type:
                for key in json_schema:
                    if not key in merged_type:
                        merged_type[key] = copy.deepcopy(json_schema[key])
                    else:
                        if key == 'required':
                            merged_type[key] = list(
                                set(merged_type[key]).union(set(json_schema[key])))
                        if key == 'name' or key == 'title' or key == 'description':
                            merged_type[key] = merged_type[key] + \
                                json_schema[key]
                        elif isinstance(merged_type[key], dict):
                            merged_type[key] = merge_structures(
                                merged_type[key], copy.deepcopy(json_schema[key]))
                        elif isinstance(merged_type[key], list) and isinstance(json_schema[key], list):
                            for item in json_schema[key]:
                                if item not in merged_type[key]:
                                    merged_type[key].append(item)
                        else:
                            if merged_type[key] is None:
                                merged_type[key] = json_schema[key]
                            else:
                                merged_type[key] = [merged_type[key],
                                                    copy.deepcopy(json_schema[key])]
            else:
                if 'type' in merged_type and json_schema['type'] != merged_type['type']:
                    if isinstance(merged_type['type'], str):
                        merged_type['type'] = [merged_type['type']]
                    merged_type['type'].append(json_schema['type'])
                if 'required' in json_schema:
                    if 'required' in merged_type:
                        merged_type['required'] = list(
                            set(merged_type['required']).union(set(json_schema['required'])))
                    else:
                        merged_type['required'] = json_schema['required']
                if 'name' in json_schema:
                    if 'name' in merged_type:
                        merged_type['name'] = merged_type.get(
                            'name', '') + json_schema['name']
                    else:
                        merged_type['name'] = json_schema['name']
                if 'properties' in json_schema:
                    if 'properties' in merged_type:
                        for prop in json_schema['properties']:
                            if prop in merged_type['properties']:
                                merged_type['properties'][prop] = merge_structures(
                                    merged_type['properties'][prop], copy.deepcopy(json_schema['properties'][prop]))
                            else:
                                merged_type['properties'][prop] = json_schema['properties'][prop]
                    else:
                        merged_type['properties'] = json_schema['properties']
                if 'enum' in json_schema:
                    if 'enum' in merged_type:
                        merged_type['enum'] = list(
                            set(merged_type['enum']).union(set(json_schema['enum'])))
                    else:
                        merged_type['enum'] = json_schema['enum']
                if 'format' in json_schema:
                    if 'format' in merged_type:
                        merged_type['format'] = merged_type['format'] + \
                            json_schema['format']
                    else:
                        merged_type['format'] = json_schema['format']

        if intersect:
            # only keep the intersection of the required fields
            if 'required' in merged_type:
                new_required = merged_type['required']
                for json_schema in json_schemas:
                    new_required = list(set(new_required).intersection(
                        set(json_schema.get('required', []))))
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

    def json_schema_primitive_to_avro_type(self, json_primitive: str | list, format: str | None, enum: list | None, record_name: str, field_name: str, namespace: str, dependencies: list) -> str | dict[str, Any] | list:
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
                    enum2 = item.get('enum') if isinstance(
                        item, dict) else None
                    format2 = item.get('format') if isinstance(
                        item, dict) else None
                    avro_primitive = self.json_schema_primitive_to_avro_type(
                        item, format2, enum2, record_name, field_name, self.compose_namespace(namespace, record_name, field_name), dependencies)
                    union.append(avro_primitive)
                return union

        if json_primitive == 'string':
            avro_primitive = 'string'
        elif json_primitive == 'integer':
            avro_primitive = 'int'
            if format == 'int64':
                avro_primitive = 'long'
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
                avro_primitive = {'type': 'fixed',
                                  'size': 12, 'logicalType': 'duration'}
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
            response = requests.get(url if isinstance(
                url, str) else parsed_url.geturl(), timeout=30)
            # Raises an HTTPError if the response status code is 4XX/5XX
            response.raise_for_status()
            self.content_cache[parsed_url.geturl()] = response.text
            return response.text

        # Handle file URLs
        elif scheme == 'file':
            # Remove the leading 'file://' from the path for compatibility
            file_path = parsed_url.netloc
            if not file_path:
                file_path = parsed_url.path
            # On Windows, a file URL might start with a '/' but it's not part of the actual path
            if os.name == 'nt' and file_path.startswith('/'):
                file_path = file_path[1:]
            with open(file_path, 'r', encoding='utf-8') as file:
                text = file.read()
                self.content_cache[parsed_url.geturl()] = text
                return text
        else:
            raise NotImplementedError(f'Unsupported URL scheme: {scheme}')

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
                        json_schema = jsonpointer.resolve_pointer(
                            json_schema, url.fragment)
                    return json_schema, json_schema_doc
                except json.JSONDecodeError:
                    raise Exception(f'Error decoding JSON from {ref}')

            if url.fragment:
                json_pointer = unquote(url.fragment)
                ref_schema = jsonpointer.resolve_pointer(
                    json_doc, json_pointer)
                if ref_schema:
                    return ref_schema, json_doc
        except JsonPointerException as e:
            raise Exception(
                f'Error resolving JSON Pointer reference for {base_uri}')
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
            dir = os.path.dirname(
                parsed_file_uri.netloc if parsed_file_uri.netloc else parsed_file_uri.path)
            filename = os.path.join(dir, url.path)
            file_uri = f'file://{filename}'
        else:
            # combine the base URI with the URL
            file_uri = urllib.parse.urljoin(base_uri, url.geturl())
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

    def json_type_to_avro_type(self, json_type: str | dict, record_name: str, field_name: str, namespace: str, dependencies: list, json_schema: dict, base_uri: str, avro_schema: list, record_stack: list, recursion_depth=1) -> dict | list | str:
        """Convert a JSON type to Avro type."""

        try:
            if recursion_depth >= self.max_recursion_depth:
                print(
                    f'WARNING: Maximum recursion depth reached for {record_name} at field {field_name}')
                return generic_type()

            avro_type: list | dict | str = {}
            local_name = avro_name(field_name if field_name else record_name)
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
                    print(
                        'WARNING: Conditional schema is not supported and will be ignored.')
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
                        while isinstance(allof_option, dict) and '$ref' in allof_option:
                            resolved_json_type, resolved_schema = self.resolve_reference(
                                allof_option, base_uri, json_schema)
                            del allof_option['$ref']
                            allof_option = self.merge_json_schemas(
                                [allof_option, resolved_json_type])
                        type_list.append(copy.deepcopy(allof_option))
                    merged_type = self.merge_json_schemas(
                        type_list, intersect=False)
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
                                json_types.append(self.merge_json_schemas(
                                    [type_to_process, oneof_option], intersect=True))
                    else:
                        new_json_types = []
                        for oneof_option in oneof:
                            for json_type_option in json_types:
                                json_type_option = self.merge_json_schemas(
                                    [json_type_option, oneof_option], intersect=True)
                                new_json_types.append(json_type_option)
                        json_types = new_json_types

                if 'anyOf' in json_type:
                    types_to_process = json_types.copy() if len(json_types) > 0 else [
                        copy.deepcopy(base_type)]
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
                        merged_type = self.merge_json_schemas(
                            type_list, intersect=False)
                        json_types.append(merged_type)

                if len(json_types) > 0:
                    if len(json_types) == 1:
                        avro_type = self.json_type_to_avro_type(
                            json_types[0], record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack, recursion_depth + 1)
                        if isinstance(avro_type, dict) and self.is_empty_type(avro_type) and not 'allOf' in json_type:
                            avro_type['type'] = generic_type()
                        avro_type = self.post_check_avro_type(
                            dependencies, avro_type)
                        return avro_type
                    else:
                        try:
                            record_stack.append(
                                field_name if field_name else record_name)
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
                                sub_field_name = avro_name(local_name + '_' + str(count)) if not isinstance(
                                    json_type_option, dict) or not '$ref' in json_type_option else None
                                avro_subtype = self.json_type_to_avro_type(
                                    json_type_option, record_name, sub_field_name, namespace, subtype_deps, json_schema, base_uri, avro_schema, record_stack, recursion_depth + 1)
                                if not avro_subtype:
                                    continue
                                if isinstance(avro_subtype, dict) and 'name' in avro_subtype and 'type' in avro_subtype and (avro_subtype['type'] == 'record' or avro_subtype['type'] == 'enum'):
                                    # we have a standalone record or enum so we need to add it to the schema at the top-level
                                    # and reference it as a dependency from the parent type if it's not already been added.
                                    existing_type = next((t for t in avro_schema if t.get('name') == avro_subtype['name'] and t.get(
                                        'namespace') == avro_subtype.get('namespace')), None)
                                    if not existing_type:
                                        if subtype_deps:
                                            if not 'dependencies' in avro_subtype:
                                                avro_subtype['dependencies'] = subtype_deps
                                            else:
                                                avro_subtype['dependencies'].extend(
                                                    subtype_deps)
                                        if self.is_empty_type(avro_subtype):
                                            print(
                                                f'WARN: Standalone type {avro_subtype["name"]} is empty')
                                        if avro_subtype['type'] != 'enum' and avro_subtype['type'] != 'record' and avro_subtype['type'] != 'fixed':
                                            raise ValueError(
                                                f'WARN: Standalone type {avro_subtype["name"]} is not a record or enum or fixed type')
                                        avro_schema.append(avro_subtype)
                                    full_name = self.get_qualified_name(
                                        avro_subtype)
                                    subtype_deps = [full_name]
                                    avro_subtype = full_name
                                if isinstance(avro_subtype, dict) and 'dependencies' in avro_subtype:
                                    subtype_deps.extend(
                                        avro_subtype['dependencies'])
                                    del avro_subtype['dependencies']
                                if len(subtype_deps) > 0:
                                    type_deps.extend(subtype_deps)
                                if not self.is_empty_type(avro_subtype):
                                    if isinstance(avro_subtype, list):
                                        subtypes.extend(
                                            copy.deepcopy(avro_subtype))
                                    else:
                                        subtypes.append(
                                            copy.deepcopy(avro_subtype))
                                count += 1
                            if len(type_deps) > 0:
                                dependencies.extend(type_deps)
                            if len(subtypes) == 1:
                                return self.post_check_avro_type(dependencies, subtypes[0])
                        finally:
                            record_stack.pop()

                        if hasAnyOf:
                            # if all subtypes are strings, they are either primitive types or type references
                            # which means there's nothing to merge, so we'll return the list of types
                            if all([isinstance(st, str) for st in subtypes]):
                                return self.post_check_avro_type(dependencies, subtypes)

                            # we now has a list of types that may match the data, but this would be
                            # an Avro union which is mutually exclusive. We will merge this list
                            # into a record type in postprocessing when all types are available
                            if not isinstance(avro_type, dict):
                                avro_type = {}
                            avro_type['unmerged_types'] = subtypes
                            avro_type['type'] = 'record'
                            avro_type['name'] = avro_name(local_name)
                            if local_name != avro_name(local_name):
                                avro_type['altnames'] = { 'json': local_name }
                            avro_type['namespace'] = namespace
                            avro_type['fields'] = []
                            if 'description' in json_type:
                                avro_type['doc'] = json_type['description']
                            json_type = {}
                        else:
                            return self.post_check_avro_type(dependencies, subtypes)

                if 'properties' in json_type and not 'type' in json_type:
                    json_type['type'] = 'object'

                if 'description' in json_type and isinstance(avro_type, dict):
                    avro_type['doc'] = json_type['description']

                if 'title' in json_type and isinstance(avro_type, dict):
                    self.set_avro_type_value(
                        avro_type, 'name', avro_name(json_type['title']))

                # first, pull in any referenced definitions and merge with this schema
                if '$ref' in json_type:
                    # the $ref can indeed be a list as a result from a prior allOf/anyOf merge
                    # if that is so, we will copy the type and process each $ref separately
                    # and return the result as a list of types
                    if isinstance(json_type['$ref'], list):
                        types = []
                        for ref in json_type['$ref']:
                            json_type_copy = copy.deepcopy(json_type)
                            json_type_copy['$ref'] = ref
                            types.append(self.json_type_to_avro_type(json_type_copy, record_name, field_name, namespace,
                                         dependencies, json_schema, base_uri, avro_schema, record_stack, recursion_depth + 1))
                        return self.post_check_avro_type(dependencies, types)

                    ref = json_type['$ref']
                    if ref in self.imported_types:
                        # reference was already resolved, so we can resolve the reference simply by returning the type
                        type_ref = copy.deepcopy(self.imported_types[ref])
                        if isinstance(type_ref, str):
                            dependencies.append(type_ref)
                        return self.post_check_avro_type(dependencies, type_ref)
                    else:
                        new_base_uri = self.compose_uri(
                            base_uri, json_type['$ref'])
                        resolved_json_type, resolved_schema = self.resolve_reference(
                            json_type, base_uri, json_schema)
                        if self.is_empty_json_type(json_type):
                            # it's a standalone reference, so will import the type into the schema
                            # and reference it like it was in the same file
                            type_name = record_name
                            type_namespace = namespace
                            parsed_ref = urlparse(ref)
                            if parsed_ref.fragment:
                                type_name = avro_name(
                                    parsed_ref.fragment.split('/')[-1])
                                sub_namespace = self.compose_namespace(
                                    *parsed_ref.fragment.split('/')[2:-1])
                                type_namespace = self.compose_namespace(
                                    self.root_namespace, sub_namespace)

                            # registering in imported_types ahead of resolving to prevent circular references.
                            # we only cache the type if it's forseeable that it is usable as a standalone type
                            # which means that it must be either a record or an enum or a fixed type when converted
                            # to Avro. That means we look for the presence of 'type', 'properties', 'allOf', 'anyOf',
                            # and 'enum' in the resolved type.
                            if resolved_json_type and (('type' in resolved_json_type and resolved_json_type['type'] == 'object') or 'properties' in resolved_json_type or 'enum' in resolved_json_type or
                                                       'allOf' in resolved_json_type or 'anyOf' in resolved_json_type):
                                self.imported_types[ref] = self.compose_namespace(
                                    type_namespace, type_name)
                            # resolve type
                            deps: List[str] = []
                            resolved_avro_type: dict | list | str | None = self.json_type_to_avro_type(
                                resolved_json_type, type_name, '', type_namespace, deps, resolved_schema, new_base_uri, avro_schema, [], recursion_depth + 1)
                            if isinstance(resolved_avro_type, str):
                                dependencies.extend(deps)
                                return self.post_check_avro_type(dependencies, resolved_avro_type)
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
                                        print(
                                            f"WARNING: no 'type' definition for {ref} in record {record_name}: {json.dumps(resolved_avro_type)}")
                                        resolved_avro_type = generic_type()
                                elif isinstance(avro_type, str) and resolved_avro_type:
                                    # this is a plain type reference
                                    avro_type = resolved_avro_type
                                    self.imported_types[ref] = avro_type
                                    resolved_avro_type = None
                                if resolved_avro_type:
                                    # this is not a record type that can stand on its own,
                                    # so we remove the cached type entry
                                    # and pass it on as an inline type
                                    dependencies.extend(deps)
                                    if ref in self.imported_types:
                                        del self.imported_types[ref]
                                    avro_type = self.merge_avro_schemas(
                                        [avro_type, resolved_avro_type], avro_schema, local_name)
                                    if isinstance(avro_type, dict) and 'name' in avro_type and not self.is_standalone_avro_type(avro_type):
                                        del avro_type['name']
                                        return self.post_check_avro_type(dependencies, avro_type)
                            else:
                                avro_type = resolved_avro_type
                                self.imported_types[ref] = copy.deepcopy(
                                    avro_type)

                            if len(deps) > 0:
                                if isinstance(avro_type, dict):
                                    avro_type['dependencies'] = deps
                                else:
                                    dependencies.extend(deps)

                            if self.is_standalone_avro_type(avro_type):
                                self.register_type(avro_schema, avro_type)
                                full_name = self.get_qualified_name(avro_type)
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
                                type1 = self.json_type_to_avro_type(
                                    json_type, record_name, field_name, namespace, dependencies, resolved_schema, new_base_uri, avro_schema, record_stack, recursion_depth + 1)
                                type2 = self.json_type_to_avro_type(resolved_json_type, record_name, field_name, namespace,
                                                                    dependencies, resolved_schema, new_base_uri, avro_schema, record_stack, recursion_depth + 1)
                                # if either of the types are empty, use just the other one
                                if not self.is_empty_type(type1) and not self.is_empty_type(type2):
                                    return self.flatten_union([type1, type2])
                                if not self.is_empty_type(type1):
                                    avro_type = type1
                                    if isinstance(avro_type, list):
                                        return self.post_check_avro_type(dependencies, avro_type)
                                if not self.is_empty_type(type2):
                                    avro_type = type2
                                    if isinstance(avro_type, list):
                                        return self.post_check_avro_type(dependencies, avro_type)
                                json_type = {}
                            else:
                                json_type = self.merge_json_schemas(
                                    [json_type, resolved_json_type])
                                avro_type = self.json_type_to_avro_type(
                                    json_type, record_name, field_name, namespace, dependencies, resolved_schema, new_base_uri, avro_schema, record_stack, recursion_depth + 1)
                                json_type = {}
                            if ref in self.imported_types:
                                # update the import reference to the resolved type if it's cached
                                if isinstance(avro_type, dict) and 'name' in avro_type:
                                    self.imported_types[ref] = avro_type['name']
                                else:
                                    self.imported_types[ref] = avro_type

                # if 'const' is present, make this an enum
                if 'const' in json_type:
                    const_list = json_type['const'] if isinstance(
                        json_type['const'], list) else [json_type['const']]
                    avro_type = self.merge_avro_schemas([avro_type, self.create_enum_type(
                        local_name, namespace, const_list)], avro_schema, local_name)
                if json_object_type or 'enum' in json_type:
                    if json_object_type == 'array':
                        if isinstance(json_type, dict) and 'items' in json_type:
                            deps = []
                            item_type = self.json_type_to_avro_type(
                                json_type['items'], record_name, field_name, namespace, deps, json_schema, base_uri, avro_schema, record_stack, recursion_depth + 1)
                            if self.is_standalone_avro_type(item_type):
                                if isinstance(item_type, dict) and len(deps) > 0:
                                    item_type['dependencies'] = deps
                                self.register_type(avro_schema, item_type)
                                dependencies.append(
                                    self.get_qualified_name(item_type))
                            else:
                                dependencies.extend(deps)
                                if isinstance(item_type, dict) and not 'type' in item_type:
                                    item_type = generic_type()
                                elif isinstance(item_type, str) and not item_type in primitive_types:
                                    dependencies.append(item_type)
                                else:  # not a standalone type, but has a type definition, so we unwind that here
                                    item_type = self.post_check_avro_type(
                                        dependencies, item_type)
                            avro_type = self.merge_avro_schemas(
                                [avro_type, self.create_array_type(item_type)], avro_schema, '')
                        else:
                            avro_type = self.merge_avro_schemas(
                                [avro_type, self.create_array_type(generic_type())], avro_schema, '')
                    elif json_object_type and (json_object_type == 'object' or 'object' in json_object_type):
                        avro_record_type = self.json_schema_object_to_avro_record(
                            local_name, json_type, namespace, json_schema, base_uri, avro_schema, record_stack)
                        if isinstance(avro_record_type, list):
                            for record_entry in avro_record_type:
                                self.lift_dependencies_from_type(
                                    record_entry, dependencies)
                        avro_type = self.merge_avro_schemas([avro_type, avro_record_type], avro_schema, avro_type.get(
                            'name', local_name) if isinstance(avro_type, dict) else local_name)
                        self.lift_dependencies_from_type(
                            avro_type, dependencies)
                    elif 'enum' in json_type and (not 'type' in json_type or json_type['type'] == "string"):
                        # we skip all enums that are not of implicit or explicit type 'string'
                        enum = [avro_name(e) for e in json_type['enum'] if isinstance(
                            e, str) and e != '']
                        if len(enum) > 0:
                            # if the enum ends up empty (only non-strings in the enum), we will skip it
                            enum = list(set(enum))
                            if len(enum) > 0:
                                avro_type = self.create_enum_type(local_name, self.compose_namespace(
                                    namespace, record_name + '_types'), enum)
                    else:
                        avro_type = self.json_schema_primitive_to_avro_type(json_object_type, json_type.get(
                            'format'), json_type.get('enum'), record_name, field_name, namespace, dependencies)
            else:
                if isinstance(json_type, dict):
                    avro_type = self.merge_avro_schemas([avro_type, self.json_schema_primitive_to_avro_type(json_type, json_type.get('format'), json_type.get(
                        'enum'), record_name, field_name, namespace, dependencies)], avro_schema, avro_type.get('name', local_name) if isinstance(avro_type, dict) else local_name)
                else:
                    avro_type = self.merge_avro_schemas([avro_type, self.json_schema_primitive_to_avro_type(
                        json_type, None, None, record_name, field_name, namespace, dependencies)], avro_schema, avro_type.get('name', local_name) if isinstance(avro_type, dict) else local_name)

            if isinstance(avro_type, dict) and 'name' in avro_type and 'type' in avro_type and not (avro_type['type'] in ['array', 'map']):
                if not 'namespace' in avro_type:
                    avro_type['namespace'] = namespace
                existing_type = next((t for t in avro_schema if t.get(
                    'name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace')), None)
                if existing_type:
                    existing_type_name = self.get_qualified_name(existing_type)
                    if not existing_type_name in dependencies:
                        dependencies.append(existing_type_name)
                    return existing_type_name
                self.set_avro_type_value(avro_type, 'name', local_name)

            # post-check on the avro type: if the type is a dict, and the 'type' is not
            # a record, enum, fixed, array, or map, we will just return the basic type
            # and push its dependencies up the stack
            avro_type = self.post_check_avro_type(dependencies, avro_type)

            if isinstance(avro_type, dict) and 'unmerged_types' in avro_type:
                self.types_with_unmerged_types.append(avro_type)

            return avro_type
        except RecursionError as e:
            print(
                f"Recursion error while processing {namespace}:{record_name}:{field_name} with recursion depth {recursion_depth}")
            raise e

    def post_check_avro_type(self, dependencies, avro_type):
        """Post-check the Avro type and push dependencies up the stack."""
        if isinstance(avro_type, dict) and 'type' in avro_type and (isinstance(avro_type, list) or not avro_type['type'] in ['array', 'map', 'record', 'enum', 'fixed']):
            if 'dependencies' in avro_type:
                dependencies.extend(avro_type['dependencies'])
            avro_type = avro_type['type']
        return avro_type

    def register_type(self, avro_schema, avro_type) -> bool:
        """Register a type in the Avro schema."""
        existing_type = next((t for t in avro_schema if t.get(
            'name') == avro_type['name'] and t.get('namespace') == avro_type.get('namespace')), None)
        if not existing_type:
            if self.is_empty_type(avro_type) and not 'unmerged_types' in avro_type:
                print(f'WARN: Standalone type {avro_type["name"]} is empty')
            if self.is_standalone_avro_type(avro_type):
                avro_schema.append(avro_type)
                return True
            else:
                return False
        else:
            return True

    def has_composition_keywords(self, json_object: dict) -> bool:
        """Check if the JSON object has any of the combining keywords: allOf, oneOf, anyOf."""
        return isinstance(json_object, dict) and ('allOf' in json_object or 'oneOf' in json_object or 'anyOf' in json_object)

    def has_enum_keyword(self, json_object: dict) -> bool:
        """Check if the JSON object is an enum."""
        return isinstance(json_object, dict) and 'enum' in json_object

    def is_array_object(self, json_object: dict) -> bool:
        """Check if the JSON object is an array object."""
        return isinstance(json_object, dict) and 'type' in json_object and json_object['type'] == 'array'

    def is_standalone_avro_type(self, avro_type: dict | list | str) -> bool:
        """Check if the Avro type is a standalone type."""
        return isinstance(avro_type, dict) and 'type' in avro_type and (avro_type['type'] in ['record', 'enum', 'fixed'])

    def is_avro_complex_type(self, avro_type: dict) -> bool:
        """Check if the Avro type is a complex type."""
        return 'type' in avro_type and avro_type['type'] in ['record', 'enum', 'fixed', 'array', 'map']

    def set_avro_type_value(self, avro_type: dict | list | str, name: str, value: dict | list | str):
        """Set a value in an Avro type."""
        if isinstance(avro_type, dict):
            if name == 'namespace' or name == 'name':
                if 'type' in avro_type:
                    if not (avro_type['type'] in ['record', 'enum', 'fixed']):
                        return
            avro_type[name] = value

    def create_avro_record(self, name: str, namespace: str, fields: list) -> dict:
        """Create an Avro record type."""
        return {
            'type': 'record',
            'name': avro_name(name),
            'namespace': namespace,
            'fields': fields
        }

    def create_wrapper_record(self, wrapper_name: str, wrapper_namespace: str, wrapper_field: str, dependencies: list, avro_type: list | str | dict) -> dict:
        """Create a union wrapper type in Avro."""
        rec = self.create_avro_record(wrapper_name, wrapper_namespace, [
            {
                'name': wrapper_field,
                'type': avro_type
            }
        ])
        if len(dependencies) > 0:
            rec['dependencies'] = dependencies
        return rec

    def create_enum_type(self, name: str, namespace: str, symbols: list) -> dict:
        """Create an Avro enum type."""
        # the symbol list may have been merged by composition to we flatten it to have a unique list
        symbols = self.flatten_union(symbols)
        return {
            'type': 'enum',
            'name': name,
            'namespace': namespace,
            'symbols': [avro_name(s) for s in symbols]
        }

    def create_array_type(self, items: list | dict | str) -> dict:
        """Create an Avro array type."""
        return {
            'type': 'array',
            'items': items
        }

    def create_map_type(self, values: list | dict | str) -> dict:
        """Create an Avro map type."""
        return {
            'type': 'map',
            'values': values
        }

    def nullable(self, avro_type: list | dict | str) -> list | dict | str:
        """Wrap a type in a union with null."""
        if isinstance(avro_type, list):
            cp = avro_type.copy()
            cp.insert(0, 'null')
            return cp
        return ['null', avro_type]

    def merge_description_into_doc(self, source_json: dict, target_avro: dict | list | str):
        """Merge a description in JSON into Avro doc."""
        if isinstance(source_json, dict) and 'description' in source_json and isinstance(target_avro, dict):
            target_avro['doc'] = target_avro['doc'] + ", " + \
                source_json['description'] if 'doc' in target_avro else source_json['description']

    def merge_dependencies_into_parent(self, dependencies: list, child_type: dict | list | str, parent_type: dict | list | str):
        """Merge dependencies from a child type into a parent type."""
        self.lift_dependencies_from_type(child_type, dependencies)
        if len(dependencies) > 0 and isinstance(parent_type, dict):
            if 'dependencies' in parent_type:
                dependencies.extend(parent_type['dependencies'])
            else:
                parent_type['dependencies'] = dependencies

    def lift_dependencies_from_type(self, child_type: dict | list | str, dependencies: list):
        """Lift all dependencies from a type and return a new type with the dependencies lifted."""
        if isinstance(child_type, dict):
            if 'dependencies' in child_type:
                dependencies.extend(child_type['dependencies'])
                del child_type['dependencies']

    def compose_namespace(self, *names) -> str:
        """Compose a namespace from a list of names."""
        return '.'.join([avro_namespace(n) for n in names if n])

    def get_qualified_name(self, avro_type):
        """Get the qualified name of an Avro type."""
        return self.compose_namespace(avro_type.get('namespace', ''), avro_type.get('name', ''))

    def json_schema_object_to_avro_record(self, name: str, json_object: dict, namespace: str, json_schema: dict, base_uri: str, avro_schema: list, record_stack: list) -> dict | list | str | None:
        """Convert a JSON schema object declaration to an Avro record."""
        dependencies: List[str] = []
        avro_type: list | dict | str = {}

        # handle top-level allOf, anyOf, oneOf
        if self.has_composition_keywords(json_object):
            # we will merge allOf, oneOf, anyOf into a union record type
            type = self.json_type_to_avro_type(
                json_object, name, '', namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
            if isinstance(type, str):
                # we are skipping references and primitives
                return None
            if isinstance(type, list):
                # we should have a union type
                avro_type = self.create_wrapper_record(
                    name+"_union", self.utility_namespace, 'options', [], type)
            elif isinstance(type, dict) and 'type' in type and type['type'] != 'record':
                # merge the type into a record type if it's not a record type
                print(
                    f'INFO: Standalone type {name} is being wrapped in a record')
                avro_type = self.create_wrapper_record(avro_name(type.get(
                    'name', name)+'_wrapper'), self.utility_namespace, 'value', type.get('dependencies', []), type)
            else:
                avro_type = type
            # add external dependencies to the record
            self.merge_dependencies_into_parent(dependencies, type, avro_type)
            self.merge_description_into_doc(json_object, avro_type)
            # return the union type
            return avro_type

        if self.has_enum_keyword(json_object):
            # this is an enum
            avro_enum = self.create_enum_type(
                avro_name(name), namespace, json_object['enum'])
            self.merge_description_into_doc(json_object, avro_enum)
            return avro_enum

        if self.is_array_object(json_object):
            # this is an array, which can't be standalone in Avro, so we will wraps it into a record
            # and include the type as an inline
            print(
                f'WARN: Standalone array type {name} will be wrapped in a record')
            deps: List[str] = []
            array_type = self.json_type_to_avro_type(json_object, name, avro_name(
                name), namespace, deps, json_schema, base_uri, avro_schema, record_stack)
            avro_array = self.create_wrapper_record(
                avro_name(name+'_wrapper'), self.utility_namespace, 'items', [], array_type)
            self.merge_description_into_doc(json_object, avro_array)
            self.merge_dependencies_into_parent(deps, array_type, avro_array)
            return avro_array

        # at this point, we have to assume that we have a JSON schema object
        title = json_object.get('title')
        record_name = avro_name(name if name else title if title else None)
        if record_name is None:
            raise ValueError(
                f"Cannot determine record name for json_object {json_object}")
        if len(record_stack) > 0:
            # if we have a record stack, we need to add the current name to
            # the namespace since nested types are disambiguated by their namespace
            namespace = self.compose_namespace(
                namespace, record_stack[-1] + "_types")
        # at this point we have a record type
        avro_record = self.create_avro_record(record_name, namespace, [])
        # we need to prevent circular dependencies, so we will maintain a stack of the in-progress
        # records and will resolve the cycle as we go. if this record is already in the stack, we will
        # just return a reference to a record that contains this record
        if record_name in record_stack:
            # to break the cycle, we will use a containment type that references
            # the record that is being defined
            print(
                f'WARN: Circular dependency found for record {record_name}. Creating {record_name}_ref.')
            ref_name = avro_name(record_name + '_ref')
            return self.create_wrapper_record(ref_name, namespace, record_name, [], self.compose_namespace(namespace, record_name))
        try:
            # enter the record stack scope for this record
            record_stack.append(record_name)
            # collect the required fields so we can make those fields non-null
            required_fields = json_object.get('required', [])

            field_refs = []
            if 'properties' in json_object and isinstance(json_object['properties'], dict):
                # add the properties as fields
                for field_name, json_field_types in json_object['properties'].items():
                    if isinstance(json_field_types, bool):
                        # for "propertyname": true, we skip. schema bug.
                        continue
                    if not isinstance(json_field_types, list):
                        json_field_types = [json_field_types]
                    field_type_list = []
                    field_ref_type_list = []
                    const = None
                    default = None
                    description = None
                    for json_field_type in json_field_types:
                        # skip fields with an bad or empty type
                        if not isinstance(json_field_type, dict):
                            continue
                        field_name = avro_name(field_name)
                        # last const wins if there are multiple
                        const = json_field_type.get('const', const)
                        # last default wins if there are multiple
                        default_value = json_field_type.get('default')
                        if default_value and not isinstance(default_value, dict) and not isinstance(default_value, list):
                            default = default_value
                        # get the description from the field type
                        description = json_field_type.get('description', description)
                        # convert the JSON-type field to an Avro-type field
                        avro_field_ref_type = avro_field_type = self.ensure_type(self.json_type_to_avro_type(
                            json_field_type, record_name, field_name, namespace, dependencies, json_schema, base_uri, avro_schema, record_stack))
                        if isinstance(avro_field_type, list):
                            avro_field_type = self.flatten_union(
                                avro_field_type)
                            avro_field_ref_type = avro_field_type
                        elif isinstance(avro_field_type, dict):
                            self.lift_dependencies_from_type(
                                avro_field_type, dependencies)
                            # if the first call gave us a global type that got added to the schema, this call will give us a reference
                            if self.is_standalone_avro_type(avro_field_type):
                                avro_field_ref_type = self.get_qualified_name(
                                    avro_field_type)
                        if avro_field_type is None:
                            # None type is a problem
                            raise ValueError(
                                f"avro_field_type is None for field {field_name}")
                        if isinstance(avro_field_type, dict) and 'type' in avro_field_type and not self.is_avro_complex_type(avro_field_type):
                            # if the field type is a basic type, inline it
                            avro_field_type = avro_field_type['type']
                        field_type_list.append(avro_field_type)
                        field_ref_type_list.append(avro_field_ref_type)

                    effective_field_type = field_type_list[0] if len(
                        field_type_list) == 1 else field_type_list
                    effective_field_ref_type = field_ref_type_list[0] if len(
                        field_ref_type_list) == 1 else field_ref_type_list
                    avro_field = {
                        'name': avro_name(field_name),
                        'type': self.nullable(effective_field_type) if not field_name in required_fields and 'null' not in effective_field_type else effective_field_type
                    }
                    if field_name != avro_name(field_name):
                        avro_field['altnames'] = { "json": field_name }
                    if const:
                        avro_field['const'] = const
                    if default:
                        avro_field['default'] = default
                    if description:
                        avro_field['doc'] = description
                    field_type_list.append(avro_field_type)
                    avro_field_ref = {
                        'name': avro_name(field_name),
                        'type': self.nullable(effective_field_ref_type) if not field_name in required_fields and 'null' not in effective_field_ref_type else effective_field_ref_type
                    }
                    if description:
                        avro_field_ref['doc'] = description
                    field_ref_type_list.append(avro_field_ref)
                    # add the field to the record
                    avro_record['fields'].append(avro_field)
                    field_refs.append(avro_field_ref)
            elif not 'additionalProperties' in json_object and not 'patternProperties' in json_object:
                if 'type' in json_object and (json_object['type'] == 'object' or 'object' in json_object['type']) and \
                        not 'allOf' in json_object and not 'oneOf' in json_object and not 'anyOf' in json_object:
                    # we don't have any fields, but we have an object type, so we create a map
                    avro_record = self.create_map_type(generic_type())
                elif 'type' in json_object and (json_object['type'] == 'array' or 'array' in json_object['type']) and \
                        not 'allOf' in json_object and not 'oneOf' in json_object and not 'anyOf' in json_object:
                    # we don't have any fields, but we have an array type, so we create a record with an 'items' field
                    avro_record = self.create_array_type(
                        self.json_type_to_avro_type(
                            json_object['items'], record_name, 'values', namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
                        if 'items' in json_object
                        else generic_type())
                else:
                    return json_object['type'] if 'type' in json_object else generic_type()

            extension_types = []
            prop_docs = ''
            if 'patternProperties' in json_object and isinstance(json_object['patternProperties'], dict) and len(json_object['patternProperties']) > 0:
                # pattern properties are represented as a record with field names that are the patterns
                pattern_props = json_object['patternProperties']
                for pattern_name, props in pattern_props.items():
                    deps = []
                    prop_type = self.ensure_type(self.json_type_to_avro_type(
                        props, record_name, pattern_name, namespace, deps, json_schema, base_uri, avro_schema, record_stack))
                    if self.is_standalone_avro_type(prop_type):
                        self.lift_dependencies_from_type(prop_type, deps)
                        self.set_avro_type_value(
                            prop_type, 'namespace', namespace)
                        self.register_type(avro_schema, prop_type)
                        prop_type_ref = self.get_qualified_name(prop_type)
                        dependencies.append(prop_type_ref)
                    else:
                        dependencies.extend(deps)
                        if isinstance(prop_type, str) and not prop_type in primitive_types:
                            dependencies.append(prop_type)
                    if self.is_empty_type(prop_type):
                        prop_type = generic_type()
                    prop_docs += f"Name pattern '{pattern_name}': [{self.get_field_type_name({'type':prop_type})}]. "
                    extension_types.append(prop_type)

            if 'additionalProperties' in json_object and isinstance(json_object['additionalProperties'], bool):
                if True == json_object['additionalProperties']:
                    prop_type = generic_type()
                    extension_types.append(prop_type)
            elif 'additionalProperties' in json_object and isinstance(json_object['additionalProperties'], dict) and len(json_object['additionalProperties']) > 0:
                # additional properties are represented as a map of string to the type of the value
                additional_props = json_object['additionalProperties']
                deps = []
                values_type = self.json_type_to_avro_type(
                    additional_props, record_name, record_name + '_extensions', namespace, dependencies, json_schema, base_uri, avro_schema, record_stack)
                if self.is_standalone_avro_type(values_type):
                    self.lift_dependencies_from_type(values_type, deps)
                    self.set_avro_type_value(
                        values_type, 'namespace', namespace)
                    self.register_type(avro_schema, values_type)
                    values_type_ref = self.get_qualified_name(values_type)
                    dependencies.append(values_type_ref)
                else:
                    dependencies.extend(deps)
                    if isinstance(values_type, str) and not values_type in primitive_types:
                        dependencies.append(values_type)
                if self.is_empty_type(values_type):
                    values_type = generic_type()
                prop_docs += f"Extra properties: [{self.get_field_type_name({'type':values_type})}]. "
                extension_types.append(values_type)
            self.merge_description_into_doc(json_object, avro_record)

            avro_alternate_record = None
            if extension_types:
                # Since Avro Schema does not allow fields with dynamic names
                # to appear alongside regular fields, we will union the types of all properties with the
                # type of the additionalProperties and document this in the record's description
                json_field_types = [field['type'] for field in field_refs]
                field_type_names = [
                    [field['name'], self.get_field_type_name(field)] for field in field_refs]
                field_type_name_list: str = ', '.join(
                    [f"'{field[0]}': [{field[1]}]" for field in field_type_names])
                json_field_types.extend(extension_types)
                json_field_types = self.flatten_union(json_field_types)
                if len(json_field_types) == 1:
                    json_field_types = json_field_types[0]
                doc = f"Alternate map: {field_type_name_list}. " if field_type_names else ''
                doc += prop_docs
                avro_alternate_record = self.create_map_type(json_field_types)
                if not self.is_empty_type(avro_record):
                    avro_alternate_record['alternateof'] = self.get_qualified_name(avro_record)
                dependencies.append(
                    self.compose_namespace(namespace, record_name))
                avro_record['doc'] = doc if not 'doc' in avro_record else avro_record['doc'] + ', ' + doc

            if len(dependencies) > 0:
                # dedupe the list
                dependencies = list(set(dependencies))
                avro_record['dependencies'] = dependencies
        finally:
            record_stack.pop()
        if avro_alternate_record:
            if self.is_empty_type(avro_record):
                # there's no substantive content in the record,
                # so we will just return the alternate record, which
                # is a plain map
                return avro_alternate_record
            return [avro_record, avro_alternate_record]
        return avro_record

    def postprocess_schema(self, avro_schema: list) -> None:
        """ Post-process the Avro Schema for cases wheer we need a second pass """
        if len(self.types_with_unmerged_types) > 0:
            types_with_unmerged_types = copy.deepcopy(
                self.types_with_unmerged_types)
            self.types_with_unmerged_types = []
            for ref_type in types_with_unmerged_types:
                # find ref_type anywhere in the avro_schema graph, matching
                # on name and namespace.
                def find_fn(
                    t): return 'name' in t and t['name'] == ref_type['name'] and 'namespace' in t and t['namespace'] == ref_type['namespace']
                type = find_schema_node(find_fn, avro_schema)
                if not type:
                    raise ValueError(
                        f"Couldn't find type {ref_type['namespace']}.{ref_type['name']} in the Avro Schema.")
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
                self.lift_dependencies_from_type(type, deps)
                for item in unmerged_types:
                    if isinstance(item, str):
                        found_avro_type = next(
                            (t for t in avro_schema if self.get_qualified_name(t) == item), None)
                        if not found_avro_type:
                            continue
                    elif isinstance(item, dict):
                        found_avro_type = item
                        self.lift_dependencies_from_type(found_avro_type, deps)
                    if isinstance(found_avro_type, dict):
                        candidate = found_avro_type
                        if 'unmerged_types' in candidate:
                            del candidate['unmerged_types']
                        mergeable_types.append(candidate)
                merge_result = self.merge_avro_schemas(
                    mergeable_types, avro_schema, local_name, deps)
                if isinstance(merge_result, dict):
                    merge_result['dependencies'] = deps
                    if 'unmerged_types' in merge_result:
                        del merge_result['unmerged_types']
                if isinstance(merge_result, list):
                    # unmerged field containers have fields.
                    self.set_avro_type_value(
                        type, 'name', type['name'] + '_item')
                    self.set_avro_type_value(
                        type, 'fields', [{'name': 'value', 'type': merge_result}])
                    merge_result = copy.deepcopy(type)
                set_schema_node(find_fn, merge_result, avro_schema)

    def process_definition_list(self, json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema_list):
        """Process a schema definition list."""
        for sub_schema_name, schema in json_schema_list.items():
            if not isinstance(schema, dict) and not isinstance(schema, list):
                # skip items that are not schema definitions or lists
                continue
            if 'type' in schema or 'allOf' in schema or 'oneOf' in schema or 'anyOf' in schema or 'properties' in schema or 'enum' in schema or '$ref' in schema or 'additionalProperties' in schema or 'patternProperties' in schema:
                # this is a schema definition
                self.process_definition(
                    json_schema, namespace, base_uri, avro_schema, record_stack, sub_schema_name, schema)
                continue
            # it's a schema definition list
            self.process_definition_list(
                json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema)

    def process_definition(self, json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema, is_root: bool = False) -> Tuple[str, str] | None:
        """ Process a schema definition. """
        avro_schema_item = None
        avro_schema_item_list = self.json_schema_object_to_avro_record(
            schema_name, schema, namespace, json_schema, base_uri, avro_schema, record_stack)
        if not isinstance(avro_schema_item_list, list) and not isinstance(avro_schema_item_list, dict):
            # skip if the record couldn't be resolved
            return None
        # the call above usually returns a single record, but we pretend it's normally a list to handle allOf/anyOf/oneOf cases
        if isinstance(avro_schema_item_list, list) and is_root and len(avro_schema_item_list) > 1:
            # if we have multiple root-level records, we will wrap them all in a single record
            root_avro_schema_item = self.create_wrapper_record(
                schema_name+'_wrapper', namespace, 'root', [], avro_schema_item_list)
            for avro_schema_item in avro_schema_item_list:
                self.merge_dependencies_into_parent(
                    [], avro_schema_item, root_avro_schema_item)
            self.register_type(avro_schema, root_avro_schema_item)
            return root_avro_schema_item['namespace'], root_avro_schema_item['name']
        elif not isinstance(avro_schema_item_list, list):
            # is not a list, so we'll wrap it in a list
            avro_schema_item_list = [avro_schema_item_list]
        for avro_schema_item in avro_schema_item_list:
            # add the item to the schema if it's not already there
            if isinstance(avro_schema_item, str):
                continue
            if isinstance(avro_schema_item, dict) and not 'name' in avro_schema_item:
                avro_schema_item['name'] = avro_name(schema_name)
            existing_type = next((t for t in avro_schema if t.get('name') == avro_schema_item['name'] and t.get(
                'namespace') == avro_schema_item.get('namespace')), None)
            if not existing_type:
                if (not self.is_empty_type(avro_schema_item) or 'unmerged_types' in avro_schema_item) and \
                        self.is_standalone_avro_type(avro_schema_item):
                    # we only register record/enum as type. the other defs are mix-ins
                    self.register_type(avro_schema, avro_schema_item)
                    return avro_schema_item['namespace'], avro_schema_item['name']
                elif is_root:
                    # at the root, we will wrap the type in a record to make it top-level
                    deps: List[str] = []
                    self.lift_dependencies_from_type(avro_schema_item, deps)
                    avro_schema_wrapper = self.create_wrapper_record(schema_name, avro_schema_item.get(
                        'namespace', namespace), avro_schema_item['name'], deps, avro_schema_item)
                    if len(deps) > 0:
                        avro_schema_wrapper['dependencies'] = deps
                    avro_schema_item = avro_schema_wrapper
                    self.register_type(avro_schema, avro_schema_item)
                    return avro_schema_item['namespace'], avro_schema_item['name']
        return None

    def id_to_avro_namespace(self, id: str) -> str:
        """Convert a XSD namespace to Avro Namespace."""
        parsed_url = urlparse(id)
        # strip the file extension
        path = parsed_url.path.rsplit('.')[0]
        path_segments = path.strip('/').replace('-', '_').split('/')
        reversed_path_segments = reversed(path_segments)
        namespace_suffix = self.compose_namespace(*reversed_path_segments)
        if parsed_url.hostname:
            namespace_prefix = self.compose_namespace(
                *reversed(parsed_url.hostname.split('.')))
        namespace = self.compose_namespace(namespace_prefix, namespace_suffix)
        return namespace

    def jsons_to_avro(self, json_schema: dict | list, namespace: str, base_uri: str) -> list | dict | str:
        """Convert a JSON-schema to an Avro-schema."""
        avro_schema: List[dict] = []
        record_stack: List[str] = []

        parsed_url = urlparse(base_uri)
        schema_name = self.root_class_name

        if isinstance(json_schema, dict) and ('definitions' in json_schema or '$defs' in json_schema):
            # this is a swagger file or has a 'definitions' block
            json_schema_defs = json_schema.get(
                'definitions', json_schema.get('$defs', []))
            for def_schema_name, schema in json_schema_defs.items():
                if 'type' in schema or 'allOf' in schema or 'oneOf' in schema or 'anyOf' in schema or 'properties' in schema or 'enum' in schema or '$ref' in schema or 'additionalProperties' in schema or 'patternProperties' in schema:
                    # this is a schema definition
                    self.process_definition(
                        json_schema, namespace, base_uri, avro_schema, record_stack, def_schema_name, schema)
                else:
                    # it's a schema definition list
                    self.process_definition_list(
                        json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, schema.copy())
        elif isinstance(json_schema, list):
            # this is a schema definition list
            self.process_definition_list(
                json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema)

        root_namespace = None
        root_name = None
        if isinstance(json_schema, dict) and 'type' in json_schema or 'allOf' in json_schema or 'oneOf' in json_schema or 'anyOf' in json_schema or 'properties' in json_schema:
            # this is a schema definition
            if isinstance(json_schema, dict) and '$ref' in json_schema:
                # if there is a $ref at the root level, resolve the reference and merge it with the current schema
                ref = json_schema['$ref']
                if ref:
                    ref_schema, json_doc = self.resolve_reference(
                        json_schema, base_uri, json_schema)
                    json_schema = self.merge_json_schemas(
                        [json_schema, ref_schema], intersect=False)
            root_info = self.process_definition(
                json_schema, namespace, base_uri, avro_schema, record_stack, schema_name, json_schema, is_root=True)
            if root_info:
                root_namespace, root_name = root_info

        # postprocessing pass
        self.postprocess_schema(avro_schema)

        if isinstance(avro_schema, list) and len(avro_schema) > 1 and self.split_top_level_records:
            new_avro_schema = []
            for item in avro_schema:
                if isinstance(item, dict) and 'type' in item and item['type'] == 'record':
                    # we need to make a copy since the inlining operation shuffles types
                    schema_copy = copy.deepcopy(avro_schema)
                    # find the item with the same name and namespace in the copy
                    found_item = next((t for t in schema_copy if t.get(
                        'name') == item['name'] and t.get('namespace') == item.get('namespace')), None)
                    if found_item:
                        # inline all dependencies of the item
                        inline_dependencies_of(schema_copy, found_item)
                        new_avro_schema.append(found_item)
            avro_schema = new_avro_schema
        else:
            # sort the records by their dependencies
            if root_name and root_namespace and not ('definitions' in json_schema or '$defs' in json_schema):
                # inline all dependencies if this is a doc with only a root level definition
                root = find_schema_node(
                    lambda t: 'name' in t and t['name'] == root_name and 'namespace' in t and t['namespace'] == root_namespace, avro_schema)
                inline_dependencies_of(avro_schema, root)
                return root
            else:
                avro_schema = sort_messages_by_dependencies(avro_schema)

            if parsed_url.fragment and isinstance(json_schema, dict):
                # if the fragment is present in the URL, it's a reference to a schema definition
                # so we will resolve that reference and return a type
                self.imported_types.clear()
                fragment_schema: List[dict] = []
                json_pointer = parsed_url.fragment
                schema_name = parsed_url.fragment.split('/')[-1]
                schema = jsonpointer.resolve_pointer(json_schema, json_pointer)
                avro_schema_item = self.json_schema_object_to_avro_record(
                    schema_name, schema, namespace, json_schema, base_uri, fragment_schema, record_stack)
                if avro_schema_item:
                    # we roll all the types into this record as the top level type
                    inline_dependencies_of(avro_schema, avro_schema_item)
                    return avro_schema_item

        return avro_schema

    def convert_jsons_to_avro(self, json_schema_file_path: str, avro_schema_path: str, namespace: str | None = None, utility_namespace: str | None = None) -> list | dict | str:
        """Convert JSON schema file to Avro schema file."""
        # turn the file path into a file URI if it's not a URI already
        parsed_url = urlparse(json_schema_file_path)
        if not parsed_url.hostname and not parsed_url.scheme == 'file':
            json_schema_file_path = 'file://' + json_schema_file_path
            parsed_url = urlparse(json_schema_file_path)
        content = self.fetch_content(parsed_url.geturl())
        json_schema = json.loads(content)

        if not namespace:
            namespace = parsed_url.geturl().replace('\\', '/').replace('-',
                                                                       '_').split('/')[-1].split('.')[0]
            # get the $id if present
            if '$id' in json_schema:
                namespace = self.id_to_avro_namespace(json_schema['$id'])
        self.root_namespace = namespace
        if utility_namespace:
            self.utility_namespace = utility_namespace
        else:
            self.utility_namespace = self.root_namespace + '.utility'

        # drop the file name from the parsed URL to get the base URI
        avro_schema = self.jsons_to_avro(
            json_schema, namespace, parsed_url.geturl())
        if len(avro_schema) == 1:
            avro_schema = avro_schema[0]

        # create the directory for the Avro schema file if it doesn't exist
        dir = os.path.dirname(
            avro_schema_path) if not self.split_top_level_records else avro_schema_path
        if dir != '' and not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        if self.split_top_level_records:
            # if we are splitting top level records, we will create a file for each record
            for item in avro_schema:
                if isinstance(item, dict) and 'type' in item and item['type'] == 'record':
                    schema_file_path = os.path.join(
                        dir, item['name'] + '.avsc')
                    with open(schema_file_path, 'w') as avro_file:
                        json.dump(item, avro_file, indent=4)
        else:
            with open(avro_schema_path, 'w') as avro_file:
                json.dump(avro_schema, avro_file, indent=4)
        return avro_schema


def convert_jsons_to_avro(json_schema_file_path: str, avro_schema_path: str, namespace: str = '', utility_namespace='', root_class_name='', split_top_level_records=False) -> list | dict | str:
    """Convert JSON schema file to Avro schema file."""

    if not json_schema_file_path:
        raise ValueError('JSON schema file path is required')
    if not json_schema_file_path.startswith('http'):
        if not os.path.exists(json_schema_file_path):
            raise FileNotFoundError(f'JSON schema file {json_schema_file_path} not found')

    try:
        converter = JsonToAvroConverter()
        converter.split_top_level_records = split_top_level_records
        if root_class_name:
            converter.root_class_name = root_class_name
        return converter.convert_jsons_to_avro(json_schema_file_path, avro_schema_path, namespace, utility_namespace)
    except Exception as e:
        print(
            f'Error converting JSON {json_schema_file_path} to Avro: {e.args[0]}')
        return []
