# pylint: disable=line-too-long

""" StructureToTypeScript class for converting JSON Structure schema to TypeScript classes """

import json
import os
from typing import Any, Dict, List, Set, Union, Optional

from avrotize.common import pascal, process_template

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

INDENT = '    '


def is_typescript_reserved_word(word: str) -> bool:
    """Check if word is a TypeScript reserved word."""
    reserved_words = [
        'break', 'case', 'catch', 'class', 'const', 'continue', 'debugger',
        'default', 'delete', 'do', 'else', 'export', 'extends', 'finally',
        'for', 'function', 'if', 'import', 'in', 'instanceof', 'new', 'return',
        'super', 'switch', 'this', 'throw', 'try', 'typeof', 'var', 'void',
        'while', 'with', 'yield', 'enum', 'string', 'number', 'boolean', 'symbol',
        'type', 'namespace', 'module', 'declare', 'abstract', 'readonly',
    ]
    return word in reserved_words


class StructureToTypeScript:
    """Converts JSON Structure schema to TypeScript classes using templates with namespace support."""

    def __init__(self, base_package: str = '', typed_json_annotation=False, avro_annotation=False) -> None:
        self.base_package = base_package
        self.typed_json_annotation = typed_json_annotation
        self.avro_annotation = avro_annotation
        self.output_dir = os.getcwd()
        self.src_dir = os.path.join(self.output_dir, "src")
        self.schema_doc: JsonNode = None
        self.generated_types: Dict[str, str] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}
        self.type_dict: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}
        self.schema_registry: Dict[str, Dict] = {}
        self.offers: Dict[str, Any] = {}

    def get_qualified_name(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name with a dot separator."""
        return f"{namespace}.{name}" if namespace != '' else name

    def concat_namespace(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name with a dot separator."""
        if namespace and name:
            return f"{namespace}.{name}"
        return namespace or name

    def map_primitive_to_typescript(self, structure_type: str) -> str:
        """Map JSON Structure primitive type to TypeScript type."""
        mapping = {
            'null': 'null',
            'boolean': 'boolean',
            'string': 'string',
            'integer': 'number',
            'number': 'number',
            'int8': 'number',
            'uint8': 'number',
            'int16': 'number',
            'uint16': 'number',
            'int32': 'number',
            'uint32': 'number',
            'int64': 'number',
            'uint64': 'number',
            'int128': 'bigint',
            'uint128': 'bigint',
            'float8': 'number',
            'float': 'number',
            'double': 'number',
            'binary32': 'number',
            'binary64': 'number',
            'decimal': 'string',  # Decimal represented as string in TypeScript
            'binary': 'Uint8Array',
            'bytes': 'Uint8Array',
            'date': 'Date',
            'time': 'string',  # ISO time string
            'datetime': 'Date',
            'timestamp': 'Date',
            'duration': 'string',  # ISO duration string
            'uuid': 'string',
            'uri': 'string',
            'jsonpointer': 'string',
            'any': 'any'
        }
        return mapping.get(structure_type, 'any')

    def is_typescript_primitive(self, ts_type: str) -> bool:
        """Check if TypeScript type is a primitive."""
        ts_type = self.strip_nullable(ts_type)
        return ts_type in ['null', 'boolean', 'number', 'bigint', 'string', 'Date', 'any', 'Uint8Array']

    def strip_nullable(self, ts_type: str) -> str:
        """Strip nullable type from TypeScript type."""
        if ts_type.endswith('?'):
            return ts_type[:-1]
        return ts_type

    def safe_name(self, name: str) -> str:
        """Converts a name to a safe TypeScript name."""
        if is_typescript_reserved_word(name):
            return name + "_"
        return name

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition."""
        # Check if it's an absolute URI reference (schema with $id)
        if not ref.startswith('#/'):
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None

        # Handle fragment-only references (internal to document)
        path = ref[2:].split('/')
        schema = context_schema if context_schema else self.schema_doc

        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]

        return schema

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """Recursively registers schemas with $id keywords."""
        if not isinstance(schema, dict):
            return

        if '$id' in schema:
            schema_id = schema['$id']
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                from urllib.parse import urljoin
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            base_uri = schema_id

        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)

        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)

        for key in ['items', 'values', 'additionalProperties']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)

    def convert_structure_type_to_typescript(self, class_name: str, field_name: str,
                                            structure_type: JsonNode, parent_namespace: str,
                                            import_types: Set[str]) -> str:
        """Convert JSON Structure type to TypeScript type with namespace support."""
        if isinstance(structure_type, str):
            mapped_type = self.map_primitive_to_typescript(structure_type)
            if mapped_type == 'any' and structure_type != 'any':
                # It might be a reference to a generated type
                full_name = self.concat_namespace(self.base_package, self.concat_namespace(parent_namespace, structure_type))
                if full_name in self.generated_types:
                    import_types.add(full_name)
                    return pascal(structure_type.split('.')[-1])
            return mapped_type
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            if len(non_null_types) == 1:
                inner_type = self.convert_structure_type_to_typescript(
                    class_name, field_name, non_null_types[0], parent_namespace, import_types)
                if 'null' in structure_type:
                    return f'{inner_type}?'
                return inner_type
            else:
                # Generate embedded union for complex unions
                return self.generate_embedded_union(class_name, field_name, non_null_types, parent_namespace, import_types, write_file=True)
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                if ref_schema:
                    ref_path = structure_type['$ref'].split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    class_ref = self.generate_class_or_choice(ref_schema, ref_namespace, write_file=True, explicit_name=type_name)
                    import_types.add(class_ref)
                    return pascal(class_ref.split('.')[-1])
                return 'any'

            # Handle enum keyword
            if 'enum' in structure_type:
                enum_ref = self.generate_enum(structure_type, field_name, parent_namespace, write_file=True)
                import_types.add(enum_ref)
                return pascal(enum_ref.split('.')[-1])

            # Handle type keyword
            if 'type' not in structure_type:
                return 'any'

            struct_type = structure_type['type']

            # Handle complex types
            if struct_type == 'object':
                class_ref = self.generate_class(structure_type, parent_namespace, write_file=True)
                import_types.add(class_ref)
                return pascal(class_ref.split('.')[-1])
            elif struct_type == 'array':
                items_type = self.convert_structure_type_to_typescript(
                    class_name, field_name+'Item', structure_type.get('items', {'type': 'any'}),
                    parent_namespace, import_types)
                return f'{items_type}[]'
            elif struct_type == 'set':
                items_type = self.convert_structure_type_to_typescript(
                    class_name, field_name+'Item', structure_type.get('items', {'type': 'any'}),
                    parent_namespace, import_types)
                return f'Set<{items_type}>'
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_typescript(
                    class_name, field_name+'Value', structure_type.get('values', {'type': 'any'}),
                    parent_namespace, import_types)
                return f'{{ [key: string]: {values_type} }}'
            elif struct_type == 'choice':
                return self.generate_choice(structure_type, parent_namespace, write_file=True, import_types=import_types)
            elif struct_type == 'tuple':
                return self.generate_tuple(structure_type, parent_namespace, write_file=True, import_types=import_types)
            else:
                return self.convert_structure_type_to_typescript(class_name, field_name, struct_type, parent_namespace, import_types)
        return 'any'

    def generate_class_or_choice(self, structure_schema: Dict, parent_namespace: str,
                                 write_file: bool = True, explicit_name: str = '') -> str:
        """Generates a Class or Choice."""
        struct_type = structure_schema.get('type', 'object')
        if struct_type == 'object':
            return self.generate_class(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'choice':
            return self.generate_choice(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'tuple':
            return self.generate_tuple(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        return 'any'

    def generate_class(self, structure_schema: Dict, parent_namespace: str,
                      write_file: bool, explicit_name: str = '') -> str:
        """Generate TypeScript class from JSON Structure object using templates with namespace support."""
        import_types: Set[str] = set()
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'))
        namespace = self.concat_namespace(self.base_package, structure_schema.get('namespace', parent_namespace))
        ts_qualified_name = self.get_qualified_name(namespace, class_name)

        if ts_qualified_name in self.generated_types:
            return ts_qualified_name

        # Check if this is an abstract type
        is_abstract = structure_schema.get('abstract', False)

        # Generate properties
        properties = structure_schema.get('properties', {})
        required_props = structure_schema.get('required', [])

        fields = []
        for prop_name, prop_schema in properties.items():
            field_def = self.generate_field(prop_name, prop_schema, class_name,
                                           structure_schema.get('namespace', parent_namespace),
                                           required_props, import_types)
            fields.append(field_def)

        # Build field data for template
        field_data = [{
            'name': self.safe_name(field['name']),
            'original_name': field['name'],
            'type': field['type'],
            'type_no_null': self.strip_nullable(field['type']),
            'is_primitive': field['is_primitive'],
            'is_enum': field['is_enum'],
            'is_array': field['is_array'],
            'is_optional': field['is_optional'],
            'docstring': field.get('docstring', ''),
        } for field in fields]

        # Build imports with paths
        imports_with_paths: Dict[str, str] = {}
        for import_type in import_types:
            if import_type == ts_qualified_name:
                continue
            import_is_enum = import_type in self.generated_types and self.generated_types[import_type] == 'enum'
            import_type_parts = import_type.split('.')
            import_type_name = pascal(import_type_parts[-1])
            import_path = '/'.join(import_type_parts)
            current_path = '/'.join(namespace.split('.'))
            relative_import_path = os.path.relpath(import_path, current_path).replace(os.sep, '/')
            if not relative_import_path.startswith('.'):
                relative_import_path = f'./{relative_import_path}'
            if import_is_enum:
                import_type_name_and_util = f"{import_type_name}, {import_type_name}Utils"
                imports_with_paths[import_type_name_and_util] = relative_import_path + '.js'
            else:
                imports_with_paths[import_type_name] = relative_import_path + '.js'

        class_definition = process_template(
            "structuretots/class_core.ts.jinja",
            namespace=namespace,
            class_name=class_name,
            docstring=structure_schema.get('description', structure_schema.get('doc', '')).strip() if structure_schema.get('description') or structure_schema.get('doc') else f'A {class_name} record.',
            fields=field_data,
            imports=imports_with_paths,
            base_package=self.base_package,
            is_abstract=is_abstract,
            typed_json_annotation=self.typed_json_annotation,
        )

        if write_file:
            self.write_to_file(namespace, class_name, class_definition)

        self.generated_types[ts_qualified_name] = 'class'
        self.generated_structure_types[ts_qualified_name] = structure_schema
        return ts_qualified_name

    def generate_field(self, prop_name: str, prop_schema: Dict, class_name: str,
                      parent_namespace: str, required_props: List, import_types: Set[str]) -> Dict:
        """Generates a field for a TypeScript class."""
        field_name = prop_name

        # Check if this is a const field
        if 'const' in prop_schema:
            prop_type = self.convert_structure_type_to_typescript(
                class_name, field_name, prop_schema, parent_namespace, import_types)
            return {
                'name': field_name,
                'type': prop_type,
                'is_primitive': self.is_typescript_primitive(prop_type),
                'is_enum': False,
                'is_array': False,
                'is_optional': False,
                'is_const': True,
                'const_value': prop_schema['const'],
                'docstring': prop_schema.get('description', prop_schema.get('doc', field_name))
            }

        # Determine if required
        is_required = prop_name in required_props if not isinstance(required_props, list) or \
                     len(required_props) == 0 or not isinstance(required_props[0], list) else \
                     any(prop_name in req_set for req_set in required_props)

        # Get property type
        prop_type = self.convert_structure_type_to_typescript(
            class_name, field_name, prop_schema, parent_namespace, import_types)

        # Check if it's already optional (ends with ?)
        is_optional = not is_required or prop_type.endswith('?')
        if not is_required and not prop_type.endswith('?'):
            prop_type += '?'

        return {
            'name': field_name,
            'type': prop_type,
            'is_primitive': self.is_typescript_primitive(prop_type.replace('[]', '').rstrip('?')),
            'is_enum': False,  # Will be determined by import_types
            'is_array': prop_type.replace('?', '').endswith('[]') or 'Set<' in prop_type,
            'is_optional': is_optional,
            'is_const': False,
            'docstring': prop_schema.get('description', prop_schema.get('doc', field_name))
        }

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str,
                     write_file: bool) -> str:
        """Generate TypeScript enum from JSON Structure enum using templates with namespace support."""
        enum_name = pascal(structure_schema.get('name', field_name + 'Enum') if field_name else 'UnnamedEnum')
        namespace = self.concat_namespace(self.base_package, structure_schema.get('namespace', parent_namespace))
        ts_qualified_name = self.get_qualified_name(namespace, enum_name)

        if ts_qualified_name in self.generated_types:
            return ts_qualified_name

        symbols = structure_schema.get('enum', [])
        base_type = structure_schema.get('type', 'string')

        enum_definition = process_template(
            "structuretots/enum_core.ts.jinja",
            namespace=namespace,
            enum_name=enum_name,
            docstring=structure_schema.get('description', structure_schema.get('doc', '')).strip() if structure_schema.get('description') or structure_schema.get('doc') else f'A {enum_name} enum.',
            symbols=symbols,
            base_type=base_type,
        )

        if write_file:
            self.write_to_file(namespace, enum_name, enum_definition)

        self.generated_types[ts_qualified_name] = 'enum'
        self.generated_structure_types[ts_qualified_name] = structure_schema
        return ts_qualified_name

    def generate_choice(self, structure_schema: Dict, parent_namespace: str,
                       write_file: bool, explicit_name: str = '', import_types: Optional[Set[str]] = None) -> str:
        """Generates a TypeScript Union type from JSON Structure choice."""
        if import_types is None:
            import_types = set()

        choices = structure_schema.get('choices', {})
        choice_types = []

        for choice_key, choice_schema in choices.items():
            if isinstance(choice_schema, dict):
                if '$ref' in choice_schema:
                    ref_schema = self.resolve_ref(choice_schema['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                    if ref_schema:
                        ref_path = choice_schema['$ref'].split('/')
                        ref_name = ref_path[-1]
                        ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                        qualified_name = self.generate_class(ref_schema, ref_namespace, write_file=True, explicit_name=ref_name)
                        import_types.add(qualified_name)
                        choice_types.append(qualified_name.split('.')[-1])
                elif 'type' in choice_schema:
                    python_type = self.convert_structure_type_to_typescript('', choice_key, choice_schema, parent_namespace, import_types)
                    choice_types.append(python_type)

        if len(choice_types) == 0:
            return 'any'
        elif len(choice_types) == 1:
            return choice_types[0]
        else:
            return ' | '.join(choice_types)

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str,
                      write_file: bool, explicit_name: str = '', import_types: Optional[Set[str]] = None) -> str:
        """Generates a TypeScript tuple type from JSON Structure tuple."""
        if import_types is None:
            import_types = set()

        properties = structure_schema.get('properties', {})
        tuple_order = structure_schema.get('tuple', [])

        tuple_types = []
        for prop_name in tuple_order:
            if prop_name in properties:
                prop_schema = properties[prop_name]
                prop_type = self.convert_structure_type_to_typescript('', prop_name, prop_schema, parent_namespace, import_types)
                tuple_types.append(prop_type)

        if len(tuple_types) == 0:
            return 'any[]'
        else:
            return f"[{', '.join(tuple_types)}]"

    def generate_embedded_union(self, class_name: str, field_name: str, structure_types: List,
                               parent_namespace: str, import_types: Set[str], write_file: bool = True) -> str:
        """Generate embedded Union type for a field with namespace support."""
        union_types = []
        for t in structure_types:
            union_type = self.convert_structure_type_to_typescript(class_name, field_name, t, parent_namespace, import_types)
            union_types.append(union_type)

        if not union_types:
            return 'any'
        elif len(union_types) == 1:
            return union_types[0]
        else:
            return ' | '.join(union_types)

    def write_to_file(self, namespace: str, name: str, content: str):
        """Write TypeScript class to file in the correct namespace directory."""
        directory_path = os.path.join(self.src_dir, *namespace.split('.'))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)

        file_path = os.path.join(directory_path, f"{name}.ts")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)

    def generate_index_file(self):
        """Generate a root index.ts file that exports all types."""
        exports = []

        for class_name in self.generated_types:
            parts = class_name.split('.')
            file_name = parts[-1]
            module_path = parts[:-1]

            file_relative_path = os.path.join(*(module_path + [f"{file_name}.js"])).replace(os.sep, '/')
            if not file_relative_path.startswith('.'):
                file_relative_path = './' + file_relative_path

            alias_parts = [pascal(part) for part in parts]
            alias_name = '_'.join(alias_parts)

            exports.append(f"export {{ {file_name} as {alias_name} }} from '{file_relative_path}';\n")

        index_file_path = os.path.join(self.src_dir, 'index.ts')
        with open(index_file_path, 'w', encoding='utf-8') as f:
            f.writelines(exports)

    def generate_project_files(self, output_dir: str):
        """Generate project files using templates."""
        tsconfig_content = process_template(
            "structuretots/tsconfig.json.jinja",
        )

        package_json_content = process_template(
            "structuretots/package.json.jinja",
            package_name=self.base_package,
        )

        gitignore_content = process_template(
            "structuretots/gitignore.jinja",
        )

        tsconfig_path = os.path.join(output_dir, 'tsconfig.json')
        package_json_path = os.path.join(output_dir, 'package.json')
        gitignore_path = os.path.join(output_dir, '.gitignore')

        with open(tsconfig_path, 'w', encoding='utf-8') as file:
            file.write(tsconfig_content)

        with open(package_json_path, 'w', encoding='utf-8') as file:
            file.write(package_json_content)

        with open(gitignore_path, 'w', encoding='utf-8') as file:
            file.write(gitignore_content)

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """Processes the definitions section recursively."""
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition or 'enum' in definition:
                    current_namespace = self.concat_namespace(namespace_path, '')
                    check_namespace = self.concat_namespace(self.base_package, current_namespace)
                    check_name = pascal(name)
                    check_ref = self.get_qualified_name(check_namespace, check_name)
                    if check_ref not in self.generated_types:
                        self.generate_class_or_choice(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    new_namespace = self.concat_namespace(namespace_path, name)
                    self.process_definitions(definition, new_namespace)

    def convert_schema(self, schema: Union[List[Dict], Dict], output_dir: str, write_file: bool = True):
        """Convert JSON Structure schema to TypeScript classes with namespace support."""
        self.output_dir = output_dir
        self.src_dir = os.path.join(self.output_dir, "src")

        if isinstance(schema, dict):
            schema = [schema]

        self.schema_doc = schema if len(schema) == 1 else schema

        # Register all schema IDs first
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.register_schema_ids(structure_schema)

        # Process each schema
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            if 'definitions' in structure_schema:
                self.definitions = structure_schema['definitions']

            if '$offers' in structure_schema:
                self.offers = structure_schema['$offers']

            # Process root type
            if 'type' in structure_schema or 'enum' in structure_schema:
                self.generate_class_or_choice(structure_schema, '', write_file=True)
            elif '$root' in structure_schema:
                root_ref = structure_schema['$root']
                root_schema = self.resolve_ref(root_ref, structure_schema)
                if root_schema:
                    ref_path = root_ref.split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else ''
                    self.generate_class_or_choice(root_schema, ref_namespace, write_file=True, explicit_name=type_name)

            # Process remaining definitions
            if 'definitions' in structure_schema:
                self.process_definitions(self.definitions, '')

        self.generate_index_file()
        self.generate_project_files(output_dir)

    def convert(self, structure_schema_path: str, output_dir: str):
        """Convert JSON Structure schema to TypeScript classes."""
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_structure_to_typescript(structure_schema_path, ts_dir_path, package_name='', typed_json_annotation=False, avro_annotation=False):
    """Convert JSON Structure schema to TypeScript classes."""
    if not package_name:
        package_name = os.path.splitext(os.path.basename(structure_schema_path))[0].lower().replace('-', '_')

    converter = StructureToTypeScript(package_name, typed_json_annotation=typed_json_annotation,
                                     avro_annotation=avro_annotation)
    converter.convert(structure_schema_path, ts_dir_path)


def convert_structure_schema_to_typescript(structure_schema, ts_dir_path, package_name='', typed_json_annotation=False, avro_annotation=False):
    """Convert JSON Structure schema to TypeScript classes."""
    converter = StructureToTypeScript(package_name, typed_json_annotation=typed_json_annotation,
                                     avro_annotation=avro_annotation)
    converter.convert_schema(structure_schema, ts_dir_path)
