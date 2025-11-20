# pylint: disable=line-too-long

""" StructureToJavaScript class for converting JSON Structure schema to JavaScript classes """

import json
import os
from typing import Any, Dict, List, Set, Union, Optional

from avrotize.common import pascal, process_template

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

INDENT = '    '


def is_javascript_reserved_word(word: str) -> bool:
    """Check if word is a JavaScript reserved word"""
    reserved_words = [
        'break', 'case', 'catch', 'class', 'const', 'continue', 'debugger',
        'default', 'delete', 'do', 'else', 'export', 'extends', 'finally',
        'for', 'function', 'if', 'import', 'in', 'instanceof', 'new', 'return',
        'super', 'switch', 'this', 'throw', 'try', 'typeof', 'var', 'void',
        'while', 'with', 'yield', 'let', 'static', 'enum', 'await', 'async',
        'implements', 'interface', 'package', 'private', 'protected', 'public'
    ]
    return word in reserved_words


def is_javascript_primitive(word: str) -> bool:
    """Check if word is a JavaScript primitive"""
    primitives = ['null', 'boolean', 'number', 'string', 'Date', 'Array', 'Object']
    return word in primitives


class StructureToJavaScript:
    """Convert JSON Structure schema to JavaScript classes"""

    def __init__(self, base_package: str = '', avro_annotation=False) -> None:
        self.base_package = base_package
        self.avro_annotation = avro_annotation
        self.output_dir = os.getcwd()
        self.schema_doc: JsonNode = None
        self.generated_types: Dict[str, str] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}
        self.type_dict: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}
        self.schema_registry: Dict[str, Dict] = {}

    def get_qualified_name(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name with a dot separator"""
        return f"{namespace}.{name}" if namespace != '' else name

    def concat_namespace(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name with a dot separator"""
        if namespace and name:
            return f"{namespace}.{name}"
        elif namespace:
            return namespace
        else:
            return name

    def map_primitive_to_javascript(self, structure_type: str) -> str:
        """Maps JSON Structure primitive types to JavaScript types"""
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
            'decimal': 'string',  # JavaScript doesn't have native decimal
            'binary': 'string',   # Base64 encoded
            'date': 'Date',
            'time': 'string',     # ISO 8601 time string
            'datetime': 'Date',
            'timestamp': 'Date',
            'duration': 'string', # ISO 8601 duration string
            'uuid': 'string',
            'uri': 'string',
            'jsonpointer': 'string',
            'any': 'any'
        }
        qualified_class_name = self.get_qualified_name(self.base_package, pascal(structure_type))
        if qualified_class_name in self.generated_types:
            result = qualified_class_name
        else:
            result = mapping.get(structure_type, 'any')
        return result

    def is_javascript_primitive_type(self, js_type: str) -> bool:
        """Checks if a type is a JavaScript primitive type"""
        return js_type in ['null', 'boolean', 'number', 'string', 'Date', 'bigint', 'any']

    def safe_name(self, name: str) -> str:
        """Converts a name to a safe JavaScript name"""
        if is_javascript_reserved_word(name):
            return name + "_"
        return name

    def pascal_type_name(self, ref: str) -> str:
        """Converts a reference to a type name"""
        return pascal(ref.split('.')[-1])

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition"""
        if not ref.startswith('#/'):
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None

        path = ref[2:].split('/')
        schema = context_schema if context_schema else self.schema_doc
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        return schema

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """Recursively registers schemas with $id keywords"""
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

    def convert_structure_type_to_javascript(self, class_name: str, field_name: str,
                                            structure_type: JsonNode, parent_namespace: str,
                                            import_types: Set[str]) -> str:
        """Converts JSON Structure type to JavaScript type"""
        if isinstance(structure_type, str):
            js_type = self.map_primitive_to_javascript(structure_type)
            if js_type == structure_type and not self.is_javascript_primitive_type(js_type):
                import_types.add(js_type)
                return self.pascal_type_name(js_type)
            return js_type
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            if len(non_null_types) == 1:
                inner_type = self.convert_structure_type_to_javascript(
                    class_name, field_name, non_null_types[0], parent_namespace, import_types)
                if 'null' in structure_type:
                    return f'{inner_type}|null'
                return inner_type
            else:
                union_types = [self.convert_structure_type_to_javascript(
                    class_name, field_name, t, parent_namespace, import_types) for t in non_null_types]
                return '|'.join(union_types)
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc)
                if ref_schema:
                    ref_path = structure_type['$ref'].split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    ref = self.generate_class_or_choice(ref_schema, ref_namespace, write_file=True, explicit_name=type_name)
                    import_types.add(ref)
                    return ref.split('.')[-1]
                return 'any'

            # Handle enum keyword
            if 'enum' in structure_type:
                enum_ref = self.generate_enum(structure_type, field_name, parent_namespace, write_file=True)
                import_types.add(enum_ref)
                return enum_ref.split('.')[-1]

            # Handle type keyword
            if 'type' not in structure_type:
                return 'any'

            struct_type = structure_type['type']

            # Handle complex types
            if struct_type == 'object':
                class_ref = self.generate_class(structure_type, parent_namespace, write_file=True)
                import_types.add(class_ref)
                return class_ref.split('.')[-1]
            elif struct_type == 'array':
                items_type = self.convert_structure_type_to_javascript(
                    class_name, field_name+'Item', structure_type.get('items', {'type': 'any'}),
                    parent_namespace, import_types)
                return f'Array<{items_type}>'
            elif struct_type == 'set':
                items_type = self.convert_structure_type_to_javascript(
                    class_name, field_name+'Item', structure_type.get('items', {'type': 'any'}),
                    parent_namespace, import_types)
                return f'Set<{items_type}>'
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_javascript(
                    class_name, field_name+'Value', structure_type.get('values', {'type': 'any'}),
                    parent_namespace, import_types)
                return f'Object<string, {values_type}>'
            elif struct_type == 'choice':
                return self.generate_choice(structure_type, parent_namespace, write_file=True, import_types=import_types)
            elif struct_type == 'tuple':
                return self.generate_tuple(structure_type, parent_namespace, write_file=True, import_types=import_types)
            else:
                return self.convert_structure_type_to_javascript(class_name, field_name, struct_type, parent_namespace, import_types)
        return 'any'

    def generate_class_or_choice(self, structure_schema: Dict, parent_namespace: str,
                                 write_file: bool = True, explicit_name: str = '') -> str:
        """Generates a Class or Choice"""
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
        """Generates a JavaScript class from JSON Structure object type"""
        import_types: Set[str] = set()

        # Get name and namespace
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_package, schema_namespace)
        qualified_name = self.get_qualified_name(namespace, class_name)

        if qualified_name in self.generated_types:
            return qualified_name

        # Check if this is an abstract type
        is_abstract = structure_schema.get('abstract', False)

        # Generate properties
        properties = structure_schema.get('properties', {})
        required_props = structure_schema.get('required', [])

        # Collect fields for template
        fields = []
        static_fields = []

        for prop_name, prop_schema in properties.items():
            field_name = self.safe_name(prop_name)
            field_doc = prop_schema.get('description', prop_schema.get('doc', ''))

            # Check if this is a const field
            if 'const' in prop_schema:
                # Const fields are static properties
                const_value = self.format_const_value(prop_schema['const'])
                static_fields.append({
                    'name': field_name,
                    'value': const_value,
                    'docstring': field_doc
                })
                continue

            # Determine if required
            is_required = prop_name in required_props if not isinstance(required_props, list) or \
                         len(required_props) == 0 or not isinstance(required_props[0], list) else \
                         any(prop_name in req_set for req_set in required_props)

            # Get property type
            prop_type = self.convert_structure_type_to_javascript(
                class_name, field_name, prop_schema, schema_namespace, import_types)

            # Add default value if present
            if 'default' in prop_schema:
                default_val = self.format_default_value(prop_schema['default'], prop_type)
            elif not is_required:
                default_val = 'null'
            else:
                default_val = 'undefined'

            fields.append({
                'name': field_name,
                'type': prop_type,
                'default_value': default_val,
                'docstring': field_doc
            })

        # Get docstring
        doc = structure_schema.get('description', structure_schema.get('doc', class_name))

        # Generate imports dictionary
        imports = {}
        for import_type in import_types:
            import_type_package = import_type.rsplit('.', 1)[0]
            import_type_type = pascal(import_type.split('.')[-1])
            import_type_package = import_type_package.replace('.', '/')
            namespace_path = namespace.replace('.', '/')

            if import_type_package:
                import_type_package = os.path.relpath(import_type_package, namespace_path).replace(os.sep, '/')
                if not import_type_package.startswith('.'):
                    import_type_package = f'./{import_type_package}'
                imports[import_type_type] = f'{import_type_package}/{import_type_type}'
            else:
                imports[import_type_type] = f'./{import_type_type}'

        # Generate class definition using template
        class_definition = process_template(
            "structuretojs/class_core.js.jinja",
            class_name=class_name,
            docstring=doc,
            fields=fields,
            static_fields=static_fields,
            imports=imports,
            is_abstract=is_abstract
        )

        if write_file:
            self.write_to_file(namespace, class_name, class_definition)

        self.generated_types[qualified_name] = 'class'
        self.generated_structure_types[qualified_name] = structure_schema
        return qualified_name

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str,
                     write_file: bool) -> str:
        """Generates a JavaScript enum from JSON Structure enum"""
        class_name = pascal(structure_schema.get('name', field_name + 'Enum'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_package, schema_namespace)
        qualified_name = self.get_qualified_name(namespace, class_name)

        if qualified_name in self.generated_types:
            return qualified_name

        enum_values = structure_schema.get('enum', [])
        symbols = [str(symbol) if not is_javascript_reserved_word(str(symbol)) else str(symbol) + "_" 
                  for symbol in enum_values]
        symbol_values = [str(val) for val in enum_values]

        doc = structure_schema.get('description', structure_schema.get('doc', f'A {class_name} enum.'))

        enum_definition = process_template(
            "structuretojs/enum_core.js.jinja",
            class_name=class_name,
            docstring=doc,
            symbols=symbols,
            symbol_values=symbol_values,
        )

        if write_file:
            self.write_to_file(namespace, class_name, enum_definition)

        self.generated_types[qualified_name] = 'enum'
        self.generated_structure_types[qualified_name] = structure_schema
        return qualified_name

    def generate_choice(self, structure_schema: Dict, parent_namespace: str,
                       write_file: bool, explicit_name: str = '', import_types: Optional[Set[str]] = None) -> str:
        """Generates a JavaScript Union type from JSON Structure choice"""
        if import_types is None:
            import_types = set()

        # Generate types for each choice
        choice_types = []
        choices = structure_schema.get('choices', {})

        for choice_key, choice_schema in choices.items():
            if isinstance(choice_schema, dict):
                if '$ref' in choice_schema:
                    ref_schema = self.resolve_ref(choice_schema['$ref'], self.schema_doc)
                    if ref_schema:
                        ref_path = choice_schema['$ref'].split('/')
                        ref_name = ref_path[-1]
                        ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                        qualified_name = self.generate_class(ref_schema, ref_namespace, write_file=True, explicit_name=ref_name)
                        import_types.add(qualified_name)
                        choice_types.append(qualified_name.split('.')[-1])
                elif 'type' in choice_schema:
                    js_type = self.convert_structure_type_to_javascript(
                        explicit_name, choice_key, choice_schema, parent_namespace, import_types)
                    choice_types.append(js_type)

        # Return union type string
        if len(choice_types) == 0:
            return 'any'
        elif len(choice_types) == 1:
            return choice_types[0]
        else:
            return '|'.join(choice_types)

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str,
                      write_file: bool, explicit_name: str = '', import_types: Optional[Set[str]] = None) -> str:
        """Generates a JavaScript Array type for tuples"""
        if import_types is None:
            import_types = set()

        # Tuples in JavaScript are represented as arrays
        # For now, return Array<any> as tuples need special handling
        return 'Array<any>'

    def format_const_value(self, value: Any) -> str:
        """Formats a const value for JavaScript"""
        if value is None:
            return 'null'
        elif isinstance(value, bool):
            return 'true' if value else 'false'
        elif isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, list):
            items = ', '.join([self.format_const_value(v) for v in value])
            return f'[{items}]'
        elif isinstance(value, dict):
            items = ', '.join([f'"{k}": {self.format_const_value(v)}' for k, v in value.items()])
            return f'{{{items}}}'
        return 'null'

    def format_default_value(self, value: Any, js_type: str) -> str:
        """Formats a default value for JavaScript"""
        return self.format_const_value(value)

    def write_to_file(self, namespace: str, name: str, content: str):
        """Writes JavaScript class to file"""
        directory_path = os.path.join(self.output_dir, namespace.replace('.', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)

        file_path = os.path.join(directory_path, f"{name}.js")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """Processes the definitions section recursively"""
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition:
                    current_namespace = self.concat_namespace(namespace_path, '')
                    check_namespace = self.concat_namespace(self.base_package, current_namespace)
                    check_name = pascal(name)
                    check_ref = self.get_qualified_name(check_namespace, check_name)
                    if check_ref not in self.generated_types:
                        self.generate_class_or_choice(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    new_namespace = self.concat_namespace(namespace_path, name)
                    self.process_definitions(definition, new_namespace)

    def convert_schemas(self, structure_schemas: List, output_dir: str):
        """Converts JSON Structure schemas to JavaScript classes"""
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

        # Register all schema IDs first
        for structure_schema in structure_schemas:
            if isinstance(structure_schema, dict):
                self.register_schema_ids(structure_schema)

        for structure_schema in structure_schemas:
            if not isinstance(structure_schema, dict):
                continue

            self.schema_doc = structure_schema

            if 'definitions' in structure_schema:
                self.definitions = structure_schema['definitions']

            # Process root type first
            if 'type' in structure_schema:
                self.generate_class_or_choice(structure_schema, '', write_file=True)
            elif '$root' in structure_schema:
                root_ref = structure_schema['$root']
                root_schema = self.resolve_ref(root_ref, structure_schema)
                if root_schema:
                    ref_path = root_ref.split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else ''
                    self.generate_class_or_choice(root_schema, ref_namespace, write_file=True, explicit_name=type_name)

            # Process definitions
            if 'definitions' in structure_schema:
                self.process_definitions(self.definitions, '')

    def convert(self, structure_schema_path: str, output_dir: str):
        """Converts JSON Structure schema to JavaScript classes"""
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        if isinstance(schema, dict):
            schema = [schema]
        return self.convert_schemas(schema, output_dir)


def convert_structure_to_javascript(structure_schema_path, js_file_path, package_name='', avro_annotation=False):
    """Converts JSON Structure schema to JavaScript classes"""
    if not package_name:
        package_name = os.path.splitext(os.path.basename(structure_schema_path))[0].lower().replace('-', '_')

    structure_to_js = StructureToJavaScript(package_name, avro_annotation=avro_annotation)
    structure_to_js.convert(structure_schema_path, js_file_path)


def convert_structure_schema_to_javascript(structure_schema, js_file_path, package_name='', avro_annotation=False):
    """Converts JSON Structure schema to JavaScript classes"""
    structure_to_js = StructureToJavaScript(package_name, avro_annotation=avro_annotation)
    if isinstance(structure_schema, dict):
        structure_schema = [structure_schema]
    structure_to_js.convert_schemas(structure_schema, js_file_path)
