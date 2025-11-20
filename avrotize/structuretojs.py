# pylint: disable=line-too-long

""" StructureToJavaScript class for converting JSON Structure schema to JavaScript classes """

import json
import os
from typing import Any, Dict, List, Set, Union, Optional

from avrotize.common import pascal

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

INDENT = '    '


def is_javascript_reserved_word(word: str) -> bool:
    """ Check if word is a JavaScript reserved word """
    reserved_words = [
        'break', 'case', 'catch', 'class', 'const', 'continue', 'debugger',
        'default', 'delete', 'do', 'else', 'export', 'extends', 'finally',
        'for', 'function', 'if', 'import', 'in', 'instanceof', 'new', 'return',
        'super', 'switch', 'this', 'throw', 'try', 'typeof', 'var', 'void',
        'while', 'with', 'yield', 'let', 'static', 'enum', 'await', 'implements',
        'interface', 'package', 'private', 'protected', 'public', 'arguments', 'eval'
    ]
    return word in reserved_words


def is_javascript_primitive(word: str) -> bool:
    """ Check if word is a JavaScript primitive """
    primitives = ['null', 'boolean', 'number', 'string', 'Date', 'Buffer', 'Array', 'Object', 'Map', 'Set']
    return word in primitives


class StructureToJavaScript:
    """ Converts JSON Structure schema to JavaScript classes """

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package
        self.output_dir = os.getcwd()
        self.schema_doc: JsonNode = None
        self.generated_types: Dict[str, str] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}
        self.type_dict: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}
        self.schema_registry: Dict[str, Dict] = {}

    def get_qualified_name(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        return f"{namespace}.{name}" if namespace != '' else name

    def concat_namespace(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        if namespace and name:
            return f"{namespace}.{name}"
        elif namespace:
            return namespace
        else:
            return name

    def map_primitive_to_javascript(self, structure_type: str) -> str:
        """ Maps JSON Structure primitive types to JavaScript types """
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
            'int128': 'number',
            'uint128': 'number',
            'float8': 'number',
            'float': 'number',
            'double': 'number',
            'binary32': 'number',
            'binary64': 'number',
            'decimal': 'number',
            'binary': 'Buffer',
            'date': 'Date',
            'time': 'Date',
            'datetime': 'Date',
            'timestamp': 'Date',
            'duration': 'number',
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
        """ Checks if a type is a JavaScript primitive type """
        return js_type in ['null', 'boolean', 'number', 'string', 'Date', 'Buffer', 'any']

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """ Resolves a $ref to the actual schema definition """
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
        """ Recursively registers schemas with $id keywords """
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
        """ Converts JSON Structure type to JavaScript type """
        if isinstance(structure_type, str):
            js_type = self.map_primitive_to_javascript(structure_type)
            if js_type == structure_type and not self.is_javascript_primitive_type(js_type):
                import_types.add(js_type)
                return pascal(js_type.split('.')[-1])
            return js_type
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            if len(non_null_types) == 1:
                return self.convert_structure_type_to_javascript(class_name, field_name, non_null_types[0], parent_namespace, import_types)
            else:
                # For unions, return 'any' for simplicity in JavaScript
                return 'any'
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
                    return pascal(ref.split('.')[-1])
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
                items_type = self.convert_structure_type_to_javascript(
                    class_name, field_name+'List', structure_type.get('items', {'type': 'any'}),
                    parent_namespace, import_types)
                return f"Array<{items_type}>"
            elif struct_type == 'set':
                items_type = self.convert_structure_type_to_javascript(
                    class_name, field_name+'Set', structure_type.get('items', {'type': 'any'}),
                    parent_namespace, import_types)
                return f"Set<{items_type}>"
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_javascript(
                    class_name, field_name+'Map', structure_type.get('values', {'type': 'any'}),
                    parent_namespace, import_types)
                return f"Map<string, {values_type}>"
            elif struct_type == 'choice':
                return self.generate_choice(structure_type, parent_namespace, write_file=True, import_types=import_types)
            elif struct_type == 'tuple':
                return self.generate_tuple(structure_type, parent_namespace, write_file=True, import_types=import_types)
            else:
                return self.convert_structure_type_to_javascript(class_name, field_name, struct_type, parent_namespace, import_types)
        return 'any'

    def generate_class_or_choice(self, structure_schema: Dict, parent_namespace: str,
                                 write_file: bool = True, explicit_name: str = '') -> str:
        """ Generates a Class or Choice """
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
        """ Generates a JavaScript class from JSON Structure object type """
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

        constructor_body = ''
        class_body = ''

        for prop_name, prop_schema in properties.items():
            field_name = prop_name
            if is_javascript_reserved_word(field_name):
                field_name += '_'

            # Check if this is a const field
            if 'const' in prop_schema:
                # Const fields are static properties
                const_value = self.format_default_value(prop_schema['const'])
                class_body += f'{class_name}.{field_name} = {const_value};\n\n'
                continue

            field_type = self.convert_structure_type_to_javascript(
                class_name, field_name, prop_schema, schema_namespace, import_types)

            # Determine if required
            is_required = prop_name in required_props if not isinstance(required_props, list) or \
                         len(required_props) == 0 or not isinstance(required_props[0], list) else \
                         any(prop_name in req_set for req_set in required_props)

            # Add field documentation
            field_doc = prop_schema.get('description', prop_schema.get('doc', ''))
            if field_doc:
                constructor_body += f'{INDENT}/** {field_doc} */\n'

            # Initialize field
            constructor_body += f'{INDENT}this._{field_name} = null;\n'

            # Generate getter and setter
            class_body += f'Object.defineProperty({class_name}.prototype, "{field_name}", {{\n'
            class_body += f'{INDENT}get: function() {{\n'
            class_body += f'{INDENT}{INDENT}return this._{field_name};\n'
            class_body += f'{INDENT}}},\n'
            class_body += f'{INDENT}set: function(value) {{\n'

            # Add type validation in setter
            if is_required and field_type != 'any':
                class_body += self.generate_type_validation(field_name, field_type, is_required)

            class_body += f'{INDENT}{INDENT}this._{field_name} = value;\n'
            class_body += f'{INDENT}}}\n'
            class_body += '});\n\n'

        # Generate imports
        imports = ''
        for import_type in import_types:
            import_type_package = import_type.rsplit('.', 1)[0] if '.' in import_type else ''
            import_type_type = pascal(import_type.split('.')[-1])
            if import_type_package:
                import_type_package = import_type_package.replace('.', '/')
                namespace_path = namespace.replace('.', '/')
                if import_type_package:
                    import_path = os.path.relpath(import_type_package, namespace_path).replace(os.sep, '/')
                    if not import_path.startswith('.'):
                        import_path = f'./{import_path}'
                    imports += f"const {import_type_type} = require('{import_path}/{import_type_type}');\n"
            else:
                imports += f"const {import_type_type} = require('./{import_type_type}');\n"

        # Get class documentation
        doc = structure_schema.get('description', structure_schema.get('doc', class_name))

        # Generate class definition
        class_definition = imports
        if imports:
            class_definition += '\n'
        if doc:
            class_definition += f'/** {doc} */\n'
        class_definition += f"function {class_name}() {{\n{constructor_body}}}\n\n{class_body}\nmodule.exports = {class_name};\n"

        if write_file:
            self.write_to_file(namespace, class_name, class_definition)

        self.generated_types[qualified_name] = 'class'
        self.generated_structure_types[qualified_name] = structure_schema
        return qualified_name

    def generate_type_validation(self, field_name: str, field_type: str, is_required: bool) -> str:
        """ Generates type validation code for a field """
        validation = ''
        
        if is_required:
            validation += f'{INDENT}{INDENT}if (value === null || value === undefined) {{\n'
            validation += f'{INDENT}{INDENT}{INDENT}throw new Error(`{field_name} is required`);\n'
            validation += f'{INDENT}{INDENT}}}\n'

        # Type-specific validation
        if field_type == 'string':
            validation += f'{INDENT}{INDENT}if (typeof value !== "string") {{\n'
            validation += f'{INDENT}{INDENT}{INDENT}throw new Error(`Invalid type for {field_name}. Expected string, got ${{typeof value}}`);\n'
            validation += f'{INDENT}{INDENT}}}\n'
        elif field_type == 'number':
            validation += f'{INDENT}{INDENT}if (typeof value !== "number") {{\n'
            validation += f'{INDENT}{INDENT}{INDENT}throw new Error(`Invalid type for {field_name}. Expected number, got ${{typeof value}}`);\n'
            validation += f'{INDENT}{INDENT}}}\n'
        elif field_type == 'boolean':
            validation += f'{INDENT}{INDENT}if (typeof value !== "boolean") {{\n'
            validation += f'{INDENT}{INDENT}{INDENT}throw new Error(`Invalid type for {field_name}. Expected boolean, got ${{typeof value}}`);\n'
            validation += f'{INDENT}{INDENT}}}\n'
        elif field_type == 'Date':
            validation += f'{INDENT}{INDENT}if (!(value instanceof Date)) {{\n'
            validation += f'{INDENT}{INDENT}{INDENT}throw new Error(`Invalid type for {field_name}. Expected Date, got ${{typeof value}}`);\n'
            validation += f'{INDENT}{INDENT}}}\n'
        elif field_type.startswith('Array<'):
            validation += f'{INDENT}{INDENT}if (!Array.isArray(value)) {{\n'
            validation += f'{INDENT}{INDENT}{INDENT}throw new Error(`Invalid type for {field_name}. Expected Array, got ${{typeof value}}`);\n'
            validation += f'{INDENT}{INDENT}}}\n'
        elif field_type.startswith('Map<'):
            validation += f'{INDENT}{INDENT}if (!(value instanceof Map)) {{\n'
            validation += f'{INDENT}{INDENT}{INDENT}throw new Error(`Invalid type for {field_name}. Expected Map, got ${{typeof value}}`);\n'
            validation += f'{INDENT}{INDENT}}}\n'
        elif field_type.startswith('Set<'):
            validation += f'{INDENT}{INDENT}if (!(value instanceof Set)) {{\n'
            validation += f'{INDENT}{INDENT}{INDENT}throw new Error(`Invalid type for {field_name}. Expected Set, got ${{typeof value}}`);\n'
            validation += f'{INDENT}{INDENT}}}\n'
        elif not self.is_javascript_primitive_type(field_type):
            # Custom class type
            validation += f'{INDENT}{INDENT}if (!(value instanceof {field_type})) {{\n'
            validation += f'{INDENT}{INDENT}{INDENT}throw new Error(`Invalid type for {field_name}. Expected {field_type}, got ${{typeof value}}`);\n'
            validation += f'{INDENT}{INDENT}}}\n'

        return validation

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str,
                     write_file: bool) -> str:
        """ Generates a JavaScript enum from JSON Structure enum """
        enum_name = pascal(structure_schema.get('name', field_name + 'Enum'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_package, schema_namespace)
        qualified_name = self.get_qualified_name(namespace, enum_name)

        if qualified_name in self.generated_types:
            return qualified_name

        symbols = structure_schema.get('enum', [])
        if not symbols:
            return 'any'

        # Generate enum body
        enum_body = ''
        for symbol in symbols:
            symbol_name = str(symbol)
            if is_javascript_reserved_word(symbol_name):
                symbol_name += '_'
            # For string enums, use the string value
            if isinstance(symbol, str):
                enum_body += f'{INDENT}{symbol_name}: "{symbol}",\n'
            else:
                # For numeric enums, use the numeric value
                enum_body += f'{INDENT}{symbol_name}: {symbol},\n'

        # Get documentation
        doc = structure_schema.get('description', structure_schema.get('doc', enum_name))

        # Generate enum definition
        enum_definition = ''
        if doc:
            enum_definition += f"/** {doc} */\n"
        enum_definition += f"const {enum_name} = Object.freeze({{\n{enum_body}}});\n\n"
        enum_definition += f"module.exports = {enum_name};\n"

        if write_file:
            self.write_to_file(namespace, enum_name, enum_definition)

        self.generated_types[qualified_name] = 'enum'
        self.generated_structure_types[qualified_name] = structure_schema
        return qualified_name

    def generate_choice(self, structure_schema: Dict, parent_namespace: str,
                       write_file: bool, explicit_name: str = '', import_types: Optional[Set[str]] = None) -> str:
        """ Generates a JavaScript union type from JSON Structure choice """
        # For JavaScript, we return 'any' for union types since JS doesn't have discriminated unions
        # but we still generate the choice classes for reference
        choice_name = explicit_name if explicit_name else structure_schema.get('name', 'UnnamedChoice')
        schema_namespace = structure_schema.get('namespace', parent_namespace)
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
                        choice_types.append(pascal(qualified_name.split('.')[-1]))
                elif 'type' in choice_schema:
                    js_type = self.convert_structure_type_to_javascript(choice_name, choice_key, choice_schema, schema_namespace, import_types)
                    choice_types.append(js_type)

        # For JavaScript, return 'any' since we don't have strong typing
        return 'any'

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str,
                      write_file: bool, explicit_name: str = '', import_types: Optional[Set[str]] = None) -> str:
        """ Generates a JavaScript tuple type from JSON Structure tuple """
        # JavaScript doesn't have native tuples, so we use arrays
        tuple_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedTuple'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_package, schema_namespace)
        qualified_name = self.get_qualified_name(namespace, tuple_name)

        if qualified_name in self.generated_types:
            return qualified_name

        if import_types is None:
            import_types = set()

        properties = structure_schema.get('properties', {})
        tuple_order = structure_schema.get('tuple', [])

        # Build tuple constructor
        constructor_params = []
        constructor_body = ''
        for i, prop_name in enumerate(tuple_order):
            if prop_name in properties:
                field_name = prop_name
                if is_javascript_reserved_word(field_name):
                    field_name += '_'
                constructor_params.append(field_name)
                constructor_body += f'{INDENT}this.{field_name} = {field_name};\n'

        # Get documentation
        doc = structure_schema.get('description', structure_schema.get('doc', tuple_name))

        # Generate tuple class definition
        tuple_definition = ''
        if doc:
            tuple_definition += f"/** {doc} */\n"
        tuple_definition += f"function {tuple_name}({', '.join(constructor_params)}) {{\n"
        tuple_definition += constructor_body
        tuple_definition += "}\n\n"
        tuple_definition += f"module.exports = {tuple_name};\n"

        if write_file:
            self.write_to_file(namespace, tuple_name, tuple_definition)

        self.generated_types[qualified_name] = 'tuple'
        self.generated_structure_types[qualified_name] = structure_schema
        return qualified_name

    def format_default_value(self, value: Any) -> str:
        """ Formats a default value for JavaScript """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, list):
            return f"[{', '.join([self.format_default_value(v) for v in value])}]"
        elif isinstance(value, dict):
            items = [f'"{k}": {self.format_default_value(v)}' for k, v in value.items()]
            return f"{{{', '.join(items)}}}"
        return 'null'

    def write_to_file(self, namespace: str, name: str, content: str):
        """ Write JavaScript class to file """
        directory_path = os.path.join(self.output_dir, namespace.replace('.', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)

        file_path = os.path.join(directory_path, f"{name}.js")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """ Processes the definitions section recursively """
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition or 'enum' in definition:
                    # This is a type definition
                    current_namespace = self.concat_namespace(namespace_path, '')
                    check_namespace = self.concat_namespace(self.base_package, current_namespace)
                    check_name = pascal(name)
                    check_qualified = self.get_qualified_name(check_namespace, check_name)
                    if check_qualified not in self.generated_types:
                        if 'enum' in definition:
                            self.generate_enum(definition, name, current_namespace, write_file=True)
                        else:
                            self.generate_class_or_choice(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    # This is a namespace
                    new_namespace = self.concat_namespace(namespace_path, name)
                    self.process_definitions(definition, new_namespace)

    def convert_schema(self, schema: JsonNode, output_dir: str) -> None:
        """ Converts JSON Structure schema to JavaScript classes """
        if not isinstance(schema, list):
            schema = [schema]

        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

        # Register all schema IDs first
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.register_schema_ids(structure_schema)

        # Process each schema
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.schema_doc = structure_schema

            # Store definitions for later use
            if 'definitions' in structure_schema:
                self.definitions = structure_schema['definitions']

            # Process root type
            if 'enum' in structure_schema:
                self.generate_enum(structure_schema, structure_schema.get('name', 'Enum'),
                                 structure_schema.get('namespace', ''), write_file=True)
            elif 'type' in structure_schema:
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
        """ Convert JSON Structure schema file to JavaScript classes """
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_structure_to_javascript(structure_schema_path: str, js_dir_path: str, package_name: str = ''):
    """ Convert JSON Structure schema to JavaScript classes """
    if not package_name:
        package_name = os.path.splitext(os.path.basename(structure_schema_path))[0].replace('-', '_')

    converter = StructureToJavaScript(package_name)
    converter.convert(structure_schema_path, js_dir_path)


def convert_structure_schema_to_javascript(structure_schema: JsonNode, js_dir_path: str, package_name: str = ''):
    """ Convert JSON Structure schema object to JavaScript classes """
    converter = StructureToJavaScript(package_name)
    converter.convert_schema(structure_schema, js_dir_path)
