# pylint: disable=line-too-long

""" StructureToTypeScript class for converting JSON Structure schema to TypeScript classes """

import json
import os
import random
import re
from typing import Any, Dict, List, Set, Tuple, Union, Optional

from avrotize.common import pascal, process_template

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

INDENT = '    '


def is_typescript_reserved_word(word: str) -> bool:
    """Checks if a word is a TypeScript reserved word"""
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
    """ Converts JSON Structure schema to TypeScript classes """

    def __init__(self, base_package: str = '', typedjson_annotation=False, avro_annotation=False) -> None:
        self.base_package = base_package
        self.typedjson_annotation = typedjson_annotation
        self.avro_annotation = avro_annotation
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

    def map_primitive_to_typescript(self, structure_type: str) -> str:
        """ Maps JSON Structure primitive types to TypeScript types """
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
            'decimal': 'string',
            'binary': 'string',
            'bytes': 'string',
            'date': 'Date',
            'time': 'Date',
            'datetime': 'Date',
            'timestamp': 'Date',
            'duration': 'string',
            'uuid': 'string',
            'uri': 'string',
            'jsonpointer': 'string',
            'any': 'any'
        }
        qualified_class_name = self.get_qualified_name(
            self.base_package.lower(), structure_type.lower())
        if qualified_class_name in self.generated_types:
            result = qualified_class_name
        else:
            result = mapping.get(structure_type, 'any')
        return result

    def is_typescript_primitive(self, type_name: str) -> bool:
        """ Checks if a type is a TypeScript primitive type """
        return type_name in ['null', 'boolean', 'number', 'bigint', 'string', 'Date', 'any']

    def safe_name(self, name: str) -> str:
        """Converts a name to a safe TypeScript name"""
        if is_typescript_reserved_word(name):
            return name + "_"
        return name

    def pascal_type_name(self, ref: str) -> str:
        """Converts a reference to a type name"""
        return '_'.join([pascal(part) for part in ref.split('.')[-1].split('_')])

    def typescript_package_from_structure_type(self, namespace: str, type_name: str) -> str:
        """Gets the TypeScript package from a type name"""
        if '.' in type_name:
            # Type name contains dots, use it as package path
            type_name_package = '.'.join([part.lower() for part in type_name.split('.')])
            package = type_name_package
        else:
            # Use namespace as package, don't add type name to package
            namespace_package = '.'.join([part.lower() for part in namespace.split('.')]) if namespace else ''
            package = namespace_package
        if self.base_package:
            package = self.base_package + ('.' if package else '') + package
        return package

    def typescript_type_from_structure_type(self, type_name: str) -> str:
        """Gets the TypeScript class from a type name"""
        return self.pascal_type_name(type_name)

    def typescript_fully_qualified_name_from_structure_type(self, namespace: str, type_name: str) -> str:
        """Gets the fully qualified TypeScript class name from a Structure type."""
        package = self.typescript_package_from_structure_type(namespace, type_name)
        return package + ('.' if package else '') + self.typescript_type_from_structure_type(type_name)

    def strip_package_from_fully_qualified_name(self, fully_qualified_name: str) -> str:
        """Strips the package from a fully qualified name"""
        return fully_qualified_name.split('.')[-1]

    def strip_nullable(self, ts_type: str) -> str:
        """Strip nullable marker from TypeScript type"""
        if ts_type.endswith(' | null') or ts_type.endswith('| null'):
            return ts_type.replace(' | null', '').replace('| null', '')
        if ts_type.endswith('| undefined') or ts_type.endswith(' | undefined'):
            return ts_type.replace(' | undefined', '').replace('| undefined', '')
        return ts_type

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

    def convert_structure_type_to_typescript(self, class_name: str, field_name: str, 
                                            structure_type: JsonNode, parent_namespace: str, 
                                            import_types: Set[str]) -> str:
        """ Converts JSON Structure type to TypeScript type """
        if isinstance(structure_type, str):
            ts_type = self.map_primitive_to_typescript(structure_type)
            return ts_type
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            if len(non_null_types) == 1:
                inner_type = self.convert_structure_type_to_typescript(
                    class_name, field_name, non_null_types[0], parent_namespace, import_types)
                if 'null' in structure_type:
                    return f'{inner_type} | null'
                return inner_type
            else:
                union_types = [self.convert_structure_type_to_typescript(
                    class_name, field_name, t, parent_namespace, import_types) for t in non_null_types]
                result = ' | '.join(union_types)
                if 'null' in structure_type:
                    result += ' | null'
                return result
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
                    return self.strip_package_from_fully_qualified_name(ref)
                return 'any'

            # Handle enum keyword
            if 'enum' in structure_type:
                enum_ref = self.generate_enum(structure_type, field_name, parent_namespace, write_file=True)
                import_types.add(enum_ref)
                return self.strip_package_from_fully_qualified_name(enum_ref)

            # Handle type keyword
            if 'type' not in structure_type:
                return 'any'

            struct_type = structure_type['type']

            # Handle complex types
            if struct_type == 'object':
                class_ref = self.generate_class(structure_type, parent_namespace, write_file=True)
                import_types.add(class_ref)
                return self.strip_package_from_fully_qualified_name(class_ref)
            elif struct_type == 'array':
                items_type = self.convert_structure_type_to_typescript(
                    class_name, field_name+'Array', structure_type.get('items', {'type': 'any'}), 
                    parent_namespace, import_types)
                return f"{items_type}[]"
            elif struct_type == 'set':
                items_type = self.convert_structure_type_to_typescript(
                    class_name, field_name+'Set', structure_type.get('items', {'type': 'any'}), 
                    parent_namespace, import_types)
                return f"Set<{items_type}>"
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_typescript(
                    class_name, field_name+'Map', structure_type.get('values', {'type': 'any'}), 
                    parent_namespace, import_types)
                return f"{{ [key: string]: {values_type} }}"
            elif struct_type == 'choice':
                # Generate choice returns a Union type and populates import_types with the choice types
                return self.generate_choice(structure_type, parent_namespace, write_file=True, import_types=import_types)
            elif struct_type == 'tuple':
                tuple_ref = self.generate_tuple(structure_type, parent_namespace, write_file=True)
                import_types.add(tuple_ref)
                return self.strip_package_from_fully_qualified_name(tuple_ref)
            else:
                return self.convert_structure_type_to_typescript(class_name, field_name, struct_type, parent_namespace, import_types)
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
        """ Generates a TypeScript class/interface from JSON Structure object type """
        import_types: Set[str] = set()

        # Get name and namespace
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_package, schema_namespace).lower()
        typescript_qualified_name = self.typescript_fully_qualified_name_from_structure_type(schema_namespace, class_name)
        
        if typescript_qualified_name in self.generated_types:
            return typescript_qualified_name

        # Check if this is an abstract type
        is_abstract = structure_schema.get('abstract', False)

        # Handle inheritance ($extends)
        base_class = None
        if '$extends' in structure_schema:
            base_ref = structure_schema['$extends']
            if isinstance(self.schema_doc, dict):
                base_schema = self.resolve_ref(base_ref, self.schema_doc)
                if base_schema:
                    base_class_ref = self.generate_class(base_schema, parent_namespace, write_file=True)
                    base_class = self.strip_package_from_fully_qualified_name(base_class_ref)
                    import_types.add(base_class_ref)

        # Collect properties
        properties = structure_schema.get('properties', {})
        required_props = set(structure_schema.get('required', []))

        # Handle add-ins ($uses)
        if '$uses' in structure_schema and isinstance(structure_schema['$uses'], list):
            for addin_ref in structure_schema['$uses']:
                if isinstance(addin_ref, str):
                    # Resolve the add-in reference
                    addin_schema = self.resolve_ref(addin_ref, self.schema_doc)
                    if addin_schema and 'properties' in addin_schema:
                        properties.update(addin_schema['properties'])
                        if 'required' in addin_schema:
                            required_props.update(addin_schema['required'])

        # Generate fields
        fields = []
        for prop_name, prop_schema in properties.items():
            field_type = self.convert_structure_type_to_typescript(
                class_name, prop_name, prop_schema, namespace, import_types)
            is_required = prop_name in required_props
            is_optional = not is_required
            
            fields.append({
                'name': self.safe_name(prop_name),
                'original_name': prop_name,
                'type': field_type,
                'type_no_null': self.strip_nullable(field_type),
                'is_required': is_required,
                'is_optional': is_optional,
                'is_primitive': self.is_typescript_primitive(self.strip_nullable(field_type).replace('[]', '')),
                'docstring': prop_schema.get('description', '') if isinstance(prop_schema, dict) else ''
            })

        # Build imports
        imports_with_paths: Dict[str, str] = {}
        for import_type in import_types:
            if import_type == typescript_qualified_name:
                continue
            import_is_enum = import_type in self.generated_types and self.generated_types[import_type] == 'enum'
            import_type_parts = import_type.split('.')
            import_type_name = pascal(import_type_parts[-1])
            import_path = '/'.join(import_type_parts)
            current_path = '/'.join(namespace.split('.'))
            relative_import_path = os.path.relpath(import_path, current_path).replace(os.sep, '/')
            if not relative_import_path.startswith('.'):
                relative_import_path = f'./{relative_import_path}'
            imports_with_paths[import_type_name] = relative_import_path + '.js'

        # Generate class definition using template
        class_definition = process_template(
            "structuretots/class_core.ts.jinja",
            namespace=namespace,
            class_name=class_name,
            base_class=base_class,
            is_abstract=is_abstract,
            docstring=structure_schema.get('description', '').strip() if 'description' in structure_schema else f'A {class_name} class.',
            fields=fields,
            imports=imports_with_paths,
            typedjson_annotation=self.typedjson_annotation,
        )

        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
            # Generate test class
            if not is_abstract:  # Don't generate tests for abstract classes
                self.generate_test_class(namespace, class_name, fields)
        self.generated_types[typescript_qualified_name] = 'class'
        return typescript_qualified_name

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str, 
                     write_file: bool = True) -> str:
        """ Generates a TypeScript enum from JSON Structure enum """
        enum_name = pascal(structure_schema.get('name', field_name + 'Enum'))
        namespace = self.concat_namespace(self.base_package, structure_schema.get('namespace', parent_namespace)).lower()
        typescript_qualified_name = self.typescript_fully_qualified_name_from_structure_type(parent_namespace, enum_name)
        
        if typescript_qualified_name in self.generated_types:
            return typescript_qualified_name

        symbols = structure_schema.get('enum', [])
        
        enum_definition = process_template(
            "structuretots/enum_core.ts.jinja",
            namespace=namespace,
            enum_name=enum_name,
            docstring=structure_schema.get('description', '').strip() if 'description' in structure_schema else f'A {enum_name} enum.',
            symbols=symbols,
        )

        if write_file:
            self.write_to_file(namespace, enum_name, enum_definition)
        self.generated_types[typescript_qualified_name] = 'enum'
        return typescript_qualified_name

    def generate_choice(self, structure_schema: Dict, parent_namespace: str, 
                       write_file: bool = True, explicit_name: str = '', 
                       import_types: Optional[Set[str]] = None) -> str:
        """ Generates a TypeScript union type from JSON Structure choice type """
        if import_types is None:
            import_types = set()
        
        choice_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'Choice'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        
        # If the choice extends a base class, generate the base class first
        if '$extends' in structure_schema:
            base_ref = structure_schema['$extends']
            if isinstance(self.schema_doc, dict):
                base_schema = self.resolve_ref(base_ref, self.schema_doc)
                if base_schema:
                    # Generate the base class
                    ref_path = base_ref.split('/')
                    base_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    self.generate_class(base_schema, ref_namespace, write_file=True, explicit_name=base_name)
        
        # Generate types for each choice
        choice_types = []
        choices = structure_schema.get('choices', {})
        
        for choice_key, choice_schema in choices.items():
            if isinstance(choice_schema, dict):
                if '$ref' in choice_schema:
                    # Resolve reference and generate the type
                    ref_schema = self.resolve_ref(choice_schema['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                    if ref_schema:
                        ref_path = choice_schema['$ref'].split('/')
                        ref_name = ref_path[-1]
                        ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                        qualified_name = self.generate_class(ref_schema, ref_namespace, write_file=True, explicit_name=ref_name)
                        import_types.add(qualified_name)
                        choice_types.append(qualified_name.split('.')[-1])
                elif 'type' in choice_schema:
                    # Generate inline type
                    ts_type = self.convert_structure_type_to_typescript(choice_name, choice_key, choice_schema, schema_namespace, import_types)
                    choice_types.append(ts_type)
        
        # Return Union type
        if len(choice_types) == 0:
            return 'any'
        elif len(choice_types) == 1:
            return choice_types[0]
        else:
            return ' | '.join(choice_types)

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str, 
                      write_file: bool = True, explicit_name: str = '') -> str:
        """ Generates a TypeScript tuple type from JSON Structure tuple type """
        tuple_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'Tuple'))
        namespace = self.concat_namespace(self.base_package, structure_schema.get('namespace', parent_namespace)).lower()
        typescript_qualified_name = self.typescript_fully_qualified_name_from_structure_type(parent_namespace, tuple_name)
        
        if typescript_qualified_name in self.generated_types:
            return typescript_qualified_name

        import_types: Set[str] = set()
        tuple_items = structure_schema.get('items', [])
        item_types = []
        for idx, item in enumerate(tuple_items):
            item_type = self.convert_structure_type_to_typescript(
                tuple_name, f'item{idx}', item, namespace, import_types)
            item_types.append(item_type)

        # TypeScript tuples are just arrays with fixed length and types
        tuple_type = f"[{', '.join(item_types)}]"
        
        # Generate type alias
        tuple_definition = f"export type {tuple_name} = {tuple_type};\n"

        if write_file:
            self.write_to_file(namespace, tuple_name, tuple_definition)
        self.generated_types[typescript_qualified_name] = 'tuple'
        return typescript_qualified_name

    def generate_test_value(self, field: Dict) -> str:
        """Generates a test value for a given field in TypeScript"""
        import random
        field_type = field['type_no_null']
        
        # Map TypeScript types to test values
        test_values = {
            'string': '"test_string"',
            'number': str(random.randint(1, 100)),
            'bigint': 'BigInt(123)',
            'boolean': str(random.choice(['true', 'false'])).lower(),
            'Date': 'new Date()',
            'any': '{ test: "data" }',
            'null': 'null'
        }
        
        # Handle arrays
        if field_type.endswith('[]'):
            inner_type = field_type[:-2]
            if inner_type in test_values:
                return f"[{test_values[inner_type]}]"
            else:
                # For custom types, create empty array
                return f"[]"
        
        # Handle Set
        if field_type.startswith('Set<'):
            inner_type = field_type[4:-1]
            if inner_type in test_values:
                return f"new Set([{test_values[inner_type]}])"
            else:
                return f"new Set()"
        
        # Handle maps (objects with string index signature)
        if field_type.startswith('{ [key: string]:'):
            return '{}'
        
        # Return test value or construct object for custom types
        return test_values.get(field_type, f'{{}} as {field_type}')

    def generate_test_class(self, namespace: str, class_name: str, fields: List[Dict[str, Any]]) -> None:
        """Generates a unit test class for a TypeScript class"""
        # Get only required fields for the test
        required_fields = [f for f in fields if f['is_required']]
        
        # Generate test values for required fields
        for field in required_fields:
            field['test_value'] = self.generate_test_value(field)
        
        # Determine relative path from test directory to src
        namespace_path = namespace.replace('.', '/') if namespace else ''
        if namespace_path:
            relative_path = f"{namespace_path}/{class_name}"
        else:
            relative_path = class_name
        
        test_class_definition = process_template(
            "structuretots/test_class.ts.jinja",
            class_name=class_name,
            required_fields=required_fields,
            relative_path=relative_path
        )
        
        # Write test file
        test_dir = os.path.join(self.output_dir, "test")
        os.makedirs(test_dir, exist_ok=True)
        
        test_file_path = os.path.join(test_dir, f"{class_name}.test.ts")
        with open(test_file_path, 'w', encoding='utf-8') as f:
            f.write(test_class_definition)

    def write_to_file(self, namespace: str, type_name: str, content: str) -> None:
        """ Writes generated content to a TypeScript file """
        namespace_path = namespace.replace('.', '/')
        file_dir = os.path.join(self.output_dir, 'src', namespace_path)
        os.makedirs(file_dir, exist_ok=True)
        
        file_path = os.path.join(file_dir, f'{type_name}.ts')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

    def generate_package_json(self, package_name: str) -> None:
        """ Generates package.json file """
        package_json = process_template(
            "structuretots/package.json.jinja",
            package_name=package_name or 'generated-types',
        )
        
        with open(os.path.join(self.output_dir, 'package.json'), 'w', encoding='utf-8') as f:
            f.write(package_json)

    def generate_tsconfig(self) -> None:
        """ Generates tsconfig.json file """
        tsconfig = process_template("structuretots/tsconfig.json.jinja")
        with open(os.path.join(self.output_dir, 'tsconfig.json'), 'w', encoding='utf-8') as f:
            f.write(tsconfig)

    def generate_gitignore(self) -> None:
        """ Generates .gitignore file """
        gitignore = process_template("structuretots/gitignore.jinja")
        with open(os.path.join(self.output_dir, '.gitignore'), 'w', encoding='utf-8') as f:
            f.write(gitignore)

    def generate_index(self) -> None:
        """ Generates index.ts that exports all generated types """
        exports = []
        for qualified_name, type_kind in self.generated_types.items():
            type_name = qualified_name.split('.')[-1]
            namespace = '.'.join(qualified_name.split('.')[:-1])
            if namespace:
                # Lowercase the namespace to match the directory structure created by write_to_file
                relative_path = namespace.lower().replace('.', '/') + '/' + type_name
            else:
                relative_path = type_name
            exports.append(f"export * from './{relative_path}.js';")
        
        index_content = '\n'.join(exports) + '\n' if exports else ''
        
        src_dir = os.path.join(self.output_dir, 'src')
        os.makedirs(src_dir, exist_ok=True)
        with open(os.path.join(src_dir, 'index.ts'), 'w', encoding='utf-8') as f:
            f.write(index_content)

    def convert(self, structure_schema_path: str, output_dir: str, package_name: str = '') -> None:
        """ Converts a JSON Structure schema file to TypeScript classes """
        self.output_dir = output_dir
        
        # Load schema
        with open(structure_schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        
        self.convert_schema(schema, output_dir, package_name)

    def convert_schema(self, schema: JsonNode, output_dir: str, package_name: str = '') -> None:
        """ Converts a JSON Structure schema to TypeScript classes """
        self.output_dir = output_dir
        self.schema_doc = schema
        
        # Register schema IDs
        self.register_schema_ids(self.schema_doc)
        
        # Process definitions
        if 'definitions' in self.schema_doc:
            for def_name, def_schema in self.schema_doc['definitions'].items():
                if isinstance(def_schema, dict):
                    self.generate_class_or_choice(def_schema, '', write_file=True, explicit_name=def_name)
        
        # Process root schema if it's an object or choice
        if 'type' in self.schema_doc:
            root_namespace = self.schema_doc.get('namespace', '')
            self.generate_class_or_choice(self.schema_doc, root_namespace, write_file=True)
        
        # Generate project files
        self.generate_package_json(package_name)
        self.generate_tsconfig()
        self.generate_gitignore()
        self.generate_index()


def convert_structure_to_typescript(structure_schema_path: str, ts_file_path: str, 
                                   package_name: str = '', typedjson_annotation: bool = False, 
                                   avro_annotation: bool = False) -> None:
    """
    Converts a JSON Structure schema to TypeScript classes.
    
    Args:
        structure_schema_path: Path to the JSON Structure schema file
        ts_file_path: Output directory for TypeScript files
        package_name: Package name for the generated TypeScript project
        typedjson_annotation: Whether to include TypedJSON annotations
        avro_annotation: Whether to include Avro annotations
    """
    converter = StructureToTypeScript(package_name, typedjson_annotation, avro_annotation)
    converter.convert(structure_schema_path, ts_file_path, package_name)


def convert_structure_schema_to_typescript(structure_schema: JsonNode, output_dir: str, 
                                          package_name: str = '', typedjson_annotation: bool = False, 
                                          avro_annotation: bool = False) -> None:
    """
    Converts a JSON Structure schema to TypeScript classes.
    
    Args:
        structure_schema: JSON Structure schema to convert
        output_dir: Output directory for TypeScript files
        package_name: Package name for the generated TypeScript project
        typedjson_annotation: Whether to include TypedJSON annotations
        avro_annotation: Whether to include Avro annotations
    """
    converter = StructureToTypeScript(package_name, typedjson_annotation, avro_annotation)
    converter.convert_schema(structure_schema, output_dir, package_name)
