# pylint: disable=line-too-long

""" StructureToGo class for converting JSON Structure schema to Go structs """

import json
import os
from typing import Any, Dict, List, Set, Union, Optional, cast

from avrotize.common import pascal, render_template

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

INDENT = '    '


class StructureToGo:
    """ Converts JSON Structure schema to Go structs """

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package
        self.output_dir = os.getcwd()
        self.json_annotation = False
        self.avro_annotation = False
        self.package_site = 'github.com'
        self.package_username = 'username'
        self.schema_doc: JsonNode = None
        self.generated_types: Dict[str, str] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}
        self.definitions: Dict[str, Any] = {}
        self.schema_registry: Dict[str, Dict] = {}
        self.structs: List[Dict] = []
        self.enums: List[Dict] = []

    def safe_identifier(self, name: str) -> str:
        """Converts a name to a safe Go identifier"""
        reserved_words = [
            'break', 'default', 'func', 'interface', 'select', 'case', 'defer', 'go', 'map', 'struct', 'chan',
            'else', 'goto', 'package', 'switch', 'const', 'fallthrough', 'if', 'range', 'type', 'continue', 'for',
            'import', 'return', 'var',
        ]
        if name in reserved_words:
            return f"{name}_"
        return name

    def go_type_name(self, name: str, namespace: str = '') -> str:
        """Returns a qualified name for a Go struct or enum"""
        if namespace:
            namespace = ''.join([pascal(part) for part in namespace.split('.')])
            return f"{namespace}{pascal(name)}"
        return pascal(name)

    def map_primitive_to_go(self, structure_type: str, is_optional: bool) -> str:
        """Maps JSON Structure primitive types to Go types"""
        optional_mapping = {
            'null': 'interface{}',
            'boolean': '*bool',
            'string': '*string',
            'integer': '*int',
            'number': '*float64',
            'int8': '*int8',
            'uint8': '*uint8',
            'int16': '*int16',
            'uint16': '*uint16',
            'int32': '*int32',
            'uint32': '*uint32',
            'int64': '*int64',
            'uint64': '*uint64',
            'int128': '*big.Int',
            'uint128': '*big.Int',
            'float8': '*float32',
            'float': '*float32',
            'double': '*float64',
            'binary32': '*float32',
            'binary64': '*float64',
            'decimal': '*float64',
            'binary': '[]byte',
            'date': '*time.Time',
            'time': '*time.Time',
            'datetime': '*time.Time',
            'timestamp': '*time.Time',
            'duration': '*time.Duration',
            'uuid': '*string',
            'uri': '*string',
            'jsonpointer': '*string',
            'any': 'interface{}',
        }
        required_mapping = {
            'null': 'interface{}',
            'boolean': 'bool',
            'string': 'string',
            'integer': 'int',
            'number': 'float64',
            'int8': 'int8',
            'uint8': 'uint8',
            'int16': 'int16',
            'uint16': 'uint16',
            'int32': 'int32',
            'uint32': 'uint32',
            'int64': 'int64',
            'uint64': 'uint64',
            'int128': 'big.Int',
            'uint128': 'big.Int',
            'float8': 'float32',
            'float': 'float32',
            'double': 'float64',
            'binary32': 'float32',
            'binary64': 'float64',
            'decimal': 'float64',
            'binary': '[]byte',
            'date': 'time.Time',
            'time': 'time.Time',
            'datetime': 'time.Time',
            'timestamp': 'time.Time',
            'duration': 'time.Duration',
            'uuid': 'string',
            'uri': 'string',
            'jsonpointer': 'string',
            'any': 'interface{}',
        }
        if structure_type in self.generated_types:
            return structure_type
        else:
            return required_mapping.get(structure_type, 'interface{}') if not is_optional else optional_mapping.get(structure_type, 'interface{}')

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """ Resolves a $ref to the actual schema definition """
        # Check if it's an absolute URI reference
        if not ref.startswith('#/'):
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None

        # Handle fragment-only references
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

    def convert_structure_type_to_go(self, class_name: str, field_name: str, 
                                     structure_type: JsonNode, parent_namespace: str, 
                                     nullable: bool = False) -> str:
        """ Converts JSON Structure type to Go type """
        if isinstance(structure_type, str):
            return self.map_primitive_to_go(structure_type, nullable)
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            has_null = 'null' in structure_type
            if len(non_null_types) == 1:
                inner_type = self.convert_structure_type_to_go(
                    class_name, field_name, non_null_types[0], parent_namespace, has_null)
                return inner_type
            else:
                # Generate union type
                return self.generate_union_class(field_name, structure_type, parent_namespace)
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc)
                if ref_schema:
                    ref_path = structure_type['$ref'].split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    return self.generate_class_or_choice(ref_schema, ref_namespace, write_file=True, explicit_name=type_name)
                return 'interface{}'

            # Handle enum keyword
            if 'enum' in structure_type:
                return self.generate_enum(structure_type, field_name, parent_namespace, write_file=True)

            # Handle type keyword
            if 'type' not in structure_type:
                return 'interface{}'

            struct_type = structure_type['type']

            # Handle complex types
            if struct_type == 'object':
                return self.generate_class(structure_type, parent_namespace, write_file=True)
            elif struct_type == 'array':
                items_type = self.convert_structure_type_to_go(
                    class_name, field_name+'List', structure_type.get('items', {'type': 'any'}), 
                    parent_namespace, nullable=True)
                if items_type.startswith('*'):
                    return f"[]{items_type[1:]}"
                return f"[]{items_type}"
            elif struct_type == 'set':
                # Go doesn't have a built-in set, use map[T]bool
                items_type = self.convert_structure_type_to_go(
                    class_name, field_name+'Set', structure_type.get('items', {'type': 'any'}), 
                    parent_namespace, nullable=True)
                if items_type.startswith('*'):
                    items_type = items_type[1:]
                return f"map[{items_type}]bool"
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_go(
                    class_name, field_name+'Map', structure_type.get('values', {'type': 'any'}), 
                    parent_namespace, nullable=True)
                return f"map[string]{values_type}"
            elif struct_type == 'choice':
                return self.generate_choice(structure_type, parent_namespace, write_file=True)
            elif struct_type == 'tuple':
                return self.generate_tuple(structure_type, parent_namespace, write_file=True)
            else:
                return self.convert_structure_type_to_go(class_name, field_name, struct_type, parent_namespace, nullable)
        return 'interface{}'

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
        return 'interface{}'

    def generate_class(self, structure_schema: Dict, parent_namespace: str, 
                      write_file: bool, explicit_name: str = '') -> str:
        """ Generates a Go struct from JSON Structure object type """
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        go_struct_name = self.go_type_name(class_name, schema_namespace)

        if go_struct_name in self.generated_types:
            return go_struct_name

        # Check if this is an abstract type
        is_abstract = structure_schema.get('abstract', False)
        
        # If abstract, generate interface instead
        if is_abstract:
            return self.generate_interface(structure_schema, parent_namespace, write_file, explicit_name)

        self.generated_types[go_struct_name] = "struct"
        self.generated_structure_types[go_struct_name] = structure_schema

        # Handle inheritance ($extends)
        base_interface = None
        base_properties = {}
        base_required = []
        if '$extends' in structure_schema:
            base_ref = structure_schema['$extends']
            base_schema = self.resolve_ref(base_ref, self.schema_doc)
            if base_schema:
                ref_path = base_ref.split('/')
                base_name = ref_path[-1]
                ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                base_interface = self.generate_class(base_schema, ref_namespace, write_file=True, explicit_name=base_name)
                # Collect base properties to include in the concrete type
                base_properties = base_schema.get('properties', {})
                base_required = base_schema.get('required', [])

        # Generate properties - merge base properties with current properties
        properties = {**base_properties, **structure_schema.get('properties', {})}
        required_props = base_required + structure_schema.get('required', [])

        fields = []
        for prop_name, prop_schema in properties.items():
            is_required = prop_name in required_props if not isinstance(required_props, list) or \
                         len(required_props) == 0 or not isinstance(required_props[0], list) else \
                         any(prop_name in req_set for req_set in required_props)
            
            field_type = self.convert_structure_type_to_go(
                class_name, prop_name, prop_schema, schema_namespace, nullable=not is_required)
            
            # Add nullable marker if not required and not already nullable
            if not is_required and not field_type.startswith('*') and not field_type.startswith('[') and not field_type.startswith('map[') and field_type != 'interface{}':
                field_type = f'*{field_type}'
            
            fields.append({
                'name': pascal(prop_name),
                'type': field_type,
                'original_name': prop_name
            })

        # Get imports needed
        imports = self.get_imports_for_fields([f['type'] for f in fields])
        
        # Generate Avro schema if avro_annotation is enabled
        avro_schema_str = None
        if self.avro_annotation:
            try:
                from avrotize.jstructtoavro import JsonStructureToAvro
                converter = JsonStructureToAvro()
                avro_schema = converter.convert(structure_schema)
                avro_schema_str = json.dumps(avro_schema)
            except Exception as e:
                # If conversion fails, log but continue without Avro schema
                print(f"Warning: Failed to generate Avro schema for {go_struct_name}: {e}")

        context = {
            'doc': structure_schema.get('description', structure_schema.get('doc', class_name)),
            'struct_name': go_struct_name,
            'fields': fields,
            'imports': imports,
            'json_annotation': self.json_annotation,
            'avro_annotation': self.avro_annotation,
            'avro_schema': avro_schema_str,
            'base_package': self.base_package,
            'base_interface': base_interface,
            'referenced_packages': set(),
        }

        pkg_dir = os.path.join(self.output_dir, 'pkg', self.base_package)
        if not os.path.exists(pkg_dir):
            os.makedirs(pkg_dir, exist_ok=True)
        file_name = os.path.join(pkg_dir, f"{go_struct_name}.go")
        render_template('structuretogo/go_struct.jinja', file_name, **context)

        self.structs.append({
            'name': go_struct_name,
            'fields': fields,
        })

        self.generate_unit_test('struct', go_struct_name, fields)

        return go_struct_name

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str, 
                     write_file: bool) -> str:
        """ Generates a Go enum from JSON Structure enum """
        enum_name = pascal(structure_schema.get('name', field_name + 'Enum'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        go_enum_name = self.go_type_name(enum_name, schema_namespace)

        if go_enum_name in self.generated_types:
            return go_enum_name

        self.generated_types[go_enum_name] = "enum"
        self.generated_structure_types[go_enum_name] = structure_schema

        symbols = structure_schema.get('enum', [])
        
        # Determine base type
        base_type = structure_schema.get('type', 'string')
        go_base_type = self.map_primitive_to_go(base_type, False)

        context = {
            'doc': structure_schema.get('description', structure_schema.get('doc', enum_name)),
            'enum_name': go_enum_name,
            'symbols': symbols,
            'base_type': go_base_type,
            'base_package': self.base_package,
        }

        pkg_dir = os.path.join(self.output_dir, 'pkg', self.base_package)
        if not os.path.exists(pkg_dir):
            os.makedirs(pkg_dir, exist_ok=True)
        file_name = os.path.join(pkg_dir, f"{go_enum_name}.go")
        render_template('structuretogo/go_enum.jinja', file_name, **context)

        self.enums.append({
            'name': go_enum_name,
            'symbols': symbols,
        })

        self.generate_unit_test('enum', go_enum_name, symbols)

        return go_enum_name

    def generate_interface(self, structure_schema: Dict, parent_namespace: str,
                          write_file: bool, explicit_name: str = '') -> str:
        """ Generates a Go interface from JSON Structure abstract type """
        interface_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedInterface'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        go_interface_name = self.go_type_name(interface_name, schema_namespace)

        if go_interface_name in self.generated_types:
            return go_interface_name

        self.generated_types[go_interface_name] = "interface"
        self.generated_structure_types[go_interface_name] = structure_schema

        # Get properties to define getter methods
        properties = structure_schema.get('properties', {})
        required_props = structure_schema.get('required', [])
        
        methods = []
        for prop_name, prop_schema in properties.items():
            go_prop_name = pascal(prop_name)
            is_required = prop_name in required_props if not isinstance(required_props, list) or \
                         len(required_props) == 0 or not isinstance(required_props[0], list) else \
                         any(prop_name in req_set for req_set in required_props)
            
            field_type = self.convert_structure_type_to_go(
                interface_name, prop_name, prop_schema, schema_namespace, nullable=not is_required)
            
            # Add nullable marker if not required and not already nullable
            if not is_required and not field_type.startswith('*') and not field_type.startswith('[') and not field_type.startswith('map[') and field_type != 'interface{}':
                field_type = f'*{field_type}'
            
            # Generate getter method
            methods.append({
                'name': f'Get{go_prop_name}',
                'return_type': field_type,
                'doc': f'Get{go_prop_name} returns the {prop_name} field'
            })
            
            # Generate setter method
            methods.append({
                'name': f'Set{go_prop_name}',
                'return_type': '',
                'param_type': field_type,
                'param_name': prop_name,
                'doc': f'Set{go_prop_name} sets the {prop_name} field'
            })

        context = {
            'doc': structure_schema.get('description', structure_schema.get('doc', interface_name)),
            'interface_name': go_interface_name,
            'methods': methods,
            'base_package': self.base_package,
        }

        pkg_dir = os.path.join(self.output_dir, 'pkg', self.base_package)
        if not os.path.exists(pkg_dir):
            os.makedirs(pkg_dir, exist_ok=True)
        file_name = os.path.join(pkg_dir, f"{go_interface_name}.go")
        render_template('structuretogo/go_interface.jinja', file_name, **context)

        return go_interface_name

    def generate_choice(self, structure_schema: Dict, parent_namespace: str, 
                       write_file: bool, explicit_name: str = '') -> str:
        """ Generates a choice/union type """
        # For simplicity, generate as interface{} for now
        # A complete implementation would generate proper union types
        return 'interface{}'

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str, 
                      write_file: bool, explicit_name: str = '') -> str:
        """ Generates a tuple type """
        # For simplicity, generate as interface{} for now
        return 'interface{}'

    def generate_union_class(self, field_name: str, structure_types: List, parent_namespace: str) -> str:
        """ Generates a union type """
        # For simplicity, generate as interface{} for now
        return 'interface{}'

    def get_imports_for_fields(self, types: List[str]) -> Set[str]:
        """Collects necessary imports based on the Go types"""
        imports = set()
        for field_type in types:
            if "time.Time" in field_type or "time.Duration" in field_type:
                imports.add("time")
            if "big.Int" in field_type:
                imports.add("math/big")
        return imports

    def random_value(self, go_type: str) -> str:
        """Generates a random value for a given Go type"""
        import random
        import string

        is_optional = False
        if go_type.startswith('*'):
            is_optional = True
            go_type = go_type[1:]

        if go_type == 'string':
            v = '"' + ''.join(random.choices(string.ascii_letters + string.digits, k=10)) + '"'
        elif go_type == 'bool':
            v = 'true' if random.choice([True, False]) else 'false'
        elif go_type == 'int':
            v = str(random.randint(-100, 100))
        elif go_type == 'int8':
            v = f'int8({random.randint(-100, 100)})'
        elif go_type == 'int16':
            v = f'int16({random.randint(-100, 100)})'
        elif go_type == 'int32':
            v = f'int32({random.randint(-100, 100)})'
        elif go_type == 'int64':
            v = f'int64({random.randint(-100, 100)})'
        elif go_type == 'uint':
            v = f'uint({random.randint(0, 200)})'
        elif go_type == 'uint8':
            v = f'uint8({random.randint(0, 200)})'
        elif go_type == 'uint16':
            v = f'uint16({random.randint(0, 200)})'
        elif go_type == 'uint32':
            v = f'uint32({random.randint(0, 200)})'
        elif go_type == 'uint64':
            v = f'uint64({random.randint(0, 200)})'
        elif go_type == 'float32':
            v = f'float32({random.uniform(-100, 100)})'
        elif go_type == 'float64':
            v = f'float64({random.uniform(-100, 100)})'
        elif go_type == '[]byte':
            v = '[]byte("' + ''.join(random.choices(string.ascii_letters + string.digits, k=10)) + '")'
        elif go_type == 'time.Time':
            v = 'time.Now()'
        elif go_type == 'time.Duration':
            v = 'time.Hour'
        elif go_type.startswith('[]'):
            inner_type = go_type[2:]
            v = f'{go_type}{{{self.random_value(inner_type)}}}'
        elif go_type.startswith('map['):
            # Extract value type from map[KeyType]ValueType
            value_type = go_type.split(']', 1)[1]
            v = f'{go_type}{{"key": {self.random_value(value_type)}}}'
        elif go_type in self.generated_types:
            v = f'CreateInstance{go_type}()'
        elif go_type == 'interface{}':
            v = 'nil'
        else:
            v = 'nil'

        if is_optional and v != 'nil':
            # Create a helper function to get pointer with proper type
            return f'func() *{go_type} {{ v := {v}; return &v }}()'
        return v

    def generate_helpers(self) -> None:
        """Generates helper functions for initializing structs with random values"""
        context = {
            'structs': self.structs,
            'enums': self.enums,
            'base_package': self.base_package,
        }
        needs_time_import = False
        for struct in context['structs']:
            for field in struct['fields']:
                if 'value' not in field:
                    field['value'] = self.random_value(field['type'])
                # Check if time package is needed
                if 'time.Time' in field['type'] or 'time.Duration' in field['type']:
                    needs_time_import = True
        context['needs_time_import'] = needs_time_import
        helpers_file_name = os.path.join(self.output_dir, 'pkg', self.base_package, f"{self.base_package}_helpers.go")
        render_template('structuretogo/go_helpers.jinja', helpers_file_name, **context)

    def generate_unit_test(self, kind: str, name: str, fields: Any):
        """Generates unit tests for Go struct or enum"""
        context = {
            'struct_name': name,
            'fields': fields,
            'kind': kind,
            'base_package': self.base_package,
            'package_site': self.package_site,
            'package_username': self.package_username,
            'json_annotation': self.json_annotation,
            'avro_annotation': self.avro_annotation
        }

        pkg_dir = os.path.join(self.output_dir, 'pkg', self.base_package)
        if not os.path.exists(pkg_dir):
            os.makedirs(pkg_dir, exist_ok=True)
        test_file_name = os.path.join(pkg_dir, f"{name}_test.go")
        render_template('structuretogo/go_test.jinja', test_file_name, **context)

    def write_go_mod_file(self):
        """Writes the go.mod file for the Go project"""
        go_mod_content = ""
        go_mod_content += "module " + self.package_site + "/" + self.package_username + "/" + self.base_package + "\n\n"
        go_mod_content += "go 1.21\n\n"
        if self.avro_annotation:
            go_mod_content += "require (\n"
            go_mod_content += "    github.com/hamba/avro/v2 v2.27.0\n"
            go_mod_content += ")\n"

        go_mod_path = os.path.join(self.output_dir, "go.mod")
        with open(go_mod_path, 'w', encoding='utf-8') as file:
            file.write(go_mod_content)

    def write_modname_go_file(self):
        """Writes the modname.go file for the Go project"""
        modname_go_content = ""
        modname_go_content += "package " + self.base_package + "\n\n"
        modname_go_content += "const ModName = \"" + self.base_package + "\"\n"

        modname_go_path = os.path.join(self.output_dir, 'pkg', self.base_package, "module.go")
        with open(modname_go_path, 'w', encoding='utf-8') as file:
            file.write(modname_go_content)

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """ Processes the definitions section recursively """
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition or 'enum' in definition:
                    # This is a type definition
                    current_namespace = namespace_path
                    # Check if this type was already generated
                    check_name = self.go_type_name(name, current_namespace)
                    if check_name not in self.generated_types:
                        if 'enum' in definition:
                            self.generate_enum(definition, name, current_namespace, write_file=True)
                        else:
                            self.generate_class_or_choice(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    # This is a nested namespace
                    new_namespace = f"{namespace_path}.{name}" if namespace_path else name
                    self.process_definitions(definition, new_namespace)

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts JSON Structure schema to Go"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir

        self.structs = []
        self.enums = []

        # Register all schemas with $id keywords for cross-references
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.register_schema_ids(structure_schema)

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

            # Process remaining definitions
            if 'definitions' in structure_schema:
                self.process_definitions(self.definitions, '')

        self.write_go_mod_file()
        self.write_modname_go_file()
        self.generate_helpers()

    def convert(self, structure_schema_path: str, output_dir: str):
        """Converts JSON Structure schema to Go"""
        if not self.base_package:
            self.base_package = os.path.splitext(os.path.basename(structure_schema_path))[0].replace('-', '_').lower()

        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_structure_to_go(structure_schema_path: str, go_file_path: str, package_name: str = '',
                            json_annotation: bool = False, avro_annotation: bool = False,
                            package_site: str = 'github.com', package_username: str = 'username'):
    """Converts JSON Structure schema to Go structs

    Args:
        structure_schema_path (str): JSON Structure input schema path
        go_file_path (str): Output Go directory path
        package_name (str): Base package name
        json_annotation (bool): Include JSON annotations
        avro_annotation (bool): Include Avro annotations
        package_site (str): Package site for Go module
        package_username (str): Package username for Go module
    """
    if not package_name:
        package_name = os.path.splitext(os.path.basename(structure_schema_path))[0].replace('-', '_').lower()

    structuretogo = StructureToGo(package_name)
    structuretogo.json_annotation = json_annotation
    structuretogo.avro_annotation = avro_annotation
    structuretogo.package_site = package_site
    structuretogo.package_username = package_username
    structuretogo.convert(structure_schema_path, go_file_path)


def convert_structure_schema_to_go(structure_schema: JsonNode, output_dir: str, package_name: str = '',
                                   json_annotation: bool = False, avro_annotation: bool = False,
                                   package_site: str = 'github.com', package_username: str = 'username'):
    """Converts JSON Structure schema to Go structs

    Args:
        structure_schema (JsonNode): JSON Structure schema as a dictionary or list of dictionaries
        output_dir (str): Output directory path
        package_name (str): Base package name
        json_annotation (bool): Include JSON annotations
        avro_annotation (bool): Include Avro annotations
        package_site (str): Package site for Go module
        package_username (str): Package username for Go module
    """
    structuretogo = StructureToGo(package_name)
    structuretogo.json_annotation = json_annotation
    structuretogo.avro_annotation = avro_annotation
    structuretogo.package_site = package_site
    structuretogo.package_username = package_username
    structuretogo.convert_schema(structure_schema, output_dir)
