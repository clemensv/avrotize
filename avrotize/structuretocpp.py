# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

"""Generates C++ code from JSON Structure schema"""
import json
import os
from typing import Dict, List, Union, Set, Optional, Any, cast

from avrotize.common import pascal, process_template

INDENT = '    '

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


class StructureToCpp:
    """Converts JSON Structure schema to C++ code, including JSON serialization methods"""

    def __init__(self, base_namespace: str = '') -> None:
        self.base_namespace = base_namespace
        self.output_dir = os.getcwd()
        self.generated_types_namespace: Dict[str, str] = {}
        self.generated_types_cpp_namespace: Dict[str, str] = {}
        self.json_annotation = True  # JSON Structure always has JSON serialization
        self.generated_files: List[str] = []
        self.test_files: List[str] = []
        self.schema_doc: JsonNode = None
        self.schema_registry: Dict[str, Dict] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}

    def safe_identifier(self, name: str) -> str:
        """Converts a name to a safe C++ identifier"""
        reserved_words = [
            'alignas', 'alignof', 'and', 'and_eq', 'asm', 'atomic_cancel', 'atomic_commit', 'atomic_noexcept', 'auto',
            'bitand', 'bitor', 'bool', 'break', 'case', 'catch', 'char', 'char8_t', 'char16_t', 'char32_t', 'class',
            'compl', 'concept', 'const', 'consteval', 'constexpr', 'constinit', 'const_cast', 'continue', 'co_await',
            'co_return', 'co_yield', 'decltype', 'default', 'delete', 'do', 'double', 'dynamic_cast', 'else', 'enum',
            'explicit', 'export', 'extern', 'false', 'float', 'for', 'friend', 'goto', 'if', 'inline', 'int', 'long',
            'mutable', 'namespace', 'new', 'noexcept', 'not', 'not_eq', 'nullptr', 'operator', 'or', 'or_eq', 'private',
            'protected', 'public', 'reflexpr', 'register', 'reinterpret_cast', 'requires', 'return', 'short', 'signed',
            'sizeof', 'static', 'static_assert', 'static_cast', 'struct', 'switch', 'synchronized', 'template', 'this',
            'thread_local', 'throw', 'true', 'try', 'typedef', 'typeid', 'typename', 'union', 'unsigned', 'using',
            'virtual', 'void', 'volatile', 'wchar_t', 'while', 'xor', 'xor_eq'
        ]
        if name in reserved_words:
            return f"{name}_"
        return name

    def map_primitive_to_cpp(self, structure_type: str, is_optional: bool) -> str:
        """Maps JSON Structure primitive types to C++ types"""
        optional_mapping = {
            'null': 'std::optional<std::monostate>',
            'boolean': 'std::optional<bool>',
            'string': 'std::optional<std::string>',
            'integer': 'std::optional<int>',
            'number': 'std::optional<double>',
            'int8': 'std::optional<int8_t>',
            'uint8': 'std::optional<uint8_t>',
            'int16': 'std::optional<int16_t>',
            'uint16': 'std::optional<uint16_t>',
            'int32': 'std::optional<int32_t>',
            'uint32': 'std::optional<uint32_t>',
            'int64': 'std::optional<int64_t>',
            'uint64': 'std::optional<uint64_t>',
            'float': 'std::optional<float>',
            'double': 'std::optional<double>',
            'binary': 'std::optional<std::vector<uint8_t>>',
            'date': 'std::optional<std::chrono::system_clock::time_point>',
            'time': 'std::optional<std::chrono::milliseconds>',
            'datetime': 'std::optional<std::chrono::system_clock::time_point>',
            'timestamp': 'std::optional<std::chrono::system_clock::time_point>',
            'duration': 'std::optional<std::chrono::milliseconds>',
            'uuid': 'std::optional<boost::uuids::uuid>',
            'uri': 'std::optional<std::string>',
            'jsonpointer': 'std::optional<std::string>',
            'decimal': 'std::optional<std::string>',
            'any': 'nlohmann::json'
        }
        required_mapping = {
            'null': 'std::monostate',
            'boolean': 'bool',
            'string': 'std::string',
            'integer': 'int',
            'number': 'double',
            'int8': 'int8_t',
            'uint8': 'uint8_t',
            'int16': 'int16_t',
            'uint16': 'uint16_t',
            'int32': 'int32_t',
            'uint32': 'uint32_t',
            'int64': 'int64_t',
            'uint64': 'uint64_t',
            'float': 'float',
            'double': 'double',
            'binary': 'std::vector<uint8_t>',
            'date': 'std::chrono::system_clock::time_point',
            'time': 'std::chrono::milliseconds',
            'datetime': 'std::chrono::system_clock::time_point',
            'timestamp': 'std::chrono::system_clock::time_point',
            'duration': 'std::chrono::milliseconds',
            'uuid': 'boost::uuids::uuid',
            'uri': 'std::string',
            'jsonpointer': 'std::string',
            'decimal': 'std::string',
            'any': 'nlohmann::json'
        }
        if '.' in structure_type:
            type_name = structure_type.split('.')[-1]
            package_name = '::'.join(structure_type.split('.')[:-1]).lower()
            structure_type = self.get_qualified_name(package_name, type_name)
        if structure_type in self.generated_types_namespace:
            return self.get_qualified_name(self.base_namespace, structure_type)
        else:
            return required_mapping.get(structure_type, structure_type) if not is_optional else optional_mapping.get(structure_type, f'std::optional<{required_mapping.get(structure_type, structure_type)}>')

    def get_qualified_name(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name using a double colon separator"""
        return f"{namespace}::{name}" if namespace else name
    
    def concat_namespace(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name using a double colon separator"""
        if namespace and name:
            return f"{namespace}::{name}"
        elif namespace:
            return namespace
        return name

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """ Resolves a $ref to the actual schema definition """
        # Check if it's an absolute URI reference (schema with $id)
        if not ref.startswith('#/'):
            # Try to resolve from schema registry
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
        """ Recursively registers schemas with $id keywords """
        if not isinstance(schema, dict):
            return
        
        # Register this schema if it has an $id
        if '$id' in schema:
            schema_id = schema['$id']
            # Handle relative URIs
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                from urllib.parse import urljoin
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            base_uri = schema_id  # Update base URI for nested schemas
        
        # Recursively process definitions
        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)
        
        # Recursively process properties
        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)
        
        # Recursively process items, values, etc.
        for key in ['items', 'values', 'additionalProperties']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)

    def convert_structure_type_to_cpp(self, class_name: str, field_name: str, structure_type: JsonNode, parent_namespace: str, nullable: bool = False) -> str:
        """Converts JSON Structure type to C++ type"""
        if isinstance(structure_type, str):
            return self.map_primitive_to_cpp(structure_type, nullable)
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            is_nullable = 'null' in structure_type
            if len(non_null_types) == 1:
                if isinstance(non_null_types[0], str):
                    return self.map_primitive_to_cpp(non_null_types[0], is_nullable)
                else:
                    cpp_type = self.convert_structure_type_to_cpp(class_name, field_name, non_null_types[0], parent_namespace, nullable=False)
                    if is_nullable:
                        return f'std::optional<{cpp_type}>'
                    return cpp_type
            else:
                types: List[str] = [self.convert_structure_type_to_cpp(class_name, field_name, t, parent_namespace, nullable=False) for t in non_null_types]
                return 'std::variant<' + ', '.join(types) + '>'
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                if ref_schema:
                    ref_path = structure_type['$ref'].split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    return self.generate_class_or_choice(ref_schema, ref_namespace, write_file=True, explicit_name=type_name)
                return 'nlohmann::json'
            
            # Handle enum keyword
            if 'enum' in structure_type:
                return self.generate_enum(structure_type, field_name, parent_namespace, write_file=True)
            
            # Handle type keyword
            if 'type' not in structure_type:
                return 'nlohmann::json'
            
            struct_type = structure_type['type']
            
            if struct_type == 'object':
                return self.generate_class(structure_type, parent_namespace, write_file=True)
            elif struct_type == 'array':
                items_type = self.convert_structure_type_to_cpp(class_name, field_name+'Item', structure_type.get('items', {'type': 'any'}), parent_namespace, nullable=False)
                return f"std::vector<{items_type}>"
            elif struct_type == 'set':
                items_type = self.convert_structure_type_to_cpp(class_name, field_name+'Item', structure_type.get('items', {'type': 'any'}), parent_namespace, nullable=False)
                return f"std::set<{items_type}>"
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_cpp(class_name, field_name+'Value', structure_type.get('values', {'type': 'any'}), parent_namespace, nullable=False)
                return f"std::map<std::string, {values_type}>"
            elif struct_type == 'choice':
                return self.generate_choice(structure_type, parent_namespace, write_file=True)
            elif struct_type == 'tuple':
                return self.generate_tuple(structure_type, parent_namespace, write_file=True)
            else:
                return self.convert_structure_type_to_cpp(class_name, field_name, struct_type, parent_namespace, nullable)
        return 'nlohmann::json'

    def generate_class_or_choice(self, structure_schema: Dict, parent_namespace: str, write_file: bool = True, explicit_name: str = '') -> str:
        """Generates a Class or Choice"""
        struct_type = structure_schema.get('type', 'object')
        if struct_type == 'object':
            return self.generate_class(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'choice':
            return self.generate_choice(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'tuple':
            return self.generate_tuple(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type in ('map', 'array', 'set'):
            # For root-level container types, generate a type alias
            return self.generate_container_alias(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        return 'nlohmann::json'

    def generate_class(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """Generates a C++ class from a JSON Structure object type"""
        class_definition = ''
        
        # Get name and namespace
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_namespace, schema_namespace.replace('.', '::'))
        
        qualified_class_name = self.get_qualified_name(namespace, class_name)
        if qualified_class_name in self.generated_types_namespace:
            return qualified_class_name
        
        self.generated_types_namespace[qualified_class_name] = schema_namespace
        self.generated_types_cpp_namespace[qualified_class_name] = "class"
        self.generated_structure_types[qualified_class_name] = structure_schema

        # Track the includes for member types
        member_includes = set()

        # Generate class documentation
        if 'description' in structure_schema or 'doc' in structure_schema:
            doc = structure_schema.get('description', structure_schema.get('doc', ''))
            class_definition += f"// {doc}\n"
        
        # Check if this is an abstract type
        is_abstract = structure_schema.get('abstract', False)
        
        class_definition += f"class {class_name} {{\n"
        class_definition += "public:\n"
        
        # Generate properties
        properties = structure_schema.get('properties', {})
        required_props = structure_schema.get('required', [])
        
        for prop_name, prop_schema in properties.items():
            field_name = self.safe_identifier(prop_name)
            is_required = prop_name in required_props if not isinstance(required_props, list) or len(required_props) == 0 or not isinstance(required_props[0], list) else any(prop_name in req_set for req_set in required_props)
            
            # Convert to C++ type
            field_type = self.convert_structure_type_to_cpp(class_name, field_name, prop_schema, schema_namespace, nullable=not is_required)
            
            # Add documentation
            if 'description' in prop_schema or 'doc' in prop_schema:
                field_doc = prop_schema.get('description', prop_schema.get('doc', ''))
                class_definition += f"{INDENT}// {field_doc}\n"
            
            class_definition += f"{INDENT}{field_type} {field_name};\n"
        
        # Add default constructor
        class_definition += f"\npublic:\n"
        class_definition += f"{INDENT}{class_name}() = default;\n"

        # Add JSON serialization methods
        class_definition += process_template("structuretocpp/dataclass_body.jinja", 
                                            class_name=class_name, 
                                            json_annotation=self.json_annotation)
        
        if self.json_annotation:
            class_definition += self.generate_to_json_method(class_name)
        
        class_definition += "};\n\n"

        # Create includes
        includes = self.generate_includes(member_includes)

        if write_file:
            self.write_to_file(namespace, class_name, includes, class_definition)
            self.generate_unit_test(class_name, properties, namespace, required_props)
        
        return qualified_class_name

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str, write_file: bool) -> str:
        """Generates a C++ enum from a JSON Structure enum"""
        enum_definition = ''
        
        # Generate enum name
        enum_name = pascal(structure_schema.get('name', field_name + 'Enum'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_namespace, schema_namespace.replace('.', '::'))
        
        qualified_enum_name = self.get_qualified_name(namespace, enum_name)
        if qualified_enum_name in self.generated_types_namespace:
            return qualified_enum_name
        
        self.generated_types_namespace[qualified_enum_name] = schema_namespace
        self.generated_types_cpp_namespace[qualified_enum_name] = "enum"
        
        if 'description' in structure_schema or 'doc' in structure_schema:
            doc = structure_schema.get('description', structure_schema.get('doc', ''))
            enum_definition += f"// {doc}\n"
        
        symbols = structure_schema.get('enum', [])
        enum_definition += f"enum class {enum_name} {{\n"
        for symbol in symbols:
            enum_definition += f"{INDENT}{self.safe_identifier(str(symbol))},\n"
        enum_definition += "};\n\n"
        
        if write_file:
            self.write_to_file(namespace, enum_name, "", enum_definition)
        
        return qualified_enum_name

    def generate_choice(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """Generates a choice (discriminated union) type"""
        # For now, return variant of the choice types
        choices = structure_schema.get('choices', {})
        choice_types = []
        
        for choice_name, choice_schema in choices.items():
            if isinstance(choice_schema, dict):
                if '$ref' in choice_schema:
                    ref_schema = self.resolve_ref(choice_schema['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                    if ref_schema:
                        ref_path = choice_schema['$ref'].split('/')
                        type_name = ref_path[-1]
                        ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                        qualified_name = self.generate_class(ref_schema, ref_namespace, write_file=True, explicit_name=type_name)
                        choice_types.append(qualified_name)
                elif 'type' in choice_schema:
                    choice_type = self.convert_structure_type_to_cpp('Choice', choice_name, choice_schema, parent_namespace, nullable=False)
                    choice_types.append(choice_type)
        
        if len(choice_types) == 0:
            return 'nlohmann::json'
        elif len(choice_types) == 1:
            return choice_types[0]
        else:
            return f"std::variant<{', '.join(choice_types)}>"

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """Generates a tuple type"""
        # Tuples serialize as JSON arrays
        # For now, use std::tuple
        properties = structure_schema.get('properties', {})
        tuple_order = structure_schema.get('tuple', [])
        
        tuple_types = []
        for prop_name in tuple_order:
            if prop_name in properties:
                prop_schema = properties[prop_name]
                prop_type = self.convert_structure_type_to_cpp('Tuple', prop_name, prop_schema, parent_namespace, nullable=False)
                tuple_types.append(prop_type)
        
        if len(tuple_types) == 0:
            return 'nlohmann::json'
        
        return f"std::tuple<{', '.join(tuple_types)}>"

    def generate_container_alias(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """Generates a type alias for root-level container types (map, array, set)"""
        struct_type = structure_schema.get('type', 'map')
        
        # Get name and namespace
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', f'Root{struct_type.capitalize()}'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_namespace, schema_namespace.replace('.', '::'))
        
        qualified_name = self.get_qualified_name(namespace, class_name)
        if qualified_name in self.generated_types_namespace:
            return qualified_name
        
        self.generated_types_namespace[qualified_name] = schema_namespace
        self.generated_types_cpp_namespace[qualified_name] = "alias"
        self.generated_structure_types[qualified_name] = structure_schema
        
        # Determine the underlying type
        if struct_type == 'map':
            values_type = self.convert_structure_type_to_cpp(class_name, 'Value', structure_schema.get('values', {'type': 'any'}), schema_namespace, nullable=False)
            underlying_type = f"std::map<std::string, {values_type}>"
        elif struct_type == 'array':
            items_type = self.convert_structure_type_to_cpp(class_name, 'Item', structure_schema.get('items', {'type': 'any'}), schema_namespace, nullable=False)
            underlying_type = f"std::vector<{items_type}>"
        elif struct_type == 'set':
            items_type = self.convert_structure_type_to_cpp(class_name, 'Item', structure_schema.get('items', {'type': 'any'}), schema_namespace, nullable=False)
            underlying_type = f"std::set<{items_type}>"
        else:
            underlying_type = 'nlohmann::json'
        
        # Generate type alias
        alias_definition = ''
        if 'description' in structure_schema or 'doc' in structure_schema:
            doc = structure_schema.get('description', structure_schema.get('doc', ''))
            alias_definition += f"// {doc}\n"
        
        alias_definition += f"using {class_name} = {underlying_type};\n\n"
        
        if write_file:
            self.write_to_file(namespace, class_name, "", alias_definition)
        
        return qualified_name

    def generate_to_json_method(self, class_name: str) -> str:
        """Generates the to_json method for the class"""
        method_definition = f"\nnlohmann::json to_json() const {{\n"
        method_definition += f"{INDENT}return nlohmann::json(*this);\n"
        method_definition += f"}}\n"
        return method_definition

    def generate_includes(self, member_includes: set) -> str:
        """Generates the include statements for the member types"""
        includes = '\n'.join([f'#include "{include.replace("::", "/")}.hpp"' for include in member_includes])
        return includes
    
    def generate_unit_test(self, class_name: str, properties: Dict, namespace: str, required_props: List) -> None:
        """Generates a unit test for a given class"""
        test_definition = f'#include <gtest/gtest.h>\n'
        test_definition += f'#include "{namespace.replace("::", "/")}/{class_name}.hpp"\n\n'
        test_definition += f'TEST({class_name}Test, PropertiesTest) {{\n'
        test_definition += f'{INDENT}{namespace}::{class_name} instance;\n'

        for prop_name, prop_schema in properties.items():
            field_name = self.safe_identifier(prop_name)
            is_required = prop_name in required_props if not isinstance(required_props, list) or len(required_props) == 0 or not isinstance(required_props[0], list) else any(prop_name in req_set for req_set in required_props)
            test_value = self.get_test_value(prop_schema, is_required)
            test_definition += f'{INDENT}instance.{field_name} = {test_value};\n'
            test_definition += f'{INDENT}EXPECT_EQ(instance.{field_name}, {test_value});\n'

        test_definition += '}\n'

        test_dir = os.path.join(self.output_dir, "tests")
        if not os.path.exists(test_dir):
            os.makedirs(test_dir, exist_ok=True)

        test_file_path = os.path.join(test_dir, f"{class_name}_test.cpp")
        with open(test_file_path, 'w', encoding='utf-8') as file:
            file.write(test_definition)

        self.test_files.append(test_file_path.replace(os.sep, '/'))

    def get_test_value(self, structure_schema: JsonNode, is_required: bool) -> str:
        """Returns a default test value based on the Structure type"""
        if isinstance(structure_schema, str):
            test_values = {
                'string': '"test_string"',
                'boolean': 'true',
                'integer': '42',
                'number': '3.14',
                'int8': '42',
                'uint8': '42',
                'int16': '42',
                'uint16': '42',
                'int32': '42',
                'uint32': '42',
                'int64': '42LL',
                'uint64': '42ULL',
                'float': '3.14f',
                'double': '3.14',
                'binary': 'std::vector<uint8_t>{0x01, 0x02, 0x03}',
                'date': 'std::chrono::system_clock::now()',
                'time': 'std::chrono::milliseconds(123456)',
                'datetime': 'std::chrono::system_clock::now()',
                'timestamp': 'std::chrono::system_clock::now()',
                'duration': 'std::chrono::milliseconds(1000)',
                'uuid': 'boost::uuids::random_generator()()',
                'uri': '"https://example.com"',
                'jsonpointer': '"/path/to/field"',
                'decimal': '"123.45"',
                'any': 'nlohmann::json::object()',
                'null': 'std::monostate()',
            }
            return test_values.get(structure_schema, '"test"')

        elif isinstance(structure_schema, list):
            # For unions, use the first non-null type
            non_null_types = [t for t in structure_schema if t != 'null']
            if non_null_types:
                return self.get_test_value(non_null_types[0], True)
            return 'std::monostate()'

        elif isinstance(structure_schema, dict):
            struct_type = structure_schema.get('type', 'any')
            
            if 'enum' in structure_schema:
                symbols = structure_schema.get('enum', [])
                if symbols:
                    return f"{pascal(structure_schema.get('name', 'Enum'))}_::{self.safe_identifier(str(symbols[0]))}"
                return '"test"'
            
            if struct_type == 'object':
                return f"{pascal(structure_schema.get('name', 'Object'))}()"
            elif struct_type == 'array':
                item_type = self.convert_structure_type_to_cpp('Test', 'Item', structure_schema.get('items', {'type': 'any'}), '', False)
                return f"std::vector<{item_type}>{{}}"
            elif struct_type == 'set':
                item_type = self.convert_structure_type_to_cpp('Test', 'Item', structure_schema.get('items', {'type': 'any'}), '', False)
                return f"std::set<{item_type}>{{}}"
            elif struct_type == 'map':
                value_type = self.convert_structure_type_to_cpp('Test', 'Value', structure_schema.get('values', {'type': 'any'}), '', False)
                return f"std::map<std::string, {value_type}>{{}}"
            elif struct_type == 'tuple':
                return '"tuple_test"'
            else:
                return self.get_test_value(struct_type, is_required)

        return '"test"'

    def write_to_file(self, namespace: str, name: str, includes: str, definition: str) -> None:
        """Writes a C++ class or enum to a file"""
        directory_path = os.path.join(
            self.output_dir, "include", namespace.replace('::', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}.hpp")

        with open(file_path, 'w', encoding='utf-8') as file:
            file.write("#pragma once\n")
            if self.json_annotation:
                file.write("#include <nlohmann/json.hpp>\n")
            if "std::vector" in definition:
                file.write("#include <vector>\n")
            if "std::set" in definition:
                file.write("#include <set>\n")
            if "std::map" in definition:
                file.write("#include <map>\n")
            if "std::tuple" in definition:
                file.write("#include <tuple>\n")
            if "std::variant" in definition:
                file.write("#include <variant>\n")
            if "std::optional" in definition:
                file.write("#include <optional>\n")
            file.write("#include <stdexcept>\n")
            if "std::chrono" in definition:
                file.write("#include <chrono>\n")
            if "boost::uuid" in definition:
                file.write("#include <boost/uuid/uuid.hpp>\n")
                file.write("#include <boost/uuid/uuid_io.hpp>\n")
                file.write("#include <boost/uuid/uuid_generators.hpp>\n")
            if includes:
                file.write(includes + '\n')
            if namespace:
                file.write(f"\nnamespace {namespace} {{\n\n")
            file.write(definition)
            if namespace:
                file.write(f"}} // namespace {namespace}\n")

        # Collect the generated file names
        self.generated_files.append(file_path.replace(os.sep, '/'))

    def generate_cmake_lists(self, project_name: str) -> None:
        """Generates a CMakeLists.txt file"""
        cmake_content = process_template("structuretocpp/CMakeLists.txt.jinja", 
                                        project_name=project_name, 
                                        json_annotation=self.json_annotation)
        cmake_path = os.path.join(self.output_dir, 'CMakeLists.txt')
        with open(cmake_path, 'w', encoding='utf-8') as file:
            file.write(cmake_content)
    
    def generate_vcpkg_json(self) -> None:
        """Generates a vcpkg.json file"""
        vcpkg_json = process_template("structuretocpp/vcpkg.json.jinja", 
                                      project_name=self.base_namespace, 
                                      json_annotation=self.json_annotation)
        vcpkg_json_path = os.path.join(self.output_dir, 'vcpkg.json')
        with open(vcpkg_json_path, 'w', encoding='utf-8') as file:
            file.write(vcpkg_json)
            
    def generate_build_scripts(self) -> None:
        """Generates build scripts for Windows and Linux"""
        build_script_linux = process_template("structuretocpp/build.sh.jinja")
        build_script_windows = process_template("structuretocpp/build.bat.jinja")        
        script_path_linux = os.path.join(self.output_dir, 'build.sh')
        script_path_windows = os.path.join(self.output_dir, 'build.bat')

        with open(script_path_linux, 'w', encoding='utf-8') as file:
            file.write(build_script_linux)

        with open(script_path_windows, 'w', encoding='utf-8') as file:
            file.write(build_script_windows)

    def convert_schema(self, schema: Union[Dict, List], output_dir: str) -> None:
        """Converts JSON Structure schema to C++"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        
        # Register all schema IDs first
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.register_schema_ids(structure_schema)
        
        # Process each schema
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.schema_doc = structure_schema
            
            # Process root type
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
                self.process_definitions(structure_schema['definitions'], '')
        
        self.generate_cmake_lists(self.base_namespace)
        self.generate_build_scripts()
        self.generate_vcpkg_json()

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """ Processes the definitions section recursively """
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition or 'enum' in definition:
                    # This is a type definition
                    current_namespace = self.concat_namespace(namespace_path, '')
                    # Check if this type was already generated
                    check_namespace = self.concat_namespace(self.base_namespace, current_namespace).replace('.', '::')
                    check_name = pascal(name)
                    check_ref = self.get_qualified_name(check_namespace, check_name)
                    if check_ref not in self.generated_types_namespace:
                        self.generate_class_or_choice(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    # This is a namespace
                    new_namespace = self.concat_namespace(namespace_path, name)
                    self.process_definitions(definition, new_namespace)

    def convert(self, structure_schema_path: str, output_dir: str) -> None:
        """Converts JSON Structure schema to C++"""
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_structure_to_cpp(structure_schema_path: str, output_dir: str, namespace: str = '', json_annotation: bool = True) -> None:
    """Converts JSON Structure schema to C++ classes"""
    
    if not namespace:
        namespace = os.path.splitext(os.path.basename(structure_schema_path))[0].replace('-', '_')
    
    structure_to_cpp = StructureToCpp(namespace)
    structure_to_cpp.json_annotation = json_annotation
    structure_to_cpp.convert(structure_schema_path, output_dir)

def convert_structure_schema_to_cpp(structure_schema: Dict, output_dir: str, namespace: str = '', json_annotation: bool = True) -> None:
    """Converts JSON Structure schema to C++ classes"""
    structure_to_cpp = StructureToCpp(namespace)
    structure_to_cpp.json_annotation = json_annotation
    structure_to_cpp.convert_schema(structure_schema, output_dir)
