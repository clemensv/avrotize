# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

"""Generates C++ code from Avro schema"""
import json
import os
from typing import Dict, List, Union

from avrotize.common import is_generic_avro_type, pascal, process_template

INDENT = '    '

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


class AvroToCpp:
    """Converts Avro schema to C++ code, including JSON and Avro methods"""

    def __init__(self, base_namespace: str = '') -> None:
        self.base_namespace = base_namespace
        self.output_dir = os.getcwd()
        self.generated_types_avro_namespace: Dict[str, str] = {}
        self.generated_types_cpp_namespace: Dict[str, str] = {}
        self.avro_annotation = False
        self.json_annotation = False
        self.generated_files: List[str] = []
        self.test_files: List[str] = []

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

    def map_primitive_to_cpp(self, avro_type: str, is_optional: bool) -> str:
        """Maps Avro primitive types to C++ types"""
        optional_mapping = {
            'null': 'std::optional<std::monostate>',
            'boolean': 'std::optional<bool>',
            'int': 'std::optional<int>',
            'long': 'std::optional<long long>',
            'float': 'std::optional<float>',
            'double': 'std::optional<double>',
            'bytes': 'std::optional<std::vector<uint8_t>>',
            'string': 'std::optional<std::string>'
        }
        required_mapping = {
            'null': 'std::monostate',
            'boolean': 'bool',
            'int': 'int',
            'long': 'long long',
            'float': 'float',
            'double': 'double',
            'bytes': 'std::vector<uint8_t>',
            'string': 'std::string'
        }
        if '.' in avro_type:
            type_name = avro_type.split('.')[-1]
            package_name = '::'.join(avro_type.split('.')[:-1]).lower()
            avro_type = self.get_qualified_name(package_name, type_name)
        if avro_type in self.generated_types_avro_namespace:
            kind = self.generated_types_avro_namespace[avro_type]
            qualified_class_name = self.get_qualified_name(self.base_namespace, avro_type)
            return qualified_class_name
        else:
            return required_mapping.get(avro_type, avro_type) if not is_optional else optional_mapping.get(avro_type, avro_type)

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

    def convert_avro_type_to_cpp(self, field_name: str, avro_type: Union[str, Dict, List], nullable: bool = False, parent_namespace: str = '') -> str:
        """Converts Avro type to C++ type"""
        if isinstance(avro_type, str):
            return self.map_primitive_to_cpp(avro_type, nullable)
        elif isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                return 'nlohmann::json'
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                if isinstance(non_null_types[0], str):
                    return self.map_primitive_to_cpp(non_null_types[0], True)
                else:
                    return self.convert_avro_type_to_cpp(field_name, non_null_types[0], parent_namespace=parent_namespace)
            else:
                types: List[str] = [self.convert_avro_type_to_cpp(field_name, t, parent_namespace=parent_namespace) for t in non_null_types]
                return 'std::variant<' + ', '.join(types) + '>'
        elif isinstance(avro_type, dict):
            if avro_type['type'] in ['record', 'enum']:
                return self.generate_class_or_enum(avro_type, parent_namespace)
            elif avro_type['type'] == 'fixed' or avro_type['type'] == 'bytes' and 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'decimal':
                    return 'std::string'  # Handle decimal as string for simplicity
            elif avro_type['type'] == 'array':
                item_type = self.convert_avro_type_to_cpp(field_name, avro_type['items'], nullable=True, parent_namespace=parent_namespace)
                return f"std::vector<{item_type}>"
            elif avro_type['type'] == 'map':
                values_type = self.convert_avro_type_to_cpp(field_name, avro_type['values'], nullable=True, parent_namespace=parent_namespace)
                return f"std::map<std::string, {values_type}>"
            elif 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'date':
                    return 'std::chrono::system_clock::time_point'
                elif avro_type['logicalType'] == 'time-millis' or avro_type['logicalType'] == 'time-micros':
                    return 'std::chrono::milliseconds'
                elif avro_type['logicalType'] == 'timestamp-millis' or avro_type['logicalType'] == 'timestamp-micros':
                    return 'std::chrono::system_clock::time_point'
                elif avro_type['logicalType'] == 'uuid':
                    return 'boost::uuids::uuid'
            return self.convert_avro_type_to_cpp(field_name, avro_type['type'], parent_namespace=parent_namespace)
        return 'nlohmann::json'

    def generate_class_or_enum(self, avro_schema: Dict, parent_namespace: str) -> str:
        """Generates a C++ class or enum from an Avro schema"""
        if avro_schema['type'] == 'record':
            return self.generate_class(avro_schema, parent_namespace)
        elif avro_schema['type'] == 'enum':
            return self.generate_enum(avro_schema, parent_namespace)
        return 'nlohmann::json'

    def generate_class(self, avro_schema: Dict, parent_namespace: str) -> str:
        """Generates a C++ class from an Avro record schema"""
        class_definition = ''
        if 'doc' in avro_schema:
            class_definition += f"// {avro_schema['doc']}\n"
        avro_namespace = avro_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_namespace, avro_namespace.replace('.', '::'))
        class_name = self.safe_identifier(avro_schema['name'])
        qualified_class_name = self.get_qualified_name(namespace, class_name)
        if qualified_class_name in self.generated_types_avro_namespace:
            return qualified_class_name
        self.generated_types_avro_namespace[qualified_class_name] = avro_namespace
        self.generated_types_cpp_namespace[qualified_class_name] = "class"

        # Track the includes for member types
        member_includes = set()

        class_definition += f"class {class_name} {{\n"
        class_definition += "public:\n"
        for field in avro_schema.get('fields', []):
            field_name = self.safe_identifier(field['name'])

            # Track the Avro type before conversion to C++ type
            avro_field_type = field['type']

            # Convert to C++ type
            field_type = self.convert_avro_type_to_cpp(field_name, avro_field_type, parent_namespace=avro_namespace)

            # Check if the field_type is a custom type that requires an include
            if isinstance(avro_field_type, dict) and avro_field_type['type'] in ['record', 'enum']:
                include_namespace = self.concat_namespace(
                    self.base_namespace,
                    avro_field_type.get('namespace', avro_namespace).replace('.', '::')
                )
                include_name = avro_field_type['name']
                member_includes.add(self.get_qualified_name(include_namespace, include_name))

            class_definition += f"{INDENT}{field_type} {field_name};\n"
        class_definition += "public:\n"
        class_definition += f"{INDENT}{class_name}() = default;\n"

        class_definition += process_template("avrotocpp/dataclass_body.jinja", class_name=class_name, avro_annotation=self.avro_annotation, json_annotation=self.json_annotation)
        if self.json_annotation:
            class_definition += self.generate_is_json_match_method(class_name, avro_schema)
            class_definition += self.generate_to_json_method(class_name)
        class_definition += "};\n\n"

        # Create includes
        includes = self.generate_includes(member_includes)

        self.write_to_file(namespace, class_name, includes, class_definition)
        self.generate_unit_test(class_name, avro_schema.get('fields', []), namespace)
        return qualified_class_name

    def generate_enum(self, avro_schema: Dict, parent_namespace: str) -> str:
        """Generates a C++ enum from an Avro enum schema"""
        enum_definition = ''
        if 'doc' in avro_schema:
            enum_definition += f"// {avro_schema['doc']}\n"
        avro_namespace = avro_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_namespace, avro_namespace.replace('.', '::'))
        enum_name = self.safe_identifier(avro_schema['name'])
        qualified_enum_name = self.get_qualified_name(namespace, enum_name)
        self.generated_types_avro_namespace[qualified_enum_name] = avro_namespace
        self.generated_types_cpp_namespace[qualified_enum_name] = "enum"
        symbols = avro_schema.get('symbols', [])
        enum_definition += f"enum class {enum_name} {{\n"
        for symbol in symbols:
            enum_definition += f"{INDENT}{symbol},\n"
        enum_definition += "};\n\n"
        self.write_to_file(namespace, enum_name, "", enum_definition)
        return qualified_enum_name

    def generate_to_byte_array_method(self, class_name: str) -> str:
        """Generates the to_byte_array method for the class"""
        return process_template("avrotocpp/to_byte_array_method.jinja", class_name=class_name, avro_annotation=self.avro_annotation, json_annotation=self.json_annotation)

    def generate_from_data_method(self, class_name: str) -> str:
        """Generates the from_data method for the class"""
        return process_template("avrotocpp/from_data_method.jinja", class_name=class_name, avro_annotation=self.avro_annotation, json_annotation=self.json_annotation)
     
    def generate_is_json_match_method(self, class_name: str, avro_schema: Dict) -> str:
        """Generates the is_json_match method for the class"""
        method_definition = f"\nstatic bool is_json_match(const nlohmann::json& node) {{\n"
        predicates = []
        for field in avro_schema.get('fields', []):
            field_name = self.safe_identifier(field['name'])
            field_type = self.convert_avro_type_to_cpp(field_name, field['type'])
            predicates.append(self.get_is_json_match_clause(field_name, field_type))
        method_definition += f"{INDENT}return " + " && ".join(predicates) + ";\n"
        method_definition += f"}}\n"
        return method_definition

    def get_is_json_match_clause(self, field_name: str, field_type: str) -> str:
        """Generates the is_json_match clause for a field"""
        if field_type == 'std::string' or field_type == 'std::optional<std::string>':
            return f"node.contains(\"{field_name}\") && node[\"{field_name}\"].is_string()"
        elif field_type == 'bool' or field_type == 'std::optional<bool>':
            return f"node.contains(\"{field_name}\") && node[\"{field_name}\"].is_boolean()"
        elif field_type == 'int' or field_type == 'std::optional<int>':
            return f"node.contains(\"{field_name}\") && node[\"{field_name}\"].is_number_integer()"
        elif field_type == 'long long' or field_type == 'std::optional<long long>':
            return f"node.contains(\"{field_name}\") && node[\"{field_name}\"].is_number_integer()"
        elif field_type == 'float' or field_type == 'std::optional<float>':
            return f"node.contains(\"{field_name}\") && node[\"{field_name}\"].is_number_float()"
        elif field_type == 'double' or field_type == 'std::optional<double>':
            return f"node.contains(\"{field_name}\") && node[\"{field_name}\"].is_number_float()"
        elif field_type == 'std::vector<uint8_t>' or field_type == 'std::optional<std::vector<uint8_t>>':
            return f"node.contains(\"{field_name}\") && node[\"{field_name}\"].is_binary()"
        elif field_type == 'nlohmann::json':
            return f"node.contains(\"{field_name}\") && node[\"{field_name}\"].is_object()"
        elif field_type.startswith('std::map<std::string, '):
            return f"node.contains(\"{field_name}\") && node[\"{field_name}\"].is_object()"
        elif field_type.startswith('std::vector<'):
            return f"node.contains(\"{field_name}\") && node[\"{field_name}\"].is_array()"
        else:
            return f"{field_type}::is_json_match(node[\"{field_name}\"])"

    def generate_to_json_method(self, class_name: str) -> str:
        """Generates the to_object method for the class"""
        method_definition = f"\nnlohmann::json to_json() const {{\n"
        method_definition += f"{INDENT}return nlohmann::json(*this);\n"
        method_definition += f"}}\n"
        return method_definition

    def generate_avro_schema(self, class_name: str, avro_schema: Dict) -> str:
        """Generates the AVRO_SCHEMA static variable and initialization code"""
        schema_json = json.dumps(avro_schema, indent=4)
        return process_template("avrotocpp/avro_schema.jinja", class_name=class_name, schema_json=schema_json)

    def generate_serialize_avro_method(self, class_name: str) -> str:
        """Generates the serialize_avro method for the class"""
        return process_template("avrotocpp/serialize_avro_method.jinja", class_name=class_name)

    def generate_deserialize_avro_method(self, class_name: str) -> str:
        """Generates the deserialize_avro method for the class"""
        return process_template("avrotocpp/deserialize_avro_method.jinja", class_name=class_name)

    def generate_union_class(self, class_name: str, field_name: str, avro_type: List) -> str:
        """Generates a union class for C++"""
        union_class_name = class_name + pascal(field_name) + 'Union'
        class_definition = f"class {union_class_name} {{\n"
        class_definition += "public:\n"
        union_types = [self.convert_avro_type_to_cpp(field_name + "Option" + str(i), t) for i, t in enumerate(avro_type)]
        for union_type in union_types:
            field_name = self.safe_identifier(union_type.split('::')[-1])
            class_definition += f"{INDENT}{union_type} get_{field_name}() const;\n"
        class_definition += "};\n\n"

        # Write the union class to a separate file
        namespace = self.get_qualified_name(self.base_namespace, class_name.lower())
        includes = self.generate_includes(set(union_types))
        self.write_to_file(namespace, union_class_name, includes, class_definition)

        return union_class_name

    def generate_includes(self, member_includes: set) -> str:
        """Generates the include statements for the member types"""
        includes = '\n'.join([f'#include "{include.replace("::", "/")}.hpp"' for include in member_includes])
        return includes
    
    def generate_unit_test(self, class_name: str, fields: List[Dict[str, str]], namespace: str):
        """Generates a unit test for a given class"""
        test_definition = f'#include <gtest/gtest.h>\n'
        test_definition += f'#include "{namespace.replace("::", "/")}/{class_name}.hpp"\n\n'
        test_definition += f'TEST({class_name}Test, PropertiesTest) {{\n'
        test_definition += f'{INDENT}{namespace}::{class_name} instance;\n'

        for field in fields:
            field_name = self.safe_identifier(field['name'])
            test_value = self.get_test_value(field['type'])
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

    def get_test_value(self, avro_type: Union[str, Dict, List]) -> str:
        """Returns a default test value based on the Avro type"""
        if isinstance(avro_type, str):
            test_values = {
                'string': '"test_string"',
                'boolean': 'true',
                'int': '42',
                'long': '42LL',
                'float': '3.14f',
                'double': '3.14',
                'bytes': '{0x01, 0x02, 0x03}',
                'null': 'std::monostate()',
            }
            return test_values.get(avro_type, '/* Unknown type */')

        elif isinstance(avro_type, list):
            # For unions, use the first non-null type
            non_null_types = [t for t in avro_type if t != 'null']
            if non_null_types:
                return self.get_test_value(non_null_types[0])
            return '/* Unknown union type */'

        elif isinstance(avro_type, dict):
            avro_type_name = avro_type['type']
            if avro_type_name == 'record':
                return f"{avro_type['name']}()"
            elif avro_type_name == 'enum':
                return f"{avro_type['name']}::{avro_type['symbols'][0]}"
            elif avro_type_name == 'array':
                item_type = self.get_test_value(avro_type['items'])
                return f"std::vector<{item_type}>{{{item_type}}}"
            elif avro_type_name == 'map':
                value_type = self.get_test_value(avro_type['values'])
                return f"std::map<std::string, {value_type}>{{{{\"key\", {value_type}}}}}"
            elif avro_type_name == 'fixed' or (avro_type_name == 'bytes' and 'logicalType' in avro_type and avro_type['logicalType'] == 'decimal'):
                return '"fixed_bytes"'
            elif 'logicalType' in avro_type:
                logical_type = avro_type['logicalType']
                if logical_type == 'date':
                    return 'std::chrono::system_clock::now()'
                elif logical_type in ['time-millis', 'time-micros']:
                    return 'std::chrono::milliseconds(123456)'
                elif logical_type in ['timestamp-millis', 'timestamp-micros']:
                    return 'std::chrono::system_clock::now()'
                elif logical_type == 'uuid':
                    return 'boost::uuids::random_generator()()'
            return '/* Unknown complex type */'

        return '/* Unknown type */'



    def write_to_file(self, namespace: str, name: str, includes: str, definition: str):
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
            if self.avro_annotation:
                file.write("#include <avro/Specific.hh>\n")
                file.write("#include <avro/Encoder.hh>\n")
                file.write("#include <avro/Decoder.hh>\n")
                file.write("#include <avro/Compiler.hh>\n")
                file.write("#include <avro/Stream.hh>\n")
            if "std::vector" in definition:
                file.write("#include <vector>\n")
            if "std::map" in definition:
               file.write("#include <map>\n")
            if "std::optional" in definition:
                file.write("#include <optional>\n")
            file.write("#include <stdexcept>\n")
            if "std::chrono" in definition:
                file.write("#include <chrono>\n")
            if "boost::uuid" in definition:
               file.write("#include <boost/uuid/uuid.hpp>\n")
               file.write("#include <boost/uuid/uuid_io.hpp>\n")
            if includes:
                file.write(includes + '\n')
            if namespace:
                file.write(f"namespace {namespace} {{\n\n")
            file.write(definition)
            if namespace:
                file.write(f"}} // namespace {namespace}\n")

        # Collect the generated file names
        self.generated_files.append(file_path.replace(os.sep, '/'))

    def generate_cmake_lists(self, project_name: str):
        """Generates a CMakeLists.txt file"""

        # get the current file dir
        cmake_content = process_template("avrotocpp/CMakeLists.txt.jinja", project_name=project_name, avro_annotation=self.avro_annotation, json_annotation=self.json_annotation)
        cmake_path = os.path.join(self.output_dir, 'CMakeLists.txt')
        with open(cmake_path, 'w', encoding='utf-8') as file:
            file.write(cmake_content)
    
    def generate_vcpkg_json(self):
        """Generates a vcpkg.json file"""
        vcpkg_json = process_template("avrotocpp/vcpkg.json.jinja", project_name=self.base_namespace, avro_annotation=self.avro_annotation, json_annotation=self.json_annotation)
        vcpkg_json_path = os.path.join(self.output_dir, 'vcpkg.json')
        with open(vcpkg_json_path, 'w', encoding='utf-8') as file:
            file.write(vcpkg_json)
            
    def generate_build_scripts(self):
        """Generates build scripts for Windows and Linux"""
        build_script_linux = process_template("avrotocpp/build.sh.jinja")
        build_script_windows = process_template("avrotocpp/build.bat.jinja")        
        script_path_linux = os.path.join(self.output_dir, 'build.sh')
        script_path_windows = os.path.join(self.output_dir, 'build.bat')

        with open(script_path_linux, 'w', encoding='utf-8') as file:
            file.write(build_script_linux)

        with open(script_path_windows, 'w', encoding='utf-8') as file:
            file.write(build_script_windows)

    def convert_schema(self, schema: Union[Dict, List], output_dir: str):
        """Converts Avro schema to C++"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        for avro_schema in (x for x in schema if isinstance(x, dict)):
            self.generate_class_or_enum(avro_schema, '')
        self.generate_cmake_lists(self.base_namespace)
        self.generate_build_scripts()
        self.generate_vcpkg_json()

    def convert(self, avro_schema_path: str, output_dir: str):
        """Converts Avro schema to C++"""
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_avro_to_cpp(avro_schema_path, output_dir, namespace='', avro_annotation=False, json_annotation=False):
    """Converts Avro schema to C++ classes"""
    
    if not namespace:
        namespace = os.path.splitext(os.path.basename(avro_schema_path))[0].replace('-', '_')
    
    avroToCpp = AvroToCpp(namespace)
    avroToCpp.avro_annotation = avro_annotation
    avroToCpp.json_annotation = json_annotation
    avroToCpp.convert(avro_schema_path, output_dir)

def convert_avro_schema_to_cpp(avro_schema: Dict, output_dir: str, namespace: str = '', avro_annotation: bool = False, json_annotation: bool = False):
    """Converts Avro schema to C++ classes"""
    avroToCpp = AvroToCpp(namespace)
    avroToCpp.avro_annotation = avro_annotation
    avroToCpp.json_annotation = json_annotation
    avroToCpp.convert_schema(avro_schema, output_dir)
