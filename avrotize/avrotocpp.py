# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

""" Generates C++ code from Avro schema """
import json
import os
from typing import Dict, List, Union

from avrotize.common import is_generic_avro_type, pascal, camel

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
            avro_type = self.concat_namespace(package_name, type_name)
        if avro_type in self.generated_types_avro_namespace:
            kind = self.generated_types_avro_namespace[avro_type]
            qualified_class_name = self.concat_namespace(self.base_namespace, avro_type)
            return qualified_class_name
        else:
            return required_mapping.get(avro_type, avro_type) if not is_optional else optional_mapping.get(avro_type, avro_type)

    def concat_namespace(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name using a double colon separator"""
        return f"{namespace}::{name}" if namespace else name

    def convert_avro_type_to_cpp(self, field_name: str, avro_type: Union[str, Dict, List], nullable: bool = False) -> str:
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
                    return self.convert_avro_type_to_cpp(field_name, non_null_types[0])
            else:
                types: List[str] = [self.convert_avro_type_to_cpp(field_name, t) for t in non_null_types]
                return 'std::variant<' + ', '.join(types) + '>'
        elif isinstance(avro_type, dict):
            if avro_type['type'] in ['record', 'enum']:
                return self.generate_class_or_enum(avro_type)
            elif avro_type['type'] == 'fixed' or avro_type['type'] == 'bytes' and 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'decimal':
                    return 'std::string'  # Handle decimal as string for simplicity
            elif avro_type['type'] == 'array':
                item_type = self.convert_avro_type_to_cpp(field_name, avro_type['items'], nullable=True)
                return f"std::vector<{item_type}>"
            elif avro_type['type'] == 'map':
                values_type = self.convert_avro_type_to_cpp(field_name, avro_type['values'], nullable=True)
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
            return self.convert_avro_type_to_cpp(field_name, avro_type['type'])
        return 'nlohmann::json'

    def generate_class_or_enum(self, avro_schema: Dict) -> str:
        """ Generates a C++ class or enum from an Avro schema """
        if avro_schema['type'] == 'record':
            return self.generate_class(avro_schema)
        elif avro_schema['type'] == 'enum':
            return self.generate_enum(avro_schema)
        return 'nlohmann::json'

    def generate_class(self, avro_schema: Dict) -> str:
        """ Generates a C++ class from an Avro record schema """
        class_definition = ''
        if 'doc' in avro_schema:
            class_definition += f"// {avro_schema['doc']}\n"
        namespace = avro_schema.get('namespace', '')
        class_name = self.safe_identifier(avro_schema['name'])
        qualified_class_name = self.concat_namespace(namespace.replace('.', '::'), class_name)
        if qualified_class_name in self.generated_types_avro_namespace:
            return qualified_class_name
        self.generated_types_avro_namespace[qualified_class_name] = "class"
        self.generated_types_cpp_namespace[qualified_class_name] = "class"
        class_definition += f"class {class_name} {{\n"
        class_definition += "public:\n"
        for field in avro_schema.get('fields', []):
            field_name = self.safe_identifier(field['name'])
            field_type = self.convert_avro_type_to_cpp(field_name, field['type'])
            class_definition += f"{INDENT}{field_type} {field_name};\n"
        class_definition += "public:\n"
        class_definition += f"{INDENT}{class_name}() = default;\n"
        class_definition += self.generate_to_byte_array_method(class_name)
        class_definition += self.generate_from_data_method(class_name)
        class_definition += self.generate_is_json_match_method(class_name, avro_schema)
        class_definition += self.generate_to_object_method(class_name)
        class_definition += "};\n\n"

        self.write_to_file(namespace.replace('.', '::'), class_name, class_definition)
        return qualified_class_name

    def generate_enum(self, avro_schema: Dict) -> str:
        """ Generates a C++ enum from an Avro enum schema """
        enum_definition = ''
        if 'doc' in avro_schema:
            enum_definition += f"// {avro_schema['doc']}\n"
        namespace = avro_schema.get('namespace', '')
        enum_name = self.safe_identifier(avro_schema['name'])
        qualified_enum_name = self.concat_namespace(namespace.replace('.', '::'), enum_name)
        self.generated_types_avro_namespace[qualified_enum_name] = "enum"
        self.generated_types_cpp_namespace[qualified_enum_name] = "enum"
        symbols = avro_schema.get('symbols', [])
        enum_definition += f"enum class {enum_name} {{\n"
        for symbol in symbols:
            enum_definition += f"{INDENT}{symbol},\n"
        enum_definition += "};\n\n"
        self.write_to_file(namespace.replace('.', '::'), enum_name, enum_definition)
        return qualified_enum_name

    def generate_to_byte_array_method(self, class_name: str) -> str:
        """Generates the to_byte_array method for the class"""
        method_definition = f"\nstd::vector<uint8_t> to_byte_array(const std::string& content_type) const {{\n"
        method_definition += f"{INDENT}std::vector<uint8_t> result;\n"
        method_definition += f"{INDENT}std::string media_type = content_type.substr(0, content_type.find(';'));\n"
        method_definition += f"{INDENT}if (media_type == \"application/json\") {{\n"
        method_definition += f"{INDENT*2}result = nlohmann::json::to_cbor(*this);\n"
        method_definition += f"{INDENT}}} else if (media_type == \"avro/binary\" || media_type == \"application/vnd.apache.avro+avro\") {{\n"
        method_definition += f"{INDENT*2}result = serialize_avro(*this);\n"
        method_definition += f"{INDENT}}} else {{\n"
        method_definition += f"{INDENT*2}throw std::invalid_argument(\"Unsupported media type: \" + media_type);\n"
        method_definition += f"{INDENT}}}\n"
        method_definition += f"{INDENT}if (media_type.find(\"+gzip\") != std::string::npos) {{\n"
        method_definition += f"{INDENT*2}result = compress_gzip(result);\n"
        method_definition += f"{INDENT}}}\n"
        method_definition += f"{INDENT}return result;\n"
        method_definition += f"}}\n"
        return method_definition

    def generate_from_data_method(self, class_name: str) -> str:
        """Generates the from_data method for the class"""
        method_definition = f"\nstatic {class_name} from_data(const std::vector<uint8_t>& data, const std::string& content_type) {{\n"
        method_definition += f"{INDENT}{class_name} result;\n"
        method_definition += f"{INDENT}std::string media_type = content_type.substr(0, content_type.find(';'));\n"
        method_definition += f"{INDENT}std::vector<uint8_t> decompressed_data = data;\n"
        method_definition += f"{INDENT}if (media_type.find(\"+gzip\") != std::string::npos) {{\n"
        method_definition += f"{INDENT*2}decompressed_data = decompress_gzip(data);\n"
        method_definition += f"{INDENT*2}media_type = media_type.substr(0, media_type.find(\"+gzip\"));\n"
        method_definition += f"{INDENT}}}\n"
        method_definition += f"{INDENT}if (media_type == \"application/json\") {{\n"
        method_definition += f"{INDENT*2}result = nlohmann::json::from_cbor(decompressed_data).get<{class_name}>();\n"
        method_definition += f"{INDENT}}} else if (media_type == \"avro/binary\" || media_type == \"application/vnd.apache.avro+avro\") {{\n"
        method_definition += f"{INDENT*2}result = deserialize_avro<{class_name}>(decompressed_data);\n"
        method_definition += f"{INDENT}}} else {{\n"
        method_definition += f"{INDENT*2}throw std::invalid_argument(\"Unsupported media type: \" + media_type);\n"
        method_definition += f"{INDENT}}}\n"
        method_definition += f"{INDENT}return result;\n"
        method_definition += f"}}\n"
        return method_definition

    def generate_is_json_match_method(self, class_name: str, avro_schema: Dict) -> str:
        """Generates the is_json_match method for the class"""
        method_definition = f"\nstatic bool is_json_match(const nlohmann::json& node) {{\n"
        predicates = []
        for field in avro_schema.get('fields', []):
            field_name = camel(field['name'])
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

    def generate_to_object_method(self, class_name: str) -> str:
        """Generates the to_object method for the class"""
        method_definition = f"\nnlohmann::json to_object() const {{\n"
        method_definition += f"{INDENT}return nlohmann::json::to_json(*this);\n"
        method_definition += f"}}\n"
        return method_definition

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
        return class_definition

    def write_to_file(self, namespace: str, name: str, definition: str):
        """ Writes a C++ class or enum to a file """
        directory_path = os.path.join(
            self.output_dir, namespace.replace('::', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}.hpp")

        with open(file_path, 'w', encoding='utf-8') as file:
            if namespace:
                file.write(f"namespace {namespace.replace('::', ' ')} {{\n\n")
            file.write("#include <nlohmann/json.hpp>\n")
            file.write("#include <vector>\n")
            file.write("#include <map>\n")
            file.write("#include <optional>\n")
            file.write("#include <stdexcept>\n")
            file.write("#include <chrono>\n")
            file.write("#include <boost/uuid/uuid.hpp>\n")
            file.write("#include <boost/uuid/uuid_io.hpp>\n")
            file.write("#include \"gzip/compress.hpp\"\n")
            file.write("#include \"gzip/decompress.hpp\"\n")
            file.write(definition)
            if namespace:
                file.write(f"}} // namespace {namespace.replace('::', ' ')}\n")

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts Avro schema to C++"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        for avro_schema in (x for x in schema if isinstance(x, dict)):
            self.generate_class_or_enum(avro_schema)

    def convert(self, avro_schema_path: str, output_dir: str):
        """Converts Avro schema to C++"""
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_avro_to_cpp(avro_schema_path, cpp_file_path, namespace='', avro_annotation=False, json_annotation=False):
    """Converts Avro schema to C++ classes

    Args:
        avro_schema_path (str): Avro input schema path  
        cpp_file_path (str): Output C++ file path 
        namespace (str): Base namespace name
        avro_annotation (bool): Include Avro annotations
        json_annotation (bool): Include JSON annotations
    """
    avrotocpp = AvroToCpp()
    avrotocpp.base_namespace = namespace
    avrotocpp.avro_annotation = avro_annotation
    avrotocpp.json_annotation = json_annotation
    avrotocpp.convert(avro_schema_path, cpp_file_path)


def convert_avro_schema_to_cpp(avro_schema: JsonNode, output_dir: str, namespace='', avro_annotation=False, json_annotation=False):
    """Converts Avro schema to C++ classes

    Args:
        avro_schema (JsonNode): Avro schema as a dictionary or list of dictionaries
        output_dir (str): Output directory path 
        namespace (str): Base namespace name
        avro_annotation (bool): Include Avro annotations
        json_annotation (bool): Include JSON annotations
    """
    avrotocpp = AvroToCpp()
    avrotocpp.base_namespace = namespace
    avrotocpp.avro_annotation = avro_annotation
    avrotocpp.json_annotation = json_annotation
    avrotocpp.convert_schema(avro_schema, output_dir)
