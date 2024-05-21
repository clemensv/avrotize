# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

"""Generates Go structs from Avro schema"""
import json
import os
from typing import Dict, List, Union, Set

from avrotize.common import is_generic_avro_type, camel, pascal

INDENT = '    '

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

class AvroToGo:
    """Converts Avro schema to Go structs, including JSON and Avro marshalling methods"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package.replace('.', '/')
        self.output_dir = os.getcwd()
        self.generated_types_avro_namespace: Dict[str, str] = {}
        self.generated_types_go_package: Dict[str, str] = {}
        self.referenced_packages: Dict[str, Set[str]] = {}
        self.referenced_packages_stack: List[Dict[str, Set[str]]] = []
        self.avro_annotation = False
        self.json_annotation = False

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

    def map_primitive_to_go(self, avro_type: str, is_optional: bool) -> str:
        """Maps Avro primitive types to Go types"""
        optional_mapping = {
            'null': 'interface{}',
            'boolean': '*bool',
            'int': '*int32',
            'long': '*int64',
            'float': '*float32',
            'double': '*float64',
            'bytes': '[]byte',
            'string': '*string',
        }
        required_mapping = {
            'null': 'interface{}',
            'boolean': 'bool',
            'int': 'int32',
            'long': 'int64',
            'float': 'float32',
            'double': 'float64',
            'bytes': '[]byte',
            'string': 'string',
        }
        qualified_avro_type = avro_type
        if '.' in avro_type:
            type_name = avro_type.split('.')[-1]
            package_name = '/'.join(avro_type.split('.')[:-1]).lower()
            qualified_avro_type = package_name + '/' + type_name
            avro_type = type_name
            self.referenced_packages.setdefault(package_name, set()).add(type_name)
        if qualified_avro_type in self.generated_types_avro_namespace:
            return avro_type
        else:
            return required_mapping.get(avro_type, avro_type) if not is_optional else optional_mapping.get(avro_type, avro_type)

    def concat_package(self, package: str, name: str) -> str:
        """Concatenates package and name using a slash separator"""
        return f"{package.lower()}/{name}" if package else name

    def convert_avro_type_to_go(self, field_name: str, avro_type: Union[str, Dict, List], nullable: bool = False) -> str:
        """Converts Avro type to Go type"""
        if isinstance(avro_type, str):
            return self.map_primitive_to_go(avro_type, nullable)
        elif isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                return 'interface{}'
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                if isinstance(non_null_types[0], str):
                    return self.map_primitive_to_go(non_null_types[0], True)
                else:
                    return self.convert_avro_type_to_go(field_name, non_null_types[0])
            else:
                return self.generate_union_class(field_name, avro_type)
        elif isinstance(avro_type, dict):
            if avro_type['type'] in ['record', 'enum']:
                return self.generate_class_or_enum(avro_type)
            elif avro_type['type'] == 'fixed' or avro_type['type'] == 'bytes' and 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'decimal':
                    return 'float64'
            elif avro_type['type'] == 'array':
                item_type = self.convert_avro_type_to_go(field_name, avro_type['items'], nullable=True)
                return f"[]{item_type}"
            elif avro_type['type'] == 'map':
                values_type = self.convert_avro_type_to_go(field_name, avro_type['values'], nullable=True)
                return f"map[string]{values_type}"
            elif 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'date':
                    return 'time.Time'
                elif avro_type['logicalType'] == 'time-millis' or avro_type['logicalType'] == 'time-micros':
                    return 'time.Time'
                elif avro_type['logicalType'] == 'timestamp-millis' or avro_type['logicalType'] == 'timestamp-micros':
                    return 'time.Time'
                elif avro_type['logicalType'] == 'uuid':
                    return 'string'
            return self.convert_avro_type_to_go(field_name, avro_type['type'])
        return 'interface{}'

    def generate_class_or_enum(self, avro_schema: Dict) -> str:
        """Generates a Go struct or enum from an Avro schema"""
        self.referenced_packages_stack.append(self.referenced_packages)
        self.referenced_packages = {}
        qualified_type = ''
        if avro_schema['type'] == 'record':
            qualified_type = self.generate_struct(avro_schema)
        elif avro_schema['type'] == 'enum':
            qualified_type = self.generate_enum(avro_schema)
        if not qualified_type:
            return 'interface{}'
        self.referenced_packages = self.referenced_packages_stack.pop()
        type_name = qualified_type
        if '/' in qualified_type:
            package_name = qualified_type.rsplit('/', 1)[0]
            type_name = qualified_type.rsplit('/', 1)[1]
            self.referenced_packages.setdefault(package_name, set()).add(type_name)
        return type_name

    def generate_struct(self, avro_schema: Dict) -> str:
        """Generates a Go struct from an Avro record schema"""
        struct_definition = ''
        if 'doc' in avro_schema:
            struct_definition += f"// {avro_schema['doc']}\n"
        namespace = avro_schema.get('namespace', '')
        struct_name = self.safe_identifier(avro_schema['name'])
        qualified_struct_name = self.concat_package(namespace.replace('.', '/'), struct_name)
        if qualified_struct_name in self.generated_types_avro_namespace:
            return qualified_struct_name
        self.generated_types_avro_namespace[qualified_struct_name] = "struct"
        self.generated_types_go_package[qualified_struct_name] = "struct"
        struct_definition += f"type {struct_name} struct {{\n"
        for field in avro_schema.get('fields', []):
            field_name = pascal(field['name'])
            field_type = self.convert_avro_type_to_go(field_name, field['type'])
            struct_definition += f"{INDENT}{field_name} {field_type}"
            if self.json_annotation or self.avro_annotation:
                struct_definition += " `"
            if self.json_annotation:
                struct_definition += f"json:\"{field['name']}\""
            if self.avro_annotation:
                struct_definition += f"{' ' if self.json_annotation else ''}avro:\"{field['name']}\""
            if self.json_annotation or self.avro_annotation:
                struct_definition += "`"
            struct_definition += "\n"
        struct_definition += "}\n\n"

        struct_definition += self.generate_to_byte_array_method(struct_name)
        struct_definition += self.generate_from_data_method(struct_name)
        struct_definition += self.generate_is_json_match_method(struct_name, avro_schema)
        struct_definition += self.generate_to_object_method(struct_name)

        if self.avro_annotation:
            schema_json = json.dumps(avro_schema).replace('"', '\\"')
            struct_definition += f"var {struct_name}Schema = avro.MustParse(`{schema_json}`)\n"

        self.write_to_file(namespace.replace('.', '/'), struct_name, struct_definition)
        return qualified_struct_name

    def generate_enum(self, avro_schema: Dict) -> str:
        """Generates a Go enum from an Avro enum schema"""
        enum_definition = ''
        if 'doc' in avro_schema:
            enum_definition += f"// {avro_schema['doc']}\n"
        namespace = avro_schema.get('namespace', '')
        enum_name = self.safe_identifier(avro_schema['name'])
        qualified_enum_name = self.concat_package(namespace.replace('.', '/'), enum_name)
        self.generated_types_avro_namespace[qualified_enum_name] = "enum"
        self.generated_types_go_package[qualified_enum_name] = "enum"
        symbols = avro_schema.get('symbols', [])
        enum_definition += f"type {enum_name} int\n\n"
        enum_definition += f"const (\n"
        for i, symbol in enumerate(symbols):
            enum_definition += f"{INDENT}{enum_name}_{symbol} {enum_name} = {i}\n"
        enum_definition += ")\n\n"
        self.write_to_file(namespace.replace('.', '/'), enum_name, enum_definition)
        return qualified_enum_name

    def generate_to_byte_array_method(self, struct_name: str) -> str:
        """Generates the ToByteArray method for the struct"""
        method_definition = f"\nfunc (s *{struct_name}) ToByteArray(contentType string) ([]byte, error) {{\n"
        if not (self.avro_annotation or self.json_annotation):
            method_definition += f"{INDENT}return nil, fmt.Errorf(\"unsupported content type: %s\", contentType)\n"
            method_definition += f"}}\n\n"
            return method_definition
        method_definition += f"{INDENT}var result []byte\n"
        method_definition += f"{INDENT}var err error\n"
        method_definition += f"{INDENT}mediaType := strings.Split(contentType, \";\")[0]\n"
        method_definition += f"{INDENT}switch mediaType {{\n"
        if self.json_annotation:
            method_definition += f"{INDENT}case \"application/json\":\n"
            method_definition += f"{INDENT*2}result, err = json.Marshal(s)\n"
            method_definition += f"{INDENT*2}if err != nil {{\n"
            method_definition += f"{INDENT*3}return nil, err\n"
            method_definition += f"{INDENT*2}}}\n"
        if self.avro_annotation:
            method_definition += f"{INDENT}case \"avro/binary\", \"application/vnd.apache.avro+avro\":\n"
            method_definition += f"{INDENT*2}result, err = avro.Marshal({struct_name}Schema, s)\n"
            method_definition += f"{INDENT*2}if err != nil {{\n"
            method_definition += f"{INDENT*3}return nil, err\n"
            method_definition += f"{INDENT*2}}}\n"
        method_definition += f"{INDENT}default:\n"
        method_definition += f"{INDENT*2}return nil, fmt.Errorf(\"unsupported media type: %s\", mediaType)\n"
        method_definition += f"{INDENT}}}\n"
        if self.avro_annotation or self.json_annotation:
            method_definition += f"{INDENT}if strings.HasSuffix(mediaType, \"+gzip\") {{\n"
            method_definition += f"{INDENT*2}var buf bytes.Buffer\n"
            method_definition += f"{INDENT*2}gzipWriter := gzip.NewWriter(&buf)\n"
            method_definition += f"{INDENT*2}_, err = gzipWriter.Write(result)\n"
            method_definition += f"{INDENT*2}if err != nil {{\n"
            method_definition += f"{INDENT*3}return nil, err\n"
            method_definition += f"{INDENT*2}}}\n"
            method_definition += f"{INDENT*2}err = gzipWriter.Close()\n"
            method_definition += f"{INDENT*2}if err != nil {{\n"
            method_definition += f"{INDENT*3}return nil, err\n"
            method_definition += f"{INDENT*2}}}\n"
            method_definition += f"{INDENT*2}result = buf.Bytes()\n"
            method_definition += f"{INDENT}}}\n"
        method_definition += f"{INDENT}return result, nil\n"
        method_definition += f"}}\n\n"
        return method_definition

    def generate_from_data_method(self, struct_name: str) -> str:
        """Generates the FromData method for the struct"""
        method_definition = f"\nfunc FromData(data interface{{}}, contentType string) (*{struct_name}, error) {{\n"
        if not (self.avro_annotation or self.json_annotation):
            method_definition += f"{INDENT*1}return nil, fmt.Errorf(\"unsupported content type: %s\", contentType)\n"
            method_definition += f"}}\n\n"
            return method_definition
        method_definition += f"{INDENT}var s {struct_name}\n"
        method_definition += f"{INDENT}var err error\n"
        method_definition += f"{INDENT}mediaType := strings.Split(contentType, \";\")[0]\n"
        if self.avro_annotation or self.json_annotation:
            method_definition += f"{INDENT}if strings.HasSuffix(mediaType, \"+gzip\") {{\n"
            method_definition += f"{INDENT*2}var reader io.Reader\n"
            method_definition += f"{INDENT*2}switch v := data.(type) {{\n"
            method_definition += f"{INDENT*3}case []byte:\n"
            method_definition += f"{INDENT*4}reader = bytes.NewReader(v)\n"
            method_definition += f"{INDENT*3}case io.Reader:\n"
            method_definition += f"{INDENT*4}reader = v\n"
            method_definition += f"{INDENT*3}default:\n"
            method_definition += f"{INDENT*4}return nil, fmt.Errorf(\"unsupported data type for gzip: %T\", data)\n"
            method_definition += f"{INDENT*2}}}\n"
            method_definition += f"{INDENT*2}gzipReader, err := gzip.NewReader(reader)\n"
            method_definition += f"{INDENT*2}if err != nil {{\n"
            method_definition += f"{INDENT*3}return nil, err\n"
            method_definition += f"{INDENT*2}}}\n"
            method_definition += f"{INDENT*2}defer gzipReader.Close()\n"
            method_definition += f"{INDENT*2}data, err = io.ReadAll(gzipReader)\n"
            method_definition += f"{INDENT*2}if err != nil {{\n"
            method_definition += f"{INDENT*3}return nil, err\n"
            method_definition += f"{INDENT*2}}}\n"
            method_definition += f"{INDENT}}}\n"
        method_definition += f"{INDENT}switch mediaType {{\n"
        if self.json_annotation:
            method_definition += f"{INDENT}case \"application/json\":\n"
            method_definition += f"{INDENT*2}switch v := data.(type) {{\n"
            method_definition += f"{INDENT*3}case []byte:\n"
            method_definition += f"{INDENT*4}err = json.Unmarshal(v, &s)\n"
            method_definition += f"{INDENT*3}case string:\n"
            method_definition += f"{INDENT*4}err = json.Unmarshal([]byte(v), &s)\n"
            method_definition += f"{INDENT*3}case io.Reader:\n"
            method_definition += f"{INDENT*4}err = json.NewDecoder(v).Decode(&s)\n"
            method_definition += f"{INDENT*3}default:\n"
            method_definition += f"{INDENT*4}return nil, fmt.Errorf(\"unsupported data type for JSON: %T\", data)\n"
            method_definition += f"{INDENT*2}}}\n"
        if self.avro_annotation:
            method_definition += f"{INDENT}case \"avro/binary\", \"application/vnd.apache.avro+avro\":\n"
            method_definition += f"{INDENT*2}switch v := data.(type) {{\n"
            method_definition += f"{INDENT*3}case []byte:\n"
            method_definition += f"{INDENT*4}err = avro.Unmarshal({struct_name}Schema, v, &s)\n"
            method_definition += f"{INDENT*3}case io.Reader:\n"
            method_definition += f"{INDENT*4}buf, err := io.ReadAll(v)\n"
            method_definition += f"{INDENT*4}if err != nil {{\n"
            method_definition += f"{INDENT*5}return nil, err\n"
            method_definition += f"{INDENT*4}}}\n"
            method_definition += f"{INDENT*4}err = avro.Unmarshal({struct_name}Schema, buf, &s)\n"
            method_definition += f"{INDENT*3}default:\n"
            method_definition += f"{INDENT*4}return nil, fmt.Errorf(\"unsupported data type for Avro: %T\", data)\n"
            method_definition += f"{INDENT*2}}}\n"
        method_definition += f"{INDENT}default:\n"
        method_definition += f"{INDENT*2}return nil, fmt.Errorf(\"unsupported media type: %s\", mediaType)\n"
        method_definition += f"{INDENT}}}\n"
        method_definition += f"{INDENT}if err != nil {{\n"
        method_definition += f"{INDENT*2}return nil, err\n"
        method_definition += f"{INDENT}}}\n"
        method_definition += f"{INDENT}return &s, nil\n"
        method_definition += f"}}\n\n"
        return method_definition

    def generate_is_json_match_method(self, struct_name: str, avro_schema: Dict) -> str:
        """Generates the isJsonMatch method for the struct"""
        method_definition = f"\nfunc IsJsonMatch(node map[string]interface{{}}) bool {{\n"
        predicates = []
        for field in avro_schema.get('fields', []):
            field_name = pascal(field['name'])
            field_type = self.convert_avro_type_to_go(field['name'], field['type'])
            predicates.append(self.get_is_json_match_clause(field_name, field_type))
        method_definition += f"{INDENT}" + f"\n{INDENT}".join(predicates) + "\n"
        method_definition += f"{INDENT}return true\n"
        method_definition += f"}}\n\n"
        return method_definition

    def get_is_json_match_clause(self, field_name: str, field_type: str) -> str:
        """Generates the isJsonMatch clause for a field"""
        if field_type == 'string' or field_type == '*string':
            return f"if _, ok := node[\"{field_name}\"].(string); !ok {{ return false }}"
        elif field_type == 'bool' or field_type == '*bool':
            return f"if _, ok := node[\"{field_name}\"].(bool); !ok {{ return false }}"
        elif field_type == 'int32' or field_type == '*int32':
            return f"if _, ok := node[\"{field_name}\"].(int); !ok {{ return false }}"
        elif field_type == 'int64' or field_type == '*int64':
            return f"if _, ok := node[\"{field_name}\"].(int); !ok {{ return false }}"
        elif field_type == 'float32' or field_type == '*float32':
            return f"if _, ok := node[\"{field_name}\"].(float64); !ok {{ return false }}"
        elif field_type == 'float64' or field_type == '*float64':
            return f"if _, ok := node[\"{field_name}\"].(float64); !ok {{ return false }}"
        elif field_type == '[]byte':
            return f"if _, ok := node[\"{field_name}\"].([]byte); !ok {{ return false }}"
        elif field_type == 'interface{}':
            return f"if _, ok := node[\"{field_name}\"].(interface{{}}); !ok {{ return false }}"
        elif field_type.startswith('map[string]'):
            return f"if _, ok := node[\"{field_name}\"].(map[string]interface{{}}); !ok {{ return false }}"
        elif field_type.startswith('[]'):
            return f"if _, ok := node[\"{field_name}\"].([]interface{{}}); !ok {{ return false }}"
        else:
            return f"if _, ok := node[\"{field_name}\"].(map[string]interface{{}}); !ok {{ return false }}"

    def generate_to_object_method(self, struct_name: str) -> str:
        """Generates the toObject method for the struct"""
        method_definition = f"\nfunc (s *{struct_name}) ToObject() interface{{}} {{\n"
        method_definition += f"{INDENT}return s\n"
        method_definition += f"}}\n\n"
        return method_definition

    def generate_union_class(self, field_name: str, avro_type: List) -> str:
        """Generates a union class for Go"""
        union_class_name = pascal(field_name) + 'Union'
        class_definition = f"type {union_class_name} struct {{\n"
        union_types = [self.convert_avro_type_to_go(field_name + "Option" + str(i), t) for i, t in enumerate(avro_type)]
        for union_type in union_types:
            field_name = self.safe_identifier(union_type.split('/')[-1])
            class_definition += f"{INDENT}{pascal(field_name)} {union_type}\n"
        class_definition += "}\n\n"
        class_definition += self.generate_union_class_methods(union_class_name, union_types)
        self.write_to_file(self.base_package, union_class_name, class_definition)
        return union_class_name

    def generate_union_class_methods(self, union_class_name: str, union_types: List[str]) -> str:
        """Generates methods for the union class"""
        methods = f"func (u *{union_class_name}) ToObject() interface{{}} {{\n"
        methods += f"{INDENT}if u == nil {{\n"
        methods += f"{INDENT*2}return nil\n"
        methods += f"{INDENT}}}\n"
        for union_type in union_types:
            field_name = self.safe_identifier(union_type.split('/')[-1])
            methods += f"{INDENT}if u.{pascal(field_name)} != nil {{\n"
            methods += f"{INDENT*2}return u.{pascal(field_name)}\n"
            methods += f"{INDENT}}}\n"
        methods += f"{INDENT}return nil\n"
        methods += f"}}\n\n"
        methods += f"func (u *{union_class_name}) ToByteArray(contentType string) ([]byte, error) {{\n"
        methods += f"{INDENT}if u == nil {{\n"
        methods += f"{INDENT*2}return nil, nil\n"
        methods += f"{INDENT}}}\n"
        methods += f"{INDENT}var result []byte\n"
        methods += f"{INDENT}var err error\n"
        methods += f"{INDENT}mediaType := strings.Split(contentType, \";\")[0]\n"
        methods += f"{INDENT}switch mediaType {{\n"
        if self.json_annotation:
            methods += f"{INDENT}case \"application/json\":\n"
            for union_type in union_types:
                field_name = self.safe_identifier(union_type.split('/')[-1])
                methods += f"{INDENT*2}if u.{pascal(field_name)} != nil {{\n"
                methods += f"{INDENT*3}result, err = json.Marshal(u.{pascal(field_name)})\n"
                methods += f"{INDENT*3}if err != nil {{\n"
                methods += f"{INDENT*4}return nil, err\n"
                methods += f"{INDENT*3}}}\n"
                methods += f"{INDENT*2}return result, nil\n"
                methods += f"{INDENT*2}}}\n"
        if self.avro_annotation:
            methods += f"{INDENT}case \"avro/binary\", \"application/vnd.apache.avro+avro\":\n"
            for union_type in union_types:
                field_name = self.safe_identifier(union_type.split('/')[-1])
                methods += f"{INDENT*2}if u.{pascal(field_name)} != nil {{\n"
                methods += f"{INDENT*3}result, err = avro.Marshal({union_class_name}Schema, u.{pascal(field_name)})\n"
                methods += f"{INDENT*3}if err != nil {{\n"
                methods += f"{INDENT*4}return nil, err\n"
                methods += f"{INDENT*3}}}\n"
                methods += f"{INDENT*2}return result, nil\n"
                methods += f"{INDENT*2}}}\n"
        methods += f"{INDENT}default:\n"
        methods += f"{INDENT*2}return nil, fmt.Errorf(\"unsupported media type: %s\", mediaType)\n"
        methods += f"{INDENT}}}\n"
        methods += f"{INDENT}return nil, fmt.Errorf(\"no valid union member to marshal\")\n"
        methods += f"}}\n\n"
        methods += f"func (u *{union_class_name}) IsJsonMatch(node map[string]interface{{}}) bool {{\n"
        methods += f"{INDENT}if u == nil {{\n"
        methods += f"{INDENT*2}return false\n"
        methods += f"{INDENT}}}\n"
        for union_type in union_types:
            field_name = self.safe_identifier(union_type.split('/')[-1])
            methods += f"{INDENT}if u.{pascal(field_name)} != nil {{\n"
            methods += f"{INDENT*2}return IsJsonMatch(node)\n"
            methods += f"{INDENT}}}\n"
        methods += f"{INDENT}return false\n"
        methods += f"}}\n\n"
        return methods

    def write_to_file(self, package: str, name: str, definition: str):
        """Writes a Go struct or enum to a file"""
        directory_path = os.path.join(
            self.output_dir, package.replace('.', os.sep).replace('/', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}.go")

        with open(file_path, 'w', encoding='utf-8') as file:
            if package:
                file.write(f"package {package.split('/')[-1]}\n\n")
            imports = ""
            if "time.Time" in definition:
                imports += f"{INDENT}\"time\"\n"
            if self.avro_annotation or self.json_annotation:
                imports += f"{INDENT}\"compress/gzip\"\n"
            if self.json_annotation:
                imports += f"{INDENT}\"encoding/json\"\n"
            if "bytes" in definition:
                imports += f"{INDENT}\"bytes\"\n"
            if "fmt" in definition:
                imports += f"{INDENT}\"fmt\"\n"
            if "io" in definition:
                imports += f"{INDENT}\"io\"\n"
            if "strings" in definition:
                imports += f"{INDENT}\"strings\"\n"
            if self.avro_annotation:
                imports += f"{INDENT}\"github.com/hamba/avro/v2\"\n"
            for ref_pkg, types in self.referenced_packages.items():
                imports += f"{INDENT}\"{ref_pkg}\"\n"
            file.write(f"import (\n{imports})\n\n")
            file.write(definition)

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts Avro schema to Go"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        for avro_schema in (x for x in schema if isinstance(x, dict)):
            self.generate_class_or_enum(avro_schema)
        self.write_go_mod_file()

    def write_go_mod_file(self):
        """Writes the go.mod file for the Go project"""
        go_mod_content = ""
        go_mod_content += "module " + self.base_package + "\n\n"
        go_mod_content += "go 1.16\n\n"
        if self.avro_annotation:
            go_mod_content += "require (\n"
            go_mod_content += "    github.com/hamba/avro/v2 v2.0.0\n"
            go_mod_content += ")\n"
        
        go_mod_path = os.path.join(self.output_dir, "go.mod")
        with open(go_mod_path, 'w', encoding='utf-8') as file:
            file.write(go_mod_content)

   
    def convert(self, avro_schema_path: str, output_dir: str):
        """Converts Avro schema to Go"""
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_avro_to_go(avro_schema_path, go_file_path, package_name='', avro_annotation=False, json_annotation=False):
    """Converts Avro schema to Go structs

    Args:
        avro_schema_path (str): Avro input schema path
        go_file_path (str): Output Go file path
        package_name (str): Base package name
        avro_annotation (bool): Include Avro annotations
        json_annotation (bool): Include JSON annotations
    """
    avrotogo = AvroToGo()
    avrotogo.base_package = package_name
    avrotogo.avro_annotation = avro_annotation
    avrotogo.json_annotation = json_annotation
    avrotogo.convert(avro_schema_path, go_file_path)


def convert_avro_schema_to_go(avro_schema: JsonNode, output_dir: str, package_name='', avro_annotation=False, json_annotation=False):
    """Converts Avro schema to Go structs

    Args:
        avro_schema (JsonNode): Avro schema as a dictionary or list of dictionaries
        output_dir (str): Output directory path
        package_name (str): Base package name
        avro_annotation (bool): Include Avro annotations
        json_annotation (bool): Include JSON annotations
    """
    avrotogo = AvroToGo()
    avrotogo.base_package = package_name
    avrotogo.avro_annotation = avro_annotation
    avrotogo.json_annotation = json_annotation
    avrotogo.convert_schema(avro_schema, output_dir)
