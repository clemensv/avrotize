 # pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

""" Generates Go structs from Avro schema """
import json
import os
from typing import Dict, List, Union

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
        if '.' in avro_type:
            type_name = avro_type.split('.')[-1]
            package_name = '/'.join(avro_type.split('.')[:-1]).lower()
            avro_type = self.concat_package(package_name, type_name)
        if avro_type in self.generated_types_avro_namespace:
            kind = self.generated_types_avro_namespace[avro_type]
            qualified_class_name = self.concat_package(self.base_package, avro_type)
            return qualified_class_name
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
                types: List[str] = [self.convert_avro_type_to_go(field_name, t) for t in non_null_types]
                return f'interface{{}}'  # Go doesn't support union types directly
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
        """ Generates a Go struct or enum from an Avro schema """
        if avro_schema['type'] == 'record':
            return self.generate_struct(avro_schema)
        elif avro_schema['type'] == 'enum':
            return self.generate_enum(avro_schema)
        return 'interface{}'

    def generate_struct(self, avro_schema: Dict) -> str:
        """ Generates a Go struct from an Avro record schema """
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
            struct_definition += f"{INDENT}{field_name} {field_type} `json:\"{field['name']}\"`\n"
        struct_definition += "}\n\n"

        struct_definition += self.generate_to_byte_array_method(struct_name)
        struct_definition += self.generate_from_data_method(struct_name)
        struct_definition += self.generate_is_json_match_method(struct_name, avro_schema)
        struct_definition += self.generate_to_object_method(struct_name)

        self.write_to_file(namespace.replace('.', '/'), struct_name, struct_definition)
        return qualified_struct_name

    def generate_enum(self, avro_schema: Dict) -> str:
        """ Generates a Go enum from an Avro enum schema """
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
            method_definition += f"{INDENT*2}var buf bytes.Buffer\n"
            method_definition += f"{INDENT*2}encoder := avro.NewBinaryEncoder(&buf)\n"
            method_definition += f"{INDENT*2}err = avro.Marshal(s, encoder)\n"
            method_definition += f"{INDENT*2}if err != nil {{\n"
            method_definition += f"{INDENT*3}return nil, err\n"
            method_definition += f"{INDENT*2}}}\n"
            method_definition += f"{INDENT*2}result = buf.Bytes()\n"
        method_definition += f"{INDENT}default:\n"
        method_definition += f"{INDENT*2}return nil, fmt.Errorf(\"unsupported media type: %s\", mediaType)\n"
        method_definition += f"{INDENT}}}\n"
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
        method_definition += f"{INDENT}var s {struct_name}\n"
        method_definition += f"{INDENT}var err error\n"
        method_definition += f"{INDENT}mediaType := strings.Split(contentType, \";\")[0]\n"
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
        method_definition += f"{INDENT*2}mediaType = mediaType[:len(mediaType)-5]\n"
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
            method_definition += f"{INDENT*4}buf := bytes.NewReader(v)\n"
            method_definition += f"{INDENT*4}decoder := avro.NewBinaryDecoder(buf)\n"
            method_definition += f"{INDENT*4}err = avro.Unmarshal(decoder, &s)\n"
            method_definition += f"{INDENT*3}case io.Reader:\n"
            method_definition += f"{INDENT*4}decoder := avro.NewBinaryDecoder(v)\n"
            method_definition += f"{INDENT*4}err = avro.Unmarshal(decoder, &s)\n"
            method_definition += f"{INDENT*3}default:\n"
            method_definition += f"{INDENT*4}return nil, fmt.Errorf(\"unsupported data type for Avro: %T\", data)\n"
            method_definition += f"{INDENT*2}}}\n"
        method_definition += f"{INDENT}default:\n"
        method_definition += f"{INDENT*2}return nil, fmt.Errorf(\"unsupported media type: %s\", mediaType)\n"
        method_definition += f"{INDENT}}}\n"
        method_definition += f"{INDENT}return &s, err\n"
        method_definition += f"}}\n\n"
        return method_definition

    def generate_is_json_match_method(self, struct_name: str, avro_schema: Dict) -> str:
        """Generates the isJsonMatch method for the struct"""
        method_definition = f"\nfunc IsJsonMatch(node map[string]interface{{}}) bool {{\n"
        predicates = []
        for field in avro_schema.get('fields', []):
            field_name = pascal(field['name'])
            field_type = self.convert_avro_type_to_go(field_name, field['type'])
            predicates.append(self.get_is_json_match_clause(field_name, field_type))
        method_definition += f"{INDENT}return " + " && ".join(predicates) + "\n"
        method_definition += f"}}\n\n"
        return method_definition

    def get_is_json_match_clause(self, field_name: str, field_type: str) -> str:
        """Generates the isJsonMatch clause for a field"""
        if field_type == 'string' or field_type == '*string':
            return f"_, ok := node[\"{field_name}\"].(string); ok"
        elif field_type == 'bool' or field_type == '*bool':
            return f"_, ok := node[\"{field_name}\"].(bool); ok"
        elif field_type == 'int32' or field_type == '*int32':
            return f"_, ok := node[\"{field_name}\"].(int); ok"
        elif field_type == 'int64' or field_type == '*int64':
            return f"_, ok := node[\"{field_name}\"].(int); ok"
        elif field_type == 'float32' or field_type == '*float32':
            return f"_, ok := node[\"{field_name}\"].(float64); ok"
        elif field_type == 'float64' or field_type == '*float64':
            return f"_, ok := node[\"{field_name}\"].(float64); ok"
        elif field_type == '[]byte':
            return f"_, ok := node[\"{field_name}\"].([]byte); ok"
        elif field_type == 'interface{}':
            return f"_, ok := node[\"{field_name}\"].(interface{{}}); ok"
        elif field_type.startswith('map[string]'):
            return f"_, ok := node[\"{field_name}\"].(map[string]interface{{}}); ok"
        elif field_type.startswith('[]'):
            return f"_, ok := node[\"{field_name}\"].([]interface{{}}); ok"
        else:
            return f"_, ok := node[\"{field_name}\"].(map[string]interface{{}}); ok"

    def generate_to_object_method(self, struct_name: str) -> str:
        """Generates the toObject method for the struct"""
        method_definition = f"\nfunc (s *{struct_name}) ToObject() map[string]interface{{}} {{\n"
        method_definition += f"{INDENT}obj := make(map[string]interface{{}})\n"
        method_definition += f"{INDENT}jsonBytes, _ := json.Marshal(s)\n"
        method_definition += f"{INDENT}json.Unmarshal(jsonBytes, &obj)\n"
        method_definition += f"{INDENT}return obj\n"
        method_definition += f"}}\n\n"
        return method_definition

    def generate_union_class(self, class_name: str, field_name: str, avro_type: List) -> str:
        """Generates a union class for Go"""
        union_class_name = class_name + pascal(field_name) + 'Union'
        class_definition = f"type {union_class_name} struct {{\n"
        union_types = [self.convert_avro_type_to_go(field_name + "Option" + str(i), t) for i, t in enumerate(avro_type)]
        for union_type in union_types:
            field_name = self.safe_identifier(union_type.split('.')[-1])
            class_definition += f"{INDENT}{pascal(field_name)} {union_type}\n"
        class_definition += "}\n\n"
        class_definition += self.generate_union_class_methods(union_class_name, union_types)
        return class_definition

    def generate_union_class_methods(self, union_class_name: str, union_types: List[str]) -> str:
        """Generates methods for the union class"""
        methods = f"func (u *{union_class_name}) ToObject() map[string]interface{{}} {{\n"
        methods += f"{INDENT}obj := make(map[string]interface{{}})\n"
        methods += f"{INDENT}jsonBytes, _ := json.Marshal(u)\n"
        methods += f"{INDENT}json.Unmarshal(jsonBytes, &obj)\n"
        methods += f"{INDENT}return obj\n"
        methods += f"}}\n\n"
        return methods

    def write_to_file(self, package: str, name: str, definition: str):
        """ Writes a Go struct or enum to a file """
        directory_path = os.path.join(
            self.output_dir, package.replace('.', os.sep).replace('/', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}.go")

        with open(file_path, 'w', encoding='utf-8') as file:
            if package:
                file.write(f"package {package.split('/')[-1]}\n\n")
            if "time.Time" in definition:
                file.write("import \"time\"\n\n")
            if "gzip" in definition:
                file.write("import \"compress/gzip\"\n\n")
            if "bytes" in definition:
                file.write("import \"bytes\"\n\n")
            if "fmt" in definition:
                file.write("import \"fmt\"\n\n")
            if "io" in definition:
                file.write("import \"io\"\n\n")
            if "encoding/json" in definition:
                file.write("import \"encoding/json\"\n\n")
            if "github.com/hamba/avro/v2" in definition:
                file.write("import \"github.com/hamba/avro/v2\"\n\n")
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
