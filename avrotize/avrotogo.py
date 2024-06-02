import json
import os
from typing import Dict, List, Union, Set
from avrotize.common import get_longest_namespace_prefix, is_generic_avro_type, pascal, render_template

INDENT = '    '

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

class AvroToGo:
    """Converts Avro schema to Go structs, including JSON and Avro marshalling methods"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package
        self.output_dir = os.getcwd()
        self.generated_types_avro_namespace: Dict[str, str] = {}
        self.generated_types_go_package: Dict[str, str] = {}
        self.referenced_packages: Dict[str, Set[str]] = {}
        self.referenced_packages_stack: List[Dict[str, Set[str]]] = []
        self.avro_annotation = False
        self.json_annotation = False
        self.longest_common_prefix = ''
        self.package_site = 'github.com'
        self.package_username = 'username'
        self.structs = []
        self.enums = []

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

    def go_type_name(self, name: str, namespace: str) -> str:
        """Returns a qualified name for a Go struct or enum"""
        if namespace:
            if namespace.startswith(self.longest_common_prefix):
                namespace = namespace[len(self.longest_common_prefix):]
            namespace = ''.join([pascal(t[:-6] if t.endswith("_types") else t) for t in namespace.split('.')])
            return f"{namespace}{pascal(name)}"
        return pascal(name)

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
        if avro_type in self.generated_types_avro_namespace:
            type_name = avro_type.rsplit('.', 1)[-1]
            namespace = avro_type.rsplit('.', 1)[0] if '.' in avro_type else ''
            return self.go_type_name(type_name, namespace)
        else:
            return required_mapping.get(avro_type, avro_type) if not is_optional else optional_mapping.get(avro_type, avro_type)

    def concat_package(self, package: str, name: str) -> str:
        """Concatenates package and name using a slash separator"""
        return f"{package.lower()}/{name}" if package else name

    def convert_avro_type_to_go(self, field_name: str, avro_type: Union[str, Dict, List], nullable: bool = False, parent_namespace: str = '') -> str:
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
                    return self.convert_avro_type_to_go(field_name, non_null_types[0], nullable, parent_namespace)
            else:
                return self.generate_union_class(field_name, avro_type, parent_namespace)
        elif isinstance(avro_type, dict):
            if avro_type['type'] in ['record', 'enum']:
                return self.generate_class_or_enum(avro_type, parent_namespace)
            elif avro_type['type'] == 'fixed' or avro_type['type'] == 'bytes' and 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'decimal':
                    return 'float64'
            elif avro_type['type'] == 'array':
                item_type = self.convert_avro_type_to_go(field_name, avro_type['items'], nullable=True, parent_namespace=parent_namespace)
                if item_type.startswith('*'):
                    return f"[]{item_type[1:]}"
                return f"[]{item_type}"
            elif avro_type['type'] == 'map':
                values_type = self.convert_avro_type_to_go(field_name, avro_type['values'], nullable=True, parent_namespace=parent_namespace)
                if values_type.startswith('*'): 
                    return f"map[string]{values_type}"
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
            return self.convert_avro_type_to_go(field_name, avro_type['type'], parent_namespace=parent_namespace)
        return 'interface{}'

    def generate_class_or_enum(self, avro_schema: Dict, parent_namespace: str = '') -> str:
        """Generates a Go struct or enum from an Avro schema"""
        self.referenced_packages_stack.append(self.referenced_packages)
        self.referenced_packages = {}
        namespace = avro_schema.get('namespace', parent_namespace)
        qualified_type = ''
        if avro_schema['type'] == 'record':
            qualified_type = self.generate_struct(avro_schema, namespace)
        elif avro_schema['type'] == 'enum':
            qualified_type = self.generate_enum(avro_schema, namespace)
        if not qualified_type:
            return 'interface{}'
        self.referenced_packages = self.referenced_packages_stack.pop()
        type_name = qualified_type
        if '/' in qualified_type:
            package_name = qualified_type.rsplit('/', 1)[0]
            type_name = qualified_type.rsplit('/', 1)[1]
            self.referenced_packages.setdefault(package_name, set()).add(type_name)
        return type_name

    def generate_struct(self, avro_schema: Dict, parent_namespace: str) -> str:
        """Generates a Go struct from an Avro record schema"""
        namespace = avro_schema.get('namespace', parent_namespace)
        avro_fullname = namespace + '.' + avro_schema['name'] if namespace else avro_schema['name']
        go_struct_name = self.go_type_name(avro_schema['name'], namespace)
        if avro_fullname in self.generated_types_avro_namespace:
            return go_struct_name
        self.generated_types_avro_namespace[avro_fullname] = "struct"
        self.generated_types_go_package[go_struct_name] = "struct"

        fields = [{
            'name': pascal(field['name']),
            'type': self.convert_avro_type_to_go(field['name'], field['type'], parent_namespace=namespace),
            'original_name': field['name']
        } for field in avro_schema.get('fields', [])]

        context = {
            'doc': avro_schema.get('doc', ''),
            'struct_name': go_struct_name,
            'fields': fields,
            'avro_schema': json.dumps(avro_schema),
            'json_annotation': self.json_annotation,
            'avro_annotation': self.avro_annotation,
            'json_match_predicates': [self.get_is_json_match_clause(f['name'], f['type']) for f in fields],
            'base_package': self.base_package,
        }

        pkg_dir = os.path.join(self.output_dir, 'pkg', self.base_package)
        if not os.path.exists(pkg_dir):
            os.makedirs(pkg_dir, exist_ok=True)
        file_name = os.path.join(pkg_dir, f"{go_struct_name}.go")
        render_template('avrotogo/go_struct.jinja', file_name, **context)

        self.structs.append({
            'name': go_struct_name,
            'fields': fields,
        })

        self.generate_unit_test('struct', go_struct_name, fields)

        return go_struct_name


    def generate_enum(self, avro_schema: Dict, parent_namespace: str) -> str:
        """Generates a Go enum from an Avro enum schema"""
        namespace = avro_schema.get('namespace', parent_namespace)
        avro_fullname = namespace + '.' + avro_schema['name'] if namespace else avro_schema['name']
        enum_name = self.go_type_name(avro_schema['name'], namespace)
        self.generated_types_avro_namespace[avro_fullname] = "enum"
        self.generated_types_go_package[enum_name] = "enum"

        go_types = []  # Enums do not require additional imports based on field types
        imports = self.get_imports_for_definition(go_types)

        context = {
            'doc': avro_schema.get('doc', ''),
            'struct_name': enum_name,
            'symbols': avro_schema.get('symbols', []),
            'imports': imports,
            'base_package': self.base_package,
            'referenced_packages': self.referenced_packages.keys()
        }

        pkg_dir = os.path.join(self.output_dir, 'pkg', self.base_package)
        if not os.path.exists(pkg_dir):
            os.makedirs(pkg_dir, exist_ok=True)
        file_name = os.path.join(pkg_dir, f"{enum_name}.go")
        render_template('avrotogo/go_enum.jinja', file_name, **context)
        
        self.enums.append({
            'name': enum_name,
            'symbols': avro_schema.get('symbols', []),
        })

        self.generate_unit_test('enum', enum_name, context['symbols'])

        return enum_name

    def generate_union_class(self, field_name: str, avro_type: List, parent_namespace: str) -> str:
        """Generates a union class for Go"""
        union_class_name = self.go_type_name(pascal(field_name) + 'Union', parent_namespace)
        union_types = [self.convert_avro_type_to_go(field_name + "Option" + str(i), t, parent_namespace=parent_namespace) for i, t in enumerate(avro_type)]
        if union_class_name in self.generated_types_go_package:
            return union_class_name
        
        self.generated_types_go_package[union_class_name] = "union"
        context = {
            'union_class_name': union_class_name,
            'union_types': union_types,
            'json_annotation': self.json_annotation,
            'avro_annotation': self.avro_annotation,
            'get_is_json_match_clause': self.get_is_json_match_clause,
            'base_package': self.base_package,
        }

        pkg_dir = os.path.join(self.output_dir, 'pkg', self.base_package)
        if not os.path.exists(pkg_dir):
            os.makedirs(pkg_dir, exist_ok=True)
        file_name = os.path.join(pkg_dir, f"{union_class_name}.go")
        render_template('avrotogo/go_union.jinja', file_name, **context)

        fields = []
        for i, field_type in enumerate(union_types):
            v = self.random_value(field_type)
            fields.append({
                'name': pascal(field_type),
                'type': field_type,
                'value': f'Opt({v})' if v != 'nil' else 'nil',
            })
        self.structs.append({
            'name': union_class_name,
            'fields': fields
        })

        self.generate_unit_test('union', union_class_name, union_types)

        return union_class_name


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
        elif field_type in self.generated_types_go_package:
            return f"if _, ok := node[\"{field_name}\"].({field_type}); !ok {{ return false }}"
        else:
            return f"if _, ok := node[\"{field_name}\"].(map[string.interface{{}}); !ok {{ return false }}"

    def get_imports_for_definition(self, types: List[str]) -> Set[str]:
        """Collects necessary imports for the Go definition based on the Go types"""
        imports = set()
        for field_type in types:
            if "time.Time" in field_type:
                imports.add("time")
            if "gzip." in field_type:
                imports.add("compress/gzip")
            if "json." in field_type:
                imports.add("encoding/json")
            if "bytes." in field_type:
                imports.add("bytes")
            if "fmt." in field_type:
                imports.add("fmt")
            if "io." in field_type:
                imports.add("io")
            if "strings." in field_type:
                imports.add("strings")
            if "avro." in field_type:
                imports.add("github.com/hamba/avro/v2")
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
            v = f"string({v})"
        elif go_type == 'bool':
            v = 'true' if random.choice([True, False]) else 'false'
            v = f"bool({v})"
        elif go_type == 'int32' or go_type == 'int':
            v = str(random.randint(-100,100))
            v = f"int32({v})"
        elif go_type == 'int64':
            v = str(random.randint(-100,100))
            v = f"int64({v})"
        elif go_type == 'float32':
            v = str(random.uniform(-100,100))
            v = f"float32({v})"
        elif go_type == 'float64':
            v = str(random.uniform(-100,100))
            v = f"float64({v})"
        elif go_type == '[]byte':
            v = '[]byte("' + ''.join(random.choices(string.ascii_letters + string.digits, k=10)) + '")'
        elif go_type.startswith('[]'):
            v = f'{go_type}{{{self.random_value(go_type[2:])}}}'
        elif go_type.startswith('map[string]'):
            v = f'map[string]{go_type[11:]}{{"key": {self.random_value(go_type[11:])}}}'
        elif go_type in self.generated_types_go_package:
            v = f'random{go_type}()'
        elif go_type == 'interface{}':
            v = 'nil'
        else:
            return 'nil'
        if is_optional and v != 'nil':
            return f'Opt({v})'
        return v

    def generate_helpers(self) -> None:
        """Generates helper functions for initializing structs with random values"""
        context = {
            'structs': self.structs,
            'enums': self.enums,
            'base_package': self.base_package,
        }
        for struct in context['structs']:
            for field in struct['fields']:
                if not 'value' in field:
                    field['value'] = self.random_value(field['type'])
        helpers_file_name = os.path.join(self.output_dir, 'pkg', self.base_package, f"{self.base_package}_helpers.go")
        render_template('avrotogo/go_helpers.jinja', helpers_file_name, **context)

    def generate_unit_test(self, kind: str, name: str, fields: List[Dict[str, str]]):
        """Generates unit tests for Go struct, enum, or union"""
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
        render_template('avrotogo/go_test.jinja', test_file_name, **context)

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts Avro schema to Go"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir

        self.longest_common_prefix = get_longest_namespace_prefix(schema)
        self.structs = []

        for avro_schema in (x for x in schema if isinstance(x, dict)):
            self.generate_class_or_enum(avro_schema)
        self.write_go_mod_file()
        self.write_modname_go_file()
        self.generate_helpers()

    def write_go_mod_file(self):
        """Writes the go.mod file for the Go project"""
        go_mod_content = ""
        go_mod_content += "module " + self.package_site + "/" + self.package_username + "/" + self.base_package + "\n\n"
        go_mod_content += "go 1.18\n\n"
        if self.avro_annotation:
            go_mod_content += "require (\n"
            go_mod_content += "    github.com/hamba/avro/v2 v2.0.0\n"
            go_mod_content += ")\n"
        
        go_mod_path = os.path.join(self.output_dir, "go.mod")
        with open(go_mod_path, 'w', encoding='utf-8') as file:
            file.write(go_mod_content)

    def write_modname_go_file(self):
        """Writes the modname.go file for the Go project"""
        modname_go_content = ""
        modname_go_content += "package " + self.base_package + "\n\n"
        modname_go_content += "const ModName = \"" + self.base_package + "\"\n"
        
        modname_go_path = os.path.join(self.output_dir, 'pkg', self.base_package, f"{self.base_package}.go")
        with open(modname_go_path, 'w', encoding='utf-8') as file:
            file.write(modname_go_content)

    def convert(self, avro_schema_path: str, output_dir: str):
        """Converts Avro schema to Go"""
        if not self.base_package:
            self.base_package = os.path.splitext(os.path.basename(avro_schema_path))[0]

        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_avro_to_go(avro_schema_path, go_file_path, package_name='', avro_annotation=False, json_annotation=False, package_site='github.com', package_username='username'):
    """Converts Avro schema to Go structs

    Args:
        avro_schema_path (str): Avro input schema path
        go_file_path (str): Output Go file path
        package_name (str): Base package name
        avro_annotation (bool): Include Avro annotations
        json_annotation (bool): Include JSON annotations
    """
    if not package_name:
        package_name = os.path.splitext(os.path.basename(avro_schema_path))[0]

    avrotogo = AvroToGo(package_name)
    avrotogo.avro_annotation = avro_annotation
    avrotogo.json_annotation = json_annotation
    avrotogo.package_site = package_site
    avrotogo.package_username = package_username
    avrotogo.convert(avro_schema_path, go_file_path)


def convert_avro_schema_to_go(avro_schema: JsonNode, output_dir: str, package_name='', avro_annotation=False, json_annotation=False, package_site='github.com', package_username='username'):
    """Converts Avro schema to Go structs

    Args:
        avro_schema (JsonNode): Avro schema as a dictionary or list of dictionaries
        output_dir (str): Output directory path
        package_name (str): Base package name
        avro_annotation (bool): Include Avro annotations
        json_annotation (bool): Include JSON annotations
    """
    avrotogo = AvroToGo(package_name)
    avrotogo.avro_annotation = avro_annotation
    avrotogo.json_annotation = json_annotation
    avrotogo.package_site = package_site
    avrotogo.package_username = package_username
    avrotogo.convert_schema(avro_schema, output_dir)
