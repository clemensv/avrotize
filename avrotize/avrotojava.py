import os
import json
from typing import List, Dict, Union, Any, Tuple
from avrotize.common import build_flat_type_dict, inline_avro_references, process_template, pascal

JsonNode = Union[Dict[str, Any], List[Any], str, None]

INDENT = '    '

def flatten_type_name(name: str) -> str:
    """Strips the namespace and special characters from a name"""
    if name.endswith('[]'):
        return flatten_type_name(name[:-2] + 'Array')
    base_name = pascal(name.replace(' ', '').split('.')[-1].replace('>', '').replace('<', '').replace(',', ''))
    return base_name


def is_java_reserved_word(word: str) -> bool:
    """Checks if a word is a Java reserved word"""
    reserved_words = [
        'abstract', 'assert', 'boolean', 'break', 'byte', 'case', 'catch', 'char', 'class', 'const',
        'continue', 'default', 'do', 'double', 'else', 'enum', 'extends', 'final', 'finally', 'float',
        'for', 'goto', 'if', 'implements', 'import', 'instanceof', 'int', 'interface', 'long', 'native',
        'new', 'package', 'private', 'protected', 'public', 'return', 'short', 'static', 'strictfp',
        'super', 'switch', 'synchronized', 'this', 'throw', 'throws', 'transient', 'try', 'void', 'volatile',
        'while', 'true', 'false', 'null', 'record',
    ]
    return word in reserved_words


class AvroToJava:
    """Converts Avro schema to Java classes, including Jackson annotations and Avro SpecificRecord methods"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package
        self.output_dir = os.getcwd()
        self.avro_annotation = False
        self.jackson_annotations = False
        self.pascal_properties = False
        self.generated_types_avro_namespace: Dict[str, str] = {}
        self.generated_types_java_package: Dict[str, str] = {}
        self.schema_doc: JsonNode = None
        self.type_dict: Dict[str, Any] = {}
        self.imports: Dict[str, set] = {}
        self.generated_avro_types: Dict[str, JsonNode] = {}

    def qualified_name(self, package: str, name: str) -> str:
        """Concatenates package and name using a dot separator"""
        return f"{package.lower()}.{name}" if package else name

    def join_packages(self, parent_package: str, package: str) -> str:
        """Joins package and name using a dot separator"""
        if parent_package and package:
            return f"{parent_package}.{package}".lower()
        elif parent_package:
            return parent_package.lower()
        return package.lower()

    class JavaType:
        """Java type definition"""

        def __init__(self, type_name: str, union_types: List['AvroToJava.JavaType'] = None, is_class: bool = False, is_enum: bool = False) -> None:
            self.type_name = type_name
            self.union_types = union_types or []
            self.is_class = is_class
            self.is_enum = is_enum

    def safe_identifier(self, name: str, class_name: str = '') -> str:
        """Converts a name to a safe Java identifier"""
        if is_java_reserved_word(name):
            return f"_{name}"
        if class_name and name == class_name:
            return f"{name}_"
        return name

    def map_primitive_to_java(self, avro_type: str, is_optional: bool) -> 'AvroToJava.JavaType':
        """Maps Avro primitive types to Java types"""
        optional_mapping = {
            'null': 'Void',
            'boolean': 'Boolean',
            'int': 'Integer',
            'long': 'Long',
            'float': 'Float',
            'double': 'Double',
            'bytes': 'byte[]',
            'string': 'String',
        }
        required_mapping = {
            'null': 'void',
            'boolean': 'boolean',
            'int': 'int',
            'long': 'long',
            'float': 'float',
            'double': 'double',
            'bytes': 'byte[]',
            'string': 'String',
        }
        if '.' in avro_type:
            type_name = avro_type.split('.')[-1]
            package_name = '.'.join(avro_type.split('.')[:-1]).lower()
            avro_type = self.qualified_name(package_name, type_name)
        if avro_type in self.generated_types_avro_namespace:
            kind = self.generated_types_avro_namespace[avro_type]
            qualified_class_name = self.qualified_name(self.base_package, avro_type)
            return AvroToJava.JavaType(qualified_class_name, is_class=(kind == "class"), is_enum=(kind == "enum"))
        else:
            return AvroToJava.JavaType(required_mapping.get(avro_type, avro_type) if not is_optional else optional_mapping.get(avro_type, avro_type))

    def is_java_primitive(self, java_type: 'AvroToJava.JavaType') -> bool:
        """Checks if a Java type is a primitive type"""
        return java_type.type_name in [
            'void', 'boolean', 'int', 'long', 'float', 'double', 'byte[]', 'String',
            'Boolean', 'Integer', 'Long', 'Float', 'Double', 'Void']

    def is_java_optional_type(self, java_type: 'AvroToJava.JavaType') -> bool:
        """Checks if a Java type is an optional type"""
        return java_type.type_name in ['Void', 'Boolean', 'Integer', 'Long', 'Float', 'Double']

    def is_java_numeric_type(self, java_type: 'AvroToJava.JavaType') -> bool:
        """Checks if a Java type is a numeric type"""
        return java_type.type_name in ['int', 'long', 'float', 'double', 'Integer', 'Long', 'Float', 'Double']

    def is_java_integer_type(self, java_type: 'AvroToJava.JavaType') -> bool:
        """Checks if a Java type is an integer type"""
        return java_type.type_name in ['int', 'long', 'Integer', 'Long']

    def convert_avro_type_to_java(self, class_name: str, field_name: str, avro_type: Union[str, Dict, List], parent_package: str, nullable: bool = False) -> 'AvroToJava.JavaType':
        """Converts Avro type to Java type"""
        package = self.join_packages(self.base_package, parent_package).lower()
        qualified_class_name = self.qualified_name(package.replace('/', '.'), class_name)
        if isinstance(avro_type, str):
            return self.map_primitive_to_java(avro_type, nullable)
        elif isinstance(avro_type, list):
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                return self.convert_avro_type_to_java(class_name, field_name, non_null_types[0], parent_package, nullable=True)
            else:
                # Handle union types
                union_types = [self.convert_avro_type_to_java(class_name, field_name, t, parent_package) for t in non_null_types]
                return AvroToJava.JavaType(self.generate_union_class_jackson(class_name, field_name, non_null_types, parent_package), union_types=union_types, is_class=True)
        elif isinstance(avro_type, dict):
            avro_type_type = avro_type.get('type')
            if avro_type_type in ['record', 'enum']:
                return self.generate_class_or_enum(avro_type, parent_package)
            elif avro_type_type == 'array':
                item_type = self.convert_avro_type_to_java(class_name, field_name, avro_type['items'], parent_package, nullable=True).type_name
                self.imports.setdefault(qualified_class_name, set()).add('java.util.ArrayList')
                return AvroToJava.JavaType(f"ArrayList<{item_type}>")
            elif avro_type_type == 'map':
                value_type = self.convert_avro_type_to_java(class_name, field_name, avro_type['values'], parent_package, nullable=True).type_name
                self.imports.setdefault(qualified_class_name, set()).add('java.util.Map')
                return AvroToJava.JavaType(f"Map<String, {value_type}>")
            elif 'logicalType' in avro_type:
                logical_type = avro_type['logicalType']
                if logical_type == 'decimal':
                    self.imports.setdefault(qualified_class_name, set()).add('java.math.BigDecimal')
                    return AvroToJava.JavaType('BigDecimal')
                elif logical_type == 'date':
                    self.imports.setdefault(qualified_class_name, set()).add('java.time.LocalDate')
                    return AvroToJava.JavaType('LocalDate')
                elif logical_type in ['time-millis', 'time-micros']:
                    self.imports.setdefault(qualified_class_name, set()).add('java.time.LocalTime')
                    return AvroToJava.JavaType('LocalTime')
                elif logical_type in ['timestamp-millis', 'timestamp-micros']:
                    self.imports.setdefault(qualified_class_name, set()).add('java.time.Instant')
                    return AvroToJava.JavaType('Instant')
                elif logical_type == 'uuid':
                    self.imports.setdefault(qualified_class_name, set()).add('java.util.UUID')
                    return AvroToJava.JavaType('UUID')
            else:
                return self.convert_avro_type_to_java(class_name, field_name, avro_type_type, parent_package)
        return AvroToJava.JavaType('Object')

    def generate_class_or_enum(self, avro_schema: Dict, parent_package: str) -> 'AvroToJava.JavaType':
        """Generates a Java class or enum from an Avro schema"""
        avro_type = avro_schema.get('type')
        if avro_type == 'record':
            return self.generate_class(avro_schema, parent_package)
        elif avro_type == 'enum':
            return self.generate_enum(avro_schema, parent_package)
        return AvroToJava.JavaType('Object')

    def generate_class(self, avro_schema: Dict, parent_package: str) -> 'AvroToJava.JavaType':
        """Generates a Java class from an Avro record schema"""
        namespace = avro_schema.get('namespace', parent_package)
        package = self.join_packages(self.base_package, namespace).lower()
        class_name = self.safe_identifier(avro_schema['name'])
        qualified_class_name = self.qualified_name(package.replace('/', '.'), class_name)
        avro_namespace_name = self.qualified_name(namespace, avro_schema['name'])
        if avro_namespace_name in self.generated_types_avro_namespace:
            return AvroToJava.JavaType(qualified_class_name, is_class=True)
        self.generated_types_avro_namespace[avro_namespace_name] = "class"
        self.generated_types_java_package[qualified_class_name] = "class"
        self.generated_avro_types[qualified_class_name] = avro_schema

        # Generate fields
        fields = []
        for field in avro_schema.get('fields', []):
            field_name = self.safe_identifier(field['name'], class_name)
            java_type = self.convert_avro_type_to_java(class_name, field_name, field['type'], namespace)
            fields.append({
                'name': field_name,
                'type': java_type.type_name,
                'original_name': field['name'],
                'is_enum': java_type.is_enum,
                'is_class': java_type.is_class,
                'union_types': [ut.type_name for ut in java_type.union_types],
                'doc': field.get('doc', ''),
                'java_type': java_type,
                'test_value': self.get_test_value(java_type.type_name) if not java_type.is_enum else java_type.type_name+".values()[0]"
            })
            if java_type.is_class or java_type.is_enum:
                # Add import for this type
                self.imports.setdefault(qualified_class_name, set()).add(java_type.type_name)

        # Generate the isJsonMatch method
        is_json_match_method = ''
        if self.jackson_annotations:
            is_json_match_method = self.create_is_json_match_method(class_name, fields, namespace)
            self.imports.setdefault(qualified_class_name, set()).add('com.fasterxml.jackson.databind.JsonNode')
            self.imports.setdefault(qualified_class_name, set()).add('java.util.function.Predicate')

        if self.avro_annotation:
            local_avro_schema = inline_avro_references(avro_schema.copy(), self.type_dict, '')
            avro_schema_json = json.dumps(local_avro_schema)
            # wrap schema at 80 characters
            avro_schema_json = avro_schema_json.replace('\\\"', '⁜')
            avro_schema_json = avro_schema_json.replace('\"', '※')
            avro_schema_json = f'\"+\n{INDENT}\"'.join(
                 [avro_schema_json[i:i+80] for i in range(0, len(avro_schema_json), 80)])
            avro_schema_json = avro_schema_json.replace('※', '\\\"')
            avro_schema_json = avro_schema_json.replace('⁜', '\\\\\\\"')
        
        # Generate class content using the template
        context = {
            'package_name': package.replace('/', '.'),
            'class_name': class_name,
            'fields': fields,
            'imports': sorted(self.imports.get(qualified_class_name, set())),
            'jackson_annotation': self.jackson_annotations,
            'avro_annotation': self.avro_annotation,
            'avro_schema': avro_schema_json if self.avro_annotation else None,
            'is_json_match_method': is_json_match_method
        }

        class_content = process_template('avrotojava/dataclass_core.java.jinja', **context)
        self.write_to_file(package, class_name, class_content)

        # Generate test class
        test_content = process_template('avrotojava/dataclass_test.java.jinja', **context)
        self.write_test_file(package, class_name, test_content)

        return AvroToJava.JavaType(qualified_class_name, is_class=True)


    def get_test_value(self, java_type: str) -> str:
        """Returns a default test value based on the Avro type"""
        test_values = {
            'String': '"test_string"',
            'boolean': 'true',
            'int': '42',
            'long': '42L',
            'float': '3.14f',
            'double': '3.14',
            'byte[]': 'new byte[]{0x01, 0x02, 0x03}',
            'null': 'null',
            'LocalDate': 'java.time.LocalDate.now()',
            'LocalTime': 'java.time.LocalTime.now()',
            'Instant': 'java.time.Instant.now()',
            'UUID': 'java.util.UUID.randomUUID()',
            'Boolean' : 'true',
            'Integer' : '42',
            'Long' : '42L',
            'Float' : '3.14f',
            'Double' : '3.14',
        }
        if java_type.endswith('?'):
            java_type = java_type[:-1]
        return test_values.get(java_type, f'new {java_type}()')
    
    def generate_enum(self, avro_schema: Dict, parent_package: str) -> 'AvroToJava.JavaType':
        """Generates a Java enum from an Avro enum schema"""
        namespace = avro_schema.get('namespace', parent_package)
        package = self.join_packages(self.base_package, namespace).lower()
        enum_name = self.safe_identifier(avro_schema['name'])
        qualified_enum_name = self.qualified_name(package.replace('/', '.'), enum_name)
        avro_namespace_name = self.qualified_name(namespace, avro_schema['name'])
        if avro_namespace_name in self.generated_types_avro_namespace:
            return AvroToJava.JavaType(qualified_enum_name, is_enum=True)
        self.generated_types_avro_namespace[avro_namespace_name] = "enum"
        self.generated_types_java_package[qualified_enum_name] = "enum"
        self.generated_avro_types[qualified_enum_name] = avro_schema

        symbols = avro_schema.get('symbols', [])

        # Generate enum content using the template
        context = {
            'package_name': package.replace('/', '.'),
            'enum_name': enum_name,
            'enum_values': symbols,
            'jackson_annotation': self.jackson_annotations
        }

        enum_content = process_template('avrotojava/enum_core.java.jinja', **context)
        self.write_to_file(package, enum_name, enum_content)

        # Generate test class
        test_content = process_template('avrotojava/enum_test.java.jinja', test_package_name=package.replace('/', '.'), enum_name=enum_name, enum_values=symbols)
        self.write_test_file(package, enum_name, test_content)

        return AvroToJava.JavaType(qualified_enum_name, is_enum=True)

    def generate_union_class_jackson(self, class_name: str, field_name: str, avro_types: List[Union[str, Dict]], parent_package: str) -> str:
        """Generates a Union class using Jackson for handling union types"""
        union_class_name = f"{class_name}{pascal(field_name)}Union"
        package = self.join_packages(self.base_package, parent_package).lower()
        qualified_union_name = self.qualified_name(package.replace('/', '.'), union_class_name)
        if qualified_union_name in self.generated_types_avro_namespace:
            return qualified_union_name
        self.generated_types_avro_namespace[qualified_union_name] = "union"
        self.generated_types_java_package[qualified_union_name] = "union"

        union_types = [self.convert_avro_type_to_java(class_name, field_name, t, parent_package, nullable=True) for t in avro_types]

        # Generate union class content using the template
        context = {
            'package_name': package.replace('/', '.'),
            'union_class_name': union_class_name,
            'union_types': [ut.type_name for ut in union_types],
            'jackson_annotation': self.jackson_annotations,
            'avro_annotation': self.avro_annotation,
            'imports': sorted(self.imports.get(union_class_name, set()))
        }

        union_content = process_template('avrotojava/unionclass_core.java.jinja', **context)
        self.write_to_file(package, union_class_name, union_content)

        # Generate test class
        test_content = process_template('avrotojava/unionclass_test.java.jinja', test_package_name=package.replace('/', '.'), union_class_name=union_class_name, union_types=[ut.type_name for ut in union_types], jackson_annotation=self.jackson_annotations)
        self.write_test_file(package, union_class_name, test_content)

        return qualified_union_name

    def write_to_file(self, package: str, name: str, content: str):
        """Writes content to a Java file"""
        directory_path = os.path.join(self.output_dir, 'src/main/java', package.replace('.', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}.java")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)

    def write_test_file(self, package: str, name: str, content: str):
        """Writes content to a Java test file"""
        directory_path = os.path.join(self.output_dir, 'src/test/java', package.replace('.', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}Test.java")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)

    def generate_pom(self, group_id: str, artifact_id: str, version: str):
        """Generates a Maven POM file"""
        context = {
            'group_id': group_id,
            'artifact_id': artifact_id,
            'version': version,
            'jdk_version': '21',
            'jackson_annotation': self.jackson_annotations,
            'avro_annotation': self.avro_annotation
        }
        content = process_template('avrotojava/pom.xml.jinja', **context)
        file_path = os.path.join(self.output_dir, 'pom.xml')
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)

    def process_schema(self, schema: Union[Dict, List], parent_namespace: str = ''):
        """Processes Avro schema and generates the necessary classes and files"""
        if isinstance(schema, list):
            for entry in schema:
                self.process_schema(entry, parent_namespace)
        elif isinstance(schema, dict):
            avro_type = schema.get('type')
            if avro_type == 'record':
                self.generate_class(schema, parent_namespace)
            elif avro_type == 'enum':
                self.generate_enum(schema, parent_namespace)
            elif avro_type == 'fixed':
                pass  # Handle fixed types if necessary
            elif avro_type == 'array':
                self.convert_avro_type_to_java('', '', schema, parent_namespace)
            elif avro_type == 'map':
                self.convert_avro_type_to_java('', '', schema, parent_namespace)

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts Avro schema to Java"""
        self.output_dir = output_dir
        self.schema_doc = schema
        self.type_dict = build_flat_type_dict(self.schema_doc)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        # Generate POM file
        package_elements = self.base_package.split('.') if self.base_package else ["com", "example"]
        group_id = '.'.join(package_elements[:-1]) if len(package_elements) > 1 else package_elements[0]
        artifact_id = package_elements[-1]
        self.generate_pom(group_id, artifact_id, '1.0-SNAPSHOT')
        self.process_schema(schema)

    def convert(self, avro_schema_path: str, output_dir: str):
        """Converts Avro schema file to Java classes and generates project files"""
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)

    def create_is_json_match_method(self, class_name: str, fields: List[Dict], parent_package: str) -> str:
        """Generates the isJsonMatch method for a class using Jackson"""
        method_body = ''
        predicates = ''
        for field in fields:
            field_name = field['original_name']
            field_type = field['java_type']
            predicate, clause = self.get_is_json_match_clause(class_name, field_name, field_type)
            method_body += f"{INDENT*2}{clause} &&\n"
            if predicate:
                predicates += f"{INDENT*2}{predicate}\n"

        # Remove the trailing ' &&\n' from method_body
        method_body = method_body.rstrip(' &&\n')

        method = f"public static boolean isJsonMatch(JsonNode node) {{\n"
        if predicates:
            method += predicates
        method += f"{INDENT*2}return {method_body};\n"
        method += f"{INDENT}}}\n"
        return method

    def get_is_json_match_clause(self, class_name: str, field_name: str, field_type: 'AvroToJava.JavaType') -> Tuple[str, str]:
        """Generates the isJsonMatch clause for a field using Jackson"""
        predicate = ''
        clause = ''
        node_check = f"node.has(\"{field_name}\")"

        if field_type.type_name in ['String', 'char', 'CharSequence']:
            clause = f"(!{node_check} || node.get(\"{field_name}\").isTextual())"
        elif field_type.type_name in ['int', 'Integer']:
            clause = f"(!{node_check} || node.get(\"{field_name}\").canConvertToInt())"
        elif field_type.type_name in ['long', 'Long']:
            clause = f"(!{node_check} || node.get(\"{field_name}\").canConvertToLong())"
        elif field_type.type_name in ['float', 'Float']:
            clause = f"(!{node_check} || node.get(\"{field_name}\").isFloat())"
        elif field_type.type_name in ['double', 'Double']:
            clause = f"(!{node_check} || node.get(\"{field_name}\").isDouble())"
        elif field_type.type_name in ['boolean', 'Boolean']:
            clause = f"(!{node_check} || node.get(\"{field_name}\").isBoolean())"
        elif field_type.type_name == 'byte[]':
            clause = f"(!{node_check} || node.get(\"{field_name}\").isBinary())"
        elif field_type.type_name.startswith('ArrayList<'):
            predicate_var = f"val_{field_name}"
            item_type = field_type.type_name[5:-1]
            predicate_test = self.predicate_test(item_type)
            predicate = f"Predicate<JsonNode> {predicate_var} = n -> !n.isArray() || !n.elements().hasNext() || n.elements().next(){predicate_test};"
            clause = f"(!{node_check} || {predicate_var}.test(node.get(\"{field_name}\")))"
        elif field_type.type_name.startswith('Map<'):
            predicate_var = f"val_{field_name}"
            value_type = field_type.type_name[field_type.type_name.find(',') + 1:-1]
            predicate_test = self.predicate_test(value_type)
            predicate = f"Predicate<JsonNode> {predicate_var} = n -> !n.isObject() || !n.fields().hasNext() || n.fields().next().getValue(){predicate_test};"
            clause = f"(!{node_check} || {predicate_var}.test(node.get(\"{field_name}\")))"
        elif field_type.is_class:
            clause = f"(!{node_check} || {field_type.type_name}.isJsonMatch(node.get(\"{field_name}\")))"
        elif field_type.is_enum:
            clause = f"(!{node_check} || (node.get(\"{field_name}\").isTextual() && Enum.valueOf({field_type.type_name}.class, node.get(\"{field_name}\").asText()) != null))"
        else:
            clause = f"(!{node_check})"

        return predicate, clause

    def predicate_test(self, item_type: str) -> str:
        """Generates the predicate test for a list or map"""
        if item_type == 'String':
            return '.isTextual()'
        elif item_type in ['int', 'Integer']:
            return '.canConvertToInt()'
        elif item_type in ['long', 'Long']:
            return '.canConvertToLong()'
        elif item_type in ['float', 'Float', 'double', 'Double']:
            return '.isNumber()'
        elif item_type in ['boolean', 'Boolean']:
            return '.isBoolean()'
        elif item_type == 'byte[]':
            return '.isBinary()'
        else:
            return '.isObject()'
        
def convert_avro_to_java(avro_schema_path: str, java_file_path: str, package_name: str = 'com.example', pascal_properties: bool = False, jackson_annotation: bool = False, avro_annotation: bool = False):
    """Entry function to initiate Avro schema conversion to Java classes."""
    avro_to_java = AvroToJava(base_package=package_name)
    avro_to_java.jackson_annotations = jackson_annotation
    avro_to_java.avro_annotation = avro_annotation
    avro_to_java.pascal_properties = pascal_properties
    avro_to_java.convert(avro_schema_path, java_file_path)
