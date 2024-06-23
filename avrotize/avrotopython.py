"""Converts Avro schema to Python data classes"""

# pylint: disable=line-too-long,too-many-instance-attributes

import json
import os
import re
import random
from typing import Dict, List, Set, Union, Any, get_args
from avrotize.common import get_typing_args_from_string, is_generic_avro_type, pascal, process_template, build_flat_type_dict, inline_avro_references, is_type_with_alternate, strip_alternate_type

INDENT = '    '


def is_python_reserved_word(word: str) -> bool:
    """Checks if a word is a Python reserved word"""
    reserved_words = [
        'False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await',
        'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except',
        'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is',
        'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return',
        'try', 'while', 'with', 'yield', 'record', 'self', 'cls'
    ]
    return word in reserved_words


class AvroToPython:
    """Converts Avro schema to Python data classes"""

    def __init__(self, base_package: str = '', dataclasses_json_annotation=False, avro_annotation=False) -> None:
        self.base_package = base_package
        self.dataclasses_json_annotation = dataclasses_json_annotation
        self.avro_annotation = avro_annotation
        self.output_dir = os.getcwd()
        self.main_schema = None
        self.type_dict = None
        self.generated_types:Dict[str, str] = {}
        self.avro_generated_types:Dict[str, str] = {}

    def is_python_primitive(self, type_name: str) -> bool:
        """ Checks if a type is a Python primitive type """
        return type_name in ['None', 'bool', 'int', 'float', 'str', 'bytes']
    
    def is_python_typing_struct(self, type_name: str) -> bool:
        """ Checks if a type is a Python typing type """
        return type_name.startswith('Dict[') or type_name.startswith('List[') or type_name.startswith('Optional[') or type_name.startswith('Union[') or type_name == 'Any'

    def map_primitive_to_python(self, avro_type: str) -> str:
        """Maps Avro primitive types to Python types"""
        mapping = {
            'null': 'None',
            'boolean': 'bool',
            'int': 'int',
            'long': 'int',
            'float': 'float',
            'double': 'float',
            'bytes': 'bytes',
            'string': 'str',
        }
        if is_generic_avro_type(avro_type):
            return 'Any'
        return mapping.get(avro_type, avro_type)

    def safe_name(self, name: str) -> str:
        """Converts a name to a safe Python name"""
        if is_python_reserved_word(name):
            return name + "_"
        return name

    def convert_logical_type_to_python(self, avro_type: Dict, import_types: Set[str]) -> str:
        """Converts Avro logical type to Python type"""
        if avro_type['logicalType'] == 'decimal':
            import_types.add('decimal.Decimal')
            return 'Decimal'
        elif avro_type['logicalType'] == 'date':
            import_types.add('datetime.date')
            return 'date'
        elif avro_type['logicalType'] == 'time-millis':
            import_types.add('datetime.time')
            return 'time'
        elif avro_type['logicalType'] == 'time-micros':
            import_types.add('datetime.time')
            return 'time'
        elif avro_type['logicalType'] == 'timestamp-millis':
            import_types.add('datetime.datetime')
            return 'datetime'
        elif avro_type['logicalType'] == 'timestamp-micros':
            import_types.add('datetime.datetime')
            return 'datetime'
        elif avro_type['logicalType'] == 'duration':
            import_types.add('datetime.timedelta')
            return 'timedelta'
        return 'Any'

    def type_name(self, ref: str) -> str:
        """Converts a reference to a type name"""
        return '_'.join([pascal(part) for part in ref.split('.')[-1].split('_')])

    def convert_avro_type_to_python(self, avro_type: Union[str, Dict, List], parent_package: str, import_types: set) -> str:
        """Converts Avro type to Python type"""
        if isinstance(avro_type, str):
            mapped_type = self.map_primitive_to_python(avro_type)
            if mapped_type == avro_type and not self.is_python_primitive(mapped_type):
                python_package = '.'.join(
                    [part.lower() for part in mapped_type.split('.')[:-1]])
                if self.base_package:
                    python_package = f"{self.base_package}.{python_package}"
                python_type = python_package + \
                    '.' + self.type_name(mapped_type)
                import_types.add(python_type)
                return self.type_name(python_type)
            return mapped_type
        elif isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                return 'Any'
            if is_type_with_alternate(avro_type):
                return self.convert_avro_type_to_python(strip_alternate_type(avro_type), parent_package, import_types)
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                t = self.convert_avro_type_to_python(
                    non_null_types[0], parent_package, import_types)
                if 'null' in avro_type:
                    return f"Optional[{t}]"
                else:
                    return t
            else:
                return f"Union[{', '.join(self.convert_avro_type_to_python(t, parent_package, import_types) for t in non_null_types)}]"
        elif isinstance(avro_type, dict):
            if avro_type['type'] == 'record':
                class_ref = self.generate_class(
                    avro_type, parent_package, write_file=True)
                import_types.add(self.base_package + '.' +
                                 class_ref if self.base_package else class_ref)
                return class_ref.split('.')[-1]
            elif avro_type['type'] == 'enum':
                enum_ref = self.generate_enum(
                    avro_type, parent_package, write_file=True)
                import_types.add(self.base_package + '.' +
                                 enum_ref if self.base_package else enum_ref)
                return enum_ref.split('.')[-1]
            elif avro_type['type'] == 'array':
                return f"List[{self.convert_avro_type_to_python(avro_type['items'], parent_package, import_types)}]"
            elif avro_type['type'] == 'map':
                return f"Dict[str,{self.convert_avro_type_to_python(avro_type['values'], parent_package, import_types)}]"
            elif 'logicalType' in avro_type:
                return self.convert_logical_type_to_python(avro_type, import_types)
            return self.convert_avro_type_to_python(avro_type['type'], parent_package, import_types)
        return 'Any'
    
    # pylint: disable=eval-used
    def init_field_value(self, field_type: str, field_name: str, field_is_enum: str, field_ref: str, enum_types: List[str]):
        """ Initialize the field value based on its type. """
        if field_type == "Any":
            return field_ref
        elif field_type in ['int', 'str', 'float', 'bool', 'bytes', 'Decimal', 'datetime', 'date', 'time', 'timedelta']:
            return f"{field_type}({field_ref})"
        elif field_type.startswith("List["):
            inner_type = get_typing_args_from_string(field_type)[0]
            return f"{field_ref} if isinstance({field_ref}, list) else [{self.init_field_value(inner_type, field_name, field_is_enum, 'v', enum_types)} for v in {field_ref}]"
        elif field_type.startswith("Dict["):
            inner_type = get_typing_args_from_string(field_type)[1]
            return f"{field_ref} if isinstance({field_ref}, dict) else {{k: {self.init_field_value(inner_type, field_name, field_is_enum, 'v', enum_types)} for k, v in {field_ref}.items()}}"
        elif field_type.startswith("Optional["):
            inner_type = get_typing_args_from_string(field_type)[0]     
            return self.init_field_value(inner_type, field_name, field_is_enum, field_ref, enum_types)
        elif field_type.startswith("Union["):
            return self.init_field_value_from_union(get_typing_args_from_string(field_type), field_name, field_ref, enum_types)
        elif field_is_enum or field_type in enum_types:
            return f"{field_type}({field_ref})"
        else:
            return f"{field_ref} if isinstance({field_ref}, {field_type}) else {field_type}.from_serializer_dict({field_ref})"

    def init_field_value_from_union(self, union_args: List[str], field_name, field_ref, enum_types):
        """Initialize the field value based on the Union type."""
        init_statements = []
        for field_union_type in union_args:
            init_statements.append(f"{self.init_field_value(field_union_type, field_name, False, field_ref, enum_types)} if isinstance({field_ref}, {field_union_type}) else")
        return ' '.join(init_statements) + ' None'
    
    def init_fields(self, fields: List[Dict[str, Any]], enum_types: List[str]) -> str:
        """Initialize the fields of a class."""
        init_statements = []
        for field in fields:
            if field['is_enum'] or field['type'] in enum_types or field['is_primitive']:
                init_statements.append(f"self.{field['name']}={self.init_field_value(field['type'], field['name'], field['is_enum'], 'kwargs.get('+chr(39)+field['name']+chr(39)+')', enum_types)}")
            else:
                init_statements.append(f"value_{field['name']} = kwargs.get('{field['name']}')")
                init_statements.append(f"self.{field['name']} = {self.init_field_value(field['type'], field['name'], field['is_enum'], 'value_'+field['name'], enum_types)}")
        return '\n'.join(init_statements)
    
    def generate_class(self, avro_schema: Dict, parent_package: str, write_file: bool) -> str:
        """Generates a Python data class from an Avro record schema"""
        import_types: Set[str] = set()
        class_name = '_'.join([pascal(part)
                              for part in avro_schema['name'].split('_')])
        package_name: str = avro_schema.get('namespace', parent_package).lower()
        qualified_name = f'{package_name}.{class_name}' if package_name else class_name
        avro_qualified_name = f'{package_name}.{avro_schema["name"]}' if package_name else avro_schema["name"]
        if qualified_name in self.generated_types:
            return qualified_name

        fields = [{
            'definition': self.generate_field(field, package_name, import_types),
            'docstring': self.generate_field_docstring(field, package_name)
        } for field in avro_schema.get('fields', [])]
        fields = [{
            'name': self.safe_name(field['definition']['name']),
            'original_name': field['definition']['name'],
            'type': field['definition']['type'],
            'is_primitive': field['definition']['is_primitive'],
            'is_enum': field['definition']['is_enum'],
            'docstring': field['docstring'],
            'test_value': self.generate_test_value(field),
        } for field in fields]

        # we are including a copy of the avro schema of this type. Since that may
        # depend on other types, we need to inline all references to other types
        # into this schema
        local_avro_schema = inline_avro_references(
            avro_schema.copy(), self.type_dict, '')
        avro_schema_json = json.dumps(local_avro_schema).replace('"', '\\"')
        
        enum_types = []
        qualified_generated_types = {(self.base_package + '.' + t if self.base_package else t): v for t, v in self.generated_types.items()}
        for import_type in import_types:
            if import_type in qualified_generated_types and qualified_generated_types[import_type] == "enum":
                enum_types.append(import_type.split('.')[-1])

        class_definition = process_template(
            "avrotopython/dataclass_core.jinja",
            class_name=class_name,
            docstring=avro_schema.get('doc', '').strip(
            ) if 'doc' in avro_schema else f'A {class_name} record.',
            fields=fields,
            import_types=import_types,
            base_package=self.base_package,
            avro_annotation=self.avro_annotation,
            dataclasses_json_annotation=self.dataclasses_json_annotation,
            avro_schema_json=avro_schema_json,
            init_fields=self.init_fields(fields, enum_types),
        )
        
        if write_file:
            self.write_to_file(package_name, class_name, class_definition)
            self.generate_test_class(
                package_name, class_name, fields, import_types)
        self.generated_types[qualified_name] = 'class'
        self.avro_generated_types[avro_qualified_name] = 'class'
        return qualified_name

    def generate_enum(self, avro_schema: Dict, parent_package: str, write_file: bool) -> str:
        """Generates a Python enum from an Avro enum schema"""
        class_name = '_'.join([pascal(part)
                              for part in avro_schema['name'].split('_')])
        package_name: str = avro_schema.get('namespace', parent_package).lower()
        qualified_name = f'{package_name}.{class_name}' if package_name else class_name
        avro_qualified_name = f'{package_name}.{avro_schema["name"]}' if package_name else avro_schema["name"]
        if qualified_name in self.generated_types:
            return qualified_name

        symbols = [symbol if not is_python_reserved_word(
            symbol) else symbol + "_" for symbol in avro_schema.get('symbols', [])]

        enum_definition = process_template(
            "avrotopython/enum_core.jinja",
            class_name=class_name,
            docstring=avro_schema.get('doc', '').strip(
            ) if 'doc' in avro_schema else f'A {class_name} enum.',
            symbols=symbols
        )

        if write_file:
            self.write_to_file(package_name, class_name, enum_definition)
            self.generate_test_enum(package_name, class_name, symbols)
        self.generated_types[qualified_name] = 'enum'
        self.avro_generated_types[avro_qualified_name] = 'enum'
        return qualified_name

    def generate_test_class(self, package_name: str, class_name: str, fields: List[Dict[str, str]], import_types: Set[str]) -> None:
        """Generates a unit test class for a Python data class"""
        test_class_name = f"Test_{class_name}"
        base_package = 'tests_'+self.base_package if self.base_package else 'tests'
        test_class_definition = process_template(
            "avrotopython/test_class.jinja",
            test_base_package=base_package,
            base_package=self.base_package,
            package_name=package_name,
            class_name=class_name,
            test_class_name=test_class_name,
            fields=fields,
            avro_annotation=self.avro_annotation,
            import_types=import_types
        )

        base_dir = os.path.join(self.output_dir, "tests")
        test_file_path = os.path.join(
            base_dir, f"test_{self.base_package.replace('.', '_').lower()+'_' if self.base_package else ''}{package_name.replace('.', '_').lower()}_{class_name.lower()}.py")
        if not os.path.exists(os.path.dirname(test_file_path)):
            os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
        with open(test_file_path, 'w', encoding='utf-8') as file:
            file.write(test_class_definition)

    def generate_test_enum(self, package_name: str, class_name: str, symbols: List[str]) -> None:
        """Generates a unit test class for a Python enum"""
        test_class_name = f"Test_{class_name}"
        base_package = 'tests_'+self.base_package if self.base_package else 'tests'
        test_class_definition = process_template(
            "avrotopython/test_enum.jinja",
            test_base_package=base_package,
            base_package=self.base_package,
            package_name=package_name,
            class_name=class_name,
            test_class_name=test_class_name,
            symbols=symbols
        )
        base_dir = os.path.join(self.output_dir, "tests")
        test_file_path = os.path.join(
            base_dir, f"test_{self.base_package.replace('.', '_').lower()+'_' if self.base_package else ''}{package_name.replace('.', '_').lower()}_{class_name.lower()}.py")
        if not os.path.exists(os.path.dirname(test_file_path)):
            os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
        with open(test_file_path, 'w', encoding='utf-8') as file:
            file.write(test_class_definition)

    def generate_test_value(self, field: Dict) -> Any:
        """Generates a test value for a given field"""
        field_type = field['definition']['type']

        def generate_value(field_type: str):
            test_values = {
                'str': chr(39)+''.join([chr(random.randint(97, 122)) for _ in range(0, 20)])+chr(39),
                'bool': str(random.choice([True, False])),
                'int': f'int({random.randint(0, 100)})',
                'float': f'float({random.uniform(0, 100)})',
                'bytes': 'b"test_bytes"',
                'None': 'None',
                'date': random.choice(['datetime.date.today()', 'datetime.date(2021, 1, 1)']),
                'datetime': 'datetime.datetime.now()',
                'time': 'datetime.datetime.now().time()',
                'Decimal': f'Decimal("{random.randint(0, 100)}.{random.randint(0, 100)}")',
                'timedelta': 'datetime.timedelta(days=1)',
                'Any': '{"test": "test"}'
            }

            def resolve(field_type: str) -> str:
                # Regex pattern to find the inner type
                pattern = re.compile(r'^(Optional|List|Dict|Union)\[(.+)\]$')

                match = pattern.match(field_type)
                if not match:
                    return field_type

                outer_type, inner_type = match.groups()

                if outer_type == 'Optional':
                    return inner_type
                elif outer_type == 'List':
                    return resolve(inner_type)
                elif outer_type == 'Dict':
                    # For Dict, only return the value type
                    _, value_type = inner_type.split(',', 1)
                    return resolve(value_type.strip())
                elif outer_type == 'Union':
                    first_type = inner_type.split(',', 1)[0]
                    return resolve(first_type.strip())

                return field_type

            if field_type.startswith('Optional['):
                field_type = resolve(field_type)

            if field_type.startswith('List['):
                field_type = resolve(field_type)
                array_range = random.randint(1, 5)
                return f"[{', '.join([generate_value(field_type) for _ in range(array_range)])}]"
            elif field_type.startswith('Dict['):
                field_type = resolve(field_type)
                dict_range = random.randint(1, 5)
                dict_data = {}
                for _ in range(dict_range):
                    dict_data[''.join([chr(random.randint(97, 122)) for _ in range(
                        0, 20)])] = generate_value(field_type)
                return f"{{{', '.join([chr(39)+key+chr(39)+f': {value}' for key, value in dict_data.items()])}}}"
            elif field_type.startswith('Union['):
                field_type = resolve(field_type)
                return generate_value(field_type)
            return test_values.get(field_type, 'Test_'+field_type + '.create_instance()')

        return generate_value(field_type)

    def is_enum(self, avro_type: Union[str, Dict, List]) -> bool:
        """Checks if a type is an Avro enum"""
        if isinstance(avro_type, list) and len(avro_type) == 2 and 'null' in avro_type:
            return self.is_enum(next(t for t in iter(avro_type) if t != 'null'))
        if isinstance(avro_type, str):
            return avro_type in self.avro_generated_types and self.avro_generated_types[avro_type] == 'enum'
        elif isinstance(avro_type, dict):
            return avro_type['type'] == 'enum'
        return False

    def generate_field(self, field: Dict, parent_package: str, import_types: set) -> Any:
        """Generates a field for a Python data class"""
        field_type = self.convert_avro_type_to_python(
            field['type'], parent_package, import_types)
        field_name = field['name']
        return {
            'name': field_name,
            'type': field_type,
            'is_primitive': self.is_python_primitive(field_type) or self.is_python_typing_struct(field_type),
            'is_enum': self.is_enum(field['type']),
        }

    def generate_field_docstring(self, field: Dict, parent_package: str) -> str:
        """Generates a field docstring for a Python data class"""
        field_type = self.convert_avro_type_to_python(
            field['type'], parent_package, set())
        field_name = self.safe_name(field['name'])
        field_doc = field.get('doc', '').strip()
        if is_python_reserved_word(field_name):
            field_name += "_"
        field_docstring = f"{field_name} ({field_type}): {field_doc}"
        return field_docstring

    def write_to_file(self, package: str, name: str, definition: str):
        """Writes a Python class to a file"""
        if self.base_package:
            package = f"{self.base_package}.{package}"
        directory_path = os.path.join(self.output_dir, "src", package.replace(
            '.', '/').replace('/', os.sep).lower())
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)

        # drop an __init.py__ file in all directories along the path above output_dir
        package_name = package
        while package_name:
            package_directory_path = os.path.join(
                self.output_dir, "src", package_name.replace('.', '/').replace('/', os.sep).lower())
            init_file_path = os.path.join(
                package_directory_path, '__init__.py')
            if not os.path.exists(init_file_path):
                with open(init_file_path, 'w', encoding='utf-8') as file:
                    file.write('')
            if '.' in package_name:
                package_name = package_name.rsplit('.', 1)[0]
            else:
                package_name = ''

        file_path = os.path.join(directory_path, f"{name.lower()}.py")

        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(definition)

    def write_pyproject_toml(self, output_dir: str):
        """Writes pyproject.toml file to the output directory"""
        pyproject_content = process_template(
            "avrotopython/pyproject_toml.jinja",
            package_name=self.base_package.replace('_', '-')
        )
        with open(os.path.join(output_dir, 'pyproject.toml'), 'w', encoding='utf-8') as file:
            file.write(pyproject_content)

    def convert_schemas(self, avro_schemas: List, output_dir: str):
        """ Converts Avro schema to Python data classes"""
        self.main_schema = avro_schemas
        self.type_dict = build_flat_type_dict(avro_schemas)
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
        with open(os.path.join(self.output_dir, "__init__.py"), 'w', encoding='utf-8') as file:
            file.write('')
        for avro_schema in avro_schemas:
            if avro_schema['type'] == 'enum':
                self.generate_enum(
                    avro_schema, self.base_package, write_file=True)
            elif avro_schema['type'] == 'record':
                self.generate_class(
                    avro_schema, self.base_package, write_file=True)
        self.write_pyproject_toml(self.output_dir)

    def convert(self, avro_schema_path: str, output_dir: str):
        """Converts Avro schema to Python data classes"""
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        if isinstance(schema, dict):
            schema = [schema]
        return self.convert_schemas(schema, output_dir)


def convert_avro_to_python(avro_schema_path, py_file_path, package_name='', dataclasses_json_annotation=False, avro_annotation=False):
    """Converts Avro schema to Python data classes"""
    if not package_name:
        package_name = os.path.splitext(os.path.basename(avro_schema_path))[
            0].lower().replace('-', '_')

    avro_to_python = AvroToPython(
        package_name, dataclasses_json_annotation=dataclasses_json_annotation, avro_annotation=avro_annotation)
    avro_to_python.convert(avro_schema_path, py_file_path)


def convert_avro_schema_to_python(avro_schema, py_file_path, package_name='', dataclasses_json_annotation=False, avro_annotation=False):
    """Converts Avro schema to Python data classes"""
    avro_to_python = AvroToPython(
        package_name, dataclasses_json_annotation=dataclasses_json_annotation, avro_annotation=avro_annotation)
    if isinstance(avro_schema, dict):
        avro_schema = [avro_schema]
    avro_to_python.convert_schemas(avro_schema, py_file_path)
