"""Converts Avro schema to Python data classes"""

# pylint: disable=line-too-long,too-many-instance-attributes

import json
import os
import re
import random
from typing import Dict, List, Set, Tuple, Union, Any
from avrotize.common import fullname, get_typing_args_from_string, is_generic_avro_type, pascal, process_template, build_flat_type_dict, inline_avro_references, is_type_with_alternate, strip_alternate_type

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
        self.generated_types: Dict[str, str] = {}

    def is_python_primitive(self, type_name: str) -> bool:
        """ Checks if a type is a Python primitive type """
        return type_name in ['None', 'bool', 'int', 'float', 'str', 'bytes']

    def is_python_typing_struct(self, type_name: str) -> bool:
        """ Checks if a type is a Python typing type """
        return type_name.startswith('typing.Dict[') or type_name.startswith('typing.List[') or type_name.startswith('typing.Optional[') or type_name.startswith('typing.Union[') or type_name == 'typing.Any'

    def safe_name(self, name: str) -> str:
        """Converts a name to a safe Python name"""
        if is_python_reserved_word(name):
            return name + "_"
        return name

    def pascal_type_name(self, ref: str) -> str:
        """Converts a reference to a type name"""
        return '_'.join([pascal(part) for part in ref.split('.')[-1].split('_')])

    def python_package_from_avro_type(self, namespace: str, type_name: str) -> str:
        """Gets the Python package from a type name"""
        type_name_package = '.'.join([part.lower() for part in type_name.split('.')]) if '.' in type_name else type_name.lower()
        if '.' in type_name:
            #  if the type name was already qualified, we don't need to add the namespace
            package = type_name_package
        else:
            namespace_package = '.'.join([part.lower() for part in namespace.split('.')]) if namespace else ''
            package = namespace_package + ('.' if namespace_package and type_name_package else '') + type_name_package
        if self.base_package:
            package = self.base_package + '.' + package
        return package

    def python_type_from_avro_type(self, type_name: str) -> str:
        """Gets the Python class from a type name"""
        return self.pascal_type_name(type_name)

    def python_fully_qualified_name_from_avro_type(self, namespace: str, type_name: str) -> str:
        """
        Gets the fully qualified Python class name from an Avro type.
        """
        package = self.python_package_from_avro_type(namespace, type_name)
        return package + ('.' if package else '') + self.python_type_from_avro_type(type_name)

    def strip_package_from_fully_qualified_name(self, fully_qualified_name: str) -> str:
        """Strips the package from a fully qualified name"""
        return fully_qualified_name.split('.')[-1]

    def map_plain_type_reference_to_python(self, parent_namespace: str, avro_type: str) -> Tuple[bool, str]:
        """
        Maps an Avro type to a Python type

        Args:
            avro_type (str): Avro type

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating
                if the type is a primitive type and the Python type
        """
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
            return True, 'typing.Any'
        mapped = mapping.get(avro_type, None)
        if mapped:
            return True, mapped
        return False, self.python_fully_qualified_name_from_avro_type(parent_namespace, avro_type)

    def convert_logical_type_to_python(self, avro_type: Dict, import_types: Set[str]) -> str:
        """Converts Avro logical type to Python type"""
        if avro_type['logicalType'] == 'decimal':
            import_types.add('decimal.Decimal')
            return 'decimal.Decimal'
        elif avro_type['logicalType'] == 'date':
            import_types.add('datetime.date')
            return 'datetime.date'
        elif avro_type['logicalType'] == 'time-millis':
            import_types.add('datetime.time')
            return 'datetime.time'
        elif avro_type['logicalType'] == 'time-micros':
            import_types.add('datetime.time')
            return 'datetime.time'
        elif avro_type['logicalType'] == 'timestamp-millis':
            import_types.add('datetime.datetime')
            return 'datetime.datetime'
        elif avro_type['logicalType'] == 'timestamp-micros':
            import_types.add('datetime.datetime')
            return 'datetime.datetime'
        elif avro_type['logicalType'] == 'duration':
            import_types.add('datetime.timedelta')
            return 'datetime.timedelta'
        return 'typing.Any'

    def convert_avro_type_to_python(self, avro_type: Union[str, Dict, List], parent_package: str, import_types: set) -> str:
        """Converts Avro type to Python type"""
        if isinstance(avro_type, str):
            is_primitive, mapped_type = self.map_plain_type_reference_to_python(parent_package, avro_type)
            if not is_primitive:
                import_types.add(mapped_type)
                return self.pascal_type_name(mapped_type)
            return mapped_type
        elif isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                return 'typing.Any'
            if is_type_with_alternate(avro_type):
                return self.convert_avro_type_to_python(strip_alternate_type(avro_type), parent_package, import_types)
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                t = self.convert_avro_type_to_python(non_null_types[0], parent_package, import_types)
                if 'null' in avro_type:
                    return f'typing.Optional[{t}]'
                else:
                    return t
            else:
                return f"typing.Union[{', '.join(self.convert_avro_type_to_python(t, parent_package, import_types) for t in non_null_types)}]"
        elif isinstance(avro_type, dict):
            if avro_type['type'] == 'record':
                class_ref = self.generate_class(avro_type, parent_package, write_file=True)
                import_types.add(class_ref)
                return self.strip_package_from_fully_qualified_name(class_ref)
            elif avro_type['type'] == 'enum':
                enum_ref = self.generate_enum(avro_type, parent_package, write_file=True)
                import_types.add(enum_ref)
                return self.strip_package_from_fully_qualified_name(enum_ref)
            elif avro_type['type'] == 'array':
                return f"typing.List[{self.convert_avro_type_to_python(avro_type['items'], parent_package, import_types)}]"
            elif avro_type['type'] == 'map':
                return f"typing.Dict[str,{self.convert_avro_type_to_python(avro_type['values'], parent_package, import_types)}]"
            elif 'logicalType' in avro_type:
                return self.convert_logical_type_to_python(avro_type, import_types)
            return self.convert_avro_type_to_python(avro_type['type'], parent_package, import_types)
        return 'typing.Any'

    # pylint: disable=eval-used
    def init_field_value(self, field_type: str, field_name: str, field_is_enum: bool, field_ref: str, enum_types: List[str]):
        """ Initialize the field value based on its type. """
        if field_type == "typing.Any":
            return field_ref
        elif field_type in ['datetime.datetime', 'datetime.date', 'datetime.time', 'datetime.timedelta']:
            return f"{field_ref}"
        elif field_type in ['int', 'str', 'float', 'bool', 'bytes', 'Decimal']:
            return f"{field_type}({field_ref})"
        elif field_type.startswith("typing.List["):
            inner_type = get_typing_args_from_string(field_type)[0]
            return f"{field_ref} if isinstance({field_ref}, list) else [{self.init_field_value(inner_type, field_name, field_is_enum, 'v', enum_types)} for v in {field_ref}] if {field_ref} else None"
        elif field_type.startswith("typing.Dict["):
            inner_type = get_typing_args_from_string(field_type)[1]
            return f"{field_ref} if isinstance({field_ref}, dict) else {{k: {self.init_field_value(inner_type, field_name, field_is_enum, 'v', enum_types)} for k, v in {field_ref}.items()}} if {field_ref} else None"
        elif field_type.startswith("typing.Optional["):
            inner_type = get_typing_args_from_string(field_type)[0]
            return self.init_field_value(inner_type, field_name, field_is_enum, field_ref, enum_types) + ' if ' + field_ref + ' else None'
        elif field_type.startswith("typing.Union["):
            return self.init_field_value_from_union(get_typing_args_from_string(field_type), field_name, field_ref, enum_types)
        elif field_is_enum or field_type in enum_types:
            return f"{field_type}({field_ref})"
        else:
            return f"{field_ref} if isinstance({field_ref}, {field_type}) else {field_type}.from_serializer_dict({field_ref}) if {field_ref} else None"

    def init_field_value_from_union(self, union_args: List[str], field_name, field_ref, enum_types):
        """Initialize the field value based on the Union type."""
        init_statements = []
        for field_union_type in union_args:
            init_statements.append(
                f"{self.init_field_value(field_union_type, field_name, field_union_type in enum_types, field_ref, enum_types)} if isinstance({field_ref}, {field_union_type}) else")
        return ' '.join(init_statements) + ' None'

    def init_fields(self, fields: List[Dict[str, Any]], enum_types: List[str]) -> str:
        """Initialize the fields of a class."""
        init_statements = []
        for field in fields:
            if field['is_enum'] or field['type'] in enum_types or field['is_primitive']:
                init_statements.append(
                    f"self.{field['name']}={self.init_field_value(field['type'], field['name'], field['is_enum'], 'self.'+field['name'], enum_types)}")
            else:
                init_statements.append(f"value_{field['name']} = self.{field['name']}")
                init_statements.append(
                    f"self.{field['name']} = {self.init_field_value(field['type'], field['name'], field['is_enum'], 'value_'+field['name'], enum_types)}")
        return '\n'.join(init_statements)

    def generate_class(self, avro_schema: Dict, parent_package: str, write_file: bool) -> str:
        """
        Generates a Python data class from an Avro record schema

        Args:
            avro_schema (Dict): Avro record schema
            parent_package (str): Parent package
            write_file (bool): Write the class to a file

        Returns:
            str: Python fully qualified class name
        """

        import_types: Set[str] = set()
        class_name = self.python_type_from_avro_type(avro_schema['name'])
        package_name = self.python_package_from_avro_type(avro_schema.get('namespace', parent_package), avro_schema['name'])
        python_qualified_name = self.python_fully_qualified_name_from_avro_type(avro_schema.get('namespace', parent_package), avro_schema['name'])
        if python_qualified_name in self.generated_types:
            return python_qualified_name

        fields = [{
            'definition': self.generate_field(field, avro_schema.get('namespace', parent_package), import_types),
            'docstring': self.generate_field_docstring(field, avro_schema.get('namespace', parent_package))
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
        local_avro_schema = inline_avro_references(avro_schema.copy(), self.type_dict, '')
        avro_schema_json = json.dumps(local_avro_schema).replace('\\"', '\'').replace('"', '\\"')
        enum_types = []
        for import_type in import_types:
            if import_type in self.generated_types and self.generated_types[import_type] == "enum":
                enum_types.append(self.strip_package_from_fully_qualified_name(import_type))

        class_definition = process_template(
            "avrotopython/dataclass_core.jinja",
            class_name=class_name,
            docstring=avro_schema.get('doc', '').strip() if 'doc' in avro_schema else f'A {class_name} record.',
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
            self.generate_test_class(package_name, class_name, fields, import_types)
        self.generated_types[python_qualified_name] = 'class'
        return python_qualified_name

    def generate_enum(self, avro_schema: Dict, parent_package: str, write_file: bool) -> str:
        """
        Generates a Python enum from an Avro enum schema

        Args:
            avro_schema (Dict): Avro enum schema
            parent_package (str): Parent package
            write_file (bool): Write the enum to a file

        Returns:
            str: Python fully qualified enum name
        """

        class_name = self.python_type_from_avro_type(avro_schema['name'])
        package_name = self.python_package_from_avro_type(avro_schema.get('namespace', parent_package), avro_schema['name'])
        python_qualified_name = self.python_fully_qualified_name_from_avro_type(avro_schema.get('namespace', parent_package), avro_schema['name'])
        if python_qualified_name in self.generated_types:
            return python_qualified_name

        symbols = [symbol if not is_python_reserved_word(
            symbol) else symbol + "_" for symbol in avro_schema.get('symbols', [])]
        ordinals =  avro_schema.get('ordinals', {})

        enum_definition = process_template(
            "avrotopython/enum_core.jinja",
            class_name=class_name,
            docstring=avro_schema.get('doc', '').strip(
            ) if 'doc' in avro_schema else f'A {class_name} enum.',
            symbols=symbols,
            ordinals=ordinals
        )

        if write_file:
            self.write_to_file(package_name, class_name, enum_definition)
            self.generate_test_enum(package_name, class_name, symbols)
        self.generated_types[python_qualified_name] = 'enum'
        return python_qualified_name

    def generate_test_class(self, package_name: str, class_name: str, fields: List[Dict[str, str]], import_types: Set[str]) -> None:
        """Generates a unit test class for a Python data class"""
        test_class_name = f"Test_{class_name}"
        tests_package_name = "test_"+package_name.replace('.', '_').lower()
        test_class_definition = process_template(
            "avrotopython/test_class.jinja",
            package_name=package_name,
            class_name=class_name,
            test_class_name=test_class_name,
            fields=fields,
            avro_annotation=self.avro_annotation,
            import_types=import_types
        )

        base_dir = os.path.join(self.output_dir, "tests")
        test_file_path = os.path.join(base_dir, f"{tests_package_name.replace('.', '_').lower()}.py")
        if not os.path.exists(os.path.dirname(test_file_path)):
            os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
        with open(test_file_path, 'w', encoding='utf-8') as file:
            file.write(test_class_definition)

    def generate_test_enum(self, package_name: str, class_name: str, symbols: List[str]) -> None:
        """Generates a unit test class for a Python enum"""
        test_class_name = f"Test_{class_name}"
        tests_package_name = "test_"+package_name.replace('.', '_').lower()
        test_class_definition = process_template(
            "avrotopython/test_enum.jinja",
            package_name=package_name,
            class_name=class_name,
            test_class_name=test_class_name,
            symbols=symbols
        )
        base_dir = os.path.join(self.output_dir, "tests")
        test_file_path = os.path.join(base_dir, f"{tests_package_name.replace('.', '_').lower()}.py")
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
                'datetime.date': random.choice(['datetime.date.today()', 'datetime.date(2021, 1, 1)']),
                'datetime.datetime': 'datetime.datetime.now(datetime.timezone.utc)',
                'datetime.time': 'datetime.datetime.now(datetime.timezone.utc).time()',
                'decimal.Decimal': f'decimal.Decimal("{random.randint(0, 100)}.{random.randint(0, 100)}")',
                'datetime.timedelta': 'datetime.timedelta(days=1)',
                'typing.Any': '{"test": "test"}'
            }

            def resolve(field_type: str) -> str:
                # Regex pattern to find the inner type
                pattern = re.compile(r'^(?:typing\.)*(Optional|List|Dict|Union)\[(.+)\]$')

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

            if field_type.startswith('typing.Optional['):
                field_type = resolve(field_type)

            if field_type.startswith('typing.List['):
                field_type = resolve(field_type)
                array_range = random.randint(1, 5)
                return f"[{', '.join([generate_value(field_type) for _ in range(array_range)])}]"
            elif field_type.startswith('typing.Dict['):
                field_type = resolve(field_type)
                dict_range = random.randint(1, 5)
                dict_data = {}
                for _ in range(dict_range):
                    dict_data[''.join([chr(random.randint(97, 122)) for _ in range(
                        0, 20)])] = generate_value(field_type)
                return f"{{{', '.join([chr(39)+key+chr(39)+f': {value}' for key, value in dict_data.items()])}}}"
            elif field_type.startswith('typing.Union['):
                field_type = resolve(field_type)
                return generate_value(field_type)
            return test_values.get(field_type, 'Test_'+field_type + '.create_instance()')

        return generate_value(field_type)

    def generate_field(self, field: Dict, parent_package: str, import_types: set) -> Any:
        """Generates a field for a Python data class"""
        field_type = self.convert_avro_type_to_python(field['type'], parent_package, import_types)
        field_name = field['name']
        return {
            'name': field_name,
            'type': field_type,
            'is_primitive': self.is_python_primitive(field_type) or self.is_python_typing_struct(field_type),
            'is_enum': field_type in self.generated_types and self.generated_types[field_type] == 'enum'
        }

    def generate_field_docstring(self, field: Dict, parent_package: str) -> str:
        """Generates a field docstring for a Python data class"""
        field_type = self.convert_avro_type_to_python(field['type'], parent_package, set())
        field_name = self.safe_name(field['name'])
        field_doc = field.get('doc', '').strip()
        if is_python_reserved_word(field_name):
            field_name += "_"
        field_docstring = f"{field_name} ({field_type}): {field_doc}"
        return field_docstring

    def write_to_file(self, package: str, class_name: str, python_code: str):
        """
        Writes a Python class to a file

        Args:
            package (str): Python package
            class_name (str): Python class name
            python_code (str): Python class definition
        """

        # the containing directory is the parent package
        parent_package_name = '.'.join(package.split('.')[:-1])
        parent_package_path = os.sep.join(parent_package_name.split('.')).lower()
        directory_path = os.path.join(self.output_dir, "src", parent_package_path)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{class_name.lower()}.py")

        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(python_code)

    def write_init_files(self):
        """Writes __init__.py files to the output directories"""

        def organize_generated_types():
            """
            Organizes the generated_types into a tree structure
            """
            generated_types_tree = {}
            for generated_type, _ in self.generated_types.items():
                package_parts = generated_type.split('.')
                current_node = generated_types_tree
                count:int = 0
                for part in package_parts[:-1]:
                    count += 1
                    if part not in current_node:
                        current_node[part] = {} if count < len(package_parts) - 1 else generated_type
                    current_node = current_node[part]
                current_node = package_parts[-1]
            return generated_types_tree

        def write_init_files_recursive(generated_types_tree, current_package: str):
            """
            Writes __init__.py files recursively
            """
            import_statements = []
            all_statement = []
            for package_name, package_content in generated_types_tree.items():
                if isinstance(package_content, dict):
                    import_statements.append(f"from .{package_name} import {', '.join(package_content.keys())}")
                    all_statement.append(', '.join(['"'+k+'"' for k in package_content.keys()]))
                    write_init_files_recursive(package_content, current_package + ('.' if current_package else '') + package_name)
                else:
                    class_name = package_content.split('.')[-1]
                    import_statements.append(f"from .{package_name} import {class_name}")
                    all_statement.append('"'+class_name+'"')
            if current_package:
                package_path = os.path.join(self.output_dir, 'src', current_package.replace('.', os.sep).lower())
                init_file_path = os.path.join(package_path, '__init__.py')
                with open(init_file_path, 'w', encoding='utf-8') as file:
                    file.write('\n'.join(import_statements) + '\n\n__all__ = [' + ', '.join(all_statement) + ']\n')

        # main function
        write_init_files_recursive(organize_generated_types(), '')

    def write_pyproject_toml(self):
        """Writes pyproject.toml file to the output directory"""
        pyproject_content = process_template(
            "avrotopython/pyproject_toml.jinja",
            package_name=self.base_package.replace('_', '-')
        )
        with open(os.path.join(self.output_dir, 'pyproject.toml'), 'w', encoding='utf-8') as file:
            file.write(pyproject_content)

    def convert_schemas(self, avro_schemas: List, output_dir: str):
        """ Converts Avro schema to Python data classes"""
        self.main_schema = avro_schemas
        self.type_dict = build_flat_type_dict(avro_schemas)
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
        for avro_schema in avro_schemas:
            if avro_schema['type'] == 'enum':
                self.generate_enum(
                    avro_schema, self.base_package, write_file=True)
            elif avro_schema['type'] == 'record':
                self.generate_class(avro_schema, self.base_package, write_file=True)
        self.write_init_files()
        self.write_pyproject_toml()

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
