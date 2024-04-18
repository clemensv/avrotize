""" Converts Avro schema to Python data classes """
import json
import os
import re
from typing import Dict, List, Set, Union

from avrotize.common import pascal

INDENT = '    '

def is_python_reserved_word(word: str) -> bool:
    """Checks if a word is a Python reserved word"""
    reserved_words = [
        'False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await',
        'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except',
        'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is',
        'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return',
        'try', 'while', 'with', 'yield'
    ]
    return word in reserved_words

class AvroToPython:
    """Converts Avro schema to Python data classes"""
    
    def __init__(self, base_package: str = '', dataclasses_json_annotation=False, avro_annotation=False) -> None:
        self.base_package = base_package
        self.dataclasses_json_annotation = dataclasses_json_annotation
        self.avro_annotation = avro_annotation
        self.output_dir = os.getcwd()

    def is_python_primitive(self, type: str) -> bool:
        """ Checks if a type is a Python primitive type """
        return type in ['None', 'bool', 'int', 'float', 'str', 'bytes']
        
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
        return mapping.get(avro_type, avro_type)
    
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
        return 'Any'
        

    def convert_avro_type_to_python(self, avro_type: Union[str, Dict, List], parent_package: str, import_types: set) -> str:
        """Converts Avro type to Python type"""
        if isinstance(avro_type, str):
            mapped_type = self.map_primitive_to_python(avro_type)
            if mapped_type == avro_type and not self.is_python_primitive(mapped_type):
                import_types.add(mapped_type)
                return pascal(mapped_type.split('.')[-1])
            return mapped_type
        elif isinstance(avro_type, list):
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                t = self.convert_avro_type_to_python(non_null_types[0], parent_package, import_types)
                if 'null' in avro_type:
                    return f"Optional[{t}]"
                else:
                    return t
            else:
                return f"Union[{', '.join(self.convert_avro_type_to_python(t, parent_package, import_types) for t in non_null_types)}]"
        elif isinstance(avro_type, dict):
            if avro_type['type'] == 'record':
                class_ref = self.generate_class(avro_type, parent_package, write_file=True)
                import_types.add(class_ref)
                return class_ref.split('.')[-1]
            elif avro_type['type'] == 'enum':
                enum_ref = self.generate_enum(avro_type, parent_package, write_file=True)
                import_types.add(enum_ref)
                return enum_ref.split('.')[-1]
            elif avro_type['type'] == 'array':
                return f"List[{self.convert_avro_type_to_python(avro_type['items'], parent_package, import_types)}]"
            elif avro_type['type'] == 'map':
                return f"Dict[str, {self.convert_avro_type_to_python(avro_type['values'], parent_package, import_types)}]"
            elif 'logicalType' in avro_type:
                return self.convert_logical_type_to_python(avro_type, import_types)
            return self.convert_avro_type_to_python(avro_type['type'], parent_package, import_types)
        return 'Any'

    def generate_class(self, avro_schema: Dict, parent_package: str, write_file: bool) -> str:
        """Generates a Python data class from an Avro record schema"""
        import_types: Set[str] = set()
        class_name = pascal(avro_schema['name'])
        package_name: str = avro_schema.get('namespace', '')
        if parent_package:
            package_name = f"{parent_package}.{package_name}"        
        fields = [self.generate_field(field, parent_package, import_types) for field in avro_schema.get('fields', [])]
        class_definition = f"@dataclass\nclass {class_name}:\n"
        docstring = avro_schema.get('doc', '').strip() if 'doc' in avro_schema else f'A {class_name} record.'
        class_definition += INDENT + f'"""\n{INDENT}{docstring}\n\n{INDENT}Attributes:\n'
        class_definition += ''.join([self.generate_field_docstring(field, parent_package) for field in avro_schema.get('fields', [])])
        class_definition += INDENT + '"""\n'            
        class_definition += ''.join(fields) if fields else INDENT + "\n"
        
        imports = "from dataclasses import dataclass\n"
        local_imports = ''
        if self.avro_annotation:
            imports += 'import avro.schema\n'            
            avro_schema_json = json.dumps(avro_schema)
            avro_schema_json = avro_schema_json.replace('"', '§')           
            avro_schema_json = f"\"+\n{' '*8}\"".join([avro_schema_json[i:i+70] for i in range(0, len(avro_schema_json), 70)])
            avro_schema_json = avro_schema_json.replace('§', '\\"')
            class_definition += f'\n{INDENT}AvroType: ClassVar[avro.schema.Schema] = avro.schema.parse(\n{INDENT*2}"{avro_schema_json}");\n'        
        
        if self.dataclasses_json_annotation:
            imports += "from dataclasses_json import dataclass_json\n"
            class_definition = class_definition.replace('@dataclass', '@dataclass_json\n@dataclass')
        for import_type in import_types:
            import_type_package = import_type.rsplit('.',1)[0]
            import_type_type = pascal(import_type.split('.')[-1])
            if import_type_package.startswith(package_name):
                import_type_package = import_type_package[len(package_name):]
            if import_type_package:
                imp = f"from {import_type_package.lower()} import {import_type_type}\n"
                if import_type_package.startswith('.'):
                    local_imports += imp
                else:
                    imports += imp
            else:
                local_imports += f"from .{import_type_type.lower()} import {import_type_type}\n"
        class_definition = imports + '\n' + local_imports + '\n' + class_definition

        if write_file:
            self.write_to_file(package_name, class_name, class_definition)
        return f'{package_name}.{class_name}' if package_name else class_name
    
    def generate_enum(self, avro_schema: Dict, parent_package: str, write_file: bool) -> str:
        """Generates a Python enum from an Avro enum schema"""
        class_name = pascal(avro_schema['name'])
        package_name: str = avro_schema.get('namespace', '')
        if parent_package:
            package_name = f"{parent_package}.{package_name}"
        symbols = avro_schema.get('symbols', [])
        enum_definition = f"class {class_name}(Enum):\n"
        docstring = avro_schema.get('doc', '').strip() if 'doc' in avro_schema else f'A {class_name} enum.'
        enum_definition += INDENT + f'"""\n{INDENT}{docstring}"""\n\n'
        for i, symbol in enumerate(symbols):
            if is_python_reserved_word(symbol):
                symbol += "_"
            enum_definition += INDENT + f"{symbol} = {i}\n"
        
        imports = 'from enum import Enum\n'
        if write_file:
            self.write_to_file(package_name, class_name, imports + '\n' + enum_definition)
        return f'{package_name}.{class_name}' if package_name else class_name
    

    def generate_field(self, field: Dict, parent_package: str, import_types : set) -> str:
        """Generates a field for a Python data class"""
        field_type = self.convert_avro_type_to_python(field['type'], parent_package, import_types)
        field_name = field['name']
        if is_python_reserved_word(field_name):
            field_name += "_"
        field_definition = INDENT + f"{field_name}: {field_type}\n"
        return field_definition
    
    def generate_field_docstring(self, field: Dict, parent_package: str) -> str:
        """Generates a field docstring for a Python data class"""
        field_type = self.convert_avro_type_to_python(field['type'], parent_package, set())
        field_name = field['name']
        field_doc = field.get('doc', '').strip()
        if is_python_reserved_word(field_name):
            field_name += "_"
        field_docstring = INDENT*2 + f"{field_name} ({field_type}): {field_doc}\n"
        return field_docstring
    
    def write_to_file(self, package:str, name: str, definition: str):
        """Writes a Python class to a file"""
        directory_path = os.path.join(self.output_dir, package.replace('.', '/').replace('/', os.sep).lower())
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        
        # drop an __init.py__ file in all directories along the path above output_dir
        package_name = package
        while package_name:
            init_file_path = os.path.join(directory_path, '__init__.py')
            if not os.path.exists(init_file_path):
                with open(init_file_path, 'w', encoding='utf-8') as file:
                    file.write('')
            if '.' in package_name:
                package_name = package_name.rsplit('.', 1)[0]
            else:
                package_name = ''
            
        file_path = os.path.join(directory_path, f"{name.lower()}.py")

        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(f'""" {name} """\n\n')
            file.write("# pylint: disable=invalid-name,line-too-long,too-many-instance-attributes\n\n")
            
            pattern = r"(?<!\w)(Dict|List|Union|Optional|Any|ClassVar)"
            references = set(re.findall(pattern, definition))
            if references:
                file.write(f'from typing import {",".join(references)}\n')
            file.write('\n'+definition)

    def convert_schemas(self, avro_schemas: List, output_dir: str):
        """ Converts Avro schema to Python data classes"""
        self.output_dir = output_dir
        for avro_schema in avro_schemas:
            if avro_schema['type'] == 'enum':
                self.generate_enum(avro_schema, self.base_package, write_file=True)
            elif avro_schema['type'] == 'record':
                self.generate_class(avro_schema, self.base_package, write_file=True)
    
    def convert(self, avro_schema_path: str, output_dir: str):
        """Converts Avro schema to Python data classes"""
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        if isinstance(schema, dict):
            schema = [schema]
        return self.convert_schemas(schema, output_dir)

def convert_avro_to_python(avro_schema_path, py_file_path, package_name = '', dataclasses_json_annotation = False, avro_annotation = False):
    """Converts Avro schema to Python data classes"""
    avro_to_python = AvroToPython(package_name, dataclasses_json_annotation=dataclasses_json_annotation, avro_annotation=avro_annotation)
    avro_to_python.convert(avro_schema_path, py_file_path)

def convert_avro_schema_to_python(avro_schema, py_file_path, package_name = '', dataclasses_json_annotation = False, avro_annotation = False):
    """Converts Avro schema to Python data classes"""
    avro_to_python = AvroToPython(package_name, dataclasses_json_annotation=dataclasses_json_annotation, avro_annotation=avro_annotation)
    if isinstance(avro_schema, dict):
        avro_schema = [avro_schema]
    avro_to_python.convert_schemas(avro_schema, py_file_path)
