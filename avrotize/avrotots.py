""" Convert Avro schema to TypeScript classes """

import json
import os
from typing import Dict, List, Set, Union

from avrotize.common import pascal

def is_typescript_reserved_word(word: str) -> bool:
    """ Check if word is a TypeScript reserved word """
    reserved_words = [
        'break', 'case', 'catch', 'class', 'const', 'continue', 'debugger',
        'default', 'delete', 'do', 'else', 'export', 'extends', 'finally',
        'for', 'function', 'if', 'import', 'in', 'instanceof', 'new', 'return',
        'super', 'switch', 'this', 'throw', 'try', 'typeof', 'var', 'void',
        'while', 'with', 'yield'
    ]
    return word in reserved_words

class AvroToTypeScript:
    """ Convert Avro schema to TypeScript classes """
    
    def __init__(self, base_package: str = '', typed_json_annotation=False, avro_annotation=False) -> None:
        self.base_package = base_package
        self.typed_json_annotation = typed_json_annotation
        self.avro_annotation = avro_annotation
        self.output_dir = os.getcwd()

    def map_primitive_to_typescript(self, avro_type: str) -> str:
        """ Map Avro primitive type to TypeScript type """
        mapping = {
            'null': 'null',
            'boolean': 'boolean',
            'int': 'number',
            'long': 'number',
            'float': 'number',
            'double': 'number',
            'bytes': 'string',
            'string': 'string',
        }
        return mapping.get(avro_type, avro_type)

    def convert_logical_type_to_typescript(self, avro_type: Dict) -> str:
        """ Convert Avro logical type to TypeScript type """
        if 'logicalType' in avro_type:
            if avro_type['logicalType'] in ['decimal', 'uuid']:
                return 'string'
            if avro_type['logicalType'] in ['date', 'time-millis', 'time-micros', 'timestamp-millis', 'timestamp-micros']:
                return 'Date'
            if avro_type['logicalType'] == 'duration':
                return 'string'
        return 'any'
    
    def is_typescript_primitive(self, avro_type: str) -> bool:
        """ Check if Avro type is a TypeScript primitive """
        return avro_type in ['null', 'boolean', 'number', 'string', 'Date']

    def convert_avro_type_to_typescript(self, avro_type: Union[str, Dict, List], parent_namespace: str, import_types: set) -> str:
        """ Convert Avro type to TypeScript type """
        if isinstance(avro_type, str):
            mapped_type = self.map_primitive_to_typescript(avro_type)
            if mapped_type == avro_type and not self.is_typescript_primitive(mapped_type):
                import_types.add(mapped_type)
                return pascal(mapped_type.split('.')[-1])
            return mapped_type
        elif isinstance(avro_type, list):
            types = [self.convert_avro_type_to_typescript(t, parent_namespace, import_types) for t in avro_type if t != 'null']
            if len(types) == 1 and 'null' in avro_type:
                return f'{types[0]} | null'
            return ' | '.join(types)
        elif isinstance(avro_type, dict):
            if 'type' in avro_type and avro_type['type'] == 'record':
                class_ref = self.generate_class(avro_type, parent_namespace)
                import_types.add(class_ref)
                return class_ref.split('.')[-1]
            elif 'type' in avro_type and avro_type['type'] == 'enum':
                enum_ref = self.generate_enum(avro_type, parent_namespace)
                import_types.add(enum_ref)
                return enum_ref.split('.')[-1]
            elif 'type' in avro_type and avro_type['type'] == 'array':
                return f'{self.convert_avro_type_to_typescript(avro_type["items"], parent_namespace, import_types)}[]'
            if 'type' in avro_type and avro_type['type'] == 'map':
                return f'{{ [key: string]: {self.convert_avro_type_to_typescript(avro_type["values"], parent_namespace, import_types)} }}'
            if 'logicalType' in avro_type:
                return self.convert_logical_type_to_typescript(avro_type)
            return self.convert_avro_type_to_typescript(avro_type['type'], parent_namespace, import_types)
        return 'any'

    def generate_class(self, avro_schema: Dict, parent_namespace: str) -> str:
        """ Generate TypeScript class from Avro record """
        import_types: Set[str] = set()
        class_name = pascal(avro_schema['name'])
        namespace = avro_schema.get('namespace', '')
        if not namespace and parent_namespace:
            namespace = parent_namespace
        if self.base_package:
            namespace = f'{self.base_package}.{namespace}'
        fields = avro_schema.get('fields', [])
        doc = avro_schema.get('doc', '')
        
        class_body = ''
        if self.avro_annotation:
            avro_schema_json = json.dumps(avro_schema)
            avro_schema_json = avro_schema_json.replace('"', '§')           
            avro_schema_json = f"\"+\n{' '*8}\"".join([avro_schema_json[i:i+80] for i in range(0, len(avro_schema_json), 80)])
            avro_schema_json = avro_schema_json.replace('§', '\\"')
            class_body += f'    static AvroType : Type = avro.parse("{avro_schema_json}");\n'
        for field in fields:
            field_name = field['name']
            field_doc = field.get('doc', '')
            if is_typescript_reserved_word(field_name):
                field_name += '_'
            field_type = self.convert_avro_type_to_typescript(field['type'], namespace, import_types)
            if field_doc:
                class_body += f'    /** {field_doc} */\n'
            if self.typed_json_annotation:
                class_body += '    @jsonMember\n'
            class_body += f'    {field_name}: {field_type};\n'
        
        imports = ''
        if self.typed_json_annotation:
            imports += "import { jsonObject, jsonMember } from 'typedjson';\n"
        if self.avro_annotation:
            imports += "import { avro, Type } from 'avro-js';\n"
        for import_type in import_types:
            import_type_package = import_type.rsplit('.',1)[0]
            import_type_type = pascal(import_type.split('.')[-1])
            import_type_package = import_type_package.replace('.', '/')
            namespace_path = namespace.replace('.', '/')
            
            if import_type_package:# get the relative path from the namespace to the import_type_package
                import_type_package = os.path.relpath(import_type_package, namespace_path).replace(os.sep, '/')
                if not import_type_package.startswith('.'):
                    import_type_package = f'./{import_type_package}'
                imports += f"import {{ {import_type_type} }} from '{import_type_package}/{import_type_type}';\n"
            else:
                imports += f"import {{ {import_type_type} }} from '.{import_type_type}';\n"
        
        class_definition = imports + '\n'
        if doc:
            class_definition += f'/** {doc} */\n'        
        if self.typed_json_annotation:
            class_definition += "@jsonObject\n"
        class_definition += f"export class {class_name} {{\n{class_body}}}\n"
        self.write_to_file(namespace, class_name, class_definition)
        return f'{namespace}.{class_name}'

    def generate_enum(self, avro_schema: Dict, parent_namespace: str) -> str:
        """ Generate TypeScript enum from Avro enum """
        enum_name = pascal(avro_schema['name'])
        namespace = avro_schema.get('namespace', '')
        if not namespace and parent_namespace:
            namespace = parent_namespace
        if self.base_package:
            namespace = f'{self.base_package}.{namespace}'
        symbols = avro_schema.get('symbols', [])
        
        enum_body = ''
        for symbol in symbols:
            if is_typescript_reserved_word(symbol):
                symbol += '_'
            enum_body += f'    {symbol} = "{symbol}",\n'
        
        enum_definition = f"export enum {enum_name} {{\n{enum_body}}}\n"
        self.write_to_file(namespace, enum_name, enum_definition)
        return f'{namespace}.{enum_name}'

    def write_to_file(self, namespace: str, name: str, content: str):
        """ Write TypeScript class to file """
        directory_path = os.path.join(self.output_dir, namespace.replace('.', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        
        file_path = os.path.join(directory_path, f"{name}.ts")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)

    def convert_schema(self, schema: List|Dict, output_dir: str):
        """ Convert Avro schema to TypeScript classes """
        self.output_dir = output_dir
        if isinstance(schema, dict):
            schema = [schema]
        for avro_schema in schema:
            if avro_schema['type'] == 'record':
                self.generate_class(avro_schema, self.base_package)
            elif avro_schema['type'] == 'enum':
                self.generate_enum(avro_schema, self.base_package)
                
    def convert(self, avro_schema_path: str, output_dir: str):
        """ Convert Avro schema to TypeScript classes """
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)

def convert_avro_to_typescript(avro_schema_path, js_dir_path, package_name='', typedjson_annotation=False, avro_annotation=False):
    """ Convert Avro schema to TypeScript classes """
    converter = AvroToTypeScript(package_name, typed_json_annotation=typedjson_annotation, avro_annotation=avro_annotation)
    converter.convert(avro_schema_path, js_dir_path)
    
def convert_avro_schema_to_typescript(avro_schema, js_dir_path, package_name='', typedjson_annotation=False, avro_annotation=False):
    """ Convert Avro schema to TypeScript classes """
    converter = AvroToTypeScript(package_name, typed_json_annotation=typedjson_annotation, avro_annotation=avro_annotation)
    converter.convert_schema(avro_schema, js_dir_path)
