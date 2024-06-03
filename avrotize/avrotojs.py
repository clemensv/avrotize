""" Convert Avro schema to TypeScript classes """

import json
import os
from typing import Any, Dict, List, Set, Union

from avrotize.common import pascal

INDENT = ' ' * 4

def is_javascript_reserved_word(word: str) -> bool:
    """ Check if word is a TypeScript reserved word """
    reserved_words = [
        'break', 'case', 'catch', 'class', 'const', 'continue', 'debugger',
        'default', 'delete', 'do', 'else', 'export', 'extends', 'finally',
        'for', 'function', 'if', 'import', 'in', 'instanceof', 'new', 'return',
        'super', 'switch', 'this', 'throw', 'try', 'typeof', 'var', 'void',
        'while', 'with', 'yield'
    ]
    return word in reserved_words

def is_javascript_primitive(word: str) -> bool:
    """ Check if word is a TypeScript primitive """
    primitives = ['null', 'boolean', 'number', 'string', 'Date']
    return word in primitives

class AvroToJavaScript:
    """ Convert Avro schema to TypeScript classes """
    
    def __init__(self, base_package: str = '', avro_annotation=False) -> None:
        self.base_package = base_package
        self.avro_annotation = avro_annotation
        self.output_dir = os.getcwd()

    def map_primitive_to_javascript(self, avro_type: str) -> str:
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

    def convert_logical_type_to_javascript(self, avro_type: Dict) -> str:
        """ Convert Avro logical type to TypeScript type """
        if 'logicalType' in avro_type:
            if avro_type['logicalType'] in ['decimal', 'uuid']:
                return 'string'
            if avro_type['logicalType'] in ['date', 'time-millis', 'time-micros', 'timestamp-millis', 'timestamp-micros']:
                return 'Date'
            if avro_type['logicalType'] == 'duration':
                return 'string'
        return 'any'
    
    def is_javascript_primitive(self, avro_type: str) -> bool:
        """ Check if Avro type is a TypeScript primitive """
        return avro_type in ['null', 'boolean', 'number', 'string', 'Date']

    def convert_avro_type_to_javascript(self, avro_type: Union[str, Dict, List], parent_namespace: str, import_types: set) -> str | List[Any]:
        """ Convert Avro type to TypeScript type """
        if isinstance(avro_type, str):
            mapped_type = self.map_primitive_to_javascript(avro_type)
            if mapped_type == avro_type and not self.is_javascript_primitive(mapped_type):
                import_types.add(mapped_type)
                return pascal(mapped_type.split('.')[-1])
            return mapped_type
        elif isinstance(avro_type, list):
            return [self.convert_avro_type_to_javascript(t, parent_namespace, import_types) for t in avro_type]
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
                return [self.convert_avro_type_to_javascript(avro_type["items"], parent_namespace, import_types)]
            if 'type' in avro_type and avro_type['type'] == 'map':
                return [self.convert_avro_type_to_javascript(avro_type["values"], parent_namespace, import_types)]
            if 'logicalType' in avro_type:
                return self.convert_logical_type_to_javascript(avro_type)
            return self.convert_avro_type_to_javascript(avro_type['type'], parent_namespace, import_types)
        return 'any'

    def generate_class(self, avro_schema: Dict, parent_namespace: str) -> str:
        """ Generate TypeScript class from Avro record """
        
        def add_check(arr_check, ft):
            if isinstance(ft, list):
                for ft2 in ft:
                    add_check(arr_check, ft2)                
            elif ft == 'null':
                arr_check.append('v !== null')
            elif is_javascript_primitive(ft):
                arr_check.append(f'typeof v !== "{ft}"')
            else:
                arr_check.append(f'!(v instanceof {ft})')
        
        import_types: Set[str] = set()
        class_name = pascal(avro_schema['name'])
        namespace = avro_schema.get('namespace', '')
        if not namespace and parent_namespace:
            namespace = parent_namespace
        if self.base_package:
            namespace = f'{self.base_package}.{namespace}'
        fields = avro_schema.get('fields', [])
        doc = avro_schema.get('doc', '')
        
        constructor_body = ''
        class_body = ''
        if self.avro_annotation:
            avro_schema_json = json.dumps(avro_schema)
            avro_schema_json = avro_schema_json.replace('"', '§')           
            avro_schema_json = f"\"+\n{' '*8}\"".join([avro_schema_json[i:i+80] for i in range(0, len(avro_schema_json), 80)])
            avro_schema_json = avro_schema_json.replace('§', '\\"')
            class_body += f'{class_name}.AvroType = avro.parse("{avro_schema_json}");\n'
        for field in fields:
            field_name = field['name']
            field_doc = field.get('doc', '')
            field_avro_type = field['type']
            if is_javascript_reserved_word(field_name):
                field_name += '_'
            field_type = self.convert_avro_type_to_javascript(field['type'], namespace, import_types)
            if field_doc:
                constructor_body += f'{INDENT}/** {field_doc} */\n'
            constructor_body += f'{INDENT}_{field_name} = null;\n'
            class_body += f'Object.defineProperty({class_name}.prototype, "{field_name}", {{\n'
            class_body += f'{INDENT}get: function() {{'+'\n'
            class_body += f'{INDENT}{INDENT}return this._{field_name};\n'
            class_body += f'{INDENT}}},\n'
            class_body += f'{INDENT}set: function(value) {{'+'\n'
            type_check = []
            if field_avro_type == 'array':
                arr = '!Array.isArray(value) || value.some(v => '
                arr_check: List[str] = []
                for ft in field_type if isinstance(field_type, list) else [field_type]:
                    add_check(arr_check, ft)
                arr += ' || '.join(arr_check) + ')'
                type_check.append(arr)
            elif field_avro_type == 'map':
                map = '!Object.entries(value).every(([k, v]) => typeof k === "string" && '
                map_check: List[str] = []
                for ft in field_type if isinstance(field_type, list) else [field_type]:
                    add_check(map_check, ft)
                map += ' && '.join(map_check) + ')'
                type_check.append(map)
            else:
                for ft in field_type if isinstance(field_type, list) else [field_type]:
                    add_check(type_check, ft)
            class_body += f'{INDENT}{INDENT}if ( {" && ".join(type_check)} ) throw new Error(`Invalid type for {field_name}. Expected {field_type}, got ${{value}}`);\n'
            class_body += f'{INDENT}{INDENT}this._{field_name} = value;\n'
            class_body += f'{INDENT}}}'+'\n'
            class_body += '});\n\n'
        
        imports = ''
        if self.avro_annotation:
            imports += "var avro = require('avro-js');\n"
        for import_type in import_types:
            import_type_package = import_type.rsplit('.',1)[0]
            import_type_type = pascal(import_type.split('.')[-1])
            import_type_package = import_type_package.replace('.', '/')
            namespace_path = namespace.replace('.', '/')
            
            if import_type_package:# get the relative path from the namespace to the import_type_package
                import_type_package = os.path.relpath(import_type_package, namespace_path).replace(os.sep, '/')
                if not import_type_package.startswith('.'):
                    import_type_package = f'./{import_type_package}'
                imports += f"var {import_type_type} = require('{import_type_package}/{import_type_type}').{import_type_type};\n"
            else:
                imports += f"var {import_type_type} = require('{import_type_type}'){import_type_type};\n"
        
        class_definition = imports + '\n'
        if doc:
            class_definition += f'/** {doc} */\n'        
        class_definition += f"function {class_name}() {{\n{constructor_body}}}\n\n{class_body}\nmodule.exports = {class_name};\n"
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
            if is_javascript_reserved_word(symbol):
                symbol += '_'
            enum_body += f'{INDENT}{symbol}: "{symbol}",\n'
            
        enum_definition = ''
        if 'doc' in avro_schema:
            enum_definition += f"/** {avro_schema['doc']} */\n"
        enum_definition += f"const {enum_name} = Object.freeze({{\n{enum_body}}});\n\n"
        enum_definition += f"module.exports = {enum_name};\n"
        self.write_to_file(namespace, enum_name, enum_definition)
        return f'{namespace}.{enum_name}'

    def write_to_file(self, namespace: str, name: str, content: str):
        """ Write TypeScript class to file """
        directory_path = os.path.join(self.output_dir, namespace.replace('.', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        
        file_path = os.path.join(directory_path, f"{name}.js")
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

def convert_avro_to_javascript(avro_schema_path, js_dir_path, package_name='', avro_annotation=False):
    """ Convert Avro schema to TypeScript classes """
    
    if not package_name:
        package_name = os.path.splitext(os.path.basename(avro_schema_path))[0].replace('-', '_')
    
    converter = AvroToJavaScript(package_name, avro_annotation=avro_annotation)
    converter.convert(avro_schema_path, js_dir_path)
    
def convert_avro_schema_to_javascript(avro_schema, js_dir_path, package_name='', avro_annotation=False):
    """ Convert Avro schema to TypeScript classes """
    converter = AvroToJavaScript(package_name, avro_annotation=avro_annotation)
    converter.convert_schema(avro_schema, js_dir_path)
