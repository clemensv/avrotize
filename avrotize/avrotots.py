import json
import os
import re
import zlib
from typing import Dict, List, Set, Union

from avrotize.common import pascal, is_generic_avro_type

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

    INDENT = '    '

    def __init__(self, base_package: str = '', typed_json_annotation=False, avro_annotation=False) -> None:
        self.base_package = base_package
        self.typed_json_annotation = typed_json_annotation
        self.avro_annotation = avro_annotation
        self.output_dir = os.getcwd()
        self.src_dir = os.path.join(self.output_dir, "src")
        self.generated_types: Dict[str, str] = {}

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

    def convert_avro_type_to_typescript(self, avro_type: Union[str, Dict, List], parent_namespace: str, import_types: set, class_name: str = '', field_name: str = '') -> str:
        """ Convert Avro type to TypeScript type """
        if isinstance(avro_type, str):
            mapped_type = self.map_primitive_to_typescript(avro_type)
            if mapped_type == avro_type and not self.is_typescript_primitive(mapped_type):
                import_types.add(mapped_type)
                return pascal(mapped_type.split('.')[-1])
            return mapped_type
        elif isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                return '{ [key: string]: any }'
            if 'null' in avro_type:
                if len(avro_type) == 2:
                    return f'{self.convert_avro_type_to_typescript([t for t in avro_type if t != "null"][0], parent_namespace, import_types, class_name, field_name)} | null'
                return f'{self.generate_embedded_union(class_name, field_name, avro_type, parent_namespace)} | null'
            return self.generate_embedded_union(class_name, field_name, avro_type, parent_namespace)
        elif isinstance(avro_type, dict):
            if 'type' in avro_type and avro_type['type'] == 'record':
                class_ref = self.generate_class(avro_type, parent_namespace, write_file=True)
                import_types.add(class_ref)
                return class_ref.split('.')[-1]
            elif 'type' in avro_type and avro_type['type'] == 'enum':
                enum_ref = self.generate_enum(avro_type, parent_namespace, write_file=True)
                import_types.add(enum_ref)
                return enum_ref.split('.')[-1]
            elif 'type' in avro_type and avro_type['type'] == 'array':
                return f'{self.convert_avro_type_to_typescript(avro_type["items"], parent_namespace, import_types, class_name, field_name)}[]'
            if 'type' in avro_type and avro_type['type'] == 'map':
                return f'{{ [key: string]: {self.convert_avro_type_to_typescript(avro_type["values"], parent_namespace, import_types, class_name, field_name)} }}'
            if 'logicalType' in avro_type:
                return self.convert_logical_type_to_typescript(avro_type)
            return self.convert_avro_type_to_typescript(avro_type['type'], parent_namespace, import_types, class_name, field_name)
        return 'any'

    def get_qualified_name(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        return f"{namespace}.{name}" if namespace != '' else name
    
    def concat_namespace(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        if namespace and name:
            return f"{namespace}.{name}"
        return namespace or name
    
    def generate_class_or_enum(self, avro_schema: Dict, parent_namespace: str, write_file: bool = True) -> str:
        """ Generates a Class or Enum """
        if avro_schema['type'] == 'record':
            return self.generate_class(avro_schema, parent_namespace, write_file)
        elif avro_schema['type'] == 'enum':
            return self.generate_enum(avro_schema, parent_namespace, write_file)
        return ''

    def generate_class(self, avro_schema: Dict, parent_namespace: str, write_file: bool = True) -> str:
        """ Generate TypeScript class from Avro record """
        import_types: Set[str] = set()
        class_name = pascal(avro_schema['name'])
        
        namespace = self.concat_namespace(self.base_package, avro_schema.get('namespace', parent_namespace))
        fields = avro_schema.get('fields', [])
        doc = avro_schema.get('doc', '')

        class_body = ''
        constructor_body = ''
        constructor_params = ''

        if self.avro_annotation:
            avro_schema_json = json.dumps(avro_schema)
            avro_schema_json = avro_schema_json.replace('"', '\\"')
            avro_schema_json = f"\"+\n{' '*8}\"".join([avro_schema_json[i:i+80] for i in range(0, len(avro_schema_json), 80)])
            class_body += f'    static AvroType : Type = avro.parse("{avro_schema_json}");\n'
        for field in fields:
            field_name = field['name']
            field_doc = field.get('doc', '')
            if is_typescript_reserved_word(field_name):
                field_name += '_'
            field_type = self.convert_avro_type_to_typescript(field['type'], namespace, import_types, class_name, field_name)
            if field_doc:
                class_body += f'    /** {field_doc} */\n'
            if self.typed_json_annotation:
                class_body += '    @jsonMember\n'
            class_body += f'    {field_name}: {field_type};\n'
            constructor_params += f'{field_name}: {field_type}, '
            constructor_body += f'        this.{field_name} = {field_name};\n'

        imports = ''
        if self.typed_json_annotation or self.avro_annotation:
            imports += "import pako from 'pako';\n"
        if self.typed_json_annotation:
            imports += "import 'reflect-metadata';\n"
            imports += "import { jsonObject, jsonMember, TypedJSON  } from 'typedjson';\n"
        if self.avro_annotation:
            imports += "import { avro, Type } from 'avro-js';\n"
        for import_type in import_types:
            import_type_package = import_type.rsplit('.',1)[0]
            import_type_type = pascal(import_type.split('.')[-1])
            import_type_package = import_type_package.replace('.', '/')
            namespace_path = namespace.replace('.', '/')

            if import_type_package:
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
        class_definition += f"export class {class_name} {{\n{class_body}\n"
        class_definition += f'    constructor({constructor_params.rstrip(", ")}) {{\n{constructor_body}    }}\n'
        class_definition += self.generate_to_byte_array_method(class_name)
        class_definition += self.generate_from_data_method(class_name)
        class_definition += self.generate_is_json_match_method(avro_schema, namespace, class_name)
        class_definition += f"}}\n"

        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
        ref = f'{namespace}.{class_name}'
        self.generated_types[ref] = "class"
        return ref

    def generate_enum(self, avro_schema: Dict, parent_namespace: str, write_file: bool = True) -> str:
        """ Generate TypeScript enum from Avro enum """
        enum_name = pascal(avro_schema['name'])
        namespace = self.concat_namespace(self.base_package, avro_schema.get('namespace', parent_namespace))
        symbols = avro_schema.get('symbols', [])

        enum_body = ''
        for symbol in symbols:
            if is_typescript_reserved_word(symbol):
                symbol += '_'
            enum_body += f'    {symbol} = "{symbol}",\n'

        enum_definition = f"export enum {enum_name} {{\n{enum_body}}}\n"

        if write_file:
            self.write_to_file(namespace, enum_name, enum_definition)
        ref = f'{namespace}.{enum_name}'
        self.generated_types[ref] = "enum"
        return ref

    def generate_to_byte_array_method(self, class_name: str) -> str:
        """ Generate the toByteArray method for serialization """
        parts = []
        parts.append(f"    public toByteArray(contentTypeString: string): Uint8Array {{\n")
        parts.append(f"        const contentType = new TextEncoder().encode(contentTypeString);\n")
        parts.append(f"        let result: Uint8Array | null = null;\n")

        if self.avro_annotation:
            parts.append(f"\n        if (contentTypeString.startsWith('avro/binary') || contentTypeString.startsWith('application/vnd.apache.avro+avro')) {{\n")
            parts.append(f"            const avroType = {class_name}.AvroType;\n")
            parts.append(f"            result = avroType.toBuffer(this);\n")
            parts.append(f"        }}\n")

        if self.typed_json_annotation:
            parts.append(f"        if (contentTypeString.startsWith('application/json')) {{\n")
            parts.append(f"            result = new TextEncoder().encode(JSON.stringify(this));\n")
            parts.append(f"        }}\n")

        parts.append(f"\n        if (result && contentTypeString.endsWith('+gzip')) {{\n")
        parts.append(f"            result = pako.gzip(result);\n")
        parts.append(f"        }}\n")
        parts.append(f"\n        if (result) {{\n")
        parts.append(f"            return result;\n")
        parts.append(f"        }} else {{\n")
        parts.append(f"            throw new Error(`Unsupported media type: ${{contentTypeString}}`);\n")
        parts.append(f"        }}\n")
        parts.append(f"    }}\n")
        return ''.join(parts)

    def generate_from_data_method(self, class_name: str) -> str:
        """ Generate the fromData method for deserialization """
        parts = []
        parts.append(f"    public static fromData(data: any, contentTypeString: string): {class_name} {{\n")
        parts.append(f"        const contentType = new TextEncoder().encode(contentTypeString);\n")

        parts.append(f"\n        if (contentTypeString.endsWith('+gzip')) {{\n")
        parts.append(f"            data = pako.ungzip(data);\n")
        parts.append(f"        }}\n")

        if self.avro_annotation:
            parts.append(f"\n        if (contentTypeString.startsWith('avro/') || contentTypeString.startsWith('application/vnd.apache.avro')) {{\n")
            parts.append(f"            const avroType = {class_name}.AvroType;\n")
            parts.append(f"            if (contentTypeString.startsWith('avro/binary') || contentTypeString.startsWith('application/vnd.apache.avro+avro')) {{\n")
            parts.append(f"                return avroType.fromBuffer(data);\n")
            parts.append(f"            }} else if (contentTypeString.startsWith('avro/json') || contentTypeString.startsWith('application/vnd.apache.avro+json')) {{\n")
            parts.append(f"                return avroType.fromString(data);\n")
            parts.append(f"            }}\n")
            parts.append(f"        }}\n")

        if self.typed_json_annotation:
            parts.append(f"        if (contentTypeString.startsWith('application/json')) {{\n")
            parts.append(f"            return JSON.parse(data);\n")
            parts.append(f"        }}\n")

        parts.append(f"\n        throw new Error(`Unsupported media type: ${{contentTypeString}}`);\n")
        parts.append(f"    }}\n")
        return ''.join(parts)

    def generate_is_json_match_method(self, avro_schema: Dict, parent_namespace: str, class_name: str) -> str:
        """ Generate the isJsonMatch method """
        class_definition = ''
        class_definition += f"\n\n{self.INDENT}/// <summary>\n{self.INDENT}/// Checks if the JSON element matches the schema\n{self.INDENT}/// </summary>"
        class_definition += f"\n{self.INDENT}public static isJsonMatch(element: any): boolean {{"
        class_definition += f"\n{self.INDENT*2}return "
        namespace = self.concat_namespace(self.base_package, avro_schema.get('namespace', parent_namespace))
       
        field_count = 0
        for field in avro_schema.get('fields', []):
            if field_count > 0:
                class_definition += f" && \n{self.INDENT*3}"
            field_count += 1
            field_name = field['name']
            if is_typescript_reserved_word(field_name):
                field_name = f"@{field_name}"
            field_type = self.convert_avro_type_to_typescript(field["type"], namespace, set())
            class_definition += self.get_is_json_match_clause(field_name, field_type)
        if field_count == 0:
            class_definition += "true"
        class_definition += f";\n{self.INDENT}}}"
        return class_definition

    def get_is_json_match_clause(self, field_name: str, field_type: str) -> str:
        """ Generates the isJsonMatch clause for a field """
        field_name_js = field_name.rstrip('_')
        is_optional = field_type.endswith(' | null')
        field_type = field_type.replace(' | null', '')

        if '|' in field_type:
            union_types = field_type.split('|')
            union_clauses = [self.get_is_json_match_clause(field_name, union_type) for union_type in union_types]
            clause = f"({') && ('.join(union_clauses)})"
            return clause
        
        clause = f"element.hasOwnProperty('{field_name_js}') && ("

        if field_type == 'string':
            clause += f"typeof element['{field_name_js}'] === 'string'"
        elif field_type == 'number':
            clause += f"typeof element['{field_name_js}'] === 'number'"
        elif field_type == 'boolean':
            clause += f"typeof element['{field_name_js}'] === 'boolean'"
        elif field_type == 'Date':
            clause += f"typeof element['{field_name_js}'] === 'string' && !isNaN(Date.parse(element['{field_name_js}']))"
        elif field_type.startswith('{ [key: string]:'):
            clause += f"typeof element['{field_name_js}'] === 'object' && !Array.isArray(element['{field_name_js}'])"
        elif field_type.endswith('[]'):
            clause += f"Array.isArray(element['{field_name_js}'])"
        else:
            clause += f"{field_type}.isJsonMatch(element['{field_name_js}'])"

        if is_optional:
            clause += f" || element['{field_name_js}'] === null"

        clause += ")"
        return clause

    def generate_embedded_union(self, class_name: str, field_name: str, avro_type: List, parent_namespace: str, write_file: bool = True) -> str:
        """ Generate embedded Union class for a field """
        union_class_name = pascal(field_name) + 'Union' if field_name else pascal(class_name) + 'Union'
        namespace = parent_namespace
        union_types = [self.convert_avro_type_to_typescript(t, parent_namespace, set()) for t in avro_type if t != 'null']
        import_types = []
        for t in avro_type:
            if isinstance(t, str) and t in self.generated_types:
                import_types.append(t)
            elif isinstance(t, dict) and 'type' in t and t['type'] == "array" and isinstance(t['items'], str) and t['items'] in self.generated_types:
                import_types.append(t['items'])
            elif isinstance(t, dict) and 'type' in t and t['type'] == "map" and isinstance(t['values'], str) and t['values'] in self.generated_types:
                import_types.append(t['values'])
        if not import_types:
            return '|'.join(union_types)
        class_definition = ''
        for import_type in import_types:
            import_type_package = import_type.rsplit('.',1)[0]
            import_type_type = pascal(import_type.split('.')[-1])
            import_type_package = import_type_package.replace('.', '/')
            namespace_path = namespace.replace('.', '/')

            if import_type_package:
                import_type_package = os.path.relpath(import_type_package, namespace_path).replace(os.sep, '/')
                if not import_type_package.startswith('.'):
                    import_type_package = f'./{import_type_package}'
                class_definition += f"import {{ {import_type_type} }} from '{import_type_package}/{import_type_type}';\n"
            else:
                class_definition += f"import {{ {import_type_type} }} from '.{import_type_type}';\n"

        class_definition += f"\nclass {union_class_name} {{\n"

        class_definition += f"{self.INDENT}private value: any;\n\n"

        # Constructor
        class_definition += f"{self.INDENT}constructor(value: { ' | '.join(union_types) }) {{\n"
        class_definition += f"{self.INDENT*2}this.value = value;\n"
        class_definition += f"{self.INDENT}}}\n\n"

        # Method to check which type is set
        for union_type in union_types:
            type_check_method = f"{self.INDENT}public is{pascal(union_type)}(): boolean {{\n"
            if union_type == 'string' or union_type == 'number' or union_type == 'boolean':
                type_check_method += f"{self.INDENT*2}return typeof this.value === '{union_type}';\n"
            elif union_type == 'Date':
                type_check_method += f"{self.INDENT*2}return this.value instanceof Date;\n"
            else:
                type_check_method += f"{self.INDENT*2}return this.value instanceof {union_type};\n"
            type_check_method += f"{self.INDENT}}}\n\n"
            class_definition += type_check_method

        # Method to return the current value
        class_definition += f"{self.INDENT}public toObject(): any {{\n"
        class_definition += f"{self.INDENT*2}return this.value;\n"
        class_definition += f"{self.INDENT}}}\n\n"

        # Method to check if JSON matches any of the union types
        class_definition += f"{self.INDENT}public static isJsonMatch(element: any): boolean {{\n"
        match_clauses = []
        for union_type in union_types:
           match_clauses.append(f"({self.get_is_json_match_clause('value', union_type)})")
        class_definition += f"{self.INDENT*2}return {'||'.join(match_clauses)};\n"
        class_definition += f"{self.INDENT}}}\n\n"

        # Method to deserialize from JSON
        class_definition += f"{self.INDENT}public static fromData(element: any, contentType: string): {union_class_name} {{\n"
        class_definition += f"{self.INDENT*2}for (const type of [{', '.join([t for t in union_types if not self.is_typescript_primitive(t)])}]) {{\n"
        class_definition += f"{self.INDENT*3}if (type.isJsonMatch(element)) {{\n"
        class_definition += f"{self.INDENT*4}return new {union_class_name}(type.fromData(element, contentType));\n"
        class_definition += f"{self.INDENT*3}}}\n"
        class_definition += f"{self.INDENT*2}}}\n"
        class_definition += f"{self.INDENT*2}throw new Error('No matching type for union');\n"
        class_definition += f"{self.INDENT}}}\n"

        class_definition += "}\n"

        if write_file:
            self.write_to_file(namespace, union_class_name, class_definition)

        return union_class_name

    def write_to_file(self, namespace: str, name: str, content: str):
        """ Write TypeScript class to file """
        directory_path = os.path.join(self.src_dir, namespace.replace('.', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)

        file_path = os.path.join(directory_path, f"{name}.ts")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)

    def generate_project_files(self, output_dir: str):
        """ Generate project files like tsconfig.json, package.json, and .gitignore """
        tsconfig_content = {
            "compilerOptions": {
                "target": "ES6",
                "module": "commonjs",
                "strict": True,
                "esModuleInterop": True,
                "skipLibCheck": True,
                "forceConsistentCasingInFileNames": True,
                "outDir": "./dist",
                "rootDir": "./src",               
                "experimentalDecorators": True,
                "emitDecoratorMetadata": True
            },
            "include": [
                "src/**/*"
            ]
        }

        package_json_content = {
            "name": "avro-to-typescript",
            "version": "1.0.0",
            "description": "Generated TypeScript classes from Avro schema",
            "main": "dist/index.js",
            "scripts": {
                "build": "tsc",
                "start": "node dist/index.js"
            },
            "dependencies": {
                "avro-js": "^1.11.3",
                "typedjson": "^1.8.0",
                "pako": "^2.1.0"
            },
            "devDependencies": {
                "typescript": "^5.4.5"
            },
            "author": "",
            "license": "ISC"
        }

        gitignore_content = (
            "# Logs\n"
            "logs\n"
            "*.log\n"
            "npm-debug.log*\n"
            "yarn-debug.log*\n"
            "yarn-error.log*\n\n"
            "# Runtime data\n"
            "pids\n"
            "*.pid\n"
            "*.seed\n"
            "*.pid.lock\n\n"
            "# Directory for instrumented libs generated by jscoverage/JSCover\n"
            "lib-cov\n\n"
            "# Coverage directory used by tools like istanbul\n"
            "coverage\n\n"
            "# nyc test coverage\n"
            ".nyc_output\n\n"
            "# Grunt intermediate storage (http://gruntjs.com/creating-plugins#storing-task-files)\n"
            ".grunt\n\n"
            "# Bower dependency directory (https://bower.io/)\n"
            "bower_components\n\n"
            "# node-waf configuration\n"
            ".lock-wscript\n\n"
            "# Compiled binary addons (https://nodejs.org/api/addons.html)\n"
            "build/Release\n\n"
            "# Dependency directories\n"
            "node_modules/\n"
            "jspm_packages/\n\n"
            "# Typescript v1 declaration files\n"
            "typings/\n\n"
            "# Optional npm cache directory\n"
            ".npm\n\n"
            "# Optional eslint cache\n"
            ".eslintcache\n\n"
            "# Optional REPL history\n"
            ".node_repl_history\n\n"
            "# Output of 'npm pack'\n"
            "*.tgz\n\n"
            "# Yarn Integrity file\n"
            ".yarn-integrity\n\n"
            "# dotenv environment variables file\n"
            ".env\n\n"
            "# next.js build output\n"
            ".next\n\n"
            "# Nuxt.js build output\n"
            ".nuxt\n\n"
            "# vuepress build output\n"
            ".vuepress/dist\n\n"
            "# Serverless directories\n"
            ".serverless\n\n"
            "# FuseBox cache\n"
            ".fusebox/\n\n"
            "# DynamoDB Local files\n"
            ".dynamodb/\n\n"
            "# Tern plugin configuration file\n"
            ".tern-project\n\n"
            "# Platform specific files\n"
            ".DS_Store\n"
            "Thumbs.db\n"
        )

        tsconfig_path = os.path.join(output_dir, 'tsconfig.json')
        package_json_path = os.path.join(output_dir, 'package.json')
        gitignore_path = os.path.join(output_dir, '.gitignore')

        with open(tsconfig_path, 'w', encoding='utf-8') as file:
            json.dump(tsconfig_content, file, indent=4)

        with open(package_json_path, 'w', encoding='utf-8') as file:
            json.dump(package_json_content, file, indent=4)

        with open(gitignore_path, 'w', encoding='utf-8') as file:
            file.write(gitignore_content)

    def convert_schema(self, schema: List[Dict] | Dict, output_dir: str, write_file: bool = True):
        """ Convert Avro schema to TypeScript classes """
        self.output_dir = output_dir
        self.src_dir = os.path.join(self.output_dir, "src")
        if isinstance(schema, dict):
            schema = [schema]
        for avro_schema in schema:
            if avro_schema['type'] == 'record':
                self.generate_class(avro_schema, self.base_package, write_file)
            elif avro_schema['type'] == 'enum':
                self.generate_enum(avro_schema, self.base_package, write_file)

    def convert(self, avro_schema_path: str, output_dir: str):
        """ Convert Avro schema to TypeScript classes """
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)
        self.generate_project_files(output_dir)


def convert_avro_to_typescript(avro_schema_path, js_dir_path, package_name='', typedjson_annotation=False, avro_annotation=False):
    """ Convert Avro schema to TypeScript classes """
    
    if not package_name:
        package_name = os.path.splitext(os.path.basename(avro_schema_path))[0].lower().replace('-', '_')
        
    converter = AvroToTypeScript(package_name, typed_json_annotation=typedjson_annotation, avro_annotation=avro_annotation)
    converter.convert(avro_schema_path, js_dir_path)


def convert_avro_schema_to_typescript(avro_schema, js_dir_path, package_name='', typedjson_annotation=False, avro_annotation=False):
    """ Convert Avro schema to TypeScript classes """
    converter = AvroToTypeScript(package_name, typed_json_annotation=typedjson_annotation, avro_annotation=avro_annotation)
    converter.convert_schema(avro_schema, js_dir_path)
    converter.generate_project_files(js_dir_path)
