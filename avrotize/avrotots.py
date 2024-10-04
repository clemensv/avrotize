# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring, line-too-long, too-many-locals, too-many-branches, too-many-statements

import json
import os
from typing import Dict, List, Set, Union

from avrotize.common import build_flat_type_dict, fullname, inline_avro_references, is_generic_avro_type, is_type_with_alternate, pascal, process_template, strip_alternate_type
from numpy import full


def is_typescript_reserved_word(word: str) -> bool:
    """Check if word is a TypeScript reserved word."""
    reserved_words = [
        'break', 'case', 'catch', 'class', 'const', 'continue', 'debugger',
        'default', 'delete', 'do', 'else', 'export', 'extends', 'finally',
        'for', 'function', 'if', 'import', 'in', 'instanceof', 'new', 'return',
        'super', 'switch', 'this', 'throw', 'try', 'typeof', 'var', 'void',
        'while', 'with', 'yield', 'enum', 'string', 'number', 'boolean', 'symbol',
        'type', 'namespace', 'module', 'declare', 'abstract', 'readonly',
    ]
    return word in reserved_words


class AvroToTypeScript:
    """Converts Avro schema to TypeScript classes using templates with namespace support."""

    def __init__(self, base_package: str = '', typed_json_annotation=False, avro_annotation=False) -> None:
        self.base_package = base_package
        self.typed_json_annotation = typed_json_annotation
        self.avro_annotation = avro_annotation
        self.output_dir = os.getcwd()
        self.src_dir = os.path.join(self.output_dir, "src")
        self.generated_types: Dict[str, str] = {}
        self.main_schema = None
        self.type_dict = None
        self.INDENT = ' ' * 4

    def map_primitive_to_typescript(self, avro_type: str) -> str:
        """Map Avro primitive type to TypeScript type."""
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
        """Convert Avro logical type to TypeScript type."""
        if 'logicalType' in avro_type:
            if avro_type['logicalType'] in ['decimal', 'uuid']:
                return 'string'
            if avro_type['logicalType'] in ['date', 'time-millis', 'time-micros', 'timestamp-millis', 'timestamp-micros']:
                return 'Date'
            if avro_type['logicalType'] == 'duration':
                return 'string'
        return 'any'

    def strip_nullable(self, ts_type: str) -> str:
        """Strip nullable type from TypeScript type."""
        if  ts_type.endswith('?'):
            return ts_type[:-1]
        return ts_type
    
    def is_typescript_primitive(self, ts_type: str) -> bool:
        """Check if TypeScript type is a primitive."""
        ts_type = self.strip_nullable(ts_type)
        return ts_type in ['null', 'boolean', 'number', 'string', 'Date', 'any']
    
    def is_enum_type(self, ts_type: str, namespace: str) -> bool:
        """Check if TypeScript type is an enum."""
        ts_type = self.strip_nullable(ts_type)
        fn_type = fullname(ts_type, namespace)
        return not self.is_typescript_primitive(ts_type) and fn_type in self.generated_types and self.generated_types[fn_type] == 'enum'

    def safe_name(self, name: str) -> str:
        """Converts a name to a safe TypeScript name."""
        if is_typescript_reserved_word(name):
            return name + "_"
        return name

    def convert_avro_type_to_typescript(self, avro_type: Union[str, Dict, List], parent_namespace: str, import_types: Set[str], class_name: str = '', field_name: str = '') -> str:
        """Convert Avro type to TypeScript type with namespace support."""
        if isinstance(avro_type, str):
            mapped_type = self.map_primitive_to_typescript(avro_type)
            if mapped_type == avro_type and not self.is_typescript_primitive(mapped_type):
                full_name = self.concat_namespace(self.base_package,fullname(avro_type, parent_namespace))
                import_types.add(full_name)
                return pascal(avro_type.split('.')[-1])
            return mapped_type
        elif isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                return '{ [key: string]: any }'
            if 'null' in avro_type:
                if len(avro_type) == 2:
                    return f'{self.convert_avro_type_to_typescript([t for t in avro_type if t != "null"][0], parent_namespace, import_types, class_name, field_name)}?'
                return f'{self.generate_embedded_union(class_name, field_name, avro_type, parent_namespace, import_types)}?'
            return self.generate_embedded_union(class_name, field_name, avro_type, parent_namespace, import_types)
        elif isinstance(avro_type, dict):
            if avro_type['type'] == 'record':
                class_ref = self.generate_class(avro_type, parent_namespace, write_file=True)
                import_types.add(class_ref)
                return pascal(class_ref.split('.')[-1])
            elif avro_type['type'] == 'enum':
                enum_ref = self.generate_enum(avro_type, parent_namespace, write_file=True)
                import_types.add(enum_ref)
                return pascal(enum_ref.split('.')[-1])
            elif avro_type['type'] == 'array':
                return f'{self.convert_avro_type_to_typescript(avro_type["items"], parent_namespace, import_types, class_name, field_name)}[]'
            elif avro_type['type'] == 'map':
                return f'{{ [key: string]: {self.convert_avro_type_to_typescript(avro_type["values"], parent_namespace, import_types, class_name, field_name)} }}'
            elif 'logicalType' in avro_type:
                return self.convert_logical_type_to_typescript(avro_type)
            return self.convert_avro_type_to_typescript(avro_type['type'], parent_namespace, import_types, class_name, field_name)
        return 'any'

    def get_qualified_name(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name with a dot separator."""
        return f"{namespace}.{name}" if namespace != '' else name

    def concat_namespace(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name with a dot separator."""
        if namespace and name:
            return f"{namespace}.{name}"
        return namespace or name

    def generate_class_or_enum(self, avro_schema: Dict, parent_namespace: str, write_file: bool = True) -> str:
        """Generates a Class or Enum."""
        if avro_schema['type'] == 'record':
            return self.generate_class(avro_schema, parent_namespace, write_file)
        elif avro_schema['type'] == 'enum':
            return self.generate_enum(avro_schema, parent_namespace, write_file)
        return ''

    def generate_class(self, avro_schema: Dict, parent_namespace: str, write_file: bool = True) -> str:
        """Generate TypeScript class from Avro record using templates with namespace support."""
        import_types: Set[str] = set()
        class_name = pascal(avro_schema['name'])
        namespace = self.concat_namespace(self.base_package, avro_schema.get('namespace', parent_namespace))
        ts_qualified_name = self.get_qualified_name(namespace, class_name)
        if ts_qualified_name in self.generated_types:
            return ts_qualified_name

        fields = [{
            'definition': self.generate_field(field, avro_schema.get('namespace', parent_namespace), import_types, class_name),
            'docstring': field.get('doc', '')
        } for field in avro_schema.get('fields', [])]

        fields = [{
            'name': self.safe_name(field['definition']['name']),
            'original_name': field['definition']['name'],
            'type': field['definition']['type'],
            'type_no_null': self.strip_nullable(field['definition']['type']),
            'is_primitive': field['definition']['is_primitive'],
            'is_enum': field['definition']['is_enum'],
            'is_array': field['definition']['is_array'],
            'docstring': field['docstring'],
        } for field in fields]

        imports_with_paths: Dict[str, str] = {}
        for import_type in import_types:
            if import_type == ts_qualified_name:
                continue  
            import_is_enum = import_type in self.generated_types and self.generated_types[import_type] == 'enum'
            import_type_parts = import_type.split('.')
            import_type_name = pascal(import_type_parts[-1])
            import_path = '/'.join(import_type_parts)
            current_path = '/'.join(namespace.split('.'))
            relative_import_path = os.path.relpath(import_path, current_path).replace(os.sep, '/')
            if not relative_import_path.startswith('.'):
                relative_import_path = f'./{relative_import_path}'
            if import_is_enum:
                import_type_name_and_util = f"{import_type_name}, {import_type_name}Utils"
                imports_with_paths[import_type_name_and_util] = relative_import_path + '.js'
            else:
                imports_with_paths[import_type_name] = relative_import_path + '.js'

        # Inline the schema
        local_avro_schema = inline_avro_references(avro_schema.copy(), self.type_dict, parent_namespace)
        avro_schema_json = json.dumps(local_avro_schema).replace('"', '\\"')

        class_definition = process_template(
            "avrotots/class_core.ts.jinja",
            namespace=namespace,
            class_name=class_name,
            docstring=avro_schema.get('doc', '').strip() if 'doc' in avro_schema else f'A {class_name} record.',
            fields=fields,
            imports=imports_with_paths,
            base_package=self.base_package,
            avro_annotation=self.avro_annotation,
            typed_json_annotation=self.typed_json_annotation,
            avro_schema_json=avro_schema_json,
            get_is_json_match_clause=self.get_is_json_match_clause,
        )

        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
        self.generated_types[ts_qualified_name] = 'class'
        return ts_qualified_name

    def generate_enum(self, avro_schema: Dict, parent_namespace: str, write_file: bool = True) -> str:
        """Generate TypeScript enum from Avro enum using templates with namespace support."""
        enum_name = pascal(avro_schema['name'])
        namespace = self.concat_namespace(self.base_package, avro_schema.get('namespace', parent_namespace))
        ts_qualified_name = self.get_qualified_name(namespace, enum_name)
        if ts_qualified_name in self.generated_types:
            return ts_qualified_name

        symbols = avro_schema.get('symbols', [])
        enum_definition = process_template(
            "avrotots/enum_core.ts.jinja",
            namespace=namespace,
            enum_name=enum_name,
            docstring=avro_schema.get('doc', '').strip() if 'doc' in avro_schema else f'A {enum_name} enum.',
            symbols=symbols,
        )

        if write_file:
            self.write_to_file(namespace, enum_name, enum_definition)
        self.generated_types[ts_qualified_name] = 'enum'
        return ts_qualified_name

    def generate_field(self, field: Dict, parent_namespace: str, import_types: Set[str], class_name: str) -> Dict:
        """Generates a field for a TypeScript class."""
        import_types_this = set()
        field_type = self.convert_avro_type_to_typescript(
            field['type'], parent_namespace, import_types_this, class_name, field['name'])
        import_types.update(import_types_this)
        field_name = field['name']
        return {
            'name': field_name,
            'type': field_type,
            'is_primitive': self.is_typescript_primitive(field_type.replace('[]', '')),
            'is_array': field_type.endswith('[]'),
            'is_enum': len(import_types_this) > 0 and self.is_enum_type(import_types_this.pop(),'')
        }

    def get_is_json_match_clause(self, field_name: str, field_type: str, field_is_enum: bool) -> str:
        """Generates the isJsonMatch clause for a field."""
        field_name_js = field_name.rstrip('_')
        is_optional = field_type.endswith('?')
        field_type = self.strip_nullable(field_type)

        if '|' in field_type:
            union_types = [t.strip() for t in field_type.split('|')]
            union_clauses = [self.get_is_json_match_clause(field_name, union_type, False) for union_type in union_types]
            clause = f"({' || '.join(union_clauses)})"
            return clause

        clause = f"(element.hasOwnProperty('{field_name_js}') && "

        if field_is_enum:
            clause += f"(typeof element['{field_name_js}'] === 'string' || typeof element['{field_name_js}'] === 'number')"
        else:
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
            clause += f") || element['{field_name_js}'] === null"
        else:
            clause += ")"

        return clause

    def generate_embedded_union(self, class_name: str, field_name: str, avro_type: List, parent_namespace: str, parent_import_types: Set[str], write_file: bool = True) -> str:
        """Generate embedded Union class for a field with namespace support."""
        union_class_name = pascal(field_name) + 'Union' if field_name else pascal(class_name) + 'Union'
        namespace = parent_namespace
        union_types = [self.convert_avro_type_to_typescript( t, parent_namespace, parent_import_types) for t in avro_type if t != 'null']
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
            if import_type == union_class_name:
                continue  # Avoid importing itself
            import_type_parts = import_type.split('.')
            import_type_name = pascal(import_type_parts[-1])
            import_namespace = '.'.join(import_type_parts[:-1])
            import_path = '/'.join(import_type_parts)
            current_path = '/'.join(namespace.split('.'))
            relative_import_path = os.path.relpath(import_path, current_path).replace(os.sep, '/')
            if not relative_import_path.startswith('.'):
                relative_import_path = f'./{relative_import_path}'
            class_definition += f"import {{ {import_type_name} }} from '{relative_import_path}.js';\n"

        class_definition += f"\nexport namespace {namespace} {{\n"
        class_definition += f"{self.INDENT}export class {union_class_name} {{\n"

        class_definition += f"{self.INDENT*2}private value: any;\n\n"

        # Constructor
        class_definition += f"{self.INDENT*2}constructor(value: { ' | '.join(union_types) }) {{\n"
        class_definition += f"{self.INDENT*3}this.value = value;\n"
        class_definition += f"{self.INDENT*2}}}\n\n"

        # Method to check which type is set
        for union_type in union_types:
            type_check_method = f"{self.INDENT*2}public is{pascal(union_type)}(): boolean {{\n"
            if union_type.strip() in ['string', 'number', 'boolean']:
                type_check_method += f"{self.INDENT*3}return typeof this.value === '{union_type.strip()}';\n"
            elif union_type.strip() == 'Date':
                type_check_method += f"{self.INDENT*3}return this.value instanceof Date;\n"
            else:
                type_check_method += f"{self.INDENT*3}return this.value instanceof {union_type.strip()};\n"
            type_check_method += f"{self.INDENT*2}}}\n\n"
            class_definition += type_check_method

        # Method to return the current value
        class_definition += f"{self.INDENT*2}public toObject(): any {{\n"
        class_definition += f"{self.INDENT*3}return this.value;\n"
        class_definition += f"{self.INDENT*2}}}\n\n"

        # Method to check if JSON matches any of the union types
        class_definition += f"{self.INDENT*2}public static isJsonMatch(element: any): boolean {{\n"
        match_clauses = []
        for union_type in union_types:
            match_clauses.append(f"({self.get_is_json_match_clause('value', union_type, False)})")
        class_definition += f"{self.INDENT*3}return {' || '.join(match_clauses)};\n"
        class_definition += f"{self.INDENT*2}}}\n\n"

        # Method to deserialize from JSON
        class_definition += f"{self.INDENT*2}public static fromData(element: any, contentTypeString: string): {union_class_name} {{\n"
        class_definition += f"{self.INDENT*3}const unionTypes = [{', '.join([t.strip() for t in union_types if not self.is_typescript_primitive(t.strip())])}];\n"
        class_definition += f"{self.INDENT*3}for (const type of unionTypes) {{\n"
        class_definition += f"{self.INDENT*4}if (type.isJsonMatch(element)) {{\n"
        class_definition += f"{self.INDENT*5}return new {union_class_name}(type.fromData(element, contentTypeString));\n"
        class_definition += f"{self.INDENT*4}}}\n"
        class_definition += f"{self.INDENT*3}}}\n"
        class_definition += f"{self.INDENT*3}throw new Error('No matching type for union');\n"
        class_definition += f"{self.INDENT*2}}}\n"

        class_definition += f"{self.INDENT}}}\n"
        class_definition += "}\n"

        if write_file:
            self.write_to_file(namespace, union_class_name, class_definition)

        return f"{namespace}.{union_class_name}"

    def write_to_file(self, namespace: str, name: str, content: str):
            """Write TypeScript class to file in the correct namespace directory."""
            directory_path = os.path.join(self.src_dir, *namespace.split('.'))
            if not os.path.exists(directory_path):
                os.makedirs(directory_path, exist_ok=True)

            file_path = os.path.join(directory_path, f"{name}.ts")
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(content)

    def generate_index_file(self):
        """Generate a root index.ts file that exports all types with aliases scoped to their modules."""
        exports = []

        for class_name in self.generated_types:
            # Split the class_name into parts
            parts = class_name.split('.')
            file_name = parts[-1]  # The actual type name (e.g., 'FareRules')
            module_path = parts[:-1]  # The module path excluding the type (e.g., ['gtfs_dash_data', 'GeneralTransitFeedStatic'])

            # Construct the relative path to the .js file
            # Exclude 'gtfs_dash_data' from the module path for the file path
            file_relative_path = os.path.join(*(module_path[0:] + [f"{file_name}.js"])).replace(os.sep, '/')
            if not file_relative_path.startswith('.'):
                file_relative_path = './' + file_relative_path

            # Construct the alias name by joining module parts with underscores
            # Exclude 'gtfs_dash_data' for brevity
            alias_parts =  [pascal(part) for part in parts]
            alias_name = '_'.join(alias_parts)

            # Generate the export statement with alias
            exports.append(f"export {{ {file_name} as {alias_name} }} from '{file_relative_path}';\n")

        # Write the root index.ts file
        index_file_path = os.path.join(self.src_dir, 'index.ts')
        with open(index_file_path, 'w', encoding='utf-8') as f:
            f.writelines(exports)

    def generate_project_files(self, output_dir: str):
        """Generate project files using templates."""
        tsconfig_content = process_template(
            "avrotots/tsconfig.json.jinja",
        )

        package_json_content = process_template(
            "avrotots/package.json.jinja",
            package_name=self.base_package,
        )

        gitignore_content = process_template(
            "avrotots/gitignore.jinja",
        )

        tsconfig_path = os.path.join(output_dir, 'tsconfig.json')
        package_json_path = os.path.join(output_dir, 'package.json')
        gitignore_path = os.path.join(output_dir, '.gitignore')

        with open(tsconfig_path, 'w', encoding='utf-8') as file:
            file.write(tsconfig_content)

        with open(package_json_path, 'w', encoding='utf-8') as file:
            file.write(package_json_content)

        with open(gitignore_path, 'w', encoding='utf-8') as file:
            file.write(gitignore_content)

    def convert_schema(self, schema: Union[List[Dict], Dict], output_dir: str, write_file: bool = True):
        """Convert Avro schema to TypeScript classes with namespace support."""
        self.output_dir = output_dir
        self.src_dir = os.path.join(self.output_dir, "src")
        if isinstance(schema, dict):
            schema = [schema]
        self.main_schema = schema
        self.type_dict = build_flat_type_dict(schema)
        for avro_schema in schema:
            if avro_schema['type'] == 'record':
                self.generate_class(avro_schema, '', write_file)
            elif avro_schema['type'] == 'enum':
                self.generate_enum(avro_schema, '', write_file)
        self.generate_index_file()
        self.generate_project_files(output_dir)

    def convert(self, avro_schema_path: str, output_dir: str):
        """Convert Avro schema to TypeScript classes."""
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)
        self.generate_project_files(output_dir)


def convert_avro_to_typescript(avro_schema_path, js_dir_path, package_name='', typedjson_annotation=False, avro_annotation=False):
    """Convert Avro schema to TypeScript classes."""
    if not package_name:
        package_name = os.path.splitext(os.path.basename(avro_schema_path))[0].lower().replace('-', '_')

    converter = AvroToTypeScript(package_name, typed_json_annotation=typedjson_annotation,
                                 avro_annotation=avro_annotation)
    converter.convert(avro_schema_path, js_dir_path)


def convert_avro_schema_to_typescript(avro_schema, js_dir_path, package_name='', typedjson_annotation=False, avro_annotation=False):
    """Convert Avro schema to TypeScript classes."""
    converter = AvroToTypeScript(package_name, typed_json_annotation=typedjson_annotation,
                                 avro_annotation=avro_annotation)
    converter.convert_schema(avro_schema, js_dir_path)
    converter.generate_project_files(js_dir_path)
