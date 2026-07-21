"""Convert Avro schemas to CommonJS JavaScript classes."""

import json
import os
from typing import Any, Dict, List, Set, Union

from avrotize.common import is_any_value_type, pascal, process_template

INDENT = ' ' * 4


def is_javascript_reserved_word(word: str) -> bool:
    """Return whether *word* is reserved in JavaScript."""
    return word in {
        'break', 'case', 'catch', 'class', 'const', 'continue', 'debugger',
        'default', 'delete', 'do', 'else', 'export', 'extends', 'finally',
        'for', 'function', 'if', 'import', 'in', 'instanceof', 'new', 'return',
        'super', 'switch', 'this', 'throw', 'try', 'typeof', 'var', 'void',
        'while', 'with', 'yield', 'let', 'static', 'enum', 'await', 'async'
    }


def is_javascript_primitive(word: str) -> bool:
    """Return whether *word* is represented by a JavaScript primitive."""
    return word in ['null', 'boolean', 'number', 'string', 'Date', 'any']


def xml_name(schema_obj: Dict[str, Any], default_name: str) -> str:
    """Resolve the XML wire name, honoring ``altnames.xml``."""
    altnames = schema_obj.get('altnames')
    if isinstance(altnames, dict) and isinstance(altnames.get('xml'), str):
        return altnames['xml']
    return default_name


def xml_enum_values(schema: Dict[str, Any], values: List[Any]) -> Dict[str, str]:
    """Resolve XML enum wire values, honoring ``altenums.xml``."""
    alternate = schema.get('altenums', {})
    alternate = alternate.get('xml', {}) if isinstance(alternate, dict) else {}
    return {str(value): str(alternate.get(str(value), value)) for value in values}


class AvroToJavaScript:
    """Convert Avro schemas to CommonJS JavaScript classes."""

    def __init__(self, base_package: str = '', avro_annotation: bool = False,
                 xml_annotation: bool = False) -> None:
        self.base_package = base_package
        self.avro_annotation = avro_annotation
        self.xml_annotation = xml_annotation
        self.output_dir = os.getcwd()
        self.generated_types: Set[str] = set()
        self.named_schemas: Dict[str, Dict[str, Any]] = {}

    def safe_name(self, name: str) -> str:
        return name + '_' if is_javascript_reserved_word(name) else name

    def qualify_namespace(self, schema_namespace: str) -> str:
        return '.'.join(part for part in (self.base_package, schema_namespace) if part)

    def map_primitive_to_javascript(self, avro_type: str) -> str:
        if is_any_value_type(avro_type):
            return 'any'
        return {
            'null': 'null', 'boolean': 'boolean', 'int': 'number',
            'long': 'number', 'float': 'number', 'double': 'number',
            'bytes': 'string', 'string': 'string'
        }.get(avro_type, avro_type)

    def convert_logical_type_to_javascript(self, avro_type: Dict) -> str:
        logical_type = avro_type.get('logicalType')
        if logical_type in ['decimal', 'uuid']:
            return 'string'
        if logical_type in ['date', 'time-millis', 'time-micros', 'timestamp-millis', 'timestamp-micros']:
            return 'Date'
        if logical_type == 'duration':
            return 'string'
        return 'any'

    def convert_avro_type_to_javascript(self, avro_type: Union[str, Dict, List],
                                        parent_namespace: str, import_types: Set[str]) -> str | List[Any]:
        if isinstance(avro_type, str):
            mapped_type = self.map_primitive_to_javascript(avro_type)
            if mapped_type == avro_type and not is_javascript_primitive(mapped_type):
                named = self.resolve_named_schema(avro_type, parent_namespace)
                if named:
                    named_namespace = named.get('namespace', parent_namespace)
                    package = self.qualify_namespace(named_namespace)
                    import_types.add(f'{package}.{named["name"]}' if package else named['name'])
                else:
                    import_types.add(avro_type)
                return pascal(avro_type.split('.')[-1])
            return mapped_type
        if isinstance(avro_type, list):
            return [self.convert_avro_type_to_javascript(item, parent_namespace, import_types) for item in avro_type]
        if isinstance(avro_type, dict):
            type_name = avro_type.get('type')
            if type_name == 'record':
                class_ref = self.generate_class(avro_type, parent_namespace)
                import_types.add(class_ref)
                return class_ref.split('.')[-1]
            if type_name == 'enum':
                enum_ref = self.generate_enum(avro_type, parent_namespace)
                import_types.add(enum_ref)
                return enum_ref.split('.')[-1]
            if type_name in ('array', 'map'):
                child = avro_type['items'] if type_name == 'array' else avro_type['values']
                return [self.convert_avro_type_to_javascript(child, parent_namespace, import_types)]
            if 'logicalType' in avro_type:
                return self.convert_logical_type_to_javascript(avro_type)
            return self.convert_avro_type_to_javascript(type_name, parent_namespace, import_types)
        return 'any'

    def register_named_schemas(self, node: Any, parent_namespace: str = '') -> None:
        if isinstance(node, list):
            for item in node:
                self.register_named_schemas(item, parent_namespace)
            return
        if not isinstance(node, dict):
            return
        namespace = node.get('namespace', parent_namespace)
        if node.get('type') in ('record', 'enum') and node.get('name'):
            name = node['name']
            self.named_schemas[name] = node
            self.named_schemas[f'{namespace}.{name}' if namespace else name] = node
            parent_namespace = namespace
        for field in node.get('fields', []):
            self.register_named_schemas(field.get('type'), parent_namespace)
        for key in ('items', 'values'):
            if key in node:
                self.register_named_schemas(node[key], parent_namespace)

    def resolve_named_schema(self, name: str, parent_namespace: str) -> Dict[str, Any] | None:
        return self.named_schemas.get(name) or self.named_schemas.get(
            f'{parent_namespace}.{name}' if parent_namespace and '.' not in name else name)

    def xml_type_descriptor(self, avro_type: Any, parent_namespace: str) -> str:
        if isinstance(avro_type, list):
            choices = ', '.join(self.xml_type_descriptor(item, parent_namespace) for item in avro_type)
            return f'{{ kind: "union", choices: [{choices}] }}'
        if isinstance(avro_type, str):
            if is_any_value_type(avro_type):
                return '{ kind: "any" }'
            primitive = {
                'null': 'null', 'boolean': 'boolean', 'int': 'integer', 'long': 'integer',
                'float': 'number', 'double': 'number', 'bytes': 'string', 'string': 'string'
            }.get(avro_type)
            if primitive:
                return f'{{ kind: "{primitive}" }}'
            named = self.resolve_named_schema(avro_type, parent_namespace)
            if named and named.get('type') == 'enum':
                values = json.dumps(xml_enum_values(named, named.get('symbols', [])), ensure_ascii=False)
                return f'{{ kind: "enum", values: {values} }}'
            return f'{{ kind: "record", ctor: {pascal(avro_type.split(".")[-1])} }}'
        if isinstance(avro_type, dict):
            type_name = avro_type.get('type')
            if 'logicalType' in avro_type:
                logical = avro_type['logicalType']
                return '{ kind: "date" }' if logical in (
                    'date', 'time-millis', 'time-micros', 'timestamp-millis', 'timestamp-micros') else '{ kind: "string" }'
            if type_name == 'record':
                return f'{{ kind: "record", ctor: {pascal(avro_type["name"])} }}'
            if type_name == 'enum':
                values = json.dumps(xml_enum_values(avro_type, avro_type.get('symbols', [])), ensure_ascii=False)
                return f'{{ kind: "enum", values: {values} }}'
            if type_name == 'array':
                return f'{{ kind: "array", items: {self.xml_type_descriptor(avro_type["items"], parent_namespace)} }}'
            if type_name == 'map':
                return f'{{ kind: "map", values: {self.xml_type_descriptor(avro_type["values"], parent_namespace)} }}'
            return self.xml_type_descriptor(type_name, parent_namespace)
        return '{ kind: "any" }'

    def validation_expression(self, avro_type: Any, value: str, parent_namespace: str) -> str:
        if isinstance(avro_type, list):
            return '(' + ' || '.join(self.validation_expression(item, value, parent_namespace) for item in avro_type) + ')'
        if isinstance(avro_type, dict):
            type_name = avro_type.get('type')
            if 'logicalType' in avro_type and avro_type['logicalType'] in (
                    'date', 'time-millis', 'time-micros', 'timestamp-millis', 'timestamp-micros'):
                return f'{value} instanceof Date'
            if type_name == 'array':
                child = self.validation_expression(avro_type['items'], 'item', parent_namespace)
                return f'Array.isArray({value}) && {value}.every((item) => {child})'
            if type_name == 'map':
                child = self.validation_expression(avro_type['values'], 'item', parent_namespace)
                return f'{value} !== null && typeof {value} === "object" && !Array.isArray({value}) && Object.values({value}).every((item) => {child})'
            if type_name == 'record':
                return f'{value} instanceof {pascal(avro_type["name"])}'
            if type_name == 'enum':
                return f'Object.values({pascal(avro_type["name"])}).includes({value})'
            return self.validation_expression(type_name, value, parent_namespace)
        mapped = self.map_primitive_to_javascript(avro_type)
        if mapped == 'null':
            return f'{value} === null'
        if mapped == 'Date':
            return f'{value} instanceof Date'
        if mapped == 'any':
            return 'true'
        if mapped in ('boolean', 'number', 'string'):
            return f'typeof {value} === "{mapped}"'
        named = self.resolve_named_schema(avro_type, parent_namespace)
        if named and named.get('type') == 'enum':
            return f'Object.values({pascal(avro_type.split(".")[-1])}).includes({value})'
        return f'{value} instanceof {pascal(avro_type.split(".")[-1])}'

    def package_root(self) -> str:
        return os.path.join(self.output_dir, self.base_package.replace('.', os.sep)) if self.base_package else self.output_dir

    def xml_runtime_import(self, namespace: str) -> str:
        class_dir = os.path.join(self.output_dir, namespace.replace('.', os.sep))
        runtime = os.path.join(self.package_root(), 'xml-runtime')
        relative = os.path.relpath(runtime, class_dir).replace(os.sep, '/')
        return relative if relative.startswith('.') else f'./{relative}'

    def generate_class(self, avro_schema: Dict, parent_namespace: str) -> str:
        import_types: Set[str] = set()
        class_name = pascal(avro_schema['name'])
        schema_namespace = avro_schema.get('namespace', parent_namespace)
        namespace = self.qualify_namespace(schema_namespace)
        qualified_name = f'{namespace}.{class_name}' if namespace else class_name
        if qualified_name in self.generated_types:
            return qualified_name
        self.generated_types.add(qualified_name)

        constructor_body = ''
        class_body = ''
        fields = avro_schema.get('fields', [])
        for field in fields:
            field_name = self.safe_name(field['name'])
            self.convert_avro_type_to_javascript(field['type'], schema_namespace, import_types)
            if field.get('doc'):
                constructor_body += f'{INDENT}/** {field["doc"]} */\n'
            constructor_body += f'{INDENT}this._{field_name} = null;\n'
            valid = self.validation_expression(field['type'], 'value', schema_namespace)
            class_body += f'Object.defineProperty({class_name}.prototype, {json.dumps(field_name)}, {{\n'
            class_body += f'{INDENT}get: function() {{ return this._{field_name}; }},\n'
            class_body += f'{INDENT}set: function(value) {{\n'
            class_body += f'{INDENT * 2}if (!({valid})) throw new TypeError("Invalid type for {field_name}");\n'
            class_body += f'{INDENT * 2}this._{field_name} = value;\n{INDENT}}}\n}});\n\n'

        imports = ''
        if self.avro_annotation:
            imports += "const avro = require('avro-js');\n"
        if self.xml_annotation:
            imports += f"const xmlRuntime = require('{self.xml_runtime_import(namespace)}');\n"
        for import_type in sorted(import_types):
            import_name = pascal(import_type.split('.')[-1])
            if import_name == class_name:
                continue
            import_package = import_type.rsplit('.', 1)[0] if '.' in import_type else ''
            import_path = import_package.replace('.', '/')
            namespace_path = namespace.replace('.', '/')
            relative = os.path.relpath(import_path or '.', namespace_path or '.').replace(os.sep, '/')
            if not relative.startswith('.'):
                relative = f'./{relative}'
            imports += f"const {import_name} = require('{relative}/{import_name}');\n"

        definition = imports + ('\n' if imports else '')
        if avro_schema.get('doc'):
            definition += f'/** {avro_schema["doc"]} */\n'
        definition += f'function {class_name}() {{\n{constructor_body}}}\n\n{class_body}'
        if self.avro_annotation:
            definition += f'{class_name}.AvroType = avro.parse({json.dumps(json.dumps(avro_schema))});\n\n'
        if self.xml_annotation:
            mapping_fields = []
            for field in fields:
                property_name = self.safe_name(field['name'])
                wire_name = xml_name(field, field['name'])
                kind = 'attribute' if field.get('xmlkind', 'element') == 'attribute' else 'element'
                descriptor = self.xml_type_descriptor(field['type'], schema_namespace)
                mapping_fields.append(
                    f'{json.dumps(property_name)}: {{ name: {json.dumps(wire_name)}, kind: "{kind}", required: true, type: {descriptor} }}')
            fields_literal = ',\n        '.join(mapping_fields)
            avro_type = f'{class_name}.AvroType' if self.avro_annotation else 'undefined'
            definition += f"""{class_name}.XmlOptions = xmlRuntime.XML_OPTIONS;
{class_name}.XmlMapping = Object.freeze({{
    name: {json.dumps(xml_name(avro_schema, avro_schema['name']))},
    namespace: {json.dumps(avro_schema.get('xmlns', ''))},
    ctor: {class_name},
    fields: Object.freeze({{
        {fields_literal}
    }})
}});
{class_name}.prototype.ToByteArray = function(contentType) {{
    return xmlRuntime.toByteArray(this, {class_name}.XmlMapping, contentType, {avro_type});
}};
{class_name}.FromData = function(data, contentType) {{
    return xmlRuntime.fromData(data, {class_name}.XmlMapping, contentType, {avro_type});
}};

"""
        definition += f'module.exports = {class_name};\n'
        self.write_to_file(namespace, class_name, definition)
        return qualified_name

    def generate_enum(self, avro_schema: Dict, parent_namespace: str) -> str:
        enum_name = pascal(avro_schema['name'])
        schema_namespace = avro_schema.get('namespace', parent_namespace)
        namespace = self.qualify_namespace(schema_namespace)
        qualified_name = f'{namespace}.{enum_name}' if namespace else enum_name
        if qualified_name in self.generated_types:
            return qualified_name
        self.generated_types.add(qualified_name)
        members = [f'{INDENT}{self.safe_name(str(symbol))}: {json.dumps(str(symbol))}'
                   for symbol in avro_schema.get('symbols', [])]
        definition = ''
        if avro_schema.get('doc'):
            definition += f'/** {avro_schema["doc"]} */\n'
        definition += f'const {enum_name} = {{\n' + ',\n'.join(members) + '\n};\n'
        if self.xml_annotation:
            mapping = {
                'name': xml_name(avro_schema, avro_schema['name']),
                'namespace': avro_schema.get('xmlns', ''),
                'values': xml_enum_values(avro_schema, avro_schema.get('symbols', []))
            }
            definition += f'Object.defineProperty({enum_name}, "XmlMapping", {{ value: Object.freeze({json.dumps(mapping, ensure_ascii=False)}) }});\n'
        definition += f'Object.freeze({enum_name});\n\nmodule.exports = {enum_name};\n'
        self.write_to_file(namespace, enum_name, definition)
        return qualified_name

    def write_to_file(self, namespace: str, name: str, content: str) -> None:
        directory = os.path.join(self.output_dir, namespace.replace('.', os.sep))
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, f'{name}.js'), 'w', encoding='utf-8') as file:
            file.write(content)

    def write_package_files(self) -> None:
        package_root = self.package_root()
        os.makedirs(package_root, exist_ok=True)
        package_name = (self.base_package or 'generated-avro-js').replace('.', '-').replace('_', '-').lower()
        dependencies = {}
        if self.avro_annotation:
            dependencies['avro-js'] = '^1.12.0'
        if self.xml_annotation:
            dependencies['fast-xml-parser'] = '^5.2.5'
            runtime = process_template('javascript/xml_runtime.js.jinja')
            with open(os.path.join(package_root, 'xml-runtime.js'), 'w', encoding='utf-8') as file:
                file.write(runtime)
        package = process_template(
            'avrotojs/package.json.jinja', package_name_json=json.dumps(package_name), dependencies=dependencies)
        with open(os.path.join(package_root, 'package.json'), 'w', encoding='utf-8') as file:
            file.write(package)

    def convert_schema(self, schema: List | Dict, output_dir: str) -> None:
        self.output_dir = output_dir
        schemas = [schema] if isinstance(schema, dict) else schema
        self.register_named_schemas(schemas)
        for avro_schema in schemas:
            if avro_schema['type'] == 'record':
                self.generate_class(avro_schema, '')
            elif avro_schema['type'] == 'enum':
                self.generate_enum(avro_schema, '')
        self.write_package_files()

    def convert(self, avro_schema_path: str, output_dir: str) -> None:
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            self.convert_schema(json.load(file), output_dir)


def convert_avro_to_javascript(avro_schema_path, js_dir_path, package_name='',
                               avro_annotation=False, xml_annotation=False):
    """Convert an Avro schema file to JavaScript classes."""
    if not package_name:
        package_name = os.path.splitext(os.path.basename(avro_schema_path))[0].replace('-', '_')
    converter = AvroToJavaScript(package_name, avro_annotation=avro_annotation,
                                 xml_annotation=xml_annotation)
    converter.convert(avro_schema_path, js_dir_path)


def convert_avro_schema_to_javascript(avro_schema, js_dir_path, package_name='',
                                      avro_annotation=False, xml_annotation=False):
    """Convert an in-memory Avro schema to JavaScript classes."""
    converter = AvroToJavaScript(package_name, avro_annotation=avro_annotation,
                                 xml_annotation=xml_annotation)
    converter.convert_schema(avro_schema, js_dir_path)
