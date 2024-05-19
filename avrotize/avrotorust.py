# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

"""Generates Rust structs from Avro schema"""
import json
import os
from typing import Dict, List, Union
from avrotize.common import is_generic_avro_type

INDENT = '    '

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


def pascal(s: str) -> str:
    """Convert string to PascalCase."""
    return ''.join(word.capitalize() for word in s.split('_'))


def camel(s: str) -> str:
    """Convert string to camelCase."""
    s = pascal(s)
    return s[0].lower() + s[1:] if s else s


class AvroToRust:
    """Converts Avro schema to Rust structs, including Serde and Avro marshalling methods"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package.replace('.', '/')
        self.output_dir = os.getcwd()
        self.generated_types_avro_namespace: Dict[str, str] = {}
        self.generated_types_rust_package: Dict[str, str] = {}
        self.avro_annotation = False
        self.serde_annotation = False

    def safe_identifier(self, name: str) -> str:
        """Converts a name to a safe Rust identifier"""
        reserved_words = [
            'as', 'break', 'const', 'continue', 'crate', 'else', 'enum', 'extern', 'false', 'fn', 'for', 'if', 'impl',
            'in', 'let', 'loop', 'match', 'mod', 'move', 'mut', 'pub', 'ref', 'return', 'self', 'Self', 'static',
            'struct', 'super', 'trait', 'true', 'type', 'unsafe', 'use', 'where', 'while', 'async', 'await', 'dyn',
        ]
        if name in reserved_words:
            return f"{name}_"
        return name

    def map_primitive_to_rust(self, avro_type: str, is_optional: bool) -> str:
        """Maps Avro primitive types to Rust types"""
        optional_mapping = {
            'null': 'Option<()>',
            'boolean': 'Option<bool>',
            'int': 'Option<i32>',
            'long': 'Option<i64>',
            'float': 'Option<f32>',
            'double': 'Option<f64>',
            'bytes': 'Option<Vec<u8>>',
            'string': 'Option<String>',
        }
        required_mapping = {
            'null': '()',
            'boolean': 'bool',
            'int': 'i32',
            'long': 'i64',
            'float': 'f32',
            'double': 'f64',
            'bytes': 'Vec<u8>',
            'string': 'String',
        }
        if '.' in avro_type:
            type_name = avro_type.split('.')[-1]
            package_name = '::'.join(avro_type.split('.')[:-1]).lower()
            avro_type = self.concat_package(package_name, type_name)
        if avro_type in self.generated_types_avro_namespace:
            qualified_class_name = self.concat_package(self.base_package, avro_type)
            return qualified_class_name
        else:
            return required_mapping.get(avro_type, avro_type) if not is_optional else optional_mapping.get(avro_type, avro_type)

    def concat_package(self, package: str, name: str) -> str:
        """Concatenates package and name using a double colon separator"""
        return f"{package.lower()}::{name}" if package else name

    def convert_avro_type_to_rust(self, field_name: str, avro_type: Union[str, Dict, List], namespace: str, nullable: bool = False) -> str:
        """Converts Avro type to Rust type"""
        if isinstance(avro_type, str):
            return self.map_primitive_to_rust(avro_type, nullable)
        elif isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                return 'serde_json::Value' if self.serde_annotation else 'std::collections::HashMap<String, String>'
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                if isinstance(non_null_types[0], str):
                    return self.map_primitive_to_rust(non_null_types[0], True)
                else:
                    return self.convert_avro_type_to_rust(field_name, non_null_types[0], namespace)
            else:
                return self.generate_union_enum(field_name, avro_type, namespace)
        elif isinstance(avro_type, dict):
            if avro_type['type'] in ['record', 'enum']:
                return self.generate_class_or_enum(avro_type, namespace)
            elif avro_type['type'] == 'fixed' or avro_type['type'] == 'bytes' and 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'decimal':
                    return 'f64'
            elif avro_type['type'] == 'array':
                item_type = self.convert_avro_type_to_rust(field_name, avro_type['items'], namespace, nullable=True)
                return f"Vec<{item_type}>"
            elif avro_type['type'] == 'map':
                values_type = self.convert_avro_type_to_rust(field_name, avro_type['values'], namespace, nullable=True)
                return f"std::collections::HashMap<String, {values_type}>"
            elif 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'date':
                    return 'chrono::NaiveDate'
                elif avro_type['logicalType'] == 'time-millis' or avro_type['logicalType'] == 'time-micros':
                    return 'chrono::NaiveTime'
                elif avro_type['logicalType'] == 'timestamp-millis' or avro_type['logicalType'] == 'timestamp-micros':
                    return 'chrono::NaiveDateTime'
                elif avro_type['logicalType'] == 'uuid':
                    return 'uuid::Uuid'
            return self.convert_avro_type_to_rust(field_name, avro_type['type'], namespace)
        return 'serde_json::Value' if self.serde_annotation else 'std::collections::HashMap<String, String>'

    def generate_class_or_enum(self, avro_schema: Dict, parent_namespace: str = '') -> str:
        """Generates a Rust struct or enum from an Avro schema"""
        namespace = avro_schema.get('namespace', parent_namespace)
        if avro_schema['type'] == 'record':
            return self.generate_struct(avro_schema, namespace)
        elif avro_schema['type'] == 'enum':
            return self.generate_enum(avro_schema, namespace)
        return 'serde_json::Value'

    def generate_struct(self, avro_schema: Dict, parent_namespace: str) -> str:
        """Generates a Rust struct from an Avro record schema"""
        struct_definition = ''
        if 'doc' in avro_schema:
            struct_definition += f"/// {avro_schema['doc']}\n"
        namespace = avro_schema.get('namespace', parent_namespace)
        struct_name = self.safe_identifier(avro_schema['name'])
        qualified_struct_name = self.concat_package(namespace.replace('.', '::'), struct_name)
        if qualified_struct_name in self.generated_types_avro_namespace:
            return qualified_struct_name
        self.generated_types_avro_namespace[qualified_struct_name] = "struct"
        self.generated_types_rust_package[qualified_struct_name] = "struct"
        struct_definition += f"#[derive(Debug, Serialize, Deserialize)]\n" if self.serde_annotation else f"#[derive(Debug)]\n"
        struct_definition += f"pub struct {struct_name} {{\n"
        for field in avro_schema.get('fields', []):
            original_field_name = field['name']
            field_name = self.safe_identifier(camel(original_field_name))
            field_type = self.convert_avro_type_to_rust(field_name, field['type'], namespace)
            serde_rename = f'#[serde(rename = "{original_field_name}")] ' if field_name != original_field_name else ''
            struct_definition += f"{INDENT}{serde_rename}pub {field_name}: {field_type},\n"
        struct_definition += "}\n\n"

        struct_definition += self.generate_impl_block(struct_name, avro_schema)
        self.write_to_file(namespace.replace('.', '::'), struct_name, struct_definition)
        return qualified_struct_name

    def generate_enum(self, avro_schema: Dict, parent_namespace: str) -> str:
        """Generates a Rust enum from an Avro enum schema"""
        enum_definition = ''
        if 'doc' in avro_schema:
            enum_definition += f"/// {avro_schema['doc']}\n"
        namespace = avro_schema.get('namespace', parent_namespace)
        enum_name = self.safe_identifier(avro_schema['name'])
        qualified_enum_name = self.concat_package(namespace.replace('.', '::'), enum_name)
        self.generated_types_avro_namespace[qualified_enum_name] = "enum"
        self.generated_types_rust_package[qualified_enum_name] = "enum"
        symbols = avro_schema.get('symbols', [])
        enum_definition += f"#[derive(Debug, Serialize, Deserialize)]\n" if self.serde_annotation else f"#[derive(Debug)]\n"
        enum_definition += f"pub enum {enum_name} {{\n"
        for symbol in symbols:
            enum_definition += f"{INDENT}{symbol},\n"
        enum_definition += "}\n\n"
        self.write_to_file(namespace.replace('.', '::'), enum_name, enum_definition)
        return qualified_enum_name

    def generate_impl_block(self, struct_name: str, avro_schema: Dict) -> str:
        """Generates a single impl block for all methods and static fields"""
        impl_block = f"impl {struct_name} {{\n"
        impl_block += self.generate_static_schema_field(avro_schema)
        impl_block += self.generate_to_byte_array_method(struct_name)
        impl_block += self.generate_from_data_method(struct_name)
        impl_block += self.generate_is_json_match_method(struct_name, avro_schema)
        impl_block += self.generate_to_object_method(struct_name)
        impl_block += "}\n"
        return impl_block

    def generate_static_schema_field(self, avro_schema: Dict) -> str:
        """Generates a static field containing the Avro schema"""
        schema_json = json.dumps(avro_schema).replace('"', '\\"')
        static_field = f"{INDENT}/// The static Avro schema as a JSON string\n"
        static_field += f"{INDENT}pub const SCHEMA: &'static str = \"{schema_json}\";\n"
        return static_field

    def generate_to_byte_array_method(self, struct_name: str) -> str:
        """Generates the to_byte_array method for the struct"""
        method_definition = f"{INDENT}/// Serializes the struct to a byte array based on the provided content type\n"
        method_definition += f"{INDENT}pub fn to_byte_array(&self, content_type: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {{\n"
        method_definition += f"{INDENT*2}let result: Vec<u8>;\n"
        method_definition += f"{INDENT*2}let media_type = content_type.split(';').next().unwrap_or(\"\");\n"
        method_definition += f"{INDENT*2}match media_type {{\n"
        if self.serde_annotation:
            method_definition += f"{INDENT*3}\"application/json\" => {{\n"
            method_definition += f"{INDENT*4}result = serde_json::to_vec(self)?;\n"
            method_definition += f"{INDENT*3}}}\n"
        if self.avro_annotation:
            method_definition += f"{INDENT*3}\"avro/binary\" | \"application/vnd.apache.avro+avro\" => {{\n"
            method_definition += f"{INDENT*4}let mut writer = avro_rs::Writer::new(&Self::SCHEMA, &self)?;\n"
            method_definition += f"{INDENT*4}result = writer.into_inner()?;\n"
            method_definition += f"{INDENT*3}}}\n"
        method_definition += f"{INDENT*3}_ => return Err(format!(\"unsupported media type: {{}}\", media_type).into()),\n"
        method_definition += f"{INDENT*2}}}\n"
        method_definition += f"{INDENT*2}if media_type.ends_with(\"+gzip\") {{\n"
        method_definition += f"{INDENT*3}let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());\n"
        method_definition += f"{INDENT*3}encoder.write_all(&result)?;\n"
        method_definition += f"{INDENT*3}result = encoder.finish()?;\n"
        method_definition += f"{INDENT*2}}}\n"
        method_definition += f"{INDENT*2}Ok(result)\n"
        method_definition += f"{INDENT}}}\n"
        return method_definition

    def generate_from_data_method(self, struct_name: str) -> str:
        """Generates the from_data method for the struct"""
        method_definition = f"{INDENT}/// Deserializes the struct from a byte array based on the provided content type\n"
        method_definition += f"{INDENT}pub fn from_data(data: impl AsRef<[u8]>, content_type: &str) -> Result<Self, Box<dyn std::error::Error>> {{\n"
        method_definition += f"{INDENT*2}let media_type = content_type.split(';').next().unwrap_or(\"\");\n"
        method_definition += f"{INDENT*2}let data = if media_type.ends_with(\"+gzip\") {{\n"
        method_definition += f"{INDENT*3}let mut decoder = flate2::read::GzDecoder::new(data.as_ref());\n"
        method_definition += f"{INDENT*3}let mut decompressed_data = Vec::new();\n"
        method_definition += f"{INDENT*3}std::io::copy(&mut decoder, &mut decompressed_data)?;\n"
        method_definition += f"{INDENT*3}decompressed_data\n"
        method_definition += f"{INDENT*2}}} else {{\n"
        method_definition += f"{INDENT*3}data.as_ref().to_vec()\n"
        method_definition += f"{INDENT*2}}};\n"
        method_definition += f"{INDENT*2}match media_type {{\n"
        if self.serde_annotation:
            method_definition += f"{INDENT*3}\"application/json\" => {{\n"
            method_definition += f"{INDENT*4}let result = serde_json::from_slice(&data)?;\n"
            method_definition += f"{INDENT*4}Ok(result)\n"
            method_definition += f"{INDENT*3}}}\n"
        if self.avro_annotation:
            method_definition += f"{INDENT*3}\"avro/binary\" | \"application/vnd.apache.avro+avro\" => {{\n"
            method_definition += f"{INDENT*4}let reader = avro_rs::Reader::new(&data[..], &Self::SCHEMA)?;\n"
            method_definition += f"{INDENT*4}let result = reader.collect::<Result<Self, _>>()?.pop().ok_or(\"failed to read Avro data\")?;\n"
            method_definition += f"{INDENT*4}Ok(result)\n"
            method_definition += f"{INDENT*3}}}\n"
        method_definition += f"{INDENT*3}_ => Err(format!(\"unsupported media type: {{}}\", media_type).into()),\n"
        method_definition += f"{INDENT*2}}}\n"
        method_definition += f"{INDENT}}}\n"
        return method_definition

    def generate_is_json_match_method(self, struct_name: str, avro_schema: Dict) -> str:
        """Generates the is_json_match method for the struct"""
        method_definition = f"{INDENT}/// Checks if the given JSON value matches the schema of the struct\n"
        method_definition += f"{INDENT}pub fn is_json_match(node: &serde_json::Value) -> bool {{\n"
        predicates = []
        for field in avro_schema.get('fields', []):
            field_name = camel(field['name'])
            field_type = self.convert_avro_type_to_rust(field_name, field['type'], namespace='')
            predicates.append(self.get_is_json_match_clause(field_name, field_type))
        method_definition += f"{INDENT*2}" + " && ".join(predicates) + "\n"
        method_definition += f"{INDENT}}}\n"
        return method_definition

    def get_is_json_match_clause(self, field_name: str, field_type: str) -> str:
        """Generates the is_json_match clause for a field"""
        if field_type == 'String' or field_type == 'Option<String>':
            return f"node[\"{field_name}\"].is_string()"
        elif field_type == 'bool' or field_type == 'Option<bool>':
            return f"node[\"{field_name}\"].is_boolean()"
        elif field_type == 'i32' or field_type == 'Option<i32>':
            return f"node[\"{field_name}\"].is_i64()"
        elif field_type == 'i64' or field_type == 'Option<i64>':
            return f"node[\"{field_name}\"].is_i64()"
        elif field_type == 'f32' or field_type == 'Option<f32>':
            return f"node[\"{field_name}\"].is_f64()"
        elif field_type == 'f64' or field_type == 'Option<f64>':
            return f"node[\"{field_name}\"].is_f64()"
        elif field_type == 'Vec<u8>' or field_type == 'Option<Vec<u8>>':
            return f"node[\"{field_name}\"].is_array()"
        elif field_type == 'serde_json::Value' or field_type == 'std::collections::HashMap<String, String>':
            return f"node[\"{field_name}\"].is_object()"
        elif field_type.startswith('std::collections::HashMap<String, '):
            return f"node[\"{field_name}\"].is_object()"
        elif field_type.startswith('Vec<'):
            return f"node[\"{field_name}\"].is_array()"
        else:
            return f"Self::is_json_match(node[\"{field_name}\"])"

    def generate_to_object_method(self, struct_name: str) -> str:
        """Generates the to_object method for the struct"""
        method_definition = f"{INDENT}/// Converts the struct to a JSON object\n"
        method_definition += f"{INDENT}pub fn to_object(&self) -> serde_json::Value {{\n"
        method_definition += f"{INDENT*2}serde_json::to_value(self).unwrap_or(serde_json::Value::Null)\n"
        method_definition += f"{INDENT}}}\n"
        return method_definition

    def generate_union_enum(self, field_name: str, avro_type: List, namespace: str) -> str:
        """Generates a union enum for Rust"""
        union_enum_name = pascal(field_name) + 'Union'
        enum_definition = f"#[derive(Debug, Serialize, Deserialize)]\n" if self.serde_annotation else f"#[derive(Debug)]\n"
        if self.serde_annotation:
            enum_definition += f"#[serde(untagged)]\n"
        enum_definition += f"pub enum {union_enum_name} {{\n"
        union_types = [self.convert_avro_type_to_rust(field_name + "Option" + str(i), t, namespace) for i, t in enumerate(avro_type)]
        for union_type in union_types:
            safe_field_name = self.safe_identifier(union_type.split('::')[-1])
            enum_definition += f"{INDENT}{safe_field_name}({union_type}),\n"
        enum_definition += "}\n\n"
        self.write_to_file(namespace.replace('.', '::'), union_enum_name, enum_definition)
        return union_enum_name

    def write_to_file(self, package: str, name: str, definition: str):
        """Writes a Rust struct or enum to a file"""
        directory_path = os.path.join(
            self.output_dir, "src", package.replace('.', os.sep).replace('::', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}.rs")

        with open(file_path, 'w', encoding='utf-8') as file:
            if "chrono::NaiveDate" in definition:
                file.write("use chrono::NaiveDate;\n")
            if "chrono::NaiveTime" in definition:
                file.write("use chrono::NaiveTime;\n")
            if "chrono::NaiveDateTime" in definition:
                file.write("use chrono::NaiveDateTime;\n")
            if "uuid::Uuid" in definition:
                file.write("use uuid::Uuid;\n")
            if "flate2" in definition:
                file.write("use flate2::read::GzDecoder;\n")
                file.write("use flate2::write::GzEncoder;\n")
            if "serde_json::Value" in definition:
                file.write("use serde_json::Value;\n")
            if "std::collections::HashMap" in definition:
                file.write("use std::collections::HashMap;\n")
            if "serde::Serialize" in definition:
                file.write("use serde::Serialize;\n")
            if "serde::Deserialize" in definition:
                file.write("use serde::Deserialize;\n")
            if "avro_rs" in definition:
                file.write("use avro_rs::{Reader, Writer};\n")
            if "std::io" in definition:
                file.write("use std::io;\n")
            file.write(definition)

        self.write_mod_rs(package)

    def write_mod_rs(self, package: str):
        """Writes the mod.rs file for a Rust module"""
        directories = package.split('::')
        for i in range(len(directories)):
            sub_package = '::'.join(directories[:i + 1])
            directory_path = os.path.join(
                self.output_dir, "src", sub_package.replace('.', os.sep).replace('::', os.sep))
            if not os.path.exists(directory_path):
                os.makedirs(directory_path, exist_ok=True)
            mod_rs_path = os.path.join(directory_path, "mod.rs")
            
            types = [file.replace('.rs', '') for file in os.listdir(directory_path) if file.endswith('.rs') and file != "mod.rs"]
            mod_statements = '\n'.join(f'pub mod {typ};' for typ in types)

            with open(mod_rs_path, 'w', encoding='utf-8') as file:
                file.write(mod_statements)

    def write_cargo_toml(self):
        """Writes the Cargo.toml file for the Rust project"""
        dependencies = []
        if self.serde_annotation:
            dependencies.append('serde = { version = "1.0", features = ["derive"] }')
            dependencies.append('serde_json = "1.0"')
        if any(typ in self.generated_types_avro_namespace.values() for typ in ['chrono::NaiveDate', 'chrono::NaiveTime', 'chrono::NaiveDateTime']):
            dependencies.append('chrono = "0.4"')
        if any(typ in self.generated_types_avro_namespace.values() for typ in ['uuid::Uuid']):
            dependencies.append('uuid = { version = "0.8", features = ["serde"] }')
        if any(typ in self.generated_types_avro_namespace.values() for typ in ['flate2']):
            dependencies.append('flate2 = "1.0"')
        if self.avro_annotation:
            dependencies.append('avro-rs = "0.9"')

        cargo_toml_content =  f"[package]\n"
        cargo_toml_content += f"name = \"{self.base_package.replace('/', '_')}\"\n"
        cargo_toml_content += f"version = \"0.1.0\"\n"
        cargo_toml_content += f"edition = \"2018\"\n\n"
        cargo_toml_content += f"[dependencies]\n"
        cargo_toml_content += "\n".join(f"{dependency}" for dependency in dependencies)
        cargo_toml_path = os.path.join(self.output_dir, "Cargo.toml")
        with open(cargo_toml_path, 'w', encoding='utf-8') as file:
            file.write(cargo_toml_content)

    def write_lib_rs(self):
        """Writes the lib.rs file for the Rust project"""
        modules = {name.split('::')[0] for name in self.generated_types_rust_package.keys()}
        mod_statements = '\n'.join(f'pub mod {module};' for module in modules)
        
        lib_rs_content = f"""
// This is the library entry point

{mod_statements}
"""
        lib_rs_path = os.path.join(self.output_dir, "src", "lib.rs")
        if not os.path.exists(os.path.dirname(lib_rs_path)):
            os.makedirs(os.path.dirname(lib_rs_path), exist_ok=True)
        with open(lib_rs_path, 'w', encoding='utf-8') as file:
            file.write(lib_rs_content)

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts Avro schema to Rust"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        for avro_schema in (x for x in schema if isinstance(x, dict)):
            self.generate_class_or_enum(avro_schema)

        self.write_cargo_toml()
        self.write_lib_rs()

    def convert(self, avro_schema_path: str, output_dir: str):
        """Converts Avro schema to Rust"""
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_avro_to_rust(avro_schema_path, rust_file_path, package_name='', avro_annotation=False, serde_annotation=False):
    """Converts Avro schema to Rust structs

    Args:
        avro_schema_path (str): Avro input schema path  
        rust_file_path (str): Output Rust file path 
        package_name (str): Base package name
        avro_annotation (bool): Include Avro annotations
        serde_annotation (bool): Include Serde annotations
    """
    avrotorust = AvroToRust()
    avrotorust.base_package = package_name
    avrotorust.avro_annotation = avro_annotation
    avrotorust.serde_annotation = serde_annotation
    avrotorust.convert(avro_schema_path, rust_file_path)


def convert_avro_schema_to_rust(avro_schema: JsonNode, output_dir: str, package_name='', avro_annotation=False, serde_annotation=False):
    """Converts Avro schema to Rust structs

    Args:
        avro_schema (JsonNode): Avro schema as a dictionary or list of dictionaries
        output_dir (str): Output directory path 
        package_name (str): Base package name
        avro_annotation (bool): Include Avro annotations
        serde_annotation (bool): Include Serde annotations
    """
    avrotorust = AvroToRust()
    avrotorust.base_package = package_name
    avrotorust.avro_annotation = avro_annotation
    avrotorust.serde_annotation = serde_annotation
    avrotorust.convert_schema(avro_schema, output_dir)
