import json
import os
from typing import Dict, List, Union
from avrotize.common import is_generic_avro_type, render_template, pascal, camel, snake

INDENT = '    '

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


class AvroToRust:
    """Converts Avro schema to Rust structs, including Serde and Avro marshalling methods"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package.replace('.', '/').lower()
        self.output_dir = os.getcwd()
        self.generated_types_avro_namespace: Dict[str, str] = {}
        self.generated_types_rust_package: Dict[str, str] = {}
        self.avro_annotation = False
        self.serde_annotation = False
        
    reserved_words = [
            'as', 'break', 'const', 'continue', 'crate', 'else', 'enum', 'extern', 'false', 'fn', 'for', 'if', 'impl',
            'in', 'let', 'loop', 'match', 'mod', 'move', 'mut', 'pub', 'ref', 'return', 'self', 'Self', 'static',
            'struct', 'super', 'trait', 'true', 'type', 'unsafe', 'use', 'where', 'while', 'async', 'await', 'dyn',
        ]

    def safe_identifier(self, name: str) -> str:
        """Converts a name to a safe Rust identifier"""
        if name in AvroToRust.reserved_words:
            return f"{name}_"
        return name
    
    def escaped_identifier(self, name: str) -> str:
        """Converts a name to a safe Rust identifier with a leading r# prefix"""
        if name != "crate" and name in AvroToRust.reserved_words:
            return f"r#{name}"
        return name
    
    def safe_package(self, package: str) -> str:
        """Converts a package name to a safe Rust package name"""
        elements = package.split('::')
        return '::'.join([self.escaped_identifier(element) for element in elements])

    def map_primitive_to_rust(self, avro_fullname: str, is_optional: bool) -> str:
        """Maps Avro primitive types to Rust types"""
        optional_mapping = {
            'null': 'None',
            'boolean': 'Option<bool>',
            'int': 'Option<i32>',
            'long': 'Option<i64>',
            'float': 'Option<f32>',
            'double': 'Option<f64>',
            'bytes': 'Option<Vec<u8>>',
            'string': 'Option<String>',
        }
        required_mapping = {
            'null': 'None',
            'boolean': 'bool',
            'int': 'i32',
            'long': 'i64',
            'float': 'f32',
            'double': 'f64',
            'bytes': 'Vec<u8>',
            'string': 'String',
        }
        rust_fullname = avro_fullname
        if '.' in rust_fullname:
            type_name = pascal(avro_fullname.split('.')[-1])
            package_name = '::'.join(avro_fullname.split('.')[:-1]).lower()
            rust_fullname = self.safe_package(self.concat_package(package_name, type_name))
        if rust_fullname in self.generated_types_rust_package:
            return rust_fullname
        else:
            return required_mapping.get(avro_fullname, avro_fullname) if not is_optional else optional_mapping.get(avro_fullname, avro_fullname)

    def concat_package(self, package: str, name: str) -> str:
        """Concatenates package and name using a double colon separator"""
        return f"crate::{package.lower()}::{name.lower()}::{name}" if package else name

    def convert_avro_type_to_rust(self, field_name: str, avro_type: Union[str, Dict, List], namespace: str, nullable: bool = False) -> str:
        """Converts Avro type to Rust type"""
        ns = namespace.replace('.', '::').lower()
        type_name = ''
        if isinstance(avro_type, str):
            type_name = self.map_primitive_to_rust(avro_type, nullable)
        elif isinstance(avro_type, list):
            if is_generic_avro_type(avro_type):
                return 'serde_json::Value' if self.serde_annotation else 'std::collections::HashMap<String, String>'
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                # Rust apache-avro has a bug in the union type handling, so we need to swap the types
                # if the first type is not null
                if avro_type[0] != 'null':
                    avro_type[1] = avro_type[0]
                    avro_type[0] = 'null'
                if isinstance(non_null_types[0], str):
                    type_name = self.map_primitive_to_rust(non_null_types[0], True)
                else:
                    type_name = self.convert_avro_type_to_rust(field_name, non_null_types[0], namespace)
            else:
                type_name = self.generate_union_enum(field_name, avro_type, namespace)
        elif isinstance(avro_type, dict):
            if avro_type['type'] in ['record', 'enum']:
                type_name = self.generate_class_or_enum(avro_type, namespace)
            elif avro_type['type'] == 'fixed' or avro_type['type'] == 'bytes' and 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'decimal':
                    return 'f64'
            elif avro_type['type'] == 'array':
                item_type = self.convert_avro_type_to_rust(field_name, avro_type['items'], namespace)
                return f"Vec<{item_type}>"
            elif avro_type['type'] == 'map':
                values_type = self.convert_avro_type_to_rust(field_name, avro_type['values'], namespace)
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
            else:
                type_name = self.convert_avro_type_to_rust(field_name, avro_type['type'], namespace)
        if type_name:
            return type_name
        return 'serde_json::Value' if self.serde_annotation else 'std::collections::HashMap<String, String>'

    def generate_class_or_enum(self, avro_schema: Dict, parent_namespace: str = '') -> str:
        """Generates a Rust struct or enum from an Avro schema"""
        namespace = avro_schema.get('namespace', parent_namespace).lower()
        if avro_schema['type'] == 'record':
            return self.generate_struct(avro_schema, namespace)
        elif avro_schema['type'] == 'enum':
            return self.generate_enum(avro_schema, namespace)
        return 'serde_json::Value'

    def generate_struct(self, avro_schema: Dict, parent_namespace: str) -> str:
        """Generates a Rust struct from an Avro record schema"""
        fields = []
        for field in avro_schema.get('fields', []):
            original_field_name = field['name']
            field_name = self.safe_identifier(snake(original_field_name))
            field_type = self.convert_avro_type_to_rust(field_name, field['type'], parent_namespace)
            serde_rename = field_name != original_field_name
            fields.append({
                'original_name': original_field_name,
                'name': field_name,
                'type': field_type,
                'serde_rename': serde_rename,
                'random_value': self.generate_random_value(field_type)
            })
        
        struct_name = self.safe_identifier(pascal(avro_schema['name']))
        ns = parent_namespace.replace('.', '::').lower()
        qualified_struct_name = self.safe_package(self.concat_package(ns, struct_name))
        if not 'namespace' in avro_schema:
            avro_schema['namespace'] = parent_namespace
        avro_schema_str = json.dumps(avro_schema)        
        avro_schema_str = avro_schema_str.replace('"', '§')
        avro_schema_str = f"\",\n{INDENT*2}\"".join(
            [avro_schema_str[i:i+80] for i in range(0, len(avro_schema_str), 80)])
        avro_schema_str = avro_schema_str.replace('§', '\\"')
        avro_schema_str = f"concat!(\"{avro_schema_str}\")"

        context = {
            'avro_annotation': self.avro_annotation,
            'serde_annotation': self.serde_annotation,
            'doc': avro_schema.get('doc', ''),
            'struct_name': struct_name,
            'fields': fields,
            'avro_schema': avro_schema_str,
            'json_match_predicates': [self.get_is_json_match_clause(f['original_name'], f['type']) for f in fields]
        }

        file_name = self.to_file_name(qualified_struct_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('avrotorust/dataclass_struct.rs.jinja', target_file, **context)
        self.write_mod_rs(parent_namespace)

        self.generated_types_avro_namespace[qualified_struct_name] = "struct"
        self.generated_types_rust_package[qualified_struct_name] = "struct"

        return qualified_struct_name
    
    def get_is_json_match_clause(self, field_name: str, field_type: str, for_union=False) -> str:
        """Generates the is_json_match clause for a field"""
        ref = f'node[\"{field_name}\"]' if not for_union else 'node'
        if field_type == 'String' or field_type == 'Option<String>':
            return f"{ref}.is_string()"
        elif field_type == 'bool' or field_type == 'Option<bool>':
            return f"{ref}.is_boolean()"
        elif field_type == 'i32' or field_type == 'Option<i32>':
            return f"{ref}.is_i64()"
        elif field_type == 'i64' or field_type == 'Option<i64>':
            return f"{ref}.is_i64()"
        elif field_type == 'f32' or field_type == 'Option<f32>':
            return f"{ref}.is_f64()"
        elif field_type == 'f64' or field_type == 'Option<f64>':
            return f"{ref}.is_f64()"
        elif field_type == 'Vec<u8>' or field_type == 'Option<Vec<u8>>':
            return f"{ref}.is_array()"
        elif field_type == 'serde_json::Value' or field_type == 'std::collections::HashMap<String, String>':
            return f"{ref}.is_object()"
        elif field_type.startswith('std::collections::HashMap<String, '):
            return f"{ref}.is_object()"
        elif field_type.startswith('Vec<'):
            return f"{ref}.is_array()"
        else:
            return f"{field_type}::is_json_match(&{ref})"


    def generate_enum(self, avro_schema: Dict, parent_namespace: str) -> str:
        """Generates a Rust enum from an Avro enum schema"""
        symbols = avro_schema.get('symbols', [])
        enum_name = self.safe_identifier(pascal(avro_schema['name']))
        ns = parent_namespace.replace('.', '::').lower()
        qualified_enum_name = self.safe_package(self.concat_package(ns, enum_name))
        
        if not 'namespace' in avro_schema:
            avro_schema['namespace'] = parent_namespace
        avro_schema_str = json.dumps(avro_schema)
        avro_schema_str = avro_schema_str.replace('"', '§')
        avro_schema_str = f"\",\n{INDENT*2}\"".join(
            [avro_schema_str[i:i+80] for i in range(0, len(avro_schema_str), 80)])
        avro_schema_str = avro_schema_str.replace('§', '\\"')
        avro_schema_str = f"concat!(\"{avro_schema_str}\")"

        context = {
            'avro_annotation': self.avro_annotation,
            'serde_annotation': self.serde_annotation,
            'enum_name': enum_name,
            'symbols': symbols,
            'avro_schema': avro_schema_str,
        }

        file_name = self.to_file_name(qualified_enum_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('avrotorust/dataclass_enum.rs.jinja', target_file, **context)
        self.write_mod_rs(parent_namespace)

        self.generated_types_avro_namespace[qualified_enum_name] = "enum"
        self.generated_types_rust_package[qualified_enum_name] = "enum"

        return qualified_enum_name

    def generate_union_enum(self, field_name: str, avro_type: List, namespace: str) -> str:
        """Generates a union enum for Rust"""
        ns = namespace.replace('.', '::').lower()
        union_enum_name = pascal(field_name) + 'Union'
        union_types = [self.convert_avro_type_to_rust(field_name + "Option" + str(i), t, namespace) for i, t in enumerate(avro_type) if t != 'null']
        union_fields = [
            {
                'name': pascal(t.rsplit('::',1)[-1]), 
                'type': t, 
                'random_value': self.generate_random_value(t),
                'default_value': 'Default::default()',
                'json_match_predicate': self.get_is_json_match_clause(field_name, t, for_union=True),
            } for i, t in enumerate(union_types)]
        qualified_union_enum_name = self.safe_package(self.concat_package(ns, union_enum_name))
        context = {
            'serde_annotation': self.serde_annotation,
            'union_enum_name': union_enum_name,
            'union_fields': union_fields,
            'json_match_predicates': [self.get_is_json_match_clause(f['name'], f['type'], for_union=True) for f in union_fields]
        }

        file_name = self.to_file_name(qualified_union_enum_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs").lower()
        render_template('avrotorust/dataclass_union.rs.jinja', target_file, **context)
        self.generated_types_avro_namespace[qualified_union_enum_name] = "union"
        self.generated_types_rust_package[qualified_union_enum_name] = "union"
        self.write_mod_rs(namespace)

        return qualified_union_enum_name

    def to_file_name(self, qualified_name):
        """Converts a qualified union enum name to a file name"""
        if qualified_name.startswith('crate::'):
            qualified_name = qualified_name[(len('crate::')):]
        qualified_name = qualified_name.replace('r#', '')
        return qualified_name.rsplit('::',1)[0].replace('::', os.sep).lower()
    
    def generate_random_value(self, rust_type: str) -> str:
        """Generates a random value for a given Rust type"""
        if rust_type == 'String' or rust_type == 'Option<String>':
            return 'format!("random_string_{}", rand::Rng::gen::<u32>(&mut rng))'
        elif rust_type == 'bool' or rust_type == 'Option<bool>':
            return 'rand::Rng::gen::<bool>(&mut rng)'
        elif rust_type == 'i32' or rust_type == 'Option<i32>':
            return 'rand::Rng::gen_range(&mut rng, 0..100)'
        elif rust_type == 'i64' or rust_type == 'Option<i64>':
            return 'rand::Rng::gen_range(&mut rng, 0..100) as i64'
        elif rust_type == 'f32' or rust_type == 'Option<f32>':
            return '(rand::Rng::gen::<f32>(&mut rng)*1000.0).round()/1000.0'
        elif rust_type == 'f64' or rust_type == 'Option<f64>':
            return '(rand::Rng::gen::<f64>(&mut rng)*1000.0).round()/1000.0'
        elif rust_type == 'Vec<u8>' or rust_type == 'Option<Vec<u8>>':
            return 'vec![rand::Rng::gen::<u8>(&mut rng); 10]'
        elif rust_type == 'chrono::NaiveDate':
            return 'chrono::NaiveDate::from_ymd(rand::Rng::gen_range(&mut rng, 2000..2023), rand::Rng::gen_range(&mut rng, 1..13), rand::Rng::gen_range(&mut rng, 1..29))'
        elif rust_type == 'chrono::NaiveTime':
            return 'chrono::NaiveTime::from_hms(rand::Rng::gen_range(&mut rng, 0..24),rand::Rng::gen_range(&mut rng, 0..60), rand::Rng::gen_range(&mut rng, 0..60))'
        elif rust_type == 'chrono::NaiveDateTime':
            return 'chrono::NaiveDateTime::new(chrono::NaiveDate::from_ymd(rand::Rng::gen_range(&mut rng, 2000..2023), rand::Rng::gen_range(&mut rng, 1..13), rand::Rng::gen_range(&mut rng, 1..29)), chrono::NaiveTime::from_hms(rand::Rng::gen_range(&mut rng, 0..24), rand::Rng::gen_range(&mut rng, 0..60), rand::Rng::gen_range(&mut rng, 0..60)))'
        elif rust_type == 'uuid::Uuid':
            return 'uuid::Uuid::new_v4()'
        elif rust_type.startswith('std::collections::HashMap<String, '):
            inner_type = rust_type.split(', ')[1][:-1]
            return f'(0..3).map(|_| (format!("key_{{}}", rand::Rng::gen::<u32>(&mut rng)), {self.generate_random_value(inner_type)})).collect()'
        elif rust_type.startswith('Vec<'):
            inner_type = rust_type[4:-1]
            return f'(0..3).map(|_| {self.generate_random_value(inner_type)}).collect()'
        elif rust_type in self.generated_types_rust_package:
            return f'{rust_type}::generate_random_instance()'
        else:
            return 'Default::default()'

    def write_mod_rs(self, namespace: str):
        """Writes the mod.rs file for a Rust module"""
        directories = namespace.split('.')
        for i in range(len(directories)):
            sub_package = '::'.join(directories[:i + 1])
            directory_path = os.path.join(
                self.output_dir, "src", sub_package.replace('.', os.sep).replace('::', os.sep))
            if not os.path.exists(directory_path):
                os.makedirs(directory_path, exist_ok=True)
            mod_rs_path = os.path.join(directory_path, "mod.rs")
            
            types = [file.replace('.rs', '') for file in os.listdir(directory_path) if file.endswith('.rs') and file != "mod.rs"]
            mod_statements = '\n'.join(f'pub mod {self.escaped_identifier(typ.lower())};' for typ in types)
            mods = [dir for dir in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, dir))]
            mod_statements += '\n' + '\n'.join(f'pub mod {self.escaped_identifier(mod.lower())};' for mod in mods)

            with open(mod_rs_path, 'w', encoding='utf-8') as file:
                file.write(mod_statements)

    def write_cargo_toml(self):
        """Writes the Cargo.toml file for the Rust project"""
        dependencies = []
        if self.serde_annotation or self.avro_annotation:
            dependencies.append('serde = { version = "1.0", features = ["derive"] }')
            dependencies.append('serde_json = "1.0"')
        dependencies.append('chrono = "0.4"')
        dependencies.append('uuid = { version = "0.8", features = ["serde"] }')
        if self.avro_annotation or self.serde_annotation:
            dependencies.append('flate2 = "1.0"')
        if self.avro_annotation:
            dependencies.append('apache-avro = "0.16.0"')
            dependencies.append('lazy_static = "1.4"')
        dependencies.append('rand = "0.8"')

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
        modules = {name[(len('crate::')):].split('::')[0] for name in self.generated_types_rust_package}
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
    
    if not package_name:
        package_name = os.path.splitext(os.path.basename(avro_schema_path))[0].lower().replace('-', '_')
        
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
