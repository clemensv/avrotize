# pylint: disable=line-too-long

""" StructureToRust class for converting JSON Structure schema to Rust structs """

import json
import os
from typing import Dict, List, Set, Union, Optional, Any, cast

from avrotize.common import pascal, snake, render_template

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

INDENT = '    '


class StructureToRust:
    """Converts JSON Structure schema to Rust structs, including Serde marshalling methods"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package.replace('.', '/').replace('-', '_').lower()
        self.output_dir = os.getcwd()
        self.generated_types_namespace: Dict[str, str] = {}
        self.generated_types_rust_package: Dict[str, str] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}
        self.serde_annotation = True  # Always use serde for JSON Structure
        self.schema_doc: JsonNode = None
        self.schema_registry: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}

    reserved_words = [
        'as', 'break', 'const', 'continue', 'crate', 'else', 'enum', 'extern', 'false', 'fn', 'for', 'if', 'impl',
        'in', 'let', 'loop', 'match', 'mod', 'move', 'mut', 'pub', 'ref', 'return', 'self', 'Self', 'static',
        'struct', 'super', 'trait', 'true', 'type', 'unsafe', 'use', 'where', 'while', 'async', 'await', 'dyn',
    ]

    def safe_identifier(self, name: str) -> str:
        """Converts a name to a safe Rust identifier"""
        if name in StructureToRust.reserved_words:
            return f"{name}_"
        return name

    def escaped_identifier(self, name: str) -> str:
        """Converts a name to a safe Rust identifier with a leading r# prefix"""
        if name != "crate" and name in StructureToRust.reserved_words:
            return f"r#{name}"
        return name

    def safe_package(self, package: str) -> str:
        """Converts a package name to a safe Rust package name"""
        elements = package.split('::')
        return '::'.join([self.escaped_identifier(element) for element in elements])

    def map_primitive_to_rust(self, structure_type: str, is_optional: bool) -> str:
        """Maps JSON Structure primitive types to Rust types"""
        optional_mapping = {
            'null': 'Option<()>',
            'boolean': 'Option<bool>',
            'string': 'Option<String>',
            'integer': 'Option<i64>',
            'number': 'Option<f64>',
            'int8': 'Option<i8>',
            'uint8': 'Option<u8>',
            'int16': 'Option<i16>',
            'uint16': 'Option<u16>',
            'int32': 'Option<i32>',
            'uint32': 'Option<u32>',
            'int64': 'Option<i64>',
            'uint64': 'Option<u64>',
            'int128': 'Option<i128>',
            'uint128': 'Option<u128>',
            'float8': 'Option<f32>',  # Approximation
            'float': 'Option<f32>',
            'double': 'Option<f64>',
            'binary32': 'Option<f32>',
            'binary64': 'Option<f64>',
            'decimal': 'Option<f64>',  # Could use rust_decimal crate
            'binary': 'Option<Vec<u8>>',
            'date': 'Option<chrono::NaiveDate>',
            'time': 'Option<chrono::NaiveTime>',
            'datetime': 'Option<chrono::DateTime<chrono::Utc>>',
            'timestamp': 'Option<chrono::DateTime<chrono::Utc>>',
            'duration': 'Option<chrono::Duration>',
            'uuid': 'Option<uuid::Uuid>',
            'uri': 'Option<String>',
            'jsonpointer': 'Option<String>',
            'any': 'Option<serde_json::Value>',
        }
        required_mapping = {
            'null': '()',
            'boolean': 'bool',
            'string': 'String',
            'integer': 'i64',
            'number': 'f64',
            'int8': 'i8',
            'uint8': 'u8',
            'int16': 'i16',
            'uint16': 'u16',
            'int32': 'i32',
            'uint32': 'u32',
            'int64': 'i64',
            'uint64': 'u64',
            'int128': 'i128',
            'uint128': 'u128',
            'float8': 'f32',
            'float': 'f32',
            'double': 'f64',
            'binary32': 'f32',
            'binary64': 'f64',
            'decimal': 'f64',
            'binary': 'Vec<u8>',
            'date': 'chrono::NaiveDate',
            'time': 'chrono::NaiveTime',
            'datetime': 'chrono::DateTime<chrono::Utc>',
            'timestamp': 'chrono::DateTime<chrono::Utc>',
            'duration': 'chrono::Duration',
            'uuid': 'uuid::Uuid',
            'uri': 'String',
            'jsonpointer': 'String',
            'any': 'serde_json::Value',
        }
        rust_fullname = structure_type
        if '.' in rust_fullname:
            type_name = pascal(structure_type.split('.')[-1])
            package_name = '::'.join(structure_type.split('.')[:-1]).lower()
            rust_fullname = self.safe_package(self.concat_package(package_name, type_name))
        if rust_fullname in self.generated_types_rust_package:
            return rust_fullname
        else:
            return required_mapping.get(structure_type, structure_type) if not is_optional else optional_mapping.get(structure_type, structure_type)

    def concat_package(self, package: str, name: str) -> str:
        """Concatenates package and name using a double colon separator"""
        return f"crate::{package.lower()}::{name.lower()}::{name}" if package else name

    def get_qualified_name(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name with a dot separator"""
        return f"{namespace}.{name}" if namespace != '' else name

    def concat_namespace(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name with a dot separator"""
        if namespace and name:
            return f"{namespace}.{name}"
        elif namespace:
            return namespace
        else:
            return name

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition"""
        # Check if it's an absolute URI reference (schema with $id)
        if not ref.startswith('#/'):
            # Try to resolve from schema registry
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None

        # Handle fragment-only references (internal to document)
        path = ref[2:].split('/')
        schema = context_schema if context_schema else self.schema_doc

        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]

        return schema

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """Recursively registers schemas with $id keywords"""
        if not isinstance(schema, dict):
            return

        # Register this schema if it has an $id
        if '$id' in schema:
            schema_id = schema['$id']
            # Handle relative URIs
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                from urllib.parse import urljoin
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            base_uri = schema_id  # Update base URI for nested schemas

        # Recursively process definitions
        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)

        # Recursively process properties
        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)

        # Recursively process items, values, etc.
        for key in ['items', 'values', 'additionalProperties', 'choices']:
            if key in schema and isinstance(schema[key], dict):
                if key == 'choices':
                    # Choices is a dict of schemas
                    for choice_schema in schema[key].values():
                        if isinstance(choice_schema, dict):
                            self.register_schema_ids(choice_schema, base_uri)
                else:
                    self.register_schema_ids(schema[key], base_uri)

    def convert_structure_type_to_rust(self, field_name: str, structure_type: Union[str, Dict, List], 
                                       namespace: str, nullable: bool = False) -> str:
        """Converts JSON Structure type to Rust type"""
        ns = namespace.replace('.', '::').lower()
        type_name = ''

        if isinstance(structure_type, str):
            type_name = self.map_primitive_to_rust(structure_type, nullable)
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            if len(non_null_types) == 1:
                if isinstance(non_null_types[0], str):
                    type_name = self.map_primitive_to_rust(non_null_types[0], True)
                else:
                    type_name = self.convert_structure_type_to_rust(field_name, non_null_types[0], namespace, nullable=True)
            else:
                # Generate union enum for multiple types
                type_name = self.generate_union_enum(field_name, non_null_types, namespace)
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                if ref_schema:
                    ref_path = structure_type['$ref'].split('/')
                    type_name_ref = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else namespace
                    return self.generate_class_or_choice(ref_schema, ref_namespace, write_file=True, explicit_name=type_name_ref)
                return 'serde_json::Value'

            # Handle enum keyword
            if 'enum' in structure_type:
                return self.generate_enum(structure_type, field_name, namespace, write_file=True)

            # Handle type keyword
            if 'type' not in structure_type:
                return 'serde_json::Value'

            struct_type = structure_type['type']

            # Handle complex types
            if struct_type == 'object':
                return self.generate_struct(structure_type, namespace)
            elif struct_type == 'array':
                items_type = self.convert_structure_type_to_rust(field_name+'Item', structure_type.get('items', {'type': 'any'}), namespace)
                return f"Vec<{items_type}>"
            elif struct_type == 'set':
                items_type = self.convert_structure_type_to_rust(field_name+'Item', structure_type.get('items', {'type': 'any'}), namespace)
                return f"std::collections::HashSet<{items_type}>"
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_rust(field_name+'Value', structure_type.get('values', {'type': 'any'}), namespace)
                return f"std::collections::HashMap<String, {values_type}>"
            elif struct_type == 'choice':
                return self.generate_choice(structure_type, namespace, write_file=True)
            elif struct_type == 'tuple':
                return self.generate_tuple(structure_type, namespace, write_file=True)
            else:
                type_name = self.convert_structure_type_to_rust(field_name, struct_type, namespace, nullable)

        if type_name:
            return type_name
        return 'serde_json::Value'

    def generate_class_or_choice(self, structure_schema: Dict, parent_namespace: str, 
                                 write_file: bool = True, explicit_name: str = '') -> str:
        """Generates a Struct or Choice"""
        struct_type = structure_schema.get('type', 'object')
        if struct_type == 'object':
            return self.generate_struct(structure_schema, parent_namespace, explicit_name=explicit_name)
        elif struct_type == 'choice':
            return self.generate_choice(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'tuple':
            return self.generate_tuple(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        return 'serde_json::Value'

    def generate_struct(self, structure_schema: Dict, parent_namespace: str, explicit_name: str = '') -> str:
        """Generates a Rust struct from a JSON Structure object schema"""
        fields = []
        properties = structure_schema.get('properties', {})
        required_props = structure_schema.get('required', [])

        struct_name = self.safe_identifier(pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedStruct')))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        ns = schema_namespace.replace('.', '::').lower()
        qualified_struct_name = self.safe_package(self.concat_package(ns, struct_name))

        if qualified_struct_name in self.generated_types_rust_package:
            return qualified_struct_name

        for prop_name, prop_schema in properties.items():
            original_field_name = prop_name
            field_name = self.safe_identifier(snake(original_field_name))

            # Determine if required
            is_required = prop_name in required_props if not isinstance(required_props, list) or \
                         len(required_props) == 0 or not isinstance(required_props[0], list) else \
                         any(prop_name in req_set for req_set in required_props)

            field_type = self.convert_structure_type_to_rust(field_name, prop_schema, schema_namespace, nullable=not is_required)
            
            # Add Option wrapper if not required and not already optional
            if not is_required and not field_type.startswith('Option<'):
                field_type = f'Option<{field_type}>'

            serde_rename = field_name != original_field_name
            fields.append({
                'original_name': original_field_name,
                'name': field_name,
                'type': field_type,
                'serde_rename': serde_rename,
                'doc': prop_schema.get('description', prop_schema.get('doc', '')),
                'is_required': is_required,
            })

        # Check if this is an abstract type
        is_abstract = structure_schema.get('abstract', False)

        context = {
            'serde_annotation': self.serde_annotation,
            'doc': structure_schema.get('description', structure_schema.get('doc', struct_name)),
            'struct_name': struct_name,
            'fields': fields,
            'is_abstract': is_abstract,
        }

        file_name = self.to_file_name(qualified_struct_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('structuretorust/dataclass_struct.rs.jinja', target_file, **context)
        self.write_mod_rs(schema_namespace)

        self.generated_types_namespace[qualified_struct_name] = "struct"
        self.generated_types_rust_package[qualified_struct_name] = "struct"
        self.generated_structure_types[qualified_struct_name] = structure_schema

        return qualified_struct_name

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str, write_file: bool) -> str:
        """Generates a Rust enum from JSON Structure enum keyword"""
        symbols = structure_schema.get('enum', [])
        if not symbols:
            return 'serde_json::Value'

        # Determine enum name
        enum_name = self.safe_identifier(pascal(structure_schema.get('name', field_name + 'Enum')))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        ns = schema_namespace.replace('.', '::').lower()
        qualified_enum_name = self.safe_package(self.concat_package(ns, enum_name))

        if qualified_enum_name in self.generated_types_rust_package:
            return qualified_enum_name

        # Convert symbols to safe Rust identifiers
        safe_symbols = []
        for symbol in symbols:
            if isinstance(symbol, str):
                # Convert to PascalCase and make safe
                safe_symbol = pascal(str(symbol).replace('-', '_').replace(' ', '_'))
                safe_symbols.append({'original': symbol, 'safe': self.safe_identifier(safe_symbol)})
            else:
                # For numeric enums
                safe_symbols.append({'original': symbol, 'safe': f'Value{symbol}'})

        context = {
            'serde_annotation': self.serde_annotation,
            'enum_name': enum_name,
            'symbols': safe_symbols,
            'doc': structure_schema.get('description', structure_schema.get('doc', enum_name)),
        }

        file_name = self.to_file_name(qualified_enum_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('structuretorust/dataclass_enum.rs.jinja', target_file, **context)
        self.write_mod_rs(schema_namespace)

        self.generated_types_namespace[qualified_enum_name] = "enum"
        self.generated_types_rust_package[qualified_enum_name] = "enum"
        self.generated_structure_types[qualified_enum_name] = structure_schema

        return qualified_enum_name

    def generate_union_enum(self, field_name: str, structure_types: List, namespace: str) -> str:
        """Generates a union enum for Rust"""
        ns = namespace.replace('.', '::').lower()
        union_enum_name = pascal(field_name) + 'Union'
        union_types = [self.convert_structure_type_to_rust(field_name + "Option" + str(i), t, namespace) 
                      for i, t in enumerate(structure_types)]
        union_fields = [
            {
                'name': pascal(t.rsplit('::',1)[-1]) if '::' in t else pascal(t.split('<')[0]), 
                'type': t,
            } for i, t in enumerate(union_types)]
        qualified_union_enum_name = self.safe_package(self.concat_package(ns, union_enum_name))

        if qualified_union_enum_name in self.generated_types_rust_package:
            return qualified_union_enum_name

        context = {
            'serde_annotation': self.serde_annotation,
            'union_enum_name': union_enum_name,
            'union_fields': union_fields,
        }

        file_name = self.to_file_name(qualified_union_enum_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs").lower()
        render_template('structuretorust/dataclass_union.rs.jinja', target_file, **context)
        self.generated_types_namespace[qualified_union_enum_name] = "union"
        self.generated_types_rust_package[qualified_union_enum_name] = "union"
        self.write_mod_rs(namespace)

        return qualified_union_enum_name

    def generate_choice(self, structure_schema: Dict, parent_namespace: str, 
                       write_file: bool, explicit_name: str = '') -> str:
        """Generates a discriminated union (choice) type"""
        # Choice types can be tagged unions or inline unions
        choice_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedChoice'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        ns = schema_namespace.replace('.', '::').lower()
        qualified_choice_name = self.safe_package(self.concat_package(ns, choice_name))

        if qualified_choice_name in self.generated_types_rust_package:
            return qualified_choice_name

        choices = structure_schema.get('choices', {})
        choice_variants = []

        for choice_key, choice_schema in choices.items():
            variant_name = pascal(choice_key)
            if isinstance(choice_schema, dict):
                if '$ref' in choice_schema:
                    ref_schema = self.resolve_ref(choice_schema['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                    if ref_schema:
                        ref_path = choice_schema['$ref'].split('/')
                        ref_type_name = ref_path[-1]
                        ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                        variant_type = self.generate_class_or_choice(ref_schema, ref_namespace, write_file=True, explicit_name=ref_type_name)
                        variant_type_name = variant_type.rsplit('::', 1)[-1] if '::' in variant_type else variant_type
                        choice_variants.append({
                            'name': self.safe_identifier(variant_name),
                            'type': variant_type_name,
                            'original_name': choice_key,
                        })
                elif 'type' in choice_schema:
                    variant_type = self.convert_structure_type_to_rust(choice_key, choice_schema, schema_namespace)
                    choice_variants.append({
                        'name': self.safe_identifier(variant_name),
                        'type': variant_type,
                        'original_name': choice_key,
                    })

        # Check for selector (for discriminated unions)
        selector = structure_schema.get('selector', 'type')

        context = {
            'serde_annotation': self.serde_annotation,
            'choice_name': choice_name,
            'variants': choice_variants,
            'selector': selector,
            'doc': structure_schema.get('description', structure_schema.get('doc', choice_name)),
        }

        file_name = self.to_file_name(qualified_choice_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('structuretorust/dataclass_choice.rs.jinja', target_file, **context)
        self.write_mod_rs(schema_namespace)

        self.generated_types_namespace[qualified_choice_name] = "choice"
        self.generated_types_rust_package[qualified_choice_name] = "choice"
        self.generated_structure_types[qualified_choice_name] = structure_schema

        return qualified_choice_name

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str, 
                      write_file: bool, explicit_name: str = '') -> str:
        """Generates a Rust tuple type from JSON Structure tuple"""
        # For tuples, we generate a struct with numbered fields since Rust tuples don't serialize well with serde
        tuple_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedTuple'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        ns = schema_namespace.replace('.', '::').lower()
        qualified_tuple_name = self.safe_package(self.concat_package(ns, tuple_name))

        if qualified_tuple_name in self.generated_types_rust_package:
            return qualified_tuple_name

        properties = structure_schema.get('properties', {})
        tuple_order = structure_schema.get('tuple', [])

        # Build tuple elements in order
        fields = []
        for i, prop_name in enumerate(tuple_order):
            if prop_name in properties:
                prop_schema = properties[prop_name]
                field_type = self.convert_structure_type_to_rust(f'field_{i}', prop_schema, schema_namespace)
                fields.append({
                    'original_name': prop_name,
                    'name': snake(prop_name),
                    'type': field_type,
                    'serde_rename': False,
                    'doc': prop_schema.get('description', ''),
                    'is_required': True,
                })

        context = {
            'serde_annotation': self.serde_annotation,
            'tuple_name': tuple_name,
            'fields': fields,
            'doc': structure_schema.get('description', structure_schema.get('doc', tuple_name)),
        }

        file_name = self.to_file_name(qualified_tuple_name)
        target_file = os.path.join(self.output_dir, "src", file_name + ".rs")
        render_template('structuretorust/dataclass_tuple.rs.jinja', target_file, **context)
        self.write_mod_rs(schema_namespace)

        self.generated_types_namespace[qualified_tuple_name] = "tuple"
        self.generated_types_rust_package[qualified_tuple_name] = "tuple"
        self.generated_structure_types[qualified_tuple_name] = structure_schema

        return qualified_tuple_name

    def to_file_name(self, qualified_name):
        """Converts a qualified name to a file name"""
        if qualified_name.startswith('crate::'):
            qualified_name = qualified_name[(len('crate::')):]
        qualified_name = qualified_name.replace('r#', '')
        return qualified_name.rsplit('::',1)[0].replace('::', os.sep).lower()

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
        dependencies = [
            'serde = { version = "1.0", features = ["derive"] }',
            'serde_json = "1.0"',
            'chrono = { version = "0.4", features = ["serde"] }',
            'uuid = { version = "1.11", features = ["serde", "v4"] }',
        ]

        cargo_toml_content = f"[package]\n"
        cargo_toml_content += f"name = \"{self.base_package.replace('/', '_')}\"\n"
        cargo_toml_content += f"version = \"0.1.0\"\n"
        cargo_toml_content += f"edition = \"2021\"\n\n"
        cargo_toml_content += f"[dependencies]\n"
        cargo_toml_content += "\n".join(f"{dependency}" for dependency in dependencies)
        cargo_toml_path = os.path.join(self.output_dir, "Cargo.toml")
        with open(cargo_toml_path, 'w', encoding='utf-8') as file:
            file.write(cargo_toml_content)

    def write_lib_rs(self):
        """Writes the lib.rs file for the Rust project"""
        modules = {name[(len('crate::')):].split('::')[0] for name in self.generated_types_rust_package if name.startswith('crate::')}
        mod_statements = '\n'.join(f'pub mod {module};' for module in modules if module)

        lib_rs_content = f"""// This is the library entry point

{mod_statements}
"""
        lib_rs_path = os.path.join(self.output_dir, "src", "lib.rs")
        if not os.path.exists(os.path.dirname(lib_rs_path)):
            os.makedirs(os.path.dirname(lib_rs_path), exist_ok=True)
        with open(lib_rs_path, 'w', encoding='utf-8') as file:
            file.write(lib_rs_content)

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """Processes the definitions section recursively"""
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition or 'enum' in definition:
                    # This is a type definition
                    current_namespace = self.concat_namespace(namespace_path, '')
                    # Check if this type was already generated
                    check_namespace = current_namespace.replace('.', '::').lower()
                    check_name = pascal(name)
                    check_ref = self.safe_package(self.concat_package(check_namespace, check_name))
                    if check_ref not in self.generated_types_rust_package:
                        self.generate_class_or_choice(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    # This is a namespace
                    new_namespace = self.concat_namespace(namespace_path, name)
                    self.process_definitions(definition, new_namespace)

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts JSON Structure schema to Rust"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir

        # Register all schemas with $id keywords for cross-references
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.register_schema_ids(structure_schema)

        # Process each schema
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.schema_doc = structure_schema

            # Store definitions for later use
            if 'definitions' in structure_schema:
                self.definitions = structure_schema['definitions']

            # Process root type
            if 'type' in structure_schema or 'enum' in structure_schema:
                self.generate_class_or_choice(structure_schema, structure_schema.get('namespace', ''), write_file=True)
            elif '$root' in structure_schema:
                root_ref = structure_schema['$root']
                root_schema = self.resolve_ref(root_ref, structure_schema)
                if root_schema:
                    ref_path = root_ref.split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else ''
                    self.generate_class_or_choice(root_schema, ref_namespace, write_file=True, explicit_name=type_name)

            # Process definitions
            if 'definitions' in structure_schema:
                self.process_definitions(self.definitions, '')

        self.write_cargo_toml()
        self.write_lib_rs()

    def convert(self, structure_schema_path: str, output_dir: str):
        """Converts JSON Structure schema to Rust"""
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_structure_to_rust(structure_schema_path, rust_file_path, package_name=''):
    """Converts JSON Structure schema to Rust structs

    Args:
        structure_schema_path (str): JSON Structure input schema path  
        rust_file_path (str): Output Rust directory path 
        package_name (str): Base package name
    """

    if not package_name:
        package_name = os.path.splitext(os.path.basename(structure_schema_path))[0].lower().replace('-', '_')

    structtorust = StructureToRust()
    structtorust.base_package = package_name
    structtorust.convert(structure_schema_path, rust_file_path)


def convert_structure_schema_to_rust(structure_schema: JsonNode, output_dir: str, package_name=''):
    """Converts JSON Structure schema to Rust structs

    Args:
        structure_schema (JsonNode): JSON Structure schema as a dictionary or list of dictionaries
        output_dir (str): Output directory path 
        package_name (str): Base package name
    """
    structtorust = StructureToRust()
    structtorust.base_package = package_name
    structtorust.convert_schema(structure_schema, output_dir)
