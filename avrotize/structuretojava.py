# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

""" Generates Java classes from JSON Structure schema """
import json
import os
from typing import Dict, List, Set, Tuple, Union, Optional, Any, cast
from avrotize.constants import AVRO_VERSION, JACKSON_VERSION, JDK_VERSION
from avrotize.common import pascal, camel
from avrotize.avrotojava import (
    flatten_type_name,
    is_java_reserved_word,
    POM_CONTENT,
    INDENT
)

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


class StructureToJava:
    """Converts JSON Structure schema to Java classes with Jackson annotations"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package.replace('.', '/')
        self.output_dir = os.getcwd()
        self.jackson_annotations = True  # Always use Jackson for JSON Structure
        self.pascal_properties = False
        self.generated_types: Dict[str, str] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}
        self.schema_doc: JsonNode = None
        self.schema_registry: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}

    def qualified_name(self, package: str, name: str) -> str:
        """Concatenates package and name using a dot separator"""
        slash_package_name = package.replace('.', '/')
        safe_package_slash = self.safe_package(slash_package_name.lower())
        safe_package = safe_package_slash.replace('/', '.')
        return f"{safe_package}.{name}" if package else name

    def join_packages(self, parent_package: str, package: str) -> str:
        """Joins package and name using a dot separator"""
        if parent_package and package:
            return f"{parent_package}.{package}".lower()
        elif parent_package:
            return parent_package.lower()
        return package.lower()

    def concat_namespace(self, namespace: str, name: str) -> str:
        """Concatenates namespace and name with a dot separator"""
        if namespace and name:
            return f"{namespace}.{name}"
        elif namespace:
            return namespace
        else:
            return name

    class JavaType:
        """Java type definition"""

        def __init__(self, type_name: str, is_class: bool = False, is_enum: bool = False) -> None:
            self.type_name = type_name
            self.is_class = is_class
            self.is_enum = is_enum

    def safe_identifier(self, name: str, class_name: str = '') -> str:
        """Converts a name to a safe Java identifier"""
        if is_java_reserved_word(name):
            return f"_{name}"
        if class_name and name == class_name:
            return f"{name}_"
        return name

    def safe_package(self, packageName: str) -> str:
        """Converts a name to a safe Java identifier by checking each path segment"""
        segments = packageName.split('/')
        safe_segments = [
            self.safe_identifier(segment)
            for segment in segments
        ]
        return '/'.join(safe_segments)

    def map_primitive_to_java(self, structure_type: str, is_optional: bool) -> JavaType:
        """Maps JSON Structure primitive types to Java types"""
        optional_mapping = {
            'null': 'Void',
            'boolean': 'Boolean',
            'string': 'String',
            'integer': 'Integer',
            'number': 'Double',
            'int8': 'Byte',
            'uint8': 'Short',
            'int16': 'Short',
            'uint16': 'Integer',
            'int32': 'Integer',
            'uint32': 'Long',
            'int64': 'Long',
            'uint64': 'Long',
            'int128': 'java.math.BigInteger',
            'uint128': 'java.math.BigInteger',
            'float8': 'Float',
            'float': 'Float',
            'double': 'Double',
            'binary32': 'Float',
            'binary64': 'Double',
            'decimal': 'java.math.BigDecimal',
            'binary': 'byte[]',
            'date': 'java.time.LocalDate',
            'time': 'java.time.LocalTime',
            'datetime': 'java.time.OffsetDateTime',
            'timestamp': 'java.time.Instant',
            'duration': 'java.time.Duration',
            'uuid': 'java.util.UUID',
            'uri': 'java.net.URI',
            'jsonpointer': 'String',
            'any': 'Object'
        }
        required_mapping = {
            'null': 'void',
            'boolean': 'boolean',
            'string': 'String',
            'integer': 'int',
            'number': 'double',
            'int8': 'byte',
            'uint8': 'short',
            'int16': 'short',
            'uint16': 'int',
            'int32': 'int',
            'uint32': 'long',
            'int64': 'long',
            'uint64': 'long',
            'int128': 'java.math.BigInteger',
            'uint128': 'java.math.BigInteger',
            'float8': 'float',
            'float': 'float',
            'double': 'double',
            'binary32': 'float',
            'binary64': 'double',
            'decimal': 'java.math.BigDecimal',
            'binary': 'byte[]',
            'date': 'java.time.LocalDate',
            'time': 'java.time.LocalTime',
            'datetime': 'java.time.OffsetDateTime',
            'timestamp': 'java.time.Instant',
            'duration': 'java.time.Duration',
            'uuid': 'java.util.UUID',
            'uri': 'java.net.URI',
            'jsonpointer': 'String',
            'any': 'Object'
        }
        if '.' in structure_type:
            type_name = structure_type.split('.')[-1]
            package_name = '.'.join(structure_type.split('.')[:-1]).lower()
            structure_type = self.qualified_name(package_name, type_name)

        qualified_class_name = self.qualified_name(self.base_package, structure_type)
        if qualified_class_name in self.generated_types:
            kind = self.generated_types[qualified_class_name]
            return StructureToJava.JavaType(qualified_class_name, is_class=kind == "class", is_enum=kind == "enum")
        else:
            return StructureToJava.JavaType(
                required_mapping.get(structure_type, structure_type) if not is_optional else optional_mapping.get(structure_type, structure_type))

    def is_java_primitive(self, java_type: JavaType) -> bool:
        """Checks if a Java type is a primitive type"""
        return java_type.type_name in [
            'void', 'boolean', 'int', 'long', 'float', 'double', 'byte', 'short', 'byte[]', 'String',
            'Boolean', 'Integer', 'Long', 'Float', 'Double', 'Void', 'Byte', 'Short']

    def is_java_optional_type(self, java_type: JavaType) -> bool:
        """Checks if a Java type is an optional type"""
        return java_type.type_name in ['Void', 'Boolean', 'Integer', 'Long', 'Float', 'Double', 'Byte', 'Short']

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
        for key in ['items', 'values', 'additionalProperties']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)

    def convert_structure_type_to_java(self, class_name: str, field_name: str,
                                      structure_type: JsonNode, parent_namespace: str, nullable: bool = False) -> JavaType:
        """Converts JSON Structure type to Java type"""
        if isinstance(structure_type, str):
            return self.map_primitive_to_java(structure_type, nullable)
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            if len(non_null_types) == 1:
                if isinstance(non_null_types[0], str):
                    return self.map_primitive_to_java(non_null_types[0], True)
                else:
                    return self.convert_structure_type_to_java(class_name, field_name, non_null_types[0], parent_namespace)
            else:
                # Multiple non-null types - generate union class
                return self.generate_embedded_union_class(class_name, field_name, non_null_types, parent_namespace, write_file=True)
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                if ref_schema:
                    # Extract type name from the ref
                    ref_path = structure_type['$ref'].split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    return self.generate_class_or_choice(ref_schema, ref_namespace, write_file=True, explicit_name=type_name)
                return StructureToJava.JavaType('Object')

            # Handle enum keyword
            if 'enum' in structure_type:
                return self.generate_enum(structure_type, field_name, parent_namespace, write_file=True)

            # Handle type keyword
            if 'type' not in structure_type:
                return StructureToJava.JavaType('Object')

            struct_type = structure_type['type']

            # Handle complex types
            if struct_type == 'object':
                return self.generate_class(structure_type, parent_namespace, write_file=True)
            elif struct_type == 'array':
                item_type = self.convert_structure_type_to_java(
                    class_name, field_name, structure_type.get('items', {'type': 'any'}), parent_namespace, nullable=True).type_name
                return StructureToJava.JavaType(f"List<{item_type}>")
            elif struct_type == 'set':
                item_type = self.convert_structure_type_to_java(
                    class_name, field_name, structure_type.get('items', {'type': 'any'}), parent_namespace, nullable=True).type_name
                return StructureToJava.JavaType(f"Set<{item_type}>")
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_java(
                    class_name, field_name, structure_type.get('values', {'type': 'any'}), parent_namespace, nullable=True).type_name
                return StructureToJava.JavaType(f"Map<String,{values_type}>")
            elif struct_type == 'choice':
                return self.generate_choice(structure_type, parent_namespace, write_file=True)
            elif struct_type == 'tuple':
                return self.generate_tuple(structure_type, parent_namespace, write_file=True)
            else:
                return self.convert_structure_type_to_java(class_name, field_name, struct_type, parent_namespace)
        return StructureToJava.JavaType('Object')

    def generate_class_or_choice(self, structure_schema: Dict, parent_namespace: str,
                                 write_file: bool = True, explicit_name: str = '') -> JavaType:
        """Generates a Class or Choice"""
        struct_type = structure_schema.get('type', 'object')
        if struct_type == 'object':
            return self.generate_class(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'choice':
            return self.generate_choice(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'tuple':
            return self.generate_tuple(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        return StructureToJava.JavaType('Object')

    def generate_class(self, structure_schema: Dict, parent_namespace: str, write_file: bool,
                      explicit_name: str = '') -> JavaType:
        """Generates a Java class from JSON Structure object type"""
        class_definition = ''

        # Get name and namespace
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        if not 'namespace' in structure_schema:
            structure_schema['namespace'] = schema_namespace
        
        # Build package path
        # If schema has its own namespace, use it directly (it's already fully qualified)
        # Otherwise, prepend base_package
        if schema_namespace:
            package = schema_namespace.replace('.', '/').lower()
        elif self.base_package:
            package = self.base_package.lower()
        else:
            package = ''
        
        package = self.safe_package(package)
        class_name = self.safe_identifier(class_name)
        namespace_qualified_name = self.qualified_name(schema_namespace, class_name)
        qualified_class_name = self.qualified_name(package.replace('/', '.'), class_name)

        if namespace_qualified_name in self.generated_types:
            return StructureToJava.JavaType(qualified_class_name, is_class=True)

        # Check if this is an abstract type
        is_abstract = structure_schema.get('abstract', False)

        # Generate class documentation
        doc = structure_schema.get('description', structure_schema.get('doc', class_name))
        class_definition += f"/** {doc} */\n"

        # Generate properties
        properties = structure_schema.get('properties', {})
        required_props = structure_schema.get('required', [])

        fields_str = [self.generate_property(class_name, prop_name, prop_schema, schema_namespace, required_props)
                     for prop_name, prop_schema in properties.items()]
        class_body = "\n".join(fields_str)

        abstract_modifier = "abstract " if is_abstract else ""
        class_definition += f"public {abstract_modifier}class {class_name} {{\n"
        class_definition += f"{INDENT}public {class_name}() {{}}\n"
        class_definition += class_body

        # Add toByteArray and fromData methods for JSON serialization using Jackson
        class_definition += self.generate_serialization_methods(class_name)

        # Generate Equals and GetHashCode
        class_definition += self.generate_equals_and_gethashcode(structure_schema, class_name, schema_namespace)

        class_definition += "\n}"

        if write_file:
            self.write_to_file(package, class_name, class_definition)

        self.generated_types[namespace_qualified_name] = "class"
        self.generated_structure_types[namespace_qualified_name] = structure_schema
        return StructureToJava.JavaType(qualified_class_name, is_class=True)

    def generate_property(self, class_name: str, prop_name: str, prop_schema: Dict,
                         parent_namespace: str, required_props: List) -> str:
        """Generates a Java property definition"""
        field_name = pascal(prop_name) if self.pascal_properties else prop_name
        safe_field_name = self.safe_identifier(field_name, class_name)

        # Check if this is a const field
        if 'const' in prop_schema:
            const_value = prop_schema['const']
            prop_type = self.convert_structure_type_to_java(class_name, field_name, prop_schema, parent_namespace)
            const_val = self.format_const_value(const_value, prop_type.type_name)

            property_def = ''
            if 'description' in prop_schema or 'doc' in prop_schema:
                property_def += f"{INDENT}/** {prop_schema.get('description', prop_schema.get('doc', ''))} */\n"
            if self.jackson_annotations:
                property_def += f'{INDENT}@com.fasterxml.jackson.annotation.JsonProperty("{prop_name}")\n'
            property_def += f"{INDENT}public static final {prop_type.type_name} {safe_field_name.upper()} = {const_val};\n"
            return property_def

        # Determine if required
        is_required = prop_name in required_props if not isinstance(required_props, list) or \
                     len(required_props) == 0 or not isinstance(required_props[0], list) else \
                     any(prop_name in req_set for req_set in required_props)

        # Get property type
        field_type = self.convert_structure_type_to_java(class_name, field_name, prop_schema, parent_namespace, nullable=not is_required)

        property_def = ''
        if 'description' in prop_schema or 'doc' in prop_schema:
            property_def += f"{INDENT}/** {prop_schema.get('description', prop_schema.get('doc', ''))} */\n"
        if self.jackson_annotations:
            property_def += f'{INDENT}@com.fasterxml.jackson.annotation.JsonProperty("{prop_name}")\n'
        property_def += f"{INDENT}private {field_type.type_name} {safe_field_name};\n"
        property_def += f"{INDENT}public {field_type.type_name} get{pascal(field_name)}() {{ return {safe_field_name}; }}\n"
        property_def += f"{INDENT}public void set{pascal(field_name)}({field_type.type_name} {safe_field_name}) {{ this.{safe_field_name} = {safe_field_name}; }}\n"

        return property_def

    def format_const_value(self, value: Any, java_type: str) -> str:
        """Formats a const value for Java"""
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, (int, float)):
            return str(value)
        return f"null"

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str,
                     write_file: bool) -> JavaType:
        """Generates a Java enum from JSON Structure enum"""
        enum_definition = ''
        enum_name = pascal(structure_schema.get('name', field_name + 'Enum'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)

        package = self.join_packages(self.base_package, schema_namespace).replace('.', '/').lower()
        enum_name = self.safe_identifier(enum_name)
        type_name = self.qualified_name(package.replace('/', '.'), enum_name)
        namespace_qualified_name = self.qualified_name(schema_namespace, enum_name)

        if namespace_qualified_name in self.generated_types:
            return StructureToJava.JavaType(type_name, is_enum=True)

        doc = structure_schema.get('description', structure_schema.get('doc', enum_name))
        enum_definition += f"/** {doc} */\n"

        symbols = structure_schema.get('enum', [])
        symbols_str = ', '.join([str(symbol).upper().replace('-', '_').replace(' ', '_') for symbol in symbols])
        enum_definition += f"public enum {enum_name} {{\n"
        enum_definition += f"{INDENT}{symbols_str};\n"
        enum_definition += "}\n"

        if write_file:
            self.write_to_file(package, enum_name, enum_definition)

        self.generated_types[namespace_qualified_name] = "enum"
        self.generated_structure_types[namespace_qualified_name] = structure_schema
        return StructureToJava.JavaType(type_name, is_enum=True)

    def generate_choice(self, structure_schema: Dict, parent_namespace: str, write_file: bool,
                       explicit_name: str = '') -> JavaType:
        """Generates a discriminated union (choice) type"""
        # For now, return Object type - full implementation would generate proper union class
        return StructureToJava.JavaType('Object')

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str, write_file: bool,
                      explicit_name: str = '') -> JavaType:
        """Generates a tuple type"""
        # For now, return Object type - full implementation would generate proper tuple class
        return StructureToJava.JavaType('Object')

    def generate_embedded_union_class(self, class_name: str, field_name: str, structure_types: List,
                                     parent_namespace: str, write_file: bool) -> JavaType:
        """Generates an embedded union class for handling multiple types"""
        # For now, return Object type - full implementation would generate proper union class
        return StructureToJava.JavaType('Object')

    def generate_serialization_methods(self, class_name: str) -> str:
        """Generates toByteArray and fromData methods for JSON serialization"""
        methods = "\n"

        # toByteArray method
        methods += f"\n{INDENT}/**\n{INDENT} * Converts the object to a byte array\n{INDENT} * @param contentType the content type of the byte array\n{INDENT} * @return the byte array\n{INDENT} */\n"
        methods += f"{INDENT}public byte[] toByteArray(String contentType) throws com.fasterxml.jackson.core.JsonProcessingException {{\n"
        methods += f"{INDENT*2}String mediaType = contentType.split(\";\")[0].trim().toLowerCase();\n"
        methods += f"{INDENT*2}if (mediaType.equals(\"application/json\")) {{\n"
        methods += f"{INDENT*3}return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(this);\n"
        methods += f"{INDENT*2}}}\n"
        methods += f"{INDENT*2}throw new UnsupportedOperationException(\"Unsupported media type: \" + mediaType);\n"
        methods += f"{INDENT}}}\n"

        # fromData factory method
        methods += f"\n{INDENT}/**\n{INDENT} * Converts the data to an object\n{INDENT} * @param data the data to convert\n{INDENT} * @param contentType the content type of the data\n{INDENT} * @return the object\n{INDENT} */\n"
        methods += f"{INDENT}public static {class_name} fromData(Object data, String contentType) throws java.io.IOException {{\n"
        methods += f"{INDENT*2}if (data instanceof {class_name}) return ({class_name})data;\n"
        methods += f"{INDENT*2}String mediaType = contentType.split(\";\")[0].trim().toLowerCase();\n"
        methods += f"{INDENT*2}if (mediaType.equals(\"application/json\")) {{\n"
        methods += f"{INDENT*3}if (data instanceof byte[]) {{\n"
        methods += f"{INDENT*4}return new com.fasterxml.jackson.databind.ObjectMapper().readValue((byte[])data, {class_name}.class);\n"
        methods += f"{INDENT*3}}} else if (data instanceof java.io.InputStream) {{\n"
        methods += f"{INDENT*4}return new com.fasterxml.jackson.databind.ObjectMapper().readValue((java.io.InputStream)data, {class_name}.class);\n"
        methods += f"{INDENT*3}}} else if (data instanceof String) {{\n"
        methods += f"{INDENT*4}return new com.fasterxml.jackson.databind.ObjectMapper().readValue((String)data, {class_name}.class);\n"
        methods += f"{INDENT*3}}}\n"
        methods += f"{INDENT*2}}}\n"
        methods += f"{INDENT*2}throw new UnsupportedOperationException(\"Unsupported media type: \" + contentType);\n"
        methods += f"{INDENT}}}\n"

        return methods

    def generate_equals_and_gethashcode(self, structure_schema: Dict, class_name: str,
                                       parent_namespace: str) -> str:
        """Generates Equals and GetHashCode methods"""
        code = "\n"
        properties = structure_schema.get('properties', {})

        # Filter out const properties
        non_const_properties = {k: v for k, v in properties.items() if 'const' not in v}

        if not non_const_properties:
            code += f"{INDENT}@Override\n{INDENT}public boolean equals(Object obj) {{\n"
            code += f"{INDENT*2}return obj instanceof {class_name};\n"
            code += f"{INDENT}}}\n\n"
            code += f"{INDENT}@Override\n{INDENT}public int hashCode() {{\n"
            code += f"{INDENT*2}return 0;\n"
            code += f"{INDENT}}}\n"
            return code

        # Generate equals method
        code += f"{INDENT}@Override\n{INDENT}public boolean equals(Object obj) {{\n"
        code += f"{INDENT*2}if (this == obj) return true;\n"
        code += f"{INDENT*2}if (obj == null || getClass() != obj.getClass()) return false;\n"
        code += f"{INDENT*2}{class_name} other = ({class_name}) obj;\n"

        equality_checks = []
        for prop_name, prop_schema in non_const_properties.items():
            field_name = pascal(prop_name) if self.pascal_properties else prop_name
            safe_field_name = self.safe_identifier(field_name, class_name)
            field_type = self.convert_structure_type_to_java(class_name, field_name, prop_schema, parent_namespace)

            if field_type.type_name == 'byte[]':
                equality_checks.append(f"java.util.Arrays.equals(this.{safe_field_name}, other.{safe_field_name})")
            elif self.is_java_primitive(field_type) and not self.is_java_optional_type(field_type) and field_type.type_name not in ['String', 'byte[]']:
                # Use == for primitive types (int, long, float, double, boolean, byte, short) but not String or byte[]
                equality_checks.append(f"this.{safe_field_name} == other.{safe_field_name}")
            else:
                equality_checks.append(f"java.util.Objects.equals(this.{safe_field_name}, other.{safe_field_name})")

        code += f"{INDENT*2}return " + f"\n{INDENT*3}&& ".join(equality_checks) + ";\n"
        code += f"{INDENT}}}\n\n"

        # Generate hashCode method
        code += f"{INDENT}@Override\n{INDENT}public int hashCode() {{\n"

        hash_fields = []
        for prop_name, prop_schema in non_const_properties.items():
            field_name = pascal(prop_name) if self.pascal_properties else prop_name
            safe_field_name = self.safe_identifier(field_name, class_name)
            field_type = self.convert_structure_type_to_java(class_name, field_name, prop_schema, parent_namespace)

            if field_type.type_name == 'byte[]':
                hash_fields.append(f"java.util.Arrays.hashCode({safe_field_name})")
            else:
                hash_fields.append(safe_field_name)

        if len(hash_fields) <= 8:
            code += f"{INDENT*2}return java.util.Objects.hash({', '.join(hash_fields)});\n"
        else:
            code += f"{INDENT*2}return java.util.Objects.hash(\n{INDENT*3}{f',{INDENT*3}'.join(hash_fields)}\n{INDENT*2});\n"

        code += f"{INDENT}}}\n"

        return code

    def write_to_file(self, package: str, name: str, definition: str):
        """Writes a Java class or enum to a file"""
        package = package.lower()
        package = self.safe_package(package)
        directory_path = os.path.join(
            self.output_dir, package.replace('.', os.sep).replace('/', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}.java")

        with open(file_path, 'w', encoding='utf-8') as file:
            if package:
                file.write(f"package {package.replace('/', '.')};\n\n")
                # Add common imports
                if "List<" in definition:
                    file.write("import java.util.List;\n")
                if "Set<" in definition:
                    file.write("import java.util.Set;\n")
                if "Map<" in definition:
                    file.write("import java.util.Map;\n")
                if "BigDecimal" in definition:
                    file.write("import java.math.BigDecimal;\n")
                if "BigInteger" in definition:
                    file.write("import java.math.BigInteger;\n")
                if "LocalDate" in definition:
                    file.write("import java.time.LocalDate;\n")
                if "LocalTime" in definition:
                    file.write("import java.time.LocalTime;\n")
                if "OffsetDateTime" in definition:
                    file.write("import java.time.OffsetDateTime;\n")
                if "Instant" in definition:
                    file.write("import java.time.Instant;\n")
                if "Duration" in definition:
                    file.write("import java.time.Duration;\n")
                if "UUID" in definition:
                    file.write("import java.util.UUID;\n")
                if "URI" in definition:
                    file.write("import java.net.URI;\n")
                if self.jackson_annotations:
                    if 'JsonProperty' in definition:
                        file.write("import com.fasterxml.jackson.annotation.JsonProperty;\n")
                    if 'ObjectMapper' in definition:
                        file.write("import com.fasterxml.jackson.databind.ObjectMapper;\n")
                    if 'JsonProcessingException' in definition:
                        file.write("import com.fasterxml.jackson.core.JsonProcessingException;\n")
                if "InputStream" in definition:
                    file.write("import java.io.InputStream;\n")
                if "IOException" in definition:
                    file.write("import java.io.IOException;\n")
            file.write("\n")
            file.write(definition)

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts JSON Structure schema to Java"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        pom_path = os.path.join(output_dir, "pom.xml")
        if not os.path.exists(pom_path):
            package_elements = self.base_package.split('.') if self.base_package else ["com", "example"]
            groupid = '.'.join(package_elements[:-1]) if len(package_elements) > 1 else package_elements[0]
            artifactid = package_elements[-1] if len(package_elements) > 0 else "generated"
            with open(pom_path, 'w', encoding='utf-8') as file:
                file.write(POM_CONTENT.format(
                    groupid=groupid, artifactid=artifactid,
                    AVRO_VERSION=AVRO_VERSION, JACKSON_VERSION=JACKSON_VERSION,
                    JDK_VERSION=JDK_VERSION, PACKAGE=self.base_package))
        output_dir = os.path.join(
            output_dir, "src/main/java".replace('/', os.sep))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir

        # Register all schema IDs first
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.register_schema_ids(structure_schema)

        # Process each schema
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.schema_doc = structure_schema

            # Store definitions for later use
            if 'definitions' in structure_schema:
                self.definitions = structure_schema['definitions']

            # Process root type
            if 'type' in structure_schema:
                self.generate_class_or_choice(structure_schema, '', write_file=True)
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

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """Processes the definitions section recursively"""
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition or 'enum' in definition:
                    # This is a type definition
                    current_namespace = self.concat_namespace(namespace_path, '')
                    check_namespace = self.concat_namespace(self.base_package, current_namespace)
                    check_name = pascal(name)
                    check_ref = self.qualified_name(check_namespace, check_name)
                    if check_ref not in self.generated_types:
                        self.generate_class_or_choice(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    # This is a namespace
                    new_namespace = self.concat_namespace(namespace_path, name)
                    self.process_definitions(definition, new_namespace)

    def convert(self, structure_schema_path: str, output_dir: str):
        """Converts JSON Structure schema to Java"""
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_structure_to_java(structure_schema_path, java_file_path, package_name='', pascal_properties=False):
    """Converts JSON Structure schema to Java classes

    Args:
        structure_schema_path: JSON Structure input schema path
        java_file_path: Output Java directory path
        package_name: Base package name
        pascal_properties: Use PascalCase for property names
    """
    if not package_name:
        package_name = os.path.splitext(os.path.basename(java_file_path))[0].replace('-', '_').lower()
    structuretojava = StructureToJava()
    structuretojava.base_package = package_name
    structuretojava.pascal_properties = pascal_properties
    structuretojava.convert(structure_schema_path, java_file_path)


def convert_structure_schema_to_java(structure_schema: JsonNode, output_dir: str, package_name='', pascal_properties=False):
    """Converts JSON Structure schema to Java classes

    Args:
        structure_schema: JSON Structure schema as a dictionary or list of dictionaries
        output_dir: Output directory path
        package_name: Base package name
        pascal_properties: Use PascalCase for property names
    """
    structuretojava = StructureToJava()
    structuretojava.base_package = package_name
    structuretojava.pascal_properties = pascal_properties
    structuretojava.convert_schema(structure_schema, output_dir)
