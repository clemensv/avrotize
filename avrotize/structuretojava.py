# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

""" Generates Java classes from JSON Structure schema """
import json
import os
from typing import Dict, List, Tuple, Union, Set, Optional, Any
from avrotize.constants import JACKSON_VERSION, JDK_VERSION

from avrotize.common import pascal, camel

INDENT = '    '
POM_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>{groupid}</groupId>
    <artifactId>{artifactid}</artifactId>
    <version>1.0-SNAPSHOT</version>
    <properties>
        <maven.compiler.source>{JDK_VERSION}</maven.compiler.source>
        <maven.compiler.target>{JDK_VERSION}</maven.compiler.target>
    </properties>
    <dependencies>
        <dependency>
            <groupId>com.fasterxml.jackson</groupId>
            <artifactId>jackson-bom</artifactId>
            <version>{JACKSON_VERSION}</version>
            <type>pom</type>
        </dependency>
    </dependencies>
</project>
"""

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


def flatten_type_name(name: str) -> str:
    """Strips the namespace from a name"""
    if name.endswith('[]'):
        return flatten_type_name(name[:-2]+'Array')
    base_name = pascal(name.replace(' ', '').split('.')[-1].replace('>', '').replace('<', '').replace(',', ''))
    return base_name


def is_java_reserved_word(word: str) -> bool:
    """Checks if a word is a Java reserved word"""
    reserved_words = [
        'abstract', 'assert', 'boolean', 'break', 'byte', 'case', 'catch', 'char', 'class', 'const',
        'continue', 'default', 'do', 'double', 'else', 'enum', 'extends', 'final', 'finally', 'float',
        'for', 'goto', 'if', 'implements', 'import', 'instanceof', 'int', 'interface', 'long', 'native',
        'new', 'package', 'private', 'protected', 'public', 'return', 'short', 'static', 'strictfp',
        'super', 'switch', 'synchronized', 'this', 'throw', 'throws', 'transient', 'try', 'void', 'volatile',
        'while', 'true', 'false', 'null', 'record', 
    ]
    return word in reserved_words


class StructureToJava:
    """Converts JSON Structure schema to Java classes, including Jackson annotations"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package.replace('.', '/')
        self.output_dir = os.getcwd()
        self.jackson_annotations = True  # Always use Jackson annotations for JSON Structure
        self.pascal_properties = False
        self.generated_types_structure_namespace: Dict[str,str] = {}
        self.generated_types_java_package: Dict[str,str] = {}
        self.schema_doc: JsonNode = None
        self.schema_registry: Dict[str, Dict] = {}

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

    class JavaType:
        """Java type definition"""

        def __init__(self, type_name: str, union_types: List['StructureToJava.JavaType'] | None = None, is_class: bool = False, is_enum: bool = False) -> None:
            self.type_name = type_name
            self.union_types = union_types
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
            'uint8': 'Short',  # Java doesn't have unsigned byte, use Short
            'int16': 'Short',
            'uint16': 'Integer',  # Java doesn't have unsigned short, use Integer
            'int32': 'Integer',
            'uint32': 'Long',  # Java doesn't have unsigned int, use Long
            'int64': 'Long',
            'uint64': 'Long',  # Java doesn't have unsigned long, use Long (BigInteger would be precise)
            'int128': 'BigInteger',
            'uint128': 'BigInteger',
            'float8': 'Float',
            'float': 'Float',
            'double': 'Double',
            'binary32': 'Float',
            'binary64': 'Double',
            'decimal': 'BigDecimal',
            'binary': 'byte[]',
            'date': 'LocalDate',
            'time': 'LocalTime',
            'datetime': 'Instant',
            'timestamp': 'Instant',
            'duration': 'Duration',
            'uuid': 'UUID',
            'uri': 'URI',
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
            'int128': 'BigInteger',
            'uint128': 'BigInteger',
            'float8': 'float',
            'float': 'float',
            'double': 'double',
            'binary32': 'float',
            'binary64': 'double',
            'decimal': 'BigDecimal',
            'binary': 'byte[]',
            'date': 'LocalDate',
            'time': 'LocalTime',
            'datetime': 'Instant',
            'timestamp': 'Instant',
            'duration': 'Duration',
            'uuid': 'UUID',
            'uri': 'URI',
            'jsonpointer': 'String',
            'any': 'Object'
        }
        if '.' in structure_type:
            type_name = structure_type.split('.')[-1]
            package_name = '.'.join(structure_type.split('.')[:-1]).lower()
            structure_type = self.qualified_name(package_name, type_name)
        if structure_type in self.generated_types_structure_namespace:
            kind = self.generated_types_structure_namespace[structure_type]
            qualified_class_name = self.qualified_name(self.base_package, structure_type)
            return StructureToJava.JavaType(qualified_class_name, is_class=kind=="class", is_enum=kind=="enum")
        else:
            return StructureToJava.JavaType(required_mapping.get(structure_type, structure_type) if not is_optional else optional_mapping.get(structure_type, structure_type))
    
    def is_java_primitive(self, java_type: JavaType) -> bool:
        """Checks if a Java type is a primitive type"""
        return java_type.type_name in [
            'void', 'boolean', 'int', 'long', 'float', 'double', 'byte', 'short', 'byte[]', 'String',
            'Boolean', 'Integer', 'Long', 'Float', 'Double', 'Short', 'Byte', 'Void']
        
    def is_java_optional_type(self, java_type: JavaType) -> bool:
        """Checks if a Java type is an optional type"""
        return java_type.type_name in ['Void', 'Boolean', 'Integer', 'Long', 'Float', 'Double', 'Short', 'Byte']
        
    def is_java_numeric_type(self, java_type: JavaType) -> bool:
        """Checks if a Java type is a numeric type"""
        return java_type.type_name in ['int', 'long', 'float', 'double', 'short', 'byte', 'Integer', 'Long', 'Float', 'Double', 'Short', 'Byte', 'BigInteger', 'BigDecimal']
    
    def is_java_integer_type(self, java_type: JavaType) -> bool:
        """Checks if a Java type is an integer type"""
        return java_type.type_name in ['int', 'long', 'short', 'byte', 'Integer', 'Long', 'Short', 'Byte', 'BigInteger']

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """ Resolves a $ref to the actual schema definition """
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
        """ Recursively registers schemas with $id keywords """
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

    def convert_structure_type_to_java(self, class_name: str, field_name: str, structure_type: Union[str, Dict, List], parent_package: str, nullable: bool = False) -> JavaType:
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
                    return self.convert_structure_type_to_java(class_name, field_name, non_null_types[0], parent_package)
            else:
                # Multiple non-null types - generate union class
                if self.jackson_annotations:
                    return StructureToJava.JavaType(self.generate_embedded_union_class_jackson(class_name, field_name, non_null_types, parent_package, write_file=True), is_class=True)
                else:
                    types: List[StructureToJava.JavaType] = [self.convert_structure_type_to_java(
                        class_name, field_name, t, parent_package) for t in non_null_types]
                    return StructureToJava.JavaType('Object', types)
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                if ref_schema:
                    ref_path = structure_type['$ref'].split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_package
                    return self.generate_class_or_enum(ref_schema, ref_namespace, write_file=True, explicit_name=type_name)
                return StructureToJava.JavaType('Object')
            
            # Handle enum keyword
            if 'enum' in structure_type:
                return self.generate_enum(structure_type, field_name, parent_package, write_file=True)
            
            # Handle type keyword
            if 'type' not in structure_type:
                return StructureToJava.JavaType('Object')
            
            struct_type = structure_type['type']
            
            # Handle complex types
            if struct_type == 'object':
                return self.generate_class(structure_type, parent_package, write_file=True)
            elif struct_type == 'array':
                item_type = self.convert_structure_type_to_java(class_name, field_name, structure_type.get('items', {'type': 'any'}), parent_package, nullable=True).type_name
                return StructureToJava.JavaType(f"List<{item_type}>")
            elif struct_type == 'set':
                item_type = self.convert_structure_type_to_java(class_name, field_name, structure_type.get('items', {'type': 'any'}), parent_package, nullable=True).type_name
                return StructureToJava.JavaType(f"Set<{item_type}>")
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_java(class_name, field_name, structure_type.get('values', {'type': 'any'}), parent_package, nullable=True).type_name
                return StructureToJava.JavaType(f"Map<String,{values_type}>")
            elif struct_type == 'choice':
                return self.generate_choice(structure_type, parent_package, write_file=True)
            elif struct_type == 'tuple':
                return self.generate_tuple(structure_type, parent_package, write_file=True)
            else:
                return self.convert_structure_type_to_java(class_name, field_name, struct_type, parent_package)
        return StructureToJava.JavaType('Object')

    def generate_class_or_enum(self, structure_schema: Dict, parent_package: str, write_file: bool = True, explicit_name: str = '') -> JavaType:
        """ Generates a Java class or enum from a JSON Structure schema """
        struct_type = structure_schema.get('type', 'object')
        if 'enum' in structure_schema:
            return self.generate_enum(structure_schema, explicit_name or structure_schema.get('name', 'UnnamedEnum'), parent_package, write_file)
        elif struct_type == 'object':
            return self.generate_class(structure_schema, parent_package, write_file, explicit_name=explicit_name)
        elif struct_type == 'choice':
            return self.generate_choice(structure_schema, parent_package, write_file, explicit_name=explicit_name)
        elif struct_type == 'tuple':
            return self.generate_tuple(structure_schema, parent_package, write_file, explicit_name=explicit_name)
        return StructureToJava.JavaType('Object')

    def generate_class(self, structure_schema: Dict, parent_package: str, write_file: bool, explicit_name: str = '') -> JavaType:
        """ Generates a Java class from a JSON Structure object schema """
        class_definition = ''
        
        # Get name and namespace
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'))
        schema_namespace = structure_schema.get('namespace', parent_package)
        if not 'namespace' in structure_schema:
            structure_schema['namespace'] = schema_namespace
        package = self.join_packages(self.base_package, schema_namespace).replace('.', '/').lower()
        package = package.replace('.', '/').lower()
        package = self.safe_package(package)
        class_name = self.safe_identifier(class_name)
        namespace_qualified_name = self.qualified_name(schema_namespace, explicit_name or structure_schema.get('name', 'UnnamedClass'))
        qualified_class_name = self.qualified_name(package.replace('/', '.'), class_name)
        if namespace_qualified_name in self.generated_types_structure_namespace:
            return StructureToJava.JavaType(qualified_class_name, is_class=True)
        self.generated_types_structure_namespace[namespace_qualified_name] = "class"
        self.generated_types_java_package[qualified_class_name] = "class"
        
        # Check if this is an abstract type
        is_abstract = structure_schema.get('abstract', False)
        
        # Generate documentation
        doc = structure_schema.get('description', structure_schema.get('doc', class_name))
        class_definition += f"/** {doc} */\n"
        
        if is_abstract:
            class_definition += f"/** This is an abstract type and cannot be instantiated directly. */\n"
        
        # Add deprecation annotation if needed
        if structure_schema.get('deprecated', False):
            deprecated_msg = structure_schema.get('description', f'{class_name} is deprecated')
            class_definition += f"@Deprecated\n"
        
        # Handle inheritance
        base_class = None
        if '$extends' in structure_schema:
            base_ref = structure_schema['$extends']
            base_schema = self.resolve_ref(base_ref, self.schema_doc if isinstance(self.schema_doc, dict) else None)
            if base_schema:
                ref_path = base_ref.split('/')
                base_name = ref_path[-1]
                ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_package
                base_type = self.generate_class(base_schema, ref_namespace, write_file=True, explicit_name=base_name)
                base_class = base_type.type_name
        
        fields_str = [self.generate_property(class_name, field, schema_namespace) for field in structure_schema.get('properties', {}).items()]
        class_body = "\n".join(fields_str)
        
        abstract_modifier = "abstract " if is_abstract else ""
        class_definition += f"public {abstract_modifier}class {class_name}"
        if base_class:
            class_definition += f" extends {base_class}"
        class_definition += " {\n"
        
        # Add default constructor
        constructor_modifier = "protected" if is_abstract else "public"
        class_definition += f"{INDENT}public {class_name}() {{}}\n"
        class_definition += class_body

        # Generate Equals and GetHashCode
        class_definition += self.generate_equals_and_gethashcode(structure_schema, class_name, schema_namespace)

        class_definition += "\n}"

        if write_file:
            self.write_to_file(package, class_name, class_definition)
        return StructureToJava.JavaType(qualified_class_name, is_class=True)

    def generate_property(self, class_name: str, field: Tuple[str, Dict], parent_package: str) -> str:
        """ Generates a Java property definition """
        field_name, field_schema = field
        field_name_java = pascal(field_name) if self.pascal_properties else field_name
        field_type = self.convert_structure_type_to_java(class_name, field_name, field_schema, parent_package)
        safe_field_name = self.safe_identifier(field_name_java, class_name)
        property_def = ''
        
        # Add documentation
        doc = field_schema.get('description', field_schema.get('doc', field_name_java))
        property_def += f"{INDENT}/** {doc} */\n"
        
        # Add JSON property annotation
        if self.jackson_annotations and field_name != field_name_java:
            property_def += f'{INDENT}@JsonProperty("{field_name}")\n'
        
        # Check if this is a const field
        if 'const' in field_schema:
            const_value = field_schema['const']
            prop_type = field_type.type_name
            if prop_type.endswith('?'):
                prop_type = prop_type[:-1]
            const_val = self.format_const_value(const_value, prop_type)
            property_def += f"{INDENT}public static final {prop_type} {safe_field_name} = {const_val};\n"
            return property_def
        
        property_def += f"{INDENT}private {field_type.type_name} {safe_field_name};\n"
        property_def += f"{INDENT}public {field_type.type_name} get{pascal(field_name_java)}() {{ return {safe_field_name}; }}\n"
        property_def += f"{INDENT}public void set{pascal(field_name_java)}({field_type.type_name} {safe_field_name}) {{ this.{safe_field_name} = {safe_field_name}; }}\n"
        
        return property_def

    def format_const_value(self, value: Any, java_type: str) -> str:
        """ Formats a constant value for Java """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, (int, float)):
            if java_type in ['float', 'Float']:
                return f"{value}f"
            elif java_type in ['long', 'Long']:
                return f"{value}L"
            elif java_type in ['double', 'Double']:
                return f"{value}d"
            return str(value)
        return f"/* unsupported const value */"

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_package: str, write_file: bool) -> JavaType:
        """ Generates a Java enum from JSON Structure enum schema """
        enum_definition = ''
        
        # Determine enum name
        enum_name = pascal(structure_schema.get('name', field_name + 'Enum'))
        schema_namespace = structure_schema.get('namespace', parent_package)
        package = self.join_packages(self.base_package, schema_namespace).replace('.', '/').lower()       
        enum_name = self.safe_identifier(enum_name)
        type_name = self.qualified_name(package.replace('/', '.'), enum_name)
        namespace_qualified_name = self.qualified_name(schema_namespace, structure_schema.get('name', field_name + 'Enum'))
        self.generated_types_structure_namespace[namespace_qualified_name] = "enum"
        self.generated_types_java_package[type_name] = "enum"
        
        # Get enum values
        symbols = structure_schema.get('enum', [])
        if not symbols:
            return StructureToJava.JavaType('Object')
        
        # Generate documentation
        doc = structure_schema.get('description', structure_schema.get('doc', enum_name))
        enum_definition += f"/** {doc} */\n"
        
        # Add deprecation if needed
        if structure_schema.get('deprecated', False):
            enum_definition += f"@Deprecated\n"
        
        # Determine base type
        base_type = structure_schema.get('type', 'string')
        numeric_types = ['int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64', 'uint64']
        is_numeric = base_type in numeric_types
        
        if is_numeric:
            java_base_type = self.map_primitive_to_java(base_type, False).type_name
            enum_definition += f"public enum {enum_name} {{\n"
            # Generate numeric enum members
            for i, value in enumerate(symbols):
                member_name = f"VALUE_{value}".upper()
                enum_definition += f"{INDENT}{member_name}({value})"
                if i < len(symbols) - 1:
                    enum_definition += ","
                enum_definition += "\n"
            enum_definition += f"{INDENT};\n"
            enum_definition += f"{INDENT}private final {java_base_type} value;\n"
            enum_definition += f"{INDENT}{enum_name}({java_base_type} value) {{ this.value = value; }}\n"
            enum_definition += f"{INDENT}public {java_base_type} getValue() {{ return value; }}\n"
        else:
            # String enum
            enum_definition += f"public enum {enum_name} {{\n"
            symbols_str = ', '.join([self.safe_identifier(pascal(str(symbol).replace('-', '_').replace(' ', '_'))) for symbol in symbols])
            enum_definition += f"{INDENT}{symbols_str};\n"
        
        enum_definition += "}\n"
        
        if write_file:
            self.write_to_file(package, enum_name, enum_definition)
        return StructureToJava.JavaType(type_name, is_enum=True)

    def generate_choice(self, structure_schema: Dict, parent_package: str, write_file: bool, explicit_name: str = '') -> JavaType:
        """ Generates a choice (discriminated union) type """
        # For simplicity, we'll generate as Object for now
        # Full implementation would need to handle tagged unions properly
        return StructureToJava.JavaType('Object')

    def generate_tuple(self, structure_schema: Dict, parent_package: str, write_file: bool, explicit_name: str = '') -> JavaType:
        """ Generates a tuple type """
        # For simplicity, we'll generate as Object for now
        # Full implementation would generate a proper tuple class
        return StructureToJava.JavaType('Object')

    def generate_embedded_union_class_jackson(self, class_name: str, field_name: str, structure_types: List, parent_package: str, write_file: bool) -> str:
        """ Generates an embedded Union Class for Java using Jackson """
        # Simplified implementation - just return Object for unions
        return 'Object'

    def generate_equals_and_gethashcode(self, structure_schema: Dict, class_name: str, parent_namespace: str) -> str:
        """ Generates Equals and GetHashCode methods """
        code = "\n"
        properties = structure_schema.get('properties', {})
        
        # Filter out const properties
        non_const_properties = {k: v for k, v in properties.items() if 'const' not in v}
        
        if not non_const_properties:
            # Empty class
            code += f"{INDENT}@Override\n{INDENT}public boolean equals(Object obj) {{\n"
            code += f"{INDENT*2}return obj instanceof {class_name};\n"
            code += f"{INDENT}}}\n\n"
            code += f"{INDENT}@Override\n{INDENT}public int hashCode() {{\n"
            code += f"{INDENT*2}return 0;\n"
            code += f"{INDENT}}}\n"
            return code
        
        # Generate equals
        code += f"{INDENT}@Override\n{INDENT}public boolean equals(Object obj) {{\n"
        code += f"{INDENT*2}if (this == obj) return true;\n"
        code += f"{INDENT*2}if (!(obj instanceof {class_name})) return false;\n"
        code += f"{INDENT*2}{class_name} other = ({class_name}) obj;\n"
        
        equality_checks = []
        for prop_name in non_const_properties.keys():
            field_name_java = pascal(prop_name) if self.pascal_properties else prop_name
            safe_field_name = self.safe_identifier(field_name_java, class_name)
            equality_checks.append(f"Objects.equals(this.{safe_field_name}, other.{safe_field_name})")
        
        if len(equality_checks) == 1:
            code += f"{INDENT*2}return {equality_checks[0]};\n"
        else:
            code += f"{INDENT*2}return " + f"\n{INDENT*3}&& ".join(equality_checks) + ";\n"
        
        code += f"{INDENT}}}\n\n"
        
        # Generate hashCode
        code += f"{INDENT}@Override\n{INDENT}public int hashCode() {{\n"
        
        hash_fields = []
        for prop_name in non_const_properties.keys():
            field_name_java = pascal(prop_name) if self.pascal_properties else prop_name
            safe_field_name = self.safe_identifier(field_name_java, class_name)
            hash_fields.append(safe_field_name)
        
        if len(hash_fields) <= 8:
            code += f"{INDENT*2}return Objects.hash({', '.join(hash_fields)});\n"
        else:
            code += f"{INDENT*2}int result = Objects.hash({', '.join(hash_fields[:8])});\n"
            for i in range(8, len(hash_fields)):
                code += f"{INDENT*2}result = 31 * result + Objects.hashCode({hash_fields[i]});\n"
            code += f"{INDENT*2}return result;\n"
        
        code += f"{INDENT}}}\n"
        
        return code

    def write_to_file(self, package: str, name: str, definition: str):
        """ Writes a Java class or enum to a file """
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
                if "Instant" in definition:
                    file.write("import java.time.Instant;\n")
                if "LocalDateTime" in definition:
                    file.write("import java.time.LocalDateTime;\n")
                if "UUID" in definition:
                    file.write("import java.util.UUID;\n")
                if "Duration" in definition:
                    file.write("import java.time.Duration;\n")
                if "URI" in definition:
                    file.write("import java.net.URI;\n")
                if "Objects" in definition:
                    file.write("import java.util.Objects;\n")
                    
            if self.jackson_annotations:
                if 'JsonProperty' in definition:
                    file.write("import com.fasterxml.jackson.annotation.JsonProperty;\n")
                if 'JsonNode' in definition:
                    file.write("import com.fasterxml.jackson.databind.JsonNode;\n")
                if 'ObjectMapper' in definition:
                    file.write("import com.fasterxml.jackson.databind.ObjectMapper;\n")
                if 'JsonSerialize' in definition:
                    file.write("import com.fasterxml.jackson.databind.annotation.JsonSerialize;\n")
                if 'JsonDeserialize' in definition:
                    file.write("import com.fasterxml.jackson.databind.annotation.JsonDeserialize;\n")               
                if 'JsonSerializer' in definition:
                    file.write("import com.fasterxml.jackson.databind.JsonSerializer;\n") 
                if 'SerializerProvider' in definition:
                    file.write("import com.fasterxml.jackson.databind.SerializerProvider;\n")
                if 'JsonDeserializer' in definition:
                    file.write("import com.fasterxml.jackson.databind.JsonDeserializer;\n")
                if 'DeserializationContext' in definition:
                    file.write("import com.fasterxml.jackson.databind.DeserializationContext;\n")
                if 'JsonParser' in definition:
                    file.write("import com.fasterxml.jackson.core.JsonParser;\n")
                if 'JsonIgnore' in definition:
                    file.write("import com.fasterxml.jackson.annotation.JsonIgnore;\n")
                if 'JsonProcessingException' in definition:
                    file.write("import com.fasterxml.jackson.core.JsonProcessingException;\n")
                if 'JsonGenerator' in definition:
                    file.write("import com.fasterxml.jackson.core.JsonGenerator;\n")
                if 'TypeReference' in definition:
                    file.write("import com.fasterxml.jackson.core.type.TypeReference;\n")
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
            artifactid = package_elements[-1]
            with open(pom_path, 'w', encoding='utf-8') as file:
                file.write(POM_CONTENT.format(groupid=groupid, artifactid=artifactid, JACKSON_VERSION=JACKSON_VERSION, JDK_VERSION=JDK_VERSION, PACKAGE=self.base_package))
        output_dir = os.path.join(
            output_dir, "src/main/java".replace('/', os.sep))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        
        # Register all schemas with $id keywords
        for structure_schema in (x for x in schema if isinstance(x, dict)):
            self.schema_doc = structure_schema
            self.register_schema_ids(structure_schema)
        
        # Generate classes
        for structure_schema in (x for x in schema if isinstance(x, dict)):
            self.schema_doc = structure_schema
            if 'definitions' in structure_schema:
                self.process_definitions(structure_schema['definitions'], '')
            if 'type' in structure_schema or 'enum' in structure_schema:
                self.generate_class_or_enum(structure_schema, '')

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """ Processes the definitions section recursively """
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition or 'enum' in definition:
                    # This is a type definition
                    current_namespace = namespace_path
                    self.generate_class_or_enum(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    # This might be a nested namespace
                    new_namespace = f"{namespace_path}.{name}" if namespace_path else name
                    self.process_definitions(definition, new_namespace)

    def convert(self, structure_schema_path: str, output_dir: str):
        """Converts JSON Structure schema to Java"""
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_structure_to_java(structure_schema_path, java_file_path, package_name='', pascal_properties=False, jackson_annotation=True):
    """
    Converts JSON Structure schema to Java classes

    Args:
        structure_schema_path: JSON Structure input schema path  
        java_file_path: Output Java directory path
        package_name: Base package name
        pascal_properties: Use PascalCase for properties
        jackson_annotation: Add Jackson annotations (always True for Structure)
    """
    if not package_name:
        package_name = os.path.splitext(os.path.basename(java_file_path))[0].replace('-', '_').lower()
    structuretojava = StructureToJava()
    structuretojava.base_package = package_name
    structuretojava.pascal_properties = pascal_properties
    structuretojava.jackson_annotations = jackson_annotation
    structuretojava.convert(structure_schema_path, java_file_path)


def convert_structure_schema_to_java(structure_schema: JsonNode, output_dir: str, package_name='', pascal_properties=False, jackson_annotation=True):
    """
    Converts JSON Structure schema to Java classes

    Args:
        structure_schema: JSON Structure schema as a dictionary or list of dictionaries
        output_dir: Output directory path
        package_name: Base package name
        pascal_properties: Use PascalCase for properties
        jackson_annotation: Add Jackson annotations (always True for Structure)
    """
    structuretojava = StructureToJava()
    structuretojava.base_package = package_name
    structuretojava.pascal_properties = pascal_properties
    structuretojava.jackson_annotations = jackson_annotation
    structuretojava.convert_schema(structure_schema, output_dir)
