# pylint: disable=line-too-long

""" StructureToCSharp class for converting JSON Structure schema to C# classes """

import json
import os
import re
from typing import Any, Dict, List, Tuple, Union, cast, Optional
import uuid

from avrotize.common import pascal, process_template
import glob

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


INDENT = '    '


class StructureToCSharp:
    """ Converts JSON Structure schema to C# classes """

    def __init__(self, base_namespace: str = '') -> None:
        self.base_namespace = base_namespace
        self.project_name: str = ''  # Optional explicit project name, separate from namespace
        self.schema_doc: JsonNode = None
        self.output_dir = os.getcwd()
        self.pascal_properties = False
        self.system_text_json_annotation = False
        self.newtonsoft_json_annotation = False
        self.system_xml_annotation = False
        self.generated_types: Dict[str,str] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}
        self.type_dict: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}

    def get_qualified_name(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        return f"{namespace}.{name}" if namespace != '' else name

    def concat_namespace(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        if namespace and name:
            return f"{namespace}.{name}"
        elif namespace:
            return namespace
        else:
            return name

    def map_primitive_to_csharp(self, structure_type: str) -> str:
        """ Maps JSON Structure primitive types to C# types """
        mapping = {
            'null': 'void',  # Placeholder, actual handling for nullable types is in the union logic
            'boolean': 'bool',
            'string': 'string',
            'int8': 'sbyte',
            'uint8': 'byte',
            'int16': 'short',
            'uint16': 'ushort',
            'int32': 'int',
            'uint32': 'uint',
            'int64': 'long',
            'uint64': 'ulong',
            'int128': 'System.Int128',
            'uint128': 'System.UInt128',
            'float8': 'float',  # Approximation - C# doesn't have native 8-bit float
            'float': 'float',
            'double': 'double',
            'binary32': 'float',  # IEEE 754 binary32
            'binary64': 'double',  # IEEE 754 binary64
            'decimal': 'decimal',
            'binary': 'byte[]',
            'date': 'DateOnly',
            'time': 'TimeOnly',
            'datetime': 'DateTimeOffset',
            'timestamp': 'DateTimeOffset',
            'duration': 'TimeSpan',
            'uuid': 'Guid',
            'uri': 'Uri',
            'jsonpointer': 'string',
            'any': 'object'
        }
        qualified_class_name = 'global::'+self.get_qualified_name(pascal(self.base_namespace), pascal(structure_type))
        if qualified_class_name in self.generated_structure_types:
            result = qualified_class_name
        else:
            result = mapping.get(structure_type, 'object')
        return result

    def is_csharp_reserved_word(self, word: str) -> bool:
        """ Checks if a word is a reserved C# keyword """
        reserved_words = [
            'abstract', 'as', 'base', 'bool', 'break', 'byte', 'case', 'catch', 'char', 'checked', 'class', 'const',
            'continue', 'decimal', 'default', 'delegate', 'do', 'double', 'else', 'enum', 'event', 'explicit', 'extern',
            'false', 'finally', 'fixed', 'float', 'for', 'foreach', 'goto', 'if', 'implicit', 'in', 'int', 'interface',
            'internal', 'is', 'lock', 'long', 'namespace', 'new', 'null', 'object', 'operator', 'out', 'override',
            'params', 'private', 'protected', 'public', 'readonly', 'ref', 'return', 'sbyte', 'sealed', 'short', 'sizeof',
            'stackalloc', 'static', 'string', 'struct', 'switch', 'this', 'throw', 'true', 'try', 'typeof', 'uint', 'ulong',
            'unchecked', 'unsafe', 'ushort', 'using', 'virtual', 'void', 'volatile', 'while'
        ]
        return word in reserved_words

    def is_csharp_primitive_type(self, csharp_type: str) -> bool:
        """ Checks if a type is a C# primitive type """
        if csharp_type.endswith('?'):
            csharp_type = csharp_type[:-1]
        return csharp_type in ['void', 'bool', 'sbyte', 'byte', 'short', 'ushort', 'int', 'uint', 'long', 'ulong', 
                               'float', 'double', 'decimal', 'string', 'DateTime', 'DateTimeOffset', 'DateOnly', 
                               'TimeOnly', 'TimeSpan', 'Guid', 'byte[]', 'object', 'System.Int128', 'System.UInt128', 'Uri']

    def map_csharp_primitive_to_clr_type(self, cs_type: str) -> str:
        """ Maps C# primitive types to CLR types"""
        map = {
            "int": "Int32",
            "long": "Int64",
            "float": "Single",
            "double": "Double",
            "decimal": "Decimal",
            "short": "Int16",
            "sbyte": "SByte",
            "byte": "Byte",
            "ushort": "UInt16",
            "uint": "UInt32",
            "ulong": "UInt64",
            "bool": "Boolean",
            "string": "String",
            "Guid": "Guid"
        }
        return map.get(cs_type, cs_type)

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """ Resolves a $ref to the actual schema definition """
        if not ref.startswith('#/'):
            return None
        
        path = ref[2:].split('/')
        schema = context_schema if context_schema else self.schema_doc
        
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        
        return schema

    def convert_structure_type_to_csharp(self, class_name: str, field_name: str, structure_type: JsonNode, parent_namespace: str) -> str:
        """ Converts JSON Structure type to C# type """
        if isinstance(structure_type, str):
            return self.map_primitive_to_csharp(structure_type)
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            if len(non_null_types) == 1:
                # Nullable type
                return f"{self.convert_structure_type_to_csharp(class_name, field_name, non_null_types[0], parent_namespace)}?"
            else:
                return self.generate_embedded_union(class_name, field_name, non_null_types, parent_namespace, write_file=True)
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc)
                if ref_schema:
                    # Extract type name from the ref
                    ref_path = structure_type['$ref'].split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    return self.generate_class_or_choice(ref_schema, ref_namespace, write_file=True, explicit_name=type_name)
                return 'object'
            
            # Handle enum keyword - must be checked before 'type'
            if 'enum' in structure_type:
                return self.generate_enum(structure_type, field_name, parent_namespace, write_file=True)
            
            # Handle type keyword
            if 'type' not in structure_type:
                return 'object'
            
            struct_type = structure_type['type']
            
            # Handle complex types
            if struct_type == 'object':
                return self.generate_class(structure_type, parent_namespace, write_file=True)
            elif struct_type == 'array':
                items_type = self.convert_structure_type_to_csharp(class_name, field_name+'List', structure_type.get('items', {'type': 'any'}), parent_namespace)
                return f"List<{items_type}>"
            elif struct_type == 'set':
                items_type = self.convert_structure_type_to_csharp(class_name, field_name+'Set', structure_type.get('items', {'type': 'any'}), parent_namespace)
                return f"HashSet<{items_type}>"
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_csharp(class_name, field_name+'Map', structure_type.get('values', {'type': 'any'}), parent_namespace)
                return f"Dictionary<string, {values_type}>"
            elif struct_type == 'choice':
                return self.generate_choice(structure_type, parent_namespace, write_file=True)
            elif struct_type == 'tuple':
                return self.generate_tuple(structure_type, parent_namespace, write_file=True)
            else:
                return self.convert_structure_type_to_csharp(class_name, field_name, struct_type, parent_namespace)
        return 'object'

    def generate_class_or_choice(self, structure_schema: Dict, parent_namespace: str, write_file: bool = True, explicit_name: str = '') -> str:
        """ Generates a Class or Choice """
        struct_type = structure_schema.get('type', 'object')
        if struct_type == 'object':
            return self.generate_class(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'choice':
            return self.generate_choice(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'tuple':
            return self.generate_tuple(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        return 'object'

    def generate_class(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """ Generates a Class from JSON Structure object type """
        class_definition = ''
        
        # Get name and namespace
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = pascal(self.concat_namespace(self.base_namespace, schema_namespace))
        xml_namespace = structure_schema.get('xmlns', None)
        
        ref = 'global::'+self.get_qualified_name(namespace, class_name)
        if ref in self.generated_types:
            return ref

        # Generate class documentation
        doc = structure_schema.get('description', structure_schema.get('doc', class_name))
        class_definition += f"/// <summary>\n/// {doc}\n/// </summary>\n"

        # Add XML serialization attribute for the class if enabled
        if self.system_xml_annotation:
            if xml_namespace:
                class_definition += f"[XmlRoot(\"{class_name}\", Namespace=\"{xml_namespace}\")]\n"
            else:
                class_definition += f"[XmlRoot(\"{class_name}\")]\n"

        # Generate properties
        properties = structure_schema.get('properties', {})
        required_props = structure_schema.get('required', [])
        
        # Handle alternative required sets
        is_alternative_required = isinstance(required_props, list) and len(required_props) > 0 and isinstance(required_props[0], list)
        
        fields_str = []
        for prop_name, prop_schema in properties.items():
            field_def = self.generate_property(prop_name, prop_schema, class_name, schema_namespace, required_props)
            fields_str.append(field_def)
        
        class_body = "\n".join(fields_str)
        class_definition += f"public partial class {class_name}\n{{\n{class_body}"
        
        # Add default constructor
        class_definition += f"\n{INDENT}/// <summary>\n{INDENT}/// Default constructor\n{INDENT}/// </summary>\n"
        class_definition += f"{INDENT}public {class_name}()\n{INDENT}{{\n{INDENT}}}"

        # Generate Equals and GetHashCode
        class_definition += self.generate_equals_and_gethashcode(structure_schema, class_name, schema_namespace)

        class_definition += "\n"+"}"

        if write_file:
            self.write_to_file(namespace, class_name, class_definition)

        self.generated_types[ref] = "class"
        self.generated_structure_types[ref] = structure_schema
        return ref

    def generate_property(self, prop_name: str, prop_schema: Dict, class_name: str, parent_namespace: str, required_props: List) -> str:
        """ Generates a property for a class """
        property_definition = ''
        
        # Resolve property name
        field_name = prop_name
        if self.is_csharp_reserved_word(field_name):
            field_name = f"@{field_name}"
        if self.pascal_properties:
            field_name_cs = pascal(field_name)
        else:
            field_name_cs = field_name
        if field_name_cs == class_name:
            field_name_cs += "_"
        
        # Check if this is a const field
        if 'const' in prop_schema:
            const_value = prop_schema['const']
            prop_type = self.convert_structure_type_to_csharp(class_name, field_name, prop_schema, parent_namespace)
            
            # Remove nullable marker for const
            if prop_type.endswith('?'):
                prop_type = prop_type[:-1]
            
            # Generate documentation
            doc = prop_schema.get('description', prop_schema.get('doc', field_name_cs))
            property_definition += f"{INDENT}/// <summary>\n{INDENT}/// {doc}\n{INDENT}/// </summary>\n"
            
            # Add JSON property name annotation
            if self.system_text_json_annotation and field_name != field_name_cs:
                property_definition += f'{INDENT}[System.Text.Json.Serialization.JsonPropertyName("{prop_name}")]\n'
            if self.newtonsoft_json_annotation and field_name != field_name_cs:
                property_definition += f'{INDENT}[Newtonsoft.Json.JsonProperty("{prop_name}")]\n'
            
            # Add XML element annotation if enabled
            if self.system_xml_annotation:
                property_definition += f'{INDENT}[System.Xml.Serialization.XmlElement("{prop_name}")]\n'
            
            # Generate const field
            const_val = self.format_default_value(const_value, prop_type)
            property_definition += f"{INDENT}public const {prop_type} {field_name_cs} = {const_val};\n"
            
            return property_definition
        
        # Determine if required
        is_required = prop_name in required_props if not isinstance(required_props, list) or len(required_props) == 0 or not isinstance(required_props[0], list) else any(prop_name in req_set for req_set in required_props)
        
        # Get property type
        prop_type = self.convert_structure_type_to_csharp(class_name, field_name, prop_schema, parent_namespace)
        
        # Add nullable marker if not required and not already nullable
        if not is_required and not prop_type.endswith('?') and not prop_type.startswith('List<') and not prop_type.startswith('HashSet<') and not prop_type.startswith('Dictionary<'):
            prop_type += '?'
        
        # Generate documentation
        doc = prop_schema.get('description', prop_schema.get('doc', field_name_cs))
        property_definition += f"{INDENT}/// <summary>\n{INDENT}/// {doc}\n{INDENT}/// </summary>\n"
        
        # Add JSON property name annotation
        if self.system_text_json_annotation and field_name != field_name_cs:
            property_definition += f'{INDENT}[System.Text.Json.Serialization.JsonPropertyName("{prop_name}")]\n'
        if self.newtonsoft_json_annotation and field_name != field_name_cs:
            property_definition += f'{INDENT}[Newtonsoft.Json.JsonProperty("{prop_name}")]\n'
        
        # Add XML element annotation if enabled
        if self.system_xml_annotation:
            property_definition += f'{INDENT}[System.Xml.Serialization.XmlElement("{prop_name}")]\n'
        
        # Generate property with required modifier if needed
        required_modifier = "required " if is_required and not prop_type.endswith('?') else ""
        property_definition += f"{INDENT}public {required_modifier}{prop_type} {field_name_cs} {{ get; set; }}"
        
        # Add default value if present
        if 'default' in prop_schema:
            default_val = self.format_default_value(prop_schema['default'], prop_type)
            property_definition += f" = {default_val};\n"
        else:
            property_definition += "\n"
        
        return property_definition

    def format_default_value(self, value: Any, csharp_type: str) -> str:
        """ Formats a default value for C# """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, list):
            return f"new {csharp_type}()"
        elif isinstance(value, dict):
            return f"new {csharp_type}()"
        return f"default({csharp_type})"

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str, write_file: bool) -> str:
        """ Generates a C# enum from JSON Structure enum keyword """
        enum_values = structure_schema.get('enum', [])
        if not enum_values:
            return 'object'
        
        # Determine enum name from field name
        enum_name = pascal(field_name) + 'Enum' if field_name else 'UnnamedEnum'
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = pascal(self.concat_namespace(self.base_namespace, schema_namespace))
        
        ref = 'global::'+self.get_qualified_name(namespace, enum_name)
        if ref in self.generated_types:
            return ref
        
        # Determine underlying type
        base_type = structure_schema.get('type', 'string')
        
        # For string enums, we don't specify an underlying type
        # For numeric enums, we map the type
        numeric_types = ['int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64', 'uint64']
        is_numeric = base_type in numeric_types
        
        enum_definition = ''
        doc = structure_schema.get('description', structure_schema.get('doc', enum_name))
        enum_definition += f"/// <summary>\n/// {doc}\n/// </summary>\n"
        
        if is_numeric:
            cs_base_type = self.map_primitive_to_csharp(base_type)
            enum_definition += f"public enum {enum_name} : {cs_base_type}\n{{\n"
        else:
            # String enum - for System.Text.Json, use JsonConverter with JsonStringEnumConverter
            if self.system_text_json_annotation:
                enum_definition += f"[System.Text.Json.Serialization.JsonConverter(typeof(System.Text.Json.Serialization.JsonStringEnumConverter))]\n"
            enum_definition += f"public enum {enum_name}\n{{\n"
        
        # Generate enum members
        for i, value in enumerate(enum_values):
            if is_numeric:
                # Numeric enum - use the value directly
                member_name = f"Value{value}"  # Prefix with "Value" since enum members can't start with numbers
                enum_definition += f"{INDENT}{member_name} = {value}"
            else:
                # String enum - create member from the string
                member_name = pascal(str(value).replace('-', '_').replace(' ', '_'))
                enum_definition += f"{INDENT}{member_name}"
            
            if i < len(enum_values) - 1:
                enum_definition += ",\n"
            else:
                enum_definition += "\n"
        
        enum_definition += "}"
        
        if write_file:
            self.write_to_file(namespace, enum_name, enum_definition)
        
        self.generated_types[ref] = "enum"
        self.generated_structure_types[ref] = structure_schema
        return ref

    def generate_choice(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """ Generates a discriminated union (choice) type """
        # Choice types in JSON Structure can be:
        # 1. Tagged unions - single property with the choice type as key
        # 2. Inline unions - with $extends and selector
        
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedChoice'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = pascal(self.concat_namespace(self.base_namespace, schema_namespace))
        
        ref = 'global::'+self.get_qualified_name(namespace, class_name)
        if ref in self.generated_types:
            return ref
        
        choices = structure_schema.get('choices', {})
        selector = structure_schema.get('selector')
        extends = structure_schema.get('$extends')
        
        if extends and selector:
            # Inline union - generate as inheritance hierarchy
            return self.generate_inline_union(structure_schema, parent_namespace, write_file, explicit_name)
        else:
            # Tagged union - generate as a union class similar to Avro
            return self.generate_tagged_union(structure_schema, parent_namespace, write_file, explicit_name)

    def generate_tagged_union(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """ Generates a tagged union type """
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedChoice'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = pascal(self.concat_namespace(self.base_namespace, schema_namespace))
        
        ref = 'global::'+self.get_qualified_name(namespace, class_name)
        if ref in self.generated_types:
            return ref
        
        choices = structure_schema.get('choices', {})
        choice_types = []
        
        for choice_name, choice_schema in choices.items():
            choice_type = self.convert_structure_type_to_csharp(class_name, choice_name, choice_schema, schema_namespace)
            choice_types.append((choice_name, choice_type))
        
        # Generate the union class similar to Avro unions
        class_definition = f"/// <summary>\n/// {structure_schema.get('description', class_name)}\n/// </summary>\n"
        class_definition += f"public partial class {class_name}\n{{\n"
        
        # Generate properties for each choice
        for choice_name, choice_type in choice_types:
            prop_name = pascal(choice_name)
            class_definition += f"{INDENT}/// <summary>\n{INDENT}/// Gets or sets the {prop_name} value\n{INDENT}/// </summary>\n"
            class_definition += f"{INDENT}public {choice_type}? {prop_name} {{ get; set; }} = null;\n"
        
        # Add constructor
        class_definition += f"\n{INDENT}/// <summary>\n{INDENT}/// Default constructor\n{INDENT}/// </summary>\n"
        class_definition += f"{INDENT}public {class_name}()\n{INDENT}{{\n{INDENT}}}\n"
        
        # Add constructors for each choice
        for choice_name, choice_type in choice_types:
            prop_name = pascal(choice_name)
            class_definition += f"\n{INDENT}/// <summary>\n{INDENT}/// Constructor for {prop_name} values\n{INDENT}/// </summary>\n"
            class_definition += f"{INDENT}public {class_name}({choice_type} {prop_name.lower()})\n{INDENT}{{\n"
            class_definition += f"{INDENT*2}this.{prop_name} = {prop_name.lower()};\n{INDENT}}}\n"
        
        class_definition += "}"
        
        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
        
        self.generated_types[ref] = "choice"
        self.generated_structure_types[ref] = structure_schema
        return ref

    def generate_inline_union(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """ Generates an inline union type with inheritance """
        # For inline unions, we generate an abstract base class and derived classes
        # The selector property indicates which derived class is being used
        
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedChoice'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = pascal(self.concat_namespace(self.base_namespace, schema_namespace))
        
        ref = 'global::'+self.get_qualified_name(namespace, class_name)
        if ref in self.generated_types:
            return ref
        
        # Generate base class from $extends
        extends_ref = structure_schema.get('$extends', '')
        base_schema = self.resolve_ref(extends_ref, self.schema_doc) if extends_ref else None
        
        choices = structure_schema.get('choices', {})
        selector = structure_schema.get('selector', 'type')
        
        # For now, generate as a regular class with a discriminator property
        # A more complete implementation would generate the full inheritance hierarchy
        class_definition = f"/// <summary>\n/// {structure_schema.get('description', class_name)}\n/// </summary>\n"
        
        if self.system_text_json_annotation:
            class_definition += f'[System.Text.Json.Serialization.JsonPolymorphic(TypeDiscriminatorPropertyName = "{selector}")]\n'
            for choice_name in choices.keys():
                class_definition += f'[System.Text.Json.Serialization.JsonDerivedType(typeof({pascal(choice_name)}), "{choice_name}")]\n'
        
        class_definition += f"public abstract partial class {class_name}\n{{\n"
        
        # Add selector property
        class_definition += f"{INDENT}/// <summary>\n{INDENT}/// Type discriminator\n{INDENT}/// </summary>\n"
        class_definition += f'{INDENT}[System.Text.Json.Serialization.JsonPropertyName("{selector}")]\n'
        class_definition += f"{INDENT}public string {pascal(selector)} {{ get; set; }} = \"\";\n"
        
        class_definition += "}"
        
        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
        
        # Generate derived classes for each choice
        for choice_name, choice_schema in choices.items():
            self.generate_class(choice_schema, schema_namespace, write_file, explicit_name=choice_name)
        
        self.generated_types[ref] = "choice"
        self.generated_structure_types[ref] = structure_schema
        return ref

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """ Generates a tuple type """
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedTuple'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = pascal(self.concat_namespace(self.base_namespace, schema_namespace))
        
        ref = 'global::'+self.get_qualified_name(namespace, class_name)
        if ref in self.generated_types:
            return ref
        
        properties = structure_schema.get('properties', {})
        tuple_order = structure_schema.get('tuple', [])
        
        # Generate as a class with a specific tuple order
        class_definition = f"/// <summary>\n/// {structure_schema.get('description', class_name)}\n/// </summary>\n"
        class_definition += f"public partial class {class_name}\n{{\n"
        
        # Generate properties in tuple order
        for prop_name in tuple_order:
            if prop_name in properties:
                prop_schema = properties[prop_name]
                field_def = self.generate_property(prop_name, prop_schema, class_name, schema_namespace, tuple_order)
                class_definition += field_def
        
        # Add default constructor
        class_definition += f"\n{INDENT}/// <summary>\n{INDENT}/// Default constructor\n{INDENT}/// </summary>\n"
        class_definition += f"{INDENT}public {class_name}()\n{INDENT}{{\n{INDENT}}}\n"
        
        class_definition += "}"
        
        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
        
        self.generated_types[ref] = "tuple"
        self.generated_structure_types[ref] = structure_schema
        return ref

    def generate_embedded_union(self, class_name: str, field_name: str, structure_types: List, parent_namespace: str, write_file: bool) -> str:
        """ Generates an embedded Union Class """
        # Similar to Avro's union handling, but for JSON Structure types
        union_class_name = pascal(field_name)+'Union'
        ref = class_name+'.'+union_class_name
        
        # For simplicity, generate as object type
        # A complete implementation would generate a proper union class
        return 'object'

    def generate_equals_and_gethashcode(self, structure_schema: Dict, class_name: str, parent_namespace: str) -> str:
        """ Generates Equals and GetHashCode methods for value equality """
        code = "\n"
        properties = structure_schema.get('properties', {})
        
        # Filter out const properties since they're static and same for all instances
        non_const_properties = {k: v for k, v in properties.items() if 'const' not in v}
        
        if not non_const_properties:
            # Empty class or only const fields - simple implementation
            code += f"{INDENT}/// <summary>\n{INDENT}/// Determines whether the specified object is equal to the current object.\n{INDENT}/// </summary>\n"
            code += f"{INDENT}public override bool Equals(object? obj)\n{INDENT}{{\n"
            code += f"{INDENT*2}return obj is {class_name};\n"
            code += f"{INDENT}}}\n\n"
            code += f"{INDENT}/// <summary>\n{INDENT}/// Serves as the default hash function.\n{INDENT}/// </summary>\n"
            code += f"{INDENT}public override int GetHashCode()\n{INDENT}{{\n"
            code += f"{INDENT*2}return 0;\n"
            code += f"{INDENT}}}\n"
            return code
        
        # Generate Equals method
        code += f"{INDENT}/// <summary>\n{INDENT}/// Determines whether the specified object is equal to the current object.\n{INDENT}/// </summary>\n"
        code += f"{INDENT}public override bool Equals(object? obj)\n{INDENT}{{\n"
        code += f"{INDENT*2}if (obj is not {class_name} other) return false;\n"
        
        # Build equality comparisons for each non-const property
        equality_checks = []
        for prop_name, prop_schema in non_const_properties.items():
            field_name = prop_name
            if self.is_csharp_reserved_word(field_name):
                field_name = f"@{field_name}"
            if self.pascal_properties:
                field_name = pascal(field_name)
            if field_name == class_name:
                field_name += "_"
            
            field_type = self.convert_structure_type_to_csharp(class_name, field_name, prop_schema, parent_namespace)
            
            # Handle different types of comparisons
            if field_type == 'byte[]' or field_type == 'byte[]?':
                # Byte arrays need special handling
                equality_checks.append(f"System.Linq.Enumerable.SequenceEqual({field_name} ?? Array.Empty<byte>(), other.{field_name} ?? Array.Empty<byte>())")
            elif field_type.startswith('List<') or field_type.startswith('HashSet<') or field_type.startswith('Dictionary<'):
                # Collections need sequence comparison
                if field_type.endswith('?'):
                    equality_checks.append(f"(({field_name} == null && other.{field_name} == null) || ({field_name} != null && other.{field_name} != null && {field_name}.SequenceEqual(other.{field_name})))")
                else:
                    equality_checks.append(f"{field_name}.SequenceEqual(other.{field_name})")
            else:
                # Use Equals for reference types, == for value types
                if field_type.endswith('?') or not self.is_csharp_primitive_type(field_type):
                    equality_checks.append(f"Equals({field_name}, other.{field_name})")
                else:
                    equality_checks.append(f"{field_name} == other.{field_name}")
        
        # Join all checks with &&
        if len(equality_checks) == 1:
            code += f"{INDENT*2}return {equality_checks[0]};\n"
        else:
            code += f"{INDENT*2}return " + f"\n{INDENT*3}&& ".join(equality_checks) + ";\n"
        
        code += f"{INDENT}}}\n\n"
        
        # Generate GetHashCode method
        code += f"{INDENT}/// <summary>\n{INDENT}/// Serves as the default hash function.\n{INDENT}/// </summary>\n"
        code += f"{INDENT}public override int GetHashCode()\n{INDENT}{{\n"
        
        # Collect field names for HashCode.Combine (skip const fields)
        hash_fields = []
        for prop_name, prop_schema in non_const_properties.items():
            field_name = prop_name
            if self.is_csharp_reserved_word(field_name):
                field_name = f"@{field_name}"
            if self.pascal_properties:
                field_name = pascal(field_name)
            if field_name == class_name:
                field_name += "_"
            
            field_type = self.convert_structure_type_to_csharp(class_name, field_name, prop_schema, parent_namespace)
            
            # Handle special types that need custom hash code computation
            if field_type == 'byte[]' or field_type == 'byte[]?':
                hash_fields.append(f"({field_name} != null ? System.Convert.ToBase64String({field_name}).GetHashCode() : 0)")
            elif field_type.startswith('List<') or field_type.startswith('HashSet<') or field_type.startswith('Dictionary<'):
                # For collections, compute hash from elements
                if field_type.endswith('?'):
                    hash_fields.append(f"({field_name} != null ? {field_name}.Aggregate(0, (acc, item) => HashCode.Combine(acc, item)) : 0)")
                else:
                    hash_fields.append(f"{field_name}.Aggregate(0, (acc, item) => HashCode.Combine(acc, item))")
            else:
                hash_fields.append(field_name)
        
        # HashCode.Combine supports up to 8 parameters
        if len(hash_fields) <= 8:
            code += f"{INDENT*2}return HashCode.Combine({', '.join(hash_fields)});\n"
        else:
            # For more than 8 fields, use HashCode.Add
            code += f"{INDENT*2}var hash = new HashCode();\n"
            for field in hash_fields:
                code += f"{INDENT*2}hash.Add({field});\n"
            code += f"{INDENT*2}return hash.ToHashCode();\n"
        
        code += f"{INDENT}}}\n"
        
        return code

    def write_to_file(self, namespace: str, class_name: str, class_definition: str) -> None:
        """ Writes the class definition to a file """
        os.makedirs(os.path.join(self.output_dir, namespace.replace('.', os.sep)), exist_ok=True)
        file_path = os.path.join(self.output_dir, namespace.replace('.', os.sep), f"{class_name}.cs")
        
        with open(file_path, 'w', encoding='utf-8') as file:
            # Write using statements
            file.write("using System;\n")
            file.write("using System.Collections.Generic;\n")
            file.write("using System.Linq;\n")
            
            if self.system_text_json_annotation:
                file.write("using System.Text.Json;\n")
                file.write("using System.Text.Json.Serialization;\n")
            
            if self.newtonsoft_json_annotation:
                file.write("using Newtonsoft.Json;\n")
            
            if self.system_xml_annotation:
                file.write("using System.Xml.Serialization;\n")
            
            file.write("\n")
            file.write(f"namespace {namespace}\n{{\n")
            file.write(class_definition)
            file.write("\n}\n")

    def convert(self, structure_schema_path: str, output_dir: str) -> None:
        """ Converts a JSON Structure schema file to C# classes """
        self.output_dir = output_dir
        
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        
        self.convert_schema(schema, output_dir)

    def convert_schema(self, schema: Dict, output_dir: str) -> None:
        """ Converts a JSON Structure schema to C# classes """
        self.output_dir = output_dir
        self.schema_doc = schema
        
        # Process definitions
        if 'definitions' in schema:
            self.definitions = schema['definitions']
            self.process_definitions(self.definitions, '')
        
        # Process root type
        if 'type' in schema:
            self.generate_class_or_choice(schema, '', write_file=True)
        elif '$root' in schema:
            root_ref = schema['$root']
            root_schema = self.resolve_ref(root_ref, schema)
            if root_schema:
                ref_path = root_ref.split('/')
                type_name = ref_path[-1]
                ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else ''
                self.generate_class_or_choice(root_schema, ref_namespace, write_file=True, explicit_name=type_name)
        
        # Generate project file
        self.generate_project_file()

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """ Processes the definitions section recursively """
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition:
                    # This is a type definition
                    current_namespace = self.concat_namespace(namespace_path, '')
                    self.generate_class_or_choice(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    # This is a namespace
                    new_namespace = self.concat_namespace(namespace_path, name)
                    self.process_definitions(definition, new_namespace)

    def generate_project_file(self) -> None:
        """ Generates a .csproj file for the generated classes """
        # Determine project name
        project_name = self.project_name if self.project_name else pascal(self.base_namespace) if self.base_namespace else "StructureTypes"
        
        csproj_content = f"""<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>

</Project>
"""
        
        csproj_path = os.path.join(self.output_dir, f"{project_name}.csproj")
        with open(csproj_path, 'w', encoding='utf-8') as file:
            file.write(csproj_content)


def convert_structure_to_csharp(
    structure_schema_path: str, 
    cs_file_path: str, 
    base_namespace: str = '',
    project_name: str = '',
    pascal_properties: bool = False, 
    system_text_json_annotation: bool = False, 
    newtonsoft_json_annotation: bool = False, 
    system_xml_annotation: bool = False
):
    """Converts JSON Structure schema to C# classes

    Args:
        structure_schema_path (str): JSON Structure input schema path
        cs_file_path (str): Output C# file path
        base_namespace (str, optional): Base namespace. Defaults to ''.
        project_name (str, optional): Explicit project name for .csproj files (separate from namespace). Defaults to ''.
        pascal_properties (bool, optional): Pascal case properties. Defaults to False.
        system_text_json_annotation (bool, optional): Use System.Text.Json annotations. Defaults to False.
        newtonsoft_json_annotation (bool, optional): Use Newtonsoft.Json annotations. Defaults to False.
        system_xml_annotation (bool, optional): Use System.Xml.Serialization annotations. Defaults to False.
    """

    if not base_namespace:
        base_namespace = os.path.splitext(os.path.basename(cs_file_path))[0].replace('-', '_')
    
    structtocs = StructureToCSharp(base_namespace)
    structtocs.project_name = project_name
    structtocs.pascal_properties = pascal_properties
    structtocs.system_text_json_annotation = system_text_json_annotation
    structtocs.newtonsoft_json_annotation = newtonsoft_json_annotation
    structtocs.system_xml_annotation = system_xml_annotation
    structtocs.convert(structure_schema_path, cs_file_path)


def convert_structure_schema_to_csharp(
    structure_schema: JsonNode, 
    output_dir: str, 
    base_namespace: str = '',
    project_name: str = '',
    pascal_properties: bool = False, 
    system_text_json_annotation: bool = False, 
    newtonsoft_json_annotation: bool = False, 
    system_xml_annotation: bool = False
):
    """Converts JSON Structure schema to C# classes

    Args:
        structure_schema (JsonNode): JSON Structure schema to convert
        output_dir (str): Output directory
        base_namespace (str, optional): Base namespace for the generated classes. Defaults to ''.
        project_name (str, optional): Explicit project name for .csproj files (separate from namespace). Defaults to ''.
        pascal_properties (bool, optional): Pascal case properties. Defaults to False.
        system_text_json_annotation (bool, optional): Use System.Text.Json annotations. Defaults to False.
        newtonsoft_json_annotation (bool, optional): Use Newtonsoft.Json annotations. Defaults to False.
        system_xml_annotation (bool, optional): Use System.Xml.Serialization annotations. Defaults to False.
    """
    structtocs = StructureToCSharp(base_namespace)
    structtocs.project_name = project_name
    structtocs.pascal_properties = pascal_properties
    structtocs.system_text_json_annotation = system_text_json_annotation
    structtocs.newtonsoft_json_annotation = newtonsoft_json_annotation
    structtocs.system_xml_annotation = system_xml_annotation
    structtocs.convert_schema(structure_schema, output_dir)
