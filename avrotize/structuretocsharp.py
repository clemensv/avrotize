# pylint: disable=line-too-long

""" StructureToCSharp class for converting JSON Structure schema to C# classes """

import json
import os
import re
from typing import Any, Dict, List, Tuple, Union, cast, Optional
import uuid

from avrotize.common import pascal, process_template
from avrotize.jstructtoavro import JsonStructureToAvro
from avrotize.constants import (
    NEWTONSOFT_JSON_VERSION,
    SYSTEM_TEXT_JSON_VERSION,
    SYSTEM_MEMORY_DATA_VERSION,
    CSHARP_AVRO_VERSION,
    NUNIT_VERSION,
    NUNIT_ADAPTER_VERSION,
    MSTEST_SDK_VERSION,
)
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
        self.avro_annotation = False
        self.generated_types: Dict[str,str] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}
        self.type_dict: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}
        self.schema_registry: Dict[str, Dict] = {}  # Maps $id URIs to schemas
        self.offers: Dict[str, Any] = {}  # Maps add-in names to property definitions from $offers
        self.needs_json_structure_converters = False  # Track if any types need JSON Structure converters
        self.discriminator_properties: Dict[str, str] = {}  # Maps type ref -> discriminator property name (for inline unions)

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
            'integer': 'int',  # Generic integer type without format
            'number': 'double',  # Generic number type without format
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

    def get_json_structure_converter(self, schema_type: str, is_required: bool) -> str | None:
        """ Returns the appropriate JSON Structure converter type for types requiring string serialization.
        
        Per JSON Structure Core spec, int64, uint64, int128, uint128, and decimal types
        use string representation in JSON to preserve precision. Duration (TimeSpan)
        uses ISO 8601 format.
        
        Args:
            schema_type: The JSON Structure type name
            is_required: Whether the property is required (affects nullable converter selection)
            
        Returns:
            The converter class name if needed, or None if no special converter is required
        """
        # Map JSON Structure types to their converter class names
        converter_map = {
            'int64': ('Int64StringConverter', 'NullableInt64StringConverter'),
            'uint64': ('UInt64StringConverter', 'NullableUInt64StringConverter'),
            'int128': ('Int128StringConverter', 'NullableInt128StringConverter'),
            'uint128': ('UInt128StringConverter', 'NullableUInt128StringConverter'),
            'decimal': ('DecimalStringConverter', 'NullableDecimalStringConverter'),
            'duration': ('TimeSpanIso8601Converter', 'NullableTimeSpanIso8601Converter'),
        }
        
        if schema_type in converter_map:
            required_converter, nullable_converter = converter_map[schema_type]
            return required_converter if is_required else nullable_converter
        
        return None

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

    def safe_identifier(self, name: str, class_name: str = '', fallback_prefix: str = 'field') -> str:
        """Converts a name to a safe C# identifier.
        
        Handles:
        - Reserved words (prepend @)
        - Numeric prefixes (prepend _)
        - Special characters (replace with _)
        - All-special-char names (use fallback_prefix)
        - Class name collision (append _)
        """
        import re
        # Replace invalid characters with underscores
        safe = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
        # Remove leading/trailing underscores from sanitization
        safe = safe.strip('_') if safe != name else safe
        # If nothing left after removing special chars, use fallback
        if not safe or not re.match(r'^[a-zA-Z_@]', safe):
            if safe and re.match(r'^[0-9]', safe):
                safe = '_' + safe  # Numeric prefix
            else:
                safe = fallback_prefix + '_' + (safe if safe else 'unnamed')
        # Handle reserved words with @ prefix
        if self.is_csharp_reserved_word(safe):
            safe = '@' + safe
        # Handle class name collision
        if class_name and safe == class_name:
            safe = safe + '_'
        return safe

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
    
    def validate_abstract_ref(self, ref_schema: Dict, ref: str, is_extends_context: bool = False) -> None:
        """
        Validates that abstract types are only referenced in $extends context.
        Per JSON Structure Core Spec Section 3.10.1, abstract types cannot be 
        directly instantiated and should only be referenced via $extends.
        
        Args:
            ref_schema: The resolved schema being referenced
            ref: The $ref string for error reporting
            is_extends_context: True if this reference is in a $extends context
        """
        import sys
        is_abstract = ref_schema.get('abstract', False)
        if is_abstract and not is_extends_context:
            print(f"WARNING: Abstract type referenced outside $extends context: {ref}", file=sys.stderr)
            print(f"  Abstract types cannot be directly instantiated. Use $extends to inherit from them.", file=sys.stderr)
    
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
                    # Validate abstract type usage (Section 3.10.1)
                    # Abstract types should only be referenced via $extends
                    self.validate_abstract_ref(ref_schema, structure_type['$ref'], is_extends_context=False)
                    
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
        elif struct_type in ('map', 'array', 'set'):
            # Root-level container types: generate wrapper class with implicit conversions
            return self.generate_container_wrapper(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
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

        # Check if this is an abstract type (Section 3.10.1)
        is_abstract = structure_schema.get('abstract', False)
        
        # Generate class documentation
        doc = structure_schema.get('description', structure_schema.get('doc', class_name))
        class_definition += f"/// <summary>\n/// {doc}\n/// </summary>\n"
        
        if is_abstract:
            class_definition += f"/// <remarks>\n/// This is an abstract type and cannot be instantiated directly.\n/// </remarks>\n"

        # Add Obsolete attribute if deprecated
        if structure_schema.get('deprecated', False):
            deprecated_msg = structure_schema.get('description', f'{class_name} is deprecated')
            class_definition += f"[System.Obsolete(\"{deprecated_msg}\")]\n"

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
        
        # Check additionalProperties setting (Section 3.7.8)
        additional_props = structure_schema.get('additionalProperties', True if is_abstract else None)
        
        # Check if any property in this class is a discriminator for an inline union
        discriminator_prop = self.discriminator_properties.get(ref, None)
        
        fields_str = []
        for prop_name, prop_schema in properties.items():
            is_discriminator = (prop_name == discriminator_prop)
            field_def = self.generate_property(prop_name, prop_schema, class_name, schema_namespace, required_props, is_discriminator=is_discriminator)
            fields_str.append(field_def)
        
        # Add dictionary for additional properties if needed
        if additional_props is not False and additional_props is not None:
            fields_str.append(f"{INDENT}/// <summary>\n{INDENT}/// Additional properties not defined in schema\n{INDENT}/// </summary>\n")
            # Use JsonExtensionData for automatic capture of unknown properties during deserialization
            fields_str.append(f"{INDENT}[System.Text.Json.Serialization.JsonExtensionData]\n")
            if isinstance(additional_props, dict):
                # additionalProperties is a schema - use the typed value
                value_type = self.convert_structure_type_to_csharp(class_name, 'additionalValue', additional_props, schema_namespace)
                fields_str.append(f"{INDENT}public Dictionary<string, {value_type}>? AdditionalProperties {{ get; set; }}\n")
            else:
                # additionalProperties: true - allow any additional properties with boxed values
                fields_str.append(f"{INDENT}public Dictionary<string, object>? AdditionalProperties {{ get; set; }}\n")
        
        class_body = "\n".join(fields_str)
        
        # Generate class declaration
        abstract_modifier = "abstract " if is_abstract else ""
        sealed_modifier = "sealed " if additional_props is False and not is_abstract else ""
        
        class_definition += f"public {abstract_modifier}{sealed_modifier}partial class {class_name}\n{{\n{class_body}"
        
        # Add default constructor (not for abstract classes with no concrete constructors)
        if not is_abstract or properties:
            class_definition += f"\n{INDENT}/// <summary>\n{INDENT}/// Default constructor\n{INDENT}/// </summary>\n"
            constructor_modifier = "protected" if is_abstract else "public"
            class_definition += f"{INDENT}{constructor_modifier} {class_name}()\n{INDENT}{{\n{INDENT}}}"

        # Convert JSON Structure schema to Avro schema if avro_annotation is enabled
        avro_schema_json = ''
        if self.avro_annotation:
            # Use JsonStructureToAvro to convert the schema
            converter = JsonStructureToAvro()
            schema_copy = structure_schema.copy()
            avro_schema = converter.convert(schema_copy)
            # Escape the JSON for C# string literal
            # json.dumps produces compact JSON that only needs backslash and quote escaping
            avro_schema_json = json.dumps(avro_schema, separators=(',', ':')).replace('\\', '\\\\').replace('"', '\\"')
            # Also enable system_text_json_annotation internally for Avro serialization helpers
            # since ToAvroRecord and FromAvroRecord use System.Text.Json
            needs_json_for_avro = not self.system_text_json_annotation and not self.newtonsoft_json_annotation

        # Add helper methods from template if any annotations are enabled
        if self.system_text_json_annotation or self.newtonsoft_json_annotation or self.system_xml_annotation or self.avro_annotation:
            class_definition += process_template(
                "structuretocsharp/dataclass_core.jinja",
                class_name=class_name,
                system_text_json_annotation=self.system_text_json_annotation or (self.avro_annotation and needs_json_for_avro),
                newtonsoft_json_annotation=self.newtonsoft_json_annotation,
                system_xml_annotation=self.system_xml_annotation,
                avro_annotation=self.avro_annotation,
                avro_schema_json=avro_schema_json
            )

        # Generate Equals and GetHashCode
        class_definition += self.generate_equals_and_gethashcode(structure_schema, class_name, schema_namespace)

        class_definition += "\n"+"}"

        if write_file:
            self.write_to_file(namespace, class_name, class_definition)

        self.generated_types[ref] = "class"
        self.generated_structure_types[ref] = structure_schema
        return ref

    def generate_property(self, prop_name: str, prop_schema: Dict, class_name: str, parent_namespace: str, required_props: List, is_discriminator: bool = False) -> str:
        """ Generates a property for a class """
        property_definition = ''
        
        # Resolve property name using safe_identifier for special chars, numeric prefixes, etc.
        field_name = self.safe_identifier(prop_name, class_name)
        if self.pascal_properties:
            field_name_cs = pascal(field_name.lstrip('@'))
            # Re-check for class name collision after pascal casing
            if field_name_cs == class_name:
                field_name_cs += "_"
        else:
            field_name_cs = field_name
        
        # Track if field name differs from original for JSON annotation
        needs_json_annotation = field_name_cs != prop_name
        
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
            
            # Add JSON property name annotation when property name differs from schema name
            # This is needed for proper JSON serialization/deserialization, especially with pascal_properties
            if needs_json_annotation:
                property_definition += f'{INDENT}[System.Text.Json.Serialization.JsonPropertyName("{prop_name}")]\n'
            if self.newtonsoft_json_annotation and needs_json_annotation:
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
        
        # If this property is used as a discriminator in an inline union, add JsonIgnore
        # because JsonPolymorphic handles it as metadata
        if is_discriminator and self.system_text_json_annotation:
            property_definition += f'{INDENT}[System.Text.Json.Serialization.JsonIgnore]\n'
        # Add JSON property name annotation when property name differs from schema name
        # This is needed for proper JSON serialization/deserialization, especially with pascal_properties
        elif needs_json_annotation:
            property_definition += f'{INDENT}[System.Text.Json.Serialization.JsonPropertyName("{prop_name}")]\n'
        if self.newtonsoft_json_annotation and needs_json_annotation:
            property_definition += f'{INDENT}[Newtonsoft.Json.JsonProperty("{prop_name}")]\n'
        
        # Add XML element annotation if enabled
        if self.system_xml_annotation:
            property_definition += f'{INDENT}[System.Xml.Serialization.XmlElement("{prop_name}")]\n'
        
        # Add JSON Structure converters for types requiring string serialization
        if self.system_text_json_annotation:
            schema_type = prop_schema.get('type', '')
            converter_type = self.get_json_structure_converter(schema_type, is_required)
            if converter_type:
                property_definition += f'{INDENT}[System.Text.Json.Serialization.JsonConverter(typeof({converter_type}))]\n'
                self.needs_json_structure_converters = True
        
        # Add validation attributes based on schema constraints
        # Get the property type to determine which attributes to apply
        prop_type_base = prop_schema.get('type', '')
        if isinstance(prop_type_base, list):
            # Handle type unions - use the first non-null type
            non_null_types = [t for t in prop_type_base if t != 'null']
            prop_type_base = non_null_types[0] if non_null_types else ''
        
        # EmailAddress attribute for format: "email"
        if prop_schema.get('format') == 'email':
            property_definition += f'{INDENT}[System.ComponentModel.DataAnnotations.EmailAddress]\n'
        
        # String length constraints (for string types)
        if prop_type_base == 'string' or (not prop_type_base and ('minLength' in prop_schema or 'maxLength' in prop_schema)):
            if 'maxLength' in prop_schema:
                max_length = prop_schema['maxLength']
                if 'minLength' in prop_schema:
                    min_length = prop_schema['minLength']
                    property_definition += f'{INDENT}[System.ComponentModel.DataAnnotations.StringLength({max_length}, MinimumLength = {min_length})]\n'
                else:
                    property_definition += f'{INDENT}[System.ComponentModel.DataAnnotations.StringLength({max_length})]\n'
            elif 'minLength' in prop_schema:
                # MinLength only (no max)
                min_length = prop_schema['minLength']
                property_definition += f'{INDENT}[System.ComponentModel.DataAnnotations.MinLength({min_length})]\n'
        
        # Array length constraints (for array types)
        if prop_type_base == 'array':
            if 'minItems' in prop_schema:
                min_items = prop_schema['minItems']
                property_definition += f'{INDENT}[System.ComponentModel.DataAnnotations.MinLength({min_items})]\n'
            if 'maxItems' in prop_schema:
                max_items = prop_schema['maxItems']
                property_definition += f'{INDENT}[System.ComponentModel.DataAnnotations.MaxLength({max_items})]\n'
        
        # RegularExpression attribute for pattern
        if 'pattern' in prop_schema:
            pattern = prop_schema['pattern'].replace('\\', '\\\\').replace('"', '\\"')
            property_definition += f'{INDENT}[System.ComponentModel.DataAnnotations.RegularExpression(@"{pattern}")]\n'
        
        # Range attribute for minimum/maximum on numeric types
        if 'minimum' in prop_schema or 'maximum' in prop_schema or 'exclusiveMinimum' in prop_schema or 'exclusiveMaximum' in prop_schema:
            # Determine the minimum and maximum values
            has_min = 'minimum' in prop_schema
            has_max = 'maximum' in prop_schema
            has_exclusive_min = 'exclusiveMinimum' in prop_schema
            has_exclusive_max = 'exclusiveMaximum' in prop_schema
            
            # Use minimum/maximum if present, otherwise use exclusiveMinimum/exclusiveMaximum
            if has_min:
                min_val = prop_schema['minimum']
                min_is_exclusive = False
            elif has_exclusive_min:
                min_val = prop_schema['exclusiveMinimum']
                min_is_exclusive = True
            else:
                min_val = 'double.MinValue'
                min_is_exclusive = False
            
            if has_max:
                max_val = prop_schema['maximum']
                max_is_exclusive = False
            elif has_exclusive_max:
                max_val = prop_schema['exclusiveMaximum']
                max_is_exclusive = True
            else:
                max_val = 'double.MaxValue'
                max_is_exclusive = False
            
            # Convert to appropriate format
            min_str = str(min_val)
            max_str = str(max_val)
            
            # Build the Range attribute with exclusive parameters if needed
            range_params = f'{min_str}, {max_str}'
            if min_is_exclusive or max_is_exclusive:
                extra_params = []
                if min_is_exclusive:
                    extra_params.append('MinimumIsExclusive = true')
                if max_is_exclusive:
                    extra_params.append('MaximumIsExclusive = true')
                range_params += ', ' + ', '.join(extra_params)
            
            property_definition += f'{INDENT}[System.ComponentModel.DataAnnotations.Range({range_params})]\n'
        
        # Add Obsolete attribute if deprecated
        if prop_schema.get('deprecated', False):
            deprecated_msg = prop_schema.get('description', f'{prop_name} is deprecated')
            property_definition += f'{INDENT}[System.Obsolete("{deprecated_msg}")]\n'
        
        # Generate property with required modifier if needed
        required_modifier = "required " if is_required and not prop_type.endswith('?') else ""
        
        # Handle readOnly and writeOnly
        is_read_only = prop_schema.get('readOnly', False)
        is_write_only = prop_schema.get('writeOnly', False)
        
        if is_read_only:
            # readOnly: private or init-only setter
            property_definition += f"{INDENT}public {required_modifier}{prop_type} {field_name_cs} {{ get; init; }}"
        elif is_write_only:
            # writeOnly: private getter
            property_definition += f"{INDENT}public {required_modifier}{prop_type} {field_name_cs} {{ private get; set; }}"
        else:
            # Normal property
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
        
        # Add Obsolete attribute if deprecated
        if structure_schema.get('deprecated', False):
            deprecated_msg = structure_schema.get('description', f'{enum_name} is deprecated')
            enum_definition += f"[System.Obsolete(\"{deprecated_msg}\")]\n"
        
        # Add converter attributes - always include System.Text.Json since it's the default .NET serializer
        # This ensures enums serialize correctly with proper value mapping even if system_text_json_annotation is False
        enum_definition += f"[System.Text.Json.Serialization.JsonConverter(typeof({enum_name}Converter))]\n"
        if self.newtonsoft_json_annotation:
            enum_definition += f"[Newtonsoft.Json.JsonConverter(typeof({enum_name}NewtonsoftConverter))]\n"
        
        if is_numeric:
            cs_base_type = self.map_primitive_to_csharp(base_type)
            enum_definition += f"public enum {enum_name} : {cs_base_type}\n{{\n"
        else:
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
        
        enum_definition += "}\n\n"
        
        # Always generate System.Text.Json converter since it's the default .NET serializer
        # This ensures enums serialize correctly even when system_text_json_annotation is False
        enum_definition += f"/// <summary>\n/// System.Text.Json converter for {enum_name} that maps to schema values\n/// </summary>\n"
        enum_definition += f"public class {enum_name}Converter : System.Text.Json.Serialization.JsonConverter<{enum_name}>\n{{\n"
        
        # Read method
        enum_definition += f"{INDENT}/// <inheritdoc/>\n"
        enum_definition += f"{INDENT}public override {enum_name} Read(ref System.Text.Json.Utf8JsonReader reader, Type typeToConvert, System.Text.Json.JsonSerializerOptions options)\n"
        enum_definition += f"{INDENT}{{\n"
        
        if is_numeric:
            enum_definition += f"{INDENT*2}if (reader.TokenType == System.Text.Json.JsonTokenType.Number)\n"
            enum_definition += f"{INDENT*2}{{\n"
            enum_definition += f"{INDENT*3}return ({enum_name})reader.GetInt32();\n"
            enum_definition += f"{INDENT*2}}}\n"
            enum_definition += f"{INDENT*2}throw new System.Text.Json.JsonException($\"Expected number for {enum_name}\");\n"
        else:
            enum_definition += f"{INDENT*2}var stringValue = reader.GetString();\n"
            enum_definition += f"{INDENT*2}return stringValue switch\n"
            enum_definition += f"{INDENT*2}{{\n"
            for value in enum_values:
                member_name = pascal(str(value).replace('-', '_').replace(' ', '_'))
                enum_definition += f'{INDENT*3}"{value}" => {enum_name}.{member_name},\n'
            enum_definition += f'{INDENT*3}_ => throw new System.Text.Json.JsonException($"Unknown value \'{{stringValue}}\' for {enum_name}")\n'
            enum_definition += f"{INDENT*2}}};\n"
        
        enum_definition += f"{INDENT}}}\n\n"
        
        # Write method
        enum_definition += f"{INDENT}/// <inheritdoc/>\n"
        enum_definition += f"{INDENT}public override void Write(System.Text.Json.Utf8JsonWriter writer, {enum_name} value, System.Text.Json.JsonSerializerOptions options)\n"
        enum_definition += f"{INDENT}{{\n"
        
        if is_numeric:
            enum_definition += f"{INDENT*2}writer.WriteNumberValue((int)value);\n"
        else:
            enum_definition += f"{INDENT*2}var stringValue = value switch\n"
            enum_definition += f"{INDENT*2}{{\n"
            for value in enum_values:
                member_name = pascal(str(value).replace('-', '_').replace(' ', '_'))
                enum_definition += f'{INDENT*3}{enum_name}.{member_name} => "{value}",\n'
            enum_definition += f'{INDENT*3}_ => throw new System.ArgumentOutOfRangeException(nameof(value))\n'
            enum_definition += f"{INDENT*2}}};\n"
            enum_definition += f"{INDENT*2}writer.WriteStringValue(stringValue);\n"
        
        enum_definition += f"{INDENT}}}\n"
        enum_definition += "}\n\n"
        
        # Generate Newtonsoft.Json converter when enabled
        if self.newtonsoft_json_annotation:
            enum_definition += f"/// <summary>\n/// Newtonsoft.Json converter for {enum_name} that maps to schema values\n/// </summary>\n"
            enum_definition += f"public class {enum_name}NewtonsoftConverter : Newtonsoft.Json.JsonConverter<{enum_name}>\n{{\n"
            
            # ReadJson method
            enum_definition += f"{INDENT}/// <inheritdoc/>\n"
            enum_definition += f"{INDENT}public override {enum_name} ReadJson(Newtonsoft.Json.JsonReader reader, Type objectType, {enum_name} existingValue, bool hasExistingValue, Newtonsoft.Json.JsonSerializer serializer)\n"
            enum_definition += f"{INDENT}{{\n"
            
            if is_numeric:
                enum_definition += f"{INDENT*2}if (reader.TokenType == Newtonsoft.Json.JsonToken.Integer)\n"
                enum_definition += f"{INDENT*2}{{\n"
                enum_definition += f"{INDENT*3}return ({enum_name})Convert.ToInt32(reader.Value);\n"
                enum_definition += f"{INDENT*2}}}\n"
                enum_definition += f"{INDENT*2}throw new Newtonsoft.Json.JsonException($\"Expected number for {enum_name}\");\n"
            else:
                enum_definition += f"{INDENT*2}var stringValue = reader.Value?.ToString();\n"
                enum_definition += f"{INDENT*2}return stringValue switch\n"
                enum_definition += f"{INDENT*2}{{\n"
                for value in enum_values:
                    member_name = pascal(str(value).replace('-', '_').replace(' ', '_'))
                    enum_definition += f'{INDENT*3}"{value}" => {enum_name}.{member_name},\n'
                enum_definition += f'{INDENT*3}_ => throw new Newtonsoft.Json.JsonException($"Unknown value \'{{stringValue}}\' for {enum_name}")\n'
                enum_definition += f"{INDENT*2}}};\n"
            
            enum_definition += f"{INDENT}}}\n\n"
            
            # WriteJson method
            enum_definition += f"{INDENT}/// <inheritdoc/>\n"
            enum_definition += f"{INDENT}public override void WriteJson(Newtonsoft.Json.JsonWriter writer, {enum_name} value, Newtonsoft.Json.JsonSerializer serializer)\n"
            enum_definition += f"{INDENT}{{\n"
            
            if is_numeric:
                enum_definition += f"{INDENT*2}writer.WriteValue((int)value);\n"
            else:
                enum_definition += f"{INDENT*2}var stringValue = value switch\n"
                enum_definition += f"{INDENT*2}{{\n"
                for value in enum_values:
                    member_name = pascal(str(value).replace('-', '_').replace(' ', '_'))
                    enum_definition += f'{INDENT*3}{enum_name}.{member_name} => "{value}",\n'
                enum_definition += f'{INDENT*3}_ => throw new System.ArgumentOutOfRangeException(nameof(value))\n'
                enum_definition += f"{INDENT*2}}};\n"
                enum_definition += f"{INDENT*2}writer.WriteValue(stringValue);\n"
            
            enum_definition += f"{INDENT}}}\n"
            enum_definition += "}\n"
        
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
        # Add JsonConverter attribute for proper tagged union serialization
        class_definition += f"[System.Text.Json.Serialization.JsonConverter(typeof({class_name}JsonConverter))]\n"
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
        
        # Generate Equals and GetHashCode for choice types
        class_definition += f"\n{INDENT}/// <summary>\n{INDENT}/// Determines whether the specified object is equal to the current object.\n{INDENT}/// </summary>\n"
        class_definition += f"{INDENT}public override bool Equals(object? obj)\n{INDENT}{{\n"
        class_definition += f"{INDENT*2}if (obj is not {class_name} other) return false;\n"
        
        # Compare each choice property
        equality_checks = []
        for choice_name, choice_type in choice_types:
            prop_name = pascal(choice_name)
            equality_checks.append(f"Equals(this.{prop_name}, other.{prop_name})")
        
        if len(equality_checks) == 1:
            class_definition += f"{INDENT*2}return {equality_checks[0]};\n"
        else:
            class_definition += f"{INDENT*2}return " + f"\n{INDENT*3}&& ".join(equality_checks) + ";\n"
        
        class_definition += f"{INDENT}}}\n\n"
        
        # Generate GetHashCode
        class_definition += f"{INDENT}/// <summary>\n{INDENT}/// Serves as the default hash function.\n{INDENT}/// </summary>\n"
        class_definition += f"{INDENT}public override int GetHashCode()\n{INDENT}{{\n"
        
        if len(choice_types) <= 8:
            hash_fields = [f"this.{pascal(choice_name)}" for choice_name, _ in choice_types]
            class_definition += f"{INDENT*2}return HashCode.Combine({', '.join(hash_fields)});\n"
        else:
            class_definition += f"{INDENT*2}var hash = new HashCode();\n"
            for choice_name, _ in choice_types:
                prop_name = pascal(choice_name)
                class_definition += f"{INDENT*2}hash.Add(this.{prop_name});\n"
            class_definition += f"{INDENT*2}return hash.ToHashCode();\n"
        
        class_definition += f"{INDENT}}}\n"
        
        class_definition += "}\n\n"
        
        # Generate JSON converter for tagged union serialization
        class_definition += f"/// <summary>\n/// JSON converter for {class_name} tagged union - serializes only the non-null choice\n/// </summary>\n"
        class_definition += f"public class {class_name}JsonConverter : System.Text.Json.Serialization.JsonConverter<{class_name}>\n{{\n"
        
        # Read method
        class_definition += f"{INDENT}/// <inheritdoc/>\n"
        class_definition += f"{INDENT}public override {class_name}? Read(ref System.Text.Json.Utf8JsonReader reader, Type typeToConvert, System.Text.Json.JsonSerializerOptions options)\n"
        class_definition += f"{INDENT}{{\n"
        class_definition += f"{INDENT*2}if (reader.TokenType == System.Text.Json.JsonTokenType.Null) return null;\n"
        class_definition += f"{INDENT*2}if (reader.TokenType != System.Text.Json.JsonTokenType.StartObject)\n"
        class_definition += f"{INDENT*3}throw new System.Text.Json.JsonException(\"Expected object for tagged union\");\n"
        class_definition += f"{INDENT*2}var result = new {class_name}();\n"
        class_definition += f"{INDENT*2}while (reader.Read())\n"
        class_definition += f"{INDENT*2}{{\n"
        class_definition += f"{INDENT*3}if (reader.TokenType == System.Text.Json.JsonTokenType.EndObject) break;\n"
        class_definition += f"{INDENT*3}if (reader.TokenType != System.Text.Json.JsonTokenType.PropertyName)\n"
        class_definition += f"{INDENT*4}throw new System.Text.Json.JsonException(\"Expected property name\");\n"
        class_definition += f"{INDENT*3}var propName = reader.GetString();\n"
        class_definition += f"{INDENT*3}reader.Read();\n"
        class_definition += f"{INDENT*3}switch (propName)\n"
        class_definition += f"{INDENT*3}{{\n"
        for choice_name, choice_type in choice_types:
            # Use original schema property name for matching
            class_definition += f'{INDENT*4}case "{choice_name}":\n'
            class_definition += f"{INDENT*5}result.{pascal(choice_name)} = System.Text.Json.JsonSerializer.Deserialize<{choice_type}>(ref reader, options);\n"
            class_definition += f"{INDENT*5}break;\n"
        class_definition += f"{INDENT*4}default:\n"
        class_definition += f"{INDENT*5}reader.Skip();\n"
        class_definition += f"{INDENT*5}break;\n"
        class_definition += f"{INDENT*3}}}\n"
        class_definition += f"{INDENT*2}}}\n"
        class_definition += f"{INDENT*2}return result;\n"
        class_definition += f"{INDENT}}}\n\n"
        
        # Write method - only write the non-null choice
        class_definition += f"{INDENT}/// <inheritdoc/>\n"
        class_definition += f"{INDENT}public override void Write(System.Text.Json.Utf8JsonWriter writer, {class_name} value, System.Text.Json.JsonSerializerOptions options)\n"
        class_definition += f"{INDENT}{{\n"
        class_definition += f"{INDENT*2}writer.WriteStartObject();\n"
        for i, (choice_name, choice_type) in enumerate(choice_types):
            prop_name = pascal(choice_name)
            condition = "if" if i == 0 else "else if"
            class_definition += f"{INDENT*2}{condition} (value.{prop_name} != null)\n"
            class_definition += f"{INDENT*2}{{\n"
            class_definition += f'{INDENT*3}writer.WritePropertyName("{choice_name}");\n'
            class_definition += f"{INDENT*3}System.Text.Json.JsonSerializer.Serialize(writer, value.{prop_name}, options);\n"
            class_definition += f"{INDENT*2}}}\n"
        class_definition += f"{INDENT*2}writer.WriteEndObject();\n"
        class_definition += f"{INDENT}}}\n"
        class_definition += "}\n"
        
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
        
        # Get base class from $extends
        extends_ref = structure_schema.get('$extends', '')
        if extends_ref and isinstance(extends_ref, str):
            base_schema = self.resolve_ref(extends_ref, self.schema_doc)
            if not base_schema:
                # Try resolving relative to the structure_schema itself
                base_schema = self.resolve_ref(extends_ref, structure_schema)
            
            # Validate abstract type usage - $extends is ALLOWED to reference abstract types
            if base_schema:
                self.validate_abstract_ref(base_schema, extends_ref, is_extends_context=True)
        else:
            base_schema = None
        
        if not base_schema:
            # Fallback to tagged union if no base
            return self.generate_tagged_union(structure_schema, parent_namespace, write_file, explicit_name)
        
        choices = structure_schema.get('choices', {})
        selector = structure_schema.get('selector', 'type')
        
        # Mark the selector property as a discriminator BEFORE generating the base class
        # This allows generate_class to add [JsonIgnore] to the property
        base_schema_copy = base_schema.copy()
        if 'name' not in base_schema_copy:
            # Extract name from $extends ref
            base_name = extends_ref.split('/')[-1]
            base_schema_copy['name'] = base_name
        
        # Calculate what the base class ref will be
        base_namespace_for_ref = pascal(self.concat_namespace(self.base_namespace, base_schema_copy.get('namespace', schema_namespace)))
        base_class_name_for_ref = pascal(base_schema_copy['name'])
        pending_base_ref = 'global::'+self.get_qualified_name(base_namespace_for_ref, base_class_name_for_ref)
        
        # Record that this base type's selector property is a discriminator
        if self.system_text_json_annotation and selector in base_schema.get('properties', {}):
            self.discriminator_properties[pending_base_ref] = selector
        
        # Now generate the base class (it will check discriminator_properties)
        base_class_ref = self.generate_class(base_schema_copy, schema_namespace, write_file)
        base_class_name = base_class_ref.split('::')[-1].split('.')[-1]
        
        # Generate abstract base class with selector property
        class_definition = f"/// <summary>\n/// {structure_schema.get('description', class_name + ' (inline union base)')}\n/// </summary>\n"
        
        if self.system_text_json_annotation:
            class_definition += f'[System.Text.Json.Serialization.JsonPolymorphic(TypeDiscriminatorPropertyName = "{selector}")]\n'
            for choice_name in choices.keys():
                derived_class_name = pascal(choice_name)
                class_definition += f'[System.Text.Json.Serialization.JsonDerivedType(typeof({derived_class_name}), "{choice_name}")]\n'
        
        class_definition += f"public abstract partial class {class_name}"
        
        # Inherit from base class if it exists
        if base_class_name and base_class_name != class_name:
            class_definition += f" : {base_class_name}"
        
        class_definition += "\n{\n"
        
        # Check if selector is already in base properties
        base_has_selector = selector in base_schema.get('properties', {})
        
        # Only add selector property if base class doesn't already have it
        # If base has the selector, JsonPolymorphic will use it directly
        if not base_has_selector:
            class_definition += f"{INDENT}/// <summary>\n{INDENT}/// Type discriminator\n{INDENT}/// </summary>\n"
            if self.system_text_json_annotation:
                class_definition += f'{INDENT}[System.Text.Json.Serialization.JsonPropertyName("{selector}")]\n'
            class_definition += f"{INDENT}public string {pascal(selector)} {{ get; set; }} = \"\";\n"
        
        class_definition += "}"
        
        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
        
        # Generate derived classes for each choice with property merging
        for choice_name, choice_schema_ref in choices.items():
            # Resolve the choice schema
            # Handle both formats:
            # 1. Direct $ref: {"$ref": "#/definitions/Type"}
            # 2. Nested in type: {"type": {"$ref": "#/definitions/Type"}}
            ref_to_resolve = None
            if isinstance(choice_schema_ref, dict):
                if '$ref' in choice_schema_ref:
                    ref_to_resolve = choice_schema_ref['$ref']
                elif 'type' in choice_schema_ref and isinstance(choice_schema_ref['type'], dict) and '$ref' in choice_schema_ref['type']:
                    ref_to_resolve = choice_schema_ref['type']['$ref']
            
            if ref_to_resolve:
                choice_schema = self.resolve_ref(ref_to_resolve, self.schema_doc)
                if not choice_schema:
                    # Try resolving relative to the structure_schema itself
                    choice_schema = self.resolve_ref(ref_to_resolve, structure_schema)
            else:
                choice_schema = choice_schema_ref
            
            if not choice_schema or not isinstance(choice_schema, dict):
                continue
            
            # Mark this choice as generated to prevent duplicate generation in process_definitions
            derived_class_name = pascal(choice_name)
            derived_namespace = pascal(self.concat_namespace(self.base_namespace, schema_namespace))
            derived_ref = 'global::'+self.get_qualified_name(derived_namespace, derived_class_name)
            
            # Only generate if not already generated
            if derived_ref in self.generated_types:
                continue
            
            # Merge properties from base schema into choice schema
            merged_schema = self.merge_inherited_properties(choice_schema, base_schema, class_name)
            merged_schema['name'] = choice_name
            merged_schema['namespace'] = schema_namespace
            
            # Mark that this extends the union base
            merged_schema['$extends_inline_union'] = class_name
            
            # Generate the derived class
            self.generate_derived_class(merged_schema, class_name, choice_name, selector, schema_namespace, write_file)
        
        self.generated_types[ref] = "choice"
        self.generated_structure_types[ref] = structure_schema
        return ref
    
    def merge_inherited_properties(self, derived_schema: Dict, base_schema: Dict, union_class_name: str) -> Dict:
        """ Merges properties from base schema into derived schema """
        merged = derived_schema.copy()
        
        # Get properties from both schemas
        base_props = base_schema.get('properties', {})
        derived_props = merged.get('properties', {})
        
        # Track which properties come from base (for filtering during generation)
        base_property_names = list(base_props.keys())
        
        # Merge properties (derived overrides base)
        merged_props = {}
        merged_props.update(base_props)
        merged_props.update(derived_props)
        merged['properties'] = merged_props
        
        # Store base property names so we can skip them during code generation
        merged['$base_properties'] = base_property_names
        
        # Merge required fields
        base_required = base_schema.get('required', [])
        derived_required = merged.get('required', [])
        if isinstance(base_required, list) and isinstance(derived_required, list):
            # Combine and deduplicate
            merged['required'] = list(set(base_required + derived_required))
        
        return merged
    
    def generate_derived_class(self, schema: Dict, base_class_name: str, choice_name: str, selector: str, parent_namespace: str, write_file: bool) -> str:
        """ Generates a derived class for inline union """
        class_name = pascal(choice_name)
        namespace = pascal(self.concat_namespace(self.base_namespace, schema.get('namespace', parent_namespace)))
        
        ref = 'global::'+self.get_qualified_name(namespace, class_name)
        if ref in self.generated_types:
            return ref
        
        # Generate class with inheritance
        doc = schema.get('description', f'{class_name} - {choice_name} variant')
        class_definition = f"/// <summary>\n/// {doc}\n/// </summary>\n"
        
        class_definition += f"public partial class {class_name} : {base_class_name}\n{{\n"
        
        # Generate properties (only the derived-specific ones, base properties are inherited)
        properties = schema.get('properties', {})
        required_props = schema.get('required', [])
        
        # Get base class properties to filter them out
        # We need to find the base schema to know what properties to exclude
        # For now, we'll generate all properties from merged schema
        # NOTE: BaseAddress properties come through the merged schema but shouldn't be redeclared
        #       We need to identify which properties are from the BASE vs which are NEW
        
        # Get the original (non-merged) choice schema to determine NEW properties
        # The merged schema has all properties; we want only the ones NOT in base
        # Since we don't have access to the original choice schema here, we'll use a heuristic:
        # Properties marked with a special flag during merging
        
        # Alternative approach: Only generate properties that are NOT in the InlineChoice base
        # But InlineChoice only has the selector, not the BaseAddress properties
        # So we need to look further up the chain
        
        # SIMPLEST SOLUTION: Filter out properties that come from the extended base
        # We can detect this by checking if the property exists in the base_schema context
        # But we don't have base_schema in this method
        
        # FOR NOW: Generate all properties but mark inherited ones with 'new'
        # Actually, C# doesn't allow 'new required' - that's the error
        # So we MUST skip inherited properties entirely
        
        # The merged schema has ALL properties. We need to skip base properties.
        # The base properties are those NOT in the original choice schema
        # We need to pass the original choice schema properties to know what to generate
        
        # CORRECT FIX: Only generate properties from the ORIGINAL choice schema, not merged
        # But wait - we're passing merged_schema which has all properties
        # We need to differentiate
        
        # Let's add a marker during merge to track which properties are from base
        # Or better: pass BOTH original and merged schemas
        
        # QUICK FIX: Check if selector property to skip, and skip properties that were
        # in the base by checking schema metadata
        
        # Since schema has '$extends_inline_union', we can use that
        # But we need the ORIGINAL choice properties, not merged
        
        # The issue is we're generating from merged_schema which has ALL properties
        # We need to know which properties are NEW (from choice) vs inherited (from base)
        
        # SOLUTION: Don't generate inherited properties - but how to identify them?
        # We could store in merged_schema a list of base property names
        
        # Let me fix this by adding a key to mark base properties
        base_properties = schema.get('$base_properties', [])
        
        for prop_name, prop_schema in properties.items():
            # Skip selector - it's defined in base as required
            if prop_name == selector:
                continue
            # Skip properties inherited from base schema
            if prop_name in base_properties:
                continue
            field_def = self.generate_property(prop_name, prop_schema, class_name, parent_namespace, required_props)
            class_definition += field_def
        
        # Add constructor that sets the discriminator
        # If the selector exists in the base schema, use the original property name (snake_case)
        # Otherwise use the PascalCase version we defined in the union class
        base_properties = schema.get('$base_properties', [])
        if selector in base_properties:
            # Use the snake_case name from the base class
            selector_prop_name = selector
        else:
            # Use PascalCase name we defined in the union class
            selector_prop_name = pascal(selector)
        
        class_definition += f"\n{INDENT}/// <summary>\n{INDENT}/// Constructor that sets the discriminator value\n{INDENT}/// </summary>\n"
        class_definition += f"{INDENT}public {class_name}()\n{INDENT}{{\n"
        class_definition += f"{INDENT*2}this.{selector_prop_name} = \"{choice_name}\";\n"
        class_definition += f"{INDENT}}}\n"
        
        # Generate Equals and GetHashCode
        class_definition += self.generate_equals_and_gethashcode(schema, class_name, parent_namespace)
        
        class_definition += "}"
        
        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
        
        self.generated_types[ref] = "class"
        self.generated_structure_types[ref] = schema
        return ref

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """ Generates a tuple type - Per JSON Structure spec, tuples serialize as JSON arrays, not objects """
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedTuple'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = pascal(self.concat_namespace(self.base_namespace, schema_namespace))
        
        ref = 'global::'+self.get_qualified_name(namespace, class_name)
        if ref in self.generated_types:
            return ref
        
        properties = structure_schema.get('properties', {})
        tuple_order = structure_schema.get('tuple', [])
        
        # Build list of tuple element types and names in correct order
        tuple_elements = []
        for prop_name in tuple_order:
            if prop_name in properties:
                prop_schema = properties[prop_name]
                prop_type = self.convert_structure_type_to_csharp(class_name, prop_name, prop_schema, schema_namespace)
                field_name = pascal(prop_name) if self.pascal_properties else prop_name
                tuple_elements.append((prop_type, field_name))
        
        # Generate as a C# record struct with positional parameters
        # Per JSON Structure spec: tuples serialize as JSON arrays like ["Alice", 42]
        tuple_signature = ', '.join([f"{elem_type} {elem_name}" for elem_type, elem_name in tuple_elements])
        
        # Create the tuple record struct
        class_definition = f"/// <summary>\n/// {structure_schema.get('description', class_name)}\n/// </summary>\n"
        class_definition += f"/// <remarks>\n/// JSON Structure tuple type - serializes as JSON array: [{', '.join(['...' for _ in tuple_elements])}]\n/// </remarks>\n"
        
        # Add JsonConverter attribute if System.Text.Json annotations are enabled
        if self.system_text_json_annotation:
            class_definition += f"[System.Text.Json.Serialization.JsonConverter(typeof(TupleJsonConverter<{class_name}>))]\n"
        
        class_definition += f"public record struct {class_name}({tuple_signature});\n"
        
        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
        
        self.generated_types[ref] = "tuple"
        self.generated_structure_types[ref] = structure_schema
        return ref

    def generate_container_wrapper(self, structure_schema: Dict, parent_namespace: str, write_file: bool, explicit_name: str = '') -> str:
        """ Generates a wrapper class for root-level container types (map, array, set) """
        struct_type = structure_schema.get('type', 'map')
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', f'Root{struct_type.capitalize()}'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = pascal(self.concat_namespace(self.base_namespace, schema_namespace))
        
        ref = 'global::'+self.get_qualified_name(namespace, class_name)
        if ref in self.generated_types:
            return ref
        
        # Determine the underlying collection type
        value_type = "string"  # Default
        item_type = "string"   # Default
        underlying_type = "object"
        
        if struct_type == 'map':
            values_schema = structure_schema.get('values', {'type': 'string'})
            value_type = self.convert_structure_type_to_csharp(class_name, 'value', values_schema, schema_namespace)
            underlying_type = f"Dictionary<string, {value_type}>"
        elif struct_type == 'array':
            items_schema = structure_schema.get('items', {'type': 'string'})
            item_type = self.convert_structure_type_to_csharp(class_name, 'item', items_schema, schema_namespace)
            underlying_type = f"List<{item_type}>"
        elif struct_type == 'set':
            items_schema = structure_schema.get('items', {'type': 'string'})
            item_type = self.convert_structure_type_to_csharp(class_name, 'item', items_schema, schema_namespace)
            underlying_type = f"HashSet<{item_type}>"
        
        # Generate wrapper class with implicit conversions
        class_definition = f"/// <summary>\n/// {structure_schema.get('description', class_name)}\n/// </summary>\n"
        class_definition += f"/// <remarks>\n/// Wrapper for root-level {struct_type} type\n/// </remarks>\n"
        # Add JsonConverter attribute to serialize as the underlying collection
        class_definition += f"[System.Text.Json.Serialization.JsonConverter(typeof({class_name}JsonConverter))]\n"
        class_definition += f"public class {class_name}\n{{\n"
        class_definition += f"{INDENT}internal {underlying_type} _value = new();\n\n"
        
        # Add indexer or collection access
        if struct_type == 'map':
            class_definition += f"{INDENT}public {value_type} this[string key]\n"
            class_definition += f"{INDENT}{{\n"
            class_definition += f"{INDENT*2}get => _value[key];\n"
            class_definition += f"{INDENT*2}set => _value[key] = value;\n"
            class_definition += f"{INDENT}}}\n\n"
        elif struct_type in ('array', 'set'):
            class_definition += f"{INDENT}public {item_type} this[int index]\n"
            class_definition += f"{INDENT}{{\n"
            if struct_type == 'array':
                class_definition += f"{INDENT*2}get => _value[index];\n"
                class_definition += f"{INDENT*2}set => _value[index] = value;\n"
            else:  # set
                class_definition += f"{INDENT*2}get => _value.ElementAt(index);\n"
                class_definition += f"{INDENT*2}set => throw new NotSupportedException(\"Cannot set items by index in a HashSet\");\n"
            class_definition += f"{INDENT}}}\n\n"
        
        # Add Count property
        class_definition += f"{INDENT}public int Count => _value.Count;\n\n"
        
        # Add Add method for collections
        if struct_type == 'map':
            class_definition += f"{INDENT}public void Add(string key, {value_type} value) => _value.Add(key, value);\n\n"
        elif struct_type in ('array', 'set'):
            class_definition += f"{INDENT}public void Add({item_type} item) => _value.Add(item);\n\n"
        
        # Override Equals and GetHashCode for proper value equality
        class_definition += f"{INDENT}public override bool Equals(object? obj)\n"
        class_definition += f"{INDENT}{{\n"
        class_definition += f"{INDENT*2}if (obj is not {class_name} other) return false;\n"
        if struct_type == 'map':
            class_definition += f"{INDENT*2}if (_value.Count != other._value.Count) return false;\n"
            class_definition += f"{INDENT*2}foreach (var kvp in _value)\n"
            class_definition += f"{INDENT*2}{{\n"
            class_definition += f"{INDENT*3}if (!other._value.TryGetValue(kvp.Key, out var otherValue) || !Equals(kvp.Value, otherValue))\n"
            class_definition += f"{INDENT*4}return false;\n"
            class_definition += f"{INDENT*2}}}\n"
            class_definition += f"{INDENT*2}return true;\n"
        elif struct_type == 'array':
            class_definition += f"{INDENT*2}return _value.SequenceEqual(other._value);\n"
        else:  # set
            class_definition += f"{INDENT*2}return _value.SetEquals(other._value);\n"
        class_definition += f"{INDENT}}}\n\n"
        
        class_definition += f"{INDENT}public override int GetHashCode()\n"
        class_definition += f"{INDENT}{{\n"
        class_definition += f"{INDENT*2}var hash = new HashCode();\n"
        if struct_type == 'map':
            class_definition += f"{INDENT*2}foreach (var kvp in _value)\n"
            class_definition += f"{INDENT*2}{{\n"
            class_definition += f"{INDENT*3}hash.Add(kvp.Key);\n"
            class_definition += f"{INDENT*3}hash.Add(kvp.Value);\n"
            class_definition += f"{INDENT*2}}}\n"
        else:  # array or set
            class_definition += f"{INDENT*2}foreach (var item in _value)\n"
            class_definition += f"{INDENT*2}{{\n"
            class_definition += f"{INDENT*3}hash.Add(item);\n"
            class_definition += f"{INDENT*2}}}\n"
        class_definition += f"{INDENT*2}return hash.ToHashCode();\n"
        class_definition += f"{INDENT}}}\n\n"
        
        # Implicit conversion to underlying type
        class_definition += f"{INDENT}public static implicit operator {underlying_type}({class_name} wrapper) => wrapper._value;\n\n"
        
        # Implicit conversion from underlying type
        class_definition += f"{INDENT}public static implicit operator {class_name}({underlying_type} value) => new() {{ _value = value }};\n"
        
        class_definition += "}\n\n"
        
        # Generate custom JsonConverter for the wrapper class to serialize as the underlying collection
        class_definition += f"/// <summary>\n/// JSON converter for {class_name} to serialize as the underlying collection\n/// </summary>\n"
        class_definition += f"public class {class_name}JsonConverter : System.Text.Json.Serialization.JsonConverter<{class_name}>\n{{\n"
        class_definition += f"{INDENT}/// <inheritdoc/>\n"
        class_definition += f"{INDENT}public override {class_name}? Read(ref System.Text.Json.Utf8JsonReader reader, Type typeToConvert, System.Text.Json.JsonSerializerOptions options)\n"
        class_definition += f"{INDENT}{{\n"
        class_definition += f"{INDENT*2}var value = System.Text.Json.JsonSerializer.Deserialize<{underlying_type}>(ref reader, options);\n"
        class_definition += f"{INDENT*2}return value == null ? null : new {class_name}() {{ _value = value }};\n"
        class_definition += f"{INDENT}}}\n\n"
        class_definition += f"{INDENT}/// <inheritdoc/>\n"
        class_definition += f"{INDENT}public override void Write(System.Text.Json.Utf8JsonWriter writer, {class_name} value, System.Text.Json.JsonSerializerOptions options)\n"
        class_definition += f"{INDENT}{{\n"
        class_definition += f"{INDENT*2}System.Text.Json.JsonSerializer.Serialize(writer, value._value, options);\n"
        class_definition += f"{INDENT}}}\n"
        class_definition += "}\n"
        
        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
        
        self.generated_types[ref] = "class"
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
                equality_checks.append(f"System.Linq.Enumerable.SequenceEqual(this.{field_name} ?? Array.Empty<byte>(), other.{field_name} ?? Array.Empty<byte>())")
            elif field_type.startswith('Dictionary<'):
                # Dictionaries need special comparison - compare keys and values
                if field_type.endswith('?'):
                    dict_compare = f"((this.{field_name} == null && other.{field_name} == null) || (this.{field_name} != null && other.{field_name} != null && this.{field_name}.Count == other.{field_name}.Count && this.{field_name}.All(kvp => other.{field_name}.TryGetValue(kvp.Key, out var val) && Equals(kvp.Value, val))))"
                    equality_checks.append(dict_compare)
                else:
                    dict_compare = f"(this.{field_name}.Count == other.{field_name}.Count && this.{field_name}.All(kvp => other.{field_name}.TryGetValue(kvp.Key, out var val) && Equals(kvp.Value, val)))"
                    equality_checks.append(dict_compare)
            elif field_type.startswith('List<') or field_type.startswith('HashSet<'):
                # Lists and HashSets need sequence comparison
                if field_type.endswith('?'):
                    equality_checks.append(f"((this.{field_name} == null && other.{field_name} == null) || (this.{field_name} != null && other.{field_name} != null && this.{field_name}.SequenceEqual(other.{field_name})))")
                else:
                    equality_checks.append(f"this.{field_name}.SequenceEqual(other.{field_name})")
            else:
                # Use Equals for reference types, == for value types
                if field_type.endswith('?') or not self.is_csharp_primitive_type(field_type):
                    equality_checks.append(f"Equals(this.{field_name}, other.{field_name})")
                else:
                    equality_checks.append(f"this.{field_name} == other.{field_name}")
        
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

    def write_to_file(self, namespace: str, name: str, definition: str) -> None:
        """ Writes the class or enum to a file """
        directory_path = os.path.join(
            self.output_dir, os.path.join('src', namespace.replace('.', os.sep)))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}.cs")

        with open(file_path, 'w', encoding='utf-8') as file:
            # Common using statements (add more as needed)
            file_content = "using System;\nusing System.Collections.Generic;\n"
            file_content += "using System.Linq;\n"
            if self.system_text_json_annotation:
                file_content += "using System.Text.Json;\n"
                file_content += "using System.Text.Json.Serialization;\n"
            if self.newtonsoft_json_annotation:
                file_content += "using Newtonsoft.Json;\n"
            if self.system_xml_annotation:  # Add XML serialization using directive
                file_content += "using System.Xml.Serialization;\n"
            if self.avro_annotation:  # Add Avro using directives
                file_content += "using Avro;\n"
                file_content += "using Avro.Generic;\n"
                file_content += "using Avro.IO;\n"

            if namespace:
                # Namespace declaration with correct indentation for the definition
                file_content += f"\nnamespace {namespace}\n{{\n"
                indented_definition = '\n'.join(
                    [f"{INDENT}{line}" for line in definition.split('\n')])
                file_content += f"{indented_definition}\n}}"
            else:
                file_content += definition
            file.write(file_content)

    def convert(self, structure_schema_path: str, output_dir: str) -> None:
        """ Converts a JSON Structure schema file to C# classes """
        self.output_dir = output_dir
        
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        
        self.convert_schema(schema, output_dir)

    def convert_schema(self, schema: JsonNode, output_dir: str) -> None:
        """ Converts a JSON Structure schema to C# classes """
        if not isinstance(schema, list):
            schema = [schema]

        # Determine project name: use explicit project_name if set, otherwise derive from base_namespace
        if self.project_name and self.project_name.strip():
            # Use explicitly set project name
            project_name = self.project_name
        else:
            # Fall back to using base_namespace as project name
            project_name = self.base_namespace
            if not project_name or project_name.strip() == '':
                # Derive from output directory name as fallback
                project_name = os.path.basename(os.path.abspath(output_dir))
                if not project_name or project_name.strip() == '':
                    project_name = 'Generated'
                # Clean up the project name
                project_name = project_name.replace('-', '_').replace(' ', '_')
                # Update base_namespace to match (only if it was empty)
                self.base_namespace = project_name
                import warnings
                warnings.warn(f"No namespace provided, using '{project_name}' derived from output directory", UserWarning)
        
        self.schema_doc = schema
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # Create solution file if it doesn't exist
        if not glob.glob(os.path.join(output_dir, "src", "*.sln")):
            sln_file = os.path.join(output_dir, f"{project_name}.sln")
            if not os.path.exists(sln_file):
                if not os.path.exists(os.path.dirname(sln_file)) and os.path.dirname(sln_file):
                    os.makedirs(os.path.dirname(sln_file))
                with open(sln_file, 'w', encoding='utf-8') as file:
                    file.write(process_template(
                        "structuretocsharp/project.sln.jinja", 
                        project_name=project_name, 
                        uuid=lambda:str(uuid.uuid4()),
                        system_xml_annotation=self.system_xml_annotation,
                        system_text_json_annotation=self.system_text_json_annotation,
                        newtonsoft_json_annotation=self.newtonsoft_json_annotation))
        
        # Create main project file if it doesn't exist
        if not glob.glob(os.path.join(output_dir, "src", "*.csproj")):
            csproj_file = os.path.join(output_dir, "src", f"{pascal(project_name)}.csproj")
            if not os.path.exists(csproj_file):
                if not os.path.exists(os.path.dirname(csproj_file)):
                    os.makedirs(os.path.dirname(csproj_file))
                with open(csproj_file, 'w', encoding='utf-8') as file:
                    file.write(process_template(
                        "structuretocsharp/project.csproj.jinja",
                        project_name=project_name, 
                        system_xml_annotation=self.system_xml_annotation,
                        # Avro annotation requires System.Text.Json for intermediate conversions
                        system_text_json_annotation=self.system_text_json_annotation or self.avro_annotation,
                        newtonsoft_json_annotation=self.newtonsoft_json_annotation,
                        avro_annotation=self.avro_annotation,
                        NEWTONSOFT_JSON_VERSION=NEWTONSOFT_JSON_VERSION,
                        SYSTEM_TEXT_JSON_VERSION=SYSTEM_TEXT_JSON_VERSION,
                        SYSTEM_MEMORY_DATA_VERSION=SYSTEM_MEMORY_DATA_VERSION,
                        CSHARP_AVRO_VERSION=CSHARP_AVRO_VERSION,
                        NUNIT_VERSION=NUNIT_VERSION,
                        NUNIT_ADAPTER_VERSION=NUNIT_ADAPTER_VERSION,
                        MSTEST_SDK_VERSION=MSTEST_SDK_VERSION))
        
        # Create test project file if it doesn't exist
        if not glob.glob(os.path.join(output_dir, "test", "*.csproj")):
            csproj_test_file = os.path.join(output_dir, "test", f"{pascal(project_name)}.Test.csproj")
            if not os.path.exists(csproj_test_file):
                if not os.path.exists(os.path.dirname(csproj_test_file)):
                    os.makedirs(os.path.dirname(csproj_test_file))
                with open(csproj_test_file, 'w', encoding='utf-8') as file:
                    file.write(process_template(
                        "structuretocsharp/testproject.csproj.jinja", 
                        project_name=project_name,
                        system_xml_annotation=self.system_xml_annotation,
                        system_text_json_annotation=self.system_text_json_annotation,
                        newtonsoft_json_annotation=self.newtonsoft_json_annotation,
                        NUNIT_VERSION=NUNIT_VERSION,
                        NUNIT_ADAPTER_VERSION=NUNIT_ADAPTER_VERSION,
                        MSTEST_SDK_VERSION=MSTEST_SDK_VERSION))

        self.output_dir = output_dir
        
        # Register all schemas with $id keywords for cross-references
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            self.register_schema_ids(structure_schema)
        
        # Process each schema
        for structure_schema in (s for s in schema if isinstance(s, dict)):
            # Store definitions for later use
            if 'definitions' in structure_schema:
                self.definitions = structure_schema['definitions']
            
            # Store $offers for add-in system
            if '$offers' in structure_schema:
                self.offers = structure_schema['$offers']
            
            # Process root type FIRST so inline unions can generate derived classes
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
            
            # Now process remaining definitions that weren't generated as part of inline unions
            if 'definitions' in structure_schema:
                self.process_definitions(self.definitions, '')
            
            # Generate add-in interfaces and extensible wrapper classes
            if self.offers:
                self.generate_addins(structure_schema)
        
        # Generate tuple converter utility class if needed (after all types processed)
        if self.system_text_json_annotation:
            self.generate_tuple_converter(output_dir)
            self.generate_json_structure_converters(output_dir)
        
        # Generate tests
        self.generate_tests(output_dir)
        
        # Generate instance serializer program
        self.generate_instance_serializer(output_dir)

    def generate_addins(self, structure_schema: Dict) -> None:
        """
        Generates add-in interfaces and view classes for types that have $offers.
        
        For each add-in in $offers, creates:
        1. An interface I{AddinName} with the add-in properties
        2. An internal view class that wraps the Extensions dictionary
        3. Implicit operators on the base class that convert to the interface
        """
        if not self.offers or not isinstance(self.offers, dict):
            return
        
        root_type_name = structure_schema.get('name', 'Document')
        namespace_pascal = pascal(self.base_namespace)
        
        # Generate interface and view class for each add-in
        view_classes = []
        for addin_name, addin_def in self.offers.items():
            self.generate_addin_interface(addin_name, addin_def, namespace_pascal)
            view_class_name = self.generate_addin_view_class(addin_name, addin_def, namespace_pascal)
            view_classes.append((addin_name, view_class_name))
        
        # Add Extensions dictionary and implicit operators to the base class
        if 'type' in structure_schema and structure_schema['type'] == 'object':
            self.add_extensions_to_base_class(root_type_name, view_classes, namespace_pascal)

    def generate_addin_interface(self, addin_name: str, addin_def: Any, namespace: str) -> None:
        """
        Generates an interface for an add-in from $offers.
        
        Args:
            addin_name: Name of the add-in (e.g., "AuditInfo")
            addin_def: Definition of the add-in (either inline properties or a $ref)
            namespace: Target namespace for the interface
        """
        interface_name = f"I{pascal(addin_name)}"
        
        # Resolve the add-in definition if it's a reference
        if isinstance(addin_def, str):
            # It's a JSON pointer reference
            addin_def = self.resolve_ref(addin_def, self.schema_doc)
        elif isinstance(addin_def, dict) and '$ref' in addin_def:
            addin_def = self.resolve_ref(addin_def['$ref'], self.schema_doc)
        
        if not addin_def or not isinstance(addin_def, dict):
            return
        
        properties = addin_def.get('properties', {})
        if not properties:
            return
        
        # Generate interface definition
        interface_code = f"{INDENT}/// <summary>\n"
        interface_code += f"{INDENT}/// Add-in interface: {addin_name}\n"
        if 'description' in addin_def:
            interface_code += f"{INDENT}/// {addin_def['description']}\n"
        interface_code += f"{INDENT}/// </summary>\n"
        interface_code += f"{INDENT}public interface {interface_name}\n"
        interface_code += f"{INDENT}{{\n"
        
        # Generate properties
        for prop_name, prop_schema in properties.items():
            if not isinstance(prop_schema, dict):
                continue
                
            csharp_prop_name = pascal(prop_name) if self.pascal_properties else prop_name
            csharp_type = self.convert_structure_type_to_csharp(interface_name, prop_name, prop_schema, namespace)
            
            # Add XML doc comment
            if 'description' in prop_schema:
                interface_code += f"{INDENT}{INDENT}/// <summary>\n"
                interface_code += f"{INDENT}{INDENT}/// {prop_schema['description']}\n"
                interface_code += f"{INDENT}{INDENT}/// </summary>\n"
            
            # Interface properties are always nullable for add-ins (both value types and reference types)
            if not csharp_type.endswith('?'):
                csharp_type += '?'
            
            interface_code += f"{INDENT}{INDENT}{csharp_type} {csharp_prop_name} {{ get; set; }}\n"
        
        interface_code += f"{INDENT}}}\n"
        
        # Write interface to file
        self.write_to_file(namespace, interface_name, interface_code)
        
        # Track as generated
        qualified_name = 'global::' + self.get_qualified_name(namespace, interface_name)
        self.generated_types[qualified_name] = "interface"

    def generate_extensible_class(self, base_type_name: str, addin_names: List[str], namespace: str) -> None:
        """
        DEPRECATED: Replaced by generate_addin_view_class and add_extensions_to_base_class.
        This method is kept for backward compatibility but does nothing.
        """
        pass

    def generate_addin_view_class(self, addin_name: str, addin_def: Any, namespace: str) -> str:
        """
        Generates an internal view class that wraps the Extensions dictionary.
        
        Example output:
        internal sealed class AuditInfoView : IAuditInfo
        {
            private readonly Dictionary<string, object?> _extensions;
            
            public AuditInfoView(Dictionary<string, object?> extensions)
            {
                _extensions = extensions;
            }
            
            public string? CreatedBy
            {
                get => _extensions.TryGetValue("createdBy", out var val) ? val as string : null;
                set { if (value != null) _extensions["createdBy"] = value; else _extensions.Remove("createdBy"); }
            }
        }
        
        Args:
            addin_name: Name of the add-in (e.g., "AuditInfo")
            addin_def: Definition of the add-in
            namespace: Target namespace
            
        Returns:
            The name of the generated view class
        """
        view_class_name = f"{pascal(addin_name)}View"
        interface_name = f"I{pascal(addin_name)}"
        
        # Resolve the add-in definition if it's a reference
        if isinstance(addin_def, str):
            addin_def = self.resolve_ref(addin_def, self.schema_doc)
        elif isinstance(addin_def, dict) and '$ref' in addin_def:
            addin_def = self.resolve_ref(addin_def['$ref'], self.schema_doc)
        
        if not addin_def or not isinstance(addin_def, dict):
            return view_class_name
        
        properties = addin_def.get('properties', {})
        if not properties:
            return view_class_name
        
        # Generate class definition
        class_code = f"{INDENT}/// <summary>\n"
        class_code += f"{INDENT}/// View class wrapping Extensions dictionary for {addin_name} add-in\n"
        if 'description' in addin_def:
            class_code += f"{INDENT}/// {addin_def['description']}\n"
        class_code += f"{INDENT}/// </summary>\n"
        class_code += f"{INDENT}public sealed class {view_class_name} : {interface_name}\n"
        class_code += f"{INDENT}{{\n"
        
        # Add private field
        class_code += f"{INDENT}{INDENT}private readonly Dictionary<string, object?> _extensions;\n\n"
        
        # Add constructor
        class_code += f"{INDENT}{INDENT}public {view_class_name}(Dictionary<string, object?> extensions)\n"
        class_code += f"{INDENT}{INDENT}{{\n"
        class_code += f"{INDENT}{INDENT}{INDENT}_extensions = extensions;\n"
        class_code += f"{INDENT}{INDENT}}}\n\n"
        
        # Generate properties
        for prop_name, prop_schema in properties.items():
            if not isinstance(prop_schema, dict):
                continue
                
            csharp_prop_name = pascal(prop_name) if self.pascal_properties else prop_name
            csharp_type = self.convert_structure_type_to_csharp(view_class_name, prop_name, prop_schema, namespace)
            
            # Remove nullable marker for determining base type
            base_csharp_type = csharp_type.rstrip('?')
            is_nullable = csharp_type.endswith('?')
            
            # Ensure nullable for add-ins
            if not is_nullable:
                csharp_type += '?'
            
            # Add XML doc comment
            if 'description' in prop_schema:
                class_code += f"{INDENT}{INDENT}/// <summary>\n"
                class_code += f"{INDENT}{INDENT}/// {prop_schema['description']}\n"
                class_code += f"{INDENT}{INDENT}/// </summary>\n"
            
            # Generate getter that reads from dictionary
            class_code += f"{INDENT}{INDENT}public {csharp_type} {csharp_prop_name}\n"
            class_code += f"{INDENT}{INDENT}{{\n"
            
            # Getter - use TryGetValue with type-specific conversion
            class_code += f'{INDENT}{INDENT}{INDENT}get => _extensions.TryGetValue("{prop_name}", out var val) && val != null ? '
            
            # Add appropriate conversion based on type
            if base_csharp_type in ['string', 'bool', 'int', 'long', 'float', 'double', 'decimal']:
                if base_csharp_type == 'string':
                    class_code += 'val as string : null;\n'
                elif base_csharp_type == 'bool':
                    class_code += 'Convert.ToBoolean(val) : null;\n'
                elif base_csharp_type in ['int', 'long', 'float', 'double', 'decimal']:
                    class_code += f'Convert.To{base_csharp_type.capitalize()}(val) : null;\n'
                else:
                    class_code += 'val : null;\n'
            else:
                # For complex types, try direct cast
                class_code += f'({base_csharp_type})val : null;\n'
            
            # Setter - write to dictionary or remove if null
            class_code += f'{INDENT}{INDENT}{INDENT}set {{ if (value != null) _extensions["{prop_name}"] = value; else _extensions.Remove("{prop_name}"); }}\n'
            
            class_code += f"{INDENT}{INDENT}}}\n\n"
        
        class_code += f"{INDENT}}}\n"
        
        # Write class to file
        self.write_to_file(namespace, view_class_name, class_code)
        
        # Track as generated (internal, not exported)
        qualified_name = 'global::' + self.get_qualified_name(namespace, view_class_name)
        self.generated_types[qualified_name] = "view_class"
        
        return view_class_name

    def add_extensions_to_base_class(self, base_type_name: str, view_classes: List[tuple], namespace: str) -> None:
        """
        Adds Extensions dictionary property and implicit operators to the base class.
        
        Appends to the existing base class file:
        - Extensions property (Dictionary<string, object?>)
        - Implicit operators for each add-in interface
        
        Args:
            base_type_name: Name of the base type
            view_classes: List of (addin_name, view_class_name) tuples
            namespace: Target namespace
        """
        base_class_name = pascal(base_type_name)
        
        # Generate the partial class extension code
        extension_code = f"{INDENT}/// <summary>\n"
        extension_code += f"{INDENT}/// Partial class extension for {base_class_name} with add-in support\n"
        extension_code += f"{INDENT}/// </summary>\n"
        extension_code += f"{INDENT}public partial class {base_class_name}\n"
        extension_code += f"{INDENT}{{\n"
        
        # Add Extensions property
        extension_code += f"{INDENT}{INDENT}/// <summary>\n"
        extension_code += f"{INDENT}{INDENT}/// Extension properties storage for add-ins.\n"
        extension_code += f"{INDENT}{INDENT}/// Unknown JSON properties are automatically captured here during deserialization.\n"
        extension_code += f"{INDENT}{INDENT}/// </summary>\n"
        
        if self.system_text_json_annotation:
            extension_code += f'{INDENT}{INDENT}[System.Text.Json.Serialization.JsonExtensionData]\n'
        if self.newtonsoft_json_annotation:
            extension_code += f'{INDENT}{INDENT}[Newtonsoft.Json.JsonExtensionData]\n'
        
        extension_code += f"{INDENT}{INDENT}public Dictionary<string, object?> Extensions {{ get; set; }} = new();\n\n"
        
        # Add implicit operators for each add-in
        for addin_name, view_class_name in view_classes:
            interface_name = f"I{pascal(addin_name)}"
            
            extension_code += f"{INDENT}{INDENT}/// <summary>\n"
            extension_code += f"{INDENT}{INDENT}/// Implicit conversion to {interface_name} view\n"
            extension_code += f"{INDENT}{INDENT}/// </summary>\n"
            extension_code += f"{INDENT}{INDENT}public static implicit operator {view_class_name}({base_class_name} obj)\n"
            extension_code += f"{INDENT}{INDENT}{INDENT}=> new {view_class_name}(obj.Extensions);\n\n"
        
        extension_code += f"{INDENT}}}\n"
        
        # Write to a separate file (e.g., ProductExtensions.cs)
        extension_file_name = f"{base_class_name}Extensions"
        self.write_to_file(namespace, extension_file_name, extension_code)

    def process_definitions(self, definitions: Dict, namespace_path: str) -> None:
        """ Processes the definitions section recursively """
        for name, definition in definitions.items():
            if isinstance(definition, dict):
                if 'type' in definition:
                    # This is a type definition
                    current_namespace = self.concat_namespace(namespace_path, '')
                    # Check if this type was already generated (e.g., as part of inline union)
                    check_namespace = pascal(self.concat_namespace(self.base_namespace, current_namespace))
                    check_name = pascal(name)
                    check_ref = 'global::'+self.get_qualified_name(check_namespace, check_name)
                    if check_ref not in self.generated_types:
                        self.generate_class_or_choice(definition, current_namespace, write_file=True, explicit_name=name)
                else:
                    # This is a namespace
                    new_namespace = self.concat_namespace(namespace_path, name)
                    self.process_definitions(definition, new_namespace)

    def generate_tests(self, output_dir: str) -> None:
        """ Generates unit tests for all the generated C# classes and enums """
        test_directory_path = os.path.join(output_dir, "test")
        if not os.path.exists(test_directory_path):
            os.makedirs(test_directory_path, exist_ok=True)

        for class_name, type_kind in self.generated_types.items():
            # Skip test generation for:
            # 1. View classes (internal wrappers for Extensions dictionary)
            # 2. Extension partial classes (add implicit operators to base classes)
            base_name = class_name.split('.')[-1]
            
            # Skip view classes (e.g., AuditInfoView)
            if type_kind == "view_class" or base_name.endswith('View'):
                continue
            
            # Skip extension partial classes (e.g., ProductExtensions)
            if base_name.endswith('Extensions'):
                continue
            
            if type_kind in ["class", "enum"]:
                self.generate_test_class(class_name, type_kind, test_directory_path)

    def generate_tuple_converter(self, output_dir: str) -> None:
        """ Generates the TupleJsonConverter utility class for JSON array serialization """
        # Check if any tuples were generated
        has_tuples = any(type_kind == "tuple" for type_kind in self.generated_types.values())
        if not has_tuples:
            return  # No tuples, no need for converter

        # Convert base namespace to PascalCase for consistency with other generated classes
        namespace_pascal = pascal(self.base_namespace)
        
        # Generate the converter class
        converter_definition = process_template(
            "structuretocsharp/tuple_converter.cs.jinja",
            namespace=namespace_pascal
        )

        # Write to the same directory structure as other classes (using PascalCase path)
        directory_path = os.path.join(
            output_dir, os.path.join('src', namespace_pascal.replace('.', os.sep)))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        converter_file_path = os.path.join(directory_path, "TupleJsonConverter.cs")
        
        # Add using statements
        file_content = "using System;\n"
        file_content += "using System.Linq;\n"
        file_content += "using System.Reflection;\n"
        file_content += "using System.Text.Json;\n"
        file_content += "using System.Text.Json.Serialization;\n\n"
        file_content += converter_definition
        
        with open(converter_file_path, 'w', encoding='utf-8') as converter_file:
            converter_file.write(file_content)

    def generate_json_structure_converters(self, output_dir: str) -> None:
        """ Generates JSON Structure converters for types requiring string serialization.
        
        Per JSON Structure Core spec, int64, uint64, int128, uint128, decimal types
        use string representation in JSON to preserve precision. Duration (TimeSpan)
        uses ISO 8601 format.
        """
        # Check if any types need converters
        if not self.needs_json_structure_converters:
            return  # No types need special converters

        # Convert base namespace to PascalCase for consistency with other generated classes
        namespace_pascal = pascal(self.base_namespace)
        
        # Generate the converter class
        converter_definition = process_template(
            "structuretocsharp/json_structure_converters.cs.jinja",
            namespace=namespace_pascal
        )

        # Write to the same directory structure as other classes (using PascalCase path)
        directory_path = os.path.join(
            output_dir, os.path.join('src', namespace_pascal.replace('.', os.sep)))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        converter_file_path = os.path.join(directory_path, "JsonStructureConverters.cs")
        
        with open(converter_file_path, 'w', encoding='utf-8') as converter_file:
            converter_file.write(converter_definition)

    def generate_instance_serializer(self, output_dir: str) -> None:
        """ Generates InstanceSerializer.cs that creates instances and serializes them to JSON """
        test_directory_path = os.path.join(output_dir, "test")
        if not os.path.exists(test_directory_path):
            os.makedirs(test_directory_path, exist_ok=True)

        # Collect all classes (not enums, tuples, or other types) that have test classes
        # Skip abstract classes since they cannot be instantiated
        # Skip view classes and extension partial classes
        classes = []
        for class_name, type_kind in self.generated_types.items():
            if type_kind == "class":
                base_name = class_name.split('.')[-1]
                
                # Skip view classes (internal wrappers for Extensions dictionary)
                if base_name.endswith('View'):
                    continue
                
                # Skip extension partial classes
                if base_name.endswith('Extensions'):
                    continue
                
                # Skip abstract classes
                structure_schema = cast(Dict[str, JsonNode], self.generated_structure_types.get(class_name, {}))
                if structure_schema.get('abstract', False):
                    continue
                    
                if class_name.startswith("global::"):
                    class_name = class_name[8:]
                test_class_name = f"{class_name.split('.')[-1]}Tests"
                class_base_name = class_name.split('.')[-1]
                
                # Get proper namespace from class_name
                if '.' in class_name:
                    namespace = ".".join(class_name.split('.')[:-1])
                else:
                    namespace = self.base_namespace if self.base_namespace else ''
                
                # Build fully qualified test name
                full_qualified_test_name = f"{namespace}.{test_class_name}" if namespace else test_class_name
                
                classes.append({
                    'class_name': class_base_name,
                    'test_class_name': test_class_name,
                    'full_name': class_name,
                    'full_qualified_test_name': full_qualified_test_name
                })

        if not classes:
            return  # No classes to serialize

        # Determine if ToByteArray method is available (requires any serialization annotation)
        has_to_byte_array = self.system_text_json_annotation or self.newtonsoft_json_annotation or self.system_xml_annotation

        program_definition = process_template(
            "structuretocsharp/program.cs.jinja",
            classes=classes,
            has_to_byte_array=has_to_byte_array
        )

        program_file_path = os.path.join(test_directory_path, "InstanceSerializer.cs")
        with open(program_file_path, 'w', encoding='utf-8') as program_file:
            program_file.write(program_definition)

    def generate_test_class(self, class_name: str, type_kind: str, test_directory_path: str) -> None:
        """ Generates a unit test class for a given C# class or enum """
        structure_schema: Dict[str, JsonNode] = cast(Dict[str, JsonNode], self.generated_structure_types.get(class_name, {}))
        if class_name.startswith("global::"):
            class_name = class_name[8:]
        test_class_name = f"{class_name.split('.')[-1]}Tests"
        namespace = ".".join(class_name.split('.')[:-1])
        class_base_name = class_name.split('.')[-1]

        # Skip test generation for abstract classes (cannot be instantiated)
        if type_kind == "class" and structure_schema.get('abstract', False):
            return

        if type_kind == "class":
            fields = self.get_class_test_fields(structure_schema, class_base_name)
            test_class_definition = process_template(
                "structuretocsharp/class_test.cs.jinja",
                namespace=namespace,
                test_class_name=test_class_name,
                class_base_name=class_base_name,
                fields=fields,
                system_xml_annotation=self.system_xml_annotation,
                system_text_json_annotation=self.system_text_json_annotation,
                newtonsoft_json_annotation=self.newtonsoft_json_annotation
            )
        elif type_kind == "enum":
            # For enums, extract symbols from the enum schema
            enum_values = structure_schema.get('enum', [])
            symbols = []
            if enum_values:
                for value in enum_values:
                    if isinstance(value, str):
                        # Convert to PascalCase enum member name - must match generate_enum logic
                        symbol_name = pascal(str(value).replace('-', '_').replace(' ', '_'))
                        symbols.append(symbol_name)
                    else:
                        # For numeric enums, use Value1, Value2, etc.
                        symbols.append(f"Value{value}")
            
            test_class_definition = process_template(
                "structuretocsharp/enum_test.cs.jinja",
                namespace=namespace,
                test_class_name=test_class_name,
                enum_base_name=class_base_name,
                symbols=symbols,
                system_xml_annotation=self.system_xml_annotation,
                system_text_json_annotation=self.system_text_json_annotation,
                newtonsoft_json_annotation=self.newtonsoft_json_annotation
            )
        else:
            return

        test_file_path = os.path.join(test_directory_path, f"{test_class_name}.cs")
        with open(test_file_path, 'w', encoding='utf-8') as test_file:
            test_file.write(test_class_definition)

    def get_class_test_fields(self, structure_schema: Dict[str, JsonNode], class_name: str) -> List[Any]:
        """ Retrieves fields for a given class name """

        class Field:
            def __init__(self, fn: str, ft: str, tv: Any, ct: bool, pm: bool):
                self.field_name = fn
                self.field_type = ft
                self.test_value = tv
                self.is_const = ct
                self.is_primitive = pm

        fields: List[Field] = []
        if structure_schema and 'properties' in structure_schema:
            for prop_name, prop_schema in cast(Dict[str, Dict], structure_schema['properties']).items():
                field_name = prop_name
                if self.pascal_properties:
                    field_name = pascal(field_name)
                if field_name == class_name:
                    field_name += "_"
                if self.is_csharp_reserved_word(field_name):
                    field_name = f"@{field_name}"
                
                field_type = self.convert_structure_type_to_csharp(
                    class_name, field_name, prop_schema, str(structure_schema.get('namespace', '')))
                is_class = field_type in self.generated_types and self.generated_types[field_type] == "class"
                
                # Check if this is a const field
                is_const = 'const' in prop_schema
                schema_type = prop_schema.get('type', '') if isinstance(prop_schema, dict) else ''
                test_value = self.get_test_value(field_type, schema_type) if not is_const else self.format_default_value(prop_schema['const'], field_type)
                
                f = Field(field_name, field_type, test_value, is_const, not is_class)
                fields.append(f)
        return cast(List[Any], fields)

    def get_test_value(self, csharp_type: str, schema_type: str = '') -> str:
        """Returns a default test value based on the C# type and schema type"""
        # For nullable object types, return typed null to avoid var issues
        if csharp_type == "object?" or csharp_type == "object":
            return "null"  # Use null for object types (typically unions) to avoid reference inequality
        
        # Special test values for JSON Structure types that map to string in C#
        # but have specific format requirements
        if schema_type == 'jsonpointer':
            return '"/example/path"'  # Valid JSON Pointer format
        
        test_values = {
            'string': '"test_string"',
            'bool': 'true',
            'sbyte': '(sbyte)42',
            'byte': '(byte)42',
            'short': '(short)42',
            'ushort': '(ushort)42',
            'int': '42',
            'uint': '42U',
            'long': '42L',
            'ulong': '42UL',
            'System.Int128': 'new System.Int128(0, 42)',
            'System.UInt128': 'new System.UInt128(0, 42)',
            'float': '3.14f',
            'double': '3.14',
            'decimal': '3.14m',
            'byte[]': 'new byte[] { 0x01, 0x02, 0x03 }',
            'DateOnly': 'new DateOnly(2024, 1, 1)',
            'TimeOnly': 'new TimeOnly(12, 0, 0)',
            'DateTimeOffset': 'new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.Zero)',
            'TimeSpan': 'TimeSpan.FromHours(1)',
            'Guid': 'new Guid("12345678-1234-1234-1234-123456789012")',
            'Uri': 'new Uri("https://example.com")',
            'null': 'null'
        }
        if csharp_type.endswith('?'):
            csharp_type = csharp_type[:-1]
        
        # Normalize to use qualified reference (strip global:: prefix if present, then add it)
        base_type = csharp_type.replace('global::', '')
        qualified_ref = f'global::{base_type}'
        
        # Check if this is a tuple type (generated_types tracks what we've created)
        if qualified_ref in self.generated_types and self.generated_types[qualified_ref] == "tuple":
            # For tuple types, we need to construct with test values based on the schema
            schema = self.generated_structure_types.get(qualified_ref)
            if schema:
                tuple_order = schema.get('tuple', [])
                properties = schema.get('properties', {})
                test_params = []
                for prop_name in tuple_order:
                    if prop_name in properties:
                        prop_schema = properties[prop_name]
                        prop_type = self.convert_structure_type_to_csharp(base_type, prop_name, prop_schema, str(schema.get('namespace', '')))
                        test_params.append(self.get_test_value(prop_type))
                if test_params:
                    return f'new {base_type}({", ".join(test_params)})'
        
        # Check if this is a choice type (discriminated union)
        if qualified_ref in self.generated_types and self.generated_types[qualified_ref] == "choice":
            # For choice types, create an instance with the first choice property set
            schema = self.generated_structure_types.get(qualified_ref)
            if schema:
                choices = schema.get('choices', {})
                if choices:
                    # Get the first choice property
                    first_choice_name, first_choice_schema = next(iter(choices.items()))
                    choice_type = self.convert_structure_type_to_csharp(base_type, first_choice_name, first_choice_schema, str(schema.get('namespace', '')))
                    choice_test_value = self.get_test_value(choice_type)
                    # Use the constructor that takes the first choice
                    return f'new {base_type}({choice_test_value})'
        
        # Check if this is an enum type
        if qualified_ref in self.generated_types and self.generated_types[qualified_ref] == "enum":
            schema = self.generated_structure_types.get(qualified_ref)
            if schema:
                enum_values = schema.get('enum', [])
                if enum_values:
                    first_value = enum_values[0]
                    enum_base_type = schema.get('type', 'string')
                    numeric_types = ['int8', 'uint8', 'int16', 'uint16', 'int32', 'uint32', 'int64', 'uint64']
                    is_numeric = enum_base_type in numeric_types
                    
                    if is_numeric:
                        # Numeric enum - use the member name (Value1, Value2, etc.)
                        member_name = f"Value{first_value}"
                    else:
                        # String enum - convert to PascalCase member name
                        member_name = pascal(str(first_value).replace('-', '_').replace(' ', '_'))
                    
                    return f'{base_type}.{member_name}'
        
        return test_values.get(base_type, test_values.get(csharp_type, f'new {csharp_type}()'))




def convert_structure_to_csharp(
    structure_schema_path: str, 
    cs_file_path: str, 
    base_namespace: str = '',
    project_name: str = '',
    pascal_properties: bool = False, 
    system_text_json_annotation: bool = False, 
    newtonsoft_json_annotation: bool = False, 
    system_xml_annotation: bool = False,
    avro_annotation: bool = False
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
        avro_annotation (bool, optional): Use Avro annotations. Defaults to False.
    """

    if not base_namespace:
        base_namespace = os.path.splitext(os.path.basename(cs_file_path))[0].replace('-', '_')
    
    structtocs = StructureToCSharp(base_namespace)
    structtocs.project_name = project_name
    structtocs.pascal_properties = pascal_properties
    structtocs.system_text_json_annotation = system_text_json_annotation
    structtocs.newtonsoft_json_annotation = newtonsoft_json_annotation
    structtocs.system_xml_annotation = system_xml_annotation
    structtocs.avro_annotation = avro_annotation
    structtocs.convert(structure_schema_path, cs_file_path)


def convert_structure_schema_to_csharp(
    structure_schema: JsonNode, 
    output_dir: str, 
    base_namespace: str = '',
    project_name: str = '',
    pascal_properties: bool = False, 
    system_text_json_annotation: bool = False, 
    newtonsoft_json_annotation: bool = False, 
    system_xml_annotation: bool = False,
    avro_annotation: bool = False
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
        avro_annotation (bool, optional): Use Avro annotations. Defaults to False.
    """
    structtocs = StructureToCSharp(base_namespace)
    structtocs.project_name = project_name
    structtocs.pascal_properties = pascal_properties
    structtocs.system_text_json_annotation = system_text_json_annotation
    structtocs.newtonsoft_json_annotation = newtonsoft_json_annotation
    structtocs.system_xml_annotation = system_xml_annotation
    structtocs.avro_annotation = avro_annotation
    structtocs.convert_schema(structure_schema, output_dir)
