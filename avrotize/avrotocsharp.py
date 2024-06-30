# pylint: disable=line-too-long

""" AvroToCSharp class for converting Avro schema to C# classes """

import json
import os
import re
from typing import Any, Dict, List, Tuple, Union, cast
import uuid

from avrotize.common import is_generic_avro_type, pascal, process_template
import glob

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


INDENT = '    '

AVRO_CLASS_PREAMBLE = \
"""
public {type_name}(global::Avro.Generic.GenericRecord obj)
{
    global::Avro.Specific.ISpecificRecord self = this;
    for (int i = 0; obj.Schema.Fields.Count > i; ++i)
    {
        self.Put(i, obj.GetValue(i));
    }
}
"""

class AvroToCSharp:
    """ Converts Avro schema to C# classes """

    def __init__(self, base_namespace: str = '') -> None:
        self.base_namespace = base_namespace
        self.schema_doc: JsonNode = None
        self.output_dir = os.getcwd()
        self.pascal_properties = False
        self.system_text_json_annotation = False
        self.newtonsoft_json_annotation = False
        self.avro_annotation = False
        self.generated_types: Dict[str,str] = {}
        self.generated_avro_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}

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

    def map_primitive_to_csharp(self, avro_type: str) -> str:
        """ Maps Avro primitive types to C# types """
        mapping = {
            'null': 'void',  # Placeholder, actual handling for nullable types is in the union logic
            'boolean': 'bool',
            'int': 'int',
            'long': 'long',
            'float': 'float',
            'double': 'double',
            'bytes': 'byte[]',
            'string': 'string',
        }
        return mapping.get(avro_type, 'object')

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
        """ Checks if an Avro type is a C# primitive type """
        if csharp_type.endswith('?'):
            csharp_type = csharp_type[:-1]
        return csharp_type in ['null', 'bool', 'int', 'long', 'float', 'double', 'bytes', 'string', 'DateTime', 'decimal', 'short', 'sbyte', 'ushort', 'uint', 'ulong', 'byte[]', 'object']

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
            "ushort": "UInt16",
            "uint": "UInt32",
            "ulong": "UInt64"
        }
        return map.get(cs_type, cs_type)

    def convert_avro_type_to_csharp(self, class_name: str, field_name: str, avro_type: JsonNode, parent_namespace: str) -> str:
        """ Converts Avro type to C# type """
        if isinstance(avro_type, str):
            return self.map_primitive_to_csharp(avro_type)
        elif isinstance(avro_type, list):
            # Handle nullable types and unions
            if is_generic_avro_type(avro_type):
                return 'Dictionary<string, object>'
            else:
                non_null_types = [t for t in avro_type if t != 'null']
                if len(non_null_types) == 1:
                    # Nullable type
                    return f"{self.convert_avro_type_to_csharp(class_name, field_name, non_null_types[0], parent_namespace)}?"
                else:
                    return self.generate_embedded_union(class_name, field_name, non_null_types, parent_namespace, write_file=True)
        elif isinstance(avro_type, dict):
            # Handle complex types: records, enums, arrays, and maps
            if avro_type['type'] in ['record', 'enum']:
                return self.generate_class_or_enum(avro_type, parent_namespace, write_file=True)
            elif avro_type['type'] == 'array':
                return f"List<{self.convert_avro_type_to_csharp(class_name, field_name+'List', avro_type['items'], parent_namespace)}>"
            elif avro_type['type'] == 'map':
                return f"Dictionary<string, {self.convert_avro_type_to_csharp(class_name, field_name, avro_type['values'], parent_namespace)}>"
            return self.convert_avro_type_to_csharp(class_name, field_name, avro_type['type'], parent_namespace)
        return 'object'

    def generate_class_or_enum(self, avro_schema: Dict, parent_namespace: str, write_file: bool = True) -> str:
        """ Generates a Class or Enum """
        if avro_schema['type'] == 'record':
            return self.generate_class(avro_schema, parent_namespace, write_file)
        elif avro_schema['type'] == 'enum':
            return self.generate_enum(avro_schema, parent_namespace, write_file)
        return ''

    def generate_class(self, avro_schema: Dict, parent_namespace: str, write_file: bool) -> str:
        """ Generates a Class """
        class_definition = ''
        avro_namespace = avro_schema.get('namespace', parent_namespace)
        if not 'namespace' in avro_schema:
            avro_schema['namespace'] = parent_namespace
        namespace = pascal(self.concat_namespace(self.base_namespace, avro_namespace))
        class_name = pascal(avro_schema['name'])
        ref = 'global::'+self.get_qualified_name(namespace, class_name)
        if ref in self.generated_types:
            return ref

        class_definition += f"/// <summary>\n/// { avro_schema.get('doc', class_name ) }\n/// </summary>\n"
        fields_str = [self.generate_property(field, class_name, avro_namespace) for field in avro_schema.get('fields', [])]
        class_body = "\n".join(fields_str)
        class_definition += f"public partial class {class_name}"
        if self.avro_annotation:
            class_definition += " : global::Avro.Specific.ISpecificRecord"
        class_definition += "\n{\n"+class_body
        class_definition += f"\n{INDENT}/// <summary>\n{INDENT}/// Default constructor\n{INDENT}///</summary>\n"
        class_definition += f"{INDENT}public {class_name}()\n{INDENT}{{\n{INDENT}}}"
        if self.avro_annotation:
            class_definition += f"\n\n{INDENT}/// <summary>\n{INDENT}/// Constructor from Avro GenericRecord\n{INDENT}///</summary>\n"
            class_definition += f"{INDENT}public {class_name}(global::Avro.Generic.GenericRecord obj)\n{INDENT}{{\n"
            class_definition += f"{INDENT*2}global::Avro.Specific.ISpecificRecord self = this;\n"
            class_definition += f"{INDENT*2}for (int i = 0; obj.Schema.Fields.Count > i; ++i)\n{INDENT*2}{{\n"
            class_definition += f"{INDENT*3}self.Put(i, obj.GetValue(i));\n{INDENT*2}}}\n{INDENT}}}\n"
        if self.avro_annotation:
            avro_schema_json = json.dumps(avro_schema)
            # wrap schema at 80 characters
            avro_schema_json = avro_schema_json.replace('"', '§')
            avro_schema_json = f"\"+\n{INDENT}\"".join(
                [avro_schema_json[i:i+80] for i in range(0, len(avro_schema_json), 80)])
            avro_schema_json = avro_schema_json.replace('§', '\\"')
            class_definition += f"\n\n{INDENT}/// <summary>\n{INDENT}/// Avro schema for this class\n{INDENT}/// </summary>"
            class_definition += f"\n{INDENT}public static global::Avro.Schema AvroSchema = global::Avro.Schema.Parse(\n{INDENT}\"{avro_schema_json}\");\n"
            class_definition += f"\n{INDENT}global::Avro.Schema global::Avro.Specific.ISpecificRecord.Schema => AvroSchema;\n"
            get_method = f"{INDENT}object global::Avro.Specific.ISpecificRecord.Get(int fieldPos)\n" + \
                INDENT+"{"+f"\n{INDENT*2}switch (fieldPos)\n{INDENT*2}" + "{"
            put_method = f"{INDENT}void global::Avro.Specific.ISpecificRecord.Put(int fieldPos, object fieldValue)\n" + \
                INDENT+"{"+f"\n{INDENT*2}switch (fieldPos)\n{INDENT*2}"+"{"
            for pos, field in enumerate(avro_schema.get('fields', [])):
                field_name = field['name']
                if self.is_csharp_reserved_word(field_name):
                    field_name = f"@{field_name}"
                field_type = self.convert_avro_type_to_csharp(class_name, field_name, field['type'], avro_namespace)
                if self.pascal_properties:
                    field_name = pascal(field_name)
                if field_name == class_name:
                    field_name += "_"
                if field_type in self.generated_types:
                    if self.generated_types[field_type] == "union":
                        get_method += f"\n{INDENT*3}case {pos}: return this.{field_name}?.ToObject();"
                        put_method += f"\n{INDENT*3}case {pos}: this.{field_name} = {field_type}.FromObject(fieldValue); break;"
                    elif self.generated_types[field_type] == "enum":
                        get_method += f"\n{INDENT*3}case {pos}: return ({field_type})this.{field_name};"
                        put_method += f"\n{INDENT*3}case {pos}: this.{field_name} = fieldValue is global::Avro.Generic.GenericEnum?Enum.Parse<{field_type}>(((global::Avro.Generic.GenericEnum)fieldValue).Value):({field_type})fieldValue; break;"
                    elif self.generated_types[field_type] == "class":
                        get_method += f"\n{INDENT*3}case {pos}: return this.{field_name};"
                        put_method += f"\n{INDENT*3}case {pos}: this.{field_name} = fieldValue is global::Avro.Generic.GenericRecord?new {field_type}((global::Avro.Generic.GenericRecord)fieldValue):({field_type})fieldValue; break;"
                else:
                    get_method += f"\n{INDENT*3}case {pos}: return this.{field_name};"
                    if field_type.startswith("List<"):
                        inner_type = field_type.strip()[5:-2] if field_type[-1] == '?' else field_type[5:-1]
                        if inner_type in self.generated_types:
                            if self.generated_types[inner_type] == "class":
                                put_method += f"\n{INDENT*3}case {pos}: this.{field_name} = fieldValue is Object[]?((Object[])fieldValue).Select(x => new {inner_type}((global::Avro.Generic.GenericRecord)x)).ToList():({field_type})fieldValue; break;"
                            else:
                                put_method += f"\n{INDENT*3}case {pos}: this.{field_name} = fieldValue is Object[]?((Object[])fieldValue).Select(x => ({inner_type})x).ToList():({field_type})fieldValue; break;"
                        else:
                            put_method += f"\n{INDENT*3}case {pos}: this.{field_name} = fieldValue is Object[]?((Object[])fieldValue).Select(x => ({inner_type})x).ToList():({field_type})fieldValue; break;"
                    else:
                        put_method += f"\n{INDENT*3}case {pos}: this.{field_name} = ({field_type})fieldValue; break;"
            get_method += f"\n{INDENT*3}default: throw new global::Avro.AvroRuntimeException($\"Bad index {{fieldPos}} in Get()\");"
            put_method += f"\n{INDENT*3}default: throw new global::Avro.AvroRuntimeException($\"Bad index {{fieldPos}} in Put()\");"
            get_method += "\n"+INDENT+INDENT+"}\n"+INDENT+"}"
            put_method += "\n"+INDENT+INDENT+"}\n"+INDENT+"}"
            class_definition += f"\n{get_method}\n{put_method}\n"

        # emit helper methods
        class_definition += process_template(
            "avrotocsharp/dataclass_core.jinja",
            class_name=class_name,
            avro_annotation=self.avro_annotation,
            system_text_json_annotation=self.system_text_json_annotation,
            newtonsoft_json_annotation=self.newtonsoft_json_annotation,
            json_match_clauses=self.create_is_json_match_clauses(avro_schema, avro_namespace, class_name))

        class_definition += "\n"+"}"

        if write_file:
            self.write_to_file(namespace, class_name, class_definition)

        self.generated_types[ref] = "class"
        self.generated_avro_types[ref] = avro_schema
        return ref

    def create_is_json_match_clauses(self, avro_schema, parent_namespace, class_name) -> List[str]:
        """ Generates the IsJsonMatch method for System.Text.Json """
        clauses: List[str] = []
        field_count = 0
        for field in avro_schema.get('fields', []):
            field_name = field['name']
            if self.is_csharp_reserved_word(field_name):
                field_name = f"@{field_name}"
            if field_name == class_name:
                field_name += "_"
            field_type = self.convert_avro_type_to_csharp(
                    class_name, field_name, field['type'], parent_namespace)
            clauses.append(self.get_is_json_match_clause(class_name, field_name, field_type))
        if field_count == 0:
            clauses.append("true")
        return clauses

    def get_is_json_match_clause(self, class_name, field_name, field_type) -> str:
        """ Generates the IsJsonMatch clause for a field """
        class_definition = ''
        field_name_js = field_name[1:] if field_name[0] == '@' else field_name
        is_optional = field_type[-1] == '?'
        field_type = field_type[:-1] if is_optional else field_type
        if field_type == 'byte[]':
            class_definition += f"({'!' if is_optional else ''}element.TryGetProperty(\"{field_name_js}\", out System.Text.Json.JsonElement {field_name}) {'||' if is_optional else '&&'} ({field_name}.ValueKind == System.Text.Json.JsonValueKind.String){f' || {field_name}.ValueKind == System.Text.Json.JsonValueKind.Null' if is_optional else ''})"
        elif field_type == 'string':
            class_definition += f"({'!' if is_optional else ''}element.TryGetProperty(\"{field_name_js}\", out System.Text.Json.JsonElement {field_name}) {'||' if is_optional else '&&'} ({field_name}.ValueKind == System.Text.Json.JsonValueKind.String){f' || {field_name}.ValueKind == System.Text.Json.JsonValueKind.Null' if is_optional else ''})"
        elif field_type in ['int', 'long', 'float', 'double', 'decimal', 'short', 'sbyte', 'ushort', 'uint', 'ulong']:
            class_definition += f"({'!' if is_optional else ''}element.TryGetProperty(\"{field_name_js}\", out System.Text.Json.JsonElement {field_name}) {'||' if is_optional else '&&'} ({field_name}.ValueKind == System.Text.Json.JsonValueKind.Number){f' || {field_name}.ValueKind == System.Text.Json.JsonValueKind.Null' if is_optional else ''})"
        elif field_type == 'bool':
            class_definition += f"({'!' if is_optional else ''}element.TryGetProperty(\"{field_name_js}\", out System.Text.Json.JsonElement {field_name}) {'||' if is_optional else '&&'} ({field_name}.ValueKind == System.Text.Json.JsonValueKind.True || {field_name}.ValueKind == System.Text.Json.JsonValueKind.False){f' || {field_name}.ValueKind == System.Text.Json.JsonValueKind.Null' if is_optional else ''})"
        elif field_type.startswith("global::"):
            type_kind = self.generated_types[field_type] if field_type in self.generated_types else "class"
            if type_kind == "class":
                class_definition += f"({'!' if is_optional else ''}element.TryGetProperty(\"{field_name_js}\", out System.Text.Json.JsonElement {field_name}) {'||' if is_optional else '&&'} ({f'{field_name}.ValueKind == System.Text.Json.JsonValueKind.Null || ' if is_optional else ''}{field_type}.IsJsonMatch({field_name})))"
            elif type_kind == "enum":
                class_definition += f"({'!' if is_optional else ''}element.TryGetProperty(\"{field_name_js}\", out System.Text.Json.JsonElement {field_name}) {'||' if is_optional else '&&'} ({f'{field_name}.ValueKind == System.Text.Json.JsonValueKind.Null ||' if is_optional else ''}({field_name}.ValueKind == System.Text.Json.JsonValueKind.String && Enum.TryParse<{field_type}>({field_name}.GetString(), true, out _ ))))"
        else:
            is_union = False
            field_union = pascal(field_name)+'Union'
            if field_type == field_union:
                field_union = class_name+"."+pascal(field_name)+'Union'
                type_kind = self.generated_types[field_union] if field_union in self.generated_types else "class"
                if type_kind == "union":
                    is_union = True
                    class_definition += f"({'!' if is_optional else ''}element.TryGetProperty(\"{field_name_js}\", out System.Text.Json.JsonElement {field_name}) {'||' if is_optional else '&&'} ({f'{field_name}.ValueKind == System.Text.Json.JsonValueKind.Null || ' if is_optional else ''}{field_type}.IsJsonMatch({field_name})))"
            if not is_union:
                class_definition += f"({'!' if is_optional else ''}element.TryGetProperty(\"{field_name_js}\", out System.Text.Json.JsonElement {field_name}) {'||' if is_optional else '&&'} true )"
        return class_definition

    def get_is_json_match_clause_type(self, element_name, class_name, field_type) -> str:
        """ Generates the IsJsonMatch clause for a field """
        class_definition = ''
        is_optional = field_type[-1] == '?'
        field_type = field_type[:-1] if is_optional else field_type
        if field_type == 'byte[]':
            class_definition += f"({element_name}.ValueKind == System.Text.Json.JsonValueKind.String{f' || {element_name}.ValueKind == System.Text.Json.JsonValueKind.Null' if is_optional else ''})"
        elif field_type == 'string':
            class_definition += f"({element_name}.ValueKind == System.Text.Json.JsonValueKind.String{f' || {element_name}.ValueKind == System.Text.Json.JsonValueKind.Null' if is_optional else ''})"
        elif field_type in ['int', 'long', 'float', 'double', 'decimal', 'short', 'sbyte', 'ushort', 'uint', 'ulong']:
            class_definition += f"({element_name}.ValueKind == System.Text.Json.JsonValueKind.Number{f' || {element_name}.ValueKind == System.Text.Json.JsonValueKind.Null' if is_optional else ''})"
        elif field_type == 'bool':
            class_definition += f"({element_name}.ValueKind == System.Text.Json.JsonValueKind.True || {element_name}.ValueKind == System.Text.Json.JsonValueKind.False{f' || {element_name}.ValueKind == System.Text.Json.JsonValueKind.Null' if is_optional else ''})"
        elif field_type.startswith("global::"):
            type_kind = self.generated_types[field_type] if field_type in self.generated_types else "class"
            if type_kind == "class":
                class_definition += f"({f'{element_name}.ValueKind == System.Text.Json.JsonValueKind.Null || ' if is_optional else ''}{field_type}.IsJsonMatch({element_name}))"
            elif type_kind == "enum":
                class_definition += f"({f'{element_name}.ValueKind == System.Text.Json.JsonValueKind.Null ||' if is_optional else ''}({element_name}.ValueKind == System.Text.Json.JsonValueKind.String && Enum.TryParse<{field_type}>({element_name}.GetString(), true, out _ ))))"
        else:
            is_union = False
            field_union = pascal(element_name)+'Union'
            if field_type == field_union:
                field_union = class_name+"."+pascal(element_name)+'Union'
                type_kind = self.generated_types[field_union] if field_union in self.generated_types else "class"
                if type_kind == "union":
                    is_union = True
                    class_definition += f"({f'{element_name}.ValueKind == System.Text.Json.JsonValueKind.Null || ' if is_optional else ''}{field_type}.IsJsonMatch({element_name})))"
            if not is_union:
                class_definition += f"({element_name}.ValueKind == System.Text.Json.JsonValueKind.Object{f' || {element_name}.ValueKind == System.Text.Json.JsonValueKind.Null' if is_optional else ''})"
        return class_definition

    def generate_enum(self, avro_schema: Dict, parent_namespace: str, write_file: bool) -> str:
        """ Generates an Enum """
        enum_definition = ''
        namespace = pascal(self.concat_namespace(
            self.base_namespace, avro_schema.get('namespace', parent_namespace)))
        enum_name = pascal(avro_schema['name'])
        ref = 'global::'+self.get_qualified_name(namespace, enum_name)
        if ref in self.generated_types:
            return ref

        enum_definition += "#pragma warning disable 1591\n\n"
        enum_definition += f"/// <summary>\n/// {avro_schema.get('doc', enum_name )}\n/// </summary>\n"
        symbols_str = [
            f"{INDENT}{symbol}" for symbol in avro_schema['symbols']]
        enum_body = ",\n".join(symbols_str)
        enum_definition += f"public enum {enum_name}\n{{\n{enum_body}\n}}"

        if write_file:
            self.write_to_file(namespace, enum_name, enum_definition)
        ref = 'global::'+self.get_qualified_name(namespace, enum_name)
        self.generated_types[ref] = "enum"
        self.generated_avro_types[ref] = avro_schema
        return ref

    def generate_embedded_union(self, class_name: str, field_name: str, avro_type: List, parent_namespace: str, write_file: bool) -> str:
        """ Generates an embedded Union Class """

        class_definition_ctors = class_definition_decls = class_definition_read = ''
        class_definition_write = class_definition = class_definition_toobject = ''
        class_definition_objctr = class_definition_genericrecordctor = ''
        namespace = pascal(self.concat_namespace(self.base_namespace, parent_namespace))
        list_is_json_match: List [str] = []
        union_class_name = pascal(field_name)+'Union'
        ref = class_name+'.'+union_class_name

        union_types = [self.convert_avro_type_to_csharp(class_name, field_name+"Option"+str(i), t, parent_namespace) for i,t in enumerate(avro_type)]
        for i, union_type in enumerate(union_types):
            is_dict = is_list = False
            if union_type.startswith("Dictionary<"):
                # get the type information from the dictionary
                is_dict = True
                match = re.findall(r"Dictionary<(.+)\s*,\s*(.+)>", union_type)
                union_type_name = "Map" + pascal(match[0][1].rsplit('.', 1)[-1])
            elif union_type.startswith("List<"):
                # get the type information from the list
                is_list = True
                match = re.findall(r"List<(.+)>", union_type)
                union_type_name = "Array" + pascal(match[0].rsplit('.', 1)[-1])
            elif union_type == "byte[]":
                union_type_name = "bytes"
            else:
                union_type_name = union_type.rsplit('.', 1)[-1]
            if self.is_csharp_reserved_word(union_type_name):
                union_type_name = f"@{union_type_name}"
            class_definition_objctr += f"{INDENT*3}if (obj is {union_type})\n{INDENT*3}{{\n{INDENT*4}self.{union_type_name} = ({union_type})obj;\n{INDENT*4}return self;\n{INDENT*3}}}\n"
            if union_type in self.generated_types and self.generated_types[union_type] == "class":
                class_definition_genericrecordctor += f"{INDENT*3}if (obj.Schema.Fullname == {union_type}.AvroSchema.Fullname)\n{INDENT*3}{{\n{INDENT*4}this.{union_type_name} = new {union_type}(obj);\n{INDENT*4}return;\n{INDENT*3}}}\n"
            class_definition_ctors += \
                f"{INDENT*2}/// <summary>\n{INDENT*2}/// Constructor for {union_type_name} values\n{INDENT*2}/// </summary>\n" + \
                f"{INDENT*2}public {union_class_name}({union_type}? {union_type_name})\n{INDENT*2}{{\n{INDENT*3}this.{union_type_name} = {union_type_name};\n{INDENT*2}}}\n"
            class_definition_decls += \
                f"{INDENT*2}/// <summary>\n{INDENT*2}/// Gets the {union_type_name} value\n{INDENT*2}/// </summary>\n" + \
                f"{INDENT*2}public {union_type}? {union_type_name} {{ get; private set; }} = null;\n"
            class_definition_toobject += f"{INDENT*3}if ({union_type_name} != null) {{\n{INDENT*4}return {union_type_name};\n{INDENT*3}}}\n"

            if is_dict:
                class_definition_read += f"{INDENT*3}if (element.ValueKind == JsonValueKind.Object)\n{INDENT*3}{{\n" + \
                        f"{INDENT*4}var map = System.Text.Json.JsonSerializer.Deserialize<{union_type}>(element, options);\n" + \
                        f"{INDENT*4}if (map != null) {{ return new {union_class_name}(map); }} else {{ throw new NotSupportedException(); }};\n" + \
                        f"{INDENT*3}}}\n"
            elif is_list:
                class_definition_read += f"{INDENT*3}if (element.ValueKind == JsonValueKind.Array)\n{INDENT*3}{{\n" + \
                        f"{INDENT*4}var map = System.Text.Json.JsonSerializer.Deserialize<{union_type}>(element, options);\n" + \
                        f"{INDENT*4}if (map != null) {{ return new {union_class_name}(map); }} else {{ throw new NotSupportedException(); }};\n" + \
                        f"{INDENT*3}}}\n"
            elif self.is_csharp_primitive_type(union_type):
                if union_type == "byte[]":
                    class_definition_read += f"{INDENT*3}if (element.ValueKind == JsonValueKind.String)\n{INDENT*3}{{\n{INDENT*4}return new {union_class_name}(element.GetBytesFromBase64());\n{INDENT*3}}}\n"
                if union_type == "string":
                    class_definition_read += f"{INDENT*3}if (element.ValueKind == JsonValueKind.String)\n{INDENT*3}{{\n{INDENT*4}return new {union_class_name}(element.GetString());\n{INDENT*3}}}\n"
                elif union_type in ['int', 'long', 'float', 'double', 'decimal', 'short', 'sbyte', 'ushort', 'uint', 'ulong']:
                    class_definition_read += f"{INDENT*3}if (element.ValueKind == JsonValueKind.Number)\n{INDENT*3}{{\n{INDENT*4}return new {union_class_name}(element.Get{self.map_csharp_primitive_to_clr_type(union_type)}());\n{INDENT*3}}}\n"
                elif union_type == "bool":
                    class_definition_read += f"{INDENT*3}if (element.ValueKind == JsonValueKind.True || element.ValueKind == JsonValueKind.False)\n{INDENT*2}{{\n{INDENT*3}return new {union_class_name}(element.GetBoolean());\n{INDENT*3}}}\n"
                elif union_type == "DateTime":
                    class_definition_read += f"{INDENT*3}if (element.ValueKind == JsonValueKind.String)\n{INDENT*3}{{\n{INDENT*4}return new {union_class_name}(System.DateTime.Parse(element.GetString()));\n{INDENT*3}}}\n"
                elif union_type == "DateTimeOffset":
                    class_definition_read += f"{INDENT*3}if (element.ValueKind == JsonValueKind.String)\n{INDENT*3}{{\n{INDENT*4}return new {union_class_name}(System.DateTimeOffset.Parse(element.GetString()));\n{INDENT*3}}}\n"
            else:
                class_definition_read += f"{INDENT*3}if ({union_type}.IsJsonMatch(element))\n{INDENT*3}{{\n{INDENT*4}return new {union_class_name}({union_type}.FromData(element, System.Net.Mime.MediaTypeNames.Application.Json));\n{INDENT*3}}}\n"
            class_definition_write += f"{INDENT*3}{'else ' if i>0 else ''}if (value.{union_type_name} != null)\n{INDENT*3}{{\n{INDENT*4}System.Text.Json.JsonSerializer.Serialize(writer, value.{union_type_name}, options);\n{INDENT*3}}}\n"
            gij = self.get_is_json_match_clause_type("element", class_name, union_type)
            if gij:
                list_is_json_match.append(gij)

        class_definition = \
            f"/// <summary>\n/// {class_name}. Type union resolver. \n/// </summary>\n" + \
            f"public partial class {class_name}\n{{\n" + \
            f"{INDENT}/// <summary>\n{INDENT}/// Union class for {field_name}\n{INDENT}/// </summary>\n"
        if self.system_text_json_annotation:
            class_definition += \
                f"{INDENT}[System.Text.Json.Serialization.JsonConverter(typeof({union_class_name}))]\n"
        class_definition += \
            f"{INDENT}public sealed class {union_class_name}"
        if self.system_text_json_annotation:
            class_definition += f": System.Text.Json.Serialization.JsonConverter<{union_class_name}>"
        class_definition += f"\n{INDENT}{{\n" + \
            f"{INDENT*2}/// <summary>\n{INDENT*2}/// Default constructor\n{INDENT*2}/// </summary>\n" + \
            f"{INDENT*2}public {union_class_name}() {{ }}\n"
        class_definition += class_definition_ctors
        if self.avro_annotation:
            class_definition += \
                f"{INDENT*2}/// <summary>\n{INDENT*2}/// Constructor for Avro decoder\n{INDENT*2}/// </summary>\n" + \
                f"{INDENT*2}internal static {union_class_name} FromObject(object obj)\n{INDENT*2}{{\n"
            if class_definition_genericrecordctor:
                class_definition += \
                    f"{INDENT*3}if (obj is global::Avro.Generic.GenericRecord)\n{INDENT*3}{{\n" + \
                    f"{INDENT*4}return new {union_class_name}((global::Avro.Generic.GenericRecord)obj);\n" + \
                    f"{INDENT*3}}}\n"
            class_definition += \
                f"{INDENT*3}var self = new {union_class_name}();\n" + \
                class_definition_objctr + \
                f"{INDENT*3}throw new NotSupportedException(\"No record type matched the type\");\n" + \
                f"{INDENT*2}}}\n"
            if class_definition_genericrecordctor:
                class_definition += f"\n{INDENT*2}/// <summary>\n{INDENT*2}/// Constructor from Avro GenericRecord\n{INDENT*2}/// </summary>\n" + \
                    f"{INDENT*2}public {union_class_name}(global::Avro.Generic.GenericRecord obj)\n{INDENT*2}{{\n" + \
                    class_definition_genericrecordctor + \
                    f"{INDENT*3}throw new NotSupportedException(\"No record type matched the type\");\n" + \
                    f"{INDENT*2}}}\n"
        class_definition += \
            class_definition_decls + \
            f"\n{INDENT*2}/// <summary>\n{INDENT*2}/// Yields the current value of the union\n{INDENT*2}/// </summary>\n" + \
            f"\n{INDENT*2}public Object ToObject()\n{INDENT*2}{{\n" + \
            class_definition_toobject+ \
            f"{INDENT*3}throw new NotSupportedException(\"No record type is set in the union\");\n" + \
            f"{INDENT*2}}}\n"
        if self.system_text_json_annotation:
            class_definition += \
                f"\n{INDENT*2}/// <summary>\n{INDENT*2}/// Reads the JSON representation of the object.\n{INDENT*2}/// </summary>\n" + \
                f"{INDENT*2}public override {union_class_name}? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)\n{INDENT*2}{{\n{INDENT*3}var element = JsonElement.ParseValue(ref reader);\n" + \
                class_definition_read + \
                f"{INDENT*3}throw new NotSupportedException(\"No record type matched the JSON data\");\n{INDENT*2}}}\n" + \
                f"\n{INDENT*2}/// <summary>\n{INDENT*2}/// Writes the JSON representation of the object.\n{INDENT*2}/// </summary>\n" + \
                f"{INDENT*2}public override void Write(Utf8JsonWriter writer, {union_class_name} value, JsonSerializerOptions options)\n{INDENT*2}{{\n" + \
                class_definition_write + \
                f"{INDENT*3}else\n{INDENT*3}{{\n{INDENT*4}throw new NotSupportedException(\"No record type is set in the union\");\n{INDENT*3}}}\n{INDENT*2}}}\n" + \
                f"\n{INDENT*2}/// <summary>\n{INDENT*2}/// Checks if the JSON element matches the schema\n{INDENT*2}/// </summary>\n" + \
                f"{INDENT*2}public static bool IsJsonMatch(System.Text.Json.JsonElement element)\n{INDENT*2}{{" + \
                f"\n{INDENT*3}return "+f"\n{INDENT*3} || ".join(list_is_json_match)+f";\n{INDENT*2}}}\n"
        class_definition += f"{INDENT}}}\n}}"

        if write_file:
            self.write_to_file(namespace, class_name +"."+union_class_name, class_definition)

        self.generated_types[ref] = "union" # it doesn't matter if the names clash, we just need to know whether it's a union
        return ref

    def find_type(self, kind: str, avro_schema: JsonNode, type_name: str, type_namespace: str, parent_namespace = '') -> JsonNode:
        """ recursively find the type (kind 'record' or 'enum') in the schema """
        if isinstance(avro_schema, list):
            for s in avro_schema:
                found = self.find_type(kind, s, type_name, type_namespace, parent_namespace)
                if found:
                    return found
        elif isinstance(avro_schema, dict):
            if avro_schema['type'] == kind and avro_schema['name'] == type_name and avro_schema.get('namespace', parent_namespace) == type_namespace:
                return avro_schema
            parent_namespace = avro_schema.get('namespace', parent_namespace)
            if 'fields' in avro_schema and isinstance(avro_schema['fields'], list):
                for field in avro_schema['fields']:
                    if isinstance(field,dict) and 'type' in field and isinstance(field['type'], dict):
                        return self.find_type(kind, field['type'], type_name, type_namespace, parent_namespace)
        return None

    def is_enum_type(self, avro_type: Union[str, Dict, List]) -> bool:
        """ Checks if a type is an enum """
        if isinstance(avro_type, str):
            schema = self.schema_doc
            name = avro_type.split('.')[-1]
            namespace = ".".join(avro_type.split('.')[:-1])
            return self.find_type('enum', schema, name, namespace) is not None
        elif isinstance(avro_type, list):
            return False
        elif isinstance(avro_type, dict):
            return avro_type['type'] == 'enum'

    def generate_property(self, field: Dict, class_name: str, parent_namespace: str) -> str:
        """ Generates a property """
        is_enum_type = self.is_enum_type(field['type'])
        field_type = self.convert_avro_type_to_csharp(
            class_name, field['name'], field['type'], parent_namespace)
        field_default = field.get('const', field.get('default', None))
        annotation_name = field_name = field['name']
        if self.is_csharp_reserved_word(field_name):
            field_name = f"@{field_name}"
        if self.pascal_properties:
            field_name = pascal(field_name)
        if field_name == class_name:
            field_name += "_"
        prop = ''
        prop += f"{INDENT}/// <summary>\n{INDENT}/// { field.get('doc', field_name) }\n{INDENT}/// </summary>\n"
        if self.system_text_json_annotation:
            prop += f"{INDENT}[System.Text.Json.Serialization.JsonPropertyName(\"{annotation_name}\")]\n"
            if is_enum_type:
                prop += f"{INDENT}[System.Text.Json.Serialization.JsonConverter(typeof(JsonStringEnumConverter))]\n"
            if field_type.endswith("Union") and not field_type.startswith("global::"):
                prop += f"{INDENT}[System.Text.Json.Serialization.JsonConverter(typeof({field_type}))]\n"
        if self.newtonsoft_json_annotation:
            prop += f"{INDENT}[Newtonsoft.Json.JsonProperty(\"{annotation_name}\")]\n"
        prop += f"{INDENT}public {field_type} {field_name} {{ get; {'private ' if 'const' in field else ''}set; }}" + ((" = "+(f"\"{field_default}\"" if isinstance(field_default,str) else field_default) + ";") if field_default else "")
        return prop

    def write_to_file(self, namespace: str, name: str, definition: str):
        """ Writes the class or enum to a file """
        directory_path = os.path.join(
            self.output_dir, os.path.join('src', namespace.replace('.', os.sep)))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}.cs")

        with open(file_path, 'w', encoding='utf-8') as file:
            # Common using statements (add more as needed)
            file_content = "#pragma warning disable CS8618\n#pragma warning disable CS8603\n\nusing System;\nusing System.Collections.Generic;\n"
            file_content += "using System.Linq;\n"
            if self.system_text_json_annotation:
                file_content += "using System.Text.Json;\n"
                file_content += "using System.Text.Json.Serialization;\n"
            if self.newtonsoft_json_annotation:
                file_content += "using Newtonsoft.Json;\n"

            if namespace:
                # Namespace declaration with correct indentation for the definition
                file_content += f"\nnamespace {namespace}\n{{\n"
                indented_definition = '\n'.join(
                    [f"{INDENT}{line}" for line in definition.split('\n')])
                file_content += f"{indented_definition}\n}}"
            else:
                file_content += definition
            file.write(file_content)

    def generate_tests(self, output_dir: str) -> None:
        """ Generates unit tests for all the generated C# classes and enums """
        test_directory_path = os.path.join(output_dir, "test")
        if not os.path.exists(test_directory_path):
            os.makedirs(test_directory_path, exist_ok=True)

        for class_name, type_kind in self.generated_types.items():
            if type_kind in ["class", "enum"]:
                self.generate_test_class(class_name, type_kind, test_directory_path)

    def generate_test_class(self, class_name: str, type_kind: str, test_directory_path: str) -> None:
        """ Generates a unit test class for a given C# class or enum """
        avro_schema:Dict[str,JsonNode] = cast(Dict[str,JsonNode], self.generated_avro_types.get(class_name, {}))
        if class_name.startswith("global::"):
            class_name = class_name[8:]
        test_class_name = f"{class_name.split('.')[-1]}Tests"
        namespace = ".".join(class_name.split('.')[:-1])
        class_base_name = class_name.split('.')[-1]

        if type_kind == "class":
            fields = self.get_class_test_fields(avro_schema, class_base_name)
            test_class_definition = process_template(
                "avrotocsharp/class_test.cs.jinja",
                namespace=namespace,
                test_class_name=test_class_name,
                class_base_name=class_base_name,
                fields=fields,
                avro_annotation=self.avro_annotation
            )
        elif type_kind == "enum":
            test_class_definition = process_template(
                "avrotocsharp/enum_test.cs.jinja",
                namespace=namespace,
                test_class_name=test_class_name,
                enum_base_name=class_base_name,
                symbols=avro_schema.get('symbols', []),
            )

        test_file_path = os.path.join(test_directory_path, f"{test_class_name}.cs")
        with open(test_file_path, 'w', encoding='utf-8') as test_file:
            test_file.write(test_class_definition)

    def get_class_test_fields(self, avro_schema: Dict[str,JsonNode], class_name: str) -> List[Any]:
        """ Retrieves fields for a given class name """

        class Field:
            def __init__(self, fn: str, ft:str, tv:Any, ct: bool, pm: bool):
                self.field_name = fn
                self.field_type = ft
                self.test_value = tv
                self.is_const = ct
                self.is_primitive = pm

        fields: List[Field] = []
        if avro_schema and 'fields' in avro_schema:
            for field in cast(List[Dict[str,JsonNode]],avro_schema['fields']):
                field_name = str(field['name'])
                if self.pascal_properties:
                    field_name = pascal(field_name)
                if field_name == class_name:
                    field_name += "_"
                if self.is_csharp_reserved_word(field_name):
                    field_name = f"@{field_name}"
                field_type = self.convert_avro_type_to_csharp(class_name, field_name, field['type'], str(avro_schema.get('namespace', '')))
                is_class = field_type in self.generated_types and self.generated_types[field_type] == "class"
                f = Field(field_name,
                          field_type,
                          (self.get_test_value(field_type) if not "const" in field else '\"'+str(field["const"])+'\"'),
                          "const" in field and field["const"] is not None,
                          not is_class)
                fields.append(f)
        return cast(List[Any], fields)

    def get_test_value(self, csharp_type: str) -> str:
        """Returns a default test value based on the Avro type"""
        test_values = {
            'string': '"test_string"',
            'bool': 'true',
            'int': '42',
            'long': '42L',
            'float': '3.14f',
            'double': '3.14',
            'decimal': '3.14d',
            'byte[]': '{0x01, 0x02, 0x03}',
            'null': 'null',
            'Date': 'new Date()',
            'DateTime': 'DateTime.UtcNow()',
            'Guid': 'Guid.NewGuid()'
        }
        if csharp_type.endswith('?'):
            csharp_type = csharp_type[:-1]
        return test_values.get(csharp_type, f'new {csharp_type}()')

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """ Converts Avro schema to C# """
        if not isinstance(schema, list):
            schema = [schema]

        project_name = self.base_namespace
        self.schema_doc = schema
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        if not glob.glob(os.path.join(output_dir, "src", "*.sln")):
            sln_file = os.path.join(
                output_dir, f"{project_name}.sln")
            if not os.path.exists(sln_file):
                if not os.path.exists(os.path.dirname(sln_file)):
                    os.makedirs(os.path.dirname(sln_file))
                with open(sln_file, 'w', encoding='utf-8') as file:
                    file.write(process_template("avrotocsharp/project.sln.jinja", project_name=project_name, uuid=lambda:str(uuid.uuid4())))
        if not glob.glob(os.path.join(output_dir, "src", "*.csproj")):
            csproj_file = os.path.join(
                output_dir, "src", f"{pascal(project_name)}.csproj")
            if not os.path.exists(csproj_file):
                if not os.path.exists(os.path.dirname(csproj_file)):
                    os.makedirs(os.path.dirname(csproj_file))
                with open(csproj_file, 'w', encoding='utf-8') as file:
                    file.write(process_template("avrotocsharp/project.csproj.jinja"))
        if not glob.glob(os.path.join(output_dir, "test", "*.csproj")):
            csproj_test_file = os.path.join(
                output_dir, "test", f"{pascal(project_name)}.Test.csproj")
            if not os.path.exists(csproj_test_file):
                if not os.path.exists(os.path.dirname(csproj_test_file)):
                    os.makedirs(os.path.dirname(csproj_test_file))
                with open(csproj_test_file, 'w', encoding='utf-8') as file:
                    file.write(process_template("avrotocsharp/testproject.csproj.jinja", project_name=project_name))

        self.output_dir = output_dir
        for avro_schema in (avs for avs in schema if isinstance(avs, dict)):
            self.generate_class_or_enum(avro_schema, '')
        self.generate_tests(output_dir)

    def convert(self, avro_schema_path: str, output_dir: str):
        """ Converts Avro schema to C# """
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_avro_to_csharp(avro_schema_path, cs_file_path, base_namespace='', pascal_properties=False, system_text_json_annotation=False, newtonsoft_json_annotation=False, avro_annotation=False):
    """_summary_

    Converts Avro schema to C# classes

    Args:
        avro_schema_path (_type_): Avro input schema path
        cs_file_path (_type_): Output C# file path
    """

    if not base_namespace:
        base_namespace = os.path.splitext(os.path.basename(cs_file_path))[0].replace('-', '_')
    avrotocs = AvroToCSharp(base_namespace)
    avrotocs.pascal_properties = pascal_properties
    avrotocs.system_text_json_annotation = system_text_json_annotation
    avrotocs.newtonsoft_json_annotation = newtonsoft_json_annotation
    avrotocs.avro_annotation = avro_annotation
    avrotocs.convert(avro_schema_path, cs_file_path)


def convert_avro_schema_to_csharp(avro_schema: JsonNode, output_dir: str, base_namespace: str = '', pascal_properties: bool = False, system_text_json_annotation: bool = False, newtonsoft_json_annotation: bool = False, avro_annotation: bool = False):
    """_summary_

    Converts Avro schema to C# classes

    Args:
        avro_schema (_type_): Avro schema to convert
        output_dir (_type_): Output directory
        base_namespace (_type_): Base namespace for the generated classes
        pascal_properties (_type_): Pascal case properties
        system_text_json_annotation (_type_): Use System.Text.Json annotations
        newtonsoft_json_annotation (_type_): Use Newtonsoft.Json annotations
        avro_annotation (_type_): Use Avro annotations
    """
    avrotocs = AvroToCSharp(base_namespace)
    avrotocs.pascal_properties = pascal_properties
    avrotocs.system_text_json_annotation = system_text_json_annotation
    avrotocs.newtonsoft_json_annotation = newtonsoft_json_annotation
    avrotocs.avro_annotation = avro_annotation
    avrotocs.convert_schema(avro_schema, output_dir)
