# pylint: disable=line-too-long

""" AvroToCSharp class for converting Avro schema to C# classes """

import json
import os
from typing import Dict, List, Union

from avrotize.common import pascal
import glob

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

INDENT = '    '
CSPROJ_CONTENT = """
<Project Sdk="Microsoft.NET.Sdk">
    <PropertyGroup>
        <TargetFramework>net8.0</TargetFramework>
        <Nullable>enable</Nullable>
    </PropertyGroup>
    <ItemGroup>
        <PackageReference Include="Apache.Avro" Version="1.11.3" />
        <PackageReference Include="Newtonsoft.Json" Version="13.0.3" />
        <PackageReference Include="System.Text.Json" Version="8.0.3" />
        <PackageReference Include="System.Memory.Data" Version="8.0.0" />
    </ItemGroup>
</Project>     
"""

AVRO_TOBYTEARRAY = \
"""
if (contentType == "avro/binary")
{
    var stream = new System.IO.MemoryStream();
    var encoder = new Avro.IO.BinaryEncoder(stream);
    var writer = new Avro.Specific.SpecificDatumWriter<{type_name}>({type_name}.AvroSchema);
    writer.Write(this, encoder);
    return stream.ToArray();
}
"""

SYSTEM_TEXT_JSON_TOBYTEARRAY = \
"""
if (contentType == System.Net.Mime.MediaTypeNames.Application.Json)
{
    return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(this);
}"""

NEWTONSOFT_JSON_TOBYTEARRAY = \
"""
if (contentType == System.Net.Mime.MediaTypeNames.Application.Json)
{
    return System.Text.Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(this));
}
"""

SYSTEM_TEXT_JSON_FROMDATA = \
"""
if ( contentType == System.Net.Mime.MediaTypeNames.Application.Json)
{
    if (data is System.Text.Json.JsonElement) 
    {
        return System.Text.Json.JsonSerializer.Deserialize<{type_name}>((System.Text.Json.JsonElement)data);
    }
    else if ( data is string)
    {
        return System.Text.Json.JsonSerializer.Deserialize<{type_name}>((string)data);
    }
    else if (data is System.BinaryData)
    {
        return ((System.BinaryData)data).ToObjectFromJson<{type_name}>();
    }
    throw new NotSupportedException("Data is not of a supported type for JSON conversion to {type_name}");
}
"""

NEWTONSOFT_JSON_FROMDATA = \
"""
if ( contentType == System.Net.Mime.MediaTypeNames.Application.Json)
{
    if (data is string)
    {
        return Newtonsoft.Json.JsonConvert.DeserializeObject<{type_name}>((string)data);
    }
    else if (data is System.BinaryData)
    {
        return ((System.BinaryData)data).ToObjectFromJson<{type_name}>();
    }
    throw new NotSupportedException("Data is not of a supported type for JSON conversion to {type_name}");
}
"""

AVRO_FROMDATA = \
"""
if ( contentType.StartsWith("avro/") || contentType.StartsWith("application/avro") )
{
    var stream = data switch
    {
        System.IO.Stream s => s,
        System.BinaryData bd => bd.ToStream(),
        byte[] bytes => new System.IO.MemoryStream(bytes),
        _ => throw new NotSupportedException("Data is not of a supported type for conversion to Stream")
    };
    if ( contentType == "avro/binary" || contentType == "application/avro+binary")
    {
        var decoder = new Avro.IO.BinaryDecoder(stream);
        var reader = new Avro.Specific.SpecificDatumReader<{type_name}>({type_name}.AvroSchema, {type_name}.AvroSchema);
        return reader.Read(new {type_name}(), decoder);
    }
    if ( contentType == "avro/json" || contentType == "application/avro+json")
    {
        var decoder = new Avro.IO.JsonDecoder({type_name}.AvroSchema, stream);
        var reader = new Avro.Specific.SpecificDatumReader<{type_name}>({type_name}.AvroSchema, {type_name}.AvroSchema);
        return reader.Read(new {type_name}(), decoder);
    }
    throw new NotSupportedException("Unsupported Avro content type: " + contentType);
}    
"""


class AvroToCSharp:
    """ Converts Avro schema to C# classes """
    def __init__(self, base_namespace: str = '') -> None:
        self.base_namespace = base_namespace
        self.output_dir = os.getcwd()
        self.pascal_properties = False
        self.system_text_json_annotation = False
        self.newtonsoft_json_annotation = False
        self.avro_annotation = False

    def concat_namespace(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        return f"{namespace}.{name}" if namespace != '' else name

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

    def convert_avro_type_to_csharp(self, avro_type: Union[str, Dict, List], parent_namespace: str) -> str:
        """ Converts Avro type to C# type """
        if isinstance(avro_type, str):
            return self.map_primitive_to_csharp(avro_type)
        elif isinstance(avro_type, list):
            # Handle nullable types and unions
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                # Nullable type
                return f"{self.convert_avro_type_to_csharp(non_null_types[0], parent_namespace)}?"
            else:
                # Handle union by generating classes for complex types within
                for t in non_null_types:
                    if isinstance(t, dict) and (t.get('type') == 'record' or t.get('type') == 'enum'):
                        self.generate_class_or_enum(t, parent_namespace)
                return 'object'  # Placeholder for complex unions
        elif isinstance(avro_type, dict):
            # Handle complex types: records, enums, arrays, and maps
            if avro_type['type'] in ['record', 'enum']:
                return self.generate_class_or_enum(avro_type, parent_namespace, write_file=True)
            elif avro_type['type'] == 'array':
                return f"List<{self.convert_avro_type_to_csharp(avro_type['items'], parent_namespace)}>"
            elif avro_type['type'] == 'map':
                return f"Dictionary<string, {self.convert_avro_type_to_csharp(avro_type['values'], parent_namespace)}>"
            return self.convert_avro_type_to_csharp(avro_type['type'], parent_namespace)
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
        namespace = pascal(self.concat_namespace(self.base_namespace,avro_schema.get('namespace', parent_namespace)))
        if 'doc' in avro_schema:
            class_definition += f"/// <summary>\n/// {avro_schema['doc']}\n/// </summary>\n"
        class_name = pascal(avro_schema['name'])
        fields_str = [self.generate_property(field, class_name, namespace) for field in avro_schema.get('fields', [])]
        class_body = "\n".join(fields_str)
        class_definition += f"public class {class_name}"
        if self.avro_annotation:
            class_definition += " : global::Avro.Specific.ISpecificRecord"
        class_definition += "\n{\n"+class_body
        if self.avro_annotation:
            avro_schema_json = json.dumps(avro_schema)
            # wrap schema at 80 characters
            avro_schema_json = avro_schema_json.replace('"', '§')           
            avro_schema_json = f"\"+\n{INDENT}\"".join([avro_schema_json[i:i+80] for i in range(0, len(avro_schema_json), 80)])
            avro_schema_json = avro_schema_json.replace('§', '\\"')
            class_definition += f"\n\n{INDENT}public static global::Avro.Schema AvroSchema = global::Avro.Schema.Parse(\n{INDENT}\"{avro_schema_json}\");\n"
            class_definition += f"\n{INDENT}Schema global::Avro.Specific.ISpecificRecord.Schema => AvroSchema;\n"
            get_method = f"{INDENT}object global::Avro.Specific.ISpecificRecord.Get(int fieldPos)\n"+INDENT+"{"+f"\n{INDENT}{INDENT}switch (fieldPos)\n{INDENT}{INDENT}" + "{"
            put_method = f"{INDENT}void global::Avro.Specific.ISpecificRecord.Put(int fieldPos, object fieldValue)\n"+INDENT+"{"+f"\n{INDENT}{INDENT}switch (fieldPos)\n{INDENT}{INDENT}"+"{"
            for pos, field in enumerate(avro_schema.get('fields', [])):
                field_name = field['name']
                if self.is_csharp_reserved_word(field_name):
                    field_name = f"@{field_name}"
                if self.pascal_properties:
                    field_name = pascal(field_name)
                if field_name == class_name:
                    field_name += "_"
                field_type = self.convert_avro_type_to_csharp(field['type'], namespace)
                get_method += f"\n{INDENT}{INDENT}{INDENT}case {pos}: return this.{field_name};"
                put_method += f"\n{INDENT}{INDENT}{INDENT}case {pos}: this.{field_name} = ({field_type})fieldValue; break;"
            get_method += f"\n{INDENT}{INDENT}{INDENT}default: throw new global::Avro.AvroRuntimeException($\"Bad index {{fieldPos}} in Get()\");"
            put_method += f"\n{INDENT}{INDENT}{INDENT}default: throw new global::Avro.AvroRuntimeException($\"Bad index {{fieldPos}} in Put()\");"
            get_method += "\n"+INDENT+INDENT+"}\n"+INDENT+"}"
            put_method += "\n"+INDENT+INDENT+"}\n"+INDENT+"}"
            class_definition += f"\n{get_method}\n{put_method}\n"
        
        # emit ToByteArray method
        class_definition += f"\n\n{INDENT}public byte[] ToByteArray(string contentType)\n{INDENT}{{"    
        if self.avro_annotation:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(AVRO_TOBYTEARRAY.strip().replace("{type_name}", class_name).split("\n"))
        if self.system_text_json_annotation:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(SYSTEM_TEXT_JSON_TOBYTEARRAY.strip().replace("{type_name}", class_name).split("\n"))
        if self.newtonsoft_json_annotation:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(NEWTONSOFT_JSON_TOBYTEARRAY.strip().replace("{type_name}", class_name).split("\n"))
        class_definition += f"\n{INDENT*2}throw new System.NotImplementedException($\"Unsupported content type {{contentType}}\");\n{INDENT}}}"
        
        # emit FromData factory method
        class_definition += f"\n\n{INDENT}public static {class_name} FromData(object data, string contentType)\n{INDENT}{{"
        class_definition += f'\n{INDENT*2}if ( data is {class_name}) return ({class_name})data;'
        if self.avro_annotation:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(AVRO_FROMDATA.strip().replace("{type_name}", class_name).split("\n"))
        if self.system_text_json_annotation:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(SYSTEM_TEXT_JSON_FROMDATA.strip().replace("{type_name}", class_name).split("\n"))
        if self.newtonsoft_json_annotation:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(NEWTONSOFT_JSON_FROMDATA.strip().replace("{type_name}", class_name).split("\n"))
        class_definition += f"\n{INDENT*2}throw new System.NotImplementedException($\"Unsupported content type {{contentType}}\");\n{INDENT}}}"
        
        class_definition += "\n"+"}"

        if write_file:
            self.write_to_file(namespace, class_name, class_definition)
        return self.concat_namespace(namespace, class_name)

    def generate_enum(self, avro_schema: Dict, parent_namespace: str, write_file: bool) -> str:
        """ Generates an Enum """
        enum_definition = ''
        namespace = pascal(self.concat_namespace(self.base_namespace, avro_schema.get('namespace', parent_namespace)))
        if 'doc' in avro_schema:
            enum_definition += f"/// <summary>\n/// {avro_schema['doc']}\n/// </summary>\n"
        enum_name = pascal(avro_schema['name'])
        symbols_str = [f"{INDENT}{symbol}" for symbol in avro_schema['symbols']]
        enum_body = ",\n".join(symbols_str)
        enum_definition += f"public enum {enum_name}\n{{\n{enum_body}\n}}"

        if write_file:
            self.write_to_file(namespace, enum_name, enum_definition)
        return self.concat_namespace(namespace, enum_name)

    def generate_property(self, field: Dict, class_name: str, parent_namespace: str) -> str:
        """ Generates a property """
        field_type = self.convert_avro_type_to_csharp(field['type'], parent_namespace)
        annotation_name = field_name = field['name']
        if self.is_csharp_reserved_word(field_name):
            field_name = f"@{field_name}"
        if self.pascal_properties:
            field_name = pascal(field_name)           
        if field_name == class_name:
            field_name += "_" 
        prop = ''
        if 'doc' in field:
            prop += f"{INDENT}/// <summary>\n{INDENT}/// {field['doc']}\n{INDENT}/// </summary>\n"
        if self.system_text_json_annotation:
            prop += f"{INDENT}[JsonPropertyName(\"{annotation_name}\")]\n"
        if self.newtonsoft_json_annotation:
            prop += f"{INDENT}[JsonProperty(\"{annotation_name}\")]\n"
        prop += f"{INDENT}public {field_type} {field_name} {{ get; set; }}"
        return prop

    def write_to_file(self, namespace: str, name: str, definition: str):
        """ Writes the class or enum to a file """
        directory_path = os.path.join(self.output_dir, os.path.join(namespace.replace('.', os.sep)))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        file_path = os.path.join(directory_path, f"{name}.cs")

        with open(file_path, 'w', encoding='utf-8') as file:
            # Common using statements (add more as needed)
            file_content = "#pragma warning disable CS8618\n#pragma warning disable CS8603\n\nusing System;\nusing System.Collections.Generic;\n"
            if self.system_text_json_annotation:
                file_content += "using System.Text.Json.Serialization;\n"
            if self.newtonsoft_json_annotation:
                file_content += "using Newtonsoft.Json;\n"
            if self.avro_annotation:
                file_content += "using Avro;\nusing Avro.Specific;\n"
            # Namespace declaration with correct indentation for the definition
            file_content += f"\nnamespace {namespace}\n{{\n"
            indented_definition = '\n'.join([f"{INDENT}{line}" for line in definition.split('\n')])
            file_content += f"{indented_definition}\n}}"
            file.write(file_content)


    def convert_schema(self, schema: JsonNode, output_dir: str):
        """ Converts Avro schema to C# """
        if not isinstance(schema, list):
            schema = [schema]
        
        if not glob.glob(os.path.join(output_dir, '*.csproj')):
            csproj_file = os.path.join(output_dir, f"{os.path.basename(output_dir)}.csproj")
            if not os.path.exists(csproj_file):
                with open(csproj_file, 'w', encoding='utf-8') as file:
                    file.write(CSPROJ_CONTENT)
        self.output_dir = output_dir
        for avro_schema in (avs for avs in schema if isinstance(avs, dict)):
            self.generate_class_or_enum(avro_schema,'')

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