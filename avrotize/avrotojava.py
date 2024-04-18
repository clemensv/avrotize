# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

""" Generates Java classes from Avro schema """
import json
import os
from typing import Dict, List, Union

from avrotize.common import pascal

INDENT = '    '
POM_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>demo</artifactId>
    <version>1.0-SNAPSHOT</version>
    <properties>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro</artifactId>
            <version>1.11.3</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson</groupId>
            <artifactId>jackson-bom</artifactId>
            <version>2.17.0</version>
            <type>pom</type>
        </dependency>
    </dependencies>
</project>
"""

JSON_FROMDATA_THROWS = \
    ",JsonProcessingException"
JSON_FROMDATA = \
    """
if ( contentType == "application/json") {
    if (data instanceof JsonNode) {
        return (new ObjectMapper()).readValue(((JsonNode)data).toString(), {typeName}.class);
    }
    else if ( data instanceof String) {
        return (new ObjectMapper()).readValue(((String)data), {typeName}.class);
    }
    throw new UnsupportedOperationException("Data is not of a supported type for JSON conversion to {typeName}");
}
"""
JSON_TOBYTEARRAY_THROWS = ",JsonProcessingException"
JSON_TOBYTEARRAY = \
    """
if ( contentType == "application/json") {    
    return new ObjectMapper().writeValueAsBytes(this);
}
"""

AVRO_FROMDATA_THROWS = ",IOException"
AVRO_FROMDATA = \
    """
if ( contentType == "application/avro") {
    DatumReader<{typeName}> reader = new SpecificDatumReader<>({typeName}.class);
    Decoder decoder = DecoderFactory.get().binaryDecoder((byte[])data, null);
    return reader.read(null, decoder);
}
"""
AVRO_TOBYTEARRAY_THROWS = ",IOException"
AVRO_TOBYTEARRAY = \
    """
if ( contentType == "application/avro") {
    DatumWriter<{typeName}> writer = new SpecificDatumWriter<>({typeName}.class);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(this, encoder);
    encoder.flush();
    return out.toByteArray();
}
"""


JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None


def flatten_type_name(name: str) -> str:
    """Strips the namespace from a name"""
    base_name = pascal(name.replace(' ', '')).split(
        '.')[-1].replace('>', '').replace('<', '').replace(',', '')
    return base_name


def is_java_reserved_word(word: str) -> bool:
    """Checks if a word is a Java reserved word"""
    reserved_words = [
        'abstract', 'assert', 'boolean', 'break', 'byte', 'case', 'catch', 'char', 'class', 'const',
        'continue', 'default', 'do', 'double', 'else', 'enum', 'extends', 'final', 'finally', 'float',
        'for', 'goto', 'if', 'implements', 'import', 'instanceof', 'int', 'interface', 'long', 'native',
        'new', 'package', 'private', 'protected', 'public', 'return', 'short', 'static', 'strictfp',
        'super', 'switch', 'synchronized', 'this', 'throw', 'throws', 'transient', 'try', 'void', 'volatile',
        'while', 'true', 'false', 'null'
    ]
    return word in reserved_words


class AvroToJava:
    """Converts Avro schema to Java classes, including Jackson annotations and Avro SpecificRecord methods"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package.replace('.', '/')
        self.output_dir = os.getcwd()
        self.avro_annotation = False
        self.jackson_annotations = False
        self.pascal_properties = False

    def concat_package(self, package: str, name: str) -> str:
        """Concatenates package and name using a dot separator"""
        return f"{package}.{name}" if package else name

    class JavaType:
        """Java type definition"""

        def __init__(self, type_name: str, union_types: List['AvroToJava.JavaType'] | None = None):
            self.type_name = type_name
            self.union_types = union_types

    def map_primitive_to_java(self, avro_type: str) -> JavaType:
        """Maps Avro primitive types to Java types"""
        mapping = {
            'null': 'Void',
            'boolean': 'Boolean',
            'int': 'Integer',
            'long': 'Long',
            'float': 'Float',
            'double': 'Double',
            'bytes': 'byte[]',
            'string': 'String',
        }
        return AvroToJava.JavaType(mapping.get(avro_type, 'Object'))

    def convert_avro_type_to_java(self, avro_type: Union[str, Dict, List], parent_package: str) -> JavaType:
        """Converts Avro type to Java type"""
        if isinstance(avro_type, str):
            return self.map_primitive_to_java(avro_type)
        elif isinstance(avro_type, list):
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                return self.convert_avro_type_to_java(non_null_types[0], parent_package)
            else:
                types: List[AvroToJava.JavaType] = [self.convert_avro_type_to_java(
                    t, parent_package) for t in non_null_types]
                return AvroToJava.JavaType('Object', types)
        elif isinstance(avro_type, dict):
            if avro_type['type'] in ['record', 'enum']:
                return self.generate_class_or_enum(avro_type, parent_package, write_file=True)
            elif avro_type['type'] == 'array':
                return AvroToJava.JavaType(f"List<{self.convert_avro_type_to_java(avro_type['items'], parent_package).type_name}>")
            elif avro_type['type'] == 'map':
                return AvroToJava.JavaType(f"Map<String, {self.convert_avro_type_to_java(avro_type['values'], parent_package).type_name}>")
            return self.convert_avro_type_to_java(avro_type['type'], parent_package)
        return 'Object'

    def generate_class_or_enum(self, avro_schema: Dict, parent_package: str, write_file: bool = True) -> JavaType:
        """ Generates a Java class or enum from an Avro schema """
        if avro_schema['type'] == 'record':
            return self.generate_class(avro_schema, parent_package, write_file)
        elif avro_schema['type'] == 'enum':
            return self.generate_enum(avro_schema, parent_package, write_file)
        return AvroToJava.JavaType('Object')

    def generate_class(self, avro_schema: Dict, parent_package: str, write_file: bool) -> JavaType:
        """ Generates a Java class from an Avro record schema """
        class_definition = ''
        if 'doc' in avro_schema:
            class_definition += f"/** {avro_schema['doc']} */\n"
        package = self.concat_package(self.base_package, avro_schema.get(
            'namespace', parent_package).replace('.', '/')).lower()
        class_name = pascal(avro_schema['name'])
        fields_str = [self.generate_property(
            field, package) for field in avro_schema.get('fields', [])]
        class_body = "\n".join(fields_str)
        class_definition += f"public class {class_name}"
        if self.avro_annotation:
            class_definition += " implements SpecificRecord"
        class_definition += " {\n"
        class_definition += class_body
        if self.avro_annotation:
            avro_schema_json = json.dumps(avro_schema)
            avro_schema_json = avro_schema_json.replace('"', '§')
            avro_schema_json = f"\"+\n{INDENT}\"".join(
                [avro_schema_json[i:i+80] for i in range(0, len(avro_schema_json), 80)])
            avro_schema_json = avro_schema_json.replace('§', '\\"')
            class_definition += f"\n\n{INDENT}public static Schema AvroSchema = new Schema.Parser().parse(\n{INDENT}\"{avro_schema_json}\");\n"
            class_definition += f"\n{INDENT}@Override\n{INDENT}public Schema getSchema(){{ return AvroSchema; }}\n"
            class_definition += self.generate_get_method(
                avro_schema.get('fields', []), package)
            class_definition += self.generate_put_method(
                avro_schema.get('fields', []), package)

        # emit toByteArray method
        class_definition += f"\n\n{INDENT}public byte[] toByteArray(String contentType) throws UnsupportedOperationException" + \
            f"{ JSON_TOBYTEARRAY_THROWS if self.jackson_annotations else '' }" + \
            f"{ AVRO_TOBYTEARRAY_THROWS if self.avro_annotation else '' }  {{"
        if self.avro_annotation:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(
                AVRO_TOBYTEARRAY.strip().replace("{typeName}", class_name).split("\n"))
        if self.jackson_annotations:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(
                JSON_TOBYTEARRAY.strip().replace("{typeName}", class_name).split("\n"))
        class_definition += f"\n{INDENT*2}throw new UnsupportedOperationException(\"Unsupported content type \"+ contentType);\n{INDENT}}}"

        # emit fromData factory method
        class_definition += f"\n\n{INDENT}public static {class_name} fromData(Object data, String contentType) throws UnsupportedOperationException" + \
            f"{ JSON_FROMDATA_THROWS if self.jackson_annotations else '' }" + \
            f"{ AVRO_FROMDATA_THROWS if self.avro_annotation else '' }  {{"
        class_definition += f'\n{INDENT*2}if ( data instanceof {class_name}) return ({class_name})data;'
        if self.avro_annotation:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(
                AVRO_FROMDATA.strip().replace("{typeName}", class_name).split("\n"))
        if self.jackson_annotations:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(
                JSON_FROMDATA.strip().replace("{typeName}", class_name).split("\n"))
        class_definition += f"\n{INDENT*2}throw new UnsupportedOperationException(\"Unsupported content type \"+ contentType);\n{INDENT}}}"

        class_definition += "\n}"

        if write_file:
            self.write_to_file(package, class_name, class_definition)
        return AvroToJava.JavaType(self.concat_package(package.replace('/', '.'), class_name))

    def generate_get_method(self, fields: List[Dict], parent_package: str) -> str:
        """ Generates the get method for SpecificRecord """
        get_method = f"\n{INDENT}@Override\n{INDENT}public Object get(int field$) {{\n"
        get_method += f"{INDENT * 2}switch (field$) {{\n"
        for index, field in enumerate(fields):
            field_name = pascal(
                field['name']) if self.pascal_properties else field['name']
            get_method += f"{INDENT * 3}case {index}: return this.{field_name};\n"
        get_method += f"{INDENT * 3}default: throw new AvroRuntimeException(\"Bad index: \" + field$);\n"
        get_method += f"{INDENT * 2}}}\n{INDENT}}}\n"
        return get_method

    def generate_put_method(self, fields: List[Dict], parent_package: str) -> str:
        """ Generates the put method for SpecificRecord """
        put_method = f"\n{INDENT}@Override\n{INDENT}public void put(int field$, Object value$) {{\n"
        put_method += f"{INDENT * 2}switch (field$) {{\n"
        for index, field in enumerate(fields):
            field_name = pascal(
                field['name']) if self.pascal_properties else field['name']
            java_type = self.convert_avro_type_to_java(
                field['type'], parent_package)
            put_method += f"{INDENT * 3}case {index}: this.{field_name} = ({java_type.type_name})value$; break;\n"
        put_method += f"{INDENT * 3}default: throw new AvroRuntimeException(\"Bad index: \" + field$);\n"
        put_method += f"{INDENT * 2}}}\n{INDENT}}}\n"
        return put_method

    def generate_enum(self, avro_schema: Dict, parent_package: str, write_file: bool) -> JavaType:
        """ Generates a Java enum from an Avro enum schema """
        enum_definition = ''
        if 'doc' in avro_schema:
            enum_definition += f"/** {avro_schema['doc']} */\n"
        package = self.concat_package(self.base_package, avro_schema.get(
            'namespace', parent_package).replace('.', '/')).lower()
        enum_name = pascal(avro_schema['name'])
        symbols = avro_schema.get('symbols', [])
        symbols_str = ', '.join(symbols)
        enum_definition += f"public enum {enum_name} {{\n"
        enum_definition += f"{INDENT}{symbols_str};\n"
        enum_definition += "}\n"
        if write_file:
            self.write_to_file(package, enum_name, enum_definition)
        return AvroToJava.JavaType(self.concat_package(package.replace('/', '.'), enum_name))

    def generate_property(self, field: Dict, parent_package: str) -> str:
        """ Generates a Java property definition """
        field_type = self.convert_avro_type_to_java(
            field['type'], parent_package)
        field_name = pascal(
            field['name']) if self.pascal_properties else field['name']
        if is_java_reserved_word(field_name):
            field_name += "_"
        property_def = ''
        if 'doc' in field:
            property_def += f"{INDENT}/** {field['doc']} */\n"
        if self.jackson_annotations:
            property_def += f"{INDENT}@JsonProperty(\"{field['name']}\")\n"
        property_def += f"{INDENT}private {field_type.type_name} {field_name};\n"
        property_def += f"{INDENT}public {field_type.type_name} get{field_name.capitalize()}() {{ return {field_name}; }}\n"
        property_def += f"{INDENT}public void set{field_name.capitalize()}({field_type.type_name} {field_name}) {{ this.{field_name} = {field_name}; }}\n"
        if field_type.union_types:
            for union_type in field_type.union_types:
                property_def += f"{INDENT}public {union_type.type_name} get{field_name.capitalize()}As{flatten_type_name(union_type.type_name)}() {{ return ({union_type.type_name}){field_name}; }}\n"
                property_def += f"{INDENT}public void set{field_name.capitalize()}As{flatten_type_name(union_type.type_name)}({union_type.type_name} {field_name}) {{ this.{field_name} = {field_name}; }}\n"
        return property_def

    def write_to_file(self, package: str, name: str, definition: str):
        """ Writes a Java class or enum to a file """
        directory_path = os.path.join(self.output_dir, package.replace('.', os.sep))
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{name}.java")

        with open(file_path, 'w', encoding='utf-8') as file:
            if package:
                file.write(f"package {package.replace('/', '.')};\n\n")
                if "List<" in definition:
                    file.write("import java.util.List;\n")
                if "Map<" in definition:
                    file.write("import java.util.Map;\n")
            if self.avro_annotation:
                file.write("import org.apache.avro.io.*;\n")
                file.write("import org.apache.avro.specific.*;\n")
                file.write("import org.apache.avro.*;\n")
                file.write("import java.io.IOException;\n")
                file.write("import java.io.ByteArrayOutputStream;\n")
            if self.jackson_annotations:
                file.write("import com.fasterxml.jackson.databind.JsonNode;\n")
                file.write("import com.fasterxml.jackson.databind.ObjectMapper;\n")
                file.write("import com.fasterxml.jackson.annotation.JsonProperty;\n")
                file.write("import com.fasterxml.jackson.core.JsonProcessingException;\n")
            file.write("\n")
            file.write(definition)

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts Avro schema to Java"""
        if not isinstance(schema, list):
            schema = [schema]
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        pom_path = os.path.join(output_dir, "pom.xml")
        if not os.path.exists(pom_path):
            with open(pom_path, 'w', encoding='utf-8') as file:
                file.write(POM_CONTENT)
        output_dir = os.path.join(
            output_dir, "src/main/java".replace('/', os.sep))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        for avro_schema in (x for x in schema if isinstance(x, dict)):
            self.generate_class_or_enum(avro_schema, self.base_package)

    def convert(self, avro_schema_path: str, output_dir: str):
        """Converts Avro schema to Java"""
        with open(avro_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        self.convert_schema(schema, output_dir)


def convert_avro_to_java(avro_schema_path, java_file_path, package_name='', pascal_properties=False, jackson_annotation=False, avro_annotation=False):
    """_summary_

    Converts Avro schema to C# classes

    Args:
        avro_schema_path (_type_): Avro input schema path  
        cs_file_path (_type_): Output C# file path 
    """
    avrotojava = AvroToJava()
    avrotojava.base_package = package_name
    avrotojava.pascal_properties = pascal_properties
    avrotojava.avro_annotation = avro_annotation
    avrotojava.jackson_annotations = jackson_annotation
    avrotojava.convert(avro_schema_path, java_file_path)


def convert_avro_schema_to_java(avro_schema: JsonNode, output_dir: str, package_name='', pascal_properties=False, jackson_annotation=False, avro_annotation=False):
    """_summary_

    Converts Avro schema to C# classes

    Args:
        avro_schema (_type_): Avro schema as a dictionary or list of dictionaries
        output_dir (_type_): Output directory path 
    """
    avrotojava = AvroToJava()
    avrotojava.base_package = package_name
    avrotojava.pascal_properties = pascal_properties
    avrotojava.avro_annotation = avro_annotation
    avrotojava.jackson_annotations = jackson_annotation
    avrotojava.convert_schema(avro_schema, output_dir)
