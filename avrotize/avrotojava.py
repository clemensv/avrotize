# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

""" Generates Java classes from Avro schema """
import json
import os
from typing import Dict, List, Tuple, Union

from avrotize.common import pascal, camel, is_generic_avro_type

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

PREAMBLE_TOBYTEARRAY = \
"""
byte[] result = null;
String mediaType = contentType.split(";")[0].trim().toLowerCase();
"""


EPILOGUE_TOBYTEARRAY_COMPRESSION = \
    """
if (result != null && mediaType.endsWith("+gzip")) {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
        gzipOutputStream.write(result);
        gzipOutputStream.finish();
        result = byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
        throw new UnsupportedOperationException("Error compressing data to gzip");
    }
}
"""

EPILOGUE_TOBYTEARRAY = \
"""
throw new UnsupportedOperationException("Unsupported media type + mediaType");
"""

PREAMBLE_FROMDATA_COMPRESSION = \
"""
if (mediaType.endsWith("+gzip")) {
    InputStream stream = null;
    
    if (data instanceof InputStream) {
        stream = (InputStream) data;
    } else if (data instanceof byte[]) {
        stream = new ByteArrayInputStream((byte[]) data);
    } else {
        throw new UnsupportedOperationException("Data is not of a supported type for gzip decompression");
    }
    
    try (InputStream gzipStream = new GZIPInputStream(stream);
         ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[1024];
        int bytesRead;
        while ((bytesRead = gzipStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
        }
        data = outputStream.toByteArray();
    } catch (IOException e) {
        e.printStackTrace();
    }
}
"""


JSON_FROMDATA_THROWS = \
    ",JsonProcessingException"
JSON_FROMDATA = \
    """
if ( mediaType == "application/json") {
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
if ( mediaType == "application/json") {    
    result = new ObjectMapper().writeValueAsBytes(this);
}
"""

AVRO_FROMDATA_THROWS = ",IOException"
AVRO_FROMDATA = \
    """
if ( mediaType == "application/avro") {
    DatumReader<{typeName}> reader = new SpecificDatumReader<>({typeName}.class);
    Decoder decoder = DecoderFactory.get().binaryDecoder((byte[])data, null);
    return reader.read(null, decoder);
}
"""


AVRO_TOBYTEARRAY_THROWS = ",IOException"
AVRO_TOBYTEARRAY = \
    """
if ( mediaType == "application/avro") {
    DatumWriter<{typeName}> writer = new SpecificDatumWriter<>({typeName}.class);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(this, encoder);
    encoder.flush();
    result = out.toByteArray();
}
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


class AvroToJava:
    """Converts Avro schema to Java classes, including Jackson annotations and Avro SpecificRecord methods"""

    def __init__(self, base_package: str = '') -> None:
        self.base_package = base_package.replace('.', '/')
        self.output_dir = os.getcwd()
        self.avro_annotation = False
        self.jackson_annotations = False
        self.pascal_properties = False
        self.generated_types: Dict[str,str] = {}

    def concat_package(self, package: str, name: str) -> str:
        """Concatenates package and name using a dot separator"""
        return f"{package.lower()}.{name}" if package else name

    class JavaType:
        """Java type definition"""

        def __init__(self, type_name: str, union_types: List['AvroToJava.JavaType'] | None = None, is_class: bool = False, is_enum: bool = False) -> None:
            self.type_name = type_name
            self.union_types = union_types
            self.is_class = is_class
            self.is_enum = is_enum

    def safe_identifier(self, name: str) -> str:
        """Converts a name to a safe Java identifier"""
        if is_java_reserved_word(name):
            return f"_{name}"
        return name
    
    def map_primitive_to_java(self, avro_type: str, is_optional: bool) -> JavaType:
        """Maps Avro primitive types to Java types"""
        optional_mapping = {
            'null': 'Void',
            'boolean': 'Boolean',
            'int': 'Integer',
            'long': 'Long',
            'float': 'Float',
            'double': 'Double',
            'bytes': 'byte[]',
            'string': 'String',
        }
        required_mapping = {
            'null': 'void',
            'boolean': 'boolean',
            'int': 'int',
            'long': 'long',
            'float': 'float',
            'double': 'double',
            'bytes': 'byte[]',
            'string': 'String',
        }
        if '.' in avro_type:
            type_name = avro_type.split('.')[-1]
            package_name = '.'.join(avro_type.split('.')[:-1]).lower()
            avro_type = self.concat_package(package_name, type_name)
        if avro_type in self.generated_types:
            kind = self.generated_types[avro_type]
            return AvroToJava.JavaType(avro_type, is_class=kind=="class", is_enum=kind=="enum")
        else:
            return AvroToJava.JavaType(required_mapping.get(avro_type, avro_type) if not is_optional else optional_mapping.get(avro_type, avro_type))
    
    def is_java_primitive(self, java_type: JavaType) -> bool:
        """Checks if a Java type is a primitive type"""
        return java_type.type_name in [
            'void', 'boolean', 'int', 'long', 'float', 'double', 'byte[]', 'String',
            'Boolean', 'Integer', 'Long', 'Float', 'Double', 'Void']
        
    def is_java_optional_type(self, java_type: JavaType) -> bool:
        """Checks if a Java type is an optional type"""
        return java_type.type_name in ['Void', 'Boolean', 'Integer', 'Long', 'Float', 'Double']
        
    def is_java_numeric_type(self, java_type: JavaType) -> bool:
        """Checks if a Java type is a numeric type"""
        return java_type.type_name in ['int', 'long', 'float', 'double', 'Integer', 'Long', 'Float', 'Double']
    
    def is_java_integer_type(self, java_type: JavaType) -> bool:
        """Checks if a Java type is an integer type"""
        return java_type.type_name in ['int', 'long', 'Integer', 'Long']

    def convert_avro_type_to_java(self, class_name: str, field_name: str, avro_type: Union[str, Dict, List], parent_package: str, nullable: bool = False) -> JavaType:
        """Converts Avro type to Java type"""
        if isinstance(avro_type, str):
            return self.map_primitive_to_java(avro_type, nullable)
        elif isinstance(avro_type, list):
            if (is_generic_avro_type(avro_type)):
                return AvroToJava.JavaType('Object')
            non_null_types = [t for t in avro_type if t != 'null']
            if len(non_null_types) == 1:
                if isinstance(non_null_types[0], str):
                    return self.map_primitive_to_java(non_null_types[0], True)
                else:
                    return self.convert_avro_type_to_java(class_name, field_name, non_null_types[0], parent_package)
            else:
                if self.jackson_annotations:
                    return AvroToJava.JavaType(self.generate_embedded_union_class_jackson(class_name, field_name, non_null_types, parent_package, write_file=True), is_class=True)
                else:
                    types: List[AvroToJava.JavaType] = [self.convert_avro_type_to_java(
                        class_name, field_name, t, parent_package) for t in non_null_types]
                    return AvroToJava.JavaType('Object', types)
        elif isinstance(avro_type, dict):
            if avro_type['type'] in ['record', 'enum']:
                return self.generate_class_or_enum(avro_type, parent_package, write_file=True)
            elif avro_type['type'] == 'fixed' or avro_type['type'] == 'bytes' and 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'decimal':
                    return AvroToJava.JavaType('BigDecimal')
            elif avro_type['type'] == 'array':
                item_type = self.convert_avro_type_to_java(class_name, field_name, avro_type['items'], parent_package, nullable=True).type_name
                return AvroToJava.JavaType(f"List<{item_type}>")
            elif avro_type['type'] == 'map':
                values_type = self.convert_avro_type_to_java(class_name, field_name, avro_type['values'], parent_package, nullable=True).type_name
                return AvroToJava.JavaType(f"Map<String,{values_type}>")
            elif 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'date':
                    return AvroToJava.JavaType('LocalDate')
                elif avro_type['logicalType'] == 'time-millis' or avro_type['logicalType'] == 'time-micros':
                    return AvroToJava.JavaType('LocalTime')
                elif avro_type['logicalType'] == 'timestamp-millis' or avro_type['logicalType'] == 'timestamp-micros':
                    return AvroToJava.JavaType('Instant')
                elif avro_type['logicalType'] == 'local-timestamp-millis' or avro_type['logicalType'] == 'local-timestamp-micros':
                    return AvroToJava.JavaType('LocalDateTime')
                elif avro_type['logicalType'] == 'uuid':
                    return AvroToJava.JavaType('UUID')
                elif avro_type['logicalType'] == 'duration':
                    return AvroToJava.JavaType('Duration')
            return self.convert_avro_type_to_java(class_name, field_name, avro_type['type'], parent_package)
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
        package = self.concat_package(self.base_package, avro_schema.get('namespace', parent_package)).replace('.', '/').lower()
        class_name = self.safe_identifier(avro_schema['name'])
        self.generated_types[self.concat_package(avro_schema.get('namespace', parent_package),avro_schema['name'])] = "class"
        fields_str = [self.generate_property(class_name,
            field, avro_schema.get('namespace', parent_package)) for field in avro_schema.get('fields', [])]
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
            class_definition += self.generate_get_method(avro_schema.get('fields', []), package)
            class_definition += self.generate_put_method(class_name, avro_schema.get('fields', []), package)

        # emit toByteArray method
        class_definition += f"\n\n{INDENT}/**\n{INDENT} * Converts the object to a byte array\n{INDENT} * @param contentType the content type of the byte array\n{INDENT} * @return the byte array\n{INDENT} */\n"
        class_definition += f"\n\n{INDENT}public byte[] toByteArray(String contentType) throws UnsupportedOperationException" + \
            f"{ JSON_TOBYTEARRAY_THROWS if self.jackson_annotations else '' }" + \
            f"{ AVRO_TOBYTEARRAY_THROWS if self.avro_annotation else '' }  {{"
        if self.jackson_annotations or self.avro_annotation:
            class_definition += f'\n{INDENT*2}'.join((PREAMBLE_TOBYTEARRAY).split("\n"))
        if self.avro_annotation:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(
                AVRO_TOBYTEARRAY.strip().replace("{typeName}", class_name).split("\n"))
        if self.jackson_annotations:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(
                JSON_TOBYTEARRAY.strip().replace("{typeName}", class_name).split("\n"))
        if self.avro_annotation or self.jackson_annotations:
            class_definition += f'\n{INDENT*2}'.join(EPILOGUE_TOBYTEARRAY_COMPRESSION.split("\n"))
            class_definition += f'\n{INDENT*2}if ( result != null ) {{ return result; }}'        
        class_definition += (f'\n{INDENT*2}'.join((EPILOGUE_TOBYTEARRAY.strip()).split("\n")))+f"\n{INDENT}}}"

        # emit fromData factory method
        class_definition += f"\n\n{INDENT}/**\n{INDENT} * Converts the data to an object\n{INDENT} * @param data the data to convert\n{INDENT} * @param contentType the content type of the data\n{INDENT} * @return the object\n{INDENT} */\n"
        class_definition += f"\n\n{INDENT}public static {class_name} fromData(Object data, String contentType) throws UnsupportedOperationException" + \
            f"{ JSON_FROMDATA_THROWS if self.jackson_annotations else '' }" + \
            f"{ AVRO_FROMDATA_THROWS if self.avro_annotation else '' }  {{"
        class_definition += f'\n{INDENT*2}if ( data instanceof {class_name}) return ({class_name})data;'
        
        if self.avro_annotation or self.jackson_annotations:
            class_definition += f'\n{INDENT*2}String mediaType = contentType.split(";")[0].trim().toLowerCase();'
            class_definition += f'\n{INDENT*2}'.join((PREAMBLE_FROMDATA_COMPRESSION).split("\n"))
        if self.avro_annotation:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(
                AVRO_FROMDATA.strip().replace("{typeName}", class_name).split("\n"))
        if self.jackson_annotations:
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(
                JSON_FROMDATA.strip().replace("{typeName}", class_name).split("\n"))
        class_definition += f"\n{INDENT*2}throw new UnsupportedOperationException(\"Unsupported media type \"+ contentType);\n{INDENT}}}"
        
        if self.jackson_annotations:
            class_definition += self.create_is_json_match_method(avro_schema, avro_schema.get('namespace', parent_package), class_name)

        class_definition += "\n}"

        if write_file:
            self.write_to_file(package, class_name, class_definition)
        type_name = self.concat_package(package.replace('/', '.'), class_name)
        return AvroToJava.JavaType(type_name, is_class=True)
    
    def create_is_json_match_method(self, avro_schema, parent_namespace, class_name) -> str:
        """ Generates the isJsonMatch method for Jackson """
        predicates = ''
        class_definition = ''
        class_definition += f"\n\n{INDENT}/**\n{INDENT} * Checks if the JSON node matches the schema\n{INDENT}"
        class_definition += f"\n{INDENT}@param node The JSON node to check */"
        class_definition += f"\n{INDENT}public static boolean isJsonMatch(com.fasterxml.jackson.databind.JsonNode node)\n{INDENT}{{"
        field_defs = ''
        
        field_count = 0
        for field in avro_schema.get('fields', []):
            if field_count > 0:
                field_defs += f" && \n{INDENT*3}"
            field_count += 1
            field_name = field['name']
            if field_name == class_name:
                field_name += "_"
            field_type = self.convert_avro_type_to_java(class_name, field_name, field['type'], parent_namespace)
            predicate, clause = self.get_is_json_match_clause(class_name, field_name, field_type)
            field_defs += clause
            if predicate:
                predicates += predicate + "\n"
        if ( len(predicates) > 0 ):
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(predicates.split('\n'))
        class_definition += f"\n{INDENT*2}return {field_defs}"
        class_definition += f";\n{INDENT}}}"
        return class_definition
    
    def get_is_json_match_clause(self, class_name: str, field_name: str, field_type: JavaType) -> Tuple[str, str]:
        """ Generates the isJsonMatch clause for a field using Jackson """
        class_definition = ''
        predicates = ''
        field_name_js = field_name
        is_optional = self.is_java_optional_type(field_type)        

        if is_optional:
            node_check = f"!node.has(\"{field_name_js}\") || node.get(\"{field_name_js}\").isNull() || node.get(\"{field_name_js}\")"
        else:
            node_check = f"node.has(\"{field_name_js}\") && node.get(\"{field_name_js}\")"

        if field_type.type_name == 'byte[]':
            class_definition += f"({node_check}.isBinary())"
        elif field_type.type_name == 'string' or field_type.type_name == 'String':
            class_definition += f"({node_check}.isTextual())"
        elif field_type.type_name == 'int' or field_type.type_name == 'Integer':
            class_definition += f"({node_check}.canConvertToInt())"
        elif field_type.type_name == 'long' or field_type.type_name == 'Long':
            class_definition += f"({node_check}.canConvertToLong())"
        elif field_type.type_name == 'float' or field_type.type_name == 'Float':
            class_definition += f"({node_check}.isFloat())"
        elif field_type.type_name == 'double' or field_type.type_name == 'Double':
            class_definition += f"({node_check}.isDouble())"
        elif field_type.type_name == 'BigDecimal':
            class_definition += f"({node_check}.isBigDecimal())"
        elif field_type.type_name == 'boolean' or field_type.type_name == 'Boolean':
            class_definition += f"({node_check}.isBoolean())"
        elif field_type.type_name == 'UUID':
            class_definition += f"({node_check}.isTextual())"
        elif field_type.type_name == 'LocalDate':
            class_definition += f"({node_check}.isTextual())"
        elif field_type.type_name == 'LocalTime':
            class_definition += f"({node_check}.isTextual())"
        elif field_type.type_name == 'Instant':
            class_definition += f"({node_check}.isTextual())"
        elif field_type.type_name == 'LocalDateTime':
            class_definition += f"({node_check}.isTextual())"
        elif field_type.type_name == 'Duration':
            class_definition += f"({node_check}.isTextual())"
        elif field_type.type_name == "Object":
            class_definition += f"({node_check}.isObject())"   
        elif field_type.type_name.startswith("List<"):
            items_type = field_type.type_name[5:-1]
            pred = f"Predicate<JsonNode> val{field_name_js} = (JsonNode n) -> n.isArray() && !n.elements().hasNext() || "
            pred_test = self.predicate_test(items_type)
            if pred_test:
                pred += "n.elements().next()" + pred_test
            elif items_type in self.generated_types:
                kind = self.generated_types[items_type]
                if kind == "enum":
                    pred += f"n.elements().next().isTextual() && Enum.valueOf({items_type}.class, n.elements().next().asText()) != null"
                else:
                    pred += f"{items_type}.isJsonMatch(n.elements().next())"
            else:
                pred += "true"
            predicates += pred + ";"
            class_definition += f"(node.has(\"{field_name_js}\") && val{field_name_js}.test(node.get(\"{field_name_js}\")))"
        elif field_type.type_name.startswith("Map<"):
            comma_offset = field_type.type_name.find(',')+1
            values_type = field_type.type_name[comma_offset:-1]
            pred = f"Predicate<JsonNode> val{field_name_js} = (JsonNode n) -> n.isObject() && !n.elements().hasNext() || "
            pred_test = self.predicate_test(values_type)
            if pred_test:
                pred += "n.elements().next()" + pred_test
            elif values_type in self.generated_types:
                kind = self.generated_types[values_type]
                if kind == "enum":
                    pred += f"n.elements().next().isTextual() && Enum.valueOf({values_type}.class, n.elements().next().asText()) != null"
                else:
                    pred += f"{values_type}.isJsonMatch(n.elements().next())"
            else:
                pred += "true"
            predicates += pred + ";"
            class_definition += f"(node.has(\"{field_name_js}\") && val{field_name_js}.test(node.get(\"{field_name_js}\")))"
        elif field_type.is_class:
            class_definition += f"(node.has(\"{field_name_js}\") && {field_type.type_name}.isJsonMatch(node.get(\"{field_name_js}\")))"
        elif field_type.is_enum:
            class_definition += f"(node.get(\"{field_name_js}\").isTextual() && Enum.valueOf({field_type.type_name}.class, node.get(\"{field_name_js}\").asText()) != null)"
        else:
            is_union = False
            field_union = pascal(field_name) + 'Union'
            if field_type == field_union:
                field_union = class_name + "." + pascal(field_name) + 'Union'
                type_kind = self.generated_types[field_union] if field_union in self.generated_types else "class"
                if type_kind == "union":
                    is_union = True
                    class_definition += f"({node_check}.isObject() && {field_type.type_name}.isJsonMatch(node.get(\"{field_name_js}\")))"
            if not is_union:
                class_definition += f"(node.has(\"{field_name_js}\"))"
        return predicates, class_definition

    def predicate_test(self, items_type):
        """ Generates the predicate test for a list or map"""
        if items_type == "String":
            return ".isTextual()"
        elif items_type in ['int', 'Integer']:
            return ".canConvertToInt()"
        elif items_type in ['long', 'Long']:
            return ".canConvertToLong()"
        elif items_type in ['float', 'Float', 'double', 'Double', 'decimal']:
            return ".isNumber()"
        elif items_type in ['boolean', 'Boolean']:
            return ".isBoolean()"
        elif items_type == 'byte[]':
            return ".isBinary()"
        elif items_type == 'UUID':
            return ".isTextual()"
        elif items_type == 'LocalDate':
            return ".isTextual()"
        elif items_type == 'LocalTime':
            return ".isTextual()"
        elif items_type == 'Instant':
            return ".isTextual()"
        elif items_type == 'LocalDateTime':
            return ".isTextual()"
        elif items_type == 'Duration':
            return ".isTextual()"
        elif items_type == "Object":
            return ".isObject()"
        return ""
    
    def get_is_json_match_clause_type(self, element_name: str, class_name: str, field_type: JavaType) -> str:
        """ Generates the isJsonMatch clause for a field using Jackson """
        predicates = ''
        class_definition = ''
        is_optional = field_type.type_name[-1] == '?'
        #is_optional = field_type[-1] == '?'
        #field_type = field_type[:-1] if is_optional else field_type
        is_optional = False
        node_check = f"{element_name}.isMissingNode() == false && {element_name}"
        null_check = f"{element_name}.isNull()" if is_optional else "false"
        if field_type.type_name == 'byte[]':
            class_definition += f"({node_check}.isBinary()){f' || {null_check}' if is_optional else ''}"
        elif field_type.type_name == 'String':
            class_definition += f"({node_check}.isTextual()){f' || {null_check}' if is_optional else ''}"
        elif self.is_java_numeric_type(field_type):
            class_definition += f"({node_check}.isNumber()){f' || {null_check}' if is_optional else ''}"
        elif field_type.type_name == 'bool' or field_type.type_name == 'Boolean':
            class_definition += f"({node_check}.isBoolean()){f' || {null_check}' if is_optional else ''}"
        elif field_type.type_name.startswith("List<"):
            items_type = field_type.type_name[5:-1]
            predicates += f"Predicate<JsonNode> val{element_name}. = (JsonNode n) -> n.isObject() && n.fields().hasNext() && n.fields().next().getValue().isTextual();"
            class_definition += f"({node_check}.isArray()){f' || {null_check}' if is_optional else ''}"
        elif field_type.type_name.startswith("Map<"):
            values_type = field_type.type_name[4:-1]
            class_definition += f"({node_check}.isObject()){f' || {null_check}' if is_optional else ''}"
        elif field_type.is_class:
            class_definition += f"({null_check} || {field_type.type_name}.isJsonMatch({element_name}))"
        elif field_type.is_enum:
            class_definition += f"({null_check} || ({node_check}.isTextual() && Enum.valueOf({field_type.type_name}.class, {element_name}.asText()) != null))"
        else:
            is_union = False
            field_union = pascal(element_name) + 'Union'
            if field_type == field_union:
                field_union = class_name + "." + pascal(element_name) + 'Union'
                type_kind = self.generated_types[field_union] if field_union in self.generated_types else "class"
                if type_kind == "union":
                    is_union = True
                    class_definition += f"({null_check} || {field_type}.isJsonMatch({element_name}))"
            if not is_union:
                class_definition += f"({node_check}.isObject()){f' || {null_check}' if is_optional else ''}"

        return class_definition

    def generate_get_method(self, fields: List[Dict], parent_package: str) -> str:
        """ Generates the get method for SpecificRecord """
        get_method = f"\n{INDENT}@Override\n{INDENT}public Object get(int field$) {{\n"
        get_method += f"{INDENT * 2}switch (field$) {{\n"
        for index, field in enumerate(fields):
            field_name = pascal(field['name']) if self.pascal_properties else field['name']
            field_name = self.safe_identifier(field_name)
            get_method += f"{INDENT * 3}case {index}: return this.{field_name};\n"
        get_method += f"{INDENT * 3}default: throw new AvroRuntimeException(\"Bad index: \" + field$);\n"
        get_method += f"{INDENT * 2}}}\n{INDENT}}}\n"
        return get_method

    def generate_put_method(self, class_name: str, fields: List[Dict], parent_package: str) -> str:
        """ Generates the put method for SpecificRecord """
        suppress_unchecked = False
        put_method = f"\n{INDENT}@Override\n{INDENT}public void put(int field$, Object value$) {{\n"
        put_method += f"{INDENT * 2}switch (field$) {{\n"
        for index, field in enumerate(fields):
            field_name = pascal(field['name']) if self.pascal_properties else field['name']
            field_name = self.safe_identifier(field_name)
            java_type = self.convert_avro_type_to_java(class_name, field_name, field['type'], parent_package)
            if java_type.type_name.startswith("List<") or java_type.type_name.startswith("Map<"):
                suppress_unchecked = True
            put_method += f"{INDENT * 3}case {index}: this.{field_name} = ({java_type.type_name})value$; break;\n"
        put_method += f"{INDENT * 3}default: throw new AvroRuntimeException(\"Bad index: \" + field$);\n"
        put_method += f"{INDENT * 2}}}\n{INDENT}}}\n"
        if suppress_unchecked:
            put_method = f"\n{INDENT}@SuppressWarnings(\"unchecked\"){put_method}"
        return put_method

    def generate_enum(self, avro_schema: Dict, parent_package: str, write_file: bool) -> JavaType:
        """ Generates a Java enum from an Avro enum schema """
        enum_definition = ''
        if 'doc' in avro_schema:
            enum_definition += f"/** {avro_schema['doc']} */\n"
        enum_name = self.safe_identifier(avro_schema['name'])
        self.generated_types[self.concat_package(avro_schema.get('namespace', parent_package),avro_schema['name'])] = "enum"
        package = self.concat_package(self.base_package, avro_schema.get('namespace', parent_package)).replace('.', '/').lower()        
        symbols = avro_schema.get('symbols', [])
        symbols_str = ', '.join(symbols)
        enum_definition += f"public enum {enum_name} {{\n"
        enum_definition += f"{INDENT}{symbols_str};\n"
        enum_definition += "}\n"
        if write_file:
            self.write_to_file(package, enum_name, enum_definition)
        type_name = self.concat_package(package.replace('/', '.'), enum_name)
        return AvroToJava.JavaType(type_name, is_enum=True)
    
    def generate_embedded_union_class_jackson(self, class_name: str, field_name: str, avro_type: List, parent_namespace: str, write_file: bool) -> str:
        """ Generates an embedded Union Class for Java using Jackson """
        class_definition_ctors = class_definition_decls = class_definition_read = class_definition_write = class_definition = ''
        list_is_json_match: List[str] = []
        union_class_name = class_name + pascal(field_name) + 'Union'
        union_types: List[AvroToJava.JavaType] = [self.convert_avro_type_to_java(class_name, field_name + "Option" + str(i), t, parent_namespace) for i, t in enumerate(avro_type)]
        for i, union_type in enumerate(union_types):
            # we need the nullable version (wrapper) of all primitive types
            if self.is_java_primitive(union_type):
                union_type = self.map_primitive_to_java(union_type.type_name, True)
            union_variable_name = union_type.type_name
            is_dict = is_list = False
            if union_type.type_name.startswith("Map<"):
                # handle Map types
                is_dict = True
                # find the comma
                union_variable_name = flatten_type_name(union_type.type_name)
            elif union_type.type_name.startswith("List<"):
                # handle List types
                is_list = True
                union_variable_name = flatten_type_name(union_type.type_name)
            elif union_type.type_name == "byte[]":
                union_variable_name = "Bytes"
            else:
                union_variable_name = union_type.type_name.rsplit('.', 1)[-1]
            
            union_variable_name = self.safe_identifier(union_variable_name)

            # Constructor for each type
            class_definition_ctors += \
                f"{INDENT*1}public {union_class_name}({union_type.type_name} {union_variable_name}) {{\n{INDENT*2}this._{camel(union_variable_name)} = {union_variable_name};\n{INDENT*1}}}\n"

            # Declarations
            class_definition_decls += \
                f"{INDENT*1}private {union_type.type_name} _{camel(union_variable_name)};\n" + \
                f"{INDENT*1}public {union_type.type_name} get{union_variable_name}() {{ return _{camel(union_variable_name)}; }}\n";

            # Read method logic
            if is_dict:
                class_definition_read += f"{INDENT*3}if (node.isObject()) {{\n{INDENT*4}{union_type.type_name} map = mapper.readValue(node.toString(), new TypeReference<{union_type.type_name}>(){{}});\n{INDENT*3}return new {union_class_name}(map);\n{INDENT*3}}}\n"
            elif is_list:
                class_definition_read += f"{INDENT*3}if (node.isArray()) {{\n{INDENT*4}{union_type.type_name} list = mapper.readValue(node.toString(), new TypeReference<{union_type.type_name}>(){{}});\n{INDENT*4}return new {union_class_name}(list);\n{INDENT*3}}}\n"
            elif self.is_java_primitive(union_type):
                if union_type.type_name == "String":
                    class_definition_read += f"{INDENT*3}if (node.isTextual()) {{\n{INDENT*4}return new {union_class_name}(node.asText());\n{INDENT*3}}}\n"
                elif union_type.type_name == "byte[]":
                    class_definition_read += f"{INDENT*3}if (node.isBinary()) {{\n{INDENT*4}return new {union_class_name}(node.binaryValue());\n{INDENT*3}}}\n"
                elif union_type.type_name in ["int", "Int"]:
                    class_definition_read += f"{INDENT*3}if (node.canConvertToInt()) {{\n{INDENT*4}return new {union_class_name}(node.asInt());\n{INDENT*3}}}\n"
                elif union_type.type_name in ["long", "Long"]:
                    class_definition_read += f"{INDENT*3}if (node.canConvertToLong()) {{\n{INDENT*4}return new {union_class_name}(node.asLong());\n{INDENT*3}}}\n"
                elif union_type.type_name in ["float", "Float"]:
                    class_definition_read += f"{INDENT*3}if (node.isFloat()) {{\n{INDENT*4}return new {union_class_name}(node.floatValue());\n{INDENT*3}}}\n"
                elif union_type.type_name in ["double", "Double"]:
                    class_definition_read += f"{INDENT*3}if (node.isDouble()) {{\n{INDENT*4}return new {union_class_name}(node.doubleValue());\n{INDENT*3}}}\n"
                elif union_type.type_name == "decimal":
                    class_definition_read += f"{INDENT*3}if (node.isBigDecimal()) {{\n{INDENT*4}return new {union_class_name}(node.decimalValue());\n{INDENT*3}}}\n"
                elif union_type.type_name in ["boolean", "Boolean"]:
                    class_definition_read += f"{INDENT*3}if (node.isBoolean()) {{\n{INDENT*4}return new {union_class_name}(node.asBoolean());\n{INDENT*3}}}\n"
            else:
                if union_type.is_enum:
                    class_definition_read += f"{INDENT*3}if (node.isTextual()) {{\n{INDENT*4}return new {union_class_name}(Enum.valueOf({union_type.type_name}.class, node.asText()));\n{INDENT*3}}}\n"
                else:
                    class_definition_read += f"{INDENT*3}if (node.isObject() && {union_type.type_name}.isJsonMatch(node)) {{\n{INDENT*4}return new {union_class_name}(mapper.readValue(node.toString(), {union_type.type_name}.class));\n{INDENT*3}}}\n"
                
            # Write method logic
            class_definition_write += f"{INDENT*3}{union_type.type_name} {camel(union_variable_name)}Value = value.get{union_variable_name}();\n{INDENT*3}if ({camel(union_variable_name)}Value != null) {{\n{INDENT*4}generator.writeObject({camel(union_variable_name)}Value);\n{INDENT*3}}}\n"

            # JSON match method logic
            gij = self.get_is_json_match_clause_type("node", class_name, union_type)
            if gij:
                list_is_json_match.append(gij)

        class_definition =  f"@JsonSerialize(using = {union_class_name}.Serializer.class)\n"
        class_definition += f"@JsonDeserialize(using = {union_class_name}.Deserializer.class)\n"
        class_definition += f"public class {union_class_name} {{\n"
        class_definition += class_definition_decls
        class_definition += f"{INDENT}public " + union_class_name + "() {}\n"
        class_definition += class_definition_ctors
        class_definition += f"{INDENT}public static class Serializer extends JsonSerializer<" + union_class_name + "> {\n"
        class_definition += f"{INDENT*2}@Override\n"
        class_definition += f"{INDENT*2}public void serialize(" + union_class_name + " value, JsonGenerator generator, SerializerProvider serializers) throws IOException {\n"
        class_definition += class_definition_write
        class_definition += f"{INDENT*3}throw new UnsupportedOperationException(\"No record type is set in the union\");\n"
        class_definition += f"{INDENT*2}}}\n{INDENT}}}\n"
        class_definition += f"{INDENT}public static class Deserializer extends JsonDeserializer<" + union_class_name + "> {\n"
        class_definition += f"{INDENT*2}@Override\n"
        class_definition += f"{INDENT*2}public " + union_class_name + " deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {\n"
        class_definition += f"{INDENT*3}ObjectMapper mapper = (ObjectMapper) p.getCodec();\n"
        class_definition += f"{INDENT*3}JsonNode node = mapper.readTree(p);\n"
        class_definition += class_definition_read
        class_definition += f"{INDENT*3}throw new UnsupportedOperationException(\"No record type matched the JSON data\");\n"
        class_definition += f"{INDENT*2}}}\n{INDENT}}}\n"
        class_definition += f"{INDENT*1}public static boolean isJsonMatch(JsonNode node) {{\n"
        class_definition += f"{INDENT*2}return " + " || ".join(list_is_json_match) + ";\n"
        class_definition += f"{INDENT*1}}}\n}}\n"

        if write_file:
            self.write_to_file(parent_namespace, union_class_name, class_definition)
        self.generated_types[union_class_name] = "union"  # Track union types
        return union_class_name


    def generate_property(self, class_name: str, field: Dict, parent_package: str) -> str:
        """ Generates a Java property definition """
        field_name = pascal(field['name']) if self.pascal_properties else field['name']
        field_type = self.convert_avro_type_to_java(class_name, field_name, field['type'], parent_package)
        safe_field_name = self.safe_identifier(field_name)
        property_def = ''
        if 'doc' in field:
            property_def += f"{INDENT}/** {field['doc']} */\n"
        if self.jackson_annotations:
            property_def += f"{INDENT}@JsonProperty(\"{field['name']}\")\n"
        property_def += f"{INDENT}private {field_type.type_name} {safe_field_name};\n"
        property_def += f"{INDENT}public {field_type.type_name} get{pascal(field_name)}() {{ return {safe_field_name}; }}\n"
        property_def += f"{INDENT}public void set{pascal(field_name)}({field_type.type_name} {safe_field_name}) {{ this.{safe_field_name} = {safe_field_name}; }}\n"
        if field_type.union_types:
            for union_type in field_type.union_types:
                if union_type.type_name.startswith("List<") or union_type.type_name.startswith("Map<"):
                    property_def += f"{INDENT}@SuppressWarnings(\"unchecked\")\n"
                property_def += f"{INDENT}public {union_type.type_name} get{pascal(field_name)}As{flatten_type_name(union_type.type_name)}() {{ return ({union_type.type_name}){safe_field_name}; }}\n"
                property_def += f"{INDENT}public void set{pascal(field_name)}As{flatten_type_name(union_type.type_name)}({union_type.type_name} {safe_field_name}) {{ this.{safe_field_name} = {safe_field_name}; }}\n"
        return property_def

    def write_to_file(self, package: str, name: str, definition: str):
        """ Writes a Java class or enum to a file """
        package = package.lower()
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
                if "Map<" in definition:
                    file.write("import java.util.Map;\n")
                if "Predicate<" in definition:
                    file.write("import java.util.function.Predicate;\n")
                if "BigDecimal" in definition:
                    file.write("import java.math.BigDecimal;\n")
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
                    
            if self.avro_annotation:
                if 'AvroRuntimeException' in definition:
                    file.write("import org.apache.avro.AvroRuntimeException;\n")
                if 'Schema' in definition:
                    file.write("import org.apache.avro.Schema;\n")
                if 'DatumReader' in definition:
                    file.write("import org.apache.avro.io.DatumReader;\n")
                if 'DatumWriter' in definition:
                    file.write("import org.apache.avro.io.DatumWriter;\n")
                if 'DecoderFactory' in definition:
                    file.write("import org.apache.avro.io.DecoderFactory;\n")
                if 'EncoderFactory' in definition:
                    file.write("import org.apache.avro.io.EncoderFactory;\n")
                if 'SpecificDatumReader' in definition:
                    file.write("import org.apache.avro.specific.SpecificDatumReader;\n")
                if 'SpecificDatumWriter' in definition:
                    file.write("import org.apache.avro.specific.SpecificDatumWriter;\n")
                if 'SpecificRecord' in definition:
                    file.write("import org.apache.avro.specific.SpecificRecord;\n")
                if 'Encoder' in definition:
                    file.write("import org.apache.avro.io.Encoder;\n")
                if 'Decoder' in definition:
                    file.write("import org.apache.avro.io.Decoder;\n")            
            if self.jackson_annotations:
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
                if 'JsonProperty' in definition:
                    file.write("import com.fasterxml.jackson.annotation.JsonProperty;\n")
                if 'JsonProcessingException' in definition:
                    file.write("import com.fasterxml.jackson.core.JsonProcessingException;\n")
                if 'JsonGenerator' in definition:
                    file.write("import com.fasterxml.jackson.core.JsonGenerator;\n")
                if 'TypeReference' in definition:
                    file.write("import com.fasterxml.jackson.core.type.TypeReference;\n")
            if self.avro_annotation or self.jackson_annotations:
                if 'GZIPOutputStream' in definition:
                    file.write("import java.util.zip.GZIPOutputStream;\n")
                if 'GZIPInputStream' in definition:
                    file.write("import java.util.zip.GZIPInputStream;\n")
                if 'ByteArrayInputStream' in definition:
                    file.write("import java.io.ByteArrayInputStream;\n")
                if "ByteArrayOutputStream" in definition:
                    file.write("import java.io.ByteArrayOutputStream;\n")
                if "InputStream" in definition:
                    file.write("import java.io.InputStream;\n")
                if "IOException" in definition:
                    file.write("import java.io.IOException;\n")
                if "InflaterInputStream" in definition:
                    file.write("import java.util.zip.InflaterInputStream;\n")                
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
