# pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements, line-too-long

""" Generates Java classes from Avro schema """
import json
import os
from typing import Dict, List, Tuple, Union
from avrotize.constants import (AVRO_VERSION, JACKSON_VERSION, JDK_VERSION, 
                                  JUNIT_VERSION, MAVEN_COMPILER_VERSION, MAVEN_SUREFIRE_VERSION)

from avrotize.common import pascal, camel, is_generic_avro_type, inline_avro_references, build_flat_type_dict

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
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
    <dependencies>
        <dependency>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro</artifactId>
            <version>{AVRO_VERSION}</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-core</artifactId>
            <version>{JACKSON_VERSION}</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>{JACKSON_VERSION}</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-annotations</artifactId>
            <version>{JACKSON_VERSION}</version>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-api</artifactId>
            <version>{JUNIT_VERSION}</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter-engine</artifactId>
            <version>{JUNIT_VERSION}</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>{MAVEN_COMPILER_VERSION}</version>
                <configuration>
                    <compilerArgs>
                        <arg>-Xmaxerrs</arg>
                        <arg>1000</arg>
                    </compilerArgs>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>{MAVEN_SUREFIRE_VERSION}</version>
                <configuration>
                    <useSystemClassLoader>false</useSystemClassLoader>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
"""

PREAMBLE_TOBYTEARRAY = \
"""
byte[] result = null;
String mediaType = contentType.split(";")[0].trim().toLowerCase();
boolean shouldCompress = mediaType.endsWith("+gzip");
if (shouldCompress) {
    mediaType = mediaType.substring(0, mediaType.length() - 5);
}
"""


EPILOGUE_TOBYTEARRAY_COMPRESSION = \
    """
if (result != null && shouldCompress) {
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
    mediaType = mediaType.substring(0, mediaType.length() - 5);
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
    ",JsonProcessingException, IOException"
JSON_FROMDATA = \
    """
if ( mediaType.equals("application/json")) {
    if (data instanceof byte[]) {
        ByteArrayInputStream stream = new ByteArrayInputStream((byte[]) data);
        return (new ObjectMapper()).readValue(stream, {typeName}.class);
    }
    else if (data instanceof InputStream) {
        return (new ObjectMapper()).readValue((InputStream)data, {typeName}.class);
    }
    else if (data instanceof JsonNode) {
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
if ( mediaType.equals("application/json")) {    
    result = new ObjectMapper().writeValueAsBytes(this);
}
"""

AVRO_FROMDATA_THROWS = ",IOException"
AVRO_FROMDATA = \
    """
if ( mediaType.equals("avro/binary") || mediaType.equals("application/vnd.apache.avro+avro")) {
    if (data instanceof byte[]) {
        return AVROREADER.read(new {typeName}(), DecoderFactory.get().binaryDecoder((byte[])data, null));
    } else if (data instanceof InputStream) {
        return AVROREADER.read(new {typeName}(), DecoderFactory.get().binaryDecoder((InputStream)data, null));
    }
    throw new UnsupportedOperationException("Data is not of a supported type for Avro conversion to {typeName}");
} else if ( mediaType.equals("avro/json") || mediaType.equals("application/vnd.apache.avro+json")) {
    if (data instanceof byte[]) {
        return AVROREADER.read(new {typeName}(), DecoderFactory.get().jsonDecoder({typeName}.AVROSCHEMA, new ByteArrayInputStream((byte[])data)));
    } else if (data instanceof InputStream) {
        return AVROREADER.read(new {typeName}(), DecoderFactory.get().jsonDecoder({typeName}.AVROSCHEMA, (InputStream)data));
    } else if (data instanceof String) {
        return AVROREADER.read(new {typeName}(), DecoderFactory.get().jsonDecoder({typeName}.AVROSCHEMA, (String)data));
    }
    throw new UnsupportedOperationException("Data is not of a supported type for Avro conversion to {typeName}");
}
"""


AVRO_TOBYTEARRAY_THROWS = ",IOException"
AVRO_TOBYTEARRAY = \
    """
if ( mediaType.equals("avro/binary") || mediaType.equals("application/vnd.apache.avro+avro")) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    AVROWRITER.write(this, encoder);
    encoder.flush();
    result = out.toByteArray();
}
else if ( mediaType.equals("avro/json") || mediaType.equals("application/vnd.apache.avro+json")) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().jsonEncoder({typeName}.AVROSCHEMA, out);
    AVROWRITER.write(this, encoder);
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
        self.generated_types_avro_namespace: Dict[str,str] = {}
        self.generated_types_java_package: Dict[str,str] = {}
        self.generated_avro_schemas: Dict[str, Dict] = {}
        self.discriminated_unions: Dict[str, List[Dict]] = {}  # Maps union name to list of subtype schemas

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

        def __init__(self, type_name: str, union_types: List['AvroToJava.JavaType'] | None = None, is_class: bool = False, is_enum: bool = False) -> None:
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
            avro_type = self.qualified_name(package_name, type_name)
        if avro_type in self.generated_types_avro_namespace:
            kind = self.generated_types_avro_namespace[avro_type]
            qualified_class_name = self.qualified_name(self.base_package, avro_type)
            return AvroToJava.JavaType(qualified_class_name, is_class=kind=="class", is_enum=kind=="enum")
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
            elif avro_type['type'] == 'fixed':
                if 'logicalType' in avro_type and avro_type['logicalType'] == 'decimal':
                    return AvroToJava.JavaType('BigDecimal')
                return AvroToJava.JavaType('byte[]')
            elif avro_type['type'] == 'bytes' and 'logicalType' in avro_type:
                if avro_type['logicalType'] == 'decimal':
                    return AvroToJava.JavaType('BigDecimal')
            elif avro_type['type'] == 'array':
                item_java_type = self.convert_avro_type_to_java(class_name, field_name, avro_type['items'], parent_package, nullable=True)
                item_type = item_java_type.type_name
                # Check if item is a union type by name pattern or registered type
                is_union_item = (item_type.endswith("Union") or 
                                (item_type in self.generated_types_java_package and self.generated_types_java_package[item_type] == "union"))
                if is_union_item:
                    return AvroToJava.JavaType(f"List<{item_type}>", union_types=[AvroToJava.JavaType(item_type)])
                return AvroToJava.JavaType(f"List<{item_type}>")
            elif avro_type['type'] == 'map':
                value_java_type = self.convert_avro_type_to_java(class_name, field_name, avro_type['values'], parent_package, nullable=True)
                values_type = value_java_type.type_name
                # Check if value is a union type by name pattern or registered type
                is_union_value = (values_type.endswith("Union") or
                                 (values_type in self.generated_types_java_package and self.generated_types_java_package[values_type] == "union"))
                if is_union_value:
                    return AvroToJava.JavaType(f"Map<String,{values_type}>", union_types=[AvroToJava.JavaType(values_type)])
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

    def generate_create_test_instance_method(self, class_name: str, fields: List[Dict], parent_package: str) -> str:
        """ Generates a static createTestInstance method that creates a fully initialized instance """
        method = f"\n{INDENT}/**\n{INDENT} * Creates a test instance with all required fields populated\n{INDENT} * @return a fully initialized test instance\n{INDENT} */\n"
        method += f"{INDENT}public static {class_name} createTestInstance() {{\n"
        method += f"{INDENT*2}{class_name} instance = new {class_name}();\n"
        
        for field in fields:
            # Skip const fields
            if "const" in field:
                continue
                
            # Match the logic in generate_property: field_name is already Pascal-cased if needed
            field_name = pascal(field['name']) if self.pascal_properties else field['name']
            safe_field_name = self.safe_identifier(field_name, class_name)
            field_type = self.convert_avro_type_to_java(class_name, safe_field_name, field['type'], parent_package)
            
            # Get a test value for this field
            test_value = self.get_test_value(field_type.type_name, parent_package.replace('.', '/'))
            
            # Setter name matches generate_property: set{pascal(field_name)} where field_name is already potentially Pascal-cased
            method += f"{INDENT*2}instance.set{pascal(field_name)}({test_value});\n"
        
        method += f"{INDENT*2}return instance;\n"
        method += f"{INDENT}}}\n"
        return method

    def generate_class(self, avro_schema: Dict, parent_package: str, write_file: bool) -> JavaType:
        """ Generates a Java class from an Avro record schema """
        class_definition = ''
        if 'doc' in avro_schema:
            class_definition += f"/** {avro_schema['doc']} */\n"
        namespace = avro_schema.get('namespace', parent_package)
        if not 'namespace' in avro_schema:
            avro_schema['namespace'] = namespace
        package = self.join_packages(self.base_package, namespace).replace('.', '/').lower()
        package = package.replace('.', '/').lower()
        package = self.safe_package(package)
        class_name = self.safe_identifier(avro_schema['name'])
        namespace_qualified_name = self.qualified_name(namespace,avro_schema['name'])
        qualified_class_name = self.qualified_name(package.replace('/', '.'), class_name)
        if namespace_qualified_name in self.generated_types_avro_namespace:
            return AvroToJava.JavaType(qualified_class_name, is_class=True)
        self.generated_types_avro_namespace[namespace_qualified_name] = "class"
        self.generated_types_java_package[qualified_class_name] = "class"
        self.generated_avro_schemas[qualified_class_name] = avro_schema
        
        # Track discriminated union subtypes
        if 'union' in avro_schema:
            union_name = avro_schema['union']
            if union_name not in self.discriminated_unions:
                self.discriminated_unions[union_name] = []
            self.discriminated_unions[union_name].append({
                'schema': avro_schema,
                'class_name': class_name,
                'package': package.replace('/', '.'),
                'qualified_name': qualified_class_name
            })
        
        fields_str = [self.generate_property(class_name, field, namespace) for field in avro_schema.get('fields', [])]
        class_body = "\n".join(fields_str)
        class_definition += f"public class {class_name}"
        
        # Add extends clause if this is a discriminated union subtype
        if 'union' in avro_schema and self.jackson_annotations:
            union_name = avro_schema['union']
            class_definition += f" extends {union_name}"
        
        if self.avro_annotation:
            class_definition += " implements SpecificRecord"
        class_definition += " {\n"
        class_definition += f"{INDENT}public {class_name}() {{}}\n"
        class_definition += class_body

        if self.avro_annotation:
            class_definition += f"\n{INDENT}public {class_name}(GenericData.Record record) {{\n"
            class_definition += f"{INDENT*2}for( int i = 0; i < record.getSchema().getFields().size(); i++ ) {{\n"
            class_definition += f"{INDENT*3}this.put(i, record.get(i));\n"
            class_definition += f"{INDENT*2}}}\n"
            class_definition += f"{INDENT}}}\n"

        # Generate createTestInstance() method for testing
        class_definition += self.generate_create_test_instance_method(class_name, avro_schema.get('fields', []), namespace)

        if self.avro_annotation:
            # Inline all schema references like C# does - each class has self-contained schema
            local_avro_schema = inline_avro_references(avro_schema.copy(), self.type_dict, '')
            avro_schema_json = json.dumps(local_avro_schema)
            
            # Java has a limit of 65535 bytes for string constants
            # If the schema is too large, we need to split it into chunks
            MAX_STRING_CONSTANT_LENGTH = 60000  # Leave some margin for safety
            
            if len(avro_schema_json) > MAX_STRING_CONSTANT_LENGTH:
                # Split into multiple private string methods to avoid the 65535 byte limit
                # Each method returns a part of the schema, concatenated at runtime
                chunk_size = MAX_STRING_CONSTANT_LENGTH
                chunks = [avro_schema_json[i:i+chunk_size] for i in range(0, len(avro_schema_json), chunk_size)]
                
                # Generate a method for each chunk
                for i, chunk in enumerate(chunks):
                    # Use the same escaping technique as the non-chunked version
                    escaped_chunk = chunk.replace('"', '§')
                    escaped_chunk = f"\"+\n{INDENT*2}\"".join(
                        [escaped_chunk[j:j+80] for j in range(0, len(escaped_chunk), 80)])
                    escaped_chunk = escaped_chunk.replace('§', '\\"')
                    class_definition += f"\n\n{INDENT}private static String getAvroSchemaPart{i}() {{\n"
                    class_definition += f"{INDENT*2}return \"{escaped_chunk}\";\n"
                    class_definition += f"{INDENT}}}"
                
                # Generate the combining method
                class_definition += f"\n\n{INDENT}private static String getAvroSchemaJson() {{\n"
                class_definition += f"{INDENT*2}return "
                class_definition += " + ".join([f"getAvroSchemaPart{i}()" for i in range(len(chunks))])
                class_definition += ";\n"
                class_definition += f"{INDENT}}}\n"
                class_definition += f"\n{INDENT}public static final Schema AVROSCHEMA = new Schema.Parser().parse(getAvroSchemaJson());"
            else:
                avro_schema_json = avro_schema_json.replace('"', '§')
                avro_schema_json = f"\"+\n{INDENT}\"".join(
                    [avro_schema_json[i:i+80] for i in range(0, len(avro_schema_json), 80)])
                avro_schema_json = avro_schema_json.replace('§', '\\"')
                class_definition += f"\n\n{INDENT}public static final Schema AVROSCHEMA = new Schema.Parser().parse(\n{INDENT}\"{avro_schema_json}\");"
            
            # Store the schema for tracking
            avro_namespace = avro_schema.get('namespace', '')
            schema_full_name = f"{avro_namespace}.{class_name}" if avro_namespace else class_name
            self.generated_types_avro_namespace[schema_full_name] = "class"
            
            class_definition += f"\n{INDENT}public static final DatumWriter<{class_name}> AVROWRITER = new SpecificDatumWriter<{class_name}>(AVROSCHEMA);"
            class_definition += f"\n{INDENT}public static final DatumReader<{class_name}> AVROREADER = new SpecificDatumReader<{class_name}>(AVROSCHEMA);\n"

            if self.jackson_annotations:
                class_definition += f"\n{INDENT}@JsonIgnore"
            class_definition += f"\n{INDENT}@Override\n{INDENT}public Schema getSchema(){{ return AVROSCHEMA; }}\n"
            class_definition += self.generate_avro_get_method(class_name, avro_schema.get('fields', []), namespace)
            class_definition += self.generate_avro_put_method(class_name, avro_schema.get('fields', []), namespace)

        # emit toByteArray method
        class_definition += f"\n\n{INDENT}/**\n{INDENT} * Converts the object to a byte array\n{INDENT} * @param contentType the content type of the byte array\n{INDENT} * @return the byte array\n{INDENT} */\n"
        class_definition += f"{INDENT}public byte[] toByteArray(String contentType) throws UnsupportedOperationException" + \
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
        class_definition += f"{INDENT}public static {class_name} fromData(Object data, String contentType) throws UnsupportedOperationException" + \
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
            class_definition += self.create_is_json_match_method(avro_schema, avro_schema.get('namespace', namespace), class_name)

        # Add equals() and hashCode() methods
        class_definition += self.generate_equals_method(class_name, avro_schema.get('fields', []), namespace)
        class_definition += self.generate_hashcode_method(class_name, avro_schema.get('fields', []), namespace)

        class_definition += "\n}"

        if write_file:
            self.write_to_file(package, class_name, class_definition)
        return AvroToJava.JavaType(qualified_class_name, is_class=True)
    
    def create_is_json_match_method(self, avro_schema, parent_namespace, class_name) -> str:
        """ Generates the isJsonMatch method for a class using Jackson """
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
            predicate, clause = self.get_is_json_match_clause(class_name, field_name, field_type, field)
            field_defs += clause
            if predicate:
                predicates += predicate + "\n"
        if ( len(predicates) > 0 ):
            class_definition += f'\n{INDENT*2}'+f'\n{INDENT*2}'.join(predicates.split('\n'))
        class_definition += f"\n{INDENT*2}return {field_defs}"
        class_definition += f";\n{INDENT}}}"
        return class_definition
    
    def get_is_json_match_clause(self, class_name: str, field_name: str, field_type: JavaType, field: Dict = None) -> Tuple[str, str]:
        """ Generates the isJsonMatch clause for a field using Jackson """
        class_definition = ''
        predicates = ''
        field_name_js = field_name
        
        # Check if field is nullable (Avro union with null)
        is_nullable = False
        if field and 'type' in field:
            avro_type = field['type']
            if isinstance(avro_type, list) and 'null' in avro_type:
                is_nullable = True
        
        is_optional = is_nullable or self.is_java_optional_type(field_type)
        
        # Check if this is a const field (e.g., discriminator)
        has_const = field and 'const' in field and field['const'] is not None
        const_value = field['const'] if has_const else None

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
            class_definition += f"({node_check}.isNumber())"
        elif field_type.type_name == 'double' or field_type.type_name == 'Double':
            class_definition += f"({node_check}.isNumber())"
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
            elif items_type in self.generated_types_java_package:
                kind = self.generated_types_java_package[items_type]
                if kind == "enum":
                    # Try to match the incoming text against Avro symbols
                    pred += f"n.elements().next().isTextual() && java.util.Arrays.stream({items_type}.values()).anyMatch(e -> e.avroSymbol().equals(n.elements().next().asText()))"
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
            elif values_type in self.generated_types_java_package:
                kind = self.generated_types_java_package[values_type]
                if kind == "enum":
                    # Try to match the incoming text against Avro symbols
                    pred += f"n.elements().next().isTextual() && java.util.Arrays.stream({values_type}.values()).anyMatch(e -> e.avroSymbol().equals(n.elements().next().asText()))"
                else:
                    pred += f"{values_type}.isJsonMatch(n.elements().next())"
            else:
                pred += "true"
            predicates += pred + ";"
            class_definition += f"(node.has(\"{field_name_js}\") && val{field_name_js}.test(node.get(\"{field_name_js}\")))"
        elif field_type.is_class:
            if is_optional:
                class_definition += f"(!node.has(\"{field_name_js}\") || node.get(\"{field_name_js}\").isNull() || {field_type.type_name}.isJsonMatch(node.get(\"{field_name_js}\")))"
            else:
                class_definition += f"(node.has(\"{field_name_js}\") && {field_type.type_name}.isJsonMatch(node.get(\"{field_name_js}\")))"
        elif field_type.is_enum:
            # For const enum fields (discriminators), check the exact value
            if has_const:
                # const_value is the string value from the schema, not the enum qualified name
                # Ensure we use the raw string value for comparison
                raw_const = const_value if isinstance(const_value, str) else str(const_value)
                class_definition += f"(node.has(\"{field_name_js}\") && node.get(\"{field_name_js}\").isTextual() && node.get(\"{field_name_js}\").asText().equals(\"{raw_const}\"))"
            else:
                # Try to match the incoming text against Avro symbols
                class_definition += f"(node.get(\"{field_name_js}\").isTextual() && java.util.Arrays.stream({field_type.type_name}.values()).anyMatch(e -> e.avroSymbol().equals(node.get(\"{field_name_js}\").asText())))"
        else:
            is_union = False
            field_union = pascal(field_name) + 'Union'
            if field_type == field_union:
                field_union = class_name + "." + pascal(field_name) + 'Union'
                type_kind = self.generated_types_avro_namespace[field_union] if field_union in self.generated_types_avro_namespace else "class"
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
            # Try to match the incoming text against Avro symbols
            class_definition += f"({null_check} || ({node_check}.isTextual() && java.util.Arrays.stream({field_type.type_name}.values()).anyMatch(e -> e.avroSymbol().equals({element_name}.asText()))))"
        else:
            is_union = False
            field_union = pascal(element_name) + 'Union'
            if field_type == field_union:
                field_union = class_name + "." + pascal(element_name) + 'Union'
                type_kind = self.generated_types_avro_namespace[field_union] if field_union in self.generated_types_avro_namespace else "class"
                if type_kind == "union":
                    is_union = True
                    class_definition += f"({null_check} || {field_type}.isJsonMatch({element_name}))"
            if not is_union:
                class_definition += f"({node_check}.isObject()){f' || {null_check}' if is_optional else ''}"

        return class_definition

    def generate_equals_method(self, class_name: str, fields: List[Dict], parent_package: str) -> str:
        """ Generates the equals method for a class """
        equals_method = f"\n\n{INDENT}@Override\n{INDENT}public boolean equals(Object obj) {{\n"
        equals_method += f"{INDENT * 2}if (this == obj) return true;\n"
        equals_method += f"{INDENT * 2}if (obj == null || getClass() != obj.getClass()) return false;\n"
        equals_method += f"{INDENT * 2}{class_name} other = ({class_name}) obj;\n"
        
        if not fields:
            equals_method += f"{INDENT * 2}return true;\n"
        else:
            for index, field in enumerate(fields):
                field_name = pascal(field['name']) if self.pascal_properties else field['name']
                field_name = self.safe_identifier(field_name, class_name)
                field_type = self.convert_avro_type_to_java(class_name, field_name, field['type'], parent_package)
                
                if field_type.type_name in ['int', 'long', 'float', 'double', 'boolean', 'byte', 'short', 'char']:
                    equals_method += f"{INDENT * 2}if (this.{field_name} != other.{field_name}) return false;\n"
                elif field_type.type_name == 'byte[]':
                    equals_method += f"{INDENT * 2}if (!java.util.Arrays.equals(this.{field_name}, other.{field_name})) return false;\n"
                else:
                    equals_method += f"{INDENT * 2}if (this.{field_name} == null ? other.{field_name} != null : !this.{field_name}.equals(other.{field_name})) return false;\n"
            
            equals_method += f"{INDENT * 2}return true;\n"
        
        equals_method += f"{INDENT}}}\n"
        return equals_method

    def generate_hashcode_method(self, class_name: str, fields: List[Dict], parent_package: str) -> str:
        """ Generates the hashCode method for a class """
        hashcode_method = f"\n{INDENT}@Override\n{INDENT}public int hashCode() {{\n"
        
        if not fields:
            hashcode_method += f"{INDENT * 2}return 0;\n"
        else:
            hashcode_method += f"{INDENT * 2}int result = 1;\n"
            temp_counter = 0
            for field in fields:
                field_name = pascal(field['name']) if self.pascal_properties else field['name']
                field_name = self.safe_identifier(field_name, class_name)
                field_type = self.convert_avro_type_to_java(class_name, field_name, field['type'], parent_package)
                
                if field_type.type_name == 'boolean':
                    hashcode_method += f"{INDENT * 2}result = 31 * result + (this.{field_name} ? 1 : 0);\n"
                elif field_type.type_name in ['byte', 'short', 'char', 'int']:
                    hashcode_method += f"{INDENT * 2}result = 31 * result + this.{field_name};\n"
                elif field_type.type_name == 'long':
                    hashcode_method += f"{INDENT * 2}result = 31 * result + (int)(this.{field_name} ^ (this.{field_name} >>> 32));\n"
                elif field_type.type_name == 'float':
                    hashcode_method += f"{INDENT * 2}result = 31 * result + Float.floatToIntBits(this.{field_name});\n"
                elif field_type.type_name == 'double':
                    temp_var = f"temp{temp_counter}" if temp_counter > 0 else "temp"
                    temp_counter += 1
                    hashcode_method += f"{INDENT * 2}long {temp_var} = Double.doubleToLongBits(this.{field_name});\n"
                    hashcode_method += f"{INDENT * 2}result = 31 * result + (int)({temp_var} ^ ({temp_var} >>> 32));\n"
                elif field_type.type_name == 'byte[]':
                    hashcode_method += f"{INDENT * 2}result = 31 * result + java.util.Arrays.hashCode(this.{field_name});\n"
                else:
                    hashcode_method += f"{INDENT * 2}result = 31 * result + (this.{field_name} != null ? this.{field_name}.hashCode() : 0);\n"
            
            hashcode_method += f"{INDENT * 2}return result;\n"
        
        hashcode_method += f"{INDENT}}}\n"
        return hashcode_method

    def generate_union_equals_method(self, union_class_name: str, union_types: List['AvroToJava.JavaType']) -> str:
        """ Generates the equals method for a union class """
        equals_method = f"\n{INDENT}@Override\n{INDENT}public boolean equals(Object obj) {{\n"
        equals_method += f"{INDENT * 2}if (this == obj) return true;\n"
        equals_method += f"{INDENT * 2}if (obj == null || getClass() != obj.getClass()) return false;\n"
        equals_method += f"{INDENT * 2}{union_class_name} other = ({union_class_name}) obj;\n"
        
        # In a union, only ONE field should be set at a time
        # We need to check if the same field is set in both objects and if the values match
        for i, union_type in enumerate(union_types):
            # we need the nullable version (wrapper) of all primitive types
            if self.is_java_primitive(union_type):
                union_type = self.map_primitive_to_java(union_type.type_name, True)
            
            union_variable_name = union_type.type_name
            if union_type.type_name.startswith("Map<"):
                union_variable_name = flatten_type_name(union_type.type_name)
            elif union_type.type_name.startswith("List<"):
                union_variable_name = flatten_type_name(union_type.type_name)
            elif union_type.type_name == "byte[]":
                union_variable_name = "Bytes"
            else:
                union_variable_name = union_type.type_name.rsplit('.', 1)[-1]
            
            field_name = f"_{camel(union_variable_name)}"
            
            # Check if this field is set in this object
            if i == 0:
                equals_method += f"{INDENT * 2}if (this.{field_name} != null) {{\n"
            else:
                equals_method += f"{INDENT * 2}else if (this.{field_name} != null) {{\n"
            
            # If set, check if it's also set in the other object with the same value
            if union_type.type_name == 'byte[]':
                equals_method += f"{INDENT * 3}return java.util.Arrays.equals(this.{field_name}, other.{field_name});\n"
            else:
                equals_method += f"{INDENT * 3}return this.{field_name}.equals(other.{field_name});\n"
            
            equals_method += f"{INDENT * 2}}}\n"
        
        # If no field is set in this, check other is also unset
        equals_method += f"{INDENT * 2}// Both are null/unset - check other is also unset\n"
        equals_method += f"{INDENT * 2}return "
        for i, union_type in enumerate(union_types):
            # we need the nullable version (wrapper) of all primitive types
            if self.is_java_primitive(union_type):
                union_type = self.map_primitive_to_java(union_type.type_name, True)
            
            union_variable_name = union_type.type_name
            if union_type.type_name.startswith("Map<"):
                union_variable_name = flatten_type_name(union_type.type_name)
            elif union_type.type_name.startswith("List<"):
                union_variable_name = flatten_type_name(union_type.type_name)
            elif union_type.type_name == "byte[]":
                union_variable_name = "Bytes"
            else:
                union_variable_name = union_type.type_name.rsplit('.', 1)[-1]
            field_name = f"_{camel(union_variable_name)}"
            if i > 0:
                equals_method += " && "
            equals_method += f"other.{field_name} == null"
        equals_method += ";\n"
        equals_method += f"{INDENT}}}\n"
        return equals_method

    def generate_union_hashcode_method(self, union_class_name: str, union_types: List['AvroToJava.JavaType']) -> str:
        """ Generates the hashCode method for a union class """
        hashcode_method = f"\n{INDENT}@Override\n{INDENT}public int hashCode() {{\n"
        
        # In a union, only ONE field should be set at a time
        # Return the hash of whichever field is set
        for i, union_type in enumerate(union_types):
            # we need the nullable version (wrapper) of all primitive types
            if self.is_java_primitive(union_type):
                union_type = self.map_primitive_to_java(union_type.type_name, True)
            
            union_variable_name = union_type.type_name
            if union_type.type_name.startswith("Map<"):
                union_variable_name = flatten_type_name(union_type.type_name)
            elif union_type.type_name.startswith("List<"):
                union_variable_name = flatten_type_name(union_type.type_name)
            elif union_type.type_name == "byte[]":
                union_variable_name = "Bytes"
            else:
                union_variable_name = union_type.type_name.rsplit('.', 1)[-1]
            
            field_name = f"_{camel(union_variable_name)}"
            
            # Return hash of whichever field is set
            if i == 0:
                hashcode_method += f"{INDENT * 2}if (this.{field_name} != null) {{\n"
            else:
                hashcode_method += f"{INDENT * 2}else if (this.{field_name} != null) {{\n"
            
            # Use proper hash calculation based on type
            if union_type.type_name == 'byte[]':
                hashcode_method += f"{INDENT * 3}return java.util.Arrays.hashCode(this.{field_name});\n"
            else:
                hashcode_method += f"{INDENT * 3}return this.{field_name}.hashCode();\n"
            
            hashcode_method += f"{INDENT * 2}}}\n"
        
        # If no field is set, return 0
        hashcode_method += f"{INDENT * 2}return 0;\n"
        hashcode_method += f"{INDENT}}}\n"
        return hashcode_method

    def generate_avro_get_method(self, class_name: str, fields: List[Dict], parent_package: str) -> str:
        """ Generates the get method for SpecificRecord """
        get_method = f"\n{INDENT}@Override\n{INDENT}public Object get(int field$) {{\n"
        get_method += f"{INDENT * 2}switch (field$) {{\n"
        for index, field in enumerate(fields):
            field_name = pascal(field['name']) if self.pascal_properties else field['name']
            field_name = self.safe_identifier(field_name, class_name)
            field_type = self.convert_avro_type_to_java(class_name, field_name, field['type'], parent_package)
            
            # Check if field type is a union
            is_union = field_type.type_name in self.generated_types_avro_namespace and self.generated_types_avro_namespace[field_type.type_name] == "union"
            is_union = is_union or (field_type.type_name in self.generated_types_java_package and self.generated_types_java_package[field_type.type_name] == "union")
            # Also check if it's an Object with union_types (non-Jackson union)
            is_union = is_union or (field_type.type_name == "Object" and field_type.union_types is not None and len(field_type.union_types) > 1)
            
            # Check if field is List<Union> or Map<String, Union>
            is_list_of_unions = field_type.type_name.startswith("List<") and field_type.union_types and len(field_type.union_types) > 0
            is_map_of_unions = field_type.type_name.startswith("Map<") and field_type.union_types and len(field_type.union_types) > 0
            
            # For union fields, return the unwrapped object using toObject()
            # This allows Avro's SpecificDatumWriter to serialize the actual value (String, Integer, etc.)
            # instead of trying to serialize our custom wrapper class
            # The put() method will wrap it back using new UnionType(value$)
            if is_union:
                get_method += f"{INDENT * 3}case {index}: return this.{field_name} != null ? this.{field_name}.toObject() : null;\n"
            elif is_list_of_unions:
                # For List<Union>, unwrap each element by calling toObject() on it
                # Avro will deserialize this as List<Object> which put() will rewrap
                get_method += f"{INDENT * 3}case {index}: return this.{field_name} != null ? this.{field_name}.stream().map(u -> u != null ? u.toObject() : null).collect(java.util.stream.Collectors.toList()) : null;\n"
            elif is_map_of_unions:
                # For Map<String, Union>, unwrap each value by calling toObject() on it
                get_method += f"{INDENT * 3}case {index}: return this.{field_name} != null ? this.{field_name}.entrySet().stream().collect(java.util.stream.Collectors.toMap(java.util.Map.Entry::getKey, e -> e.getValue() != null ? e.getValue().toObject() : null)) : null;\n"
            elif field_type.is_enum:
                # For enum fields, convert to GenericEnumSymbol for Avro serialization
                # Use avroSymbol() to get the original Avro symbol name for serialization
                get_method += f"{INDENT * 3}case {index}: return this.{field_name} != null ? new GenericData.EnumSymbol({field_type.type_name}.SCHEMA, this.{field_name}.avroSymbol()) : null;\n"
            else:
                # For all other field types, return the field as-is
                # Avro's SpecificDatumWriter will handle serialization internally
                get_method += f"{INDENT * 3}case {index}: return this.{field_name};\n"
        get_method += f"{INDENT * 3}default: throw new AvroRuntimeException(\"Bad index: \" + field$);\n"
        get_method += f"{INDENT * 2}}}\n{INDENT}}}\n"
        return get_method

    def generate_avro_put_method(self, class_name: str, fields: List[Dict], parent_package: str) -> str:
        """ Generates the put method for SpecificRecord """
        suppress_unchecked = False
        put_method = f"\n{INDENT}@Override\n{INDENT}public void put(int field$, Object value$) {{\n"
        put_method += f"{INDENT * 2}switch (field$) {{\n"
        for index, field in enumerate(fields):
            # Skip const fields as they are final and cannot be reassigned
            if "const" in field:
                put_method += f"{INDENT * 3}case {index}: break; // const field, cannot be set\n"
                continue
            
            field_name = pascal(field['name']) if self.pascal_properties else field['name']
            field_name = self.safe_identifier(field_name, class_name)
            field_type = self.convert_avro_type_to_java(class_name, field_name, field['type'], parent_package)
            if field_type.type_name.startswith("List<") or field_type.type_name.startswith("Map<"):
                suppress_unchecked = True

            # Check if the field type is a generated type (union, class, or enum)
            type_kind = None
            if field_type.type_name in self.generated_types_avro_namespace:
                type_kind = self.generated_types_avro_namespace[field_type.type_name]
            elif field_type.type_name in self.generated_types_java_package:
                type_kind = self.generated_types_java_package[field_type.type_name]

            # Check if this is List<Union> or Map<String, Union>
            is_list_of_unions = field_type.type_name.startswith("List<") and field_type.union_types and len(field_type.union_types) > 0
            is_map_of_unions = field_type.type_name.startswith("Map<") and field_type.union_types and len(field_type.union_types) > 0
            
            if is_list_of_unions:
                # Extract the union type name from List<UnionType>
                union_type_match = field_type.type_name[5:-1]  # Remove "List<" and ">"
                # For List<Union>, handle both wrapped List<UnionWrapper> and unwrapped List<Object>
                # Avro deserialization provides List<Object>, so we need to wrap each element
                put_method += f"{INDENT * 3}case {index}: {{\n"
                put_method += f"{INDENT * 4}if (value$ instanceof List<?>) {{\n"
                put_method += f"{INDENT * 5}List<?> list = (List<?>)value$;\n"
                put_method += f"{INDENT * 5}if (list.isEmpty() || !(list.get(0) instanceof {union_type_match})) {{\n"
                put_method += f"{INDENT * 6}// Unwrapped from Avro - need to wrap, handling nulls\n"
                put_method += f"{INDENT * 6}this.{field_name} = list.stream().map(v -> v != null ? new {union_type_match}(v) : null).collect(java.util.stream.Collectors.toList());\n"
                put_method += f"{INDENT * 5}}} else {{\n"
                put_method += f"{INDENT * 6}// Already wrapped\n"
                put_method += f"{INDENT * 6}this.{field_name} = ({field_type.type_name})value$;\n"
                put_method += f"{INDENT * 5}}}\n"
                put_method += f"{INDENT * 4}}}\n"
                put_method += f"{INDENT * 4}break;\n"
                put_method += f"{INDENT * 3}}}\n"
            elif is_map_of_unions:
                # Extract the union type name from Map<String, UnionType>
                union_type_match = field_type.type_name.split(",")[1].strip()[:-1]  # Remove "Map<String, " and ">"
                put_method += f"{INDENT * 3}case {index}: {{\n"
                put_method += f"{INDENT * 4}if (value$ instanceof Map<?,?>) {{\n"
                put_method += f"{INDENT * 5}Map<?,?> map = (Map<?,?>)value$;\n"
                put_method += f"{INDENT * 5}if (map.isEmpty() || !(map.values().iterator().next() instanceof {union_type_match})) {{\n"
                put_method += f"{INDENT * 6}// Unwrapped from Avro - need to wrap, handling nulls\n"
                put_method += f"{INDENT * 6}this.{field_name} = map.entrySet().stream().collect(java.util.stream.Collectors.toMap(e -> (String)e.getKey(), e -> e.getValue() != null ? new {union_type_match}(e.getValue()) : null));\n"
                put_method += f"{INDENT * 5}}} else {{\n"
                put_method += f"{INDENT * 6}// Already wrapped\n"
                put_method += f"{INDENT * 6}this.{field_name} = ({field_type.type_name})value$;\n"
                put_method += f"{INDENT * 5}}}\n"
                put_method += f"{INDENT * 4}}}\n"
                put_method += f"{INDENT * 4}break;\n"
                put_method += f"{INDENT * 3}}}\n"
            elif type_kind == "union":
                # Unions can contain primitives or records - use the appropriate constructor
                # If Avro passes a GenericData.Record, use the GenericData.Record constructor
                # Otherwise use the Object constructor for already-constructed types
                put_method += f"{INDENT * 3}case {index}: this.{field_name} = value$ instanceof GenericData.Record ? new {field_type.type_name}((GenericData.Record)value$) : new {field_type.type_name}(value$); break;\n"
            elif type_kind == "class":
                # Record types need to be converted from GenericData.Record if that's what Avro passes
                put_method += f"{INDENT * 3}case {index}: this.{field_name} = value$ instanceof GenericData.Record ? new {field_type.type_name}((GenericData.Record)value$) : ({field_type.type_name})value$; break;\n"
            elif type_kind == "enum":
                # Enums need to be converted from GenericData.EnumSymbol
                # Use fromAvroSymbol to match original Avro symbol names
                put_method += f"{INDENT * 3}case {index}: this.{field_name} = value$ instanceof GenericData.EnumSymbol ? {field_type.type_name}.fromAvroSymbol(value$.toString()) : ({field_type.type_name})value$; break;\n"
            else:
                # Check if this is a List<RecordType> or Map<String,RecordType>
                is_list_of_records = False
                is_map_of_records = False
                if field_type.type_name.startswith("List<"):
                    item_type = field_type.type_name[5:-1]
                    if item_type in self.generated_types_java_package and self.generated_types_java_package[item_type] == "class":
                        is_list_of_records = True
                elif field_type.type_name.startswith("Map<"):
                    # Extract value type from Map<String, ValueType>
                    value_type = field_type.type_name.split(",")[1].strip()[:-1]
                    if value_type in self.generated_types_java_package and self.generated_types_java_package[value_type] == "class":
                        is_map_of_records = True
                
                if is_list_of_records:
                    item_type = field_type.type_name[5:-1]
                    put_method += f"{INDENT * 3}case {index}: {{\n"
                    put_method += f"{INDENT * 4}if (value$ instanceof List<?>)  {{\n"
                    put_method += f"{INDENT * 5}List<?> list = (List<?>)value$;\n"
                    put_method += f"{INDENT * 5}if (list.isEmpty() || !(list.get(0) instanceof {item_type})) {{\n"
                    put_method += f"{INDENT * 6}// Unwrapped from Avro - need to wrap GenericData.Record objects\n"
                    put_method += f"{INDENT * 6}this.{field_name} = list.stream().map(item -> item instanceof GenericData.Record ? new {item_type}((GenericData.Record)item) : ({item_type})item).collect(java.util.stream.Collectors.toList());\n"
                    put_method += f"{INDENT * 5}}} else {{\n"
                    put_method += f"{INDENT * 6}// Already wrapped\n"
                    put_method += f"{INDENT * 6}this.{field_name} = ({field_type.type_name})value$;\n"
                    put_method += f"{INDENT * 5}}}\n"
                    put_method += f"{INDENT * 4}}} else {{\n"
                    put_method += f"{INDENT * 5}// Handle null or other types\n"
                    put_method += f"{INDENT * 5}this.{field_name} = value$ != null ? ({field_type.type_name})value$ : null;\n"
                    put_method += f"{INDENT * 4}}}\n"
                    put_method += f"{INDENT * 4}break;\n"
                    put_method += f"{INDENT * 3}}}\n"
                elif is_map_of_records:
                    value_type = field_type.type_name.split(",")[1].strip()[:-1]
                    put_method += f"{INDENT * 3}case {index}: {{\n"
                    put_method += f"{INDENT * 4}if (value$ instanceof Map<?,?>) {{\n"
                    put_method += f"{INDENT * 5}Map<?,?> map = (Map<?,?>)value$;\n"
                    put_method += f"{INDENT * 5}if (map.isEmpty() || !(map.values().iterator().next() instanceof {value_type})) {{\n"
                    put_method += f"{INDENT * 6}// Unwrapped from Avro - need to wrap GenericData.Record objects\n"
                    put_method += f"{INDENT * 6}this.{field_name} = map.entrySet().stream().collect(java.util.stream.Collectors.toMap(e -> (String)e.getKey(), e -> e.getValue() instanceof GenericData.Record ? new {value_type}((GenericData.Record)e.getValue()) : ({value_type})e.getValue()));\n"
                    put_method += f"{INDENT * 5}}} else {{\n"
                    put_method += f"{INDENT * 6}// Already wrapped\n"
                    put_method += f"{INDENT * 6}this.{field_name} = ({field_type.type_name})value$;\n"
                    put_method += f"{INDENT * 5}}}\n"
                    put_method += f"{INDENT * 4}}} else {{\n"
                    put_method += f"{INDENT * 5}// Handle null or other types\n"
                    put_method += f"{INDENT * 5}this.{field_name} = value$ != null ? ({field_type.type_name})value$ : null;\n"
                    put_method += f"{INDENT * 4}}}\n"
                    put_method += f"{INDENT * 4}break;\n"
                    put_method += f"{INDENT * 3}}}\n"
                elif field_type.type_name == 'String':
                    # Handle null values for String fields
                    put_method += f"{INDENT * 3}case {index}: this.{field_name} = value$ != null ? value$.toString() : null; break;\n"
                elif field_type.type_name.startswith("List<"):
                    # Extract the element type
                    element_type = field_type.type_name[5:-1]
                    # Check if it's a List of enums
                    if element_type in self.generated_types_java_package and self.generated_types_java_package[element_type] == "enum":
                        # For List<Enum>, convert GenericEnumSymbol to actual enum values
                        # Use fromAvroSymbol to match original Avro symbol names
                        put_method += f"{INDENT * 3}case {index}: {{\n"
                        put_method += f"{INDENT * 4}if (value$ instanceof List<?>) {{\n"
                        put_method += f"{INDENT * 5}List<?> list = (List<?>)value$;\n"
                        put_method += f"{INDENT * 5}this.{field_name} = list.stream().map(item -> item instanceof GenericData.EnumSymbol ? {element_type}.fromAvroSymbol(item.toString()) : ({element_type})item).collect(java.util.stream.Collectors.toList());\n"
                        put_method += f"{INDENT * 4}}} else {{\n"
                        put_method += f"{INDENT * 5}this.{field_name} = null;\n"
                        put_method += f"{INDENT * 4}}}\n"
                        put_method += f"{INDENT * 4}break;\n"
                        put_method += f"{INDENT * 3}}}\n"
                    elif element_type == "String":
                        # For List<String>, convert Utf8 to String
                        put_method += f"{INDENT * 3}case {index}: {{\n"
                        put_method += f"{INDENT * 4}if (value$ instanceof List<?>) {{\n"
                        put_method += f"{INDENT * 5}List<?> list = (List<?>)value$;\n"
                        put_method += f"{INDENT * 5}this.{field_name} = list.stream().map(item -> item != null ? item.toString() : null).collect(java.util.stream.Collectors.toList());\n"
                        put_method += f"{INDENT * 4}}} else {{\n"
                        put_method += f"{INDENT * 5}this.{field_name} = null;\n"
                        put_method += f"{INDENT * 4}}}\n"
                        put_method += f"{INDENT * 4}break;\n"
                        put_method += f"{INDENT * 3}}}\n"
                    else:
                        # For other List types, create a defensive copy
                        put_method += f"{INDENT * 3}case {index}: this.{field_name} = value$ instanceof List<?> ? new java.util.ArrayList<>(({field_type.type_name})value$) : null; break;\n"
                elif field_type.type_name.startswith("Map<"):
                    # For any Map type, create a defensive copy to avoid sharing references
                    put_method += f"{INDENT * 3}case {index}: this.{field_name} = value$ instanceof Map<?,?> ? new java.util.HashMap<>(({field_type.type_name})value$) : null; break;\n"
                else:
                    put_method += f"{INDENT * 3}case {index}: this.{field_name} = ({field_type.type_name})value$; break;\n"
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
            
        package = self.join_packages(self.base_package, avro_schema.get('namespace', parent_package)).replace('.', '/').lower()       
        enum_name = self.safe_identifier(avro_schema['name'])
        type_name = self.qualified_name(package.replace('/', '.'), enum_name)
        self.generated_types_avro_namespace[self.qualified_name(avro_schema.get('namespace', parent_package),avro_schema['name'])] = "enum"
        self.generated_types_java_package[type_name] = "enum"
        self.generated_avro_schemas[type_name] = avro_schema
        symbols = avro_schema.get('symbols', [])
        # Convert symbols to valid Java identifiers in SCREAMING_CASE (uppercase)
        # Replace invalid chars, prepend _ if starts with digit or is a reserved word
        # Keep track of mapping from Java symbol to original Avro symbol for serialization
        java_symbols = []
        symbol_pairs = []  # (java_symbol, avro_symbol) pairs
        for symbol in symbols:
            java_symbol = symbol.replace('-', '_').replace('.', '_').upper()
            if java_symbol and java_symbol[0].isdigit():
                java_symbol = '_' + java_symbol
            # Check if the symbol is a Java reserved word and prefix with underscore
            if is_java_reserved_word(java_symbol.lower()):
                java_symbol = '_' + java_symbol
            java_symbols.append(java_symbol)
            symbol_pairs.append((java_symbol, symbol))
        
        # Build enum with avroSymbol field for proper Avro serialization
        enum_definition += f"public enum {enum_name} {{\n"
        # Each enum constant has its original Avro symbol stored
        enum_constants = []
        for java_symbol, avro_symbol in symbol_pairs:
            enum_constants.append(f'{java_symbol}("{avro_symbol}")')
        enum_definition += f"{INDENT}" + ", ".join(enum_constants)
        
        # Add avroSymbol field and method with Jackson annotations for proper JSON serialization
        enum_definition += f";\n\n{INDENT}private final String avroSymbol;\n\n"
        enum_definition += f"{INDENT}{enum_name}(String avroSymbol) {{\n{INDENT*2}this.avroSymbol = avroSymbol;\n{INDENT}}}\n\n"
        # @JsonValue tells Jackson to serialize the enum using avroSymbol() value
        enum_definition += f"{INDENT}@com.fasterxml.jackson.annotation.JsonValue\n"
        enum_definition += f"{INDENT}public String avroSymbol() {{\n{INDENT*2}return avroSymbol;\n{INDENT}}}\n\n"
        
        # Add static lookup method to find enum by Avro symbol with @JsonCreator for deserialization
        enum_definition += f"{INDENT}@com.fasterxml.jackson.annotation.JsonCreator\n"
        enum_definition += f"{INDENT}public static {enum_name} fromAvroSymbol(String symbol) {{\n"
        enum_definition += f"{INDENT*2}for ({enum_name} e : values()) {{\n"
        enum_definition += f"{INDENT*3}if (e.avroSymbol.equals(symbol)) return e;\n"
        enum_definition += f"{INDENT*2}}}\n"
        enum_definition += f"{INDENT*2}throw new IllegalArgumentException(\"Unknown symbol: \" + symbol);\n"
        enum_definition += f"{INDENT}}}\n"
        
        # Add Avro schema if annotations are enabled
        if self.avro_annotation:
            # Create inline schema for the enum
            enum_schema = {
                "type": "enum",
                "name": enum_name,
                "symbols": symbols
            }
            if 'namespace' in avro_schema:
                enum_schema['namespace'] = avro_schema['namespace']
            if 'doc' in avro_schema:
                enum_schema['doc'] = avro_schema['doc']
            
            enum_schema_json = json.dumps(enum_schema)
            enum_schema_json = enum_schema_json.replace('"', '§')
            enum_schema_json = f"\"+\n{INDENT}\"".join(
                [enum_schema_json[i:i+80] for i in range(0, len(enum_schema_json), 80)])
            enum_schema_json = enum_schema_json.replace('§', '\\"')
            
            enum_definition += f"\n{INDENT}public static final Schema SCHEMA = new Schema.Parser().parse(\n{INDENT}\"{enum_schema_json}\");\n"
        
        enum_definition += "}\n"
        if write_file:
            self.write_to_file(package, enum_name, enum_definition)
        return AvroToJava.JavaType(type_name, is_enum=True)
    
    def generate_embedded_union_class_jackson(self, class_name: str, field_name: str, avro_type: List, parent_package: str, write_file: bool) -> str:
        """ Generates an embedded Union Class for Java using Jackson """
        class_definition_ctors = class_definition_decls = class_definition_read = class_definition_write = class_definition = ''
        class_definition_toobject = class_definition_fromobjectctor = class_definition_genericrecordctor = ''
        
        list_is_json_match: List[str] = []
        union_class_name = class_name + pascal(field_name) + 'Union'
        package = self.join_packages(self.base_package, parent_package).replace('.', '/').lower()
        union_types: List[AvroToJava.JavaType] = [self.convert_avro_type_to_java(class_name, field_name + "Option" + str(i), t, parent_package) for i, t in enumerate(avro_type)]
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
            
            union_variable_name = self.safe_identifier(union_variable_name, class_name)

            # Constructor for each type
            class_definition_ctors += \
                f"{INDENT*1}public {union_class_name}({union_type.type_name} {union_variable_name}) {{\n{INDENT*2}this._{camel(union_variable_name)} = {union_variable_name};\n{INDENT*1}}}\n"

            # Declarations
            class_definition_decls += \
                f"{INDENT*1}private {union_type.type_name} _{camel(union_variable_name)};\n" + \
                f"{INDENT*1}public {union_type.type_name} get{union_variable_name}() {{ return _{camel(union_variable_name)}; }}\n";
                
            # For toObject(), wrap enums in GenericData.EnumSymbol so Avro can serialize them
            # Use avroSymbol() to get the original Avro symbol name for serialization
            if union_type.is_enum:
                class_definition_toobject += f"{INDENT*2}if (_{camel(union_variable_name)} != null) {{\n{INDENT*3}return new GenericData.EnumSymbol({union_type.type_name}.SCHEMA, _{camel(union_variable_name)}.avroSymbol());\n{INDENT*2}}}\n"
            else:
                class_definition_toobject += f"{INDENT*2}if (_{camel(union_variable_name)} != null) {{\n{INDENT*3}return _{camel(union_variable_name)};\n{INDENT*2}}}\n"
            
            # GenericData.Record constructor only handles record types - primitives come through fromObject
            if self.avro_annotation and union_type.is_class:            
                class_definition_genericrecordctor += f"{INDENT*2}if (record.getSchema().getFullName().equals({union_type.type_name}.AVROSCHEMA.getFullName())) {{\n"
                class_definition_genericrecordctor += f"{INDENT*3}this._{camel(union_variable_name)} = new {union_type.type_name}(record);\n{INDENT*3}return;\n{INDENT*2}}}\n"
            
            # there can only be one list and one map in the union, so we don't need to differentiate this any further
            if is_list:
                class_definition_fromobjectctor += f"{INDENT*2}if (obj instanceof List<?>) {{\n{INDENT*3}this._{camel(union_variable_name)} = ({union_type.type_name})obj;\n{INDENT*3}return;\n{INDENT*2}}}\n"
            elif is_dict:
                class_definition_fromobjectctor += f"{INDENT*2}if (obj instanceof Map<?,?>) {{\n{INDENT*3}this._{camel(union_variable_name)} = ({union_type.type_name})obj;\n{INDENT*3}return;\n{INDENT*2}}}\n"
            else:
                # For class types, check for GenericData.Record first (Avro deserialization), then typed instance
                if self.avro_annotation and union_type.is_class:
                    class_definition_fromobjectctor += f"{INDENT*2}if (obj instanceof GenericData.Record) {{\n"
                    class_definition_fromobjectctor += f"{INDENT*3}GenericData.Record record = (GenericData.Record)obj;\n"
                    # Use getFullName() for robust schema comparison instead of separate name + namespace
                    class_definition_fromobjectctor += f"{INDENT*3}String recordFullName = record.getSchema().getFullName();\n"
                    class_definition_fromobjectctor += f"{INDENT*3}String expectedFullName = {union_type.type_name}.AVROSCHEMA.getFullName();\n"
                    class_definition_fromobjectctor += f"{INDENT*3}if (recordFullName.equals(expectedFullName)) {{\n"
                    class_definition_fromobjectctor += f"{INDENT*4}this._{camel(union_variable_name)} = new {union_type.type_name}(record);\n{INDENT*4}return;\n{INDENT*3}}}\n{INDENT*2}}}\n"
                
                # Handle Avro's Utf8 type for String
                if self.avro_annotation and union_type.type_name == "String":
                    class_definition_fromobjectctor += f"{INDENT*2}if (obj instanceof org.apache.avro.util.Utf8) {{\n{INDENT*3}this._{camel(union_variable_name)} = obj.toString();\n{INDENT*3}return;\n{INDENT*2}}}\n"
                
                # Handle Avro's GenericEnumSymbol for enum types
                # Use fromAvroSymbol to match original Avro symbol names
                if self.avro_annotation and union_type.is_enum:
                    class_definition_fromobjectctor += f"{INDENT*2}if (obj instanceof GenericData.EnumSymbol) {{\n{INDENT*3}this._{camel(union_variable_name)} = {union_type.type_name}.fromAvroSymbol(obj.toString());\n{INDENT*3}return;\n{INDENT*2}}}\n"
                
                class_definition_fromobjectctor += f"{INDENT*2}if (obj instanceof {union_type.type_name}) {{\n{INDENT*3}this._{camel(union_variable_name)} = ({union_type.type_name})obj;\n{INDENT*3}return;\n{INDENT*2}}}\n"

            # Read method logic - test types in order using duck typing (like C# implementation)
            if is_dict:
                class_definition_read += f"{INDENT*3}if (node.isObject()) {{\n{INDENT*4}{union_type.type_name} map = mapper.readValue(node.toString(), new TypeReference<{union_type.type_name}>(){{}});\n{INDENT*3}return new {union_class_name}(map);\n{INDENT*3}}}\n"
            elif is_list:
                class_definition_read += f"{INDENT*3}if (node.isArray()) {{\n{INDENT*4}{union_type.type_name} list = mapper.readValue(node.toString(), new TypeReference<{union_type.type_name}>(){{}});\n{INDENT*4}return new {union_class_name}(list);\n{INDENT*3}}}\n"
            elif self.is_java_primitive(union_type):
                if union_type.type_name == "String":
                    class_definition_read += f"{INDENT*3}if (node.isTextual()) {{\n{INDENT*4}return new {union_class_name}(node.asText());\n{INDENT*3}}}\n"
                elif union_type.type_name == "byte[]":
                    class_definition_read += f"{INDENT*3}if (node.isBinary()) {{\n{INDENT*4}return new {union_class_name}(node.binaryValue());\n{INDENT*3}}}\n"
                elif union_type.type_name in ["int", "Int", "Integer"]:
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
                # For classes and enums, use duck typing with isJsonMatch() (C# pattern)
                if union_type.is_enum:
                    # Use fromAvroSymbol to match original Avro symbol names
                    class_definition_read += f"{INDENT*3}if (node.isTextual()) {{\n{INDENT*4}return new {union_class_name}({union_type.type_name}.fromAvroSymbol(node.asText()));\n{INDENT*3}}}\n"
                elif union_type.is_class:
                    # Use isJsonMatch() to test if this type matches, then use fromData() to deserialize
                    class_definition_read += f"{INDENT*3}if ({union_type.type_name}.isJsonMatch(node)) {{\n{INDENT*4}return new {union_class_name}({union_type.type_name}.fromData(node, \"application/json\"));\n{INDENT*3}}}\n"
                
            # Write method logic
            class_definition_write += f"{INDENT*3}{union_type.type_name} {camel(union_variable_name)}Value = value.get{union_variable_name}();\n{INDENT*3}if ({camel(union_variable_name)}Value != null) {{\n{INDENT*4}generator.writeObject({camel(union_variable_name)}Value);\n{INDENT*4}return;\n{INDENT*3}}}\n"

            # JSON match method logic
            gij = self.get_is_json_match_clause_type("node", class_name, union_type)
            if gij:
                list_is_json_match.append(gij)

        class_definition =  f"@JsonSerialize(using = {union_class_name}.Serializer.class)\n"
        class_definition += f"@JsonDeserialize(using = {union_class_name}.Deserializer.class)\n"
        class_definition += f"public class {union_class_name} {{\n"
        class_definition += class_definition_decls
        class_definition += f"\n{INDENT}public " + union_class_name + "() {}\n"
        if self.avro_annotation:
            class_definition += f"\n{INDENT}public {union_class_name}(GenericData.Record record) {{\n"
            class_definition += class_definition_genericrecordctor
            class_definition += f"{INDENT*2}throw new UnsupportedOperationException(\"No record type is set in the union\");\n"
            class_definition += f"{INDENT}}}\n"
        class_definition += f"\n{INDENT}public {union_class_name}(Object obj) {{\n"
        class_definition += f"{INDENT*2}if (obj == null) {{\n"
        class_definition += f"{INDENT*3}return; // null is valid for unions with null type\n"
        class_definition += f"{INDENT*2}}}\n"
        class_definition += class_definition_fromobjectctor
        class_definition += f"{INDENT*2}throw new UnsupportedOperationException(\"No record type is set in the union\");\n"
        class_definition += f"{INDENT}}}\n"
        class_definition += class_definition_ctors
        class_definition += f"\n{INDENT}public Object toObject() {{\n"
        class_definition += class_definition_toobject
        class_definition += f"{INDENT*2}throw new UnsupportedOperationException(\"No record type is set in the union\");\n"
        class_definition += f"{INDENT}}}\n"
        class_definition += f"\n{INDENT}public static class Serializer extends JsonSerializer<" + union_class_name + "> {\n"
        class_definition += f"{INDENT*2}@Override\n"
        class_definition += f"{INDENT*2}public void serialize(" + union_class_name + " value, JsonGenerator generator, SerializerProvider serializers) throws IOException {\n"
        class_definition += class_definition_write
        class_definition += f"{INDENT*3}throw new UnsupportedOperationException(\"No record type is set in the union\");\n"
        class_definition += f"{INDENT*2}}}\n{INDENT}}}\n"
        class_definition += f"\n{INDENT}public static class Deserializer extends JsonDeserializer<" + union_class_name + "> {\n"
        class_definition += f"{INDENT*2}@Override\n"
        class_definition += f"{INDENT*2}public " + union_class_name + " deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {\n"
        class_definition += f"{INDENT*3}ObjectMapper mapper = (ObjectMapper) p.getCodec();\n"
        class_definition += f"{INDENT*3}JsonNode node = mapper.readTree(p);\n"
        class_definition += class_definition_read
        class_definition += f"{INDENT*3}throw new UnsupportedOperationException(\"No record type matched the JSON data\");\n"
        class_definition += f"{INDENT*2}}}\n{INDENT}}}\n"
        class_definition += f"\n{INDENT*1}public static boolean isJsonMatch(JsonNode node) {{\n"
        class_definition += f"{INDENT*2}return " + " || ".join(list_is_json_match) + ";\n"
        class_definition += f"{INDENT*1}}}\n"
        
        # Add equals method for union class
        class_definition += self.generate_union_equals_method(union_class_name, union_types)
        
        # Add hashCode method for union class
        class_definition += self.generate_union_hashcode_method(union_class_name, union_types)
        class_definition += "}\n"

        if write_file:
            self.write_to_file(package, union_class_name, class_definition)
        # Calculate qualified name for the union
        qualified_union_name = self.qualified_name(package.replace('/', '.'), union_class_name)
        self.generated_types_avro_namespace[union_class_name] = "union"  # Track union types
        self.generated_types_java_package[union_class_name] = "union"  # Track union types with simple name
        self.generated_types_java_package[qualified_union_name] = "union"  # Also track with qualified name
        # Store the union schema with the types information
        self.generated_avro_schemas[union_class_name] = {"types": avro_type}
        self.generated_avro_schemas[qualified_union_name] = {"types": avro_type}
        return union_class_name


    def generate_property(self, class_name: str, field: Dict, parent_package: str) -> str:
        """ Generates a Java property definition """
        field_name = pascal(field['name']) if self.pascal_properties else field['name']
        field_type = self.convert_avro_type_to_java(class_name, field_name, field['type'], parent_package)
        safe_field_name = self.safe_identifier(field_name, class_name)
        property_def = ''
        if 'doc' in field:
            property_def += f"{INDENT}/** {field['doc']} */\n"
        
        # For discriminator const fields, don't put @JsonProperty on the field
        # The getter will handle JSON serialization/deserialization
        is_discriminator_const = field.get('discriminator', False) and 'const' in field
        if self.jackson_annotations and not is_discriminator_const:
            property_def += f"{INDENT}@JsonProperty(\"{field['name']}\")\n"
        
        # Handle const fields
        if 'const' in field and field['const'] is not None:
            const_value = field['const']
            is_discriminator = field.get('discriminator', False)
            
            # For enum types, qualify with the enum type name and convert to SCREAMING_CASE
            if field_type.type_name not in ('String', 'int', 'Integer', 'long', 'Long', 'double', 'Double', 'boolean', 'Boolean'):
                # Convert enum const value to uppercase to match Java enum constant naming convention
                const_value_upper = str(const_value).replace('-', '_').replace('.', '_').upper()
                if const_value_upper and const_value_upper[0].isdigit():
                    const_value_upper = '_' + const_value_upper
                if is_java_reserved_word(const_value_upper.lower()):
                    const_value_upper = '_' + const_value_upper
                const_value = f'{field_type.type_name}.{const_value_upper}'
            elif field_type.type_name == 'String':
                const_value = f'"{const_value}"'
            
            property_def += f"{INDENT}private final {field_type.type_name} {safe_field_name} = {const_value};\n"
            
            # For discriminator fields, we need both the enum value accessor and String override
            if is_discriminator:
                # Provide a typed accessor for the enum value (ignored by Jackson since it's synthetic)
                if self.jackson_annotations:
                    property_def += f"{INDENT}@JsonIgnore\n"
                property_def += f"{INDENT}public {field_type.type_name} get{pascal(field_name)}Value() {{ return {safe_field_name}; }}\n"
                # Generate the getter that returns String (Jackson will use this for serialization)
                # Use avroSymbol() to get the original Avro symbol name for serialization
                # Use READ_ONLY since this is a const field that doesn't need deserialization
                # Note: Not using @Override because not all discriminated union variants extend a base class
                if self.jackson_annotations:
                    property_def += f"{INDENT}@JsonProperty(value=\"{field['name']}\", access=JsonProperty.Access.READ_ONLY)\n"
                property_def += f"{INDENT}public String get{pascal(field_name)}() {{ return {safe_field_name}.avroSymbol(); }}\n"
            else:
                property_def += f"{INDENT}public {field_type.type_name} get{pascal(field_name)}() {{ return {safe_field_name}; }}\n"
        else:
            property_def += f"{INDENT}private {field_type.type_name} {safe_field_name};\n"
            property_def += f"{INDENT}public {field_type.type_name} get{pascal(field_name)}() {{ return {safe_field_name}; }}\n"
            property_def += f"{INDENT}public void set{pascal(field_name)}({field_type.type_name} {safe_field_name}) {{ this.{safe_field_name} = {safe_field_name}; }}\n"
        
        # Generate typed accessors only for direct union fields (not for List/Map<Union>)
        # For List<Union>, the field IS the list, not a single union value
        if field_type.union_types and not field_type.type_name.startswith("List<") and not field_type.type_name.startswith("Map<"):
            for union_type in field_type.union_types:
                if union_type.type_name.startswith("List<") or union_type.type_name.startswith("Map<"):
                    property_def += f"{INDENT}@SuppressWarnings(\"unchecked\")\n"
                property_def += f"{INDENT}public {union_type.type_name} get{pascal(field_name)}As{flatten_type_name(union_type.type_name)}() {{ return ({union_type.type_name}){safe_field_name}; }}\n"
                property_def += f"{INDENT}public void set{pascal(field_name)}As{flatten_type_name(union_type.type_name)}({union_type.type_name} {safe_field_name}) {{ this.{safe_field_name} = {safe_field_name}; }}\n"
        return property_def

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
                
                # Check if this class extends a discriminated union base class
                # Pattern: "public class ClassName extends UnionName"
                if " extends " in definition and self.jackson_annotations:
                    import re
                    match = re.search(r'public class \w+ extends (\w+)', definition)
                    if match:
                        base_class_name = match.group(1)
                        # Check if this base class is a discriminated union we generated
                        for union_name, union_subtypes in self.discriminated_unions.items():
                            if union_name == base_class_name:
                                # Get the package where the union base class is generated
                                # (it's in the same package as the first subtype)
                                union_package = union_subtypes[0]['package'] if union_subtypes else self.base_package.replace('/', '.')
                                # Only import if the union is in a different package
                                current_package = package.replace('/', '.')
                                if union_package != current_package:
                                    file.write(f"import {union_package}.{union_name};\n")
                                break
                
                if "List<" in definition or "ArrayList<" in definition:
                    file.write("import java.util.List;\n")
                if "ArrayList<" in definition or "Arrays.asList" in definition:
                    file.write("import java.util.ArrayList;\n")
                if "Map<" in definition or "HashMap<" in definition:
                    file.write("import java.util.Map;\n")
                if "HashMap<" in definition:
                    file.write("import java.util.HashMap;\n")
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
                if 'GenericData' in definition:
                    file.write("import org.apache.avro.generic.GenericData;\n")
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
                if 'JsonIgnore' in definition:
                    file.write("import com.fasterxml.jackson.annotation.JsonIgnore;\n")
                if 'JsonIgnoreProperties' in definition:
                    file.write("import com.fasterxml.jackson.annotation.JsonIgnoreProperties;\n")
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

    def generate_tests(self, base_output_dir: str) -> None:
        """ Generates unit tests for all the generated Java classes and enums """
        from avrotize.common import process_template
        
        test_directory_path = os.path.join(base_output_dir, "src/test/java")
        if not os.path.exists(test_directory_path):
            os.makedirs(test_directory_path, exist_ok=True)

        for class_name, type_kind in self.generated_types_java_package.items():
            if type_kind in ["class", "enum"]:
                self.generate_test_class(class_name, type_kind, test_directory_path)

    def generate_test_class(self, class_name: str, type_kind: str, test_directory_path: str) -> None:
        """ Generates a unit test class for a given Java class or enum """
        from avrotize.common import process_template
        
        avro_schema = self.generated_avro_schemas.get(class_name, {})
        simple_class_name = class_name.split('.')[-1]
        package = ".".join(class_name.split('.')[:-1])
        test_class_name = f"{simple_class_name}Test"

        if type_kind == "class":
            fields = self.get_class_test_fields(avro_schema, simple_class_name, package)
            imports = self.get_test_imports(fields)
            test_class_definition = process_template(
                "avrotojava/class_test.java.jinja",
                package=package,
                test_class_name=test_class_name,
                class_name=simple_class_name,
                fields=fields,
                imports=imports,
                avro_annotation=self.avro_annotation,
                jackson_annotation=self.jackson_annotations
            )
        elif type_kind == "enum":
            # Convert symbols to Java-safe identifiers in SCREAMING_CASE (same logic as generate_enum)
            raw_symbols = avro_schema.get('symbols', [])
            java_safe_symbols = []
            for symbol in raw_symbols:
                java_symbol = symbol.replace('-', '_').replace('.', '_').upper()
                if java_symbol and java_symbol[0].isdigit():
                    java_symbol = '_' + java_symbol
                if is_java_reserved_word(java_symbol.lower()):
                    java_symbol = '_' + java_symbol
                java_safe_symbols.append(java_symbol)
            
            test_class_definition = process_template(
                "avrotojava/enum_test.java.jinja",
                package=package,
                test_class_name=test_class_name,
                enum_name=simple_class_name,
                symbols=java_safe_symbols  # Pass converted symbols instead of raw
            )

        # Write test file
        package_path = package.replace('.', os.sep)
        test_file_dir = os.path.join(test_directory_path, package_path)
        if not os.path.exists(test_file_dir):
            os.makedirs(test_file_dir, exist_ok=True)
        test_file_path = os.path.join(test_file_dir, f"{test_class_name}.java")
        with open(test_file_path, 'w', encoding='utf-8') as test_file:
            test_file.write(test_class_definition)

    def get_test_imports(self, fields: List) -> List[str]:
        """ Gets the necessary imports for the test class """
        imports = []
        
        # Track simple names to detect conflicts
        # Map: simple_name -> list of FQNs that have that simple name
        simple_name_to_fqns: Dict[str, List[str]] = {}
        
        # First pass: collect all custom type FQNs and their simple names
        for field in fields:
            inner_types = []
            if field.field_type.startswith("List<"):
                inner_type = field.field_type[5:-1]
                if inner_type.startswith("Map<"):
                    start = inner_type.index('<') + 1
                    end = inner_type.rindex('>')
                    map_types = inner_type[start:end].split(',')
                    if len(map_types) > 1:
                        inner_types.append(map_types[1].strip())
                else:
                    inner_types.append(inner_type)
            elif field.field_type.startswith("Map<"):
                start = field.field_type.index('<') + 1
                end = field.field_type.rindex('>')
                map_types = field.field_type[start:end].split(',')
                if len(map_types) > 1:
                    inner_types.append(map_types[1].strip())
            if not field.field_type.startswith(("List<", "Map<")):
                inner_types.append(field.field_type)
            if hasattr(field, 'java_type_obj') and field.java_type_obj and field.java_type_obj.union_types:
                for union_member_type in field.java_type_obj.union_types:
                    inner_types.append(union_member_type.type_name)
            
            for type_to_check in inner_types:
                if type_to_check in self.generated_types_java_package and '.' in type_to_check:
                    simple_name = type_to_check.split('.')[-1]
                    if simple_name not in simple_name_to_fqns:
                        simple_name_to_fqns[simple_name] = []
                    if type_to_check not in simple_name_to_fqns[simple_name]:
                        simple_name_to_fqns[simple_name].append(type_to_check)
        
        # Find conflicting simple names (same simple name, different FQNs)
        conflicting_fqns: set = set()
        for simple_name, fqns in simple_name_to_fqns.items():
            if len(fqns) > 1:
                # This simple name has conflicts - mark all FQNs as conflicting
                conflicting_fqns.update(fqns)
        
        for field in fields:
            # Extract inner types from generic collections
            inner_types = []
            if field.field_type.startswith("List<"):
                if "import java.util.List;" not in imports:
                    imports.append("import java.util.List;")
                if "import java.util.ArrayList;" not in imports:
                    imports.append("import java.util.ArrayList;")
                # Extract the inner type: List<Type> -> Type
                inner_type = field.field_type[5:-1]
                # Check if inner type is also a Map
                if inner_type.startswith("Map<"):
                    if "import java.util.Map;" not in imports:
                        imports.append("import java.util.Map;")
                    if "import java.util.HashMap;" not in imports:
                        imports.append("import java.util.HashMap;")
                    # Extract Map value type
                    start = inner_type.index('<') + 1
                    end = inner_type.rindex('>')
                    map_types = inner_type[start:end].split(',')
                    if len(map_types) > 1:
                        inner_types.append(map_types[1].strip())
                else:
                    inner_types.append(inner_type)
            elif field.field_type.startswith("Map<"):
                if "import java.util.Map;" not in imports:
                    imports.append("import java.util.Map;")
                if "import java.util.HashMap;" not in imports:
                    imports.append("import java.util.HashMap;")
                # Extract value type from Map<K,V>
                start = field.field_type.index('<') + 1
                end = field.field_type.rindex('>')
                map_types = field.field_type[start:end].split(',')
                if len(map_types) > 1:
                    inner_types.append(map_types[1].strip())
            
            # Add the direct field type for non-generic types
            if not field.field_type.startswith(("List<", "Map<")):
                inner_types.append(field.field_type)
            
            # If field is Object with union_types (Avro-style union), add all union member types for imports
            if hasattr(field, 'java_type_obj') and field.java_type_obj and field.java_type_obj.union_types:
                for union_member_type in field.java_type_obj.union_types:
                    inner_types.append(union_member_type.type_name)
            
            # Process each type (including inner types from generics)
            for type_to_check in inner_types:
                # Add imports for enum and class types
                if type_to_check in self.generated_types_java_package:
                    type_kind = self.generated_types_java_package[type_to_check]
                    # Only import if it's a fully qualified name with a package
                    # Skip imports for types with conflicting simple names - they'll use FQN
                    if '.' in type_to_check and type_to_check not in conflicting_fqns:
                        import_stmt = f"import {type_to_check};"
                        if import_stmt not in imports:
                            imports.append(import_stmt)
                        # No longer import test classes - we instantiate classes directly
                    # Process unions regardless of whether they're fully qualified
                    # (they might be simple names that need member imports)
                    if type_kind == "union":
                            avro_schema = self.generated_avro_schemas.get(type_to_check, {})
                            if avro_schema and 'types' in avro_schema:
                                for union_type in avro_schema['types']:
                                    java_qualified_name = None
                                    if isinstance(union_type, dict) and 'name' in union_type:
                                        # It's a complex type reference (inline definition)
                                        type_name = union_type['name']
                                        if 'namespace' in union_type:
                                            avro_namespace = union_type['namespace']
                                            # Build full Java qualified name with base package
                                            java_qualified_name = self.join_packages(self.base_package, avro_namespace).replace('/', '.').lower() + '.' + type_name
                                        else:
                                            java_qualified_name = type_name
                                    elif isinstance(union_type, str) and union_type not in ['null', 'string', 'int', 'long', 'float', 'double', 'boolean', 'bytes']:
                                        # It's a string reference to a named type (could be class or enum)
                                        # The string is the Avro qualified name, need to convert to Java
                                        avro_name_parts = union_type.split('.')
                                        if len(avro_name_parts) > 1:
                                            # Has namespace
                                            type_name = avro_name_parts[-1]
                                            avro_namespace = '.'.join(avro_name_parts[:-1])
                                            java_qualified_name = self.join_packages(self.base_package, avro_namespace).replace('/', '.').lower() + '.' + type_name
                                        else:
                                            # No namespace, just a simple name
                                            java_qualified_name = union_type
                                    
                                    if java_qualified_name:
                                        if java_qualified_name in self.generated_types_java_package or java_qualified_name.split('.')[-1] in self.generated_types_java_package:
                                            member_type_kind = self.generated_types_java_package.get(java_qualified_name, self.generated_types_java_package.get(java_qualified_name.split('.')[-1], None))
                                            # Import the class/enum only if not conflicting
                                            if java_qualified_name not in conflicting_fqns:
                                                class_import = f"import {java_qualified_name};"
                                                if class_import not in imports:
                                                    imports.append(class_import)
                                            # No longer import test classes - we instantiate classes directly
        return imports

    def get_class_test_fields(self, avro_schema: Dict, class_name: str, package: str) -> List:
        """ Retrieves fields for a given class name """
        
        class Field:
            def __init__(self, fn: str, ft: str, tv: str, ct: bool, ie: bool = False, java_type_obj: 'AvroToJava.JavaType' = None, is_discrim: bool = False):
                self.field_name = fn
                self.field_type = ft
                # Extract base type for generic types (e.g., List<Object> -> List)
                if '<' in ft:
                    self.base_type = ft.split('<')[0]
                else:
                    self.base_type = ft
                self.test_value = tv
                self.is_const = ct
                self.is_enum = ie
                self.is_discriminator = is_discrim
                self.java_type_obj = java_type_obj  # Store the full JavaType object for union access

        fields: List[Field] = []
        if avro_schema and 'fields' in avro_schema:
            for field in avro_schema['fields']:
                field_name = pascal(field['name']) if self.pascal_properties else field['name']
                field_type = self.convert_avro_type_to_java(class_name, field_name, field['type'], avro_schema.get('namespace', ''))
                # Check if the field type is an enum
                is_enum = field_type.type_name in self.generated_types_java_package and \
                         self.generated_types_java_package[field_type.type_name] == "enum"
                is_discriminator = field.get('discriminator', False)
                
                # Generate test value for the field
                if "const" in field and field["const"] is not None:
                    const_value = field["const"]
                    # For enum types, qualify with the enum type name and convert to SCREAMING_CASE
                    if is_enum or (field_type.type_name not in ('String', 'int', 'Integer', 'long', 'Long', 'double', 'Double', 'boolean', 'Boolean')):
                        # Convert enum const value to uppercase to match Java enum constant naming convention
                        const_value_upper = str(const_value).replace('-', '_').replace('.', '_').upper()
                        if const_value_upper and const_value_upper[0].isdigit():
                            const_value_upper = '_' + const_value_upper
                        if is_java_reserved_word(const_value_upper.lower()):
                            const_value_upper = '_' + const_value_upper
                        test_value = f'{field_type.type_name}.{const_value_upper}'
                    else:
                        test_value = f'"{const_value}"'
                else:
                    test_value = self.get_test_value_from_field(field['type'], field_type, package)
                
                f = Field(
                    field_name,
                    field_type.type_name,
                    test_value,
                    "const" in field and field["const"] is not None,
                    is_enum,
                    field_type,  # Pass the full JavaType object
                    is_discriminator
                )
                fields.append(f)
        return fields

    def get_test_value_from_field(self, avro_field_type: Union[str, Dict, List], java_type: JavaType, package: str) -> str:
        """Returns a default test value based on the Avro field type and Java type"""
        # If it's an Object with union_types (Avro-style union), pick a member type
        if java_type.type_name == "Object" and java_type.union_types is not None and len(java_type.union_types) > 0:
            # Pick the first union type and generate a test value for it
            first_union_type = java_type.union_types[0]
            return self.get_test_value(first_union_type.type_name, package)
        # For List<Object> where Object is a union, we need to handle it specially
        elif java_type.type_name.startswith("List<Object>"):
            # avro_field_type could be: ["null", {"type": "array", "items": [union types]}]
            # or just: {"type": "array", "items": [union types]}
            array_schema = avro_field_type
            if isinstance(avro_field_type, list):
                # It's a union - find the array type
                for t in avro_field_type:
                    if isinstance(t, dict) and t.get('type') == 'array':
                        array_schema = t
                        break
           
            if isinstance(array_schema, dict) and array_schema.get('type') == 'array':
                items_type = array_schema.get('items')
                if isinstance(items_type, list):  # Union array
                    # Pick the first non-null type
                    non_null_types = [t for t in items_type if t != 'null']
                    if non_null_types:
                        inner_java_type = self.convert_avro_type_to_java('_test', '_field', non_null_types[0], package)
                        inner_value = self.get_test_value(inner_java_type.type_name, package)
                        return f'new ArrayList<>(java.util.Arrays.asList({inner_value}))'
        # Default: use type name
        return self.get_test_value(java_type.type_name, package)

    def get_test_value(self, java_type: str, package: str) -> str:
        """Returns a default test value based on the Java type"""
        test_values = {
            'String': '"test_string"',
            'boolean': 'true',
            'Boolean': 'Boolean.TRUE',
            'int': '42',
            'Integer': 'Integer.valueOf(42)',
            'long': '42L',
            'Long': 'Long.valueOf(42L)',
            'float': '3.14f',
            'Float': 'Float.valueOf(3.14f)',
            'double': '3.14',
            'Double': 'Double.valueOf(3.14)',
            'byte[]': 'new byte[] { 0x01, 0x02, 0x03 }',
            'Object': 'null',  # Use null for Object types (Avro unions) to avoid reference equality issues
            # Java time types - use factory methods, not constructors
            'Instant': 'java.time.Instant.now()',
            'java.time.Instant': 'java.time.Instant.now()',
            'LocalDate': 'java.time.LocalDate.now()',
            'java.time.LocalDate': 'java.time.LocalDate.now()',
            'LocalTime': 'java.time.LocalTime.now()',
            'java.time.LocalTime': 'java.time.LocalTime.now()',
            'LocalDateTime': 'java.time.LocalDateTime.now()',
            'java.time.LocalDateTime': 'java.time.LocalDateTime.now()',
            'Duration': 'java.time.Duration.ofSeconds(42)',
            'java.time.Duration': 'java.time.Duration.ofSeconds(42)',
            'UUID': 'java.util.UUID.randomUUID()',
            'java.util.UUID': 'java.util.UUID.randomUUID()',
            'BigDecimal': 'new java.math.BigDecimal("42.00")',
            'java.math.BigDecimal': 'new java.math.BigDecimal("42.00")',
        }
        
        # Handle generic types
        if java_type.startswith("List<"):
            inner_type = java_type[5:-1]
            inner_value = self.get_test_value(inner_type, package)
            # Arrays.asList(null) throws NPE, so create empty list for null values
            if inner_value == 'null':
                return 'new ArrayList<>()'
            return f'new ArrayList<>(java.util.Arrays.asList({inner_value}))'
        elif java_type.startswith("Map<"):
            return 'new HashMap<>()'
        
        # Check if it's a generated type (enum, class, or union)
        if java_type in self.generated_types_java_package:
            type_kind = self.generated_types_java_package[java_type]
            if type_kind == "enum":
                # Get the first symbol for the enum
                avro_schema = self.generated_avro_schemas.get(java_type, {})
                symbols = avro_schema.get('symbols', [])
                if symbols:
                    # Convert symbol to valid Java identifier in SCREAMING_CASE (same logic as in generate_enum)
                    first_symbol = symbols[0].replace('-', '_').replace('.', '_').upper()
                    if first_symbol and first_symbol[0].isdigit():
                        first_symbol = '_' + first_symbol
                    # Check if the symbol is a Java reserved word and prefix with underscore
                    if is_java_reserved_word(first_symbol.lower()):
                        first_symbol = '_' + first_symbol
                    # Use fully qualified name to avoid conflicts with field names
                    return f'{java_type}.{first_symbol}'
                return f'{java_type}.values()[0]'
            elif type_kind == "class":
                # Create a new instance using the createTestInstance() method
                # Use fully qualified name to avoid conflicts with field names
                return f'{java_type}.createTestInstance()'
            elif type_kind == "union":
                # For union types, we need to create an instance with one of the union types set
                # Get the union's schema to find available types
                avro_schema = self.generated_avro_schemas.get(java_type, {})
                if avro_schema and 'types' in avro_schema:
                    # Use the first non-null type from the union
                    for union_type in avro_schema['types']:
                        if union_type != 'null' and isinstance(union_type, dict):
                            # It's a complex type - check if enum or class
                            if 'name' in union_type:
                                type_name = union_type['name']
                                if 'namespace' in union_type:
                                    avro_namespace = union_type['namespace']
                                    # Build full Java qualified name with base package
                                    java_qualified_name = self.join_packages(self.base_package, avro_namespace).replace('/', '.').lower() + '.' + type_name
                                else:
                                    java_qualified_name = type_name
                                simple_union_name = java_type.split('.')[-1]
                                
                                # Check if this union member is an enum or class
                                member_type_kind = self.generated_types_java_package.get(java_qualified_name)
                                if member_type_kind == "enum":
                                    # For enums, use the first enum value
                                    member_value = self.get_test_value(java_qualified_name, package)
                                    return f'new {simple_union_name}({member_value})'
                                else:
                                    # For classes, create a new instance using createTestInstance()
                                    # Use fully qualified name to avoid conflicts with field names
                                    return f'new {simple_union_name}({java_qualified_name}.createTestInstance())'
                        elif union_type != 'null' and isinstance(union_type, str):
                            # It's a simple type - convert from Avro type to Java type
                            simple_union_name = java_type.split('.')[-1]
                            # Convert Avro primitive type to Java type
                            java_primitive_type = self.convert_avro_type_to_java('_test', '_field', union_type, package)
                            simple_value = self.get_test_value(java_primitive_type.type_name, package)
                            return f'new {simple_union_name}({simple_value})'
                # Fallback: create an empty union instance
                simple_name = java_type.split('.')[-1]
                return f'new {simple_name}()'
        
        return test_values.get(java_type, f'new {java_type}()')
    
    def generate_discriminated_union_base_classes(self):
        """Generate abstract base classes for discriminated unions with Jackson annotations"""
        if not self.jackson_annotations or not self.discriminated_unions:
            return
        
        for union_name, subtypes in self.discriminated_unions.items():
            if not subtypes:
                continue
            
            # Get the first subtype to determine package and discriminator field
            first_subtype = subtypes[0]
            package = first_subtype['package']
            
            # Find the discriminator field (should have 'discriminator': true)
            discriminator_field = None
            discriminator_values = {}
            
            for subtype_info in subtypes:
                schema = subtype_info['schema']
                for field in schema.get('fields', []):
                    if field.get('discriminator'):
                        discriminator_field = field['name']
                        if 'const' in field:
                            discriminator_values[subtype_info['class_name']] = field['const']
                        break
            
            if not discriminator_field:
                print(f"WARN: Could not find discriminator field for union {union_name}")
                continue
            
            # Generate the abstract base class
            class_definition = f"/**\n * Abstract base class for {union_name} discriminated union\n */\n"
            
            # Add Jackson @JsonTypeInfo annotation
            class_definition += f'@JsonTypeInfo(\n'
            class_definition += f'{INDENT}use = JsonTypeInfo.Id.NAME,\n'
            class_definition += f'{INDENT}include = JsonTypeInfo.As.EXISTING_PROPERTY,\n'
            class_definition += f'{INDENT}property = "{discriminator_field}",\n'
            class_definition += f'{INDENT}visible = true\n'
            class_definition += f')\n'
            
            # Add Jackson @JsonSubTypes annotation
            class_definition += f'@JsonSubTypes({{\n'
            for i, subtype_info in enumerate(subtypes):
                class_name = subtype_info['class_name']
                disc_value = discriminator_values.get(class_name, class_name)
                comma = ',' if i < len(subtypes) - 1 else ''
                class_definition += f'{INDENT}@JsonSubTypes.Type(value = {class_name}.class, name = "{disc_value}"){comma}\n'
            class_definition += f'}})\n'
            
            # Abstract class declaration
            class_definition += f'public abstract class {union_name} {{\n'
            
            # Add the discriminator field getter (abstract)
            class_definition += f'{INDENT}/**\n{INDENT} * Gets the discriminator value\n{INDENT} * @return the type discriminator\n{INDENT} */\n'
            class_definition += f'{INDENT}public abstract String get{pascal(discriminator_field)}();\n'
            
            class_definition += '}\n'
            
            # Write the file
            dir_path = os.path.join(self.output_dir, package.replace('.', os.sep))
            os.makedirs(dir_path, exist_ok=True)
            file_path = os.path.join(dir_path, f"{union_name}.java")
            
            # Build the full file content with imports
            imports = [
                'import com.fasterxml.jackson.annotation.JsonSubTypes;',
                'import com.fasterxml.jackson.annotation.JsonTypeInfo;'
            ]
            
            full_content = f"package {package};\n\n"
            full_content += '\n'.join(imports) + '\n\n'
            full_content += class_definition
            
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(full_content)
            
            print(f"Generated discriminated union base class: {union_name}")

    def convert_schema(self, schema: JsonNode, output_dir: str):
        """Converts Avro schema to Java"""
        if not isinstance(schema, list):
            schema = [schema]
        
        # Build type dictionary for inline schema resolution (like C# does)
        self.type_dict = build_flat_type_dict(schema)
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        base_output_dir = output_dir  # Store the base directory before changing it
        pom_path = os.path.join(output_dir, "pom.xml")
        if not os.path.exists(pom_path):
            package_elements = self.base_package.split('.') if self.base_package else ["com", "example"]
            groupid = '.'.join(package_elements[:-1]) if len(package_elements) > 1 else package_elements[0]
            artifactid = package_elements[-1]
            with open(pom_path, 'w', encoding='utf-8') as file:
                file.write(POM_CONTENT.format(
                    groupid=groupid, 
                    artifactid=artifactid, 
                    AVRO_VERSION=AVRO_VERSION, 
                    JACKSON_VERSION=JACKSON_VERSION, 
                    JDK_VERSION=JDK_VERSION, 
                    JUNIT_VERSION=JUNIT_VERSION,
                    MAVEN_COMPILER_VERSION=MAVEN_COMPILER_VERSION,
                    MAVEN_SUREFIRE_VERSION=MAVEN_SUREFIRE_VERSION,
                    PACKAGE=self.base_package))
        output_dir = os.path.join(
            output_dir, "src/main/java".replace('/', os.sep))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.output_dir = output_dir
        for avro_schema in (x for x in schema if isinstance(x, dict)):
            self.generate_class_or_enum(avro_schema, '')
        self.generate_discriminated_union_base_classes()
        self.generate_tests(base_output_dir)

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
    if not package_name:
        package_name = os.path.splitext(os.path.basename(java_file_path))[0].replace('-', '_').lower()
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
