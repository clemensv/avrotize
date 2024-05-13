# Java Code Generation in Avrotize

Avrotize is can generates Java classes from Avro schema files with the "a2java"
command. The generated classes reflect the type model described by the Avro
schema.

With the `avro_annotation` option, the code generator is an alternative to the
`avro-tools` utility provided by the Avro project. Unlike `avro-tools`, Avrotize
generates classes that directly support type unions and it allows combining Avro
annotations with annotations for
[Jackson](https://github.com/FasterXML/jackson) JSON serialization.

With the `jackson_annotation` option, the code generator emits annotations for
Jackson. This option can be used standalone and is not dependent on the
`avro_annotation` option, which means that Avro Schema can be used to generate
classes with Jackson serialization annotations as an alternative to
JSON Schema, without the Avro serialization framework being required. The
generated classes fully support type unions (equivalent to JSN Schema's `oneOf`)
without requiring a "discriminator" field, but rather deduce the type from the
serialized data's structure.

## Code generation for example schema `RecordType1`

```json
{
    "type": "record",
    "name": "RecordType1",
    "namespace": "MyNamespace",
    "fields": [
        {
            "name": "field1",
            "type": "string"
        },
        {
            "name": "field2",
            "type": "int"
        },
        {
            "name": "field3",
            "type": "string"
        },
        {
            "name": "field4",
            "type": "double"
        },
        {
            "name": "field5",
            "type": "long"
        },
        {
            "name": "fieldB",
            "type": ["string", "null"]
        }
    ]
}
```

The following is an example of the generated code for the schema above, with the
`avro_annotation` option and the `system_text_json_annotation` option turned on.
We will discuss the generated code in detail below and which parts are generated
by which option.

We omit the import statements and the package declaration for brevity.

If the `avro_annotation` option is used, the generated class will implement the
`SpecificRecord` interface. This interface is part of the Avro serialization
framework and is used to serialize and deserialize Avro records. 

```java
public class RecordType1 implements SpecificRecord {
    public RecordType1() {}
```

All fields of the Avro record are represented as properties in the generated
class. The properties are annotated with `@JsonProperty` from Jackson if the
`jackon_annotation` option is used. 

```java	
    @JsonProperty("field1")
    private String field1;
    public String getField1() { return field1; }
    public void setField1(String field1) { this.field1 = field1; }

    @JsonProperty("field2")
    private int field2;
    public int getField2() { return field2; }
    public void setField2(int field2) { this.field2 = field2; }

    @JsonProperty("field3")
    private String field3;
    public String getField3() { return field3; }
    public void setField3(String field3) { this.field3 = field3; }

    @JsonProperty("field4")
    private double field4;
    public double getField4() { return field4; }
    public void setField4(double field4) { this.field4 = field4; }

    @JsonProperty("field5")
    private long field5;
    public long getField5() { return field5; }
    public void setField5(long field5) { this.field5 = field5; }

    @JsonProperty("fieldB")
    private String fieldB;
    public String getFieldB() { return fieldB; }
    public void setFieldB(String fieldB) { this.fieldB = fieldB; }
```

If the `avro_annotation` option is used, the generated class will have constructor
that takes an Avro `GenericRecord` as an argument. This constructor is used to
convert a `GenericRecord` to an instance of the generated class.

```java	

    public RecordType1(GenericData.Record record) {
        for( int i = 0; i < record.getSchema().getFields().size(); i++ ) {
            this.put(i, record.get(i));
        }
    }
```	

The generated class will have a method `getSchema()` that returns the Avro schema
of the record. This method is required by the Avro serialization framework.

```java	
    public static Schema AvroSchema = new Schema.Parser().parse(
    "{\"type\": \"record\", \"name\": \"RecordType1\", \"fields\": [{\"name\": \"field1\", \"type\": "+
    "\"string\"}, {\"name\": \"field2\", \"type\": \"int\"}, {\"name\": \"field3\", \"type\": \"string"+
    "\"}, {\"name\": \"field4\", \"type\": \"double\"}, {\"name\": \"field5\", \"type\": \"long\"}, {\""+
    "name\": \"fieldB\", \"type\": [\"string\", \"null\"]}], \"namespace\": \"com.example.avro\"}");

    @JsonIgnore
    @Override
    public Schema getSchema(){ return AvroSchema; }
```

The generated class will have methods `get()` and `put()` that are required by the
Avro serialization framework. These methods are used to access the fields of the
record during serialization.

```java
    @Override
    public Object get(int field$) {
        switch (field$) {
            case 0: return this.field1;
            case 1: return this.field2;
            case 2: return this.field3;
            case 3: return this.field4;
            case 4: return this.field5;
            case 5: return this.fieldB;
            default: throw new AvroRuntimeException("Bad index: " + field$);
        }
    }

    @Override
    public void put(int field$, Object value$) {
        switch (field$) {
            case 0: this.field1 = value$.toString(); break;
            case 1: this.field2 = (int)value$; break;
            case 2: this.field3 = value$.toString(); break;
            case 3: this.field4 = (double)value$; break;
            case 4: this.field5 = (long)value$; break;
            case 5: this.fieldB = value$.toString(); break;
            default: throw new AvroRuntimeException("Bad index: " + field$);
        }
    }
```

The 'toByteArray()' method converts the object to a byte array. The method takes
the content type of the byte array as an argument. The method first checks the
content type and then converts the object to a byte array using the appropriate
serialization method.

The following encodings are supported:

| Enabled Option | Content Type String | Encoding |
|----------------|---------------------|----------|
| avro_annotation | `avro/binary` | Avro binary encoding |
| avro_annotation | `avro/vnd.apache.avro+avro` | Avro binary encoding |
| avro_annotation | `avro/vnd.apache.avro+avro+gzip` | Avro binary encoding with GZIP compression |
| avro_annotation | `avro/json` | Avro JSON encoding |
| avro_annotation | `application/vnd.apache.avro+json` | Avro JSON encoding |
| avro_annotation | `avro/vnd.apache.avro+json+gzip` | Avro JSON encoding with GZIP compression |
| system_text_json_annotation | `application/json` | JSON encoding |
| system_text_json_annotation | `application/json+gzip` | JSON encoding with GZIP compression |

```java	
    /**
     * Converts the object to a byte array
     * @param contentType the content type of the byte array
     * @return the byte array
     */
    public byte[] toByteArray(String contentType) throws UnsupportedOperationException,JsonProcessingException,IOException  {
        byte[] result = null;
        String mediaType = contentType.split(";")[0].trim().toLowerCase();
        
        if ( mediaType == "avro/binary" || mediaType == "application/vnd.apache.avro+avro") {
            DatumWriter<RecordType1> writer = new SpecificDatumWriter<RecordType1>(RecordType1.AvroSchema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(this, encoder);
            encoder.flush();
            result = out.toByteArray();
        }
        else if ( mediaType == "avro/json" || mediaType == "application/vnd.apache.avro+json") {
            DatumWriter<RecordType1> writer = new SpecificDatumWriter<RecordType1>(RecordType1.AvroSchema);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().jsonEncoder(RecordType1.AvroSchema, out);
            writer.write(this, encoder);
            encoder.flush();
            result = out.toByteArray();
        }
        if ( mediaType == "application/json") {    
            result = new ObjectMapper().writeValueAsBytes(this);
        }
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
        
        if ( result != null ) { return result; }throw new UnsupportedOperationException("Unsupported media type + mediaType");
    }
```

The 'fromData()' method converts a byte array to an object. The method takes the
byte array and the content type of the byte array as arguments. The method first
checks the content type and then converts the byte array to an object using the
appropriate deserialization method.

The supported encodings are the same as for `toByteArray()`

```java
    /**
     * Converts the data to an object
     * @param data the data to convert
     * @param contentType the content type of the data
     * @return the object
     */
    public static RecordType1 fromData(Object data, String contentType) throws UnsupportedOperationException,JsonProcessingException, IOException,IOException  {
        if ( data instanceof RecordType1) return (RecordType1)data;
        String mediaType = contentType.split(";")[0].trim().toLowerCase();
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
        
        if ( mediaType == "avro/binary" || mediaType == "application/vnd.apache.avro+avro") {
            DatumReader<RecordType1> reader = new SpecificDatumReader<RecordType1>(RecordType1.AvroSchema);
            if (data instanceof byte[]) {
                return reader.read(new RecordType1(), DecoderFactory.get().binaryDecoder((byte[])data, null));
            } else if (data instanceof InputStream) {
                return reader.read(new RecordType1(), DecoderFactory.get().binaryDecoder((InputStream)data, null));
            }
            throw new UnsupportedOperationException("Data is not of a supported type for Avro conversion to RecordType1");
        } else if ( mediaType == "avro/json" || mediaType == "application/vnd.apache.avro+json") {
            DatumReader<RecordType1> reader = new SpecificDatumReader<RecordType1>(RecordType1.AvroSchema);
            if (data instanceof byte[]) {
                return reader.read(new RecordType1(), DecoderFactory.get().jsonDecoder(RecordType1.AvroSchema, new ByteArrayInputStream((byte[])data)));
            } else if (data instanceof InputStream) {
                return reader.read(new RecordType1(), DecoderFactory.get().jsonDecoder(RecordType1.AvroSchema, (InputStream)data));
            } else if (data instanceof String) {
                return reader.read(new RecordType1(), DecoderFactory.get().jsonDecoder(RecordType1.AvroSchema, (String)data));
            }
            throw new UnsupportedOperationException("Data is not of a supported type for Avro conversion to RecordType1");
        }
        if ( mediaType == "application/json") {
            if (data instanceof byte[]) {
                ByteArrayInputStream stream = new ByteArrayInputStream((byte[]) data);
                return (new ObjectMapper()).readValue(stream, RecordType1.class);
            }
            else if (data instanceof InputStream) {
                return (new ObjectMapper()).readValue((InputStream)data, RecordType1.class);
            }
            else if (data instanceof JsonNode) {
                return (new ObjectMapper()).readValue(((JsonNode)data).toString(), RecordType1.class);
            }
            else if ( data instanceof String) {
                return (new ObjectMapper()).readValue(((String)data), RecordType1.class);
            }
            throw new UnsupportedOperationException("Data is not of a supported type for JSON conversion to RecordType1");
        }
        throw new UnsupportedOperationException("Unsupported media type "+ contentType);
    }
```

The 'isJsonMatch()' method checks if a JSON node matches the schema. The method
takes a JSON node as an argument and returns a boolean value indicating whether
the JSON node matches the schema.

```java
    /**
     * Checks if the JSON node matches the schema
    @param node The JSON node to check */
    public static boolean isJsonMatch(com.fasterxml.jackson.databind.JsonNode node)
    {
        return (node.has("field1") && node.get("field1").isTextual()) && 
            (node.has("field2") && node.get("field2").canConvertToInt()) && 
            (node.has("field3") && node.get("field3").isTextual()) && 
            (node.has("field4") && node.get("field4").isDouble()) && 
            (node.has("field5") && node.get("field5").canConvertToLong()) && 
            (node.has("fieldB") && node.get("fieldB").isTextual());
    }
}