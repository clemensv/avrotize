# C# Code Generation in Avrotize

Avrotize is can generates C# classes from Avro schema files with the "a2csharp" command. The generated classes reflect the type model described by the Avro schema. 

With the `avro_annotation` option, the code generator is an alternative to the `avrogen` tool provided by the Avro project. Unlike `avrogen`, Avrotize generates classes that directly support type unions and it allows combining Avro annotations
with annotations for `System.Text.Json` serialization.

With the `system_text_json_annotation` option, the code generator emits annotations for `System.Text.Json` serialization. This option can be used standalone and is not dependent on the `avro_annotation` option, which means that Avro Schema 
can be used to generate classes with `System.Text.Json` serialization annotations as an alternative to JSON Schema, without
the Avro serialization framework being required. The generated classes fully support type unions (equivalent to JSN Schema's `oneOf`) without requiring a "discriminator" field, but rather deduce the type from the serialized data's structure.

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

### Pragma Directives

The generated code starts with two pragma directives to suppress warnings about
uninitialized fields and nullable fields. These directives are necessary because
the generated code uses nullable reference types and shall work for C# 8.0 and
later, with the nullable reference types feature enabled.

```csharp
#pragma warning disable CS8618
#pragma warning disable CS8603
```

### Using Directives

The generated code includes the necessary using directives for the `System`,
`System.Collections.Generic`, `System.Text.Json`, and
`System.Text.Json.Serialization` namespaces. The Avro references are generated
without the using directive.

```csharp
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
```

### Namespace Declaration

The generated code includes a namespace declaration that is derived from the
namespace of the Avro schema. The namespace is used to avoid naming conflicts
with other classes in the project. When the `--namespace` option is provided,
the given namespace is prepended to the namespace found in the Avro schema.

```csharp
namespace MyNamespace
{
```

### Record Class

The generated code includes a class declaration for the record type defined in
the Avro schema. The class name is derived from the name of the record type in
the Avro schema.

If the `avro_annotation` option is provided, the class implements the
`ISpecificRecord` interface from the Avro library. The interface provides
methods to access the fields of the record and to convert the record to and from
byte arrays. The interface is used by the Avro serialization framework to
serialize and deserialize the record.

The Avro serialization uses the 'Specific' API, which is a strongly typed API
that generates classes for each record type in the Avro schema. Avro deserialization
however uses the 'Generic' API due to limitations in how the Specific API resolves 
type references during union handling.

```csharp	
    /// <summary>
    /// RecordType1
    /// </summary>
    public partial class RecordType1 : global::Avro.Specific.ISpecificRecord
    {
```

### Fields

The generated code includes fields for each field in the record type defined in
the Avro schema, with Avro types mapped to C# types.

The mapping for Avro types to C# types is as follows:

| Avro Type | C# Type |
|-----------|---------|
| string    | string  |
| int       | int     |
| long      | long    |
| float     | float   |
| double    | double  |
| boolean   | bool    |
| bytes     | byte[]  |
| fixed     | byte[]  |
| enum      | enum    |
| array     | List<T> |
| map       | Dictionary<string, T> |
| union     | xxxUnion() class |

| Logical Type | C# Type |
|--------------|---------|
| decimal      | decimal |
| date         | DateTime |
| time-millis  | TimeSpan |
| time-micros  | TimeSpan |
| timestamp-millis | DateTime |
| timestamp-micros | DateTime |
| duration     | TimeSpan |

If the `system_text_json_annotation` option is used, fields are annotated with
the `JsonPropertyName` attribute from the `System.Text.Json.Serialization`
namespace. The attribute specifies the name of the field in the JSON
representation of the record.

```csharp
        /// <summary>
        /// Field1
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("field1")]
        public string Field1 { get; set; }
        /// <summary>
        /// Field2
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("field2")]
        public int Field2 { get; set; }
        /// <summary>
        /// Field3
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("field3")]
        public string Field3 { get; set; }
        /// <summary>
        /// Field4
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("field4")]
        public double Field4 { get; set; }
        /// <summary>
        /// Field5
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("field5")]
        public long Field5 { get; set; }
        /// <summary>
        /// FieldB
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("fieldB")]
        public string? FieldB { get; set; }

```

### Constructor

The generated code includes a default constructor for the record type. The
constructor initializes the fields of the record to their default values.

```csharp
        /// <summary>
        /// Default constructor
        ///</summary>
        public RecordType1()
        {
        }
```

### Constructor from Avro GenericRecord

If the `avro_annotation` option is used, the generated code includes a
constructor that takes an Avro `GenericRecord` object as a parameter. The
constructor initializes the fields of the record from the values in the
`GenericRecord`.


```csharp
        /// <summary>
        /// Constructor from Avro GenericRecord
        ///</summary>
        public RecordType1(global::Avro.Generic.GenericRecord obj)
        {
            global::Avro.Specific.ISpecificRecord self = this;
            for (int i = 0; obj.Schema.Fields.Count > i; ++i)
            {
                self.Put(i, obj.GetValue(i));
            }
        }
```

### Avro Schema

If the `avro_annotation` option is used, the generated code includes a static
field that contains the Avro schema for the record type. The schema is parsed
from a JSON string that represents the schema.

```csharp
        /// <summary>
        /// Avro schema for this class
        /// </summary>
        public static global::Avro.Schema AvroSchema = global::Avro.Schema.Parse(
        "{\"type\": \"record\", \"name\": \"RecordType1\", \"fields\": [{\"name\": \"field1\", \"type\": "+
        "\"string\"}, {\"name\": \"field2\", \"type\": \"int\"}, {\"name\": \"field3\", \"type\": \"string"+
        "\"}, {\"name\": \"field4\", \"type\": \"double\"}, {\"name\": \"field5\", \"type\": \"long\"}, {\""+
        "name\": \"fieldB\", \"type\": [\"string\", \"null\"]}], \"namespace\": \"MyNamespace\"}");
        global::Avro.Schema global::Avro.Specific.ISpecificRecord.Schema => AvroSchema;
```

### ISpecificRecord Interface Implementation

If the `avro_annotation` option is used, the generated code includes
implementations of the `Get` and `Put` methods from the Avro framework's
`ISpecificRecord` interface. The methods provide access to the fields of the
record and allow the record to be serialized and deserialized by the Avro
serialization framework.

```csharp    
        object global::Avro.Specific.ISpecificRecord.Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return this.Field1;
                case 1: return this.Field2;
                case 2: return this.Field3;
                case 3: return this.Field4;
                case 4: return this.Field5;
                case 5: return this.FieldB;
                default: throw new global::Avro.AvroRuntimeException($"Bad index {fieldPos} in Get()");
            }
        }
        void global::Avro.Specific.ISpecificRecord.Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: this.Field1 = (string)fieldValue; break;
                case 1: this.Field2 = (int)fieldValue; break;
                case 2: this.Field3 = (string)fieldValue; break;
                case 3: this.Field4 = (double)fieldValue; break;
                case 4: this.Field5 = (long)fieldValue; break;
                case 5: this.FieldB = (string?)fieldValue; break;
                default: throw new global::Avro.AvroRuntimeException($"Bad index {fieldPos} in Put()");
            }
        }
```

### ToByteArray Method

If either or both the `avro_annotation` and `system_text_json_annotation`
options are used, the generated code includes a `ToByteArray` method that
converts the record to a byte array. The method takes a content type string as a
parameter that specifies the encoding of the data. The method encodes the record
in the specified format and returns the encoded data as a byte array.

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


```csharp    
        /// <summary>
        /// Converts the object to a byte array
        /// </summary>
        /// <param name="contentTypeString">The content type string of the desired encoding</param>
        /// <returns>The encoded data</returns>
        public byte[] ToByteArray(string contentTypeString)
        {
            var contentType = new System.Net.Mime.ContentType(contentTypeString);
            byte[]? result = null;
            
            if (contentType.MediaType.StartsWith("avro/binary") || contentType.MediaType.StartsWith("application/vnd.apache.avro+avro"))
            {
                var stream = new System.IO.MemoryStream();
                var writer = new global::Avro.Specific.SpecificDatumWriter<RecordType1>(RecordType1.AvroSchema);
                var encoder = new global::Avro.IO.BinaryEncoder(stream);
                writer.Write(this, encoder);
                encoder.Flush();
                result = stream.ToArray();
            }
            else if (contentType.MediaType.StartsWith("avro/json") || contentType.MediaType.StartsWith("application/vnd.apache.avro+json"))
            {
                var stream = new System.IO.MemoryStream();
                var writer = new global::Avro.Specific.SpecificDatumWriter<RecordType1>(RecordType1.AvroSchema);
                var encoder = new global::Avro.IO.JsonEncoder(RecordType1.AvroSchema, stream);
                writer.Write(this, encoder);
                encoder.Flush();
                result = stream.ToArray();
            }
            if (contentType.MediaType.StartsWith(System.Net.Mime.MediaTypeNames.Application.Json))
            {
                result = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(this);
            }
            if (result != null && contentType.MediaType.EndsWith("+gzip"))
            {
                var stream = new System.IO.MemoryStream();
                using (var gzip = new System.IO.Compression.GZipStream(stream, System.IO.Compression.CompressionMode.Compress))
                {
                    gzip.Write(result, 0, result.Length);
                }
                result = stream.ToArray();
            }
            
            return ( result != null ) ? result : throw new System.NotSupportedException($"Unsupported media type {contentType.MediaType}");
            
        }
```

### FromData Method

If either or both the `avro_annotation` and `system_text_json_annotation`
options are used, the generated code includes a `FromData` method that converts
a byte array to a record object. The method takes the encoded data and a content
type string as parameters and returns the decoded record object.

The method supports the same encodings as the `ToByteArray` method.

```csharp    
        /// <summary>
        /// Creates an object from the data
        /// </summary>
        /// <param name="data">The input data to convert</param>
        /// <param name="contentTypeString">The content type string of the derired encoding</param>
        /// <returns>The converted object</returns>
        public static RecordType1? FromData(object? data, string? contentTypeString )
        {
            if ( data == null ) return null;
            if ( data is RecordType1) return (RecordType1)data;
            if ( contentTypeString == null ) contentTypeString = System.Net.Mime.MediaTypeNames.Application.Octet;
            var contentType = new System.Net.Mime.ContentType(contentTypeString);
```

If the content type string specifies a GZIP encoding, the first step is to
decompress the data and make it available to the subsequent steps.

```csharp
            if ( contentType.MediaType.EndsWith("+gzip"))
            {
                var stream = data switch
                {
                    System.IO.Stream s => s, System.BinaryData bd => bd.ToStream(), byte[] bytes => new System.IO.MemoryStream(bytes),
                    _ => throw new NotSupportedException("Data is not of a supported type for gzip decompression")
                };
                using (var gzip = new System.IO.Compression.GZipStream(stream, System.IO.Compression.CompressionMode.Decompress))
                {
                    System.IO.MemoryStream memoryStream = new System.IO.MemoryStream();
                    gzip.CopyTo(memoryStream);
                    memoryStream.Position = 0;
                    data = memoryStream.ToArray();
                }
            }
```

The method then decodes the data based on the content type string and returns
the decoded record object. 

```csharp
            if ( contentType.MediaType.StartsWith("avro/") || contentType.MediaType.StartsWith("application/vnd.apache.avro") )
            {
                var stream = data switch
                {
                    System.IO.Stream s => s, System.BinaryData bd => bd.ToStream(), byte[] bytes => new System.IO.MemoryStream(bytes),
                    _ => throw new NotSupportedException("Data is not of a supported type for conversion to Stream")
                };
                #pragma warning disable CS8625 // Cannot convert null literal to non-nullable reference type.
                if (contentType.MediaType.StartsWith("avro/binary") || contentType.MediaType.StartsWith("application/vnd.apache.avro+avro"))
                {
                    var reader = new global::Avro.Generic.GenericDatumReader<global::Avro.Generic.GenericRecord>(RecordType1.AvroSchema, RecordType1.AvroSchema);
                    return new RecordType1(reader.Read(null, new global::Avro.IO.BinaryDecoder(stream)));
                }
                if ( contentType.MediaType.StartsWith("avro/json") || contentType.MediaType.StartsWith("application/vnd.apache.avro+json"))
                {
                    var reader = new global::Avro.Generic.GenericDatumReader<global::Avro.Generic.GenericRecord>(RecordType1.AvroSchema, RecordType1.AvroSchema);
                    return new RecordType1(reader.Read(null, new global::Avro.IO.JsonDecoder(RecordType1.AvroSchema, stream)));
                }
                #pragma warning restore CS8625
            }
            if ( contentType.MediaType.StartsWith(System.Net.Mime.MediaTypeNames.Application.Json))
            {
                if (data is System.Text.Json.JsonElement) 
                {
                    return System.Text.Json.JsonSerializer.Deserialize<RecordType1>((System.Text.Json.JsonElement)data);
                }
                else if ( data is string)
                {
                    return System.Text.Json.JsonSerializer.Deserialize<RecordType1>((string)data);
                }
                else if (data is System.BinaryData)
                {
                    return ((System.BinaryData)data).ToObjectFromJson<RecordType1>();
                }
                else if (data is byte[])
                {
                    return System.Text.Json.JsonSerializer.Deserialize<RecordType1>(new ReadOnlySpan<byte>((byte[])data));
                }
                else if (data is System.IO.Stream)
                {
                    return System.Text.Json.JsonSerializer.DeserializeAsync<RecordType1>((System.IO.Stream)data).Result;
                }
            }
            throw new System.NotSupportedException($"Unsupported media type {contentType.MediaType}");
            
        }
```

### IsJsonMatch Method

If the `system_text_json_annotation` option is used, the generated code includes
an `IsJsonMatch` method that checks if a JSON element matches the schema. The
method takes a `JsonElement` object as a parameter and returns a boolean value
that indicates whether the JSON element matches the schema.

The method checks if the JSON element contains the fields defined in the Avro
schema and if the values of the fields have the correct data types.

The method is used in the `Read` method of union classes (discussed below) to
determine whether the JSON element matches the schema of one of the types in the
union.

```csharp
    
        /// <summary>
        /// Checks if the JSON element matches the schema
        /// </summary>
        /// <param name="element">The JSON element to check</param>
        public static bool IsJsonMatch(System.Text.Json.JsonElement element)
        {
            return (element.TryGetProperty("field1", out System.Text.Json.JsonElement field1) && 
                        (field1.ValueKind == System.Text.Json.JsonValueKind.String)) && 
                (element.TryGetProperty("field2", out System.Text.Json.JsonElement field2) && 
                        (field2.ValueKind == System.Text.Json.JsonValueKind.Number)) && 
                (element.TryGetProperty("field3", out System.Text.Json.JsonElement field3) && 
                        (field3.ValueKind == System.Text.Json.JsonValueKind.String)) && 
                (element.TryGetProperty("field4", out System.Text.Json.JsonElement field4) && 
                        (field4.ValueKind == System.Text.Json.JsonValueKind.Number)) && 
                (element.TryGetProperty("field5", out System.Text.Json.JsonElement field5) && 
                        (field5.ValueKind == System.Text.Json.JsonValueKind.Number)) && 
                (!element.TryGetProperty("fieldB", out System.Text.Json.JsonElement fieldB) || 
                        (fieldB.ValueKind == System.Text.Json.JsonValueKind.String) || 
                         fieldB.ValueKind == System.Text.Json.JsonValueKind.Null);
        }
    }
}
```

## Handling Record Type Unions

The generated code supports type unions in Avro schemas. A type union is
represented as a C# class that contains fields for each type in the union.

Extending from the example above, the following is a property declaration for a
type union where the field schema is a union of `RecordType1` and `RecordType2`:

```json
{
    "name": "document",
    "type": ["RecordType1", "RecordType2"]
}
```

The `RecordType2` looks similar to `RecordType1` shown above, but is structually
different. As with JSON Schema's `oneOf` clause, the union is resolved by the
structure of the serialized data and the structure MUST be unique for each type. 

In this case: 

- `field3` is a boolean instead of a string
- `field4` is a double or null instead of a double
- `field5` is a long or null instead of a long
- `fieldA` instead of `fieldB`

> A coming version of Avrotize will support a `const` extension to the Avro
> schema to define a constant values for fields, which can then be used as
> discriminator for type unions.

```json
{
    "type": "record",
    "name": "RecordType2",
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
            "type": "boolean"
        },
        {
            "name": "field4",
            "type": ["double", "null"]
        },
        {
            "name": "field5",
            "type": ["long", "null"]
        },
        {
            "name": "fieldA",
            "type": "string"
        }
    ]
}
```

The following generated code refers to the `DocumentUnion` class that is
produced for the `document` field shown above. If the
`system_text_json_annotation` option is used, the union class is annotated with
the `JsonConverter` attribute from the `System.Text.Json.Serialization`
namespace. The attribute specifies the converter class that is used to serialize
and deserialize the union.

```csharp
    /// <summary>
    /// Document
    /// </summary>
    [System.Text.Json.Serialization.JsonPropertyName("document")]
    [System.Text.Json.Serialization.JsonConverter(typeof(DocumentUnion))]
    public DocumentUnion Document { get; set; }
```

The `DocumentUnion` class is embedded in the generated record class, into a
separate file, but into the same partial class.

If the `system_text_json_annotation` option is used, the `DocumentUnion` class
is derived from the `JsonConverter` class from the
`System.Text.Json.Serialization` namespace. The class provides methods to
serialize and deserialize the union.

```csharp
    /// <summary>
    /// Union class for document
    /// </summary>
    public sealed class DocumentUnion: System.Text.Json.Serialization.JsonConverter<DocumentUnion>
    {
```

### Constructor

The generated code includes constructors for each type in the union. The
constructors take the value of the type as a parameter and initialize the
corresponding field in the union.

```csharp
        /// <summary>
        /// Default constructor
        /// </summary>
        public DocumentUnion() { }
        /// <summary>
        /// Constructor for RecordType1 values
        /// </summary>
        public DocumentUnion(global::MyNamespace.RecordType1? RecordType1)
        {
            this.RecordType1 = RecordType1;
        }
        /// <summary>
        /// Constructor for RecordType2 values
        /// </summary>
        public DocumentUnion(global::MyNamespace.RecordType2? RecordType2)
        {
            this.RecordType2 = RecordType2;
        }
```

### FromObject Method

The generated code includes a static `FromObject` method that converts an object
to a union value. The method takes an object as a parameter and returns the
union value that corresponds to the type of the object. This is a factory method
instead of a constructor to disambiguate from generated constructors for maps,
which are also represented as `Object` in some cases.

If the `avro_annotation` option is used, the method checks whether the object is
a GenericRecord and creates a union value from the GenericRecord with the respective
constructor.

```csharp
        /// <summary>
        /// Constructor for Avro decoder
        /// </summary>
        internal static DocumentUnion FromObject(object obj)
        {
            if (obj is global::Avro.Generic.GenericRecord)
            {
                return new DocumentUnion((global::Avro.Generic.GenericRecord)obj);
            }
            var self = new DocumentUnion();
            if (obj is global::MyNamespace.RecordType1)
            {
                self.RecordType1 = (global::MyNamespace.RecordType1)obj;
                return self;
            }
            if (obj is global::MyNamespace.RecordType2)
            {
                self.RecordType2 = (global::MyNamespace.RecordType2)obj;
                return self;
            }
            throw new NotSupportedException("No record type matched the type");
        }
```

### Constructor from Avro GenericRecord

If the `avro_annotation` option is used, the generated code includes a
constructor that takes an Avro `GenericRecord` object as a parameter. The
constructor initializes the fields of the union from the values in the
`GenericRecord`. 

```csharp    
        /// <summary>
        /// Constructor from Avro GenericRecord
        /// </summary>
        public DocumentUnion(global::Avro.Generic.GenericRecord obj)
        {
            if (obj.Schema.Fullname == global::MyNamespace.RecordType1.AvroSchema.Fullname)
            {
                this.RecordType1 = new global::MyNamespace.RecordType1(obj);
                return;
            }
            if (obj.Schema.Fullname == global::MyNamespace.RecordType2.AvroSchema.Fullname)
            {
                this.RecordType2 = new global::MyNamespace.RecordType2(obj);
                return;
            }
            throw new NotSupportedException("No record type matched the type");
        }
```

### Properties

The generated code includes properties for each field in the union. The properties
are read-only and return the value of the field.


```csharp
        /// <summary>
        /// Gets the RecordType1 value
        /// </summary>
        public global::MyNamespace.RecordType1? RecordType1 { get; private set; } = null;
        /// <summary>
        /// Gets the RecordType2 value
        /// </summary>
        public global::MyNamespace.RecordType2? RecordType2 { get; private set; } = null;
```
    

### ToObject Method

The generated code includes a `ToObject` method that yields the current value of the union.

```csharp
        /// <summary>
        /// Yields the current value of the union
        /// </summary>

        public Object ToObject()
        {
            if (RecordType1 != null) {
                return RecordType1;
            }
            if (RecordType2 != null) {
                return RecordType2;
            }
            throw new NotSupportedException("No record type is set in the union");
        }
```

Since union classes are not a feature of the Avro serialization framework, they
are effectively hidden from the Avro serializer and deserializer logic in the
containing record's `ISpecificRecord.Put` and `ISpecificRecord.Get` methods.

In `Get`, the union class instance's `ToObject` method is called to get the
value of the union, which is then returned. In `Put`, the union class instance's
`FromObject` method is called to set the value of the union.

```csharp
object global::Avro.Specific.ISpecificRecord.Get(int fieldPos)
{
    switch (fieldPos)
    {
        case 0: return this.Document?.ToObject();
        default: throw new global::Avro.AvroRuntimeException($"Bad index {fieldPos} in Get()");
    }
}
void global::Avro.Specific.ISpecificRecord.Put(int fieldPos, object fieldValue)
{
    switch (fieldPos)
    {
        case 0: this.Document = DocumentUnion.FromObject(fieldValue); break;
        default: throw new global::Avro.AvroRuntimeException($"Bad index {fieldPos} in Put()");
    }
}
```

### Read Method

If the `system_text_json_annotation` option is used, the generated code includes a
`Read` method that reads the JSON representation of the object. The method takes a
`Utf8JsonReader` object as a parameter and returns the union value that corresponds
to the JSON data.

The method checks if the JSON element matches the schema of one of the types in the
union using the `IsJsonMatch` method. If the JSON element matches the schema of one
of the types, the method creates a union value from the JSON element with the respective
constructor.

```csharp    
        /// <summary>
        /// Reads the JSON representation of the object.
        /// </summary>
        public override DocumentUnion? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            var element = JsonElement.ParseValue(ref reader);
            if (global::MyNamespace.RecordType1.IsJsonMatch(element))
            {
                return new DocumentUnion(global::MyNamespace.RecordType1.FromData(element, System.Net.Mime.MediaTypeNames.Application.Json));
            }
            if (global::MyNamespace.RecordType2.IsJsonMatch(element))
            {
                return new DocumentUnion(global::MyNamespace.RecordType2.FromData(element, System.Net.Mime.MediaTypeNames.Application.Json));
            }
            throw new NotSupportedException("No record type matched the JSON data");
        }
```

### Write Method

If the `system_text_json_annotation` option is used, the generated code includes a
`Write` method that writes the JSON representation of the object. The method takes a
`Utf8JsonWriter` object as a parameter and writes the JSON representation of the union value.

The method checks which type is set in the union and serializes the value of the field
that corresponds to the type.

```csharp    
        /// <summary>
        /// Writes the JSON representation of the object.
        /// </summary>
        public override void Write(Utf8JsonWriter writer, DocumentUnion value, JsonSerializerOptions options)
        {
            if (value.RecordType1 != null)
            {
                System.Text.Json.JsonSerializer.Serialize(writer, value.RecordType1, options);
            }
            else if (value.RecordType2 != null)
            {
                System.Text.Json.JsonSerializer.Serialize(writer, value.RecordType2, options);
            }
            else
            {
                throw new NotSupportedException("No record type is set in the union");
            }
        }
```	

### IsJsonMatch Method

If the `system_text_json_annotation` option is used, the generated code includes an
`IsJsonMatch` method that checks if a JSON element matches the schema. The method
takes a `JsonElement` object as a parameter and returns a boolean value that indicates
whether the JSON element matches the schema.

The method checks if the JSON element contains the fields defined in the Avro schema
and if the values of the fields have the correct data types.

```csharp    
            /// <summary>
            /// Checks if the JSON element matches the schema
            /// </summary>
            public static bool IsJsonMatch(System.Text.Json.JsonElement element)
            {
                return (global::MyNamespace.RecordType1.IsJsonMatch(element))
                 || (global::MyNamespace.RecordType2.IsJsonMatch(element));
            }
        }
    }
}
```

## Handling Primitive Type and Mixed Type Unions

The following schema illustrates several other type union scenarios, which we will discuss below.

- `test1` is a union of `string` and `int`	
- `test2` is a union of `int` and `null`, indicating that the field is optional
- `test3` is a union of `string`, `boolean`, and a record type `SubRecord`
- `test4` is a union of an integer and a string array
- `test5` is a union of a string map and an integer map

```json
{
    "type": "record",
    "name": "Example",
    "namespace": "MyNamespace",
    "fields": [
        {
            "name": "test1",
            "type": ["string", "int"]
        },
        {
            "name": "test2",
            "type": ["int", "null"]
        },
        {
            "name": "test3",
            "type": [
                "string", 
                "boolean",
                {
                    "type": "record",
                    "name": "SubRecord",
                    "fields": [
                        {
                            "name": "sub",
                            "type": "string"
                        }
                    ]
                }
            ]
        },
        {
            "name": "test4",
            "type": [{
                "type": "array",
                "items": "int"
            }, {
                "type": "array",
                "items": "string"
            }]
        },
        {
            "name": "test5",
            "type": [{
                "type": "map",
                "values": "string"
            },
            {
                "type": "map",
                "values": "int"
            }]
        }
    ]
}
```	

### Record class

The generated code for the `Example` class, which we don't show in full here,
refers to embedded union classes for all fields except `test2`, which is simply
a nullable field.

```csharp
    /// <summary>
    /// Example
    /// </summary>
    public partial class Example
    {
        /// <summary>
        /// Test1
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("test1")]
        [System.Text.Json.Serialization.JsonConverter(typeof(Test1Union))]
        public Test1Union Test1 { get; set; }
        /// <summary>
        /// Test2
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("test2")]
        public int? Test2 { get; set; }
        /// <summary>
        /// Test3
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("test3")]
        [System.Text.Json.Serialization.JsonConverter(typeof(Test3Union))]
        public Test3Union Test3 { get; set; }
        /// <summary>
        /// Test4
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("test4")]
        [System.Text.Json.Serialization.JsonConverter(typeof(Test4Union))]
        public Test4Union Test4 { get; set; }
        /// <summary>
        /// Test5
        /// </summary>
        [System.Text.Json.Serialization.JsonPropertyName("test5")]
        [System.Text.Json.Serialization.JsonConverter(typeof(Test5Union))]
        public Test5Union Test5 { get; set; }
        
        // ...
    }
}
```

### Primitive Type Union

The `Test1Union` class is generated for the `test1` field. As you may expect
from the previous example, the union class has constructors for each type in
the union, a `ToObject` method, and the `Read`, `Write`, and `IsJsonMatch`
methods generated if 'system_text_json_annotation' is used.

```csharp
#pragma warning disable CS8618
#pragma warning disable CS8603

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MyNamespace
{
    /// <summary>
    /// Example. Type union resolver. 
    /// </summary>
    public partial class Example
    {
        /// <summary>
        /// Union class for test1
        /// </summary>
        [System.Text.Json.Serialization.JsonConverter(typeof(Test1Union))]
        public sealed class Test1Union: System.Text.Json.Serialization.JsonConverter<Test1Union>
        {
            /// <summary>
            /// Default constructor
            /// </summary>
            public Test1Union() { }
            /// <summary>
            /// Constructor for @string values
            /// </summary>
            public Test1Union(string? @string)
            {
                this.@string = @string;
            }
            /// <summary>
            /// Constructor for @int values
            /// </summary>
            public Test1Union(int? @int)
            {
                this.@int = @int;
            }
            /// <summary>
            /// Constructor for Avro decoder
            /// </summary>
            internal static Test1Union FromObject(object obj)
            {
                var self = new Test1Union();
                if (obj is string)
                {
                    self.@string = (string)obj;
                    return self;
                }
                if (obj is int)
                {
                    self.@int = (int)obj;
                    return self;
                }
                throw new NotSupportedException("No record type matched the type");
            }
            /// <summary>
            /// Gets the @string value
            /// </summary>
            public string? @string { get; private set; } = null;
            /// <summary>
            /// Gets the @int value
            /// </summary>
            public int? @int { get; private set; } = null;
    
            /// <summary>
            /// Yields the current value of the union
            /// </summary>
    
            public Object ToObject()
            {
                if (@string != null) {
                    return @string;
                }
                if (@int != null) {
                    return @int;
                }
                throw new NotSupportedException("No record type is set in the union");
            }
    
            /// <summary>
            /// Reads the JSON representation of the object.
            /// </summary>
            public override Test1Union? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                var element = JsonElement.ParseValue(ref reader);
                if (element.ValueKind == JsonValueKind.String)
                {
                    return new Test1Union(element.GetString());
                }
                if (element.ValueKind == JsonValueKind.Number)
                {
                    return new Test1Union(element.GetInt32());
                }
                throw new NotSupportedException("No record type matched the JSON data");
            }
    
            /// <summary>
            /// Writes the JSON representation of the object.
            /// </summary>
            public override void Write(Utf8JsonWriter writer, Test1Union value, JsonSerializerOptions options)
            {
                if (value.@string != null)
                {
                    System.Text.Json.JsonSerializer.Serialize(writer, value.@string, options);
                }
                else if (value.@int != null)
                {
                    System.Text.Json.JsonSerializer.Serialize(writer, value.@int, options);
                }
                else
                {
                    throw new NotSupportedException("No record type is set in the union");
                }
            }
    
            /// <summary>
            /// Checks if the JSON element matches the schema
            /// </summary>
            public static bool IsJsonMatch(System.Text.Json.JsonElement element)
            {
                return (element.ValueKind == System.Text.Json.JsonValueKind.String)
                 || (element.ValueKind == System.Text.Json.JsonValueKind.Number);
            }
        }
    }
}
``` 

