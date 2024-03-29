# JSON Schema Handling in Avrotize

This article explains the handling of JSON Schema in Avrotize in more detail.

- [Conversion from JSON Schema into Avro Schema](#conversion-from-json-schema-into-avro-schema)
  - [Supported JSON Schema elements](#supported-json-schema-elements)
    - [Primitive Types](#primitive-types)
    - [Logical Types](#logical-types)
    - [Arrays](#arrays)
    - [Objects](#objects)
    - [Objects with `additionalProperties` and/or `patternProperties`](#objects-with-additionalproperties-andor-patternproperties)
    - ['oneOf' choices](#oneof-choices)
    - [`allOf` composition](#allof-composition)
    - [`anyOf` choices](#anyof-choices)
    - [`$ref` references and `definitions`/`$defs`](#ref-references-and-definitionsdefs)
    - [Dependency resolution](#dependency-resolution)
  - [Unsupported or partially supported JSON Schema elements](#unsupported-or-partially-supported-json-schema-elements)
    - [Assertions](#assertions)
    - [Subschema Definitions](#subschema-definitions)
- [Conversion of Avro Schema into JSON Schema](#conversion-of-avro-schema-into-json-schema)
    - [Primitive Types](#primitive-types)
    - [Logical Types](#logical-types)
    - [Arrays](#arrays)
    - [Objects](#objects)
    - [Maps](#maps)
    - [Unions](#unions)
    - [Compaction](#compaction)


## Conversion from JSON Schema into Avro Schema

Avrotize can convert most of the commonly used JSON schema structure elements
into corresponding Avro schema components. There are, however, certain
limitations due to differences in the ways JSON Schema and Avro Schema are
structured and what the capabilities of Avro Schema are.

Generally, JSON Schema is used for both defining the structure of a JSON
document and for validating data against the defined structure. Avro Schema is a
language that focuses on defining the structure of data, with validation
capability being to limited to what is required ot correctly encoded and decode
data.

The following sections explain how JSON Schema structure elements are converted and which are not handled by Avrotize, at all.

The focus of the conversion implementation is the extent of the most commonly
version used "in the wild",
[JSON Schema Draft 7](https://json-schema.org/specification-links#draft-7), but
some elements of newer drafts are supported.

Avrotize can also handle aspects of some schema types that lean heavily on the
JSON Schema structure, such as
[Open API 2.0](https://swagger.io/specification/v2/) (Swagger 2.0), if they have
a `definitions` section containing JSON Schema elements.

The discussion below refers to the newest published draft for completeness,
[version 2020-12](https://json-schema.org/specification-links#2020-12).

### Supported JSON Schema elements

#### Primitive Types

The JSON primitive types are mapped directly to Avro primitive types:

| JSON-schema Primitive Type | Avro Primitive Type |
| -------------------------- | ------------------- |
| `string`                   | `string`            |
| `integer`                  | `int`               |
| `number`                   | `float`             |
| `boolean`                  | `boolean`           |

#### Logical Types

JSON Schema has a `format` keyword that allows to specify a "format" of a
primitive type, such as a date, time, UUID, and others. The following JSON
schema `format` values are supported and converted to Avro logical types:

| JSON-schema `format` | Avro Type                                             |
| -------------------- | ----------------------------------------------------- |
| `date-time`, `date`  | `int` (with `logicalType: date`)                      |
| `time`               | `int` (with `logicalType: time-millis`)               |
| `duration`           | `fixed` (with `size: 12` and `logicalType: duration`) |
| `uuid`               | `string` (with `logicalType: uuid`)                   |

#### Arrays

A JSON Schema array is converted to an Avro array.

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "array",
  "items": {
    "type": "string"
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "array",
  "items": {
    "type": "string"
  }
}
```

<td style="vertical-align:top">
</td></tr></table>

**_CAVEAT:_** Avro arrays can only exist in type unions and as a field type, not
as a top-level, shared type. Avrotize will generally inline arrays when they are
found via references and eliminate any standalone shared array types as shown below.

If the document root is declared as an array in JSON Schema, Avrotize will wrap
the array in an Avro record `document_wrapper` with a single field named
`document`, with the type being placed into a `utility` namespace nested within
the namespace used for the document, such that the schema user can tell that the
root is the array.

If arrays are declared in the `definitions`/`$defs` section of the schema,
Avrotize will inline any uses of the shared type in the Avbro Schema, but still
emit a wrapper type for the array in an Avro record with a single field named
`items`, using the `_wrapper` suffix for the record, as shown below.

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td style="vertical-align:top">
    
```json
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties" :{
        "testArray" : {
            "$ref": "#/definitions/StringArray"
        }
    },
    "definitions": {
        "StringArray": {
            "type": "array",
            "items": {
                "type": "string"
            }
        }
    }
}
```
</td>
<td style="vertical-align:top">

```json
[
    {
        "type": "record",
        "name": "StringArray_wrapper",
        "namespace": "com.test.example.utility",
        "fields": [
            {
                "name": "items",
                "type": {
                    "type": "array",
                    "items": "string"
                }
            }
        ]
    },
    {
        "type": "record",
        "name": "document",
        "namespace": "com.test.example",
        "fields": [
            {
                "name": "testArray",
                "type": [
                    "null",
                    {
                        "type": "array",
                        "items": "string"
                    }
                ]
            }
        ]
    }
]
```

</td></tr>
<tr><td style="vertical-align:top">

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "property1": {
        "type": "string"
      },
      "property2": {
        "type": "number"
      }
    },
    "required": ["property1", "property2"]
  }
}
```
</td><td style="vertical-align:top">
    
```json
{
    "type": "record",
    "name": "document_wrapper",
    "namespace": "com.test.example.utility",
    "fields": [
        {
            "name": "items",
            "type": {
                "type": "array",
                "items": {
                    "name": "document",
                    "type": "record",
                    "namespace": "com.test.example",
                    "fields": [
                        {
                            "name": "property1",
                            "type": "string"
                        },
                        {
                            "name": "property2",
                            "type": "float"
                        }
                    ]
                }
            }
        }
    ]
}
```

</table>

### Objects

A JSON Schema object is typically converted to an Avro record, except for a few
cases where an Avro record field is turned into an Avro map (see further below).

JSON objects most often define a set of properties. Each property is converted
to a field on the Avro record and is translated to the corresponding Avro type.

JSON and Avro treat optionality of fields differently. While JSON Schema defaults
to all fields being optional, Avro has a specific concept of optional fields. Avrotize
uses `required` to determine which fields are mandatory, and marks all other fields as
optional, using a union type of the field's type and the type `null`. Therefore,
a plain JSON object with only a set of properties becomes an Avro record with all optional
fields, each defined as a union of the field type and `null`.

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string"
    },
    "age": {
      "type": "integer"
    }
  },
  "required": ["name"]
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "record",
  "name": "document",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "age",
      "type": ["null", "int"]
    }
  ]
}
```

</td></tr></table>

A special case of objects are those that do not define any properties, at all. These are
handled as if they had an `additionalProperties=true` declaration, discussed below.

```json
{
  "type": "object"
}
```

#### Objects with `additionalProperties` and/or `patternProperties`

A JSON Schema object may contain a keyword `additionalProperties`, which
indicates that the object may contain additional properties with keys that are
not defined elsewhere in the schema. This is similar to the Avro concept of a
map, but JSON Schema permits mixing well-known fields with the declaration of
additional properties.

The `patternProperties` keyword is very similar, but specifies a regular
expression pattern that the keys of additional properties must match and allows
for different types of values depending on the key pattern.

When an object has `additionalProperties` and/or `patternProperties` and defines
no `properties`, the object is converted to an Avro `map` with the value type as
the Avro conversion of the `additionalProperties` type value. Just as with
arrays, this `map` type cannot stand alone and will be inlined when found in a
field.

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "additionalProperties": {
    "type": "string"
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "map",
  "values": "string"
}
```

</td></tr></table>

When an object has no properties or if the `additionalProperties` declaration is
just 'true' (which means any properties are allowed), the object is converted to a
`map` with a "generic" data type that Avrotize injects.

The generic data type is a `map` with `values` being a union of all simple Avro
types and a nested `map` type and `array` type, whereby the items or values provide
another level of nesting with the simple types and maps or arrays.

This is the "catch-all" type in Avro and is used for representing any type of
JSON object when the structure is not fully known, for instance with objects
that are meant to be dynamically extended.

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "additionalProperties": true
}
```

or

```json
{
  "type": "object"
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "map",
  "values": [ 
    "null", "boolean", "int",
    "long", "float", "double",
    "bytes", "string",
    {
      "type": "array",
      "items": [
        "null", "boolean", "int",
        "long", "float", "double",
        "bytes", "string",
        {
          "type": "array",
          "items": [
            "null", "boolean", "int",
            "long", "float", "double",
            "bytes", "string"
          ]
        },
        {
          "type": "map",
          "values": [
            "null", "boolean", "int",
            "long", "float", "double",
            "bytes", "string"
          ]
        }
      ]
    },
    {
      "type": "map",
      "values": [
        "null", "boolean", "int",
        "long", "float", "double",
        "bytes", "string"
        {
          "type": "array",
          "items": [
            "null", "boolean", "int",
            "long", "float", "double",
            "bytes", "string"
          ]
        },
        {
          "type": "map",
          "values": [
            "null", "boolean", "int",
            "long", "float", "double",
            "bytes", "string"
          ]
        }
      ]
    }
  ]
}
```

</td></tr></table>

If an object has a combination of properties and `additionalProperties` and/or
`patternProperties` declaration(s), we need to reach to a trick to handle this
in Avro. The trick is to create an Avro record with all the fields from the
properties and additional `map` structure that combines the types of all those
fields with declared `additionalProperties` and/or `patternProperties` type(s) as
described above.

The resulting type union of the map is then consolidated. The resulting union
type then allows an option for a map that holds all fields and all additional
properties similar to JSON, and a regular record with the known fields.

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Properties with Additional Properties",
  "type": "object",
  "properties": {
    "id": {
      "type": "integer"
    },
    "name": {
      "type": "string"
    }
  },
  "additionalProperties": {
    "type": "string"
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
    "type": "record",
    "name": "document_wrapper",
    "namespace": "com.test.example",
    "fields": [
        {
            "name": "root",
            "type": [
                {
                    "type": "record",
                    "name": "document",
                    "namespace": "com.test.example",
                    "fields": [
                        {
                            "name": "id",
                            "type": [
                                "null",
                                "int"
                            ]
                        },
                        {
                            "name": "name",
                            "type": [
                                "null",
                                "string"
                            ]
                        }
                    ],
                    "doc": "Alternate map: 'id': [null, int]," \
                           "'name': [null, string]."\
                           "Extra properties: [string]. "
                },
                {
                    "type": "map",
                    "values": [
                        "null",
                        "int",
                        "string"
                    ]
                }
            ]
        }
    ]
}
```

</td></tr></table>

#### 'oneOf' choices

A JSON Schema `oneOf` choice corresponds to an Avro union type, but is not
mapped directly to an Avro union.

`oneOf` is a schema construction rule that yields a list of schemas from the
combination of its containing schema and each of the `oneOf` options as shown in
the example below.

<table width="100%"><tr><th>JSON Schema</th><th>Merged JSON Schema</th><th>Avro Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "description": "look at this",
  "oneOf": [
    { 
        "properties" : {
        "prop1" {
            "type": "string"
            }
        }
    },
    { 
        "properties" : {
        "prop2" {
            "type": "integer"
            }
        }
    },
    { 
        "properties" : {
        "prop3" {
            "type": "number"
            }
        }
    }
  ]
}
```

</td>
<td style="vertical-align:top">

```json
[
    {
        "type": "object",
        "description": "look at this",
        "properties" : {
            "prop1" {
                "type": "string"
            }
        }
    },
    {
        "type": "object",
        "description": "look at this",
        "properties" : {
            "prop2" {
                "type": "integer"
            }
        }
    },
    {
        "type": "object",
        "description": "look at this",
        "properties" : {
            "prop3" { "type": "number" }
        }
    }
]

```

</td>
<td style="vertical-align:top">
    
```json
[
    {
        "type": "record",
        "name": "document",
        "fields": [
            {
            "name": "prop1",
            "type": ["null", "string"]
            }
        ]
    },
    {
        "type": "record",
        "name": "document",
        "fields": [
            {
            "name": "prop2",
            "type": ["null", "int"]
            }
        ]
    },
    {
        "type": "record",
        "name": "document",
        "fields": [
            {
            "name": "prop3",
            "type": ["null", "float"]
            }
        ]
    }
]
```

</tr></table>

Consequently, Avrotize will first expand the `oneOf` construct and then
convert the result, which yields a list of Avro records, into an Avro union.

#### `allOf` composition

A JSON Schema `allOf` composition is used to apply multiple schemas to a single
object. It's occasionally used to create a "base type" which is then extended
with another schema's properties, simulating inheritance.

Avrotize will merge the schemas defined in the `allOf` into a single JSON schema
that comprises all the properties from the original schemas. The single schema
is then converted into an Avro record, as described above.

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "allOf": [
    {
      "properties": {
        "a": {
          "type": "string"
        }
      }
    },
    {
      "properties": {
        "b": {
          "type": "number"
        }
      }
    }
  ]
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "record",
  "name": "document",
  "fields": [
    {
      "name": "a",
      "type": ["null", "string"]
    },
    {
      "name": "b",
      "type": ["null", "float"]
    }
  ]
}
```

</td></tr></table>

#### `anyOf` choices

A JSON Schema `anyOf` choice is a mix of `oneOf` and `allOf`. It specifies a
list of schemas and the data only needs to satisfy at least one of the schemas.

Avrotize collects the schemas defined in the `anyOf` and converts each of them
into Avro types as with `oneOf`. The resulting Avro types are then consolidated
into an Avro union or even a single type if possible. Any conflict between the
schemas, such as incompatible types for the same field, will lead to the creation
of an Avro union type, with both alternatives preserved.

The examples below demonstrate this. 

- `example1` is a field declared with an `anyOf` of a `string` and a `number`.
  The result is a union of the two types. 
- `example2` is an `anyOf` of a `boolean` and an object with two fields, which
  also becomes a union. 
- `example3` is an `anyOf` of an array of strings and a simple object with a
  `string` property. Those are also incompatible, so the result is a union.
- `example4` is an `anyOf` of two objects, each with a single field. Those are
  not incompatible, so they are merged into a single record type.
- `example5` is an `anyOf` of two objects. The first has one required field, the
  second has two required fields. The field `foo` is defined in both objects,
  but with different types. The resulting type is single record, where the type
  of `foo` is a union of the types from both objects. Field `bar` is identically
  typed in both objects and required in both objects and therefore a `boolean`
  field as the regular record mapping would yield.

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td style="vertical-align:top">

```json
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "example1": {
            "anyOf": [
                {
                    "type": "string"
                },
                {
                    "type": "number"
                }
            ]
        },
        "example2": {
            "anyOf": [
                {
                    "type": "boolean"
                },
                {
                    "type": "object",
                    "properties": {
                        "foo": {
                            "type": "string"
                        },
                        "bar": {
                            "type": "number"
                        }
                    }
                }
            ]
        },
        "example3": {
            "anyOf": [
                {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string"
                        }
                    }
                }
            ]
        },
        "example4": {
            "anyOf": [
                {
                    "type": "object",
                    "properties": {
                        "foo": {
                            "type": "string"
                        }
                    }
                },
                {
                    "type": "object",
                    "properties": {
                        "bar": {
                            "type": "number"
                        }
                    }
                }
            ]
        },
        "example5": {
            "anyOf": [
                {
                    "type": "object",
                    "properties": {
                        "foo": {
                            "type": "string"
                        }
                    },
                    "required": [
                        "foo"
                    ]
                },
                {
                    "type": "object",
                    "properties": {
                        "foo": {
                            "type": "number"
                        },
                        "bar": {
                            "type": "boolean"
                        }
                    },
                    "required": [
                        "foo", "bar"
                    ]
            
                }
            ]
        }
    },
    "required": [
        "example1",
        "example2",
        "example3",
        "example4",
        "example5"
    ]
}
```
</td>
<td style="vertical-align:top">

```json
{
    "type": "record",
    "name": "document",
    "namespace": "com.test.example",
    "fields": [
        {
            "name": "example1",
            "type": [
                "string",
                "float"
            ]
        },
        {
            "name": "example2",
            "type": [
                "boolean",
                {
                    "name": "example2_2",
                    "type": "record",
                    "namespace": "com.test.example.example2_types",
                    "fields": [
                        {
                            "name": "foo",
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        {
                            "name": "bar",
                            "type": [
                                "null",
                                "float"
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "name": "example3",
            "type": [
                {
                    "type": "array",
                    "items": "string"
                },
                {
                    "name": "example3_2",
                    "type": "record",
                    "namespace": "com.test.example.example3_types",
                    "fields": [
                        {
                            "name": "name",
                            "type": [
                                "null",
                                "string"
                            ]
                        }
                    ]
                }
            ]
        },
        {
            "name": "example4",
            "type": {
                "name": "example4",
                "type": "record",
                "namespace": "com.test.example.document_types",
                "fields": [
                    {
                        "name": "foo",
                        "type": [
                            "null",
                            "string"
                        ]
                    },
                    {
                        "name": "bar",
                        "type": [
                            "null",
                            "float"
                        ]
                    }
                ]
            }
        },
        {
            "name": "example5",
            "type": {
                "name": "example5",
                "type": "record",
                "namespace": "com.test.example.document_types",
                "fields": [
                    {
                        "name": "foo",
                        "type": [
                            "string",
                            "float"
                        ]
                    },
                    {
                        "name": "bar",
                        "type": "boolean"
                    }
                ]
            }
        }
    ]
}
```
</td></tr></table>

#### `$ref` references and `definitions`/`$defs`

A JSON Schema can use the `$ref` keyword to reference another schema definition
elsewhere in the document or in an external schema. The referenced schema may
define any type of schema structure, ranging from primitive types to complex objects.

The `definitions` keyword, available in JSON Schema Draft 7, can be used to
contain a set of reusable schema definitions. The `definitions` keyword in
combination with `$ref` is a way to declare a schema outside of the main
definition schema and then reference it from there. The `definitions` keyword
has been deprecated in newer JSON Schema drafts in favor of `$defs`.

Avrotize will resolve local `$ref` JSON Path references (starting with a `#`) to
anywhere in the JSON Schema. Avrotize will attempt to resolve external `$ref`
links in the file system as well as via HTTP/HTTPS. When the main document has
been referenced via a URL, Avrotize will resolve external references as if they
are relative to the URL.

All referenced schemas are resolved and expanded before the conversion to Avro
Schema and thus merged into the Avro schema, which has no external references.

> **_NOTE:_** Avrotize currently does not support referencing external schemas
> when the endpoint is secured (i.e. when the endpoint is behind a firewall or
> requires authentication). It is also not possible to provide a list of locally
> available schemas to the tool as a workaround. This limitation will be
> addressed in a future release.

Avrotize generally assumes that any JSON schema declaration pointed to by a
`$ref` reference is a shared type and will therefore create a top-level Avro
record for it, if the schema can be converted to a record or enum. If the
referenced schema resolves to an `array` or `map` type, the schema will be
inlined, but a wrapper type will be created nevertheless, as mentioned earlier.

The example below shows how `#/definitions/stringType` inlined as a `string`
type and does not appear in the output Avro Schema.
`#/definitions/additionalPropertiesObject` is inlined as a `map` type and 
also does not appear in the output Avro Schema since it first resolved into 
a nested object and then into a map. `#/definitions/arrayObject` is inlined
into the referencing field, but also appears in the output Avro Schema as
a wrapped array type. `#/definitions/propertiesObject` is mapped to a standalone
record type in the output Avro schema and referenced by name.

<table width="100%"><tr><th>JSON Schema</th><th>Resolved Avro Schema</th></tr><tr><td style="vertical-align:top">

```json
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "objectWithProperties": {
            "$ref": "#/definitions/propertiesObject"
        },
        "arrayObject": {
            "$ref": "#/definitions/arrayObject"
        },
        "objectWithAdditionalProperties": {
            "$ref": "#/definitions/additionalPropertiesObject"
        },
        "stringProperty": {
            "$ref": "#/definitions/stringType"
        }
    },
    "required": [
        "objectWithProperties",
        "arrayObject",
        "objectWithAdditionalProperties",
        "stringProperty"
    ],
    "definitions": {
        "propertiesObject": {
            "type": "object",
            "properties": {
                "property1": {
                    "type": "string"
                },
                "property2": {
                    "type": "number"
                }
            },
            "required": [
                "property1",
                "property2"
            ]
        },
        "arrayObject": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "additionalPropertiesObject": {
            "type": "object",
            "additionalProperties": {
                "type": "string"
            }
        },
        "stringType": {
            "type": "string"
        }
    }
}
```
</td><td style="vertical-align:top">

```json
[
    {
        "type": "record",
        "name": "propertiesObject",
        "namespace": "com.test.example",
        "fields": [
            {
                "name": "property1",
                "type": "string"
            },
            {
                "name": "property2",
                "type": "float"
            }
        ]
    },
    {
        "type": "record",
        "name": "document",
        "namespace": "com.test.example",
        "fields": [
            {
                "name": "objectWithProperties",
                "type": "com.test.example.propertiesObject"
            },
            {
                "name": "arrayObject",
                "type": {
                    "type": "array",
                    "items": "string"
                }
            },
            {
                "name": "objectWithAdditionalProperties",
                "type": {
                    "type": "map",
                    "values": "string"
                }
            },
            {
                "name": "stringProperty",
                "type": "string"
            }
        ]
    },
    {
        "type": "record",
        "name": "arrayObject_wrapper",
        "namespace": "com.test.example.utility",
        "fields": [
            {
                "name": "items",
                "type": {
                    "type": "array",
                    "items": "string"
                }
            }
        ]
    }
]
```
</td></tr></table>

#### Dependency resolution

`$ref` references in JSON documents often lead to situations where shared types
are referenced by name. You can see above that the `propertiesObject` record
type is referenced by name from within the `document` schema, and that the 
record type appears before the `document` type in the Avro Schema. That is the 
result of Avrotize sorting all references by the order of their appearance in
the document in a second pass, since Avro schema allows no forward references.

When the JSON Schema contains circular references, this may (very intentionally)
result in top-level record declarations to be inlined to where they are first
used.

The following example illustrates this. The declared object is defining four
properties, each referencing a declared type in `definitions` via `$ref`.
`#/definitions/Definition1` is an immediate circular reference in property `prop1`,
`#/definitions/Definition2` has a circular reference in a property of a nested object,
`#/definitions/Definition3` has a circular reference as an element in an `anyOf`
array, and `#/definitions/Definition4` has a circular reference as a property in
one of its properties. `#/definitions/Definition4` has a circular reference by
ways of a `$ref` to `#/definitions/Definition4a`, which in turn has a `$ref`
back to `#/definitions/Definition4`.

Avrotize moves `Definition1`, `Definition2`, `Definition3`, and `Definition4` to
the top of the schema, even though they are declared after the document type in
the JSON Schema. 

`Definition4a` is declared as a top-level type in the JSON Schema, but the
dependency resolution found it being used in `Definition4` with `Definition4a`
itself referring back to `Definition4`. This results in `Definition4a` being
"pulled in" into the first reference to it inside of `Definition4`, which 
establishes the proper type order for Avro.

Mind that all type declarations of `record`, `enum`, and `fixed` type
declarations are global in the scope of the schema, independent of the nesting
level at which they are declared.

<table width="100%"><tr><th>JSON Schema</th><th>Resolved Avro Schema</th></tr><tr><td style="vertical-align:top">

```json
{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "def1": {
            "$ref": "#/definitions/Definition1",
            "title": "Definition1"
        },
        "def2": {
            "$ref": "#/definitions/Definition2"
        },
        "def3": {
            "title": "Definition3",
            "$ref": "#/definitions/Definition3"
        },
        "def4": {
            "title": "Definition4",
            "$ref": "#/definitions/Definition4"
        }
    },
    "definitions": {
        "Definition1": {
            "type": "object",
            "properties": {
                "prop1": {
                    "$ref": "#/definitions/Definition1"
                }
            }
        },
        "Definition2": {
            "type": "object",
            "properties": {
                "nested": {
                    "type": "object",
                    "properties": {
                        "prop2": {
                            "$ref": "#/definitions/Definition2"
                        }
                    }
                }
            }
        },
        "Definition3": {
            "type": "object",
            "properties": {
                "anyOfProp": {
                    "anyOf": [
                        {
                            "$ref": "#/definitions/Definition3"
                        },
                        {
                            "type": "string"
                        }
                    ]
                }
            }
        },
        "Definition4": {
            "type": "object",
            "properties": {
                "prop4": {
                    "$ref": "#/definitions/Definition4a"
                }
            }
        },
        "Definition4a": {
            "type": "object",
            "properties": {
                "nested": {
                    "type": "object",
                    "properties": {
                        "prop5": {
                            "$ref": "#/definitions/Definition4"
                        }
                    }
                }
            }
        }
    }
}
```
</td><td style="vertical-align:top">

```json
[
    {
        "name": "Definition1",
        "type": "record",
        "namespace": "com.test.example",
        "fields": [
            {
                "name": "prop1",
                "type": [
                    "null",
                    "com.test.example.Definition1"
                ]
            }
        ]
    },
    {
        "name": "Definition2",
        "type": "record",
        "namespace": "com.test.example",
        "fields": [
            {
                "name": "nested",
                "type": [
                    "null",
                    {
                        "name": "nested",
                        "type": "record",
                        "namespace": "com.test.example.Definition2_types",
                        "fields": [
                            {
                                "name": "prop2",
                                "type": [
                                    "null",
                                    "com.test.example.Definition2"
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    },
    {
        "name": "Definition3",
        "type": "record",
        "namespace": "com.test.example",
        "fields": [
            {
                "name": "anyOfProp",
                "type": [
                    "null",
                    "com.test.example.Definition3",
                    "string"
                ]
            }
        ]
    },
    {
        "name": "Definition4",
        "type": "record",
        "namespace": "com.test.example",
        "fields": [
            {
                "name": "prop4",
                "type": [
                    "null",
                    {
                        "name": "Definition4a",
                        "type": "record",
                        "namespace": "com.test.example",
                        "fields": [
                            {
                                "name": "nested",
                                "type": [
                                    "null",
                                    {
                                        "name": "nested",
                                        "type": "record",
                                        "namespace": "com.test.example.Definition4a_types",
                                        "fields": [
                                            {
                                                "name": "prop5",
                                                "type": [
                                                    "null",
                                                    "com.test.example.Definition4"
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    },
    {
        "type": "record",
        "name": "document",
        "namespace": "com.test.example",
        "fields": [
            {
                "name": "def1",
                "type": [
                    "null",
                    "com.test.example.Definition1"
                ]
            },
            {
                "name": "def2",
                "type": [
                    "null",
                    "com.test.example.Definition2"
                ]
            },
            {
                "name": "def3",
                "type": [
                    "null",
                    "com.test.example.Definition3"
                ]
            },
            {
                "name": "def4",
                "type": [
                    "null",
                    "com.test.example.Definition4"
                ]
            }
        ]
    }
]
```
</td></tr></table>


### Unsupported or partially supported JSON Schema elements

The following JSON schema structure elements are not supported by Avrotize.

#### Assertions

> See [JSON Schema section 7.6](https://json-schema.org/draft/2020-12/draft-bhutton-json-schema-01#section-7.6)

`const` and `enum` are only supported for string types. Numeric and boolean
literals are converted to strings.

`enum` elements whose values are no valid as Avro enum symbols are modified to
be valid symbols, with disallowed characters being replaced by underscore and
leading digits being prefixed with an underscore as well.

`multipleOf`, `maximum`, `minimum`, `exclusiveMaximum`, `exclusiveMinimum`,
`maxLength`, `minLength`, `pattern`, `maxItems`, `minItems`, `uniqueItems`,
`maxContains`, `minContains`, `maxProperties`, `minProperties`, and
`dependentRequired` are not supported since those are validation constructs.

`format` is only supported when there is a corresponding Avro type or logical
type. Otherwise, the format is ignored. Supported:

- `date-time`: `string` with `timestamp-micros` logical type
- `date`: `string` with `date` logical type
- `time`: `string` with `time-micros` logical type
- `duration`: `string` with `duration` logical type
- `uuid`: `string` with `uuid` logical type

The `required` keyword is supported for object types. When a field is not in an
object's `required` list, it is marked as optional by ways of making it part of
a union with `null`.

#### Subschema Definitions

> See [JSON Schema Section 10](https://json-schema.org/draft/2020-12/draft-bhutton-json-schema-01#name-a-vocabulary-for-applying-s)

`if`, `then`, `else`, `dependentSchemas` are ignored completely. As a result,
subschemas defined using these keywords are not honored in the Avro schema.

This is a limitation of the implementation in Avrotize and may be addressed in
a future release. Partial support for these constructs is possible through
Avro type unions.

`prefixItems` and `contains`are not supported.

`not` is not supported since it's a negation operation during validation.

## Conversion of Avro schema into JSON Schema

The conversion to JSON schema captures almost all Avro schema constructs, but
makes a few adjustments to yield a more "normal" JSON Schema. Standalone enum
types are generally inlined, for instance.

### Primitive types

Primitive Avro types are converted directly to their JSON Schema counterparts.

| Avro Type | Logical Type | JSON Type | Format |
| --------- | ------------ | --------- | ------ |
| null      |              | null      |        |
| boolean   |              | boolean   |        |
| int       |              | integer   | int32  |
| long      |              | integer   | int64  |
| float     |              | number    | float  |
| double    |              | number    | double |
| bytes     |              | string    |       |
| string    |              | string    |        |
| fixed     |              | string    |        |
| int       | date         | string    | date-time |
| long      | timestamp-millis, timestamp-micros | string | date-time |
| int       | time-millis, time-micros | string | time |
| bytes     | decimal      | number    |        |
| string    | uuid         | string    | uuid   |

The 'bytes' type will be emitted as a `string` type with `contentEncoding` set
to `base64`.
 
### Records

Avro records are converted into JSON Schema objects. The fields are transformed
into properties with the corresponding transformed types. The Avro schema's
`doc` attribute is used as the description of the record.

### Enums

Avro enumeration types are translated to JSON Schema `string` types with an
`enum` keyword listing all symbols. The enumerated types are generally inlined,
but may be shared by `$ref` if they are used in multiple locations in the Avro schema.

### Maps 

Avro map types are converted to JSON Schema `object` types with `additionalProperties`

### Arrays

Avro arrays are converted to JSON Schema `array` types with `items`

### Unions

The tool will emit all Avro type unions as JSON Schema `oneOf` clauses. 

### Compaction

In post-processing, the tool will attempt to consolidate "duplicate" schema
sections that may stem from Avro Schema's structural limitations, for instance
not allowing for `array` or `map` fields to be shareable as types.
