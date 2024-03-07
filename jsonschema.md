# JSON Schema Handling in Avrotize

This article explains the handling of JSON Schema in Avrotize in more detail.

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

The focus of the conversion is the extent of the most commonly version used "in
the wild", [JSON Schema Draft 7](https://json-schema.org/specification-links#draft-7), but
some elements of newer drafts are supported.

The discussion below refers to the newest published draft, [version 2020-12](https://json-schema.org/specification-links#2020-12).

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

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td>

```json
{
  "type": "array",
  "items": {
    "type": "string"
  }
}
```

</td>
<td>

```json
{
  "type": "array",
  "items": {
    "type": "string"
  }
}
```

<td>
</td></tr></table>

**_CAVEAT:_** Avro arrays can only exist in type unions and as a field type, not
as a top-level, shared type. Avrotize will generally inline arrays when they are
found via references and eliminate any standalone shared array types as shown below.

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td>
    
```json
{
    "$schema": "...",
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
<td>

```json
{
  "type": "record",
  "name": "document",
  "namespace": "array",
  "fields": [
    {
      "name": "testArray",
      "type": {
        "type": "array",
        "items": "string"
      }
    }
  ]
}
```

</td></tr></table>

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

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td>

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
<td>

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

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td>

```json
{
  "type": "object",
  "additionalProperties": {
    "type": "string"
  }
}
```

</td>
<td>

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

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td>

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
<td>

```json
{
  "type": "map",
  "values": [
    "null",
    "boolean",
    "int",
    "long",
    "float",
    "double",
    "bytes",
    "string",
    {
      "type": "array",
      "items": [
        "null",
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "bytes",
        "string",
        {
          "type": "array",
          "items": [
            "null",
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "bytes",
            "string"
          ]
        },
        {
          "type": "map",
          "values": [
            "null",
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "bytes",
            "string"
          ]
        }
      ]
    },
    {
      "type": "map",
      "values": [
        "null",
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "bytes",
        "string",
        {
          "type": "array",
          "items": [
            "null",
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "bytes",
            "string"
          ]
        },
        {
          "type": "map",
          "values": [
            "null",
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "bytes",
            "string"
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

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td>

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
<td>

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

<table width="100%"><tr><th>JSON Schema</th><th>Merged JSON Schema</th><th>Avro Schema</th></tr><tr><td>

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
<td>

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
<td>
    
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

<table width="100%"><tr><th>JSON Schema</th><th>Avro Schema</th></tr><tr><td>

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
<td>

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
