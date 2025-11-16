# JSON Structure Handling in Avrotize

This article explains the handling of JSON Structure in Avrotize in more detail.

- [Conversion from JSON Structure into Avro Schema](#conversion-from-json-structure-into-avro-schema)
  - [Supported JSON Structure elements](#supported-json-structure-elements)
    - [Primitive Types](#primitive-types)
    - [Logical Types and Temporal Types](#logical-types-and-temporal-types)
    - [Extended Numeric Types](#extended-numeric-types)
    - [Arrays](#arrays)
    - [Objects](#objects)
    - [Maps](#maps)
    - [Sets](#sets)
    - [Tuples](#tuples)
    - [Choice Types](#choice-types)
    - [Any Type](#any-type)
    - [`$extends` Inheritance](#extends-inheritance)
    - [Abstract Types](#abstract-types)
    - [`$ref` references and `definitions`](#ref-references-and-definitions)
    - [Validation Constraints](#validation-constraints)
  - [Unsupported or partially supported JSON Structure elements](#unsupported-or-partially-supported-json-structure-elements)
- [Conversion of Avro Schema into JSON Structure](#conversion-of-avro-schema-into-json-structure)
    - [Primitive Types](#primitive-types-1)
    - [Logical Types](#logical-types-1)
    - [Records](#records)
    - [Enums](#enums)
    - [Arrays](#arrays-1)
    - [Maps](#maps-1)
    - [Unions](#unions)

## Conversion from JSON Structure into Avro Schema

Avrotize can convert JSON Structure documents into Avro Schema format using
**Avrotize Schema extensions**. JSON Structure is a schema format that provides
rich type definitions and inheritance mechanisms designed for modern data
modeling.

The conversion produces Avrotize Schema format, which is a superset of Apache
Avro Schema with extensions for string-based logical types (RFC 3339 format) and
additional metadata.

### Supported JSON Structure elements

#### Primitive Types

JSON Structure primitive types are mapped to Avro primitive types with Avrotize
Schema extensions where appropriate:

| JSON Structure Type | Avro/Avrotize Type | Notes |
| ------------------- | ------------------ | ----- |
| `null` | `null` | |
| `boolean` | `boolean` | |
| `int8` | `int` | 8-bit signed integer |
| `int16` | `int` | 16-bit signed integer |
| `int32` | `int` | 32-bit signed integer |
| `int64` | `long` | 64-bit signed integer |
| `uint8` | `int` | 8-bit unsigned integer |
| `uint16` | `int` | 16-bit unsigned integer |
| `uint32` | `long` | 32-bit unsigned integer |
| `uint64` | `long` | 64-bit unsigned integer |
| `int128` | `string` | Too large for Avro numeric types |
| `uint128` | `string` | Too large for Avro numeric types |
| `float8` | `float` | 8-bit floating point |
| `float16` | `float` | 16-bit floating point |
| `float32`, `float` | `float` | 32-bit floating point |
| `float64`, `double` | `double` | 64-bit floating point |
| `number` | `double` | Generic number type |
| `binary32` | `float` | IEEE 754 binary32 |
| `binary64` | `double` | IEEE 754 binary64 |
| `string` | `string` | UTF-8 string |
| `binary`, `bytes` | `bytes` | Binary data |
| `uri` | `string` | URI string |
| `jsonpointer` | `string` | JSON Pointer string |

#### Logical Types and Temporal Types

JSON Structure temporal types are converted to **Avrotize Schema string-based
logical types** following RFC 3339 format specifications:

| JSON Structure Type | Avrotize Schema Type | Format |
| ------------------- | -------------------- | ------ |
| `date` | `{"type": "string", "logicalType": "date"}` | RFC 3339 full-date |
| `time` | `{"type": "string", "logicalType": "time-millis"}` | RFC 3339 partial-time |
| `datetime` | `{"type": "string", "logicalType": "timestamp-millis"}` | RFC 3339 date-time |
| `timestamp` | `{"type": "string", "logicalType": "timestamp-millis"}` | RFC 3339 date-time |
| `duration` | `{"type": "string", "logicalType": "duration"}` | RFC 3339 duration |
| `uuid` | `{"type": "string", "logicalType": "uuid"}` | UUID format |
| `decimal` | `{"type": "string", "logicalType": "decimal"}` | Decimal string |

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "TemporalExample",
  "properties": {
    "eventDate": {
      "type": "date"
    },
    "eventTime": {
      "type": "time"
    },
    "eventTimestamp": {
      "type": "datetime"
    },
    "duration": {
      "type": "duration"
    },
    "id": {
      "type": "uuid"
    }
  },
  "required": ["eventDate", "id"]
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "record",
  "name": "TemporalExample",
  "fields": [
    {
      "name": "eventDate",
      "type": {
        "type": "string",
        "logicalType": "date"
      }
    },
    {
      "name": "eventTime",
      "type": [
        "null",
        {
          "type": "string",
          "logicalType": "time-millis"
        }
      ],
      "default": null
    },
    {
      "name": "eventTimestamp",
      "type": [
        "null",
        {
          "type": "string",
          "logicalType": "timestamp-millis"
        }
      ],
      "default": null
    },
    {
      "name": "duration",
      "type": [
        "null",
        {
          "type": "string",
          "logicalType": "duration"
        }
      ],
      "default": null
    },
    {
      "name": "id",
      "type": {
        "type": "string",
        "logicalType": "uuid"
      }
    }
  ]
}
```

</td></tr></table>

**Benefits of Avrotize Schema string-based logical types:**
- Human-readable RFC 3339 format (e.g., `"2025-11-16T14:30:00Z"`)
- Compatible with JSON encoding without conversion
- Avoids numeric timestamp ambiguity
- Standard format across different platforms

#### Extended Numeric Types

JSON Structure supports extended numeric types that map to Avro types:

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "NumericTypes",
  "properties": {
    "smallInt": {
      "type": "int8"
    },
    "mediumInt": {
      "type": "int16"
    },
    "largeInt": {
      "type": "int64"
    },
    "unsignedInt": {
      "type": "uint32"
    },
    "price": {
      "type": "decimal",
      "precision": 10,
      "scale": 2
    }
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "record",
  "name": "NumericTypes",
  "fields": [
    {
      "name": "smallInt",
      "type": ["null", "int"],
      "default": null
    },
    {
      "name": "mediumInt",
      "type": ["null", "int"],
      "default": null
    },
    {
      "name": "largeInt",
      "type": ["null", "long"],
      "default": null
    },
    {
      "name": "unsignedInt",
      "type": ["null", "long"],
      "default": null
    },
    {
      "name": "price",
      "type": [
        "null",
        {
          "type": "string",
          "logicalType": "decimal"
        }
      ],
      "default": null
    }
  ]
}
```

</td></tr></table>

#### Arrays

JSON Structure arrays are converted to Avro arrays:

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "ArrayExample",
  "properties": {
    "tags": {
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
{
  "type": "record",
  "name": "ArrayExample",
  "fields": [
    {
      "name": "tags",
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "default": null
    }
  ]
}
```

</td></tr></table>

#### Objects

JSON Structure objects are converted to Avro records. Fields in `required` are
mandatory; all others become optional (union with `null`):

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "Person",
  "properties": {
    "name": {
      "type": "string"
    },
    "age": {
      "type": "int32"
    },
    "email": {
      "type": "string"
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
  "name": "Person",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "age",
      "type": ["null", "int"],
      "default": null
    },
    {
      "name": "email",
      "type": ["null", "string"],
      "default": null
    }
  ]
}
```

</td></tr></table>

#### Maps

JSON Structure `map` types are converted to Avro maps:

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "Configuration",
  "properties": {
    "settings": {
      "type": "map",
      "values": {
        "type": "string"
      }
    }
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "record",
  "name": "Configuration",
  "fields": [
    {
      "name": "settings",
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ],
      "default": null
    }
  ]
}
```

</td></tr></table>

#### Sets

JSON Structure `set` types are represented as arrays in Avro (sets are unordered
collections of unique elements):

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "UniqueItems",
  "properties": {
    "uniqueIds": {
      "type": "set",
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
{
  "type": "record",
  "name": "UniqueItems",
  "fields": [
    {
      "name": "uniqueIds",
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "default": null
    }
  ]
}
```

</td></tr></table>

#### Tuples

JSON Structure `tuple` types are converted to Avro records with ordered,
numbered fields:

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "Coordinate",
  "properties": {
    "location": {
      "type": "tuple",
      "tuple": [
        {"type": "float64"},
        {"type": "float64"}
      ]
    }
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "record",
  "name": "Coordinate",
  "fields": [
    {
      "name": "location",
      "type": [
        "null",
        {
          "type": "record",
          "name": "Tuple_2_items",
          "fields": [
            {
              "name": "item0",
              "type": "double"
            },
            {
              "name": "item1",
              "type": "double"
            }
          ]
        }
      ],
      "default": null
    }
  ]
}
```

</td></tr></table>

#### Choice Types

JSON Structure `choice` types represent discriminated unions. When a choice has
a `selector` field, it creates an inline union. Otherwise, it creates a union of
the choice types:

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "choice",
  "name": "ChoiceTypes",
  "choices": {
    "Person": {
      "$ref": "#/definitions/Person"
    },
    "Company": {
      "$ref": "#/definitions/Company"
    }
  },
  "definitions": {
    "Person": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "age": {"type": "int32"}
      }
    },
    "Company": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "employees": {"type": "int32"}
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
    "name": "Person",
    "fields": [
      {
        "name": "name",
        "type": ["null", "string"],
        "default": null
      },
      {
        "name": "age",
        "type": ["null", "int"],
        "default": null
      }
    ]
  },
  {
    "type": "record",
    "name": "Company",
    "fields": [
      {
        "name": "name",
        "type": ["null", "string"],
        "default": null
      },
      {
        "name": "employees",
        "type": ["null", "int"],
        "default": null
      }
    ]
  },
  {
    "type": "record",
    "name": "ChoiceTypes",
    "fields": [
      {
        "name": "value",
        "type": ["Person", "Company"]
      }
    ]
  }
]
```

</td></tr></table>

#### Any Type

JSON Structure `any` type represents values that can be of any type. It's
converted to a union of all basic Avro types:

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "Flexible",
  "properties": {
    "value": {
      "type": "any"
    }
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "record",
  "name": "Flexible",
  "fields": [
    {
      "name": "value",
      "type": [
        "null",
        "boolean",
        "int",
        "long",
        "float",
        "double",
        "string",
        "bytes"
      ],
      "default": null
    }
  ]
}
```

</td></tr></table>

#### `$extends` Inheritance

JSON Structure supports type inheritance through `$extends`. The converter
merges properties from base types into derived types:

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "Person",
  "$extends": {
    "$ref": "#/definitions/BaseEntity"
  },
  "properties": {
    "age": {
      "type": "int32"
    }
  },
  "definitions": {
    "BaseEntity": {
      "type": "object",
      "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"}
      },
      "required": ["id"]
    }
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "record",
  "name": "Person",
  "doc": "Extends BaseEntity",
  "fields": [
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "name",
      "type": ["null", "string"],
      "default": null
    },
    {
      "name": "age",
      "type": ["null", "int"],
      "default": null
    }
  ]
}
```

</td></tr></table>

**Key features:**
- Base properties are merged into derived type
- `required` fields from base become required in derived type
- Property order: base properties first, then derived properties
- Documentation indicates inheritance with "Extends BaseType"

#### Abstract Types

JSON Structure supports abstract types (types that cannot be instantiated
directly). Abstract types are detected and filtered from the output but their
properties are inherited by concrete types:

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "choice",
  "name": "Entities",
  "choices": {
    "Person": {
      "$ref": "#/definitions/Person"
    },
    "Company": {
      "$ref": "#/definitions/Company"
    }
  },
  "definitions": {
    "BaseEntity": {
      "type": "object",
      "abstract": true,
      "properties": {
        "entityType": {"type": "string"},
        "name": {"type": "string"}
      },
      "required": ["entityType", "name"]
    },
    "Person": {
      "type": "object",
      "$extends": {
        "$ref": "#/definitions/BaseEntity"
      },
      "properties": {
        "age": {"type": "int32"}
      }
    },
    "Company": {
      "type": "object",
      "$extends": {
        "$ref": "#/definitions/BaseEntity"
      },
      "properties": {
        "employees": {"type": "int32"}
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
    "name": "Person",
    "doc": "Extends abstract BaseEntity",
    "fields": [
      {
        "name": "entityType",
        "type": "string"
      },
      {
        "name": "name",
        "type": "string"
      },
      {
        "name": "age",
        "type": ["null", "int"],
        "default": null
      }
    ]
  },
  {
    "type": "record",
    "name": "Company",
    "doc": "Extends abstract BaseEntity",
    "fields": [
      {
        "name": "entityType",
        "type": "string"
      },
      {
        "name": "name",
        "type": "string"
      },
      {
        "name": "employees",
        "type": ["null", "int"],
        "default": null
      }
    ]
  }
]
```

</td></tr></table>

**Note:** `BaseEntity` is marked as abstract and does not appear in the output
schema. Only concrete types `Person` and `Company` are included, with inherited
properties from `BaseEntity`.

#### `$ref` references and `definitions`

JSON Structure uses `$ref` to reference type definitions in the `definitions`
section. References are resolved and types are emitted in dependency order:

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "Order",
  "properties": {
    "customer": {
      "$ref": "#/definitions/Customer"
    },
    "items": {
      "type": "array",
      "items": {
        "$ref": "#/definitions/OrderItem"
      }
    }
  },
  "definitions": {
    "Customer": {
      "type": "object",
      "properties": {
        "name": {"type": "string"},
        "email": {"type": "string"}
      },
      "required": ["name"]
    },
    "OrderItem": {
      "type": "object",
      "properties": {
        "product": {"type": "string"},
        "quantity": {"type": "int32"}
      },
      "required": ["product", "quantity"]
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
    "name": "Customer",
    "fields": [
      {
        "name": "name",
        "type": "string"
      },
      {
        "name": "email",
        "type": ["null", "string"],
        "default": null
      }
    ]
  },
  {
    "type": "record",
    "name": "OrderItem",
    "fields": [
      {
        "name": "product",
        "type": "string"
      },
      {
        "name": "quantity",
        "type": "int"
      }
    ]
  },
  {
    "type": "record",
    "name": "Order",
    "fields": [
      {
        "name": "customer",
        "type": ["null", "Customer"],
        "default": null
      },
      {
        "name": "items",
        "type": [
          "null",
          {
            "type": "array",
            "items": "OrderItem"
          }
        ],
        "default": null
      }
    ]
  }
]
```

</td></tr></table>

#### Validation Constraints

JSON Structure validation constraints (maxLength, minLength, precision, scale,
pattern, minimum, maximum, contentEncoding, contentMediaType, contentCompression)
are preserved as annotations in the Avro schema's `doc` field:

<table width="100%"><tr><th>JSON Structure</th><th>Avrotize Schema</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "Product",
  "properties": {
    "name": {
      "type": "string",
      "maxLength": 100,
      "minLength": 1
    },
    "price": {
      "type": "decimal",
      "precision": 10,
      "scale": 2,
      "minimum": 0
    },
    "sku": {
      "type": "string",
      "pattern": "^[A-Z]{2}\\d{4}$"
    }
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "record",
  "name": "Product",
  "fields": [
    {
      "name": "name",
      "type": ["null", "string"],
      "doc": "[maxLength: 100, minLength: 1]",
      "default": null
    },
    {
      "name": "price",
      "type": [
        "null",
        {
          "type": "string",
          "logicalType": "decimal"
        }
      ],
      "doc": "[precision: 10, scale: 2, minimum: 0]",
      "default": null
    },
    {
      "name": "sku",
      "type": ["null", "string"],
      "doc": "[pattern: ^[A-Z]{2}\\d{4}$]",
      "default": null
    }
  ]
}
```

</td></tr></table>

### Unsupported or partially supported JSON Structure elements

The following JSON Structure elements have limitations:

**Not Supported:**
- **Inline unions with discriminators**: Full discriminated union pattern is not
  yet implemented
- **Complex conditional schemas**: `if`/`then`/`else` constructs
- **Pattern properties**: Regular expression-based property matching

**Partially Supported:**
- **Choice with selector**: Creates record wrapper with union field (not full
  discriminated union)
- **Validation constraints**: Captured in documentation only, not enforced

## Conversion of Avro Schema into JSON Structure

The reverse conversion from Avro Schema to JSON Structure is currently not
implemented. This is planned for a future release.

When implemented, the conversion will map:
- Avro records → JSON Structure objects
- Avro arrays → JSON Structure arrays
- Avro maps → JSON Structure maps
- Avro unions → JSON Structure choice types or optional fields
- Avro enums → JSON Structure enums
- Avrotize logical types → JSON Structure temporal types

The conversion will need to handle:
- Flattening nested inline types into definitions
- Converting union types to appropriate JSON Structure patterns
- Mapping Avrotize string-based logical types back to JSON Structure types
- Extracting validation constraints from doc annotations

### Primitive Types

Avro primitive types will be converted to JSON Structure types:

| Avro Type | JSON Structure Type |
| --------- | ------------------- |
| `null` | `null` |
| `boolean` | `boolean` |
| `int` | `int32` |
| `long` | `int64` |
| `float` | `float32` |
| `double` | `float64` |
| `bytes` | `binary` |
| `string` | `string` |

### Logical Types

Avrotize Schema logical types will be converted to JSON Structure types:

| Avrotize Logical Type | JSON Structure Type |
| --------------------- | ------------------- |
| `date` (on string) | `date` |
| `time-millis` (on string) | `time` |
| `timestamp-millis` (on string) | `datetime` |
| `duration` (on string) | `duration` |
| `uuid` (on string) | `uuid` |
| `decimal` (on string) | `decimal` |

### Records

Avro records will be converted to JSON Structure objects with properties mapped
from fields.

### Enums

Avro enums will be converted to JSON Structure objects with `enum` property
containing the symbol list.

### Arrays

Avro arrays will be converted to JSON Structure arrays with the `items` type.

### Maps

Avro maps will be converted to JSON Structure map types with the `values` type.

### Unions

Avro unions will be analyzed:
- Simple `[null, T]` unions → Optional field
- Multiple record types → Choice type
- Mixed types → Union representation (implementation-specific)
