# CDDL Handling in Avrotize

This article explains the handling of CDDL (Concise Data Definition Language) in Avrotize, specifically the conversion from CDDL to JSON Structure format.

- [Conversion from CDDL to JSON Structure](#conversion-from-cddl-to-json-structure)
  - [What is CDDL?](#what-is-cddl)
  - [What is JSON Structure?](#what-is-json-structure)
  - [Supported CDDL Elements](#supported-cddl-elements)
    - [Primitive Types](#primitive-types)
    - [Objects (Maps)](#objects-maps)
    - [Arrays](#arrays)
    - [Tuples](#tuples)
    - [Choice/Union Types](#choiceunion-types)
    - [Type References](#type-references)
    - [Occurrence Indicators](#occurrence-indicators)
    - [Control Operators](#control-operators)
    - [Generic Types](#generic-types)
    - [Unwrap Operator](#unwrap-operator)
    - [CBOR Tags](#cbor-tags)
    - [Comments and Descriptions](#comments-and-descriptions)
  - [Unsupported or Partially Supported CDDL Elements](#unsupported-or-partially-supported-cddl-elements)
  - [Name Normalization](#name-normalization)

## Conversion from CDDL to JSON Structure

Avrotize can convert CDDL schema documents into JSON Structure format. The conversion captures the structural aspects of CDDL while mapping CBOR-specific concepts to their JSON equivalents.

### What is CDDL?

CDDL (Concise Data Definition Language) is defined in [RFC 8610](https://www.rfc-editor.org/rfc/rfc8610) and is a schema language primarily used for expressing CBOR (Concise Binary Object Representation) and JSON data structures. CDDL provides a human-readable notation for defining data formats with support for complex type definitions, constraints, and generics.

### What is JSON Structure?

JSON Structure is a schema format that extends JSON Schema concepts with additional features for data modeling. It supports:
- Extended primitive types (`int64`, `float`, `double`, `binary`)
- Tuple types with ordered properties
- Alternate names (`altnames`) for field mappings
- Extension declarations (`$uses`)

### Supported CDDL Elements

#### Primitive Types

CDDL primitive types are mapped to JSON Structure types as follows:

| CDDL Type | JSON Structure Type | Notes |
|-----------|-------------------|-------|
| `uint` | `int64` | Unsigned integer (CBOR major type 0) |
| `nint` | `int64` | Negative integer (CBOR major type 1) |
| `int` | `int64` | Signed integer (uint or nint) |
| `float16` | `float` | Half precision float |
| `float32` | `float` | Single precision float |
| `float64` | `double` | Double precision float |
| `float` | `double` | Generic float |
| `tstr`, `text` | `string` | Text string (UTF-8) |
| `bstr`, `bytes` | `binary` | Byte string |
| `bool` | `boolean` | Boolean value |
| `true`, `false` | `boolean` | Boolean literals |
| `nil`, `null` | `null` | Null value |
| `any` | `any` | Any type |
| `undefined` | `null` | CBOR undefined (no JSON equivalent) |

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
primitives = {
    text-string: tstr
    integer: int
    flag: bool
    nothing: nil
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "text_string": { "type": "string" },
    "integer": { "type": "int64" },
    "flag": { "type": "boolean" },
    "nothing": { "type": "null" }
  },
  "required": ["text_string", "integer", "flag", "nothing"]
}
```

</td></tr></table>

#### Objects (Maps)

CDDL maps with member keys are converted to JSON Structure objects. Each member becomes a property with the appropriate type.

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
person = {
    name: tstr
    age: uint
    ? email: tstr
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "age": { "type": "int64" },
    "email": { "type": "string" }
  },
  "required": ["name", "age"]
}
```

</td></tr></table>

Maps with computed keys (like `* tstr => any`) are converted to JSON Structure map types with `keys` and `values` specifications:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
string-map = { * tstr => int }
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "map",
  "keys": { "type": "string" },
  "values": { "type": "int64" }
}
```

</td></tr></table>

#### Arrays

CDDL arrays with homogeneous items are converted to JSON Structure arrays:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
; Zero or more strings
string-list = [* tstr]

; One or more integers
int-array = [+ int]

; Zero or one byte string
optional-bytes = [? bstr]
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "string_list": {
      "type": "array",
      "items": { "type": "string" }
    },
    "int_array": {
      "type": "array",
      "items": { "type": "int64" }
    },
    "optional_bytes": {
      "type": "array",
      "items": { "type": "binary" }
    }
  }
}
```

</td></tr></table>

#### Tuples

CDDL arrays with heterogeneous items (different types in sequence) are converted to JSON Structure tuples. Tuples use the `tuple` keyword with a `properties` object where each position is named `_0`, `_1`, etc.

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
fixed-tuple = [tstr, int, bool]
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "tuple",
  "properties": {
    "_0": { "type": "string" },
    "_1": { "type": "int64" },
    "_2": { "type": "boolean" }
  },
  "tuple": ["_0", "_1", "_2"]
}
```

</td></tr></table>

#### Choice/Union Types

CDDL type choices (`/`) are converted to JSON Structure union types using a `type` array:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
; Simple type choice
string-or-int = tstr / int

; Literal string choices (enum-like)
status = "pending" / "active" / "completed"

; Choice between complex types
shape = circle / rectangle

circle = {
    type: "circle"
    radius: float
}

rectangle = {
    type: "rectangle"
    width: float
    height: float
}
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "string_or_int": {
      "type": ["string", "int64"]
    },
    "status": {
      "type": "string",
      "enum": ["pending", "active", "completed"]
    },
    "shape": {
      "type": [
        { "$ref": "#/definitions/circle" },
        { "$ref": "#/definitions/rectangle" }
      ]
    }
  }
}
```

</td></tr></table>

**Note:** Union types in JSON Structure use an array for the `type` field. Primitive types are represented as simple strings (e.g., `["string", "int64"]`), while references to named types use object notation with `$ref` (e.g., `{ "$ref": "#/definitions/circle" }`). When a CDDL choice consists entirely of string literals (e.g., `"a" / "b" / "c"`), it is converted to a string type with an `enum` constraint instead of a union. This follows the JSON Structure specification Section 3.5.1.

#### Type References

Named types in CDDL are converted to `$ref` references in JSON Structure, pointing to entries in the `definitions` section:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
address = {
    street: tstr
    city: tstr
}

person = {
    name: tstr
    home: address
}
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "address": {
      "type": "object",
      "properties": {
        "street": { "type": "string" },
        "city": { "type": "string" }
      },
      "required": ["street", "city"]
    },
    "person": {
      "type": "object",
      "properties": {
        "name": { "type": "string" },
        "home": { "$ref": "#/definitions/address" }
      },
      "required": ["name", "home"]
    }
  }
}
```

</td></tr></table>

#### Occurrence Indicators

CDDL occurrence indicators control field optionality and array repetition:

| Indicator | Meaning | JSON Structure Mapping |
|-----------|---------|----------------------|
| (none) | Exactly one (required) | Field in `required` array |
| `?` | Zero or one (optional) | Field not in `required` array |
| `*` | Zero or more | `type: "array"` with items |
| `+` | One or more | `type: "array"` with items |
| `n*m` | Between n and m | `type: "array"` with minItems/maxItems |

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
config = {
    host: tstr           ; required
    ? port: uint         ; optional
    ? tags: [* tstr]     ; optional array
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "host": { "type": "string" },
    "port": { "type": "int64" },
    "tags": {
      "type": "array",
      "items": { "type": "string" }
    }
  },
  "required": ["host"]
}
```

</td></tr></table>

#### Control Operators

CDDL control operators (RFC 8610 Section 3.8) provide validation constraints. The following operators are supported:

##### `.size` - Size Constraints

Controls the size of strings (character count) or byte strings:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
; String length between 3 and 20
username = tstr .size (3..20)

; Fixed 32-byte hash
sha256-hash = bstr .size 32
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "username": {
      "type": "string",
      "minLength": 3,
      "maxLength": 20
    },
    "sha256_hash": {
      "type": "binary",
      "minLength": 32,
      "maxLength": 32
    }
  }
}
```

</td></tr></table>

##### `.regexp` - Regular Expression Pattern

Constrains strings to match a regular expression pattern:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
email = tstr .regexp "[a-z]+@[a-z]+\\.[a-z]+"
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "string",
  "pattern": "[a-z]+@[a-z]+\\.[a-z]+"
}
```

</td></tr></table>

##### `.default` - Default Values

Specifies a default value for a type:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
timeout-ms = uint .default 5000
enabled = bool .default true
greeting = tstr .default "Hello!"
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "timeout_ms": {
      "type": "int64",
      "default": 5000
    },
    "enabled": {
      "type": "boolean",
      "default": "true"
    },
    "greeting": {
      "type": "string",
      "default": "Hello!"
    }
  }
}
```

</td></tr></table>

##### `.lt`, `.le`, `.gt`, `.ge` - Comparison Operators

Numeric comparison constraints:

| Operator | Meaning | JSON Structure |
|----------|---------|----------------|
| `.lt` | Less than | `exclusiveMaximum` |
| `.le` | Less than or equal | `maximum` |
| `.gt` | Greater than | `exclusiveMinimum` |
| `.ge` | Greater than or equal | `minimum` |

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
port-number = uint .le 65535
positive-int = int .ge 1
negative-int = int .lt 0
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "port_number": {
      "type": "int64",
      "maximum": 65535
    },
    "positive_int": {
      "type": "int64",
      "minimum": 1
    },
    "negative_int": {
      "type": "int64",
      "exclusiveMaximum": 0
    }
  }
}
```

</td></tr></table>

##### `.eq` - Equality (Const Values)

Constrains a value to exactly match:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
version = uint .eq 1
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "int64",
  "const": 1
}
```

</td></tr></table>

##### Range Constraints

CDDL ranges (`..` inclusive, `...` exclusive upper bound) are converted to minimum/maximum constraints:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
; Inclusive range 0-255
byte-value = 0..255

; Exclusive upper bound
percentage = 0...101
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "byte_value": {
      "type": "int64",
      "minimum": 0,
      "maximum": 255
    },
    "percentage": {
      "type": "int64",
      "minimum": 0,
      "exclusiveMaximum": 101
    }
  }
}
```

</td></tr></table>

#### Generic Types

CDDL generic types (parameterized types) are fully supported. Generic templates are expanded at each instantiation site with concrete type arguments:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
; Generic optional type
optional<T> = T / nil

; Generic pair (becomes tuple)
pair<K, V> = [K, V]

; Generic result type
result<T, E> = success<T> / error<E>
success<T> = { status: "ok", value: T }
error<E> = { status: "error", error: E }

; Usage
person-name = optional<tstr>
string-int-pair = pair<tstr, int>
api-result = result<response, error-info>
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "person_name": {
      "type": ["string", "null"]
    },
    "string_int_pair": {
      "type": "tuple",
      "properties": {
        "_0": { "type": "string" },
        "_1": { "type": "int64" }
      },
      "tuple": ["_0", "_1"]
    },
    "api_result": {
      "type": [
        {
          "type": "object",
          "properties": {
            "status": { "type": "string", "const": "ok" },
            "value": { "$ref": "#/definitions/response" }
          },
          "required": ["status", "value"]
        },
        {
          "type": "object",
          "properties": {
            "status": { "type": "string", "const": "error" },
            "error": { "$ref": "#/definitions/error_info" }
          },
          "required": ["status", "error"]
        }
      ]
    }
  }
}
```

</td></tr></table>

**Note:** Generic template definitions (like `optional<T>`, `pair<K,V>`) are not emitted to the output; only concrete instantiations are included.

#### Unwrap Operator

The CDDL unwrap operator (`~`) is used for type composition by including all members of one type into another. In JSON Structure, this is represented using the `$extends` mechanism, which is semantically equivalent:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
; Base header with common fields
base-header = {
    version: int,
    timestamp: int
}

; Extended header inherits from base
extended-header = {
    ~base-header,
    message-id: tstr,
    priority: int
}
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "base_header": {
      "type": "object",
      "properties": {
        "version": { "type": "int64" },
        "timestamp": { "type": "int64" }
      },
      "required": ["version", "timestamp"]
    },
    "extended_header": {
      "type": "object",
      "$extends": "#/definitions/base_header",
      "properties": {
        "message_id": { "type": "string" },
        "priority": { "type": "int64" }
      },
      "required": ["message_id", "priority"]
    }
  }
}
```

</td></tr></table>

**Multiple Unwraps:** When a type unwraps multiple base types, `$extends` becomes an array:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
header = { version: int }
metadata = { tags: [* tstr] }

; Multiple inheritance
full-record = {
    ~header,
    ~metadata,
    data: bstr
}
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "full_record": {
      "type": "object",
      "$extends": [
        "#/definitions/header",
        "#/definitions/metadata"
      ],
      "properties": {
        "data": { "type": "binary" }
      },
      "required": ["data"]
    }
  }
}
```

</td></tr></table>

**Note:** The `$extends` keyword is part of the JSON Structure core specification (Section 3.10.2) for type inheritance, not an add-in extension.

#### CBOR Tags

CDDL CBOR tags (`#6.n(type)`) are unwrapped to their underlying type, as CBOR tags have no direct JSON equivalent:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
; Tag 0 = date-time string
datetime = #6.0(tstr)

; Tag 37 = UUID
uuid = #6.37(bstr)
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "datetime": { "type": "string" },
    "uuid": { "type": "binary" }
  }
}
```

</td></tr></table>

#### Comments and Descriptions

CDDL comments (starting with `;`) are captured and converted to `description` fields in JSON Structure:

<table width="100%"><tr><th>CDDL</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```cddl
; A person record with basic info
person = {
    ; Full name of the person
    name: tstr,
    ; Age in years
    age: uint,
    ; Email address (optional)
    ? email: tstr
}
```

</td>
<td style="vertical-align:top">

```json
{
  "definitions": {
    "person": {
      "type": "object",
      "description": "A person record with basic info",
      "properties": {
        "name": {
          "type": "string",
          "description": "Full name of the person"
        },
        "age": {
          "type": "int64",
          "description": "Age in years"
        },
        "email": {
          "type": "string",
          "description": "Email address (optional)"
        }
      },
      "required": ["name", "age"]
    }
  }
}
```

</td></tr></table>

### Unsupported or Partially Supported CDDL Elements

The following CDDL features are not fully supported:

#### CDDL Sockets (`$name`, `$$name`)

CDDL sockets are extensibility points that allow schemas to be extended. These are not supported due to parser limitations.

#### `.ne` Operator

The "not equal" operator has no direct JSON Structure equivalent and is ignored.

#### `.bits` Operator

The bit flags operator is CBOR-specific. The base type is extracted but the constraint is not preserved:

```cddl
flags = uint .bits (flag1 / flag2 / flag3)
; Converts to: { "type": "int64" }
```

#### `.cbor` and `.cborseq` Operators

These operators specify embedded CBOR data within byte strings. The base `binary` type is used:

```cddl
embedded = bstr .cbor some-type
; Converts to: { "type": "binary" }
```

#### `.within` and `.and` Operators

Type intersection operators have limited support. The base type is used:

```cddl
bounded-int = int .within (0..100)
; Converts to: { "type": "int64" }
```

### Name Normalization

CDDL allows hyphens in identifiers, but JSON Structure property names follow different conventions. Avrotize normalizes names by replacing hyphens with underscores:

| CDDL Name | JSON Structure Name | `altnames.cddl` |
|-----------|-------------------|-----------------|
| `my-field` | `my_field` | `"my-field"` |
| `http-method` | `http_method` | `"http-method"` |
| `string-list` | `string_list` | `"string-list"` |

The original CDDL name is preserved in the `altnames` object for round-trip compatibility:

```json
{
  "my_field": {
    "type": "string",
    "altnames": {
      "cddl": "my-field"
    }
  }
}
```

### Extension Declarations

The output JSON Structure schema includes a `$uses` array listing the JSON Structure extensions used:

```json
{
  "$schema": "https://json-structure.org/meta/extended/v0/#",
  "$id": "https://example.com/schema.json",
  "$uses": [
    "JSONStructureAlternateNames",
    "JSONStructureValidation"
  ],
  "definitions": { ... }
}
```

| Extension | When Used |
|-----------|-----------|
| `JSONStructureAlternateNames` | When any field has `altnames` |
| `JSONStructureValidation` | When any validation keywords are used (`minimum`, `maximum`, `minLength`, `maxLength`, `pattern`, `const`, `default`) |
