<!-- omit from toc -->

# Avrotize Schema Specification

<!-- omit from toc -->

## Abstract

This document provides defines Avrotize Schema model. Avrotize Schema builds on
and is a superset of the [Apache
Avro](https://avro.apache.org/docs/1.11.1/specification/) schema model.

<!-- omit from toc -->

## Contents
- [Avrotize Schema Specification](#avrotize-schema-specification)
  - [Abstract](#abstract)
  - [Contents](#contents)
  - [1. Introduction](#1-introduction)
  - [2. Notational Conventions](#2-notational-conventions)
  - [3. Schema Specification](#3-schema-specification)
    - [3.1. Schema Declarations](#31-schema-declarations)
      - [3.1.1. Schema documents](#311-schema-documents)
      - [3.1.2. Media Type](#312-media-type)
    - [3.2. Documentation Strings](#32-documentation-strings)
      - [3.2.1. International Documentation Strings](#321-international-documentation-strings)
    - [3.3. Named Types](#33-named-types)
      - [3.3.1. Alias Names](#331-alias-names)
      - [3.3.2. Alternate Names](#332-alternate-names)
        - [3.3.2.1. Alternate Names for JSON Encoding](#3321-alternate-names-for-json-encoding)
        - [3.3.2.2. Alternate Names for Display (Internationalization)](#3322-alternate-names-for-display-internationalization)
    - [3.4. Naming conventions](#34-naming-conventions)
    - [3.5. Extensibility](#35-extensibility)
    - [3.6. Primitive Type Schemas](#36-primitive-type-schemas)
      - [3.6.1. null](#361-null)
      - [3.6.2. boolean](#362-boolean)
      - [3.6.3. int](#363-int)
      - [3.6.4. long](#364-long)
      - [3.6.5. float](#365-float)
      - [3.6.6. double](#366-double)
      - [3.6.7. bytes](#367-bytes)
      - [3.6.8. string](#368-string)
    - [3.7. Fixed Type](#37-fixed-type)
    - [3.8. Logical Types](#38-logical-types)
      - [3.8.1. decimal](#381-decimal)
      - [3.8.2. UUID](#382-uuid)
      - [3.8.3. date](#383-date)
      - [3.8.4. time-millis](#384-time-millis)
      - [3.8.5. time-micros](#385-time-micros)
      - [3.8.6. timestamp-millis](#386-timestamp-millis)
      - [3.8.7. timestamp-micros](#387-timestamp-micros)
      - [3.8.8. local-timestamp-millis](#388-local-timestamp-millis)
      - [3.8.9. local-timestamp-micros](#389-local-timestamp-micros)
      - [3.8.10. duration](#3810-duration)
    - [3.9. `record` Type](#39-record-type)
      - [3.9.1. `record` field Declarations](#391-record-field-declarations)
      - [3.10. `enum` Type](#310-enum-type)
    - [3.11. array](#311-array)
    - [3.12. map](#312-map)
    - [3.13. Type Unions](#313-type-unions)
  - [4. Parsing Canonical Form for Avro Schemas](#4-parsing-canonical-form-for-avro-schemas)
    - [4.1 Overview](#41-overview)
    - [4.2 Transforming into Parsing Canonical Form](#42-transforming-into-parsing-canonical-form)
  - [5. Schema Fingerprints](#5-schema-fingerprints)
    - [5.1 Overview](#51-overview)
    - [5.2 Recommended Fingerprinting Algorithms](#52-recommended-fingerprinting-algorithms)
      - [5.2.1 SHA-256](#521-sha-256)
      - [5.2.2 Rabin Fingerprint (64-bit)](#522-rabin-fingerprint-64-bit)
      - [5.2.3 MD5](#523-md5)
    - [5.3 Security Considerations](#53-security-considerations)
    - [5.4 Rabin Fingerprint Algorithm](#54-rabin-fingerprint-algorithm)
  - [6. Security Considerations](#6-security-considerations)
  - [7. IANA Considerations](#7-iana-considerations)
    - [7.1. Media Type Registration](#71-media-type-registration)
    - [7.2. Schema reference parameter](#72-schema-reference-parameter)
  - [7. References](#7-references)

---

## 1. Introduction

Avrotize Schema is a schema format to define structured data types in a platform
and programming language-independent manner.

Avrotize Schema is a full superset of the [Apache
Avro](https://avro.apache.org/docs/1.11.1/specification/) schema model.

Yet, the purpose of Avrotize Schema differs from Apache Avro Schema in that it
is not primarily intended for serialization and deserialization of data using
the Avro framework. Instead, Avrotize Schema is designed to capture any kind of
structured data in a way that is easy to read and write, and can be used in any
context where structured data definitions are needed.

The Avrotize project provides converters from and to a variety of other schema
models as well as database schema and data class code generation tools, all of
which use Avrotize Schema as the internal representation of the metadata model
on which they operate.

Avrotize uses the extensibility of Apache Avro Schema to provide additional
features and capabilities. Each Avrotize Schema is a valid Avro Schema, and
Avrotize Schema can be used in place of Avro Schema in any Avro Schema context.

As the Apache Avro project does not provide a formal specification that is
independent of the framework implementation, this document also defines the Avro
schema elements in a formal and independent manner, detailing the syntax and
semantics. Avrotize extensions to Avro Schema are annotated as such in this 
document.

Avro Schemas are defined in JSON, which is easily readable and writable.

## 2. Notational Conventions

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
interpreted as described in RFC 2119.

## 3. Schema Specification

### 3.1. Schema Declarations

A **schema** is a JSON value or object that defines the structure of data
being serialized or deserialized. Primitive type schemas are represented as JSON
values (strings), while logical and complex type schemas are represented as JSON
objects. Type unions are represented as JSON arrays.

| Type kind | Schema                                     |
| --------- | ------------------------------------------ |
| primitive | `"string"`                                 |
| logical   | `{ "type": "int", "logicalType": "date" }` |
| complex   | `{ "type": "array", "items": "string" }`   |
| union     | `["null", "string"]`                       |

Complex schemas of type `record`, `enum`, `fixed` are
[**named types**](#33-named-types), which have a **fullname** composed of a
**namespace** and a **name**. The **namespace** is a string that commonly
identifies the schema's organization or project, and the **name** is a string
that identifies the schema within the namespace.

All named types used within a schema MUST be declared where they are first used.
Named type declations are visible within the entire schema document once
declared, independent of where in the overall type hierarchy the declaration
occurs.

Subsequent references to a declared named type MUST be made by its **fullname**.

#### 3.1.1. Schema documents

A **schema document**, which is a restriction of the general Avro schema
pattern to enable sharing of schemas across different parties, MUST contain
either a single named type or a union of named types at its root. This
restriction ensures that code generation tools can generate code for the schema
with unambiguous type names.

All complex types used in a schema document MUST be defined within the same
schema document. There is no import or include mechanism for referencing types
defined in other schema documents. This restriction ensures that the schema is
self-contained and can be easily shared and distributed.

#### 3.1.2. Media Type

The media type for **schema documents** is `application/vnd.apache.avro.schema+json`.

See [IANA Considerations](#5-iana-considerations) for more information.

### 3.2. Documentation Strings

All schemas and `record` field declarations MAY contain an OPTIONAL `doc`
attribute, which is a string that provides human-readable documentation for the
schema. The `doc` attribute is used to describe the purpose and usage of the
schema.

Example:

```json
{
  "type": "record",
  "name": "Employee",
  "fields": [
    { "name": "name", "type": "string", "doc": "The name of the employee" },
    { "name": "email", "type": "string", "doc": "The email address" }
  ],
  "doc": "A record representing an employee"
}
```

#### 3.2.1. International Documentation Strings

> Avrotize extension

The OPTIONAL `docs` attribute MAY contain a map of strings keyed by language codes to
provide internationalized documentation for the schema. The language codes are defined
by [RFC 5646](https://tools.ietf.org/html/rfc5646). The `docs` attribute MAY exist
side-by-side with the `doc` attribute.

Example:

```json
{
  "type": "record",
  "name": "Employee",
  "fields": [
    {
      "name": "name",
      "type": "string",
      "doc": "The name of the employee",
      "docs": {
        "de": "Der Name des Mitarbeiters",
        "ja": "従業員の名前"
      }
    },
    {
      "name": "email",
      "type": "string",
      "docs": {
        "en": "The email address",
        "de": "Die E-Mail-Adresse",
        "ja": "メールアドレス"
      }
    }
  ],
  "doc": "A record representing an employee",
  "docs": {
    "de": "Ein Datensatz, der einen Mitarbeiter repräsentiert",
    "ja": "従業員を表すレコード"
  }
}
```

### 3.3. Named Types

Named types allow for the definition of complex types that can be reused once
declared and subsequently referenced by name within the schema.

Named types MUST be defined with a REQUIRED `name` and OPTIONAL `namespace`
attribute. Schemas with `record`, `enum`, and `fixed` types are named types.

The `name` attribute is a REQUIRED string that identifies the schema within the
namespace. The `namespace` attribute is an OPTIONAL string that identifies a
scope for names.

When the `namespace` attribute is not present, the schema is in the namespace of
its enclosing schema. When there is no enclosing schema, the schema is in the
default namespace. The default namespace is an empty string.

A schema MAY contain multiple named types within the same namespace or
across different namespaces.

The value of the `name` attribute MUST be a not-empty string and start with a
letter from `a-z` or `A-Z`. Subsequent characters MUST be letters from `a-z` or
`A-Z`, digits, or underscores (`_`). This restriction ensures that the `name`
attribute is a valid identifier in most programming languages and databases.

The value of the `namespace` attribute MUST be sequence of one or more
`name`-like strings separated by dots (`.`).

The **fullname** of a named type is the concatenation of the `namespace` and
`name` attributes, separated by a dot (`.`).

```abnf
name = ALPHA *(ALPHA / DIGIT / "_")
namespace = name *("." name)
fullname = (namespace ".") name
```

The following is an example of a record schema named `Contact` in the
`com.example` namespace. It has a nested record schema named `Address` defined
at first use for the `mailingAddress` field, which inherits the namespace from
its enclosing schema. The type os referenced again by `fullname` for the
`billingAddress` field. The "fullname" of the resulting schema is
`com.example.Contact`.

```json
{
  "type": "record",
  "name": "Contact",
  "namespace": "com.example",
  "fields": [
    { "name": "name", "type": "string" },
    { "name": "email", "type": "string" },
    {
      "name": "mailingAddress",
      "type": {
        "type": "record",
        "name": "Address",
        "fields": [
          { "name": "street", "type": "string" },
          { "name": "city", "type": "string" },
          { "name": "state", "type": "string" },
          { "name": "zip", "type": "string" }
        ]
      }
    },
    { "name": "billingAddress", "type": "com.example.Address" }
  ]
}
```

#### 3.3.1. Alias Names

Named types MAY have an OPTIONAL `aliases` attribute, which is an array of
strings that are alternative names for the named type. The `aliases` attribute
MUST NOT contain the `name` attribute of the named type.

The `aliases` attribute is used to maintain compatibility when the name of a
named type changes. When a named type is renamed, the `aliases` attribute can be
used to specify the old name of the type. This allows readers to recognize the
old name and map it to the new name.

#### 3.3.2. Alternate Names

> Avrotize extension

All named types AND fields MAY have an OPTIONAL `altnames` attribute, which
is a map of alternative names for the named type or field. Alternative names
are different from alias names in that they are not restricted to the `name`
grammar and can be any string.

The `key` of an alternate name in the `altnames` map its purpose, while the
`value` is the alternative name itself. The key value `"json"` and the key-prefix
`"display"` are reserved, any other keys are user-defined.

For enum symbols, the `altsymbols` attribute is used to provide alternate names
for symbols. The `altsymbols` attribute contains a map of maps. The top-level map
is keyed by the purpose of the alternate name as with `altnames`. The second-level
map is keyed by the symbol name and contains the alternate name.

##### 3.3.2.1. Alternate Names for JSON Encoding

For the "Plain JSON" encoding, the `altnames` attribute is used to map JSON
identifiers that are incompatible with the schema `name` restrictions. The
reserved key for this purpose is `json`.

The following is an example of a record schema named `Contact` in the
`com.example` namespace. It has a JSON field names `first-name` and `last-name`
that are mapped to the field names `firstName` and `lastName` respectively.

```json
{
  "type": "record",
  "name": "Contact",
  "namespace": "com.example",
  "fields": [
    { "name": "firstName", "type": "string", "altnames": { "json": "first-name" } }
    { "name": "lastName", "type": "string", "altnames": { "json": "last-name" } }
  ]
}
```

The following is an example of an enum schema named `Color` in the `com.example`
namespace. It has alternate names, the respective hex-symbols of the colors, for
the symbols `RED`, `GREEN`, and `BLUE` that are used in the JSON encoding.

```json
{
  "type": "enum",
  "name": "Color",
  "namespace": "com.example",
  "symbols": ["RED", "GREEN", "BLUE"],
  "altsymbols": {
    "json": {
      "RED": "#FF0000",
      "GREEN": "#00FF00",
      "BLUE": "#0000FF"
    }
  }
}
```

##### 3.3.2.2. Alternate Names for Display (Internationalization)

For the purpose of internationalization, the `altnames` attribute is used to
provide alternate names for display purposes. The reserved key-prefix for this
purpose is `display`. In many cases, schematized data is displayed to and
occasionally even defined by end-users who prefer different  
languages. The technical `name` of a field or type may not be meaningful to
end-users.

The key value for a display alternate name is the language code of the language
prefixed by `display:`, such as `display:en` for English. The language codes are
defined by [RFC 5646](https://tools.ietf.org/html/rfc5646).

The following is an example of a record schema named `Address` in the
`com.example` namespace, with display alternate names for the address elements in
German, Japanese, and French.

```json
{
  "type": "record",
  "name": "Address",
  "namespace": "com.example",
  "fields": [
    {
      "name": "street",
      "type": "string",
      "altnames": {
        "display:de": "Straße",
        "display:ja": "番地",
        "display:fr": "Rue"
      }
    },
    {
      "name": "city",
      "type": "string",
      "altnames": {
        "display:de": "Stadt",
        "display:ja": "市",
        "display:fr": "Ville"
      }
    },
    {
      "name": "state",
      "type": "string",
      "altnames": {
        "display:de": "Bundesland",
        "display:ja": "都道府県",
        "display:fr": "État"
      }
    },
    {
      "name": "zip",
      "type": "string",
      "altnames": {
        "display:de": "Postleitzahl",
        "display:ja": "郵便番号",
        "display:fr": "Code postal"
      }
    }
  ]
}
```

### 3.4. Naming conventions

It is RECOMMENDED for the `namespace` attribute to be a reverse domain name of a
domain that your organization controls, such as `com.example`, to avoid naming
conflicts. It is also RECOMMENDED for the namespace expression to be in
lowercase.

It is RECOMMENDED for the `name` attribute of named types to use `PascalCase`,
where the first letter of each word is capitalized and there are no spaces or
underscores.

It is RECOMMENDED for the `name` attribute of [record fields]() to use `camelCase`,
where the first letter of the first word is lowercase and the first letter of
each subsequent word is capitalized, with no spaces or underscores.

### 3.5. Extensibility

Schemas are extensible, allowing for the addition of any user-defined attributes
to any schema. Extension attributes MUST be ignored by processors that do not
know how to handle them. Extension attributes MUST be made accessible by Apache
implementations for reading and writing by clients.

To avoid conflicts with future schema extensions, the names of user-defined
attributes SHOULD be chosen to avoid collisions. It is RECOMMENDED to use a
prefix, as in `myorg_myattribute`, to denote user-defined attributes.

### 3.6. Primitive Type Schemas

The primitive types are defined in this section.

#### 3.6.1. null

Represents an absence of a value. Used to allow optional fields or to
represent non-existent values in data records.

#### 3.6.2. boolean

Represents a boolean value, true or false. This type is commonly utilized for
flags and boolean status indicators in data.

#### 3.6.3. int

Represents a 32-bit signed integer. It accommodates integer values ranging from
$(-2^{31})$ to $(2^{31}-1)$.

#### 3.6.4. long

Represents a 64-bit signed integer. It can store values from $(-2^{63})$ to
$(2^{63}-1)$.

#### 3.6.5. float

Represents a single precision 32-bit IEEE 754 floating-point number. Suitable
for numerical values that do not require the precision of double-precision types
but need to cover a broad range of values. IEEE 754 single-precision floats have
an approximate precision of 7 decimal digits and can represent values ranging
from approximately $(1.4 \times 10^{-45})$ to $(3.4 \times 10^{38})$.

#### 3.6.6. double

Represents a double precision 64-bit IEEE 754 floating-point number. This type
provides roughly double the precision of the `float` type, with an approximate
precision of 15 decimal digits. It can accommodate values ranging from about
$(4.9 \times 10^{-324})$ to $(1.8 \times 10^{308})$.

#### 3.6.7. bytes

Represents a sequence of 8-bit unsigned bytes. Used to store raw binary data,
such as file contents or binary-encoded values.

#### 3.6.8. string

Represents a sequence of Unicode characters encoded in UTF-8. This type is ideal
for textual data that may include any character from the Unicode standard.

### 3.7. Fixed Type

The `fixed` type is a [named type](#313-named-types) that represents a
fixed-size sequence of bytes. The size of the fixed-size sequence is defined by
the `size` attribute, which is an integer.

For example, a SHA-256 hash value can be represented as a `fixed` type with a
size of 32 bytes.

```json
{
  "type": "fixed",
  "name": "SHA256",
  "size": 32
}
```

Since the `fixed` type is a named type, it MUST be declared where it is first
used and can then be referenced by its [fullname](#313-named-types).

### 3.8. Logical Types

Logical types provide a way to extend the primitive types with additional
semantics. Logical types are defined as an attribute in the schema that
annotates the primitive type. The `logicalType` attribute value is a string that
identifies the logical type.

Applications that do not recognize the logical type MUST ignore the `logicalType`
attribute and treat the schema as if the logical type were not present.

The following logical types are well-known and defined in this document. Avrotize
Schema extends the Avro logical type set with RFC 3339 date and time annotations
for `string` types.

#### 3.8.1. decimal

The `decimal` logical type represents arbitrary-precision fixed-point numbers.
It is defined by two attributes: `precision` and `scale`. The `precision`
attribute specifies the total number of digits in the number, while the `scale`
attribute specifies the number of digits to the right of the decimal point.

The `decimal` logical type is represented as a `bytes` or `fixed` type,
where the bytes contain the two's complement representation of the decimal
number. The REQUIRED `precision` and OPTIONAL `scale` attributes are stored as
metadata in the schema.

```json
{
  "type": "bytes",
  "logicalType": "decimal",
  "precision": 10,
  "scale": 2
}
```

In Avrotize Schema, the `decimal` logical type MAY also annotate the `string`
primitive type. In this case, the string value is a decimal number represented as
a string value. The [ABNF](https://tools.ietf.org/html/rfc5234) grammar for the
`decimal` logical type when expressed as a string is as follows:

```abnf
decimal = [ sign ] int [ frac ]
sign    = "+" / "-"
int     = 1*DIGIT
frac    = "." 1*DIGIT
```

#### 3.8.2. UUID

The `uuid` logical type represents a universally unique identifier (UUID) as
defined by [RFC 4122](https://tools.ietf.org/html/rfc4122). The UUID is a
128-bit value that is typically represented as a 32-character hexadecimal string
with hyphens separating the parts.

The `uuid` logical type annotates the `string` primitive type to indicate that
the string value is a UUID.

Example:

```json
{
  "type": "string",
  "logicalType": "uuid"
}
```

#### 3.8.3. date

The `date` logical type represents a calendar date without a time component. 

In Avro Schema, the logical type annotates the `int` primitive type. It is
defined as the number of days since the Unix epoch, January 1, 1970. The `date`
logical type annotates the `int` primitive type.

Example:

```json
{
  "type": "int",
  "logicalType": "date"
}
```

In Avrotize Schema, the `date` logical type MAY additionally annotate the
`string` primitive type. In this case, the string value is an [RFC
3339](https://tools.ietf.org/html/rfc3339) "full-date" string, i.e. a date
without a time component.

#### 3.8.4. time-millis

The `time-millis` logical type represents a time of day with millisecond
precision.

In Avro Schema, the `time-millis` logical type annotates the `int` primitive
type. It is defined as the number of milliseconds after midnight.

Example:

```json
{
  "type": "int",
  "logicalType": "time-millis"
}
```

In Avrotize Schema, the `time-millis` logical type can also annotate the
`string` primitive type. In this case, the string value is an [RFC
3339](https://tools.ietf.org/html/rfc3339) "partial-time" string, i.e. a time
of day with millisecond precision.

#### 3.8.5. time-micros

The `time-micros` logical type represents a time of day with microsecond
precision. 

In Avro Schema, it is defined as the number of microseconds after midnight. The
`time-micros` logical type annotates the `long` primitive type.

Example:

```json
{
  "type": "long",
  "logicalType": "time-micros"
}
```

In Avrotize Schema, the `time-micros` logical type MAY also annotate the
`string` primitive type. In this case, the string value is an [RFC
3339](https://tools.ietf.org/html/rfc3339) "partial-time" string, i.e. a time of
day with microsecond precision.

#### 3.8.6. timestamp-millis

The `timestamp-millis` logical type represents an instant in time with
millisecond precision. 

In Avro Schema, it is defined as the number of milliseconds since the
Unix epoch, January 1, 1970 00:00:00.00 UTC. The `timestamp-millis` logical type
annotates the `long` primitive type.

Example:

```json
{
  "type": "long",
  "logicalType": "timestamp-millis"
}
```

In Avrotize Schema, the `timestamp-millis` logical type MAY also annotate the
`string` primitive type. In this case, the string value is an [RFC
3339](https://tools.ietf.org/html/rfc3339) "date-time" string, i.e. a date and
time with millisecond precision.

#### 3.8.7. timestamp-micros

The `timestamp-micros` logical type represents an instant in time with
microsecond precision. 

In Avro Schema, it is defined as the number of microseconds since the Unix
epoch, January 1, 1970 00:00:00.00 UTC. The `timestamp-micros` logical type
annotates the `long` primitive type.

Example:

```json
{
  "type": "long",
  "logicalType": "timestamp-micros"
}
```

In Avrotize Schema, the `timestamp-micros` logical type MAY also annotate the
`string` primitive type. In this case, the string value is an [RFC
3339](https://tools.ietf.org/html/rfc3339) "date-time" string, i.e. a date and
time with microsecond precision.

#### 3.8.8. local-timestamp-millis

The `local-timestamp-millis` logical type represents an instant in time with
millisecond precision in the local timezone. 

In Avro Schema, it is defined as the number of milliseconds since the Unix
epoch, January 1, 1970 00:00:00.00 in the local timezone. The
`local-timestamp-millis` logical type annotates the `long` primitive type.

Example:

```json
{
  "type": "long",
  "logicalType": "local-timestamp-millis"
}
```

In Avrotize Schema, the `local-timestamp-millis` logical type MAY also annotate
the `string` primitive type. In this case, the string value is an [RFC
3339](https://tools.ietf.org/html/rfc3339) "date-time" string, i.e. a date and
time with millisecond precision, whereby the offset is ignored.

#### 3.8.9. local-timestamp-micros

The `local-timestamp-micros` logical type represents an instant in time with
microsecond precision in the local timezone. 

In Avro Schema, it is defined as the number of microseconds since the Unix
epoch, January 1, 1970 00:00:00.00 in the local timezone. The
`local-timestamp-micros` logical type annotates the `long` primitive type.

Example:

```json
{
  "type": "long",
  "logicalType": "local-timestamp-micros"
}
```

In Avrotize Schema, the `local-timestamp-micros` logical type MAY also annotate
the `string` primitive type. In this case, the string value is an [RFC
3339](https://tools.ietf.org/html/rfc3339) "date-time" string, i.e. a date and
time with microsecond precision, whereby the offset is ignored.

#### 3.8.10. duration

The duration logical type represents an amount of time defined by a number of
months, days and milliseconds. This is not equivalent to a number of
milliseconds, because, depending on the moment in time from which the duration
is measured, the number of days in the month and number of milliseconds in a day
may differ. Other standard periods such as years, quarters, hours and minutes
can be expressed through these basic periods.

In Avro Schema, a duration logical type annotates fixed type of size 12, which
stores three little-endian unsigned integers that represent durations at
different granularities of time. The first stores a number in months, the second
stores a number in days, and the third stores a number in milliseconds.

Example:

```json
{
  "type": "fixed",
  "name": "Duration",
  "size": 12,
  "logicalType": "duration"
}
```

In Avrotize Schema, the `duration` logical type MAY also annotate the `string`
primitive type. In this case, the string value is an [RFC
3339](https://tools.ietf.org/html/rfc3339) "duration" string (Appendix A).

### 3.9. `record` Type

The `record` type is a [named type](#33-named-types) that represents a set of
named fields. Each field has a name and a type. The `record` type is used to
define structured data types.

The following attributes are used to define a `record` type:

- `name`, `namespace`, `aliases`, `altnames`: See [Named Types](#33-named-types).
- `doc`, `docs`: See [Documentation Strings](#32-documentation-strings).
- `fields`: An array of field declarations

#### 3.9.1. `record` field Declarations

A field declaration is an object that contains the following attributes:

- `name`: The name of the field. The value of the `name` attribute MUST be a
  not-empty string and start with a letter from `a-z` or `A-Z`. Subsequent
  characters MUST be letters from `a-z` or `A-Z`, digits, or underscores (`_`).
  This restriction ensures that the `name` attribute is a valid identifier in
  most programming languages and databases.
- `aliases`: See [Alias Names](#331-alias-names).
- `altnames`: See [Alternate Names](#332-alternate-names).
- `type`: The type of the field. The `type` attribute's value MUST be an
  [schema](#31-schema-declarations) expression.
- `doc`, `docs`: See [Documentation Strings](#32-documentation-strings).
- `default`: The default value of the field. The `default` attribute's value
  MUST be a valid value of the field's type. The `default` attribute is OPTIONAL.
- `order`: The sort order of the field. The `order` attribute is OPTIONAL and
  MUST be one of the following string values:
  - `ascending`: The field is sorted in ascending order.
  - `descending`: The field is sorted in descending order.
  - `ignore`: The field is not sorted.

The `default` attribute is used to provide a default value for the field when
the field is not present in the serialized data.

The value of the `default` attribute MUST be a valid value of the field's type.
Since the value is declared as a JSON value in the schema, the default
value MUST be encoded in JSON in accordance with the following mapping:

| Type      | JSON Type | Example    | Note                                                 |
| --------- | --------- | ---------- | ---------------------------------------------------- |
| null      | null      | `null`     |                                                      |
| boolean   | boolean   | `true`     |                                                      |
| int       | number    | `42`       |                                                      |
| long      | number    | `42`       |                                                      |
| float     | number    | `3.14`     |                                                      |
| double    | number    | `3.14`     |                                                      |
| bytes     | string    | `"\u00FF"` | Bytes are encoded as unicode escape sequences        |
| string    | string    | `"hello"`  |                                                      |
| fixed     | string    | `"\u00FF"` | Fixed values are encoded as unicode escape sequences |
| enum      | string    | `"SYMBOL"` |                                                      |
| array     | array     | `[]`       |                                                      |
| map       | object    | `{}`       |                                                      |

#### 3.10. `enum` Type

The named `enum` type defines a set of symbols. An enum typed value MUST one of those
symbols.

The following attributes are used to define an `enum` type:

- `name`, `namespace`, `aliases`, `altnames`: See [Named Types](#33-named-types).
- `doc`, `docs`: See [Documentation Strings](#32-documentation-strings).
- `symbols`: An array of strings that represent the symbols of the enum.
- `altsymbols`: See [Alternate Names](#332-alternate-names).

Example:

```json
{
  "type": "enum",
  "name": "Color",
  "namespace": "com.example",
  "symbols": ["RED", "GREEN", "BLUE"]
}
```

### 3.11. array

The `array` type represents a list of values, all of the same type specified by
the `items` attribute.

The following attributes are used to define an `array` type:

- `items`: The type of the elements in the array. The `items` attribute's value
  MUST be an [schema](#31-schema-declarations) expression.
- `default`: The default value of the array. The `default` attribute's value
  MUST be a valid value of the array's type. The `default` attribute is OPTIONAL.

Example:

```json
{
  "type": "array",
  "items": "string"
}
```

### 3.12. map

The `map` type represents a set of key-value pairs, where the keys are strings
and the values are of the specified type.

The following attributes are used to define a `map` type:

- `values`: The type of the values in the map. The `values` attribute's value
  MUST be an [schema](#31-schema-declarations) expression.
- `default`: The default value of the map. The `default` attribute's value MUST
  be a valid value of the map's type. The `default` attribute is OPTIONAL.

Example:

```json
{
  "type": "map",
  "values": "int"
}
```

### 3.13. Type Unions

A type union is an array of schema expressions. A value of a type union
MUST be a valid value of exactly one of the types in the union.

All types in a type union MUST be distinct.

Any primitive type MUST be included at most once, which also applies to logical
type annotations. A `UUID` logical type, which annotates `string`, and a
`string` primitive type therefore MUST NOT appear in the same type union.

A union MUST NOT contain more than one `array` type and NOT more than one `map`
type. Multiple array or map types therefore need to be modeled with type unions for
the array's `items` or map's `values` type.

A union MAY contain multiple, distinct named types directly or by reference.
Named types are distinct if they have different fullnames.

A very common use case for type unions is to declare optionality for values by
joining the desired type of the value with the `null` type in type union. The
following example shows a type union that represents a string or a null value.

```json
["null", "string"]
```

Type unions can otherwise be used to represent values that may be of different
types. The following example shows a type union that represents a string or a
boolean value.

```json
["string", "boolean"]
```

An other fairly common case for type unions is to provide a choice of two or
more `record` types. This pattern MAY also be used to define a collection of
`record` types in a single [schema document](#311-schema-documents).

With multiple records in a type union being permitted, it is RECOMMENDED for all
such records to be structurally distinct. This means that the records should
have different fields or field types. This is to help avoid ambiguity when
reading data that is serialized with a type union in cases where data
structuress are described with schema, but a data serialization model is
used where the data encoding does not support type markers.

```json
[
  {
    "type": "record",
    "name": "Person",
    "fields": [
      { "name": "name", "type": "string" },
      { "name": "age", "type": "int" }
    ]
  },
  {
    "type": "record",
    "name": "Organization",
    "fields": [
      { "name": "name", "type": "string" },
      { "name": "employees", "type": { "type": "array", "items": "Person" } }
    ]
  }
]
```

## 4. Parsing Canonical Form for Avro Schemas

### 4.1 Overview

The parsing canonical form of an Avrotize Schema is a JSON representation of the
schema that has a well-defined structure and ordering of elements, such that two
schemas that are semantically equivalent have the same canonical form.

The Parsing Canonical Form (PCF) standardizes schemas by removing irrelevant
differences. This ensures that two schemas are considered identical if their
PCFs match. PCF normalizes the JSON text, disregarding attributes irrelevant to
parsing. If the PCFs of two schemas are textually identical, then the schemas
are considered the same for reading data.

### 4.2 Transforming into Parsing Canonical Form

To transform a schema into its Parsing Canonical Form, apply the following steps
to the schema in JSON format:

1. **Primitive Conversion (PRIMITIVES)**: Convert primitive schemas to their
   simple form (e.g., `int` instead of `{"type":"int"}`).
2. **Namespace Handling (FULLNAMES)**: Replace short names with fullnames using
   the applicable namespaces, then remove namespace attributes.
3. **Attribute Stripping (STRIP)**: Retain only attributes relevant to parsing
   (`type`, `name`, `fields`, `symbols`, `items`, `values`, `size`). Remove
   others (e.g., `doc`, `aliases`).
4. **Field Ordering (ORDER)**: Order JSON object fields as follows: `name`,
   `type`, `fields`, `symbols`, `items`, `values`, `size`. For example, for an
   object with `type`, `name`, and `size` fields, the `name` field should appear
   first, followed by `type`, and then `size`.
5. **String Normalization (STRINGS)**: Replace escaped characters in JSON string
   literals with their UTF-8 equivalents.
6. **Integer Normalization (INTEGERS)**: Remove quotes around and leading zeros
   from JSON integer literals in `size` attributes.
7. **Whitespace Removal (WHITESPACE)**: Eliminate all whitespace outside JSON
   string literals.

## 5. Schema Fingerprints

### 5.1 Overview

A fingerprinting algorithm maps a large data item to a shorter bit string,
uniquely identifying the original data for practical purposes. Fingerprints of
PCFs facilitate various applications, such as caching encoders/decoders, tagging
data with schema identifiers, and negotiating common schemas between readers and
writers.

### 5.2 Recommended Fingerprinting Algorithms

#### 5.2.1 SHA-256

For applications tolerating longer fingerprints, use the SHA-256 digest
algorithm to generate 256-bit fingerprints. SHA-256 implementations are widely
available in many programming languages.

#### 5.2.2 Rabin Fingerprint (64-bit)

For minimal fingerprint length, use a 64-bit Rabin fingerprint. This provides
sufficient uniqueness for schema caches up to a million entries, with a
collision probability of 3E-8. We do not recommend shorter fingerprints due to
higher collision probabilities.

#### 5.2.3 MD5

For intermediate fingerprint length, use the MD5 message digest to generate
128-bit fingerprints. MD5 is suitable for handling tens of millions of schemas,
but for smaller sets, 64-bit fingerprints are sufficient. MD5 implementations
are commonly available.

### 5.3 Security Considerations

These fingerprints do not provide security guarantees. Surrounding security
mechanisms should prevent collision and pre-image attacks on schema
fingerprints, rather than relying on the security properties of the
fingerprints.

### 5.4 Rabin Fingerprint Algorithm

Rabin fingerprints use cyclic redundancy checks computed with irreducible
polynomials. Below is the definition of the 64-bit Avro fingerprinting
algorithm:

```java
long fingerprint64(byte[] buf) {
  if (FP_TABLE == null) initFPTable();
  long fp = EMPTY;
  for (int i = 0; i < buf.length; i++)
    fp = (fp >>> 8) ^ FP_TABLE[(int)(fp ^ buf[i]) & 0xff];
  return fp;
}

static long EMPTY = 0xc15d213aa4d7a795L;
static long[] FP_TABLE = null;

void initFPTable() {
  FP_TABLE = new long[256];
  for (int i = 0; i < 256; i++) {
    long fp = i;
    for (int j = 0; j < 8; j++)
      fp = (fp >>> 1) ^ (EMPTY & -(fp & 1L));
    FP_TABLE[i] = fp;
  }
}
```

For the mathematics behind this algorithm, refer to [Chapter 14 of Hacker’s
Delight, Second
Edition](https://books.google.com/books?id=XD9iAwAAQBAJ&pg=PA319). This
implementation prepends a single one-bit to messages to address the issue of
CRCs ignoring leading zero bits.

## 6. Security Considerations

Care must be taken when processing Avro schemas and data to avoid schema
injection attacks, unauthorized data exposure, and issues arising from malformed
data structures.

## 7. IANA Considerations

### 7.1. Media Type Registration

This specification defines the `application/vnd.apache.avro.schema+json` media
type for Avro Schema document that shall be registered with IANA.

### 7.2. Schema reference parameter 

This specification defines the `schema` reference parameter for Avro Schema
documents that shall be registered with IANA for use in conjunction the
aforementioned media type.

The parameter is a generic media type parameter that can be used with any media
type that represents Avro data. The parameter is used to reference the Avro
Schema document that describes the data.

The parameter value is a URI-reference (RFC 3986) that identifies the Avro
Schema. 

If the URI reference is an absolute URI with the 'http' or 'https' scheme, the
URI reference MUST be resolvable to the Avro Schema document with an HTTP GET
request. The operation MAY require the client to supply an authorization
context.

If the URI reference is a relative reference, resolving the URI is application
specific. The URI reference MAY be a
[base64](https://www.rfc-editor.org/rfc/rfc4648)-encoded
[schema fingerprint](#5-schema-fingerprints).

Example:

```http
Content-Type: application/json;schema=https://example.com/schemas/Person
```

## 7. References

- [RFC 2119](https://tools.ietf.org/html/rfc2119): Key words for use in RFCs to
  Indicate Requirement Levels#
- [RFC 3986](https://tools.ietf.org/html/rfc3986): Uniform Resource Identifier
- [RFC 4648](https://tools.ietf.org/html/rfc4648): The Base16, Base32, and Base64 Data Encodings
- [RFC 5646](https://tools.ietf.org/html/rfc5646): Tags for Identifying Languages
- [RFC 7231](https://tools.ietf.org/html/rfc7231): Hypertext Transfer Protocol (HTTP/1.1): Semantics and Content
- [RFC 8259](https://tools.ietf.org/html/rfc8259): The JavaScript Object Notation (JSON) Data Interchange Format

