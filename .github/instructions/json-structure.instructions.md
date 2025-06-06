---
applyTo: '**'
---

When "JSON Structure" is discussed, the following documents are relevant:


# JSON Structure

Specification shortcuts:
[Core](https://json-structure.github.io/core/draft-vasters-json-structure-core.html)
|
[Import](https://json-structure.github.io/import/draft-vasters-json-structure-import.html)
| [Alternate
Names](https://json-structure.github.io/alternate-names/draft-vasters-json-structure-alternate-names.html)
| [Symbols, Units,
Currencies](https://json-structure.github.io/units/draft-vasters-json-structure-units.html)
|
[Validation](https://json-structure.github.io/validation/draft-vasters-json-structure-validation.html)
| [Conditional
Composition](https://json-structure.github.io/conditional-composition/draft-vasters-json-structure-conditional-composition.html)

JSON Structure is a schema language that can describe data types and structures
whose definitions map cleanly to programming language types and database
constructs as well as to the popular JSON data encoding. The type model reflects
the needs of modern applications and allows for rich annotations with semantic
information that can be evaluated and understood by developers and by large
language models (LLMs).

<style>
    .language-json {
        font-size: x-small;
    }
</style>

```json
{
    "$schema": "https://json-structure.org/meta/extended/v0/#",
    "$id": "https://example.com/schemas/product",
    "$uses": ["JSONStructureAlternateNames", "JSONStructureUnits"],
    "type": "object",
    "name": "Product",
    "properties": {
        "id": {
            "type": "uuid",
            "description": "Unique identifier for the product"
        },
        "name": {
            "type": "string",
            "maxLength": 100,
            "altnames": {
                "json": "product_name",
                "lang:en": "Product Name",
                "lang:de": "Produktname"
            }
        },
        "price": {
            "type": "decimal",
            "precision": 10,
            "scale": 2,
            "currency": "USD"
        },
        "weight": {
            "type": "double",
            "unit": "kg"
        },
        "created": {
            "type": "datetime"
        },
        "tags": {
            "type": "set",
            "items": { "type": "string" }
        },
        "attributes": {
            "type": "map",
            "values": { "type": "string" }
        }
    },
    "required": ["id", "name", "price", "created" ]
}
```

JSON Structure's syntax is similar to that of JSON Schema, but while JSON Schema
focuses on document validation, JSON Structure focuses on being a strong data
definition language that also supports validation.

- Clear mapping to programming language types
- Support for more precise numeric types and date/time representations
- Modular approach to extensions
- Simplified cross-references between schema documents
- Straightforward reuse patterns for types
- Support for multilingual descriptions and alternate names
- Support for symbols, scientific units, and currency codes

## Primer and Core Specification

The [Primer](json-structure-primer.html) is a high-level overview of the JSON
Structure language and its features. It is intended for developers and users who
want to understand the language's capabilities and how to use it effectively.

The [JSON Structure Core Specification](https://json-structure.github.io/core)
provides a detailed description of the JSON Structure language, including its
syntax, semantics, and data types. It is intended for implementers and
developers who want to create tools and libraries that work with JSON Structure.

## Extensions

- [JSON Structure: Import](https://json-structure.github.io/import): Defines a
  mechanism for importing external schemas and definitions into a schema
  document.
- [JSON Structure: Alternate Names and
  Descriptions](https://json-structure.github.io/alternate-names): Provides a
  mechanism for declaring multilingual descriptions, and alternate names and
  symbols for types and properties.
- [JSON Structure: Symbols, Scientific Units, and
  Currencies](https://json-structure.github.io/units): Defines annotation
  keywords for specifying symbols, scientific units, and currency codes
  complementing type information.
- [JSON Structure: Validation](https://json-structure.github.io/validation):
  Specifies extensions to the core schema language for declaring validation
  rules for JSON data that have no structural impact on the schema.
- [JSON Structure:
  Composition](https://json-structure.github.io/conditional-composition):
  Defines a set of conditional composition rules for evaluating schemas.
