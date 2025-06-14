# JSON Schema to JSON Structure Conversion in Avrotize

This article provides a comprehensive explanation of how Avrotize converts JSON Schema documents into JSON Structure format, detailing the conversion strategies, supported features, and architectural decisions.

- [Overview](#overview)
- [Conversion Strategy](#conversion-strategy)
  - [Type System Mapping](#type-system-mapping)
    - [Primitive Types](#primitive-types)
    - [Numeric Types with Constraints](#numeric-types-with-constraints)
    - [String Types with Formats](#string-types-with-formats)
    - [Arrays and Sets](#arrays-and-sets)
    - [Objects](#objects)
    - [Maps and Additional Properties](#maps-and-additional-properties)
    - [Discriminated Unions](#discriminated-unions)
    - [Type Unions](#type-unions)
    - [Conditional Schemas](#conditional-schemas)
  - [Reference Resolution](#reference-resolution)
    - [Definition Management](#definition-management)
    - [Circular References](#circular-references)
    - [External References](#external-references)
  - [Name Normalization](#name-normalization)
    - [Invalid Identifiers](#invalid-identifiers)
    - [Alternate Names](#alternate-names)
  - [Composition Handling](#composition-handling)
    - [AllOf Resolution](#allof-resolution)
    - [AnyOf Consolidation](#anyof-consolidation)
    - [OneOf to Choice](#oneof-to-choice)
  - [Special Patterns](#special-patterns)
    - [Empty Objects](#empty-objects)
    - [Pattern Properties](#pattern-properties)
    - [Mixed Properties and Additional Properties](#mixed-properties-and-additional-properties)
- [Architectural Decisions](#architectural-decisions)
  - [Composition Preservation](#composition-preservation)
  - [Type Registry](#type-registry)
  - [Extension System](#extension-system)
- [Limitations and Future Work](#limitations-and-future-work)

## Overview

JSON Structure is a schema language designed to map cleanly to programming language types and database constructs while supporting rich semantic annotations. The conversion from JSON Schema to JSON Structure in Avrotize follows several key principles:

1. **Type Precision**: Convert to the most specific JSON Structure type that accurately represents the JSON Schema constraints
2. **Semantic Preservation**: Maintain the intent of the original schema while adapting to JSON Structure's type system
3. **Clean Mapping**: Produce JSON Structure schemas that map naturally to programming language constructs
4. **Extension Utilization**: Leverage JSON Structure extensions for validation, alternate names, and units where appropriate

The converter supports JSON Schema Draft 7 as the primary target, with support for elements from newer drafts where they align with JSON Structure capabilities.

## Conversion Strategy

### Type System Mapping

#### Primitive Types

JSON Schema primitive types are mapped to their JSON Structure equivalents with enhanced precision:

| JSON Schema Type | JSON Structure Type | Notes |
|-----------------|-------------------|-------|
| `"type": "null"` | `"type": "null"` | Direct mapping |
| `"type": "boolean"` | `"type": "boolean"` | Direct mapping |
| `"type": "string"` | `"type": "string"` | Enhanced with format-specific types |
| `"type": "integer"` | `"type": "int32"` or `"type": "int64"` | Based on constraint analysis |
| `"type": "number"` | `"type": "float"` or `"type": "double"` or `"type": "decimal"` | Based on precision requirements |

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "string",
  "minLength": 1,
  "maxLength": 100
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "string",
  "minLength": 1,
  "maxLength": 100
}
```

</td></tr></table>

#### Numeric Types with Constraints

The converter analyzes numeric constraints to select the most appropriate JSON Structure numeric type:

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "integer",
  "minimum": 0,
  "maximum": 2147483647
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "int32",
  "minimum": "0",
  "maximum": "2147483647"
}
```

</td></tr><tr><td style="vertical-align:top">

```json
{
  "type": "integer",
  "minimum": 0,
  "maximum": 9223372036854775807
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "int64",
  "minimum": "0",
  "maximum": "9223372036854775807"
}
```

</td></tr><tr><td style="vertical-align:top">

```json
{
  "type": "number",
  "multipleOf": 0.01
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "decimal",
  "precision": 10,
  "scale": 2
}
```

</td></tr></table>

Note that for `int64` types, numeric constraints are converted to strings to maintain precision in JSON representation.

#### String Types with Formats

JSON Schema format annotations are converted to specific JSON Structure types where available:

| JSON Schema Format | JSON Structure Type | Additional Properties |
|-------------------|-------------------|---------------------|
| `"format": "date-time"` | `"type": "datetime"` | - |
| `"format": "date"` | `"type": "date"` | - |
| `"format": "time"` | `"type": "time"` | - |
| `"format": "duration"` | `"type": "duration"` | - |
| `"format": "uuid"` | `"type": "uuid"` | - |
| `"format": "uri"` | `"type": "uri"` | - |
| `"format": "email"` | `"type": "string"` | `format: "email"` preserved |
| `"format": "byte"` | `"type": "bytes"` | - |

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "string",
  "format": "date-time"
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "datetime"
}
```

</td></tr></table>

#### Arrays and Sets

JSON Schema arrays are converted to JSON Structure arrays or sets based on the `uniqueItems` constraint:

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

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

</td></tr><tr><td style="vertical-align:top">

```json
{
  "type": "array",
  "items": {
    "type": "string"
  },
  "uniqueItems": true
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "set",
  "items": {
    "type": "string"
  }
}
```

</td></tr></table>

#### Objects

JSON Schema objects are converted to JSON Structure objects with careful handling of properties and requirements:

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "id": {
      "type": "integer"
    },
    "name": {
      "type": "string"
    },
    "email": {
      "type": "string",
      "format": "email"
    }
  },
  "required": ["id", "name"]
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "object",
  "name": "Document",
  "properties": {
    "id": {
      "type": "int32"
    },
    "name": {
      "type": "string"
    },
    "email": {
      "type": "string",
      "format": "email"
    }
  },
  "required": ["id", "name"]
}
```

</td></tr></table>

#### Maps and Additional Properties

Objects with `additionalProperties` are converted based on their structure:

1. **Pure additional properties** (no defined properties) → `map` type
2. **Mixed properties and additional properties** → `object` type with `propertyNames` constraint
3. **Pattern properties** → `map` type with `keyNames` constraint

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

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
  "values": {
    "type": "string"
  }
}
```

</td></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string"
    }
  },
  "additionalProperties": {
    "type": "number"
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "name": {
      "type": "string"
    }
  },
  "propertyNames": {
    "type": "string"
  },
  "additionalProperties": {
    "type": "float"
  }
}
```

</td></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "patternProperties": {
    "^[A-Z]+$": {
      "type": "string"
    }
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "map",
  "keyNames": {
    "type": "string",
    "pattern": "^[A-Z]+$"
  },
  "values": {
    "type": "string"
  }
}
```

</td></tr></table>

#### Discriminated Unions

The converter detects discriminated union patterns in `oneOf` constructs and converts them to JSON Structure `choice` types:

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "oneOf": [
    {
      "properties": {
        "email": {
          "type": "string"
        }
      },
      "required": ["email"]
    },
    {
      "properties": {
        "phone": {
          "type": "string"
        }
      },
      "required": ["phone"]
    }
  ]
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "choice",
  "choices": {
    "email": {
      "type": "any",
      "description": "Choice variant with email property"
    },
    "phone": {
      "type": "any",
      "description": "Choice variant with phone property"
    }
  }
}
```

</td></tr></table>

The converter recognizes several discriminated union patterns:
- **Simple pattern**: Each option has exactly one unique required property
- **Complex pattern**: Each option has a unique combination of required properties
- **Tagged pattern**: Options differ by a constant value in a shared property

#### Type Unions

Non-discriminated unions are preserved as arrays of types:

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "oneOf": [
    {
      "type": "string"
    },
    {
      "type": "number"
    },
    {
      "type": "boolean"
    }
  ]
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": ["string", "float", "boolean"]
}
```

</td></tr></table>

#### Conditional Schemas

JSON Schema conditional constructs (`if`/`then`/`else`) are preserved when composition preservation is enabled:

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "type": {
      "type": "string"
    }
  },
  "if": {
    "properties": {
      "type": {
        "const": "premium"
      }
    }
  },
  "then": {
    "properties": {
      "discount": {
        "type": "number"
      }
    }
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "type": {
      "type": "string"
    }
  },
  "if": {
    "properties": {
      "type": {
        "const": "premium"
      }
    }
  },
  "then": {
    "properties": {
      "discount": {
        "type": "float"
      }
    }
  }
}
```

</td></tr></table>

### Reference Resolution

#### Definition Management

The converter maintains a type registry to track all defined types and their references:

1. **Definition Discovery**: All `definitions` and `$defs` sections are scanned
2. **Name Normalization**: Definition names are normalized to valid JSON Structure identifiers
3. **Reference Tracking**: All `$ref` uses are tracked and updated to normalized names

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "address": {
      "$ref": "#/definitions/address-info"
    }
  },
  "definitions": {
    "address-info": {
      "type": "object",
      "properties": {
        "street": {
          "type": "string"
        }
      }
    }
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "address": {
      "$ref": "#/definitions/address_info"
    }
  },
  "definitions": {
    "address_info": {
      "type": "object",
      "name": "address_info",
      "altnames": {
        "json": "address-info"
      },
      "properties": {
        "street": {
          "type": "string"
        }
      }
    }
  }
}
```

</td></tr></table>

#### Circular References

Circular references are preserved and handled through the type registry system:

1. **Direct circular references**: Type A references itself
2. **Indirect circular references**: Type A references B which references A
3. **Complex circular patterns**: Multiple types forming reference cycles

The converter ensures all types are properly ordered in the output to avoid forward references where possible.

#### External References

External `$ref` references are resolved through multiple mechanisms:

1. **File System Resolution**: Local file paths are resolved relative to the source document
2. **HTTP/HTTPS Resolution**: Remote schemas are fetched and cached
3. **Import Generation**: External references can optionally generate import statements

### Name Normalization

#### Invalid Identifiers

JSON Structure requires identifiers to match the pattern `^[a-zA-Z_][a-zA-Z0-9_]*$`. The converter normalizes invalid names:

1. **Names starting with digits**: Prefixed with underscore (`123abc` → `_123abc`)
2. **Special characters**: Replaced with underscores (`my-name` → `my_name`)
3. **Reserved keywords**: Suffixed with underscore (`type` → `type_`)

#### Alternate Names

When names are normalized, the original name is preserved using the JSON Structure alternate names extension:

```json
{
  "type": "object",
  "name": "_8bf53_full",
  "altnames": {
    "json": "8bf53_full"
  }
}
```

This ensures round-trip compatibility and preserves the original JSON Schema structure.

### Composition Handling

#### AllOf Resolution

`allOf` constructs are resolved by merging all subschemas:

1. **Property Merging**: Properties from all schemas are combined
2. **Constraint Union**: Constraints are combined (most restrictive wins)
3. **Required Field Union**: Required fields from all schemas are merged

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "allOf": [
    {
      "type": "object",
      "properties": {
        "id": {
          "type": "integer"
        }
      },
      "required": ["id"]
    },
    {
      "properties": {
        "name": {
          "type": "string"
        }
      },
      "required": ["name"]
    }
  ]
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "object",
  "properties": {
    "id": {
      "type": "int32"
    },
    "name": {
      "type": "string"
    }
  },
  "required": ["id", "name"]
}
```

</td></tr></table>

#### AnyOf Consolidation

`anyOf` constructs are analyzed for consolidation opportunities:

1. **Compatible Objects**: Objects with non-conflicting properties are merged
2. **Type Unions**: Different types become type arrays
3. **Incompatible Structures**: Preserved as separate options

#### OneOf to Choice

`oneOf` constructs are analyzed for discriminated union patterns:

```python
def detect_discriminated_union(schemas):
    # Check if all schemas are objects with properties
    if not all(is_object_with_properties(s) for s in schemas):
        return None
    
    # Collect required properties for each schema
    required_sets = [set(s.get('required', [])) for s in schemas]
    
    # Check for mutual exclusivity
    for i, req_i in enumerate(required_sets):
        for j, req_j in enumerate(required_sets):
            if i != j and req_i & req_j:  # Intersection
                return None
    
    # Valid discriminated union
    return create_choice_type(schemas, required_sets)
```

### Special Patterns

#### Empty Objects

Bare object types without properties or constraints are converted to `"type": "any"`:

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object"
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "any"
}
```

</td></tr></table>

This reflects the semantic meaning: an object with no constraints accepts any object structure.

#### Pattern Properties

Pattern properties are converted to maps with key constraints:

<table width="100%"><tr><th>JSON Schema</th><th>JSON Structure</th></tr><tr><td style="vertical-align:top">

```json
{
  "type": "object",
  "patternProperties": {
    "^[A-Z]{2,3}$": {
      "type": "number"
    },
    "^[a-z]+$": {
      "type": "string"
    }
  }
}
```

</td>
<td style="vertical-align:top">

```json
{
  "type": "map",
  "keyNames": {
    "anyOf": [
      {
        "type": "string",
        "pattern": "^[A-Z]{2,3}$"
      },
      {
        "type": "string",
        "pattern": "^[a-z]+$"
      }
    ]
  },
  "values": {
    "type": ["float", "string"]
  }
}
```

</td></tr></table>

#### Mixed Properties and Additional Properties

Objects with both defined properties and additional properties require special handling:

```json
// JSON Schema
{
  "type": "object",
  "properties": {
    "name": {"type": "string"},
    "age": {"type": "integer"}
  },
  "additionalProperties": {"type": "boolean"}
}

// JSON Structure
{
  "type": "object",
  "properties": {
    "name": {"type": "string"},
    "age": {"type": "int32"}
  },
  "propertyNames": {"type": "string"},
  "additionalProperties": {"type": "boolean"}
}
```

## Architectural Decisions

### Composition Preservation

The converter supports two modes:

1. **Composition Resolution** (default): Resolves all composition keywords to produce flat schemas
2. **Composition Preservation**: Maintains composition structure for downstream processors

```python
converter = JsonToStructureConverter(preserve_composition=True)
```

### Type Registry

A central type registry maintains:
- Original to normalized name mappings
- Type definitions and their locations
- Reference relationships
- Circular reference detection

### Extension System

The converter automatically adds JSON Structure extensions based on usage:

```json
{
  "$uses": [
    "JSONStructureAlternateNames",  // Added when names are normalized
    "JSONStructureValidation",       // Added when validation constraints are present
    "JSONStructureUnits"            // Added when units/currencies are detected
  ]
}
```

## Limitations and Future Work

### Current Limitations

1. **Conditional Composition**: Limited support for complex `if`/`then`/`else` patterns
2. **Dynamic References**: `$dynamicRef` and `$dynamicAnchor` are not supported
3. **Meta-schemas**: Custom meta-schema validation is not performed
4. **Hypermedia**: Links and hypermedia controls are not converted

### Future Enhancements

1. **Semantic Annotations**: Detect and convert semantic patterns to JSON Structure annotations
2. **Validation Profiles**: Generate validation extension profiles for complex constraints
3. **Schema Optimization**: Post-process to optimize type definitions and reduce redundancy
4. **Round-trip Fidelity**: Enhance alternate name preservation for perfect round-trip conversion

### Best Practices

When converting JSON Schema to JSON Structure:

1. **Use Specific Types**: Prefer specific numeric types over generic `number`
2. **Leverage Extensions**: Use JSON Structure extensions for validation and metadata
3. **Preserve Semantics**: Maintain the original intent through appropriate type choices
4. **Document Conversions**: Add descriptions explaining non-obvious conversions
5. **Test Round-trips**: Verify data validation consistency between formats

The JSON Schema to JSON Structure converter in Avrotize provides a robust, semantic-preserving transformation that leverages the strengths of both schema languages while producing clean, type-safe schemas suitable for modern applications and code generation.