# descriptions-test.struct
A comprehensive test schema to verify that descriptions are properly emitted in the generated Markdown documentation for all schema elements including objects, properties, nested structures, definitions, and choice types.
**Schema ID:** `https://example.com/schemas/descriptions-test`
## Objects

### DescriptionsTest

A comprehensive test schema to verify that descriptions are properly emitted in the generated Markdown documentation for all schema elements including objects, properties, nested structures, definitions, and choice types.
**Properties:**
- **simpleString** (required): `string`
  - Description: A simple string field with a description explaining its purpose and expected content.
- **numberWithDescription** (required): `int32`
  - Description: An integer field that demonstrates how numeric type descriptions are rendered in the output.
- **nestedObject**: [NestedDetails](#nesteddetails)
  - Description: A nested object type that contains additional fields, testing description rendering for nested structures.
- **arrayField**: array&lt;`string`&gt;
  - Description: An array field containing string elements, testing how collection descriptions are documented.
- **mapField**: map&lt;`string`, `int32`&gt;
  - Description: A map field with string keys and integer values, verifying map type description rendering.
- **choiceField**: [DataChoice](#datachoice)
  - Description: A choice type (union) that allows selection between different data representations.
- **referencedType**: [ContactInfo](#contactinfo)
  - Description: A reference to a defined type, testing how referenced type descriptions are handled.

### NestedDetails

A nested object type that contains additional fields, testing description rendering for nested structures.
**Properties:**
- **nestedField1** (required): `string`
  - Description: First field in the nested object, verifying nested property descriptions.
- **nestedField2**: `int64`
  - Description: Second field in the nested object, demonstrating multi-level description support.
## Choice Types (Unions)

### DataChoice

A choice type (union) that allows selection between different data representations.

**Choices:**
- **textData**: `string`
  - Description: Text representation of the data.
- **numericData**: `int32`
  - Description: Numeric representation of the data.
- **structuredData**: [StructuredInfo](#structuredinfo)
## Definitions

### ContactInfo

Contact information structure defined in the definitions section, testing description rendering for reusable type definitions.
**Properties:**
- **email** (required): `string`
  - Description: Email address of the contact, following standard email format.
- **phone**: `string`
  - Description: Phone number of the contact, which may include country code and formatting.

### StructuredInfo

A structured data type with multiple fields, demonstrating complex definition descriptions.
**Properties:**
- **id** (required): `uuid`
  - Description: Unique identifier for this structured information record.
- **value** (required): `double`
  - Description: Numeric value associated with this record, using double precision.
- **metadata**: `object`
  - Description: Additional metadata as a nested inline object.

### StatusType

A choice type defined in definitions, representing various status states with descriptions.

**Choices:**
- **active**: `string`
  - Description: Active status indicating the entity is currently operational.
- **inactive**: `string`
  - Description: Inactive status indicating the entity is not currently operational.
- **pending**: `string`
  - Description: Pending status indicating the entity is awaiting activation or processing.