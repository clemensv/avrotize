# Minimal Test Cases for Discriminated Union Patterns

## Overview
Created 4 minimal test cases to isolate key patterns from complex schemas (jfrog-pipelines and cloudify).

## Test Cases Created

### 1. Simple Discriminated Union (`discriminated-union-simple.json`)
**Pattern**: Basic allOf + if/then discriminated union
**Example**: Shape union with Circle, Rectangle, Triangle variants
**Tests**:
- Discriminator field with enum type
- const and discriminator annotations in Avro schema
- Java base class generation with Jackson annotations
- Meaningful type naming (Circle vs Shape_1)

### 2. Nested Discriminated Unions (`discriminated-union-nested.json`)
**Pattern**: Discriminated union containing another discriminated union
**Example**: Vehicle (Car/Boat) containing Engine (Electric/Gasoline/Diesel)
**Tests**:
- Multiple levels of discriminated unions
- Nested base class generation
- Correct namespace handling across levels

### 3. oneOf with Title (`oneof-with-title.json`)
**Pattern**: oneOf with title property for naming
**Example**: Contact union with EmailContact, PhoneContact, PostalContact
**Tests**:
- Title-based type naming instead of numbered (contact_1, contact_2)
- Union field generation in Avro
- Clean naming in generated code

### 4. Large Schema (`large-schema-test.json`)
**Pattern**: Schema that exceeds Java's 65KB string constant limit
**Example**: Command union with 20 types, each with multiple fields
**Tests**:
- Large schema string splitting in Java code generation
- Multiple private methods with StringBuilder
- Compilation of very large schemas

## Files Created

### JSON Schemas
- `test/jsons/discriminated-union-simple.json` - 3 shape types
- `test/jsons/discriminated-union-nested.json` - 2 vehicles × 3 engines
- `test/jsons/oneof-with-title.json` - 3 contact types
- `test/jsons/large-schema-test.json` - 20 command types

### Sample Data
- `test/jsons/discriminated-union-simple-data.json`
- `test/jsons/discriminated-union-nested-data.json`
- `test/jsons/oneof-with-title-data.json`

### Reference Avro Schemas
- `test/jsons/discriminated-union-simple-ref.avsc`
- `test/jsons/discriminated-union-nested-ref.avsc`
- `test/jsons/oneof-with-title-ref.avsc`

### Tests Added

#### test_jsontoavro.py (All Passing ✅)
```python
def test_convert_discriminated_union_simple_to_avro(self)
def test_convert_discriminated_union_nested_to_avro(self)
def test_convert_oneof_with_title_to_avro(self)
```

#### test_avrotojava.py (Compilation Issue ❌)
```python
def test_convert_discriminated_union_simple_jsons_to_avro_to_java(self)
def test_convert_discriminated_union_nested_jsons_to_avro_to_java(self)
def test_convert_oneof_with_title_jsons_to_avro_to_java(self)
```

## Current Status

### JSON-to-Avro: ✅ All 3 tests pass  
- Schema conversion working correctly
- **Discriminator enum types are now shared** (single enum definition referenced by all variants)
- Discriminator annotations preserved
- Namespace handling correct
- Type naming using discriminator values

Example from discriminated-union-simple-ref.avsc:
- First variant (Circle) defines the enum inline at `com.test.example.Shape_types.type`
- Subsequent variants (Triangle, Rectangle) reference the shared enum: `"type": ["null", "com.test.example.Shape_types.type"]`

### Avro-to-Java: ❌ Compilation errors
**Issue**: getType() method returns enum type instead of String

Error example:
```
Circle is not abstract and does not override abstract method getType() in Shape
return type discriminated.union.simple...circle_types.type is not compatible with java.lang.String
cannot find symbol: variable Circle
```

**Root Cause**: The discriminator field is being generated as an enum type, but the base class expects getType() to return String. The generated code tries to return the enum field instead of calling a method that converts it to String.

## Next Steps

1. Fix Java code generation to handle discriminator enum fields properly:
   - Generate getType() that returns String from enum
   - Handle const value correctly in getter
   - Ensure proper type conversion

2. Run all 3 Java tests to verify fix

3. Update large-schema-test if needed to actually exceed 65KB

## Key Patterns Extracted

### allOf + if/then Pattern (JFrog)
```json
{
  "allOf": [
    {
      "if": { "properties": { "type": { "enum": ["Circle"] } } },
      "then": { "$ref": "#/definitions/shapeTypes/Circle" }
    }
  ]
}
```

### oneOf + title Pattern  
```json
{
  "oneOf": [
    {
      "title": "EmailContact",
      "properties": { "email": { "type": "string" } }
    }
  ]
}
```

These minimal cases allow focused testing of discriminated union features without the complexity of 11K+ line schemas.
