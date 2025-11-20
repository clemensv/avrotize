# edge-cases.struct
Test schema for edge cases and special scenarios
**Schema ID:** `https://example.com/schemas/edge-cases`
## Objects

### EdgeCases

Test schema for edge cases and special scenarios
**Properties:**
- **emptyString**: `string`
  - Constraints: maxLength: 0
- **veryLongString**: `string`
  - Constraints: maxLength: 1000000
- **preciseDecimal**: `decimal`
  - Constraints: precision: 38, scale: 18
- **largeInteger**: `int64`
  - Constraints: minimum: -9223372036854775808, maximum: 9223372036854775807
- **emptyArray**: array&lt;`string`&gt;
  - Constraints: maxItems: 0
- **nullableEverything**: `['string', 'int32', 'double', 'boolean', 'array', 'object', 'null']`
- **deeplyNested**: `object`

### 
**Properties:**
- **level1** (required): `object`

### 
**Properties:**
- **level2** (required): `object`

### 
**Properties:**
- **level3** (required): `object`

### 
**Properties:**
- **level4** (required): `object`

### 
**Properties:**
- **value** (required): `string`