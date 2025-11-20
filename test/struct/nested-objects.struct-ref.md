# nested-objects.struct
Test schema for nested object structures
**Schema ID:** `https://example.com/schemas/nested-objects`
## Objects

### NestedObjects

Test schema for nested object structures
**Properties:**
- **person** (required): `object`

### 
**Properties:**
- **firstName** (required): `string`
  - Constraints: maxLength: 50
- **lastName** (required): `string`
  - Constraints: maxLength: 50
- **address** (required): `object`
- **phoneNumbers**: array&lt;`object`&gt;

### 
**Properties:**
- **street** (required): `string`
  - Constraints: maxLength: 100
- **city** (required): `string`
  - Constraints: maxLength: 50
- **country** (required): `string`
  - Constraints: maxLength: 50
- **coordinates**: `object`

### 
**Properties:**
- **latitude** (required): `double`
  - Constraints: minimum: -90.0, maximum: 90.0
- **longitude** (required): `double`
  - Constraints: minimum: -180.0, maximum: 180.0

### 
**Properties:**
- **type** (required): `string`
  - Constraints: enum: home, work, mobile
- **number** (required): `string`
  - Constraints: pattern: `^\+?[1-9]\d{1,14}$`
## Enumerations

### type

**Values:** home, work, mobile