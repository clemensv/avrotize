# complex-scenario.struct
Complex test scenario combining multiple JSON Structure features
**Schema ID:** `https://example.com/schemas/complex-scenario`
## Objects

### ComplexScenario

Complex test scenario combining multiple JSON Structure features
**Properties:**
- **metadata** (required): `object`
- **data** (required): [Choice](#choice)
- **signature**: `bytes`
  - Description: Digital signature

### 
**Properties:**
- **id** (required): `uuid`
  - Description: Unique identifier
- **version** (required): `string`
  - Description: Semantic version
  - Constraints: pattern: `^\d+\.\d+\.\d+$`
- **created** (required): `datetime`
  - Description: Creation timestamp
- **tags**: set&lt;`string`&gt;
  - Constraints: maxItems: 10

### 
**Properties:**
- **type** (required): `string`
  - Constraints: const: measurement
- **value** (required): `decimal`
  - Constraints: precision: 15, scale: 6
- **timestamp** (required): `timestamp`
- **device**: `object`

### 
**Properties:**
- **id** (required): `string`
- **calibration**: map&lt;`string`, `double`&gt;

### 
**Properties:**
- **type** (required): `string`
  - Constraints: const: event
- **name** (required): `string`
- **severity** (required): `string`
  - Constraints: enum: low, medium, high, critical
- **details**: array&lt;`object`&gt;

### 
**Properties:**
- **key** (required): `string`
- **value** (required): `['string', 'int64', 'double', 'boolean']`
## Choice Types (Unions)

### 

**Choices:**
- **measurement**: `object`
- **event**: `object`
## Enumerations

### severity

**Values:** low, medium, high, critical