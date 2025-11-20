# numeric-types.struct
Test schema for numeric types with precision and scale
**Schema ID:** `https://example.com/schemas/numeric-types`
## Objects

### NumericTypes

Test schema for numeric types with precision and scale
**Properties:**
- **int8Field** (required): `int8`
  - Constraints: minimum: -128, maximum: 127
- **int16Field**: `int16`
  - Constraints: minimum: -32768, maximum: 32767
- **uint32Field**: `uint32`
  - Constraints: minimum: 0, maximum: 4294967295
- **uint64Field**: `uint64`
  - Constraints: minimum: 0
- **decimalField** (required): `decimal`
  - Constraints: minimum: 0, maximum: 99999999.99, precision: 10, scale: 2
- **float32Field**: `float32`
  - Description: 32-bit floating point
- **float64Field**: `float64`
  - Description: 64-bit floating point