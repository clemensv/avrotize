# validation-constraints.struct
Test schema for validation constraints
**Schema ID:** `https://example.com/schemas/validation-constraints`
## Objects

### ValidationConstraints

Test schema for validation constraints
**Properties:**
- **constrainedString** (required): `string`
  - Constraints: minLength: 5, maxLength: 50, pattern: `^[A-Z][a-zA-Z0-9]*$`
- **enumField** (required): `string`
  - Constraints: enum: red, green, blue, yellow
- **constField**: `string`
  - Constraints: const: fixed-value
- **numberWithConstraints**: `int32`
  - Constraints: minimum: 0, maximum: 100
- **exclusiveRangeNumber**: `double`
## Enumerations

### enumField

**Values:** red, green, blue, yellow