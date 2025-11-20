# extensions.struct
Test schema for JSON Structure extensions
**Schema ID:** `https://example.com/schemas/extensions`
## Objects

### ExtensionsExample

Test schema for JSON Structure extensions
**Properties:**
- **productName** (required): `string`
  - Description: Product name with alternate names
  - Constraints: maxLength: 100
- **price** (required): `decimal`
  - Constraints: minimum: 0, precision: 10, scale: 2
- **weight**: `double`
  - Constraints: minimum: 0.0
- **temperature**: `double`
- **stockSymbol**: `string`
  - Constraints: maxLength: 10, pattern: `^[A-Z]{1,5}$`