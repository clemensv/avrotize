# extensions.struct
Test schema for JSON Structure extensions
**Schema ID:** `https://example.com/schemas/extensions`
**Uses Extensions:** JSONStructureAlternateNames, JSONStructureUnits, JSONStructureValidation
## Objects

### ExtensionsExample

Test schema for JSON Structure extensions
**Uses Extensions:** JSONStructureAlternateNames, JSONStructureUnits, JSONStructureValidation
**Properties:**
- **productName** (required): `string`
  - Description: Product name with alternate names
  - Extensions: altnames: {json: product_name, lang:en: Product Name, lang:de: Produktname, lang:fr: Nom du Produit}
  - Constraints: maxLength: 100
- **price** (required): `decimal`
  - Extensions: currency: USD
  - Constraints: minimum: 0, precision: 10, scale: 2
- **weight**: `double`
  - Extensions: unit: kg
  - Constraints: minimum: 0.0
- **temperature**: `double`
  - Extensions: unit: °C, symbol: T
- **stockSymbol**: `string`
  - Extensions: symbol: STOCK
  - Constraints: maxLength: 10, pattern: `^[A-Z]{1,5}$`