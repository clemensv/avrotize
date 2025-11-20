# complex-scenario.struct
Complex test scenario combining multiple JSON Structure features
**Schema ID:** `https://example.com/schemas/complex-scenario`
**Uses Extensions:** JSONStructureAlternateNames, JSONStructureUnits, JSONStructureValidation
## Objects

### ComplexScenario

Complex test scenario combining multiple JSON Structure features
**Uses Extensions:** JSONStructureAlternateNames, JSONStructureUnits, JSONStructureValidation
**Properties:**
- **metadata** (required): `object`
- **data** (required): [Choice](#choice)
- **signature**: `bytes`
  - Description: Digital signature
## Enumerations

### severity

**Values:** low, medium, high, critical