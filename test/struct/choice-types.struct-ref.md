# choice-types.struct
Test schema for choice types (unions)
**Schema ID:** `https://example.com/schemas/choice-types`
## Objects

### ChoiceTypes

Test schema for choice types (unions)
**Properties:**
- **taggedChoice** (required): [TaggedChoice](#taggedchoice)
- **inlineChoice**: [EntityChoice](#entitychoice)
- **nullableString**: `['string', 'null']`
## Choice Types (Unions)

### TaggedChoice

**Choices:**
- **textValue**: `string`
- **numberValue**: `int32`

### EntityChoice
**Extends:** #/definitions/BaseEntity
**Selector:** `entityType`

**Choices:**
- **Person**: [Person](#person)
- **Company**: [Company](#company)
## Definitions

### BaseEntity
**Abstract:** Yes
**Properties:**
- **name**: `string`
- **entityType**: `string`

### Person
**Extends:** #/definitions/BaseEntity
**Properties:**
- **age**: `int32`

### Company
**Extends:** #/definitions/BaseEntity
**Properties:**
- **employees**: `int32`