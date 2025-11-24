# collections.struct
Test schema for collection types
**Schema ID:** `https://example.com/schemas/collections`
## Objects

### Collections

Test schema for collection types
**Properties:**
- **stringArray** (required): array&lt;`string`&gt;
  - Constraints: minItems: 1, maxItems: 10
- **numberSet**: set&lt;`int32`&gt;
  - Constraints: minItems: 0, maxItems: 20
- **stringToNumberMap**: map&lt;`string`, `double`&gt;
- **nestedArray**: array&lt;array&lt;`string`&gt;&gt;