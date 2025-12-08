# APIs-guru OpenAPI Test Samples

This directory contains real-world OpenAPI specifications downloaded from the [APIs-guru openapi-directory](https://github.com/APIs-guru/openapi-directory), a community-maintained collection of OpenAPI definitions for public APIs.

## Source

- **Repository**: https://github.com/APIs-guru/openapi-directory
- **License**: CC0 1.0 Universal (Public Domain Dedication)
- **Download Date**: 2025-01-27

## Included APIs

| File | Spec Version | Schemas | Size | Title |
|------|--------------|---------|------|-------|
| `giphy.yaml` | OpenAPI 3.0.0 | 5 | 24.9 KB | Giphy API |
| `github.yaml` | OpenAPI 3.0.3 | 582 | 8.6 MB | GitHub v3 REST API |
| `healthcare.yaml` | OpenAPI 3.0.0 | 11 | 21.1 KB | Healthcare |
| `httpbin.yaml` | OpenAPI 3.0.0 | 0 | 27.2 KB | httpbin.org |
| `spotify.yaml` | OpenAPI 3.0.3 | 93 | 281.1 KB | Spotify Web API (with fixes from sonallux) |
| `stripe.yaml` | OpenAPI 3.0.0 | 775 | 3.6 MB | Stripe API |
| `trello.yaml` | OpenAPI 3.0.0 | 122 | 538.0 KB | Trello |
| `tvmaze.yaml` | OpenAPI 3.0.0 | 16 | 29.8 KB | TVmaze user API |
| `xkcd.yaml` | OpenAPI 3.0.0 | 1 | 1.7 KB | XKCD |

## Purpose

These specifications serve as real-world test cases for the `openapitostructure` converter, validating:

- Schema extraction from `components.schemas`
- Reference resolution (`$ref`)
- Type mappings (strings, numbers, arrays, objects)
- Nullable handling (`nullable: true`)
- Enum types
- Nested and recursive structures
- allOf/oneOf/anyOf compositions
- Discriminated unions

## Usage

To generate JSON Structure output from these files:

```bash
# Single file
avrotize oa2jstruct --input test/openapi/apis-guru/giphy.yaml --output giphy.struct.json

# All files
for file in test/openapi/apis-guru/*.yaml; do
    avrotize oa2jstruct --input "$file" --output "${file%.yaml}.struct.json"
done
```

## Notes

- `httpbin.yaml` contains no schema definitions (only path operations with inline schemas)
- `github.yaml` and `stripe.yaml` are large files with complex schemas suitable for stress-testing
- All specs use OpenAPI 3.0.x; for Swagger 2.0 and OpenAPI 3.1 samples, see the parent `test/openapi/` directory
