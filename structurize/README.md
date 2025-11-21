# Structurize / Avrotize

**Structurize** is a powerful schema conversion toolkit that helps you transform between various schema formats including JSON Schema, JSON Structure, Avro Schema, Protocol Buffers, XSD, SQL, and many more.

This package is published under two names:

- **`structurize`** - The primary package name, emphasizing JSON Structure conversion capabilities
- **`avrotize`** - The original package name, emphasizing Avro Schema conversion capabilities

Both packages currently share the same features and codebase. However, in future releases, Avro-focused and JSON Structure-focused features may be split across the two tools to make the feature list more manageable and focused for users. Choose whichever variant better aligns with your primary use case.

## Quick Start

Install the package:

```bash
pip install structurize
```

or

```bash
pip install avrotize
```

Use the CLI:

```bash
# Using structurize
structurize --help

# Or using avrotize
avrotize --help
```

## Key Features

- Convert between JSON Schema, JSON Structure, and Avro Schema
- Transform schemas to and from Protocol Buffers, XSD, ASN.1
- Generate code in C#, Python, TypeScript, Java, Go, Rust, C++, JavaScript
- Export schemas to SQL databases (MySQL, PostgreSQL, SQL Server, Oracle, Cassandra, MongoDB, DynamoDB, and more)
- Convert to Parquet, Iceberg, Kusto, and other data formats
- Generate documentation in Markdown

## Documentation

For complete documentation, examples, and detailed usage instructions, please see the main repository:

**[📖 Full Documentation](https://github.com/clemensv/avrotize)**

The main README includes:

- Comprehensive command reference
- Conversion examples and use cases
- Code generation guides
- Database schema export instructions
- API documentation

## License

MIT License - see the [LICENSE](../LICENSE) file in the repository root.
