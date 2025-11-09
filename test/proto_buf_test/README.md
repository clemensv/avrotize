# Buf-style Proto Import Test

This directory demonstrates the use of the `proto_root` parameter for resolving proto imports in a buf-style project structure.

## Directory Structure

```
proto_buf_test/
└── proto/
    └── foo/           # Module root (proto_root)
        └── bar/
            ├── bizz.proto   # Imports "bar/fuzz.proto"
            └── fuzz.proto
```

## The Problem

When using `buf` toolchain, proto files are organized in modules with a root location. In this example:
- The module root is `proto/foo/`
- The file `proto/foo/bar/bizz.proto` needs to import `proto/foo/bar/fuzz.proto`
- According to buf conventions, the import statement is: `import "bar/fuzz.proto"`
- This is relative to the module root (`proto/foo/`), not the file's directory

## The Solution

Use the `--proto-root` parameter when converting proto files:

```bash
avrotize p2a test/proto_buf_test/proto/foo/bar/bizz.proto \
  --out output.avsc \
  --proto-root test/proto_buf_test/proto/foo
```

Or in Python:

```python
from avrotize import convert_proto_to_avro

convert_proto_to_avro(
    proto_file_path="test/proto_buf_test/proto/foo/bar/bizz.proto",
    avro_schema_path="output.avsc",
    proto_root="test/proto_buf_test/proto/foo"
)
```

## Backward Compatibility

The `proto_root` parameter is optional. When not provided, avrotize will fall back to resolving imports relative to the proto file's directory, maintaining backward compatibility with existing projects.
