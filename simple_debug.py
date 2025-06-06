#!/usr/bin/env python3

import tempfile
import json
import os

# Create test input
test_structure = {
    "$schema": "https://json-structure.org/meta/core/v0/#",
    "$id": "https://example.com/schemas/test",
    "name": "TestRecord",
    "type": "object",
    "properties": {
        "foo": {"type": "string"},
        "bar": {"type": "integer"}
    },
    "required": ["foo", "bar"]
}

# Create test file
with tempfile.NamedTemporaryFile(mode='w', suffix='.struct.json', delete=False) as f:
    json.dump(test_structure, f, indent=2)
    struct_file = f.name

print(f"Created test structure file: {struct_file}")

# Test the conversion
try:
    from avrotize.chained_converters import convert_structure_to_proto
    proto_file = struct_file.replace('.struct.json', '.proto')
    
    print(f"Converting {struct_file} to {proto_file}")
    convert_structure_to_proto(struct_file, proto_file)
    
    if os.path.exists(proto_file):
        size = os.path.getsize(proto_file)
        print(f"Proto file created with size: {size}")
        if size > 0:
            with open(proto_file, 'r') as f:
                content = f.read()
                print(f"Proto content:\n{content}")
        else:
            print("Proto file is empty!")
    else:
        print("Proto file was not created!")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# Cleanup
if os.path.exists(struct_file):
    os.unlink(struct_file)
if 'proto_file' in locals() and os.path.exists(proto_file):
    os.unlink(proto_file)
