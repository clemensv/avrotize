#!/usr/bin/env python3

import os
import tempfile
from avrotize.chained_converters import convert_structure_to_proto, convert_structure_to_iceberg, convert_structure_to_rust
from avrotize.jstructtoavro import convert_json_structure_to_avro

# Create test input
test_structure = {
    "$schema": "https://json-structure.org/meta/core/v0/#",
    "$id": "https://example.com/schemas/test",
    "name": "TestRecord",
    "type": "object",
    "properties": {
        "foo": {
            "type": "string"
        },
        "bar": {
            "type": "integer"
        }
    },
    "required": ["foo", "bar"]
}

with tempfile.NamedTemporaryFile(mode='w', suffix='.struct.json', delete=False) as f:
    import json
    json.dump(test_structure, f, indent=2)
    struct_file = f.name

print(f"Created test structure file: {struct_file}")

# Test step 1: struct to avro
avro_file = struct_file.replace('.struct.json', '.avsc')
try:
    print("Step 1: Converting structure to avro...")
    convert_json_structure_to_avro(struct_file, avro_file)
    
    if os.path.exists(avro_file):
        print(f"✓ Avro file created: {avro_file}")
        with open(avro_file, 'r') as f:
            print(f"Avro content: {f.read()}")
    else:
        print("✗ Avro file not created")
except Exception as e:
    print(f"✗ Error converting to avro: {e}")
    import traceback
    traceback.print_exc()

# Test s2p conversion
proto_file = struct_file.replace('.struct.json', '.proto')
try:
    print("\nStep 2: Testing s2p conversion...")
    convert_structure_to_proto(struct_file, proto_file)
    
    if os.path.exists(proto_file):
        size = os.path.getsize(proto_file)
        print(f"✓ Proto file created: {proto_file} (size: {size})")
        if size > 0:
            with open(proto_file, 'r') as f:
                print(f"Proto content: {f.read()}")
        else:
            print("✗ Proto file is empty")
    else:
        print("✗ Proto file not created")
except Exception as e:
    print(f"✗ Error in s2p conversion: {e}")
    import traceback
    traceback.print_exc()

# Test s2ib conversion
iceberg_file = struct_file.replace('.struct.json', '.iceberg.json')
try:
    print("\nStep 3: Testing s2ib conversion...")
    convert_structure_to_iceberg(struct_file, iceberg_file)
    
    if os.path.exists(iceberg_file):
        size = os.path.getsize(iceberg_file)
        print(f"✓ Iceberg file created: {iceberg_file} (size: {size})")
        if size > 0:
            with open(iceberg_file, 'rb') as f:
                content = f.read()
                print(f"Iceberg content (first 100 bytes): {content[:100]}")
                # Try to parse as JSON
                try:
                    with open(iceberg_file, 'r') as text_f:
                        import json
                        json_content = json.load(text_f)
                        print(f"✓ Valid JSON content: {json_content}")
                except Exception as json_e:
                    print(f"✗ Not valid JSON: {json_e}")
        else:
            print("✗ Iceberg file is empty")
    else:
        print("✗ Iceberg file not created")
except Exception as e:
    print(f"✗ Error in s2ib conversion: {e}")
    import traceback
    traceback.print_exc()

# Test s2rust conversion
rust_file = struct_file.replace('.struct.json', '.rs')
try:
    print("\nStep 4: Testing s2rust conversion...")
    convert_structure_to_rust(struct_file, rust_file)
    
    if os.path.exists(rust_file):
        size = os.path.getsize(rust_file)
        print(f"✓ Rust file created: {rust_file} (size: {size})")
        if size > 0:
            with open(rust_file, 'r') as f:
                print(f"Rust content: {f.read()}")
        else:
            print("✗ Rust file is empty")
    else:
        print("✗ Rust file not created")
except Exception as e:
    print(f"✗ Error in s2rust conversion: {e}")
    import traceback
    traceback.print_exc()

# Cleanup
for f in [struct_file, avro_file, proto_file, iceberg_file, rust_file]:
    if os.path.exists(f):
        os.unlink(f)
