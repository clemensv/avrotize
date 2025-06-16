#!/usr/bin/env python3
"""
Quick test script to verify JSON Structure validator integration.
"""

import sys
import json
import tempfile
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Add validator path
validator_path = project_root / "tools" / "primer-and-samples" / "samples" / "py"
sys.path.append(str(validator_path))

def main():
    """Main function to run validator test."""
    try:
        from json_structure_schema_validator import JSONStructureSchemaCoreValidator
        print("✓ JSON Structure validator imported successfully")
        
        # Test with a simple valid JSON Structure
        test_schema = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "$id": "https://example.com/test",
            "type": "object",
            "name": "TestSchema",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "int32"}
            },
            "required": ["name"]
        }
        
        # Validate using the correct API
        try:
            validator = JSONStructureSchemaCoreValidator(allow_dollar=False, allow_import=True)
            errors = validator.validate(test_schema, json.dumps(test_schema))
            if not errors:
                print("✓ Test schema validation passed")
            else:
                print(f"✗ Test schema validation failed: {errors}")
                return False
        except Exception as e:
            print(f"✗ Validation error: {e}")
            return False
        
    except ImportError as e:
        print(f"✗ Failed to import JSON Structure validator: {e}")
        print("Make sure the submodule is initialized: git submodule update --init --recursive")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False
    
    print("JSON Structure validator integration test completed successfully!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

print("JSON Structure validator integration test completed successfully!")