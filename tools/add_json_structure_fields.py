#!/usr/bin/env python3
# encoding: utf-8
"""
add_json_structure_fields.py

A utility script to add required JSON Structure fields to test files.
This script adds $schema and $id fields to JSON Structure schema files in the test/jstruct directory.

Usage:
    python tools/add_json_structure_fields.py [--dry-run]

The --dry-run parameter will show what changes would be made without actually modifying files.
"""

import json
import os
import sys
from pathlib import Path

# Constants
BASE_SCHEMA_URI = "https://json-structure.org/core/v0/#"
BASE_ID_URI = "https://example.com/schemas/"
DRY_RUN = "--dry-run" in sys.argv


def process_file(file_path):
    """Process a single JSON Structure file to add required fields."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        # Skip files that already have $schema
        if '"$schema"' in content or "'$schema'" in content:
            print(f"Skipping {file_path} (already has $schema)")
            return

    # Parse JSON (handle comments by stripping them first)
    lines = content.splitlines()
    clean_lines = [line for line in lines if not line.strip().startswith('//')]
    clean_content = '\n'.join(clean_lines)
    
    try:
        data = json.loads(clean_content)
    except json.JSONDecodeError as e:
        print(f"Error parsing {file_path}: {e}")
        return

    # Extract name for ID
    schema_name = None
    if isinstance(data, dict):
        # Try to get name from the schema
        schema_name = data.get('name', None)
        
        if not schema_name and 'type' in data and data['type'] == 'object':
            # Use file name as fallback
            schema_name = Path(file_path).stem.replace('.struct', '')
            
        # Add required fields
        data['$schema'] = BASE_SCHEMA_URI
        data['$id'] = f"{BASE_ID_URI}{schema_name.lower() if schema_name else Path(file_path).stem.replace('.struct', '')}"
        
        # Move schema fields to the top
        # Create new dict with ordered keys
        ordered_data = {
            '$schema': data['$schema'],
            '$id': data['$id']
        }
        # Add all other keys
        for key, value in data.items():
            if key not in ('$schema', '$id'):
                ordered_data[key] = value
                
        data = ordered_data
    else:
        print(f"Skipping {file_path} (not an object schema, possibly array/union type)")
        return

    # Format with pretty print and preserve comment at the top
    output = json.dumps(data, indent=2)
    
    # Add file path comment at the top if it existed
    if lines and lines[0].startswith('//'):
        output = lines[0] + '\n' + output

    # Print changes
    print(f"Processing {file_path}")
    if DRY_RUN:
        print("Would write:")
        print(output[:200] + "..." if len(output) > 200 else output)
    else:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(output)
        print(f"Updated {file_path}")


def main():
    """Main entry point."""
    # Get all .struct.json files in test/jstruct directory
    script_dir = Path(__file__).parent.resolve()
    repo_root = script_dir.parent
    jstruct_dir = repo_root / "test" / "jstruct"
    
    if not jstruct_dir.exists():
        print(f"Directory not found: {jstruct_dir}")
        return
    
    print(f"{'Analyzing' if DRY_RUN else 'Updating'} JSON Structure files in {jstruct_dir}")
    
    # Process all struct.json files
    struct_files = list(jstruct_dir.glob("*.struct.json"))
    for file_path in struct_files:
        process_file(file_path)
    
    print(f"Done! Processed {len(struct_files)} files.")


if __name__ == "__main__":
    main()
