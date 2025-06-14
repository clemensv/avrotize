#!/usr/bin/env python3
"""
JSON Structure Composition Variants Generator

This script generates two variants of JSON Structure reference files for schemas 
containing composition keywords (anyOf, allOf, oneOf, if-then-else, not):

1. *-ref-composed.struct.json (preserve_composition=True) - Preserves composition keywords
2. *-ref-flattened.struct.json (preserve_composition=False) - Flattens composition to concrete types

The script automatically:
- Identifies JSON Schema files with composition keywords
- Generates both variants using strict error handling
- Validates all generated files using the official JSON Structure validator
- Provides comprehensive reporting and error handling

Usage:
    python generate_composition_variants.py

Requirements:
    - avrotize package with JsonToStructureConverter
    - JSON Structure validator from json-structure repository
"""

import sys
import os
import json
import re
from pathlib import Path
from typing import List, Dict, Tuple, Any

# Add the current directory to the path to import avrotize modules
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

# Add the JSON Structure validator to the path
# Try multiple potential locations for the validator
validator_paths = [
    Path(__file__).parent / "primer-and-samples" / "samples" / "py",
    Path(r"C:\git\json-structure\primer-and-samples\samples\py"),
    Path.home() / "json-structure" / "primer-and-samples" / "samples" / "py"
]

validator_imported = False
for validator_path in validator_paths:
    if validator_path.exists():
        sys.path.insert(0, str(validator_path))
        try:
            from json_structure_schema_validator import validate_json_structure_schema_core
            validator_imported = True
            break
        except ImportError:
            continue

if not validator_imported:
    print("Error: JSON Structure validator not found. Please ensure json-structure repository is available.")
    print("Tried the following paths:")
    for path in validator_paths:
        print(f"  - {path}")
    sys.exit(1)

try:
    from avrotize.jsonstostructure import JsonToStructureConverter
except ImportError as e:
    print(f"Error importing JSON Schema to Structure converter: {e}")
    sys.exit(1)

def has_composition_keywords(schema_content: str) -> bool:
    """Check if schema content contains composition keywords."""
    pattern = r'\b(anyOf|allOf|oneOf|if|then|else|not)\b'
    return bool(re.search(pattern, schema_content))

def find_composition_schemas(test_dir: Path) -> List[Path]:
    """Find all JSON Schema files that contain composition keywords."""
    composition_files = []
    
    # Look for .json and .jsons files that don't end with -ref.struct.json
    for pattern in ["*.json", "*.jsons"]:
        for file_path in test_dir.glob(pattern):
            # Skip already generated structure files
            if file_path.name.endswith("-ref.struct.json") or \
               file_path.name.endswith("-ref-composed.struct.json") or \
               file_path.name.endswith("-ref-flattened.struct.json"):
                continue
                
            try:
                content = file_path.read_text(encoding='utf-8')
                if has_composition_keywords(content):
                    composition_files.append(file_path)
            except Exception as e:
                print(f"Warning: Could not read {file_path.name}: {e}")
    
    return sorted(composition_files)

def generate_structure_variants(json_schema_files: List[Path]) -> Dict[str, Any]:
    """Generate both composed and flattened JSON Structure variants."""
    results = {
        'composed_success': 0,
        'composed_errors': [],
        'flattened_success': 0, 
        'flattened_errors': []
    }
    
    print(f"=== Generating JSON Structure variants for {len(json_schema_files)} schemas ===\n")
    
    for json_file in json_schema_files:
        base_name = json_file.stem
        
        # Generate file names for both variants
        composed_file = json_file.parent / f"{base_name}-ref-composed.struct.json"
        flattened_file = json_file.parent / f"{base_name}-ref-flattened.struct.json"
        
        print(f"Processing {json_file.name}:")        # Generate composed version (preserve_composition=True)
        try:
            print(f"  -> {composed_file.name} (preserve_composition=True)")
            converter = JsonToStructureConverter()
            converter.strict_mode = True
            converter.root_namespace = 'example.com'
            converter.root_class_name = 'document'
            converter.preserve_composition = True
            
            with open(json_file, 'r', encoding='utf-8') as f:
                json_schema = json.load(f)
            
            structure_schema = converter.convert_json_schema_to_structure(json_schema, str(json_file))
            
            # Apply sorting to the generated schema
            sorted_schema = converter._sort_json_structure_properties(structure_schema)
            
            with open(composed_file, 'w', encoding='utf-8') as f:
                json.dump(sorted_schema, f, indent=2)            
            results['composed_success'] += 1
            print(f"    [OK] Generated successfully")            
        except Exception as e:
            error_msg = f"Error generating composed version for {json_file.name}: {str(e)}"
            results['composed_errors'].append(error_msg)
            print(f"    [ERR] {error_msg}")
        
        # Generate flattened version (preserve_composition=False)
        try:
            print(f"  -> {flattened_file.name} (preserve_composition=False)")
            converter = JsonToStructureConverter()
            converter.strict_mode = True
            converter.root_namespace = 'example.com'
            converter.root_class_name = 'document'
            converter.preserve_composition = False
            
            with open(json_file, 'r', encoding='utf-8') as f:
                json_schema = json.load(f)
            
            structure_schema = converter.convert_json_schema_to_structure(json_schema, str(json_file))
            
            # Apply sorting to the generated schema
            sorted_schema = converter._sort_json_structure_properties(structure_schema)
            
            with open(flattened_file, 'w', encoding='utf-8') as f:
                json.dump(sorted_schema, f, indent=2)            
            results['flattened_success'] += 1
            print(f"    [OK] Generated successfully")            
        except Exception as e:
            error_msg = f"Error generating flattened version for {json_file.name}: {str(e)}"
            results['flattened_errors'].append(error_msg)
            print(f"    [ERR] {error_msg}")
        
        print()  # Empty line between files
    
    return results

def validate_structure_file(file_path: Path) -> Tuple[bool, List[str]]:
    """Validate a single JSON Structure file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            schema_content = json.load(f)
            source_text = f.read()
        
        # Re-read the file for source text
        with open(file_path, 'r', encoding='utf-8') as f:
            source_text = f.read()
        
        # Use the official validator with extended=True to support composition
        errors = validate_json_structure_schema_core(
            schema_content, 
            source_text=source_text, 
            extended=True  # Enable extended features for composition support
        )
        
        return len(errors) == 0, errors
        
    except Exception as e:
        return False, [f"Validation error: {str(e)}"]

def validate_generated_files(test_dir: Path) -> Dict[str, Any]:
    """Validate all generated JSON Structure variant files."""
    print("=== Validating generated JSON Structure variants ===\n")
    
    # Find all variant files
    composed_files = list(test_dir.glob("*-ref-composed.struct.json"))
    flattened_files = list(test_dir.glob("*-ref-flattened.struct.json"))
    
    all_files = composed_files + flattened_files
    
    if not all_files:
        print("No JSON Structure variant files found for validation")
        return {'valid': 0, 'invalid': 0, 'errors': []}
    
    results = {'valid': 0, 'invalid': 0, 'errors': []}
    
    for file_path in sorted(all_files):
        print(f"Validating {file_path.name}:")
        
        is_valid, errors = validate_structure_file(file_path)        
        if is_valid:
            results['valid'] += 1
            print(f"  [OK] VALID - Schema is valid")
        else:
            results['invalid'] += 1
            error_summary = f"INVALID - {file_path.name}: {'; '.join(errors[:3])}"
            if len(errors) > 3:
                error_summary += f" (and {len(errors) - 3} more errors)"
            results['errors'].append(error_summary)
            print(f"  [ERR] {error_summary}")
    
    return results

def cleanup_existing_variants(test_dir: Path) -> int:
    """Remove existing variant files before regeneration."""
    patterns = ["*-ref-composed.struct.json", "*-ref-flattened.struct.json"]
    deleted_count = 0
    
    print("=== Cleaning up existing variant files ===")
    
    for pattern in patterns:
        for file_path in test_dir.glob(pattern):
            try:
                file_path.unlink()
                print(f"  Deleted: {file_path.name}")
                deleted_count += 1
            except Exception as e:
                print(f"  Error deleting {file_path.name}: {e}")
    
    print(f"Deleted {deleted_count} existing variant files\n")
    return deleted_count

def main():
    """Main execution function."""
    print("JSON Structure Composition Variants Generator")
    print("=" * 60)      # Set up paths
    test_dir = Path("../test/jsons")
    if not test_dir.exists():
        test_dir = Path("test/jsons")
    print(f"Looking for test directory: {test_dir.absolute()}")
    if not test_dir.exists():
        print(f"Error: Test directory {test_dir} does not exist")
        sys.exit(1)
    print(f"Test directory found: {test_dir.absolute()}")
    
    # Cleanup existing variants
    cleanup_existing_variants(test_dir)
    
    # Find schemas with composition keywords
    composition_schemas = find_composition_schemas(test_dir)
    
    if not composition_schemas:
        print("No schemas with composition keywords found")
        return
    
    print(f"Found {len(composition_schemas)} schemas with composition keywords:")
    for schema in composition_schemas:
        print(f"  - {schema.name}")
    print()
    
    # Generate variants
    generation_results = generate_structure_variants(composition_schemas)
    
    # Validate generated files
    validation_results = validate_generated_files(test_dir)
    
    # Print summary
    print("\n" + "=" * 60)
    print("GENERATION SUMMARY")
    print("=" * 60)
    print(f"Schemas processed: {len(composition_schemas)}")
    print(f"Composed variants generated: {generation_results['composed_success']}")
    print(f"Flattened variants generated: {generation_results['flattened_success']}")
    print(f"Composed generation errors: {len(generation_results['composed_errors'])}")
    print(f"Flattened generation errors: {len(generation_results['flattened_errors'])}")
    
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    total_files = validation_results['valid'] + validation_results['invalid']
    print(f"Total variant files: {total_files}")
    print(f"Valid: {validation_results['valid']} ({validation_results['valid']/total_files*100:.1f}%)" if total_files > 0 else "Valid: 0")
    print(f"Invalid: {validation_results['invalid']} ({validation_results['invalid']/total_files*100:.1f}%)" if total_files > 0 else "Invalid: 0")
    
    if generation_results['composed_errors']:
        print("\nComposed generation errors:")
        for error in generation_results['composed_errors']:
            print(f"  - {error}")
    
    if generation_results['flattened_errors']:
        print("\nFlattened generation errors:")
        for error in generation_results['flattened_errors']:
            print(f"  - {error}")
    
    if validation_results['errors']:
        print("\nValidation errors:")
        for error in validation_results['errors']:
            print(f"  - {error}")
    
    # Final status
    success = (
        len(generation_results['composed_errors']) == 0 and
        len(generation_results['flattened_errors']) == 0 and
        validation_results['invalid'] == 0
    )
    
    if success:
        print("\n[SUCCESS] All variants generated and validated successfully!")
    else:
        print("\n[ERROR] Some variants had errors - check the summary above")
        sys.exit(1)

if __name__ == "__main__":
    main()
