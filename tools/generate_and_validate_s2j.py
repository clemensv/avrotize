#!/usr/bin/env python3
"""
Comprehensive JSON Structure to JSON Schema Generator and Validator

This script generates and validates JSON Schema reference files from JSON Structure schemas:

1. Basic mode (--basic or -b):
   - Generates *-ref.json files from all JSON Structure files
   - Standard structure to schema conversion

2. All mode (--all or -a, default):
   - Runs basic mode generation

The script also validates all generated files using the JSON Schema Draft 2020-12 validator.

Usage:
    python generate_and_validate_s2j.py [--basic|--all] [--no-validate] [--cleanup-only]

Options:
    -b, --basic         Generate only basic reference files
    -a, --all          Generate all reference files (default)
    --no-validate      Skip validation after generation
    --cleanup-only     Only clean up existing files, don't generate new ones
"""

import sys
import os
import argparse
import json
import re
from pathlib import Path
from typing import List, Dict, Tuple, Any

# Add the current directory to the path to import avrotize modules
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

# Try to import JSON Schema validator
validator_imported = False
validator_class = None

try:
    import jsonschema
    from jsonschema import Draft202012Validator
    validator_imported = True
    validator_class = Draft202012Validator
except ImportError:
    print("Warning: jsonschema library not found. Validation will be skipped.")
    print("Install with: pip install jsonschema")

try:
    from avrotize.structuretojsons import convert_structure_to_json_schema_string
except ImportError as e:
    print(f"Error importing JSON Structure to Schema converter: {e}")
    sys.exit(1)

def cleanup_existing_files(test_dir: Path, patterns: List[str]) -> int:
    """Remove existing files matching the given patterns before regeneration."""
    deleted_count = 0
    
    print(f"=== Cleaning up existing files ===")
    
    for pattern in patterns:
        for file_path in test_dir.glob(pattern):
            try:
                file_path.unlink()
                print(f"  Deleted: {file_path.name}")
                deleted_count += 1
            except Exception as e:
                print(f"  Error deleting {file_path.name}: {e}")
    
    print(f"Deleted {deleted_count} existing files\\n")
    return deleted_count

def find_all_json_structure_files(test_dir: Path) -> List[Path]:
    """Find all JSON Structure files that need JSON Schema conversion."""
    struct_files = []
    
    # Find .struct.json files
    for file_path in test_dir.glob("*.struct.json"):
        # Skip already generated reference files
        if not file_path.name.endswith("-ref.json") and not file_path.name.endswith("-ref.struct.json"):
            struct_files.append(file_path)
    
    return sorted(struct_files)

def generate_schema_file(struct_file: Path, output_file: Path) -> bool:
    """Generate a single JSON Schema file from a JSON Structure file."""
    try:
        # Read the JSON Structure file
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        # Convert to JSON Schema
        result = convert_structure_to_json_schema_string(structure_content)
        
        # Validate the generated JSON to ensure it's well-formed
        try:
            json.loads(result)
        except json.JSONDecodeError as e:
            print(f"    [ERR] Generated invalid JSON: {str(e)}")
            return False
        
        # Write the result
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(result)
        
        print(f"    [OK]  Generated successfully")
        return True
        
    except Exception as e:
        print(f"    [ERR] Error: {str(e)}")
        return False

def generate_basic_references(struct_files: List[Path]) -> Tuple[int, List[str]]:
    """Generate basic *-ref.json files from JSON Structure sources."""
    generated_count = 0
    errors = []
    
    print(f"=== Generating {len(struct_files)} basic reference files ===")
    
    for struct_file in struct_files:
        base_name = struct_file.stem
        # Remove .struct suffix if present
        if base_name.endswith(".struct"):
            base_name = base_name[:-7]
        
        output_file = struct_file.parent / f"{base_name}-ref.json"
        
        print(f"  Converting {struct_file.name} -> {output_file.name}")
        
        if generate_schema_file(struct_file, output_file):
            generated_count += 1
        else:
            errors.append(f"{struct_file.name}: conversion failed")
    
    print(f"\\nBasic generation completed: {generated_count} successful, {len(errors)} errors")
    if errors:
        print("\\nGeneration errors:")
        for error in errors:
            print(f"  - {error}")
    print()
    
    return generated_count, errors

def validate_schema_file(file_path: Path) -> Tuple[bool, List[str]]:
    """Validate a single JSON Schema file."""
    if not validator_imported or not validator_class:
        return True, ["Validation skipped - jsonschema library not available"]
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            schema_content = f.read()
        
        # Parse the JSON Schema
        try:
            schema = json.loads(schema_content)
        except json.JSONDecodeError as e:
            return False, [f"Invalid JSON: {str(e)}"]
        
        # Validate against JSON Schema Draft 2020-12
        try:
            validator = validator_class(schema)
            # Check if the schema itself is valid
            validator.check_schema(schema)
            return True, []
        except Exception as e:
            return False, [f"Schema validation error: {str(e)}"]
        
    except Exception as e:
        return False, [f"File reading error: {str(e)}"]

def validate_all_generated_files(test_dir: Path, file_patterns: List[str]) -> Dict[str, Any]:
    """Validate all generated JSON Schema files matching the given patterns."""
    if not validator_imported:
        print("=== Skipping validation (jsonschema library not available) ===\\n")
        return {'valid': 0, 'invalid': 0, 'errors': []}
    
    print("=== Validating generated JSON Schema files ===")
    
    # Find all files matching the patterns
    all_files = []
    for pattern in file_patterns:
        all_files.extend(test_dir.glob(pattern))
    
    if not all_files:
        print("No JSON Schema files found for validation")
        return {'valid': 0, 'invalid': 0, 'errors': []}
    
    print(f"Found {len(all_files)} JSON Schema files to validate\\n")
    
    results = {'valid': 0, 'invalid': 0, 'errors': []}
    error_summary = {}
    
    for file_path in sorted(all_files):
        print(f"Validating {file_path.name}:")
        
        is_valid, errors = validate_schema_file(file_path)
        
        if is_valid:
            print(f"  [OK]  Valid JSON Schema")
            results['valid'] += 1
        else:
            print(f"  [ERR] Invalid JSON Schema:")
            for error in errors:
                print(f"        {error}")
                # Categorize common errors
                error_type = error.split(':')[0] if ':' in error else error
                error_summary[error_type] = error_summary.get(error_type, 0) + 1
            results['invalid'] += 1
            results['errors'].extend([f"{file_path.name}: {error}" for error in errors])
        
        print()
    
    # Summary
    total = results['valid'] + results['invalid']
    print(f"{'='*60}")
    print(f"VALIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total files: {total}")
    if total > 0:
        print(f"Valid: {results['valid']} ({results['valid']/total*100:.1f}%)")
        print(f"Invalid: {results['invalid']} ({results['invalid']/total*100:.1f}%)")
    else:
        print("Valid: 0")
        print("Invalid: 0")
    
    if error_summary:
        print(f"\\nCOMMON ERROR PATTERNS:")
        error_types = {}
        for error_type, count in sorted(error_summary.items(), key=lambda x: x[1], reverse=True):
            print(f"  {error_type}: {count} occurrences")
    
    return results

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate and validate JSON Schema reference files from JSON Structure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                  # Generate all reference files (default)
  %(prog)s --basic                # Generate only basic reference files
  %(prog)s --no-validate          # Skip validation after generation
  %(prog)s --cleanup-only         # Only clean up existing files
        """
    )
    
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '-b', '--basic', 
        action='store_true',
        help='Generate only basic *-ref.json files'
    )
    mode_group.add_argument(
        '-a', '--all', 
        action='store_true',
        help='Generate all reference files (default)'
    )
    
    parser.add_argument(
        '--no-validate', 
        action='store_true',
        help='Skip validation after generation'
    )
    parser.add_argument(
        '--cleanup-only', 
        action='store_true',
        help='Only clean up existing files, do not generate new ones'
    )
    
    args = parser.parse_args()
    
    # Set default mode to 'all' if no mode specified
    if not args.basic and not args.all:
        args.all = True
    
    return args


def main():
    """Main function to generate and validate JSON Schema files."""
    args = parse_arguments()
    
    print("Comprehensive JSON Structure to JSON Schema Generator and Validator")
    print("=" * 70)
    
    # Change to the avrotize2 directory to ensure correct paths
    os.chdir(current_dir)
    
    test_dir = Path("test/struct")
    if not test_dir.exists():
        print(f"Error: Test directory {test_dir} does not exist")
        sys.exit(1)
    
    print(f"Working directory: {test_dir.absolute()}")
    
    # Determine cleanup patterns based on mode
    cleanup_patterns = []
    validation_patterns = []
    
    if args.basic or args.all:
        cleanup_patterns.append("*-ref.json")
        validation_patterns.append("*-ref.json")
    
    # Step 1: Clean up existing files
    deleted_count = cleanup_existing_files(test_dir, cleanup_patterns)
    
    if args.cleanup_only:
        print("Cleanup completed. Exiting as requested.")
        sys.exit(0)
    
    # Step 2: Find JSON Structure files
    all_struct_files = find_all_json_structure_files(test_dir)
    
    if not all_struct_files:
        print("No JSON Structure files found to process")
        sys.exit(1)
    
    print(f"Found {len(all_struct_files)} JSON Structure files to process")
    
    # Step 3: Generate files based on mode
    basic_generated = 0
    basic_errors = []
    
    if args.basic or args.all:
        basic_generated, basic_errors = generate_basic_references(all_struct_files)
    
    # Step 4: Validate generated files
    validation_results = {'valid': 0, 'invalid': 0, 'errors': []}
    if not args.no_validate:
        validation_results = validate_all_generated_files(test_dir, validation_patterns)
    
    # Final summary
    print(f"\\n{'='*70}")
    print(f"FINAL SUMMARY")
    print(f"{'='*70}")
    print(f"Deleted existing files: {deleted_count}")
    print(f"JSON Structure files found: {len(all_struct_files)}")
    
    if args.basic or args.all:
        print(f"Basic reference files generated: {basic_generated}")
        if basic_errors:
            print(f"Basic generation errors: {len(basic_errors)}")
    
    if not args.no_validate:
        total_validated = validation_results['valid'] + validation_results['invalid']
        print(f"Files validated: {total_validated}")
        print(f"Valid schemas: {validation_results['valid']}")
        print(f"Invalid schemas: {validation_results['invalid']}")
    
    # Check for errors
    has_errors = (
        len(basic_errors) > 0 or
        validation_results['invalid'] > 0
    )
    
    if has_errors:
        print(f"\\n❌ Generation completed with errors")
        sys.exit(1)
    else:
        print(f"\\n✅ Generation completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()
