#!/usr/bin/env python3
"""
Comprehensive JSON Structure Generator and Validator

This script generates and validates JSON Structure reference files in multiple modes:

1. Basic mode (--basic or -b):
   - Generates *-ref.struct.json files from all JSON Schema files
   - Uses preserve_composition=False

2. Composition mode (--composition or -c):
   - Generates *-ref-composed.struct.json (preserve_composition=True)
   - Generates *-ref-flattened.struct.json (preserve_composition=False)
   - Only processes schemas containing composition keywords (anyOf, allOf, oneOf, if-then-else, not)

3. All mode (--all or -a, default):
   - Runs both basic and composition modes

The script also validates all generated files using the official JSON Structure validator.

Usage:
    python generate_and_validate_j2s.py [--basic|--composition|--all] [--no-validate] [--cleanup-only]

Options:
    -b, --basic         Generate only basic reference files
    -c, --composition   Generate only composition variants
    -a, --all          Generate all reference files (default)
    --no-validate      Skip validation after generation
    --cleanup-only     Only clean up existing files, don't generate new ones
"""

import sys
import os
import json
import re
import argparse
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
validator_function = None

for validator_path in validator_paths:
    if validator_path.exists():
        sys.path.insert(0, str(validator_path))
        try:
            from json_structure_schema_validator import JSONStructureSchemaCoreValidator, validate_json_structure_schema_core
            validator_imported = True
            validator_function = validate_json_structure_schema_core
            break
        except ImportError:
            try:
                from json_structure_schema_validator import JSONStructureSchemaCoreValidator
                validator_imported = True
                validator_function = None  # Use class-based validation
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
    
    print(f"Deleted {deleted_count} existing files\n")
    return deleted_count

def find_all_json_schema_files(test_dir: Path) -> List[Path]:
    """Find all JSON Schema files that need JSON Structure conversion."""
    json_files = []
    
    # Find .json and .jsons files, excluding already generated structure files
    for pattern in ["*.json", "*.jsons"]:
        for file_path in test_dir.glob(pattern):
            # Skip already generated structure files
            if (file_path.name.endswith("-ref.struct.json") or 
                file_path.name.endswith("-ref-composed.struct.json") or
                file_path.name.endswith("-ref-flattened.struct.json")):
                continue
            json_files.append(file_path)
    
    return sorted(json_files)

def find_composition_schemas(test_dir: Path) -> List[Path]:
    """Find all JSON Schema files that contain composition keywords."""
    composition_files = []
    
    all_files = find_all_json_schema_files(test_dir)
    
    for file_path in all_files:
        try:
            content = file_path.read_text(encoding='utf-8')
            if has_composition_keywords(content):
                composition_files.append(file_path)
        except Exception as e:
            print(f"Warning: Could not read {file_path.name}: {e}")
    
    return sorted(composition_files)

def generate_structure_file(json_file: Path, output_file: Path, preserve_composition: bool) -> bool:
    """Generate a single JSON Structure file from a JSON Schema file."""
    try:
        converter = JsonToStructureConverter()
        converter.root_namespace = 'example.com'
        converter.root_class_name = 'document'
        converter.preserve_composition = preserve_composition
        
        # Try to use convert_schema method if available
        if hasattr(converter, 'convert_schema'):
            result = converter.convert_schema(
                json_schema_path=str(json_file),
                output_path=str(output_file)
            )
            return result is not None
        else:
            # Fallback to manual conversion
            with open(json_file, 'r', encoding='utf-8') as f:
                json_schema = json.load(f)
            
            if hasattr(converter, 'convert_json_schema_to_structure'):
                structure_schema = converter.convert_json_schema_to_structure(json_schema, str(json_file))
            else:
                structure_schema = converter.jsons_to_structure(json_schema, converter.root_namespace, str(json_file))
            
            # Apply sorting if available
            if hasattr(converter, '_sort_json_structure_properties'):
                structure_schema = converter._sort_json_structure_properties(structure_schema)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(structure_schema, f, indent=2)
            
            return True
        
    except Exception as e:
        print(f"    [ERR] Error: {str(e)}")
        return False

def generate_basic_references(json_schema_files: List[Path]) -> Tuple[int, List[str]]:
    """Generate basic *-ref.struct.json files from JSON Schema sources."""
    generated_count = 0
    errors = []
    
    print(f"=== Generating {len(json_schema_files)} basic reference files ===")
    
    for json_file in json_schema_files:
        base_name = json_file.stem
        # Remove any existing -ref suffix
        if base_name.endswith("-ref"):
            base_name = base_name[:-4]
        
        output_file = json_file.parent / f"{base_name}-ref.struct.json"
        
        print(f"  Converting {json_file.name} -> {output_file.name}")
        
        if generate_structure_file(json_file, output_file, preserve_composition=False):
            generated_count += 1
            print(f"    [OK] Generated successfully")
        else:
            error_msg = f"Error converting {json_file.name}"
            errors.append(error_msg)
    
    print(f"\nBasic generation completed: {generated_count} successful, {len(errors)} errors")
    if errors:
        print("\nGeneration errors:")
        for error in errors:
            print(f"  - {error}")
    print()
    
    return generated_count, errors

def generate_composition_variants(json_schema_files: List[Path]) -> Dict[str, Any]:
    """Generate both composed and flattened JSON Structure variants."""
    results = {
        'composed_success': 0,
        'composed_errors': [],
        'flattened_success': 0, 
        'flattened_errors': []
    }
    
    print(f"=== Generating composition variants for {len(json_schema_files)} schemas ===")
    
    for json_file in json_schema_files:
        base_name = json_file.stem
        
        # Generate file names for both variants
        composed_file = json_file.parent / f"{base_name}-ref-composed.struct.json"
        flattened_file = json_file.parent / f"{base_name}-ref-flattened.struct.json"
        
        print(f"Processing {json_file.name}:")
        
        # Generate composed version (preserve_composition=True)
        print(f"  -> {composed_file.name} (preserve_composition=True)")
        if generate_structure_file(json_file, composed_file, preserve_composition=True):
            results['composed_success'] += 1
            print(f"    [OK] Generated successfully")
        else:
            error_msg = f"Error generating composed version for {json_file.name}"
            results['composed_errors'].append(error_msg)
        
        # Generate flattened version (preserve_composition=False)
        print(f"  -> {flattened_file.name} (preserve_composition=False)")
        if generate_structure_file(json_file, flattened_file, preserve_composition=False):
            results['flattened_success'] += 1
            print(f"    [OK] Generated successfully")
        else:
            error_msg = f"Error generating flattened version for {json_file.name}"
            results['flattened_errors'].append(error_msg)
        
        print()  # Empty line between files
    
    return results

def validate_structure_file(file_path: Path) -> Tuple[bool, List[str]]:
    """Validate a single JSON Structure file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            schema_content = json.load(f)
        
        # Re-read the file for source text
        with open(file_path, 'r', encoding='utf-8') as f:
            source_text = f.read()
        
        # Use function-based validation if available
        if validator_function:
            errors = validator_function(
                schema_content, 
                source_text=source_text, 
                extended=True
            )
            return len(errors) == 0, errors
        
        # Fall back to class-based validation
        elif 'JSONStructureSchemaCoreValidator' in globals():
            validator = JSONStructureSchemaCoreValidator(
                allow_dollar=True, 
                allow_import=True, 
                extended=True
            )
            errors = validator.validate(schema_content)
            return len(errors) == 0, errors
        
        else:
            return False, ["No validator available"]
        
    except Exception as e:
        return False, [f"Validation error: {str(e)}"]

def validate_all_generated_files(test_dir: Path, file_patterns: List[str]) -> Dict[str, Any]:
    """Validate all generated JSON Structure files matching the given patterns."""
    print("=== Validating generated JSON Structure files ===")
    
    # Find all files matching the patterns
    all_files = []
    for pattern in file_patterns:
        all_files.extend(test_dir.glob(pattern))
    
    if not all_files:
        print("No JSON Structure files found for validation")
        return {'valid': 0, 'invalid': 0, 'errors': []}
    
    print(f"Found {len(all_files)} JSON Structure files to validate\n")
    
    results = {'valid': 0, 'invalid': 0, 'errors': []}
    error_summary = {}
    
    for file_path in sorted(all_files):
        print(f"Validating {file_path.name}:")
        
        is_valid, errors = validate_structure_file(file_path)
        
        if is_valid:
            results['valid'] += 1
            print(f"  [OK] VALID - Schema is valid")
        else:
            results['invalid'] += 1
            print(f"  [ERR] INVALID - {len(errors)} errors:")
            for i, error in enumerate(errors[:3]):  # Show first 3 errors
                print(f"    {i+1}. {error}")
            if len(errors) > 3:
                print(f"    ... and {len(errors) - 3} more errors")
            
            error_summary[file_path.name] = errors[:5]  # Store first 5 errors
            results['errors'].extend(errors[:3])  # Add to overall errors
    
    # Summary
    total = results['valid'] + results['invalid']
    print(f"\n{'='*60}")
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
        print(f"\nCOMMON ERROR PATTERNS:")
        error_types = {}
        for filename, errors in error_summary.items():
            for error in errors:
                error_key = error.split(' at ')[0] if ' at ' in error else error
                error_types.setdefault(error_key, []).append(filename)
        
        for error_type, files in sorted(error_types.items()):
            print(f"  '{error_type}' in {len(files)} files")
    
    return results

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate and validate JSON Structure reference files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                  # Generate all reference files (default)
  %(prog)s --basic                # Generate only basic reference files
  %(prog)s --composition          # Generate only composition variants
  %(prog)s --no-validate          # Skip validation after generation
  %(prog)s --cleanup-only         # Only clean up existing files
        """
    )
    
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '-b', '--basic', 
        action='store_true',
        help='Generate only basic *-ref.struct.json files'
    )
    mode_group.add_argument(
        '-c', '--composition', 
        action='store_true',
        help='Generate only composition variants (*-ref-composed.struct.json, *-ref-flattened.struct.json)'
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
    if not args.basic and not args.composition and not args.all:
        args.all = True
    
    return args


def main():
    """Main function to generate and validate JSON Structure files."""
    args = parse_arguments()
    
    print("Comprehensive JSON Structure Generator and Validator")
    print("=" * 60)
    
    # Change to the avrotize2 directory to ensure correct paths
    os.chdir(current_dir)
    
    test_dir = Path("test/jsons")
    if not test_dir.exists():
        print(f"Error: Test directory {test_dir.absolute()} does not exist")
        sys.exit(1)
    
    print(f"Working directory: {test_dir.absolute()}")
    
    # Determine cleanup patterns based on mode
    cleanup_patterns = []
    validation_patterns = []
    
    if args.basic or args.all:
        cleanup_patterns.append("*-ref.struct.json")
        validation_patterns.append("*-ref.struct.json")
    
    if args.composition or args.all:
        cleanup_patterns.extend(["*-ref-composed.struct.json", "*-ref-flattened.struct.json"])
        validation_patterns.extend(["*-ref-composed.struct.json", "*-ref-flattened.struct.json"])
    
    # Step 1: Clean up existing files
    deleted_count = cleanup_existing_files(test_dir, cleanup_patterns)
    
    if args.cleanup_only:
        print(f"[SUCCESS] Cleanup completed: {deleted_count} files deleted")
        return
    
    # Step 2: Find JSON Schema files
    all_json_files = find_all_json_schema_files(test_dir)
    
    if not all_json_files:
        print("No JSON Schema files found to convert")
        sys.exit(1)
    
    print(f"Found {len(all_json_files)} JSON Schema files to process")
    
    # Step 3: Generate files based on mode
    basic_generated = 0
    basic_errors = []
    composition_results = {'composed_success': 0, 'flattened_success': 0, 'composed_errors': [], 'flattened_errors': []}
    
    if args.basic or args.all:
        basic_generated, basic_errors = generate_basic_references(all_json_files)
    
    if args.composition or args.all:
        composition_schemas = find_composition_schemas(test_dir)
        print(f"Found {len(composition_schemas)} schemas with composition keywords:")
        for schema in composition_schemas:
            print(f"  - {schema.name}")
        print()
        
        if composition_schemas:
            composition_results = generate_composition_variants(composition_schemas)
        else:
            print("No schemas with composition keywords found for variant generation\n")
    
    # Step 4: Validate generated files
    validation_results = {'valid': 0, 'invalid': 0, 'errors': []}
    if not args.no_validate:
        validation_results = validate_all_generated_files(test_dir, validation_patterns)
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"Deleted existing files: {deleted_count}")
    print(f"JSON Schema files found: {len(all_json_files)}")
    
    if args.basic or args.all:
        print(f"Basic reference files generated: {basic_generated}")
        print(f"Basic generation errors: {len(basic_errors)}")
    
    if args.composition or args.all:
        print(f"Composed variants generated: {composition_results['composed_success']}")
        print(f"Flattened variants generated: {composition_results['flattened_success']}")
        print(f"Composition generation errors: {len(composition_results['composed_errors']) + len(composition_results['flattened_errors'])}")
    
    if not args.no_validate:
        total_files = validation_results['valid'] + validation_results['invalid']
        print(f"Total files validated: {total_files}")
        print(f"Valid files: {validation_results['valid']}")
        print(f"Invalid files: {validation_results['invalid']}")
        
        if total_files > 0:
            success_rate = (validation_results['valid'] / total_files) * 100
            print(f"Validation success rate: {success_rate:.1f}%")
    
    # Check for errors
    has_errors = (
        len(basic_errors) > 0 or
        len(composition_results['composed_errors']) > 0 or
        len(composition_results['flattened_errors']) > 0 or
        validation_results['invalid'] > 0
    )
    
    if has_errors:
        print(f"\n⚠️  Some files had issues:")
        
        if basic_errors:
            print(f"Basic generation errors:")
            for error in basic_errors:
                print(f"  - {error}")
        
        if composition_results['composed_errors']:
            print(f"Composed generation errors:")
            for error in composition_results['composed_errors']:
                print(f"  - {error}")
        
        if composition_results['flattened_errors']:
            print(f"Flattened generation errors:")
            for error in composition_results['flattened_errors']:
                print(f"  - {error}")
        
        if validation_results['invalid'] > 0:
            print(f"Validation failed for {validation_results['invalid']} files")
    else:
        print(f"\n[SUCCESS] All files generated and validated successfully!")


if __name__ == "__main__":
    main()
