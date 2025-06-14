#!/usr/bin/env python3
"""
Generate and validate JSON Structure reference files.

This script:
1. Deletes existing *-ref.struct.json files in test/jsons
2. Finds corresponding JSON Schema files 
3. Regenerates JSON Structure files using the JSON Schema to JSON Structure converter
4. Validates the newly generated files using the official JSON Structure validator
"""

import sys
import os
import json
import glob
from pathlib import Path

# Add the current directory to the path to import avrotize modules
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

# Add the JSON Structure validator to the path
validator_path = Path(r"C:\git\json-structure\primer-and-samples\samples\py")
sys.path.insert(0, str(validator_path))

try:
    from json_structure_schema_validator import JSONStructureSchemaCoreValidator
except ImportError as e:
    print(f"Error importing JSON Structure validator: {e}")
    sys.exit(1)

try:
    from avrotize.jsonstostructure import JsonToStructureConverter
except ImportError as e:
    print(f"Error importing JSON Schema to Structure converter: {e}")
    sys.exit(1)

def delete_existing_struct_files(test_dir):
    """Delete all existing *-ref.struct.json files in the test directory."""
    struct_files = list(test_dir.glob("*-ref.struct.json"))
    deleted_count = 0
    
    print(f"=== Deleting {len(struct_files)} existing JSON Structure files ===")
    for file_path in struct_files:
        try:
            file_path.unlink()
            print(f"  Deleted: {file_path.name}")
            deleted_count += 1
        except Exception as e:
            print(f"  Error deleting {file_path.name}: {e}")
    
    print(f"Deleted {deleted_count} existing JSON Structure files\n")
    return deleted_count

def find_json_schema_files(test_dir):
    """Find all JSON Schema files that need JSON Structure conversion."""
    # Look for .json and .jsons files that don't end with -ref.struct.json
    # Also exclude composition variant files (*-ref-composed.struct.json and *-ref-flattened.struct.json)
    json_files = []
    
    # Find .json files
    for json_file in test_dir.glob("*.json"):
        # Skip already generated structure files
        if (json_file.name.endswith("-ref.struct.json") or 
            json_file.name.endswith("-ref-composed.struct.json") or
            json_file.name.endswith("-ref-flattened.struct.json")):
            continue
        json_files.append(json_file)
    
    # Find .jsons files
    for jsons_file in test_dir.glob("*.jsons"):
        json_files.append(jsons_file)
    
    print(f"=== Found {len(json_files)} JSON Schema files to convert ===")
    for json_file in sorted(json_files):
        print(f"  {json_file.name}")
    print()
    
    return sorted(json_files)

def generate_struct_file_name(json_file_path):
    """Generate the corresponding JSON Structure file name."""
    base_name = json_file_path.stem
    # Remove any existing -ref suffix
    if base_name.endswith("-ref"):
        base_name = base_name[:-4]
    return json_file_path.parent / f"{base_name}-ref.struct.json"

def regenerate_json_structure_files(json_schema_files):
    """Regenerate JSON Structure files from JSON Schema sources."""
    generated_count = 0
    error_count = 0
    errors = []
    
    print(f"=== Regenerating JSON Structure files ===")
    
    for json_file in json_schema_files:
        struct_file = generate_struct_file_name(json_file)
        
        try:
            print(f"  Converting {json_file.name} -> {struct_file.name}")            # Use the converter class to generate the JSON Structure file
            converter = JsonToStructureConverter()
            converter.strict_mode = True
            converter.root_namespace = 'example.com'
            converter.root_class_name = 'document'
            converter.preserve_composition = True  # Use preserve_composition=True for reference files
            
            result = converter.convert_schema(
                json_schema_path=str(json_file),
                output_path=str(struct_file)
            )
            
            if result:
                generated_count += 1
                print(f"    [OK] Generated successfully")
            else:
                error_count += 1
                error_msg = f"Converter returned None for {json_file.name}"
                errors.append(error_msg)
                print(f"    [ERR] {error_msg}")
                
        except Exception as e:
            error_count += 1
            error_msg = f"Error converting {json_file.name}: {str(e)}"
            errors.append(error_msg)
            print(f"    [ERR] {error_msg}")
    
    print(f"\nGeneration completed: {generated_count} successful, {error_count} errors")
    if errors:
        print("\nGeneration errors:")
        for error in errors:
            print(f"  - {error}")
    print()
    
    return generated_count, error_count

def validate_json_structure_files():
    """Validate all generated JSON Structure files using the proper validator."""
    test_dir = Path("test/jsons")
    print(f"=== Validating JSON Structure files ===")
    print(f"Looking in directory: {test_dir.absolute()}")
    
    if not test_dir.exists():
        print(f"Test directory {test_dir} does not exist")
        return 0, 0
    
    # Find all JSON Structure files
    struct_files = list(test_dir.glob("*-ref.struct.json"))
    if not struct_files:
        print("No JSON Structure files found for validation")
        return 0, 0
    
    print(f"Found {len(struct_files)} JSON Structure files to validate\n")
    
    valid_count = 0
    invalid_count = 0
    error_summary = {}
    
    for file_path in sorted(struct_files):
        print(f"=== Validating {file_path.name} ===")
        
        try:
            # Load the schema
            with open(file_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)
            
            # Create validator
            validator = JSONStructureSchemaCoreValidator(allow_dollar=True, allow_import=True, extended=True)
           
            
            # Validate the instance
            errors = validator.validate(schema)
            
            if not errors:
                print(f"  [OK] VALID - Schema is valid")
                valid_count += 1
            else:
                print(f"  [ERR] INVALID - {len(errors)} errors:")
                for i, error in enumerate(errors[:3]):  # Show first 3 errors
                    print(f"    {i+1}. {error}")
                if len(errors) > 3:
                    print(f"    ... and {len(errors) - 3} more errors")
                
                error_summary[file_path.name] = errors[:5]  # Store first 5 errors
                invalid_count += 1
                
        except Exception as e:
            print(f"  [ERR] ERROR - Exception during validation: {str(e)[:100]}...")
            error_summary[file_path.name] = [f"Exception: {str(e)}"]
            invalid_count += 1
    
    # Summary
    total = valid_count + invalid_count
    print(f"\n{'='*60}")
    print(f"VALIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total files: {total}")
    print(f"Valid: {valid_count} ({valid_count/total*100:.1f}%)")
    print(f"Invalid: {invalid_count} ({invalid_count/total*100:.1f}%)")
    
    if error_summary:
        print(f"\nCOMMON ERROR PATTERNS:")
        error_types = {}
        for filename, errors in error_summary.items():
            for error in errors:
                error_key = error.split(' at ')[0] if ' at ' in error else error
                error_types.setdefault(error_key, []).append(filename)
        
        for error_type, files in sorted(error_types.items()):
            print(f"  '{error_type}' in {len(files)} files")
    
    return valid_count, invalid_count


def main():
    """Main function to regenerate and validate JSON Structure files."""
    print("JSON Structure Reference File Generator and Validator")
    print("=" * 60)
    
    # Change to the avrotize2 directory to ensure correct paths
    os.chdir(current_dir)
    
    test_dir = Path("test/jsons")
    if not test_dir.exists():
        print(f"Error: Test directory {test_dir.absolute()} does not exist")
        sys.exit(1)
    
    # Step 1: Delete existing JSON Structure files
    deleted_count = delete_existing_struct_files(test_dir)
    
    # Step 2: Find JSON Schema files to convert
    json_schema_files = find_json_schema_files(test_dir)
    
    if not json_schema_files:
        print("No JSON Schema files found to convert")
        sys.exit(1)
    
    # Step 3: Regenerate JSON Structure files
    generated_count, generation_errors = regenerate_json_structure_files(json_schema_files)
    
    # Step 4: Validate the generated files
    valid_count, invalid_count = validate_json_structure_files()
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"Deleted existing files: {deleted_count}")
    print(f"JSON Schema files found: {len(json_schema_files)}")
    print(f"JSON Structure files generated: {generated_count}")
    print(f"Generation errors: {generation_errors}")
    print(f"Valid JSON Structure files: {valid_count}")
    print(f"Invalid JSON Structure files: {invalid_count}")
    
    total_files = valid_count + invalid_count
    if total_files > 0:
        success_rate = (valid_count / total_files) * 100
        print(f"Validation success rate: {success_rate:.1f}%")
    
    if generation_errors > 0 or invalid_count > 0:
        print(f"\n⚠️  Some files had issues. Check the output above for details.")
    else:
        print(f"\n[SUCCESS] All files generated and validated successfully!")

if __name__ == "__main__":
    main()
