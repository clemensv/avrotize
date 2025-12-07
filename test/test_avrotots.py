import os
import shutil
import sys
import tempfile
from os import path, getcwd
import re
import json

import pytest

from avrotize.avrotots import convert_avro_to_typescript
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch

class TestAvroToTypeScript(unittest.TestCase):
    def run_test(self, avro_name:str, typedjson_annotation:bool=False, avro_annotation:bool=False):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{avro_name}.avsc")
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{avro_name}-ts{'' if not typedjson_annotation else '-typed-json'}{'' if not avro_annotation else '-avro'}")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
    
        convert_avro_to_typescript(avro_path, ts_path, avro_name+"types", typedjson_annotation, avro_annotation)  

    def test_convert_address_avsc_to_typescript(self):
        """ Test converting an address.avsc file to C# """
        self.run_test("address", typedjson_annotation=True, avro_annotation=True)
        self.run_test("address", typedjson_annotation=True)
        self.run_test("address", avro_annotation=True)

    def test_convert_telemetry_avsc_to_typescript(self):
        """ Test converting a telemetry.avsc file to C# """
        self.run_test("telemetry", typedjson_annotation=True, avro_annotation=True)
        self.run_test("telemetry", typedjson_annotation=True)
        self.run_test("telemetry", avro_annotation=True)

    def test_convert_jfrog_pipelines_jsons_to_avro_to_typescript(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        ts_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-ts")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_typescript(avro_path, ts_path)
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_typescript_typed_json(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        ts_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-ts-typed-json")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_typescript(avro_path, ts_path, typedjson_annotation=True)
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_typescript_avro_annotations(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        ts_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-ts-avro")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_typescript(avro_path, ts_path, avro_annotation=True)

    def test_avro_schema_json_not_escaped(self):
        """
        Regression test for bug where Avro schema JSON had escaped quotes.
        Verifies that generated TypeScript with avro_annotation=True contains
        properly formatted JSON without backslash-escaped quotes in avro.parse().
        
        This prevents the critical bug where:
        - INCORRECT: avro.parse({\"type\": \"record\", ...})
        - CORRECT:   avro.parse({"type": "record", ...})
        """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-ts-avro-validation")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
        
        # Generate TypeScript with Avro annotations
        convert_avro_to_typescript(avro_path, ts_path, "addresstypes", 
                                   typedjson_annotation=False, 
                                   avro_annotation=True)
        
        # Find all generated .ts files (excluding index.ts)
        ts_files = []
        for root, dirs, files in os.walk(os.path.join(ts_path, "src")):
            for file in files:
                if file.endswith('.ts') and file != 'index.ts':
                    ts_files.append(os.path.join(root, file))
        
        # Should have at least one generated class file
        self.assertGreater(len(ts_files), 0, "No TypeScript class files were generated")
        
        # Check each generated file for proper JSON formatting
        for ts_file in ts_files:
            with open(ts_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # If this file contains AvroType, validate the schema
            if 'public static AvroType' in content:
                # Extract the avro.parse(...) call
                schema_pattern = r'avro\.parse\((\{[^)]+\})\)'
                matches = re.findall(schema_pattern, content, re.DOTALL)
                
                self.assertGreater(len(matches), 0, 
                    f"Could not find avro.parse() in {ts_file}")
                
                for schema_json in matches:
                    # CRITICAL: Verify no backslash-escaped quotes
                    self.assertNotIn(r'\"', schema_json,
                        f"Found escaped quotes in Avro schema JSON in {ts_file}. "
                        f"This causes TypeScript compilation to fail. "
                        f"Schema should be a valid JavaScript object literal, not an escaped string.")
                    
                    # Verify it contains unescaped quotes (valid JSON)
                    self.assertIn('"type"', schema_json,
                        f"Avro schema JSON should contain unescaped quotes in {ts_file}")
                    
                    # Verify the JSON is valid by parsing it
                    try:
                        parsed = json.loads(schema_json)
                        self.assertIn('type', parsed,
                            f"Parsed Avro schema should contain 'type' field in {ts_file}")
                    except json.JSONDecodeError as e:
                        self.fail(f"Avro schema JSON is not valid in {ts_file}: {e}\nSchema: {schema_json[:200]}...")

    def test_avro_schema_embedding_format(self):
        """
        Comprehensive test to verify Avro schema embedding in TypeScript.
        Tests multiple schema files to ensure consistent proper formatting.
        """
        test_schemas = ["address", "telemetry"]
        
        for schema_name in test_schemas:
            with self.subTest(schema=schema_name):
                cwd = os.getcwd()
                avro_path = os.path.join(cwd, "test", "avsc", f"{schema_name}.avsc")
                ts_path = os.path.join(tempfile.gettempdir(), "avrotize", 
                                      f"{schema_name}-ts-format-validation")
                
                if os.path.exists(ts_path):
                    shutil.rmtree(ts_path, ignore_errors=True)
                os.makedirs(ts_path, exist_ok=True)
                
                # Generate with Avro annotations
                convert_avro_to_typescript(avro_path, ts_path, f"{schema_name}types",
                                          avro_annotation=True)
                
                # Verify all generated files
                ts_files = []
                for root, dirs, files in os.walk(os.path.join(ts_path, "src")):
                    for file in files:
                        if file.endswith('.ts') and file != 'index.ts':
                            ts_files.append(os.path.join(root, file))
                
                for ts_file in ts_files:
                    with open(ts_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    if 'AvroType' in content:
                        # Count backslash-escaped quotes - should be ZERO
                        escaped_quote_count = content.count(r'\"')
                        self.assertEqual(escaped_quote_count, 0,
                            f"Found {escaped_quote_count} escaped quotes in {os.path.basename(ts_file)} "
                            f"from {schema_name}.avsc. Avro schema JSON should not have escaped quotes.")

    def test_avro_js_type_definitions_generated(self):
        """
        Test that avro-js.d.ts type definition file is generated when avro_annotation=True.
        This prevents TypeScript compilation error TS7016: Could not find a declaration file for module 'avro-js'.
        """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-ts-avro-types-test")
        
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
        
        # Generate TypeScript with Avro annotations
        convert_avro_to_typescript(avro_path, ts_path, "addresstypes", 
                                   typedjson_annotation=False, 
                                   avro_annotation=True)
        
        # Verify avro-js.d.ts was generated in the src directory (not root)
        avro_js_types_file = os.path.join(ts_path, "src", "avro-js.d.ts")
        self.assertTrue(os.path.exists(avro_js_types_file),
                       "avro-js.d.ts type definition file should be generated in src/ directory when avro_annotation=True")
        
        # Verify it's NOT in the root directory
        avro_js_types_file_root = os.path.join(ts_path, "avro-js.d.ts")
        self.assertFalse(os.path.exists(avro_js_types_file_root),
                        "avro-js.d.ts should be in src/ directory, not in project root")
        
        # Verify the content is valid TypeScript
        with open(avro_js_types_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for essential type declarations
        self.assertIn("declare module 'avro-js'", content,
                     "Type definition should declare the avro-js module")
        self.assertIn("export class Type", content,
                     "Type definition should export the Type class")
        self.assertIn("export function parse", content,
                     "Type definition should export the parse function")
        
        # Test without avro_annotation - file should NOT be generated
        ts_path_no_avro = os.path.join(tempfile.gettempdir(), "avrotize", "address-ts-no-avro-types-test")
        if os.path.exists(ts_path_no_avro):
            shutil.rmtree(ts_path_no_avro, ignore_errors=True)
        os.makedirs(ts_path_no_avro, exist_ok=True)
        
        convert_avro_to_typescript(avro_path, ts_path_no_avro, "addresstypes",
                                   typedjson_annotation=False,
                                   avro_annotation=False)
        
        avro_js_types_file_no_avro = os.path.join(ts_path_no_avro, "src", "avro-js.d.ts")
        self.assertFalse(os.path.exists(avro_js_types_file_no_avro),
                        "avro-js.d.ts should NOT be generated when avro_annotation=False")

    def test_create_instance_method_generated(self):
        """
        Test that the createInstance() static method is generated for TypeScript classes.
        This test helper creates instances with valid sample data for testing.
        """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-ts-createinstance-test")
        
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
        
        convert_avro_to_typescript(avro_path, ts_path, "telemetrytypes", typedjson_annotation=True)
        
        # Find all generated TypeScript class files
        ts_files = []
        for root, dirs, files in os.walk(os.path.join(ts_path, "src")):
            for file in files:
                if file.endswith('.ts') and file != 'index.ts' and not file.endswith('.d.ts'):
                    ts_files.append(os.path.join(root, file))
        
        self.assertGreater(len(ts_files), 0, "Should have generated TypeScript files")
        
        for ts_file in ts_files:
            with open(ts_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Skip enum files (they don't have createInstance)
            if '@jsonObject' not in content:
                continue
            
            # Verify createInstance method exists
            self.assertIn('public static createInstance()', content,
                f"createInstance() method should be generated in {os.path.basename(ts_file)}")
            
            # Verify it returns the class type
            class_name_match = re.search(r'export class (\w+)', content)
            if class_name_match:
                class_name = class_name_match.group(1)
                self.assertIn(f'createInstance(): {class_name}', content,
                    f"createInstance() should return {class_name} in {os.path.basename(ts_file)}")
                self.assertIn(f'return new {class_name}(', content,
                    f"createInstance() should call new {class_name}() in {os.path.basename(ts_file)}")

    def test_jest_test_files_generated(self):
        """
        Test that Jest test files are generated for TypeScript classes.
        These test files use createInstance() for testing serialization and properties.
        """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-ts-jest-tests")
        
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
        
        convert_avro_to_typescript(avro_path, ts_path, "telemetrytypes", typedjson_annotation=True)
        
        # Verify test directory exists
        test_dir = os.path.join(ts_path, "test")
        self.assertTrue(os.path.exists(test_dir), "test/ directory should be generated")
        
        # Find all test files
        test_files = []
        for root, dirs, files in os.walk(test_dir):
            for file in files:
                if file.endswith('.test.ts'):
                    test_files.append(os.path.join(root, file))
        
        self.assertGreater(len(test_files), 0, "Should have generated Jest test files")
        
        for test_file in test_files:
            with open(test_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Verify test file structure
            self.assertIn("describe('", content,
                f"Test file should use Jest describe() in {os.path.basename(test_file)}")
            self.assertIn(".createInstance()", content,
                f"Test file should use createInstance() in {os.path.basename(test_file)}")
            self.assertIn("beforeEach", content,
                f"Test file should have beforeEach setup in {os.path.basename(test_file)}")
            self.assertIn("expect(", content,
                f"Test file should have expect() assertions in {os.path.basename(test_file)}")

    def test_typescript_esm_runtime_avro_import(self):
        """
        Test that generated TypeScript code compiles and runs correctly in ESM mode.
        This tests the avro-js CJS/ESM interop issue where named imports fail at runtime.
        
        Regression test for issue #159.
        """
        import subprocess
        
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-ts-esm-runtime")
        
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
        
        # Generate TypeScript with Avro annotations (the problematic case)
        convert_avro_to_typescript(avro_path, ts_path, "addresstypes", 
                                   typedjson_annotation=False, 
                                   avro_annotation=True)
        
        # Install dependencies
        result = subprocess.run(['npm', 'install'], cwd=ts_path, 
                               capture_output=True, text=True, shell=True)
        self.assertEqual(result.returncode, 0, 
            f"npm install failed: {result.stderr}")
        
        # Build the TypeScript project
        result = subprocess.run(['npm', 'run', 'build'], cwd=ts_path,
                               capture_output=True, text=True, shell=True)
        self.assertEqual(result.returncode, 0,
            f"TypeScript build failed: {result.stderr}")
        
        # Create a test script that imports and uses the generated code
        test_script = '''
import { Addresstypes_Example_Com_Record as Record } from './dist/index.js';

// Test that AvroType static property was initialized correctly
// This will fail if avro-js import doesn't work in ESM
console.log('SUCCESS: AvroType is:', typeof Record.AvroType);

// Verify the static type was properly initialized  
if (typeof Record.AvroType === 'undefined') {
    console.error('FAIL: AvroType is undefined - avro-js import failed');
    process.exit(1);
}

// Verify we can use avro.parse result - it should have toBuffer method
if (typeof Record.AvroType.toBuffer !== 'function') {
    console.error('FAIL: AvroType.toBuffer is not a function - avro-js parse failed');
    process.exit(1);
}

console.log('SUCCESS: avro-js ESM import and parse working correctly');
process.exit(0);
'''
        test_script_path = os.path.join(ts_path, 'test-esm-runtime.mjs')
        with open(test_script_path, 'w', encoding='utf-8') as f:
            f.write(test_script)
        
        # Run the test script in Node.js ESM mode
        result = subprocess.run(['node', 'test-esm-runtime.mjs'], cwd=ts_path,
                               capture_output=True, text=True, shell=True)
        
        self.assertEqual(result.returncode, 0,
            f"ESM runtime test failed. This indicates avro-js CJS/ESM interop issue.\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}")
        
        self.assertIn('SUCCESS', result.stdout,
            f"Test script should report success. Output: {result.stdout}")
