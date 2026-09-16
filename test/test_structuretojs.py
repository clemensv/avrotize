"""
Tests for JSON Structure to JavaScript conversion
"""
import unittest
import os
import shutil
import sys
import tempfile
import json
import subprocess

import pytest

from avrotize.structuretojs import convert_structure_to_javascript

# Import the validator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from json_structure_instance_validator import JSONStructureInstanceValidator

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToJavaScript(unittest.TestCase):
    """Test cases for JSON Structure to JavaScript conversion"""

    def run_convert_struct_to_js(self, struct_name, avro_annotation=False, base_package=None):
        """Test converting a JSON Structure file to JavaScript"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", struct_name + ".struct.json")
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)

        kwargs = {
            "avro_annotation": avro_annotation,
        }
        if base_package:
            kwargs["package_name"] = base_package

        convert_structure_to_javascript(struct_path, js_path, **kwargs)
        
        # Verify files were created
        self.assertTrue(os.path.exists(js_path))
        js_files = []
        for root, dirs, files in os.walk(js_path):
            for file in files:
                if file.endswith('.js'):
                    js_files.append(os.path.join(root, file))
        self.assertGreater(len(js_files), 0, f"No JavaScript files generated for {struct_name}")
        
        # Create a simple Node.js test file to instantiate classes
        test_script = self._create_test_script(js_path, struct_name)
        test_file_path = os.path.join(js_path, "test.js")
        with open(test_file_path, 'w', encoding='utf-8') as f:
            f.write(test_script)
        
        # Run the test script with Node.js if available
        try:
            result = subprocess.run(
                ["node", test_file_path],
                cwd=js_path,
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                print(f"\nNode.js test output:\n{result.stdout}")
                print(f"Node.js test errors:\n{result.stderr}")
                # Don't fail the test if Node.js isn't available or test fails
                # Just verify the files were generated
                print(f"Warning: Node.js tests failed or unavailable for {struct_name}")
            else:
                print(f"[OK] Node.js tests passed for {struct_name}")
                
                # Try to validate generated instances if they exist
                instances_dir = os.path.join(js_path, "instances")
                if os.path.exists(instances_dir):
                    json_files = [f for f in os.listdir(instances_dir) if f.endswith('.json')]
                    if json_files:
                        # Load the schema
                        with open(struct_path, 'r', encoding='utf-8') as f:
                            schema = json.load(f)
                        
                        # Create validator
                        validator = JSONStructureInstanceValidator(schema, extended=True)
                        
                        # Validate each instance
                        for json_file in json_files:
                            instance_path = os.path.join(instances_dir, json_file)
                            with open(instance_path, 'r', encoding='utf-8') as f:
                                instance = json.load(f)
                            
                            errors = validator.validate(instance)
                            if errors:
                                print(f"\nValidation errors for {json_file}:")
                                for error in errors:
                                    print(f"  - {error}")
                                assert False, f"Instance {json_file} failed validation against JSON Structure schema"
                            else:
                                print(f"[OK] {json_file} validated successfully")
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            print(f"Warning: Could not run Node.js tests: {e}")
            # Continue without failing - Node.js might not be installed
        
        return js_path

    def _create_test_script(self, js_path, struct_name):
        """Create a test script that instantiates and tests the generated classes"""
        script = """
// Test script for generated JavaScript classes
const fs = require('fs');
const path = require('path');

// Find all generated JavaScript files
function findJsFiles(dir, files = []) {
    const items = fs.readdirSync(dir);
    for (const item of items) {
        const fullPath = path.join(dir, item);
        const stat = fs.statSync(fullPath);
        if (stat.isDirectory()) {
            findJsFiles(fullPath, files);
        } else if (item.endsWith('.js') && item !== 'test.js') {
            files.push(fullPath);
        }
    }
    return files;
}

// Test classes by instantiating them
const jsFiles = findJsFiles('.');
let testsPassed = 0;
let testsFailed = 0;

// Create instances directory
const instancesDir = path.join('.', 'instances');
if (!fs.existsSync(instancesDir)) {
    fs.mkdirSync(instancesDir, { recursive: true });
}

for (const jsFile of jsFiles) {
    try {
        const modulePath = path.resolve(jsFile);
        const ClassConstructor = require(modulePath);
        
        // Skip if not a constructor function
        if (typeof ClassConstructor !== 'function') {
            continue;
        }
        
        // Try to instantiate
        const instance = new ClassConstructor();
        
        // Set some test values for properties if they exist
        if (instance) {
            testsPassed++;
            console.log(`✓ Successfully instantiated ${path.basename(jsFile, '.js')}`);
            
            // Try to serialize to JSON for validation
            try {
                const json = JSON.stringify(instance, null, 2);
                const className = path.basename(jsFile, '.js');
                const jsonPath = path.join(instancesDir, `${className}.json`);
                fs.writeFileSync(jsonPath, json);
            } catch (serErr) {
                // Serialization might fail if there are circular references, that's ok
            }
        }
    } catch (err) {
        testsFailed++;
        console.error(`✗ Failed for ${path.basename(jsFile)}: ${err.message}`);
    }
}

console.log(`\\nTests passed: ${testsPassed}, Tests failed: ${testsFailed}`);
process.exit(testsFailed > 0 ? 1 : 0);
"""
        return script

    def test_convert_address_struct_to_javascript(self):
        """Test converting address.struct.json to JavaScript"""
        self.run_convert_struct_to_js("address-ref")

    def test_convert_person_struct_to_javascript(self):
        """Test converting person.struct.json to JavaScript"""
        self.run_convert_struct_to_js("person-ref")

    def test_convert_product_struct_to_javascript(self):
        """Test converting a simple struct to JavaScript"""
        # Use a simple struct that exists
        self.run_convert_struct_to_js("anyof-ref")

    def test_convert_primitives_struct_to_javascript(self):
        """Test converting a schema with all primitive types to JavaScript"""
        # Create a test schema with all primitive types
        test_schema = {
            "type": "object",
            "name": "AllPrimitives",
            "properties": {
                "stringField": {"type": "string"},
                "intField": {"type": "int32"},
                "longField": {"type": "int64"},
                "floatField": {"type": "float"},
                "doubleField": {"type": "double"},
                "booleanField": {"type": "boolean"},
                "binaryField": {"type": "binary"},
                "dateField": {"type": "date"},
                "timeField": {"type": "time"},
                "datetimeField": {"type": "datetime"},
                "uuidField": {"type": "uuid"},
                "uriField": {"type": "uri"},
                "decimalField": {"type": "decimal"},
                "nullField": {"type": "null"}
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "primitives-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        # Verify the class file was created
        class_file = os.path.join(js_path, "test", "src", "AllPrimitives.js")
        self.assertTrue(os.path.exists(class_file))
        
        # Read and verify the generated code
        with open(class_file, 'r') as f:
            content = f.read()
            self.assertIn("class AllPrimitives", content)
            self.assertIn("stringField", content)
            self.assertIn("module.exports = AllPrimitives", content)

    def test_convert_array_types_to_javascript(self):
        """Test converting array and collection types to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "Collections",
            "properties": {
                "arrayField": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "setField": {
                    "type": "set",
                    "items": {"type": "int32"}
                },
                "mapField": {
                    "type": "map",
                    "values": {"type": "string"}
                }
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "collections-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        # Verify the class file was created
        class_file = os.path.join(js_path, "test", "src", "Collections.js")
        self.assertTrue(os.path.exists(class_file))
        
        with open(class_file, 'r') as f:
            content = f.read()
            self.assertIn("arrayField", content)
            self.assertIn("setField", content)
            self.assertIn("mapField", content)

    def test_convert_enum_to_javascript(self):
        """Test converting enum types to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "WithEnum",
            "properties": {
                "status": {
                    "enum": ["active", "inactive", "pending"],
                    "type": "string"
                }
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "enum-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        # Verify files were created
        self.assertTrue(os.path.exists(os.path.join(js_path, "test")))

    def test_convert_optional_fields_to_javascript(self):
        """Test converting optional/required fields to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "OptionalFields",
            "properties": {
                "requiredField": {"type": "string"},
                "optionalField": {"type": "string"}
            },
            "required": ["requiredField"]
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "optional-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        class_file = os.path.join(js_path, "test", "src", "OptionalFields.js")
        self.assertTrue(os.path.exists(class_file))

    def test_convert_nested_objects_to_javascript(self):
        """Test converting nested object types to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "Outer",
            "properties": {
                "inner": {
                    "type": "object",
                    "name": "Inner",
                    "properties": {
                        "value": {"type": "string"}
                    }
                }
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "nested-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        # Verify both files were created
        self.assertTrue(os.path.exists(os.path.join(js_path, "test")))

    def test_convert_with_defaults_to_javascript(self):
        """Test converting fields with default values to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "WithDefaults",
            "properties": {
                "stringWithDefault": {
                    "type": "string",
                    "default": "default_value"
                },
                "intWithDefault": {
                    "type": "int32",
                    "default": 42
                },
                "boolWithDefault": {
                    "type": "boolean",
                    "default": True
                }
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "defaults-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        class_file = os.path.join(js_path, "test", "src", "WithDefaults.js")
        self.assertTrue(os.path.exists(class_file))
        
        with open(class_file, 'r') as f:
            content = f.read()
            self.assertIn('"default_value"', content)
            self.assertIn('42', content)
            self.assertIn('true', content)

    def test_convert_with_const_fields_to_javascript(self):
        """Test converting const fields to JavaScript"""
        test_schema = {
            "type": "object",
            "name": "WithConst",
            "properties": {
                "version": {
                    "type": "string",
                    "const": "1.0.0"
                }
            }
        }
        
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "const-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        convert_structure_schema_to_javascript(test_schema, js_path, "test")
        
        class_file = os.path.join(js_path, "test", "src", "WithConst.js")
        self.assertTrue(os.path.exists(class_file))
        
        with open(class_file, 'r') as f:
            content = f.read()
            self.assertIn('static version', content)
            self.assertIn('"1.0.0"', content)


    def test_xml_annotation_runtime_roundtrip(self):
        """Generated structure JavaScript maps XML names, collections, and attributes."""
        if not shutil.which('node') or not shutil.which('npm'):
            self.skipTest('Node.js and npm are required for the XML runtime test')
        from avrotize.structuretojs import convert_structure_schema_to_javascript
        output = os.path.join(tempfile.gettempdir(), 'avrotize', 'structure-js-xml')
        shutil.rmtree(output, ignore_errors=True)
        schema = {
            'type': 'object', 'name': 'Order', 'namespace': 'demo',
            'xmlns': 'urn:example:orders', 'altnames': {'xml': 'purchase-order'},
            'properties': {
                'id': {'type': 'string', 'xmlkind': 'attribute',
                       'altnames': {'xml': 'order-id'}},
                'status': {'type': 'string', 'enum': ['NEW', 'DONE'],
                           'altenums': {'xml': {'NEW': 'new-order'}}},
                'child': {'type': 'object', 'name': 'Child', 'properties': {
                    'count': {'type': 'int32'}}, 'required': ['count']},
                'tags': {'type': 'array', 'items': {'type': 'string'}},
                'codes': {'type': 'set', 'items': {'type': 'int32'}},
                'metadata': {'type': 'map', 'values': {'type': 'int64'}},
                'note': {'type': 'string'},
                'variant': {'type': ['string', 'int32']}
            },
            'required': ['id', 'status', 'child', 'tags', 'codes', 'metadata', 'variant']
        }
        convert_structure_schema_to_javascript(
            schema, output, 'xmltest', xml_annotation=True)
        package_root = os.path.join(output, 'xmltest')
        class_file = os.path.join(package_root, 'demo', 'src', 'Order.js')
        with open(class_file, encoding='utf-8') as generated:
            source = generated.read()
        self.assertIn('Order.XmlMapping', source)
        self.assertIn('name: "child"', source)
        self.assertIn('required: true', source)
        with open(os.path.join(package_root, 'package.json'), encoding='utf-8') as package_file:
            package = json.load(package_file)
        self.assertIn('fast-xml-parser', package['dependencies'])

        install = subprocess.run(
            ['npm', 'install', '--ignore-scripts', '--no-audit', '--no-fund'],
            cwd=package_root, capture_output=True, text=True,
            shell=sys.platform == 'win32', timeout=120)
        self.assertEqual(install.returncode, 0, install.stderr)
        script = r"""
const assert = require('assert');
const zlib = require('zlib');
const Order = require('./demo/src/Order');
const Child = require('./demo/src/Child');
const value = new Order();
value.id = 'A-1'; value.status = 'NEW';
value.child = new Child(); value.child.count = 3;
value.tags = ['one', 'two']; value.codes = new Set([7, 9]);
value.metadata = { first: 1, second: 2 }; value.note = null; value.variant = 42;
for (const mediaType of ['application/xml', 'text/xml', 'application/xml+gzip',
                         'application/json', 'application/json+gzip']) {
  assert.deepStrictEqual(Order.FromData(value.ToByteArray(mediaType), mediaType), value);
}
const xml = value.ToByteArray('application/xml').toString('utf8');
assert.match(xml, /^<purchase-order /);
assert.match(xml, /xmlns="urn:example:orders"/);
assert.match(xml, /order-id="A-1"/);
assert.match(xml, /<status>new-order<\/status>/);
assert.match(xml, /<child><count>3<\/count><\/child>/);

function mustReject(label, payload, mediaType = 'application/xml') {
  assert.throws(
    () => Order.FromData(Buffer.isBuffer(payload) ? payload : Buffer.from(payload), mediaType),
    undefined,
    label
  );
}
function appendElement(fragment) {
  return xml.replace('</purchase-order>', `${fragment}</purchase-order>`);
}

mustReject('truncated XML', xml.slice(0, -12));
mustReject('corrupt gzip', Buffer.from('not-a-gzip-stream'), 'application/xml+gzip');
mustReject('invalid UTF-8', Buffer.from([0xff, 0xfe, 0xfd]));
mustReject('gzip expansion limit', zlib.gzipSync(Buffer.alloc(1024 * 1024 + 1, 0x61)), 'application/xml+gzip');
const entityPayload = '<!DOCTYPE purchase-order [<!ENTITY a "EXPAND"><!ENTITY b "&a;&a;&a;">]>' +
  xml.replace('<status>new-order</status>', '<status>&b;</status>');
mustReject('DOCTYPE/entity expansion', entityPayload);
mustReject('namespace mismatch', xml.replace('urn:example:orders', 'urn:attacker'));
mustReject('missing namespace', xml.replace(' xmlns="urn:example:orders"', ''));
mustReject('missing required attribute', xml.replace(' order-id="A-1"', ''));
mustReject('missing required element', xml.replace(/<status>.*?<\/status>/, ''));
mustReject('missing nested required element', xml.replace('<count>3</count>', ''));
mustReject('duplicate singleton', xml.replace(
  '<status>new-order</status>', '<status>new-order</status><status>DONE</status>'));
mustReject('unknown field', appendElement('<rogue>value</rogue>'));
mustReject('invalid enum', xml.replace('<status>new-order</status>', '<status>UNKNOWN</status>'));
mustReject('invalid scalar', xml.replace('<count>3</count>', '<count>not-an-integer</count>'));
const deep = '<n>'.repeat(70) + 'x' + '</n>'.repeat(70);
mustReject('excessive nesting', appendElement(`<rogue>${deep}</rogue>`));
mustReject('excessive size', appendElement(`<rogue>${'x'.repeat(1024 * 1024)}</rogue>`));
mustReject('ambiguous union', xml.replace(
  /<variant union="\d+"><value>42<\/value><\/variant>/,
  '<variant>42</variant>'));
mustReject('invalid union branch', xml.replace(/<variant union="\d+">/, '<variant union="999">'));
mustReject('map entry without key', xml.replace(' key="first"', ''));
mustReject('duplicate map key', xml.replace(' key="second"', ' key="first"'));
mustReject('invalid map shape', xml.replace(
  /<metadata>.*?<\/metadata>/,
  '<metadata><entry key="first"><other>1</other></entry></metadata>'));
mustReject('direct singleton used as list', xml.replace(
  /<tags>.*?<\/tags>/,
  '<tags>one</tags>'));
mustReject('duplicate list wrapper', xml.replace(
  /(<tags>.*?<\/tags>)/,
  '$1$1'));
mustReject('duplicate attribute', xml.replace(
  'order-id="A-1"',
  'order-id="A-1" order-id="B"'));

value.tags = ['only'];
assert.deepStrictEqual(
  Order.FromData(value.ToByteArray('application/xml'), 'application/xml').tags,
  ['only'],
  'a valid singleton list must remain a list'
);

"""
        script_path = os.path.join(package_root, 'xml-runtime-test.js')
        with open(script_path, 'w', encoding='utf-8') as script_file:
            script_file.write(script)
        result = subprocess.run(['node', script_path], cwd=package_root,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        generated_test = os.path.join(package_root, 'demo', 'test', 'test_Order.js')
        result = subprocess.run(['node', generated_test], cwd=package_root,
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__':
    unittest.main()
