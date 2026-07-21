import os
import shutil
import sys
import tempfile
import json
import subprocess
from os import path, getcwd

import pytest

from avrotize.avrotojs import convert_avro_to_javascript, convert_avro_schema_to_javascript
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch

class TestAvroToJavaScript(unittest.TestCase):
    def test_convert_address_avsc_to_javascript(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        convert_avro_to_javascript(avro_path, js_path)

    def test_convert_telemetry_avsc_to_javascript(self):
        """ Test converting a telemetry.avsc file to C# """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        js_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
        
        convert_avro_to_javascript(avro_path, js_path)

    def test_convert_jfrog_pipelines_jsons_to_avro_to_javascript(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        js_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-js")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_javascript(avro_path, js_path)
                
    def test_convert_jfrog_pipelines_jsons_to_avro_to_javascript_avro_annotations(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        js_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-js-avro")
        if os.path.exists(js_path):
            shutil.rmtree(js_path, ignore_errors=True)
        os.makedirs(js_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_javascript(avro_path, js_path, avro_annotation=True)

    def test_xml_annotation_runtime_roundtrip(self):
        """Generated Avro JavaScript honors XML mappings and gzip round-trips."""
        if not shutil.which('node') or not shutil.which('npm'):
            self.skipTest('Node.js and npm are required for the XML runtime test')
        output = os.path.join(tempfile.gettempdir(), 'avrotize', 'avro-js-xml')
        shutil.rmtree(output, ignore_errors=True)
        schema = {
            'type': 'record', 'name': 'Order', 'namespace': 'demo',
            'xmlns': 'urn:example:orders', 'altnames': {'xml': 'purchase-order'},
            'fields': [
                {'name': 'id', 'type': 'string', 'xmlkind': 'attribute',
                 'altnames': {'xml': 'order-id'}},
                {'name': 'status', 'type': {'type': 'enum', 'name': 'Status',
                 'symbols': ['NEW', 'DONE'], 'altenums': {'xml': {'NEW': 'new-order'}}}},
                {'name': 'child', 'type': ['null', {'type': 'record', 'name': 'Child',
                 'fields': [{'name': 'count', 'type': 'int'}]}]},
                {'name': 'tags', 'type': {'type': 'array', 'items': 'string'}},
                {'name': 'metadata', 'type': {'type': 'map', 'values': 'long'}},
                {'name': 'note', 'type': ['null', 'string']},
                {'name': 'variant', 'type': ['string', 'int']}
            ]
        }
        convert_avro_schema_to_javascript(
            schema, output, 'xmltest', avro_annotation=True, xml_annotation=True)
        package_root = os.path.join(output, 'xmltest')
        class_file = os.path.join(package_root, 'demo', 'Order.js')
        with open(class_file, encoding='utf-8') as generated:
            source = generated.read()
        self.assertIn('Order.XmlMapping', source)
        self.assertIn('kind: "attribute"', source)
        self.assertIn('required: true', source)
        with open(os.path.join(package_root, 'package.json'), encoding='utf-8') as package_file:
            package = json.load(package_file)
        self.assertIn('fast-xml-parser', package['dependencies'])
        self.assertIn('avro-js', package['dependencies'])

        install = subprocess.run(
            ['npm', 'install', '--ignore-scripts', '--no-audit', '--no-fund'],
            cwd=package_root, capture_output=True, text=True,
            shell=sys.platform == 'win32', timeout=120)
        self.assertEqual(install.returncode, 0, install.stderr)
        script = r"""
const assert = require('assert');
const zlib = require('zlib');
const Order = require('./demo/Order');
const Child = require('./demo/Child');
const Status = require('./demo/Status');
const value = new Order();
value.id = 'A-1'; value.status = Status.NEW;
value.child = new Child(); value.child.count = 3;
value.tags = ['one', 'two']; value.metadata = { first: 1, second: 2 };
value.note = null; value.variant = 42;
for (const mediaType of ['application/xml', 'text/xml', 'application/xml+gzip',
                         'application/json', 'application/json+gzip', 'avro/json',
                         'application/vnd.apache.avro+json+gzip']) {
  assert.deepStrictEqual(Order.FromData(value.ToByteArray(mediaType), mediaType), value);
}
const xml = value.ToByteArray('application/xml').toString('utf8');
assert.match(xml, /^<purchase-order /);
assert.match(xml, /xmlns="urn:example:orders"/);
assert.match(xml, /order-id="A-1"/);
assert.match(xml, /<status>new-order<\/status>/);
assert.match(xml, /<tags><item>one<\/item><item>two<\/item><\/tags>/);

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
