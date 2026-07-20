'Focused generation and runtime tests for TypeScript XML support.'

import json
import os
import shutil
import subprocess
import tempfile

import pytest

from avrotize.avrotots import convert_avro_schema_to_typescript
from avrotize.structuretots import convert_structure_schema_to_typescript


AVRO_SCHEMA = {
    "type": "record", "name": "Order", "namespace": "example", "xmlns": "urn:orders",
    "altnames": {"xml": "purchase"},
    "fields": [
        {"name": "id", "type": "string", "xmlkind": "attribute", "altnames": {"xml": "order-id"}},
        {"name": "count", "type": "int"},
        {"name": "note", "type": ["null", "string"]},
        {"name": "child", "type": {"type": "record", "name": "Child", "fields": [
            {"name": "active", "type": "boolean"}
        ]}},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "state", "type": {"type": "enum", "name": "State", "symbols": ["OPEN", "CLOSED"],
                                            "altenums": {"xml": {"OPEN": "open-order"}}}},
        {"name": "scores", "type": {"type": "map", "values": "double"}},
        {"name": "choice", "type": ["string", {"type": "record", "name": "ChoiceDetail", "fields": [
            {"name": "code", "type": "int"}
        ]}]},
    ],
}

STRUCTURE_SCHEMA = {
    "type": "object", "name": "Order", "namespace": "example", "xmlns": "urn:orders",
    "altnames": {"xml": "purchase"},
    "properties": {
        "id": {"type": "string", "xmlkind": "attribute", "altnames": {"xml": "order-id"}},
        "count": {"type": "int32"},
        "note": {"type": ["string", "null"]},
        "child": {"type": "object", "name": "Child", "properties": {"active": {"type": "boolean"}},
                  "required": ["active"]},
        "tags": {"type": "array", "items": {"type": "string"}},
        "state": {"type": "string", "name": "State", "enum": ["OPEN", "CLOSED"],
                  "altenums": {"json": {"OPEN": "open"}, "xml": {"OPEN": "open-order"}}},
        "scores": {"type": "map", "values": {"type": "double"}},
    },
    "required": ["id", "count", "child", "tags", "state", "scores"],
}


def _run(command, cwd):
    return subprocess.run(command, cwd=cwd, capture_output=True, text=True,
                          shell=os.name == "nt", timeout=300)


def _build_and_run(output_dir, script):
    install = _run(["npm", "install", "--ignore-scripts"], output_dir)
    assert install.returncode == 0, install.stderr
    build = _run(["npm", "run", "build"], output_dir)
    assert build.returncode == 0, build.stderr
    script_path = os.path.join(output_dir, "xml-runtime.mjs")
    with open(script_path, "w", encoding="utf-8") as handle:
        handle.write(script)
    runtime = _run(["node", "xml-runtime.mjs"], output_dir)
    assert runtime.returncode == 0, runtime.stderr


def test_avrotots_xml_generation_and_runtime():
    if not shutil.which("npm") or not shutil.which("node"):
        pytest.skip("Node.js toolchain is unavailable")
    output = os.path.join(tempfile.gettempdir(), "avrotize", "avrotots-xml")
    shutil.rmtree(output, ignore_errors=True)
    convert_avro_schema_to_typescript(AVRO_SCHEMA, output, "xmltypes", xml_annotation=True)

    source = os.path.join(output, "src", "xmltypes", "example", "Order.ts")
    with open(source, encoding="utf-8") as handle:
        generated = handle.read()
    with open(os.path.join(output, "package.json"), encoding="utf-8") as handle:
        package = json.load(handle)
    assert 'rootName: "purchase"' in generated
    assert 'name: "order-id", kind: \'attribute\'' in generated
    assert "application/xml" in generated and "text/xml" in generated
    assert package["dependencies"]["fast-xml-parser"]

    _build_and_run(output, r'''
import assert from "node:assert/strict";
import { Order } from "./dist/xmltypes/example/Order.js";
import { Child } from "./dist/xmltypes/example/Child.js";
import { State } from "./dist/xmltypes/example/State.js";
import { ChoiceDetail } from "./dist/xmltypes/example/ChoiceDetail.js";
import { ChoiceUnion } from "./dist/xmltypes/example/ChoiceUnion.js";
const value = new Order("A-1", 3, undefined, new Child(true), ["one", "two"], State.OPEN,
    {speed: 42.5}, new ChoiceUnion(new ChoiceDetail(9)));
const bytes = value.toByteArray("application/xml");
const xml = new TextDecoder().decode(bytes);
assert.match(xml, /<purchase[^>]*xmlns="urn:orders"[^>]*>/);
assert.match(xml, /<purchase[^>]*order-id="A-1"[^>]*>/);
assert.match(xml, /<state>open-order<\/state>/);
assert.deepStrictEqual(Order.fromData(bytes, "text/xml"), value);
const gzip = value.toByteArray("application/xml+gzip");
assert.deepStrictEqual(Order.fromData(gzip, "application/xml+gzip"), value);
''')


def test_structuretots_xml_generation_and_runtime():
    if not shutil.which("npm") or not shutil.which("node"):
        pytest.skip("Node.js toolchain is unavailable")
    output = os.path.join(tempfile.gettempdir(), "avrotize", "structuretots-xml")
    shutil.rmtree(output, ignore_errors=True)
    convert_structure_schema_to_typescript(STRUCTURE_SCHEMA, output, "xmltypes", xml_annotation=True)

    source = os.path.join(output, "src", "xmltypes", "example", "Order.ts")
    with open(source, encoding="utf-8") as handle:
        generated = handle.read()
    with open(os.path.join(output, "package.json"), encoding="utf-8") as handle:
        package = json.load(handle)
    assert 'namespace: "urn:orders"' in generated
    assert "XmlRecordMapping<Order>" in generated
    assert package["dependencies"]["fast-xml-parser"]

    _build_and_run(output, r'''
import assert from "node:assert/strict";
import { Order } from "./dist/xmltypes/example/Order.js";
import { Child } from "./dist/xmltypes/example/Child.js";
import { State } from "./dist/xmltypes/example/State.js";
const value = new Order("S-1", 5, new Child(false), ["one", "two"], State.OPEN, {speed: 7.5});
const bytes = value.toByteArray("application/xml");
const xml = new TextDecoder().decode(bytes);
assert.match(xml, /<purchase[^>]*xmlns="urn:orders"[^>]*>/);
assert.match(xml, /<state>open-order<\/state>/);
assert.deepStrictEqual(Order.fromData(bytes, "text/xml"), value);
const gzip = value.toByteArray("text/xml+gzip");
assert.deepStrictEqual(Order.fromData(gzip, "text/xml+gzip"), value);
''')
