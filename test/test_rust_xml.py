"""Focused runtime tests for Rust XML generation."""

import json
import os
import shutil
import subprocess
import tempfile
import unittest

from avrotize.avrotorust import convert_avro_schema_to_rust
from avrotize.structuretorust import convert_structure_schema_to_rust


AVRO_SCHEMA = {
    "type": "record",
    "name": "Order",
    "namespace": "xmlroot",
    "xmlns": "urn:orders",
    "altnames": {"xml": "purchase-order"},
    "fields": [
        {"name": "id", "type": "string", "xmlkind": "attribute", "altnames": {"xml": "order-id"}},
        {"name": "display_name", "type": "string", "altnames": {"xml": "display-name"}},
        {"name": "note", "type": ["null", "string"]},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "properties", "type": {"type": "map", "values": "string"}},
        {"name": "choice", "type": ["string", "int"]},
        {
            "name": "state",
            "type": {
                "type": "enum",
                "name": "State",
                "namespace": "xmlenum",
                "symbols": ["OPEN", "CLOSED"],
                "altenums": {"xml": {"OPEN": "open-state", "CLOSED": "closed-state"}},
            },
        },
        {
            "name": "child",
            "type": {
                "type": "record",
                "name": "Child",
                "namespace": "xmlchild",
                "xmlns": "urn:child",
                "fields": [
                    {"name": "code", "type": "string", "xmlkind": "attribute"},
                    {"name": "value", "type": "string"},
                ],
            },
        },
    ],
}

STRUCTURE_SCHEMA = {
    "type": "object",
    "name": "Order",
    "namespace": "sxmlroot",
    "xmlns": "urn:orders",
    "altnames": {"xml": "purchase-order"},
    "properties": {
        "id": {
            "type": "string",
            "xmlkind": "attribute",
            "altnames": {"json": "json-id", "xml": "order-id"},
        },
        "display_name": {
            "type": "string",
            "altnames": {"json": "displayName", "xml": "display-name"},
        },
        "note": {"type": "string"},
        "tags": {"type": "array", "items": {"type": "string"}},
        "properties": {"type": "map", "values": {"type": "string"}},
        "choice": {"type": ["string", "int32"]},
        "state": {
            "type": "string",
            "name": "State",
            "namespace": "sxmlenum",
            "enum": ["OPEN", "CLOSED"],
            "altenums": {
                "json": {"OPEN": "json-open", "CLOSED": "json-closed"},
                "xml": {"OPEN": "open-state", "CLOSED": "closed-state"},
            },
        },
        "child": {
            "type": "object",
            "name": "Child",
            "namespace": "sxmlchild",
            "xmlns": "urn:child",
            "properties": {
                "code": {"type": "string", "xmlkind": "attribute"},
                "value": {"type": "string"},
            },
            "required": ["code", "value"],
        },
    },
    "required": ["id", "display_name", "tags", "properties", "choice", "state", "child"],
}


class TestRustXml(unittest.TestCase):
    """Generate and execute representative quick-xml crates."""

    def setUp(self):
        if not shutil.which("cargo"):
            self.skipTest("cargo not found")
        self.output_dir = tempfile.mkdtemp(prefix="avrotize-rust-xml-")
        self.addCleanup(shutil.rmtree, self.output_dir, True)

    def run_cargo_test(self, crate_dir):
        result = subprocess.run(
            ["cargo", "test", "--quiet"],
            cwd=crate_dir,
            capture_output=True,
            text=True,
            timeout=600,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def write_runtime_test(self, crate_dir, package, root_module, enum_module, child_module, json_id, json_display, json_enum):
        tests_dir = os.path.join(crate_dir, "tests")
        os.makedirs(tests_dir, exist_ok=True)
        source = f"""
use std::collections::HashMap;
use {package}::{root_module}::order::Order;
use {package}::{root_module}::choiceunion::ChoiceUnion;
use {package}::{enum_module}::state::State;
use {package}::{child_module}::child::Child;

#[test]
fn xml_and_json_wire_contracts_round_trip() {{
    let value = Order {{
        id: "42".into(),
        display_name: "example".into(),
        note: None,
        tags: vec!["one".into(), "two".into()],
        properties: HashMap::from([("key".into(), "value".into())]),
        choice: ChoiceUnion::String("selected".into()),
        state: State::OPEN,
        child: Child {{ code: "C".into(), value: "nested".into() }},
    }};

    let json = String::from_utf8(value.to_byte_array("application/json").unwrap()).unwrap();
    assert!(json.contains("\\\"{json_id}\\\":\\\"42\\\""), "{{json}}");
    assert!(json.contains("\\\"{json_display}\\\":\\\"example\\\""), "{{json}}");
    assert!(json.contains("\\\"state\\\":\\\"{json_enum}\\\""), "{{json}}");
    assert!(json.contains("\\\"choice\\\":\\\"selected\\\""), "{{json}}");
    assert!(!json.contains("@order-id"), "{{json}}");

    let xml_bytes = value.to_byte_array("application/xml").unwrap();
    let xml = String::from_utf8(xml_bytes.clone()).unwrap();
    assert!(xml.starts_with("<purchase-order "), "{{xml}}");
    assert!(xml.contains("xmlns=\\\"urn:orders\\\""), "{{xml}}");
    assert!(xml.contains("order-id=\\\"42\\\""), "{{xml}}");
    assert!(xml.contains("<display-name>example</display-name>"), "{{xml}}");
    assert!(!xml.contains("<note"), "{{xml}}");
    assert!(xml.contains("<tags>one</tags><tags>two</tags>"), "{{xml}}");
    assert!(xml.contains("<properties><key>value</key></properties>"), "{{xml}}");
    assert!(xml.contains("<choice>selected</choice>"), "{{xml}}");
    assert!(xml.contains("<state>open-state</state>"), "{{xml}}");
    assert!(xml.contains("<child xmlns=\\\"urn:child\\\" code=\\\"C\\\"><value>nested</value></child>"), "{{xml}}");
    assert_eq!(value, Order::from_data(&xml_bytes, "application/xml").unwrap());

    let text_xml = value.to_byte_array("text/xml").unwrap();
    assert_eq!(value, Order::from_data(&text_xml, "text/xml").unwrap());
    let gzip = value.to_byte_array("application/xml+gzip").unwrap();
    assert_eq!(value, Order::from_data(&gzip, "application/xml+gzip").unwrap());
    let text_gzip = value.to_byte_array("text/xml+gzip").unwrap();
    assert_eq!(value, Order::from_data(&text_gzip, "text/xml+gzip").unwrap());
}}
"""
        with open(os.path.join(tests_dir, "xml_runtime.rs"), "w", encoding="utf-8") as test_file:
            test_file.write(source)

    def assert_generated_metadata(self, crate_dir, root_module):
        with open(os.path.join(crate_dir, "Cargo.toml"), encoding="utf-8") as cargo_file:
            self.assertIn('quick-xml = { version = "0.38", features = ["serialize"] }', cargo_file.read())
        with open(os.path.join(crate_dir, "src", root_module, "order.rs"), encoding="utf-8") as source_file:
            source = source_file.read()
        self.assertIn('#[serde(rename = "purchase-order")]', source)
        self.assertIn('"@order-id"', source)
        self.assertIn('media_type.starts_with("application/xml")', source)
        self.assertIn('media_type.starts_with("text/xml")', source)

    def test_avro_xml_runtime(self):
        crate_dir = os.path.join(self.output_dir, "avro")
        convert_avro_schema_to_rust(
            AVRO_SCHEMA,
            crate_dir,
            package_name="avro_xml",
            serde_annotation=True,
            xml_annotation=True,
        )
        self.assert_generated_metadata(crate_dir, "xmlroot")
        self.write_runtime_test(crate_dir, "avro_xml", "xmlroot", "xmlenum", "xmlchild", "id", "display_name", "OPEN")
        self.run_cargo_test(crate_dir)

    def test_structure_xml_runtime(self):
        crate_dir = os.path.join(self.output_dir, "structure")
        convert_structure_schema_to_rust(
            STRUCTURE_SCHEMA,
            crate_dir,
            package_name="structure_xml",
            serde_annotation=True,
            xml_annotation=True,
        )
        self.assert_generated_metadata(crate_dir, "sxmlroot")
        self.write_runtime_test(
            crate_dir,
            "structure_xml",
            "sxmlroot",
            "sxmlenum",
            "sxmlchild",
            "json-id",
            "displayName",
            "json-open",
        )
        self.run_cargo_test(crate_dir)


class TestRustXmlCli(unittest.TestCase):
    """The public CLI surface exposes the same XML flag for both converters."""

    def test_rust_commands_expose_xml_annotation(self):
        commands_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "avrotize", "commands.json")
        with open(commands_path, encoding="utf-8") as commands_file:
            commands = {command["command"]: command for command in json.load(commands_file)}
        for command_name in ("a2rust", "s2rust"):
            command = commands[command_name]
            self.assertEqual(command["function"]["args"]["xml_annotation"], "args.xml_annotation")
            self.assertIn("--xml-annotation", [argument["name"] for argument in command["args"]])
