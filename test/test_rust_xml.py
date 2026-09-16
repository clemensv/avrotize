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
        {"name": "count", "type": "int"},
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
        "count": {"type": "int32"},
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
    "required": ["id", "display_name", "count", "tags", "properties", "choice", "state", "child"],
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
use std::io::Write;
use flate2::write::GzEncoder;
use {package}::{root_module}::order::Order;
use {package}::{root_module}::choiceunion::ChoiceUnion;
use {package}::{enum_module}::state::State;
use {package}::{child_module}::child::Child;

#[test]
fn xml_and_json_wire_contracts_round_trip() {{
    let value = Order {{
        id: "42".into(),
        display_name: "example".into(),
        count: 7,
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
    assert!(json.contains("\\\"count\\\":7"), "{{json}}");
    assert!(json.contains("\\\"state\\\":\\\"{json_enum}\\\""), "{{json}}");
    assert!(json.contains("\\\"choice\\\":\\\"selected\\\""), "{{json}}");
    assert!(!json.contains("@order-id"), "{{json}}");

    let xml_bytes = value.to_byte_array("application/xml").unwrap();
    let xml = String::from_utf8(xml_bytes.clone()).unwrap();
    assert!(xml.starts_with("<purchase-order "), "{{xml}}");
    assert!(xml.contains("xmlns=\\\"urn:orders\\\""), "{{xml}}");
    assert!(xml.contains("order-id=\\\"42\\\""), "{{xml}}");
    assert!(xml.contains("<display-name>example</display-name>"), "{{xml}}");
    assert!(xml.contains("<count>7</count>"), "{{xml}}");
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

    let mut large_json = value.clone();
    large_json.display_name = "x".repeat(17 * 1024 * 1024);
    let large_json_gzip = large_json.to_byte_array("application/json+gzip").unwrap();
    assert_eq!(
        large_json,
        Order::from_data(&large_json_gzip, "application/json+gzip").unwrap()
    );

    let mut ambiguous = value.clone();
    ambiguous.choice = ChoiceUnion::I32(42);
    let result = std::panic::catch_unwind(|| ambiguous.to_byte_array("application/xml"));
    assert!(result.is_ok(), "to_byte_array panicked");
    assert!(result.unwrap().is_err(), "ambiguous union was silently serialized");
}}

fn valid_xml() -> String {{
    concat!(
        "<purchase-order xmlns=\\"urn:orders\\" order-id=\\"42\\">",
        "<display-name>example</display-name><count>7</count>",
        "<tags>one</tags><properties><key>value</key></properties>",
        "<choice>selected</choice><state>open-state</state>",
        "<child xmlns=\\"urn:child\\" code=\\"C\\"><value>nested</value></child>",
        "</purchase-order>"
    ).to_string()
}}

fn assert_xml_error(data: &[u8], content_type: &str) {{
    let result = std::panic::catch_unwind(|| Order::from_data(data, content_type));
    assert!(result.is_ok(), "from_data panicked");
    assert!(result.unwrap().is_err(), "invalid XML was silently accepted");
}}

#[test]
fn adversarial_xml_is_rejected_without_panics() {{
    let valid = valid_xml();

    assert_xml_error(b"<purchase-order", "application/xml");
    assert_xml_error(valid[..valid.len() - 9].as_bytes(), "application/xml");
    assert_xml_error(&[0x1f, 0x8b, 0x08, 0x00, 0xff], "application/xml+gzip");

    let doctype = valid.replace(
        "<purchase-order",
        "<!DOCTYPE purchase-order [<!ENTITY xxe SYSTEM \\"file:///etc/passwd\\">]><purchase-order",
    ).replace("example", "&xxe;");
    assert_xml_error(doctype.as_bytes(), "application/xml");

    let entity_bomb = valid.replace(
        "<purchase-order",
        "<!DOCTYPE purchase-order [<!ENTITY a \\"aaaaaaaaaa\\"><!ENTITY b \\"&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;\\">]><purchase-order",
    ).replace("example", "&b;");
    assert_xml_error(entity_bomb.as_bytes(), "application/xml");

    assert_xml_error(valid.replace("urn:orders", "urn:wrong").as_bytes(), "application/xml");
    assert_xml_error(
        valid.replace("<display-name>example</display-name>", "").as_bytes(),
        "application/xml",
    );
    assert_xml_error(
        valid.replace(
            "</display-name>",
            "</display-name><display-name>duplicate</display-name>",
        ).as_bytes(),
        "application/xml",
    );
    assert_xml_error(
        valid.replace("<count>7</count>", "<count>not-a-number</count>").as_bytes(),
        "application/xml",
    );
    assert_xml_error(
        valid.replace("<state>open-state</state>", "<state>invalid</state>").as_bytes(),
        "application/xml",
    );
    assert_xml_error(
        valid.replace("</count>", "</count><unknown>value</unknown>").as_bytes(),
        "application/xml",
    );
    assert_xml_error(
        valid.replace(
            "<value>nested</value>",
            "<display-name>ignored</display-name><value>nested</value>",
        ).as_bytes(),
        "application/xml",
    );
    assert_xml_error(valid.replace("urn:child", "urn:wrong-child").as_bytes(), "application/xml");
    assert_xml_error(
        valid.replace("<display-name>", "<display-name code=\\"unexpected\\">").as_bytes(),
        "application/xml",
    );
    assert_xml_error(
        valid.replace("order-id=\\"42\\"", "order-id=\\"42\\" unexpected=\\"x\\"").as_bytes(),
        "application/xml",
    );
    assert_xml_error(
        valid.replace("order-id=\\"42\\"", "order-id=\\"42\\" order-id=\\"43\\"").as_bytes(),
        "application/xml",
    );

    let nested = format!(
        "<purchase-order xmlns=\\"urn:orders\\" order-id=\\"42\\"><display-name>{{}}{{}}example{{}}{{}}</display-name></purchase-order>",
        "<x>".repeat(140),
        "<y>",
        "</y>",
        "</x>".repeat(140),
    );
    assert_xml_error(nested.as_bytes(), "application/xml");

    let oversized = format!(
        "<purchase-order xmlns=\\"urn:orders\\" order-id=\\"42\\"><display-name>{{}}</display-name></purchase-order>",
        "x".repeat(17 * 1024 * 1024),
    );
    assert_xml_error(oversized.as_bytes(), "application/xml");
    let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(oversized.as_bytes()).unwrap();
    assert_xml_error(&encoder.finish().unwrap(), "application/xml+gzip");

    assert_xml_error(
        valid.replace("<choice>selected</choice>", "<choice>42</choice>").as_bytes(),
        "application/xml",
    );
    assert_xml_error(
        valid.replace(
            "<properties><key>value</key></properties>",
            "<properties><key>value</key><key>duplicate</key></properties>",
        ).as_bytes(),
        "application/xml",
    );
    assert_xml_error(
        valid.replace(
            "<properties><key>value</key></properties>",
            "<properties><key><nested>invalid</nested></key></properties>",
        ).as_bytes(),
        "application/xml",
    );
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
        self.assertTrue(os.path.exists(os.path.join(crate_dir, "src", "xml_support.rs")))

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
