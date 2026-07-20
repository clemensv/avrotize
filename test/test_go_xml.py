"Focused XML parity tests for the Go generators (issue #412)."

import json
from pathlib import Path
import shutil
import subprocess
import tempfile

import pytest

from avrotize.avrotogo import convert_avro_schema_to_go
from avrotize.common import xml_enum_wire_value, xml_wire_name
from avrotize.structuretogo import convert_structure_schema_to_go

AVRO_SCHEMA = {
    "type": "record", "name": "Order", "xmlns": "urn:orders", "altnames": {"xml": "order-doc"},
    "fields": [
        {"name": "id", "type": "string", "xmlkind": "attribute", "altnames": {"xml": "order-id"}},
        {"name": "note", "type": ["null", "string"], "default": None},
        {"name": "child", "type": {"type": "record", "name": "Child", "fields": [{"name": "value", "type": "string"}]}},
        {"name": "items", "type": {"type": "array", "items": "string"}},
        {"name": "labels", "type": {"type": "map", "values": "string"}},
        {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "DONE"], "altsymbols": {"xml": {"NEW": "new-order"}}}},
    ],
}

STRUCTURE_SCHEMA = {
    "type": "object", "name": "Order", "xmlns": "urn:orders", "altnames": {"xml": "order-doc"},
    "properties": {
        "id": {"type": "string", "xmlkind": "attribute", "altnames": {"xml": "order-id"}},
        "note": {"type": "string"},
        "child": {"type": "object", "name": "Child", "properties": {"value": {"type": "string"}}, "required": ["value"]},
        "items": {"type": "array", "items": {"type": "string"}},
        "labels": {"type": "map", "values": {"type": "string"}},
        "status": {"type": "string", "name": "Status", "enum": ["NEW", "DONE"], "altenums": {"xml": {"NEW": "new-order"}}},
    },
    "required": ["id", "child", "items", "labels", "status"],
}


def _generate(converter, schema, name):
    output = Path(tempfile.mkdtemp(prefix=f"avrotize-{name}-xml-"))
    converter(schema, str(output), package_name="xmltest", xml_annotation=True, json_annotation=True)
    return output


def _source(output, name):
    return (output / "pkg" / "xmltest" / f"{name}.go").read_text(encoding="utf-8")


def test_xml_alternate_name_helpers():
    assert xml_wire_name("safe", {"altnames": {"xml": "wire-name"}}) == "wire-name"
    assert xml_wire_name("safe", {"altnames": {"json": "ignored"}}) == "safe"
    assert xml_enum_wire_value("A", {"altenums": {"xml": {"A": "alpha"}}}) == "alpha"
    assert xml_enum_wire_value("A", {"altsymbols": {"xml": {"A": "alpha"}}}) == "alpha"


@pytest.mark.parametrize("converter,schema", [(convert_avro_schema_to_go, AVRO_SCHEMA), (convert_structure_schema_to_go, STRUCTURE_SCHEMA)])
def test_go_xml_generator_metadata_and_runtime_branches(converter, schema):
    output = _generate(converter, schema, converter.__name__)
    source = _source(output, "Order")
    child = _source(output, "Child")
    enum = _source(output, "Status")
    assert 'XMLName xml.Name `json:"-" xml:"urn:orders order-doc"`' in ' '.join(source.split())
    assert 'xml:"order-id,attr"' in source
    assert 'xml:"note"' in source
    assert "XMLName" not in child
    assert 'case "application/xml", "text/xml":' in source
    assert 'strings.TrimSuffix(mediaType, "+gzip")' in source
    assert "MarshalXML" in enum and 'return "new-order", nil' in enum
    assert "type XMLMap[T any] map[string]T" in _source(output, "xml_helpers")


def test_go_cli_exposes_xml_annotation_for_both_converters():
    commands_path = Path(__file__).parents[1] / "avrotize" / "commands.json"
    commands = {item["command"]: item for item in json.loads(commands_path.read_text(encoding="utf-8"))}
    for command in ("a2go", "s2go"):
        assert "xml_annotation" in commands[command]["function"]["args"]
        assert "--xml-annotation" in [arg["name"] for arg in commands[command]["args"]]


GO_RUNTIME_TEST = r'''package xmltest

import (
    "reflect"
    "strings"
    "testing"
)

func strptr(value string) *string { return &value }

func TestIssue412XMLRoundTrip(t *testing.T) {
    input := &Order{
        Id: "42", Note: strptr("optional"), Child: Child{Value: "nested"},
        Items: []string{"one", "two"}, Labels: XMLMap[*string]{"a": strptr("A")},
        Status: Status_NEW,
    }
    for _, contentType := range []string{"application/xml", "text/xml", "application/xml+gzip"} {
        data, err := input.ToByteArray(contentType)
        if err != nil { t.Fatalf("marshal %s: %v", contentType, err) }
        output, err := OrderFromData(data, contentType)
        if err != nil { t.Fatalf("unmarshal %s: %v", contentType, err) }
        if !reflect.DeepEqual(input, output) { t.Fatalf("round trip differs: %#v != %#v", input, output) }
        if contentType == "application/xml" {
            wire := string(data)
            for _, want := range []string{`<order-doc xmlns="urn:orders"`, `order-id="42"`, `<child>`, `<items>one</items>`, `<labels>`, `key="a"`, `<status>new-order</status>`} {
                if !strings.Contains(wire, want) { t.Fatalf("XML %q does not contain %q", wire, want) }
            }
        }
    }
}
'''


@pytest.mark.parametrize("converter,schema", [(convert_avro_schema_to_go, AVRO_SCHEMA), (convert_structure_schema_to_go, STRUCTURE_SCHEMA)])
def test_generated_go_xml_round_trip(converter, schema):
    go = shutil.which("go")
    gofmt = shutil.which("gofmt")
    if not go or not gofmt:
        pytest.skip("Go toolchain is not installed")
    output = _generate(converter, schema, f"runtime-{converter.__name__}")
    package = output / "pkg" / "xmltest"
    (package / "issue412_runtime_test.go").write_text(GO_RUNTIME_TEST, encoding="utf-8")
    subprocess.run([gofmt, "-w", str(output)], check=True, timeout=60)
    subprocess.run([go, "build", "./..."], cwd=output, check=True, timeout=120)
    subprocess.run([go, "test", "./..."], cwd=output, check=True, timeout=120)
