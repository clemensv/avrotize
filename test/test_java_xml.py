"""Focused XML serialization tests for the Java generators."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from avrotize.avrotojava import convert_avro_schema_to_java
from avrotize.structuretojava import convert_structure_schema_to_java


XML_NAMESPACE = "urn:test:orders"


def _avro_schema():
    return {
        "type": "record",
        "name": "Order",
        "namespace": "example.model",
        "xmlns": XML_NAMESPACE,
        "altnames": {"xml": "purchase-order"},
        "fields": [
            {"name": "id", "type": "string", "xmlkind": "attribute",
             "altnames": {"xml": "order-id"}},
            {"name": "quantity", "type": "int"},
            {"name": "note", "type": ["null", "string"], "default": None},
            {"name": "status", "altnames": {"xml": "state"}, "type": {
                "type": "enum", "name": "Status", "namespace": "example.model",
                "xmlns": XML_NAMESPACE, "symbols": ["IN_PROGRESS", "DONE"],
                "altenums": {"xml": {"IN_PROGRESS": "working", "DONE": "complete"}},
            }},
            {"name": "child", "type": {
                "type": "record", "name": "Child", "namespace": "example.model",
                "xmlns": XML_NAMESPACE, "fields": [
                    {"name": "code", "type": "string", "xmlkind": "attribute"},
                    {"name": "value", "type": "string"},
                ],
            }},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {"name": "metadata", "type": {"type": "map", "values": "string"}},
            {"name": "choice", "type": ["string", "int"]},
        ],
    }


def _structure_schema():
    return {
        "type": "object",
        "name": "Order",
        "namespace": "example.model",
        "xmlns": XML_NAMESPACE,
        "altnames": {"xml": "purchase-order"},
        "required": ["id", "quantity", "status", "child", "metadata", "choice"],
        "properties": {
            "id": {"type": "string", "xmlkind": "attribute",
                   "altnames": {"xml": "order-id"}},
            "quantity": {"type": "integer"},
            "note": {"type": ["null", "string"]},
            "status": {
                "type": "string", "name": "Status", "xmlns": XML_NAMESPACE,
                "altnames": {"xml": "state"}, "enum": ["IN_PROGRESS", "DONE"],
                "altenums": {"xml": {"IN_PROGRESS": "working", "DONE": "complete"}},
            },
            "child": {
                "type": "object", "name": "Child", "xmlns": XML_NAMESPACE,
                "properties": {
                    "code": {"type": "string", "xmlkind": "attribute"},
                    "value": {"type": "string"},
                },
            },
            "tags": {"type": "array", "items": {"type": "string"}},
            "metadata": {"type": "map", "values": {"type": "string"}},
            "choice": {"type": ["string", "integer"]},
        },
    }


def _driver(package: str, enum_value: str, union_value: str) -> str:
    return f"""package {package};
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public class XmlRoundTrip {{
    @FunctionalInterface
    private interface ThrowingAction {{
        void run() throws Exception;
    }}

    private static Throwable expectFailure(
            String name, String expectedMessage, ThrowingAction action) throws Exception {{
        try {{
            action.run();
        }} catch (Throwable failure) {{
            String message = failure.getMessage();
            if (message == null || message.isBlank()) {{
                throw new AssertionError(name + " did not provide an explicit error", failure);
            }}
            if (!message.toLowerCase().contains(expectedMessage.toLowerCase())) {{
                throw new AssertionError(
                    name + " returned an unclear error: " + message, failure);
            }}
            return failure;
        }}
        throw new AssertionError(name + " unexpectedly succeeded");
    }}

    public static void main(String[] args) throws Exception {{
        Order value = new Order();
        value.setId("A1");
        value.setQuantity(3);
        value.setNote("optional");
        value.setStatus(Status.{enum_value});
        Child child = new Child();
        child.setCode("C1");
        child.setValue("nested");
        value.setChild(child);
        value.setTags(Arrays.asList("one", "two"));
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("region", "west");
        value.setMetadata(metadata);
        value.setChoice({union_value});

        byte[] xmlBytes = value.toByteArray("application/xml");
        String xml = new String(xmlBytes, StandardCharsets.UTF_8);
        if (!xml.contains("purchase-order") || !xml.contains("{XML_NAMESPACE}")
                || !xml.contains("order-id=\\\"A1\\\"") || !xml.contains("working")
                || !xml.contains("nested") || !xml.contains("west")) {{
            throw new AssertionError(xml);
        }}
        if (!value.equals(Order.fromData(xmlBytes, "application/xml"))) {{
            throw new AssertionError("XML round-trip failed: " + xml);
        }}
        byte[] gzip = value.toByteArray("text/xml+gzip");
        if (!value.equals(Order.fromData(gzip, "text/xml+gzip"))) {{
            throw new AssertionError("gzip XML round-trip failed");
        }}

        expectFailure("malformed XML", "XML",
            () -> Order.fromData(xml.substring(0, xml.length() / 2), "application/xml"));
        expectFailure("corrupt gzip", "gzip",
            () -> Order.fromData(new byte[] {{1, 2, 3, 4}}, "application/xml+gzip"));
        expectFailure("wrong root namespace", "Unexpected XML root",
            () -> Order.fromData(xml.replace("{XML_NAMESPACE}", "urn:wrong"), "application/xml"));
        expectFailure("missing required field", "Missing required XML field",
            () -> Order.fromData(
                xml.replaceFirst("<state>.*?</state>", ""), "application/xml"));
        expectFailure("duplicate singleton", "Duplicate XML field",
            () -> Order.fromData(
                xml.replace("<state>working</state>",
                    "<state>working</state><state>complete</state>"),
                "application/xml"));
        expectFailure("unknown attribute", "Unknown XML attribute",
            () -> Order.fromData(
                xml.replaceFirst("<purchase-order",
                    "<purchase-order surprise=\\\"x\\\""),
                "application/xml"));
        expectFailure("unknown element", "Unknown XML element",
            () -> Order.fromData(
                xml.replace("</purchase-order>",
                    "<surprise>value</surprise></purchase-order>"),
                "application/xml"));
        expectFailure("invalid enum", "Invalid XML value",
            () -> Order.fromData(
                xml.replace("<state>working</state>", "<state>invalid</state>"),
                "application/xml"));
        expectFailure("invalid scalar", "Invalid XML value",
            () -> Order.fromData(
                xml.replace("<quantity>3</quantity>", "<quantity>NaN</quantity>"),
                "application/xml"));

        int choiceEnd = xml.indexOf("</choice>");
        String ambiguousUnion = xml.substring(0, choiceEnd)
            + "<extra>7</extra>" + xml.substring(choiceEnd);
        expectFailure("ambiguous union", "Ambiguous XML union",
            () -> Order.fromData(ambiguousUnion, "application/xml"));

        expectFailure("duplicate map key", "Duplicate XML map key",
            () -> Order.fromData(
                xml.replace("</metadata>",
                    "<region>east</region></metadata>"),
                "application/xml"));
        expectFailure("malformed scalar map", "Malformed scalar XML map entry",
            () -> Order.fromData(
                xml.replace("<region>west</region>",
                    "<region><nested>west</nested></region>"),
                "application/xml"));
        expectFailure("duplicate map field", "Duplicate XML field",
            () -> Order.fromData(
                xml.replace("</purchase-order>",
                    "<metadata><other>value</other></metadata></purchase-order>"),
                "application/xml"));

        String deep = xml.replace("<note>optional</note>",
            "<note>" + "<n>".repeat(101) + "x" + "</n>".repeat(101) + "</note>");
        expectFailure("excessive XML depth", "depth limit",
            () -> Order.fromData(deep, "application/xml"));
        String oversized = xml.replace("<note>optional</note>",
            "<note>" + "x".repeat(1_100_000) + "</note>");
        expectFailure("excessive XML size", "size limit",
            () -> Order.fromData(oversized, "application/xml"));

        int declarationEnd = xml.indexOf("?>") + 2;
        Path secretFile = Files.createTempFile("avrotize-xxe", ".txt");
        String secret = "XXE_SECRET_MUST_NOT_BE_READ";
        Files.writeString(secretFile, secret, StandardCharsets.UTF_8);
        try {{
            String fileDoctype = "<!DOCTYPE purchase-order [<!ENTITY xxe SYSTEM \\\""
                + secretFile.toUri() + "\\\">]>";
            String fileAttack = xml.substring(0, declarationEnd) + fileDoctype
                + xml.substring(declarationEnd)
                    .replace("<note>optional</note>", "<note>&xxe;</note>");
            Throwable fileFailure = expectFailure(
                "XXE file entity", "unsafe XML",
                () -> Order.fromData(fileAttack, "application/xml"));
            if (String.valueOf(fileFailure.getMessage()).contains(secret)) {{
                throw new AssertionError("XXE file content leaked");
            }}

            String networkDoctype =
                "<!DOCTYPE purchase-order [<!ENTITY xxe SYSTEM "
                + "\\\"http://127.0.0.1:9/xxe\\\">]>";
            String networkAttack = xml.substring(0, declarationEnd) + networkDoctype
                + xml.substring(declarationEnd)
                    .replace("<note>optional</note>", "<note>&xxe;</note>");
            expectFailure("XXE network entity", "unsafe XML",
                () -> Order.fromData(networkAttack, "application/xml"));

            String entityAttack = xml.substring(0, declarationEnd)
                + "<!DOCTYPE purchase-order ["
                + "<!ENTITY a \\\"1234567890\\\">"
                + "<!ENTITY b \\\"&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;\\\">"
                + "]>" + xml.substring(declarationEnd)
                    .replace("<note>optional</note>", "<note>&b;</note>");
            expectFailure("DOCTYPE entity expansion", "unsafe XML",
                () -> Order.fromData(entityAttack, "application/xml"));
        }} finally {{
            Files.deleteIfExists(secretFile);
        }}

        // Intentional stable Jackson tolerance: optional elements may be absent,
        // known elements need not be schema-ordered, and collection elements repeat.
        String tolerantXml = xml.replace("<note>optional</note>", "")
            .replace("<quantity>3</quantity>", "")
            .replace("<state>working</state>",
                "<state>working</state><quantity>3</quantity>");
        Order withoutOptional = Order.fromData(
            tolerantXml, "application/xml");
        if (withoutOptional.getNote() != null || withoutOptional.getTags().size() != 2) {{
            throw new AssertionError("stable XML tolerance changed");
        }}
    }}
}}
"""


def _assert_xml_metadata(project: Path, package_path: Path):
    source_root = project / "src" / "main" / "java"
    order = (source_root / package_path / "Order.java").read_text("utf-8")
    child = (source_root / package_path / "Child.java").read_text("utf-8")
    status = (source_root / package_path / "Status.java").read_text("utf-8")
    package_info = (source_root / package_path / "package-info.java").read_text("utf-8")
    pom = (project / "pom.xml").read_text("utf-8")
    assert '@XmlRootElement(name = "purchase-order", namespace = "urn:test:orders")' in order
    assert '@XmlAttribute(name = "order-id")' in order
    assert '@XmlElement(name = "state", namespace = "urn:test:orders")' in order
    assert '@XmlEnumValue("working")' in status
    assert 'namespace = "urn:test:orders"' in package_info
    assert "jackson-dataformat-xml" in pom
    assert "jackson-module-jakarta-xmlbind-annotations" in pom
    assert "jakarta.xml.bind-api" in pom

    support_files = list(source_root.glob("**/AvrotizeXmlSupport.java"))
    assert len(support_files) == 1
    support = support_files[0].read_text("utf-8")
    assert "public static final XmlMapper MAPPER = createMapper();" in support
    assert support.count("new XmlMapper(") == 1
    assert "XMLInputFactory.SUPPORT_DTD, false" in support
    assert "isSupportingExternalEntities" in support
    assert "disallow-doctype-decl" in support
    assert "ACCESS_EXTERNAL_DTD" in support
    assert "MAX_XML_DEPTH" in support
    assert "MAX_XML_BYTES" in support
    assert "FAIL_ON_UNKNOWN_PROPERTIES" in support
    assert "AvrotizeXmlSupport.MAPPER" in order
    assert "AvrotizeXmlSupport.MAPPER" in child
    assert "createXmlMapper" not in order + child
    assert "new XmlMapper(" not in order + child
    all_sources = "".join(path.read_text("utf-8") for path in source_root.glob("**/*.java"))
    assert all_sources.count("new XmlMapper(") == 1


def test_java_xml_public_flags_and_annotations(tmp_path):
    commands = json.loads(
        (Path(__file__).parents[1] / "avrotize" / "commands.json").read_text("utf-8"))
    for command_name in ("a2java", "s2java"):
        command = next(command for command in commands if command["command"] == command_name)
        assert command["function"]["args"]["xml_annotation"] == "args.xml_annotation"
        assert any(arg["name"] == "--xml-annotation" for arg in command["args"])

    avro_project = tmp_path / "avro"
    structure_project = tmp_path / "structure"
    convert_avro_schema_to_java(
        _avro_schema(), str(avro_project), package_name="test.xml.avro",
        jackson_annotation=True, avro_annotation=True, xml_annotation=True)
    convert_structure_schema_to_java(
        _structure_schema(), str(structure_project), package_name="test.xml.structure",
        jackson_annotation=True, xml_annotation=True)
    _assert_xml_metadata(avro_project, Path("test/xml/avro/example/model"))
    _assert_xml_metadata(structure_project, Path("test/xml/structure/example/model"))

    plain_project = tmp_path / "plain"
    convert_avro_schema_to_java(
        _avro_schema(), str(plain_project), package_name="test.xml.plain")
    plain_order = (
        plain_project / "src" / "main" / "java" / "test" / "xml" / "plain"
        / "example" / "model" / "Order.java").read_text("utf-8")
    assert "XmlRootElement" not in plain_order
    assert not list((plain_project / "src" / "main" / "java").glob(
        "**/AvrotizeXmlSupport.java"))


@pytest.mark.skipif(shutil.which("mvn") is None or shutil.which("java") is None,
                    reason="Maven and Java are required for generated-code round-trip")
@pytest.mark.parametrize(
    ("kind", "package", "enum_value", "union_value"),
    [
        ("avro", "test.xml.avro.example.model", "IN_PROGRESS",
         'new OrderChoiceUnion("selected")'),
        ("structure", "test.xml.structure.example.model", "InProgress", '"selected"'),
    ],
)
def test_generated_java_xml_round_trip(tmp_path, kind, package, enum_value, union_value):
    project = tmp_path / kind
    if kind == "avro":
        convert_avro_schema_to_java(
            _avro_schema(), str(project), package_name="test.xml.avro",
            jackson_annotation=True, avro_annotation=True, xml_annotation=True)
    else:
        convert_structure_schema_to_java(
            _structure_schema(), str(project), package_name="test.xml.structure",
            jackson_annotation=True, xml_annotation=True)

    package_path = Path(*package.split("."))
    driver = project / "src" / "main" / "java" / package_path / "XmlRoundTrip.java"
    driver.write_text(_driver(package, enum_value, union_value), encoding="utf-8")
    command = (
        "mvn package org.codehaus.mojo:exec-maven-plugin:3.5.0:java "
        f"-Dexec.mainClass={package}.XmlRoundTrip -B -q"
    )
    result = subprocess.run(
        command, cwd=project, capture_output=True, text=True, timeout=600,
        shell=True)
    assert result.returncode == 0, result.stdout + result.stderr
