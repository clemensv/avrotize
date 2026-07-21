"""Adversarial runtime tests for generated xsdata XML bindings."""

import gzip
import importlib
import sys
from types import SimpleNamespace
from unittest.mock import patch
import pytest
from xsdata.exceptions import ParserError

from avrotize.avrotopython import convert_avro_schema_to_python
from avrotize.structuretopython import convert_structure_schema_to_python


XML_NAMESPACE = "urn:avrotize:adversarial"
ROOT_NAME = "secure-envelope"
BASE_BODY = (
    "<count>1</count><state>allowed</state><choice>1</choice>"
    "<props><item key=\"a\">5</item></props>"
)


def xml_document(body: str = BASE_BODY, *, namespace: str = XML_NAMESPACE,
                 attributes: str = 'identity="record-1"') -> bytes:
    """Build an adversarial test document with the generated root mapping."""
    return (
        f'<{ROOT_NAME} xmlns="{namespace}" {attributes}>{body}</{ROOT_NAME}>'
    ).encode("utf-8")


@pytest.fixture(scope="module", params=("avro", "structure"))
def generated_xml_model(request, tmp_path_factory):
    """Generate equivalent Avro and Structure models with XML enabled."""
    kind = request.param
    package = f"adversarial_{kind}"
    output_dir = tmp_path_factory.mktemp(package)
    if kind == "avro":
        schema = {
            "type": "record",
            "name": "SecureEnvelope",
            "namespace": "adversarial",
            "xmlns": XML_NAMESPACE,
            "altnames": {"xml": ROOT_NAME},
            "fields": [
                {"name": "id", "type": "string", "xmlkind": "attribute",
                 "altnames": {"xml": "identity"}},
                {"name": "count", "type": "int"},
                {"name": "state", "type": {"type": "enum", "name": "State",
                 "symbols": ["ALLOW", "DENY"],
                 "altenums": {"xml": {"ALLOW": "allowed", "DENY": "denied"}}}},
                {"name": "choice", "type": ["int", "string"]},
                {"name": "props", "type": {"type": "map", "values": "int"}},
                {"name": "note", "type": ["null", "string"], "default": None},
            ],
        }
        convert_avro_schema_to_python(
            schema, str(output_dir), package_name=package, xml_annotation=True)
    else:
        schema = {
            "type": "object",
            "name": "SecureEnvelope",
            "namespace": "adversarial",
            "xmlns": XML_NAMESPACE,
            "altnames": {"xml": ROOT_NAME},
            "properties": {
                "id": {"type": "string", "xmlkind": "attribute",
                       "altnames": {"xml": "identity"}},
                "count": {"type": "integer"},
                "state": {"type": "string", "name": "State", "enum": ["ALLOW", "DENY"],
                          "altenums": {"xml": {"ALLOW": "allowed", "DENY": "denied"}}},
                "choice": {"type": ["integer", "string"]},
                "props": {"type": "map", "values": {"type": "integer"}},
                "note": {"type": "string"},
            },
            "required": ["id", "count", "state", "choice", "props"],
        }
        convert_structure_schema_to_python(
            schema, str(output_dir), package_name=package, xml_annotation=True)

    generated_src = output_dir / "src"
    for generated_file in generated_src.rglob("*.py"):
        compile(generated_file.read_text(encoding="utf-8"), str(generated_file), "exec")

    sys.path.insert(0, str(generated_src))
    try:
        module = importlib.import_module(f"{package}.adversarial.secureenvelope")
        model = module.SecureEnvelope
        valid = model.from_data(xml_document(), "application/xml")
        assert valid.id == "record-1"
        assert valid.count == 1
        assert valid.props == {"a": 5}
        yield SimpleNamespace(kind=kind, model=model, valid_xml=xml_document(), package=package)
    finally:
        sys.path.remove(str(generated_src))
        for module_name in list(sys.modules):
            if module_name == package or module_name.startswith(f"{package}."):
                sys.modules.pop(module_name, None)


@pytest.mark.parametrize("payload", [
    b"<secure-envelope",
    b'<secure-envelope xmlns="urn:avrotize:adversarial"><count>1</secure-envelope>',
    b'<secure-envelope xmlns="urn:avrotize:adversarial">',
])
def test_malformed_or_truncated_xml_raises(generated_xml_model, payload):
    """Malformed and truncated documents are never recovered or partially accepted."""
    with pytest.raises(ParserError):
        generated_xml_model.model.from_data(payload, "application/xml")


def test_corrupt_gzip_raises(generated_xml_model):
    """Invalid and truncated gzip streams fail before XML binding."""
    with pytest.raises(gzip.BadGzipFile):
        generated_xml_model.model.from_data(b"not-gzip", "application/xml+gzip")
    truncated = gzip.compress(generated_xml_model.valid_xml)[:-5]
    with pytest.raises(EOFError):
        generated_xml_model.model.from_data(truncated, "text/xml+gzip")


@pytest.mark.parametrize("declaration", [
    '<!DOCTYPE secure-envelope [<!ENTITY injected "999">]>',
    '<!DOCTYPE secure-envelope SYSTEM "https://example.invalid/attack.dtd">',
    '<!DOCTYPE secure-envelope SYSTEM "file:///definitely/not/read/attack.dtd">',
])
def test_doctype_and_entity_attacks_raise_without_io(generated_xml_model, declaration):
    """All DTD/entity declarations are rejected before file or network resolution."""
    payload = declaration.encode() + xml_document().replace(b"<count>1</count>", b"<count>&injected;</count>")
    with patch("builtins.open", side_effect=AssertionError("file access attempted")), \
         patch("socket.create_connection", side_effect=AssertionError("network access attempted")):
        with pytest.raises(ValueError, match="DTD and entity declarations are not allowed"):
            generated_xml_model.model.from_data(payload, "application/xml")


def test_wrong_namespace_with_same_local_name_raises(generated_xml_model):
    """The root QName, not only its local name, must match the model."""
    with pytest.raises(ParserError):
        generated_xml_model.model.from_data(
            xml_document(namespace="urn:avrotize:wrong"), "application/xml")


@pytest.mark.parametrize("payload", [
    xml_document(attributes=""),
    xml_document(body=BASE_BODY.replace("<count>1</count>", "")),
])
def test_missing_required_attribute_or_element_raises(generated_xml_model, payload):
    """Missing required singleton values fail model construction."""
    with pytest.raises(TypeError):
        generated_xml_model.model.from_data(payload, "application/xml")


def test_duplicate_singleton_raises(generated_xml_model):
    """Strict xsdata binding rejects a second occurrence of a singleton element."""
    body = BASE_BODY.replace("<count>1</count>", "<count>1</count><count>2</count>")
    with pytest.raises(ParserError):
        generated_xml_model.model.from_data(xml_document(body=body), "application/xml")


@pytest.mark.parametrize("payload", [
    xml_document(body=BASE_BODY + "<unexpected>value</unexpected>"),
    xml_document(attributes='identity="record-1" unexpected="value"'),
])
def test_unknown_element_or_attribute_raises(generated_xml_model, payload):
    """Unknown XML content is rejected by the strict xsdata parser configuration."""
    with pytest.raises(ParserError):
        generated_xml_model.model.from_data(payload, "application/xml")


@pytest.mark.parametrize("body", [
    BASE_BODY.replace("<count>1</count>", "<count>not-an-int</count>"),
    BASE_BODY.replace("<state>allowed</state>", "<state>unknown</state>"),
])
def test_invalid_enum_or_scalar_raises(generated_xml_model, body):
    """Converter warnings for invalid scalar and enum values are fatal."""
    with pytest.raises(ParserError):
        generated_xml_model.model.from_data(xml_document(body=body), "application/xml")


def test_payload_size_limit_raises(generated_xml_model):
    """The shared runtime enforces a practical four-MiB parser input limit."""
    oversized_note = "x" * (4 * 1024 * 1024)
    payload = xml_document(body=BASE_BODY + f"<note>{oversized_note}</note>")
    with pytest.raises(ValueError, match="XML payload exceeds"):
        generated_xml_model.model.from_data(payload, "application/xml")


def test_ambiguous_primitive_union_is_schema_ordered(generated_xml_model):
    """The lexical value '1' deliberately resolves to the first union type, integer."""
    result = generated_xml_model.model.from_data(xml_document(), "application/xml")
    assert result.choice == 1
    assert isinstance(result.choice, int)


@pytest.mark.parametrize("map_xml", [
    "<props><item>5</item></props>",
    '<props><item key="a"/></props>',
])
def test_invalid_map_entry_raises(generated_xml_model, map_xml):
    """Map entries require both a key attribute and a typed text value."""
    body = BASE_BODY.replace('<props><item key="a">5</item></props>', map_xml)
    with pytest.raises(TypeError):
        generated_xml_model.model.from_data(xml_document(body=body), "application/xml")


def test_duplicate_map_key_raises(generated_xml_model):
    """Duplicate map keys are rejected instead of being silently overwritten."""
    duplicate = '<props><item key="a">5</item><item key="a">6</item></props>'
    body = BASE_BODY.replace('<props><item key="a">5</item></props>', duplicate)
    with pytest.raises(ValueError, match="Duplicate XML map key"):
        generated_xml_model.model.from_data(xml_document(body=body), "application/xml")