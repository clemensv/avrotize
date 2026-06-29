"""Tests for issue #384: honor JSON Structure altnames.json for wire serialization.

Property `dr_type` + `altnames.json="dr-type"` must serialize `dr-type` on the JSON wire
while keeping the spec-valid identifier `dr_type` as the language member name. Covers all
structure->language direct generators.
"""

import os
import glob
import shutil
import tempfile
import unittest

from avrotize.common import json_wire_name
from avrotize.structuretopython import convert_structure_schema_to_python
from avrotize.structuretocsharp import convert_structure_schema_to_csharp
from avrotize.structuretojava import convert_structure_schema_to_java
from avrotize.structuretots import convert_structure_schema_to_typescript
from avrotize.structuretogo import convert_structure_schema_to_go
from avrotize.structuretorust import convert_structure_schema_to_rust
from avrotize.structuretocpp import convert_structure_schema_to_cpp

SCHEMA = {
    "name": "Telemetry",
    "type": "object",
    "properties": {
        "dr_type": {"type": "int32", "altnames": {"json": "dr-type"}},
        "tlp_requestid": {"type": "string", "altnames": {"json": "tlp-requestid"}},
        "signal_groupid": {"type": "int64", "altnames": {"json": "signal-groupid"}},
        "regular": {"type": "string"},
    },
    "required": ["dr_type", "regular"],
}


def _out(name: str) -> str:
    path = os.path.join(tempfile.gettempdir(), "avrotize_altnames", name)
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    return path


def _read_all(path: str) -> str:
    parts = []
    for f in glob.glob(os.path.join(path, "**", "*"), recursive=True):
        if os.path.isfile(f):
            try:
                parts.append(open(f, encoding="utf-8", errors="ignore").read())
            except OSError:
                pass
    return "\n".join(parts)


class TestJsonWireNameHelper(unittest.TestCase):
    def test_altnames_json_wins(self):
        self.assertEqual(json_wire_name("dr_type", {"altnames": {"json": "dr-type"}}), "dr-type")

    def test_no_altnames_returns_prop_name(self):
        self.assertEqual(json_wire_name("regular", {"type": "string"}), "regular")

    def test_non_dict_schema(self):
        self.assertEqual(json_wire_name("p", "string"), "p")

    def test_other_purpose_ignored(self):
        self.assertEqual(json_wire_name("d", {"altnames": {"sql": "d-x"}}), "d")


class TestStructureAltnamesWire(unittest.TestCase):
    """Each generator must map identifier members to hyphenated wire keys."""

    def test_python_dataclasses_json(self):
        out = _out("py")
        convert_structure_schema_to_python(SCHEMA, out, dataclasses_json_annotation=True)
        c = _read_all(out)
        self.assertIn('field_name="dr-type"', c)
        self.assertIn('field_name="tlp-requestid"', c)
        self.assertIn("dr_type", c)
        self.assertIn('field_name="regular"', c)

    def test_csharp_system_text_and_newtonsoft(self):
        out = _out("cs")
        convert_structure_schema_to_csharp(
            SCHEMA, out, base_namespace="Tel",
            system_text_json_annotation=True, newtonsoft_json_annotation=True)
        c = _read_all(out)
        self.assertIn('JsonPropertyName("dr-type")', c)
        self.assertIn('JsonProperty("tlp-requestid")', c)

    def test_java_jackson(self):
        out = _out("java")
        convert_structure_schema_to_java(SCHEMA, out, jackson_annotation=True)
        c = _read_all(out)
        self.assertIn('@JsonProperty("dr-type")', c)
        self.assertIn('@JsonProperty("signal-groupid")', c)

    def test_typescript_typedjson(self):
        out = _out("ts")
        convert_structure_schema_to_typescript(SCHEMA, out, typedjson_annotation=True)
        c = _read_all(out)
        self.assertIn("name: 'dr-type'", c)
        self.assertIn("name: 'tlp-requestid'", c)

    def test_go_json_tags(self):
        out = _out("go")
        convert_structure_schema_to_go(SCHEMA, out, json_annotation=True)
        c = _read_all(out)
        self.assertIn('json:"dr-type', c)
        self.assertIn('json:"signal-groupid', c)

    def test_rust_serde_rename(self):
        out = _out("rust")
        convert_structure_schema_to_rust(SCHEMA, out, serde_annotation=True)
        c = _read_all(out)
        self.assertIn('rename = "dr-type"', c)
        self.assertIn('rename = "tlp-requestid"', c)

    def test_cpp_json_keys(self):
        out = _out("cpp")
        convert_structure_schema_to_cpp(SCHEMA, out, namespace="tel", json_annotation=True)
        c = _read_all(out)
        self.assertIn('"dr-type"', c)
        self.assertIn('"tlp-requestid"', c)


if __name__ == "__main__":
    unittest.main()
