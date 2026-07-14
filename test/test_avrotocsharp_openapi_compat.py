
import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

from avrotize.avrotocsharp import convert_avro_to_csharp
from avrotize.structuretocsharp import convert_structure_to_csharp


class TestCSharpOpenApiGeneratorCompat(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[1]
        cls.work = cls.root / "test" / "_openapi_compat_tmp"
        if cls.work.exists():
            shutil.rmtree(cls.work)
        cls.work.mkdir(parents=True)
        cls.avro_schema = cls.work / "link.avsc"
        cls.structure_schema = cls.work / "link.struct.json"
        cls.avro_schema.write_text(json.dumps({
            "type": "record",
            "name": "Link",
            "namespace": "demo",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "bitly_url", "type": ["null", "string"], "default": None},
                {"name": "clicks", "type": "int"},
                {"name": "kind", "type": {"type": "enum", "name": "LinkKind", "symbols": ["Short", "Long"]}},
                {"name": "child", "type": ["null", {"type": "record", "name": "Child", "fields": [{"name": "name", "type": "string"}]}], "default": None},
                {"name": "tags", "type": {"type": "array", "items": "string"}}
            ]
        }), encoding="utf-8")
        cls.structure_schema.write_text(json.dumps({
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "LinkStruct",
            "namespace": "demo",
            "properties": {
                "id": {"type": "string"},
                "bitly_url": {"type": "string"},
                "clicks": {"type": "int32"},
                "tags": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["id", "clicks"]
        }), encoding="utf-8")

    @classmethod
    def tearDownClass(cls):
        if cls.work.exists():
            shutil.rmtree(cls.work)

    def out_dir(self, name):
        path = self.work / name
        if path.exists():
            shutil.rmtree(path)
        return path

    def read_file_named(self, directory, file_name):
        matches = list(Path(directory).rglob(file_name))
        self.assertTrue(matches, f"{file_name} not found under {directory}")
        return matches[0].read_text(encoding="utf-8")

    def generate_avro(self, name="avro", compat=False):
        out = self.out_dir(name)
        convert_avro_to_csharp(str(self.avro_schema), str(out), base_namespace="Compat", openapi_generator_compat=compat)
        return out, self.read_file_named(out, "Link.cs")

    def generate_structure(self, name="struct", compat=False):
        out = self.out_dir(name)
        convert_structure_to_csharp(str(self.structure_schema), str(out), base_namespace="Compat", project_name="CompatStruct", openapi_generator_compat=compat)
        return out, self.read_file_named(out, "LinkStruct.cs")

    def test_default_avro_has_no_compat_markers(self):
        _, text = self.generate_avro("default-avro", compat=False)
        self.assertNotIn("Option<", text)
        self.assertNotIn("IValidatableObject", text)
        self.assertNotIn("JsonConverter<", text)

    def test_default_avro_generation_is_stable(self):
        _, first = self.generate_avro("default-avro-a", compat=False)
        _, second = self.generate_avro("default-avro-b", compat=False)
        self.assertEqual(first, second)

    def test_avro_optional_property_uses_option_dual_accessors(self):
        _, text = self.generate_avro("compat-avro-option", compat=True)
        self.assertIn("public Option<string?> BitlyUrlOption { get; private set; }", text)
        self.assertIn("public string? BitlyUrl { get => BitlyUrlOption; set => BitlyUrlOption = new(value); }", text)

    def test_avro_json_property_names_preserve_wire_names(self):
        _, text = self.generate_avro("compat-avro-json", compat=True)
        self.assertIn('[System.Text.Json.Serialization.JsonPropertyName("bitly_url")]', text)
        self.assertIn('[System.Text.Json.Serialization.JsonPropertyName("id")]', text)

    def test_avro_converter_and_validation_are_emitted(self):
        _, text = self.generate_avro("compat-avro-converter", compat=True)
        self.assertIn("public class LinkJsonConverter : System.Text.Json.Serialization.JsonConverter<Link>", text)
        self.assertIn("IValidatableObject", text)
        self.assertIn("IEnumerable<System.ComponentModel.DataAnnotations.ValidationResult> Validate", text)

    def test_avro_constructor_uses_option_for_optional_and_plain_for_required(self):
        _, text = self.generate_avro("compat-avro-ctor", compat=True)
        self.assertIn("public Link(string id, Option<string?> bitlyUrl, int clicks", text)
        self.assertIn("BitlyUrlOption = bitlyUrl;", text)
        self.assertIn("Id = id;", text)

    def test_avro_nested_array_and_enum_fields_are_present(self):
        _, text = self.generate_avro("compat-avro-complex", compat=True)
        self.assertIn("public global::Compat.Demo.LinkKind Kind { get; set; }", text)
        self.assertIn("public Option<global::Compat.Demo.Child?> ChildOption { get; private set; }", text)
        self.assertIn("public List<string> Tags { get; set; } = new();", text)

    def test_avro_option_struct_is_emitted(self):
        out, _ = self.generate_avro("compat-avro-option-file", compat=True)
        option = self.read_file_named(out, "Option.cs")
        self.assertIn("public readonly struct Option<T>", option)
        self.assertIn("public bool IsSet { get; }", option)
        self.assertIn("public static implicit operator Option<T>(T value) => new(value);", option)

    def test_default_structure_has_no_compat_markers(self):
        _, text = self.generate_structure("default-struct", compat=False)
        self.assertNotIn("Option<", text)
        self.assertNotIn("IValidatableObject", text)
        self.assertNotIn("JsonConverter<LinkStruct>", text)

    def test_structure_compat_option_and_converter_are_emitted(self):
        out, text = self.generate_structure("compat-struct", compat=True)
        self.assertIn("public Option<string?> BitlyUrlOption { get; private set; }", text)
        self.assertIn("public class LinkStructJsonConverter : System.Text.Json.Serialization.JsonConverter<LinkStruct>", text)
        self.assertIn("IValidatableObject", text)
        self.assertIn("public readonly struct Option<T>", self.read_file_named(out, "Option.cs"))

    def test_cli_a2cs_openapi_generator_compat(self):
        out = self.out_dir("cli-a2cs")
        subprocess.check_call([sys.executable, "-m", "avrotize", "a2cs", str(self.avro_schema), "--out", str(out), "--namespace", "Compat", "--openapi-generator-compat"], cwd=self.root)
        self.assertIn("Option<string?> BitlyUrlOption", self.read_file_named(out, "Link.cs"))

    def test_cli_s2cs_openapi_generator_compat(self):
        out = self.out_dir("cli-s2cs")
        subprocess.check_call([sys.executable, "-m", "avrotize", "s2cs", str(self.structure_schema), "--out", str(out), "--namespace", "Compat", "--project-name", "CompatStruct", "--openapi-generator-compat"], cwd=self.root)
        self.assertIn("Option<string?> BitlyUrlOption", self.read_file_named(out, "LinkStruct.cs"))

    @unittest.skipUnless(shutil.which("dotnet"), "dotnet is not available")
    def test_dotnet_build_avro_compat_output(self):
        out, _ = self.generate_avro("compat-avro-build", compat=True)
        subprocess.check_call(["dotnet", "build", "--nologo", "--verbosity", "minimal"], cwd=out, timeout=120)


if __name__ == "__main__":
    unittest.main()
