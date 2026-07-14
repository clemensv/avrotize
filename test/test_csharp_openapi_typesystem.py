"""Full type-system range and adversarial coverage for C# OpenAPI compat output.

This suite exercises Avro -> C# and JSON Structure -> C# code generation in
both default and ``--openapi-generator-compat`` modes. It intentionally checks
source text because these converters are one-way code generators.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from avrotize.avrotocsharp import convert_avro_to_csharp
from avrotize.structuretocsharp import convert_structure_to_csharp

ROOT = Path(__file__).resolve().parents[1]
BASE_NS = "Compat"
NS = "demo.types"
QUAL_NS = "Compat.Demo.Types"


class CSharpOpenApiTypeSystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    # -- helpers ---------------------------------------------------------------

    def write_json(self, name: str, doc) -> Path:
        path = self.tmp / name
        path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        return path

    def read_file_named(self, directory: Path, file_name: str) -> str:
        matches = sorted(Path(directory).rglob(file_name))
        self.assertTrue(matches, f"{file_name} not found under {directory}")
        return matches[0].read_text(encoding="utf-8")

    def read_bytes_by_relative_path(self, directory: Path, glob: str = "*.cs") -> dict[str, bytes]:
        return {
            str(path.relative_to(directory)): path.read_bytes()
            for path in sorted(directory.rglob(glob))
        }

    def generate_avro(self, schema, name: str, compat: bool = False) -> Path:
        src = self.write_json(f"{name}.avsc", schema)
        out = self.tmp / f"{name}-out"
        convert_avro_to_csharp(
            str(src),
            str(out),
            base_namespace=BASE_NS,
            project_name="CompatAvroTypes",
            openapi_generator_compat=compat,
        )
        return out

    def generate_structure(self, schema, name: str, compat: bool = False) -> Path:
        src = self.write_json(f"{name}.struct.json", schema)
        out = self.tmp / f"{name}-out"
        convert_structure_to_csharp(
            str(src),
            str(out),
            base_namespace=BASE_NS,
            project_name="CompatStructTypes",
            openapi_generator_compat=compat,
        )
        return out

    def assert_property(self, text: str, csharp_type: str, name: str, *, required: bool = False) -> None:
        modifier = "required " if required else ""
        pattern = rf"public {re.escape(modifier + csharp_type)} {re.escape(name)} \{{ get; set; \}}"
        self.assertRegex(text, pattern)

    def full_avro_schema(self):
        return {
            "type": "record",
            "name": "FullAvro",
            "namespace": NS,
            "fields": [
                {"name": "null_field", "type": "null"},
                {"name": "bool_field", "type": "boolean"},
                {"name": "int_field", "type": "int"},
                {"name": "long_field", "type": "long"},
                {"name": "float_field", "type": "float"},
                {"name": "double_field", "type": "double"},
                {"name": "bytes_field", "type": "bytes"},
                {"name": "string_field", "type": "string"},
                {"name": "decimal_bytes", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
                {"name": "uuid_string", "type": {"type": "string", "logicalType": "uuid"}},
                {"name": "date_int", "type": {"type": "int", "logicalType": "date"}},
                {"name": "time_millis_int", "type": {"type": "int", "logicalType": "time-millis"}},
                {"name": "time_micros_long", "type": {"type": "long", "logicalType": "time-micros"}},
                {"name": "ts_millis_long", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "ts_micros_long", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                {"name": "duration_fixed", "type": {"type": "fixed", "name": "Duration12", "size": 12, "logicalType": "duration"}},
                {"name": "enum_field", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN_BLUE", "BLUE"]}},
                {"name": "fixed_field", "type": {"type": "fixed", "name": "Fixed16", "size": 16}},
                {"name": "array_field", "type": {"type": "array", "items": "string"}},
                {"name": "map_field", "type": {"type": "map", "values": "long"}},
                {"name": "nested_record", "type": {"type": "record", "name": "NestedRecord", "fields": [{"name": "value", "type": "string"}]}},
                {"name": "nullable_string", "type": ["null", "string"], "default": None},
                {"name": "multi_union", "type": ["string", "int"]},
                {"name": "optional_int", "type": ["null", "int"], "default": None},
            ],
        }

    def full_structure_schema(self):
        required = [
            "string_field", "boolean_field", "int8_field", "uint8_field", "int16_field", "uint16_field",
            "int32_field", "uint32_field", "int64_field", "uint64_field", "float_field", "double_field",
            "decimal_field", "bytes_field", "uuid_field", "date_field", "time_field", "datetime_field",
            "duration_field", "array_field", "set_field", "map_field", "object_field", "choice_field",
        ]
        return {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "FullStruct",
            "namespace": NS,
            "properties": {
                "string_field": {"type": "string"},
                "boolean_field": {"type": "boolean"},
                "int8_field": {"type": "int8"},
                "uint8_field": {"type": "uint8"},
                "int16_field": {"type": "int16"},
                "uint16_field": {"type": "uint16"},
                "int32_field": {"type": "int32"},
                "uint32_field": {"type": "uint32"},
                "int64_field": {"type": "int64"},
                "uint64_field": {"type": "uint64"},
                "float_field": {"type": "float"},
                "double_field": {"type": "double"},
                "decimal_field": {"type": "decimal"},
                "bytes_field": {"type": "binary"},
                "uuid_field": {"type": "uuid"},
                "date_field": {"type": "date"},
                "time_field": {"type": "time"},
                "datetime_field": {"type": "datetime"},
                "duration_field": {"type": "duration"},
                "array_field": {"type": "array", "items": {"type": "string"}},
                "set_field": {"type": "set", "items": {"type": "int32"}},
                "map_field": {"type": "map", "values": {"type": "int64"}},
                "object_field": {"type": "object", "name": "InnerObject", "properties": {"value": {"type": "string"}}, "required": ["value"]},
                "choice_field": {"type": "choice", "name": "MyChoice", "choices": {"text": {"type": "string"}, "count": {"type": "int32"}}},
                "optional_string": {"type": "string"},
                "snake_case": {"type": "string"},
            },
            "required": required,
        }

    # -- Avro -> C#: supported default and compat range -----------------------

    def test_avro_full_range_default_supported_mappings(self):
        out = self.generate_avro(self.full_avro_schema(), "avro-default")
        text = self.read_file_named(out, "FullAvro.cs")
        expected = {
            "bool_field": "bool",
            "int_field": "int",
            "long_field": "long",
            "float_field": "float",
            "double_field": "double",
            "bytes_field": "byte[]",
            "string_field": "string",
            "enum_field": f"global::{QUAL_NS}.Color",
            "array_field": "List<string>",
            "map_field": "Dictionary<string, long>",
            "nested_record": f"global::{QUAL_NS}.NestedRecord",
            "nullable_string": "string?",
            "multi_union": "FullAvro.MultiUnionUnion",
            "optional_int": "int?",
        }
        for name, csharp_type in expected.items():
            with self.subTest(field=name):
                self.assert_property(text, csharp_type, name)
        self.assertIn("public sealed class MultiUnionUnion", self.read_file_named(out, "FullAvro.MultiUnionUnion.cs"))
        self.assertIn("public enum Color", self.read_file_named(out, "Color.cs"))
        self.assertIn("public partial class NestedRecord", self.read_file_named(out, "NestedRecord.cs"))

    def test_avro_full_range_compat_markers_and_supported_mappings(self):
        out = self.generate_avro(self.full_avro_schema(), "avro-compat", compat=True)
        text = self.read_file_named(out, "FullAvro.cs")
        for name, csharp_type in {
            "BoolField": "bool",
            "IntField": "int",
            "LongField": "long",
            "FloatField": "float",
            "DoubleField": "double",
            "BytesField": "byte[]",
            "StringField": "string",
            "EnumField": f"global::{QUAL_NS}.Color",
            "ArrayField": "List<string>",
            "MapField": "Dictionary<string, long>",
            "NestedRecord": f"global::{QUAL_NS}.NestedRecord",
            "MultiUnion": "FullAvro.MultiUnionUnion",
        }.items():
            with self.subTest(field=name):
                self.assert_property(text, csharp_type, name)
        self.assertIn("public partial class FullAvro : System.ComponentModel.DataAnnotations.IValidatableObject", text)
        self.assertIn('[System.Text.Json.Serialization.JsonPropertyName("nullable_string")]', text)
        self.assertIn("public Option<string?> NullableStringOption { get; private set; }", text)
        self.assertIn("public string? NullableString { get => NullableStringOption; set => NullableStringOption = new(value); }", text)
        self.assertIn("public Option<int?> OptionalIntOption { get; private set; }", text)
        self.assertIn("Option<string?> nullableString", text)
        self.assertIn("Option<int?> optionalInt", text)
        self.assertIn("public class FullAvroJsonConverter : System.Text.Json.Serialization.JsonConverter<FullAvro>", text)
        self.assertIn("IEnumerable<System.ComponentModel.DataAnnotations.ValidationResult> Validate", text)
        option = self.read_file_named(out, "Option.cs")
        self.assertIn("public readonly struct Option<T>", option)
        self.assertIn("public bool IsSet { get; }", option)
        self.assertIn("public static implicit operator Option<T>(T value) => new(value);", option)

    def test_default_avro_output_has_no_openapi_compat_markers(self):
        out = self.generate_avro(self.full_avro_schema(), "avro-default-negative")
        text = self.read_file_named(out, "FullAvro.cs")
        self.assertNotIn("Option<", text)
        self.assertNotIn("IValidatableObject", text)
        self.assertNotIn("FullAvroJsonConverter", text)
        self.assertNotIn("JsonPropertyName", text)

    # -- JSON Structure -> C#: full default and compat range -------------------

    def test_structure_full_range_default_mappings(self):
        out = self.generate_structure(self.full_structure_schema(), "struct-default")
        text = self.read_file_named(out, "FullStruct.cs")
        expected = {
            "string_field": "string",
            "boolean_field": "bool",
            "int8_field": "sbyte",
            "uint8_field": "byte",
            "int16_field": "short",
            "uint16_field": "ushort",
            "int32_field": "int",
            "uint32_field": "uint",
            "int64_field": "long",
            "uint64_field": "ulong",
            "float_field": "float",
            "double_field": "double",
            "decimal_field": "decimal",
            "bytes_field": "byte[]",
            "uuid_field": "Guid",
            "date_field": "DateOnly",
            "time_field": "TimeOnly",
            "datetime_field": "DateTimeOffset",
            "duration_field": "TimeSpan",
            "array_field": "List<string>",
            "set_field": "HashSet<int>",
            "map_field": "Dictionary<string, long>",
            "object_field": f"global::{QUAL_NS}.InnerObject",
            "choice_field": f"global::{QUAL_NS}.MyChoice",
        }
        for name, csharp_type in expected.items():
            with self.subTest(field=name):
                self.assert_property(text, csharp_type, name, required=True)
        self.assert_property(text, "string?", "optional_string")
        self.assert_property(text, "string?", "snake_case")
        self.assertIn("public partial class InnerObject", self.read_file_named(out, "InnerObject.cs"))
        self.assertIn("public partial class MyChoice", self.read_file_named(out, "MyChoice.cs"))

    def test_structure_full_range_compat_markers_and_mappings(self):
        out = self.generate_structure(self.full_structure_schema(), "struct-compat", compat=True)
        text = self.read_file_named(out, "FullStruct.cs")
        expected = {
            "StringField": "string",
            "BooleanField": "bool",
            "Int8Field": "sbyte",
            "Uint8Field": "byte",
            "Int16Field": "short",
            "Uint16Field": "ushort",
            "Int32Field": "int",
            "Uint32Field": "uint",
            "Int64Field": "long",
            "Uint64Field": "ulong",
            "FloatField": "float",
            "DoubleField": "double",
            "DecimalField": "decimal",
            "BytesField": "byte[]",
            "UuidField": "Guid",
            "DateField": "DateOnly",
            "TimeField": "TimeOnly",
            "DatetimeField": "DateTimeOffset",
            "DurationField": "TimeSpan",
            "ArrayField": "List<string>",
            "SetField": "HashSet<int>",
            "MapField": "Dictionary<string, long>",
            "ObjectField": f"global::{QUAL_NS}.InnerObject",
            "ChoiceField": f"global::{QUAL_NS}.MyChoice",
        }
        for name, csharp_type in expected.items():
            with self.subTest(field=name):
                self.assert_property(text, csharp_type, name)
        self.assertIn("public partial class FullStruct : System.ComponentModel.DataAnnotations.IValidatableObject", text)
        self.assertIn('[System.Text.Json.Serialization.JsonPropertyName("snake_case")]', text)
        self.assertIn("public Option<string?> OptionalStringOption { get; private set; }", text)
        self.assertIn("public Option<string?> SnakeCaseOption { get; private set; }", text)
        self.assertIn("Option<string?> optionalString", text)
        self.assertIn("Option<string?> snakeCase", text)
        self.assertIn("public class FullStructJsonConverter : System.Text.Json.Serialization.JsonConverter<FullStruct>", text)
        self.assertIn("IEnumerable<System.ComponentModel.DataAnnotations.ValidationResult> Validate", text)
        self.assertIn("public readonly struct Option<T>", self.read_file_named(out, "Option.cs"))

    def test_default_structure_output_has_no_openapi_compat_markers_on_model(self):
        out = self.generate_structure(self.full_structure_schema(), "struct-default-negative")
        text = self.read_file_named(out, "FullStruct.cs")
        self.assertNotIn("Option<", text)
        self.assertNotIn("IValidatableObject", text)
        self.assertNotIn("FullStructJsonConverter", text)

    # -- determinism -----------------------------------------------------------

    def test_openapi_compat_csharp_sources_are_deterministic(self):
        avro_a = self.generate_avro(self.full_avro_schema(), "avro-determinism-a", compat=True)
        avro_b = self.generate_avro(self.full_avro_schema(), "avro-determinism-b", compat=True)
        struct_a = self.generate_structure(self.full_structure_schema(), "struct-determinism-a", compat=True)
        struct_b = self.generate_structure(self.full_structure_schema(), "struct-determinism-b", compat=True)
        self.assertEqual(self.read_bytes_by_relative_path(avro_a), self.read_bytes_by_relative_path(avro_b))
        self.assertEqual(self.read_bytes_by_relative_path(struct_a), self.read_bytes_by_relative_path(struct_b))

    # -- adversarial / error paths --------------------------------------------

    def test_missing_input_files_raise_filenotfounderror(self):
        with self.assertRaises(FileNotFoundError):
            convert_avro_to_csharp(str(self.tmp / "missing.avsc"), str(self.tmp / "avro-out"), base_namespace=BASE_NS)
        with self.assertRaises(FileNotFoundError):
            convert_structure_to_csharp(str(self.tmp / "missing.struct.json"), str(self.tmp / "struct-out"), base_namespace=BASE_NS)

    def test_reserved_keywords_empty_record_and_non_ascii_fields_render(self):
        avro_schema = [
            {
                "type": "record",
                "name": "Edge",
                "namespace": "demo.edge",
                "fields": [
                    {"name": "class", "type": "string"},
                    {"name": "int", "type": "int"},
                    {"name": "namespace", "type": "long"},
                    {"name": "event", "type": "boolean"},
                    {"name": "café", "type": "string"},
                    {"name": "名字", "type": "string"},
                ],
            },
            {"type": "record", "name": "Empty", "namespace": "demo.edge", "fields": []},
        ]
        out = self.generate_avro(avro_schema, "avro-edge")
        edge = self.read_file_named(out, "Edge.cs")
        for snippet in [
            "public string @class { get; set; }",
            "public int @int { get; set; }",
            "public long @namespace { get; set; }",
            "public bool @event { get; set; }",
            "public string café { get; set; }",
            "public string 名字 { get; set; }",
        ]:
            self.assertIn(snippet, edge)
        empty = self.read_file_named(out, "Empty.cs")
        self.assertIn("public partial class Empty", empty)
        self.assertNotIn("{ get; set; }", empty)

        struct_schema = {
            "type": "object",
            "name": "EdgeStruct",
            "namespace": "demo.edge",
            "properties": {
                "class": {"type": "string"},
                "int": {"type": "int32"},
                "namespace": {"type": "int64"},
                "event": {"type": "boolean"},
                "café": {"type": "string"},
                "名字": {"type": "string"},
            },
            "required": ["class", "int", "namespace", "event", "café", "名字"],
        }
        sout = self.generate_structure(struct_schema, "struct-edge")
        stext = self.read_file_named(sout, "EdgeStruct.cs")
        for snippet in [
            "public required string @class { get; set; }",
            "public required int @int { get; set; }",
            "public required long @namespace { get; set; }",
            "public required bool @event { get; set; }",
            "public required string caf { get; set; }",
            "public required string field_unnamed { get; set; }",
        ]:
            self.assertIn(snippet, stext)

    @unittest.expectedFailure
    def test_avro_logical_types_should_render_semantic_csharp_types(self):
        # BUG: avrotocsharp ignores logicalType and fixed; decimal/uuid/date/time/timestamp/duration
        # render as bytes/string/int/long/object instead of semantic C# types.
        out = self.generate_avro(self.full_avro_schema(), "avro-logical-bug")
        text = self.read_file_named(out, "FullAvro.cs")
        for name, csharp_type in {
            "null_field": "object?",
            "decimal_bytes": "decimal",
            "uuid_string": "Guid",
            "date_int": "DateOnly",
            "time_millis_int": "TimeOnly",
            "time_micros_long": "TimeOnly",
            "ts_millis_long": "DateTimeOffset",
            "ts_micros_long": "DateTimeOffset",
            "duration_fixed": "TimeSpan",
            "fixed_field": "byte[]",
        }.items():
            with self.subTest(field=name):
                self.assert_property(text, csharp_type, name)

    def test_avro_recursive_records_should_preserve_self_references(self):
        schema = {
            "type": "record",
            "name": "Node",
            "namespace": "demo.edge",
            "fields": [
                {"name": "value", "type": "string"},
                {"name": "next", "type": ["null", "Node"], "default": None},
                {"name": "children", "type": {"type": "array", "items": "Node"}},
            ],
        }
        out = self.generate_avro(schema, "avro-recursive")
        text = self.read_file_named(out, "Node.cs")
        self.assert_property(text, "global::Compat.Demo.Edge.Node?", "next")
        self.assert_property(text, "List<global::Compat.Demo.Edge.Node>", "children")

    def test_openapi_compat_name_collisions_and_unicode_are_deduplicated(self):
        avro_schema = {
            "type": "record",
            "name": "Edge",
            "namespace": "demo.edge",
            "fields": [
                {"name": "foo_bar", "type": "string"},
                {"name": "fooBar", "type": "string"},
                {"name": "名字", "type": "string"},
            ],
        }
        avro_out = self.generate_avro(avro_schema, "avro-compat-edge", compat=True)
        avro_text = self.read_file_named(avro_out, "Edge.cs")
        self.assertIn("public string FooBar { get; set; }", avro_text)
        self.assertIn("public string FooBar1 { get; set; }", avro_text)
        self.assertIn("public string FieldUnnamed { get; set; }", avro_text)

        struct_schema = {
            "type": "object",
            "name": "EdgeStruct",
            "namespace": "demo.edge",
            "properties": {
                "foo-bar": {"type": "string"},
                "foo_bar": {"type": "string"},
                "名字": {"type": "string"},
                "!!!": {"type": "string"},
            },
            "required": ["foo-bar", "foo_bar", "名字", "!!!"],
        }
        struct_out = self.generate_structure(struct_schema, "struct-compat-edge", compat=True)
        struct_text = self.read_file_named(struct_out, "EdgeStruct.cs")
        self.assertIn("public string FooBar { get; set; }", struct_text)
        self.assertIn("public string FooBar1 { get; set; }", struct_text)
        self.assertIn("public string FieldUnnamed { get; set; }", struct_text)
        self.assertIn("public string FieldUnnamed1 { get; set; }", struct_text)

    def test_avro_enum_symbols_should_be_sanitized_for_csharp(self):
        schema = {
            "type": "record",
            "name": "EnumEdge",
            "namespace": "demo.edge",
            "fields": [
                {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["OK", "needs-sanitize", "café"]}},
            ],
        }
        out = self.generate_avro(schema, "avro-enum-edge")
        enum_text = self.read_file_named(out, "Kind.cs")
        self.assertIn("NeedsSanitize", enum_text)
        self.assertIn("Café", enum_text)
        self.assertNotIn("needs-sanitize", enum_text)

    @unittest.skipUnless(shutil.which("dotnet"), "dotnet is not available")
    def test_dotnet_builds_structure_compat_source_project(self):
        out = self.generate_structure(self.full_structure_schema(), "struct-build", compat=True)
        csproj = next((out / "src").rglob("*.csproj"))
        subprocess.check_call(["dotnet", "build", "--nologo", "--verbosity", "minimal", str(csproj)], cwd=out, timeout=180)


if __name__ == "__main__":
    unittest.main()
