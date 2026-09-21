"""Tests for JSON Type Definition converters."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
from typing import Any

from avro.schema import parse as parse_apache_avro_schema
from fastavro import parse_schema

from avrotize.avrotojtd import AvroToJtdConverter, convert_avro_to_jtd
from avrotize.jtdtoavro import JtdToAvroConverter, convert_jtd_to_avro
from avrotize.jtdtostructure import convert_jtd_to_structure
from avrotize.structuretojtd import convert_structure_to_jtd


class TestJtdConverters(unittest.TestCase):
    """Focused tests for JTD conversion support."""

    root = Path(__file__).resolve().parents[1]
    fixtures = root / "test" / "jtd"
    generated = fixtures / "generated"

    @classmethod
    def setUpClass(cls) -> None:
        if cls.generated.exists():
            shutil.rmtree(cls.generated)
        cls.generated.mkdir(parents=True)

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.generated.exists():
            shutil.rmtree(cls.generated)

    def _convert_dict(self, schema: dict[str, Any], namespace: str | None = "example") -> Any:
        return JtdToAvroConverter(namespace=namespace).convert(schema, root_name="Root")

    def _assert_valid_avro(self, schema: Any) -> None:
        parse_schema(schema)

    def _root_jtd(self, schema: dict[str, Any]) -> dict[str, Any]:
        if "definitions" in schema and "ref" in schema:
            return schema["definitions"][schema["ref"]]
        return schema

    def _assert_type_mapping(self, jtd_type: str, avro_type: str, logical_type: str | None = None) -> None:
        avro = self._convert_dict({"type": jtd_type})
        self.assertIsInstance(avro, dict)
        self.assertEqual(avro["type"], avro_type)
        self.assertEqual(avro["jtdType"], jtd_type)
        if logical_type:
            self.assertEqual(avro["logicalType"], logical_type)
        self._assert_valid_avro(avro)
        self.assertEqual(AvroToJtdConverter().convert(avro), {"type": jtd_type})

    def test_boolean_type_mapping(self) -> None:
        self._assert_type_mapping("boolean", "boolean")

    def test_float32_type_mapping(self) -> None:
        self._assert_type_mapping("float32", "float")

    def test_float64_type_mapping(self) -> None:
        self._assert_type_mapping("float64", "double")

    def test_int8_type_mapping(self) -> None:
        self._assert_type_mapping("int8", "int")

    def test_uint8_type_mapping(self) -> None:
        self._assert_type_mapping("uint8", "int")

    def test_int16_type_mapping(self) -> None:
        self._assert_type_mapping("int16", "int")

    def test_uint16_type_mapping(self) -> None:
        self._assert_type_mapping("uint16", "int")

    def test_int32_type_mapping(self) -> None:
        self._assert_type_mapping("int32", "int")

    def test_uint32_type_mapping(self) -> None:
        self._assert_type_mapping("uint32", "long")

    def test_string_type_mapping(self) -> None:
        self._assert_type_mapping("string", "string")

    def test_timestamp_type_mapping_uses_valid_logical_type(self) -> None:
        self._assert_type_mapping("timestamp", "long", "timestamp-millis")

    def test_enum_symbols_are_sanitized_and_round_trip(self) -> None:
        avro = self._convert_dict({"enum": ["ok", "bad-value", "123"]})
        self._assert_valid_avro(avro)
        self.assertEqual(avro["symbols"], ["ok", "bad_value", "_123"])
        self.assertEqual(avro["jtdEnumSymbols"], {"bad_value": "bad-value", "_123": "123"})
        self.assertEqual(AvroToJtdConverter().convert(avro), {"enum": ["ok", "bad-value", "123"]})

    def test_elements_map_to_avro_array(self) -> None:
        avro = self._convert_dict({"elements": {"type": "string"}})
        self._assert_valid_avro(avro)
        self.assertEqual(avro["type"], "array")
        self.assertEqual(avro["items"]["type"], "string")
        self.assertEqual(AvroToJtdConverter().convert(avro), {"elements": {"type": "string"}})

    def test_values_map_to_avro_map(self) -> None:
        avro = self._convert_dict({"values": {"type": "int32"}})
        self._assert_valid_avro(avro)
        self.assertEqual(avro["type"], "map")
        self.assertEqual(avro["values"]["type"], "int")
        self.assertEqual(AvroToJtdConverter().convert(avro), {"values": {"type": "int32"}})

    def test_properties_and_optional_properties(self) -> None:
        avro = self._convert_dict({"properties": {"id": {"type": "string"}}, "optionalProperties": {"age": {"type": "uint8"}}})
        self._assert_valid_avro(avro)
        fields = {field["name"]: field for field in avro["fields"]}
        self.assertEqual(fields["id"]["type"]["type"], "string")
        self.assertEqual(fields["age"]["type"][0], "null")
        self.assertIsNone(fields["age"]["default"])
        jtd = self._root_jtd(AvroToJtdConverter().convert(avro))
        self.assertIn("id", jtd["properties"])
        self.assertIn("age", jtd["optionalProperties"])

    def test_nullable_type_maps_to_avro_union(self) -> None:
        avro = self._convert_dict({"type": "string", "nullable": True})
        self._assert_valid_avro(avro)
        self.assertEqual(avro[0], "null")
        self.assertEqual(AvroToJtdConverter().convert(avro), {"type": "string", "nullable": True})

    def test_additional_properties_metadata_is_preserved(self) -> None:
        avro = self._convert_dict({"properties": {"id": {"type": "string"}}, "additionalProperties": True})
        self._assert_valid_avro(avro)
        self.assertTrue(avro["jtdAdditionalProperties"])
        jtd = self._root_jtd(AvroToJtdConverter().convert(avro))
        self.assertTrue(jtd["additionalProperties"])

    def test_discriminator_mapping_becomes_avro_union(self) -> None:
        schema = json.loads((self.fixtures / "vehicle.jtd.json").read_text(encoding="utf-8"))
        avro = self._convert_dict(schema)
        self._assert_valid_avro(avro)
        self.assertIsInstance(avro, list)
        self.assertEqual({branch["jtdMappingKey"] for branch in avro}, {"car", "boat-type"})
        round_tripped = AvroToJtdConverter().convert(avro)
        self.assertEqual(round_tripped["discriminator"], "kind")
        self.assertEqual(set(round_tripped["mapping"]), {"car", "boat-type"})

    def test_ref_and_definitions_generate_named_avro_types(self) -> None:
        schema = json.loads((self.fixtures / "node.jtd.json").read_text(encoding="utf-8"))
        avro = self._convert_dict(schema)
        self._assert_valid_avro(avro)
        self.assertEqual(len(avro), 1)
        self.assertEqual(avro[0]["name"], "Node")
        child_type = next(field for field in avro[0]["fields"] if field["name"] == "child")["type"]
        self.assertEqual(child_type, ["null", "example.Node"])

    def test_definition_named_enum_does_not_rewrite_schema_kind_tokens(self) -> None:
        schema = {
            "definitions": {
                "enum": {"enum": ["reserved"]},
                "status": {"enum": ["active"]},
                "holder": {
                    "properties": {
                        "status": {"ref": "status"},
                        "reserved": {"ref": "enum"},
                    },
                },
            },
            "ref": "holder",
        }

        converted = self._convert_dict(schema)

        parse_schema(converted)
        parse_apache_avro_schema(json.dumps(converted))
        by_name = {item["name"]: item for item in converted if isinstance(item, dict)}
        self.assertEqual(by_name["status"]["type"], "enum")
        self.assertEqual(by_name["enum"]["type"], "enum")

    def test_nested_fixture_generates_valid_avro(self) -> None:
        out = self.generated / "person.avsc"
        convert_jtd_to_avro(str(self.fixtures / "person.jtd.json"), str(out), namespace="example")
        avro = json.loads(out.read_text(encoding="utf-8"))
        self._assert_valid_avro(avro)
        root = next(item for item in avro if isinstance(item, dict) and item.get("jtdAdditionalProperties"))
        self.assertEqual(root["type"], "record")
        self.assertTrue(root["jtdAdditionalProperties"])

    def test_direct_and_bridge_preserve_distinct_jtd_filename_root_naming(self) -> None:
        source_path = self.generated / "schema.jtd.json"
        avro_path = self.generated / "schema.avsc"
        structure_path = self.generated / "schema.struct.json"
        source_path.write_text(json.dumps({
            "properties": {"value": {"type": "string"}},
        }), encoding="utf-8")

        convert_jtd_to_avro(str(source_path), str(avro_path), namespace="example")
        direct_avro = json.loads(avro_path.read_text(encoding="utf-8"))
        parse_schema(direct_avro)
        parse_apache_avro_schema(json.dumps(direct_avro))
        self.assertEqual(direct_avro["name"], "schema_jtd")

        convert_jtd_to_structure(str(source_path), str(structure_path), namespace="example")
        structure = json.loads(structure_path.read_text(encoding="utf-8"))
        self.assertEqual(structure["name"], "schema")
        self.assertEqual(structure["$root"], "#/definitions/example/schema")

    def test_round_trip_preserves_core_record_semantics(self) -> None:
        avro_path = self.generated / "roundtrip.avsc"
        jtd_path = self.generated / "roundtrip.jtd.json"
        source = {"properties": {"id": {"type": "string"}, "when": {"type": "timestamp"}}, "optionalProperties": {"count": {"type": "uint32"}}}
        source_path = self.generated / "roundtrip-source.jtd.json"
        source_path.write_text(json.dumps(source), encoding="utf-8")
        convert_jtd_to_avro(str(source_path), str(avro_path), namespace="example")
        convert_avro_to_jtd(str(avro_path), str(jtd_path))
        converted = json.loads(jtd_path.read_text(encoding="utf-8"))
        root = self._root_jtd(converted)
        self.assertEqual(root["properties"], source["properties"])
        self.assertEqual(root["optionalProperties"], source["optionalProperties"])

    def test_jtd_to_structure_bridge_writes_json_structure(self) -> None:
        out = self.generated / "person.struct.json"
        convert_jtd_to_structure(str(self.fixtures / "person.jtd.json"), str(out), namespace="example")
        structure = json.loads(out.read_text(encoding="utf-8"))
        self.assertIn("$schema", structure)
        self.assertIn("definitions", structure)
        self.assertEqual(structure["$root"], "#/definitions/example/person")
        self.assertNotIn("type", structure)

    def test_jtd_to_structure_bridge_preserves_inline_roots(self) -> None:
        cases = [
            ({"type": "string"}, "string"),
            ({"elements": {"type": "int32"}}, "array"),
            ({"values": {"type": "boolean"}}, "map"),
        ]
        for source, expected_type in cases:
            with self.subTest(expected_type=expected_type):
                source_path = self.generated / f"{expected_type}.jtd.json"
                out = self.generated / f"{expected_type}.struct.json"
                source_path.write_text(json.dumps(source), encoding="utf-8")

                convert_jtd_to_structure(str(source_path), str(out), namespace="example")

                structure = json.loads(out.read_text(encoding="utf-8"))
                root = structure["definitions"]["example"][expected_type]
                self.assertEqual(root["type"], expected_type)

    def test_jtd_to_structure_bridge_preserves_nullable_root(self) -> None:
        source_path = self.generated / "nullable.jtd.json"
        out = self.generated / "nullable.struct.json"
        source_path.write_text(json.dumps({"type": "string", "nullable": True}), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        root = structure["definitions"]["example"]["nullable"]
        self.assertEqual(root["type"], [{"$ref": "#/definitions/example/nullableValue"}, "null"])

    def test_jtd_to_structure_bridge_preserves_nullable_record_root(self) -> None:
        source_path = self.generated / "nullable-record.jtd.json"
        out = self.generated / "nullable-record.struct.json"
        source_path.write_text(json.dumps({"properties": {"id": {"type": "string"}}, "nullable": True}), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        value = structure["definitions"]["example"]["nullable_recordValue"]
        self.assertIn("id", value["properties"])

    def test_jtd_to_structure_bridge_unwraps_referenced_primitive(self) -> None:
        source_path = self.generated / "alias.jtd.json"
        out = self.generated / "alias.struct.json"
        source_path.write_text(json.dumps({"definitions": {"identifier": {"type": "string"}}, "ref": "identifier"}), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        root = structure["definitions"]["example"]["alias"]
        self.assertEqual(root["type"], "string")

    def test_jtd_to_structure_bridge_unwraps_chained_nullable_alias(self) -> None:
        source_path = self.generated / "chain.jtd.json"
        out = self.generated / "chain.struct.json"
        source_path.write_text(json.dumps({
            "definitions": {
                "text": {"type": "string", "nullable": True},
                "alias": {"ref": "text"},
            },
            "ref": "alias",
        }), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        root = structure["definitions"]["example"]["chain"]
        self.assertIn("null", root["type"])

    def test_jtd_to_structure_bridge_retargets_nullable_recursive_root(self) -> None:
        source_path = self.generated / "node.jtd.json"
        out = self.generated / "node.struct.json"
        source_path.write_text(json.dumps({
            "definitions": {
                "node": {
                    "properties": {"value": {"type": "string"}},
                    "optionalProperties": {"next": {"ref": "node"}},
                },
            },
            "ref": "node",
            "nullable": True,
        }), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        value = structure["definitions"]["example"]["nodeValue"]
        self.assertEqual(
            value["properties"]["next"]["type"]["$ref"],
            "#/definitions/example/nodeValue",
        )
        self.assertNotIn("next", value["required"])

    def test_jtd_to_structure_bridge_retargets_nullable_mutual_cycle(self) -> None:
        source_path = self.generated / "a.jtd.json"
        out = self.generated / "mutual-cycle.struct.json"
        source_path.write_text(json.dumps({
            "definitions": {
                "a": {"optionalProperties": {"b": {"ref": "b"}}},
                "b": {"optionalProperties": {"a": {"ref": "a"}}},
            },
            "ref": "a",
            "nullable": True,
        }), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        definitions = structure["definitions"]["example"]
        self.assertEqual(structure["$root"], "#/definitions/example/a")
        self.assertEqual(
            definitions["a"]["type"],
            [{"$ref": "#/definitions/example/aValue"}, "null"],
        )
        self.assertEqual(
            definitions["aValue"]["properties"]["b"]["type"]["$ref"],
            "#/definitions/example/b",
        )
        self.assertEqual(
            definitions["b"]["properties"]["a"]["type"]["$ref"],
            "#/definitions/example/aValue",
        )
        try:
            from json_structure import SchemaValidator
        except ImportError:
            self.skipTest("json-structure SDK not installed")
        self.assertEqual(SchemaValidator().validate(structure), [])

    def test_jtd_to_structure_bridge_avoids_synthetic_value_collision(self) -> None:
        source_path = self.generated / "node.jtd.json"
        out = self.generated / "node-collision.struct.json"
        source_path.write_text(json.dumps({
            "definitions": {
                "nodeValue": {"properties": {"marker": {"type": "int32"}}},
                "node": {
                    "properties": {"helper": {"ref": "nodeValue"}},
                    "optionalProperties": {"next": {"ref": "node"}},
                },
            },
            "ref": "node",
            "nullable": True,
        }), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        definitions = structure["definitions"]["example"]
        self.assertIn("marker", definitions["nodeValue"]["properties"])
        self.assertEqual(definitions["node"]["type"][0]["$ref"], "#/definitions/example/nodeValue2")
        self.assertEqual(
            definitions["nodeValue2"]["properties"]["helper"]["type"]["$ref"],
            "#/definitions/example/nodeValue",
        )
        self.assertEqual(
            definitions["nodeValue2"]["properties"]["next"]["type"]["$ref"],
            "#/definitions/example/nodeValue2",
        )

    def test_jtd_root_name_is_sanitized(self) -> None:
        source_path = self.generated / "123-bad.name.jtd.json"
        out = self.generated / "named.struct.json"
        source_path.write_text(json.dumps({"type": "string"}), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(structure["name"], "_123_bad_name")

    def test_jtd_to_structure_uses_sanitized_namespace_for_synthetic_root(self) -> None:
        source_path = self.generated / "namespace-root.jtd.json"
        out = self.generated / "namespace-root.struct.json"
        source_path.write_text(json.dumps({"type": "string"}), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="bad-name.space segment")

        structure = json.loads(out.read_text(encoding="utf-8"))
        root_path = "bad_name/space_segment/namespace_root"
        self.assertEqual(structure["$root"], f"#/definitions/{root_path}")
        self.assertEqual(structure["$id"], f"https://example.com/schemas/{root_path}")
        self.assertIn("namespace_root", structure["definitions"]["bad_name"]["space_segment"])

    def test_jtd_to_structure_bridge_escapes_reserved_root_name(self) -> None:
        source_path = self.generated / "type.jtd.json"
        out = self.generated / "type.struct.json"
        source_path.write_text(json.dumps({"type": "string"}), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(structure["$root"], "#/definitions/example/type_")
        self.assertIn("type_", structure["definitions"]["example"])

    def test_jtd_to_structure_bridge_avoids_dependency_root_collision(self) -> None:
        source_path = self.generated / "item.jtd.json"
        out = self.generated / "item.struct.json"
        source_path.write_text(json.dumps({
            "definitions": {"item": {"properties": {"value": {"type": "string"}}}},
            "elements": {"ref": "item"},
        }), encoding="utf-8")

        convert_jtd_to_structure(str(source_path), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(structure["$root"], "#/definitions/example/itemRoot")
        self.assertIn("item", structure["definitions"]["example"])
        self.assertIn("itemRoot", structure["definitions"]["example"])

    def test_jtd_to_structure_bridge_preserves_discriminator_root(self) -> None:
        out = self.generated / "vehicle.struct.json"

        convert_jtd_to_structure(str(self.fixtures / "vehicle.jtd.json"), str(out), namespace="example")

        structure = json.loads(out.read_text(encoding="utf-8"))
        root = structure["definitions"]["example"]["vehicle"]
        self.assertEqual(root["type"], "choice")
        self.assertEqual(len(root["choices"]), 2)

    def test_structure_to_jtd_bridge_writes_jtd(self) -> None:
        out = self.generated / "structure-person.jtd.json"
        convert_structure_to_jtd(str(self.fixtures / "structure_person.struct.json"), str(out))
        jtd = json.loads(out.read_text(encoding="utf-8"))
        root = self._root_jtd(jtd)
        self.assertIn("id", root["properties"])
        self.assertIn("age", root["optionalProperties"])

    def test_avro_record_type_selection(self) -> None:
        avro = [
            {"type": "record", "name": "A", "fields": [{"name": "value", "type": "string"}]},
            {"type": "record", "name": "B", "fields": [{"name": "count", "type": {"type": "int", "jtdType": "uint8"}}]},
        ]
        jtd = self._root_jtd(AvroToJtdConverter(record_type="B").convert(avro))
        self.assertIn("count", jtd["properties"])

    def test_commands_json_is_valid_and_registers_jtd_commands(self) -> None:
        commands = json.loads((self.root / "avrotize" / "commands.json").read_text(encoding="utf-8"))
        names = {command["command"] for command in commands}
        self.assertTrue({"jtd2a", "a2jtd", "jtd2s", "s2jtd"}.issubset(names))

    def test_import_path_is_worktree(self) -> None:
        import avrotize

        self.assertTrue(str(avrotize.__file__).startswith(str(self.root)))

    def test_cli_smoke_jtd2a(self) -> None:
        out = self.generated / "cli-person.avsc"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(self.root)
        result = subprocess.run(
            [sys.executable, "-m", "avrotize", "jtd2a", str(self.fixtures / "person.jtd.json"), "--out", str(out), "--namespace", "example"],
            cwd=str(self.root),
            env=env,
            check=False,
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self._assert_valid_avro(json.loads(out.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
