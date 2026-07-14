"""Full type-system range and adversarial coverage for the JTD converters.

This suite deliberately exercises every branch of the JSON Type Definition
(RFC 8927) <-> Avrotize type mapping in avrotize/jtdtoavro.py,
avrotize/avrotojtd.py, avrotize/jtdtostructure.py, and
avrotize/structuretojtd.py. It complements the happy-path tests in
test_jtdtoavro.py.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from fastavro.schema import parse_schema

from avrotize.avrotojtd import AvroToJtdConverter, convert_avro_to_jtd
from avrotize.jtdtoavro import JtdToAvroConverter, convert_jtd_to_avro
from avrotize.jtdtostructure import convert_jtd_to_structure
from avrotize.structuretojtd import convert_structure_to_jtd

ROOT = Path(__file__).resolve().parents[1]
NS = "example.jtd"

JTD_TO_AVRO = {
    "boolean": {"type": "boolean", "jtdType": "boolean"},
    "float32": {"type": "float", "jtdType": "float32"},
    "float64": {"type": "double", "jtdType": "float64"},
    "int8": {"type": "int", "jtdType": "int8"},
    "uint8": {"type": "int", "jtdType": "uint8"},
    "int16": {"type": "int", "jtdType": "int16"},
    "uint16": {"type": "int", "jtdType": "uint16"},
    "int32": {"type": "int", "jtdType": "int32"},
    "uint32": {"type": "long", "jtdType": "uint32"},
    "string": {"type": "string", "jtdType": "string"},
    "timestamp": {"type": "long", "logicalType": "timestamp-millis", "jtdType": "timestamp"},
}

RAW_AVRO_TO_JTD = {
    "boolean": "boolean",
    "int": "int32",
    "long": "uint32",
    "float": "float32",
    "double": "float64",
    "string": "string",
}

JTD_TO_STRUCTURE = {
    "boolean": {"type": "boolean"},
    "float32": {"type": "float"},
    "float64": {"type": "double"},
    "int8": {"type": "int8"},
    "uint8": {"type": "uint8"},
    "int16": {"type": "int16"},
    "uint16": {"type": "uint16"},
    "int32": {"type": "int32"},
    "uint32": {"type": "uint32"},
    "string": {"type": "string"},
    "timestamp": {"type": "int64", "logicalType": "timestampMillis"},
}

STRUCTURE_TO_JTD_LOSSY = {
    "boolean": {"type": "boolean"},
    "float32": {"type": "float32"},
    "float64": {"type": "float64"},
    "int8": {"type": "int32"},
    "uint8": {"type": "int32"},
    "int16": {"type": "int32"},
    "uint16": {"type": "int32"},
    "int32": {"type": "int32"},
    "uint32": {"type": "uint32"},
    "string": {"type": "string"},
    "timestamp": {"type": "string"},
    "date": {"type": "string"},
    "time": {"type": "string"},
    "duration": {"type": "string"},
    "uuid": {"type": "string"},
    "binary": {"type": "string"},
}


class JtdTypeSystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    # -- helpers ---------------------------------------------------------------

    def assert_valid_avro(self, schema: Any) -> None:
        parse_schema(schema)

    def jtd_to_avro(self, schema: dict[str, Any], namespace: str | None = NS) -> Any:
        src = self.tmp / "schema.jtd.json"
        src.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
        out = self.tmp / "schema.avsc"
        convert_jtd_to_avro(str(src), str(out), namespace=namespace)
        avro = json.loads(out.read_text(encoding="utf-8"))
        self.assert_valid_avro(avro)
        return avro

    def avro_to_jtd(self, schema: Any, record_type: str | None = None) -> dict[str, Any]:
        self.assert_valid_avro(schema)
        src = self.tmp / "schema.avsc"
        src.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
        out = self.tmp / "schema.jtd.json"
        convert_avro_to_jtd(str(src), str(out), record_type=record_type)
        return json.loads(out.read_text(encoding="utf-8"))

    def jtd_to_structure(self, schema: dict[str, Any]) -> dict[str, Any]:
        src = self.tmp / "schema.jtd.json"
        src.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
        out = self.tmp / "schema.struct.json"
        convert_jtd_to_structure(str(src), str(out), namespace=NS)
        return json.loads(out.read_text(encoding="utf-8"))

    def structure_to_jtd(self, structure: dict[str, Any], record_type: str | None = None) -> dict[str, Any]:
        src = self.tmp / "schema.struct.json"
        src.write_text(json.dumps(structure, ensure_ascii=False), encoding="utf-8")
        out = self.tmp / "schema.jtd.json"
        convert_structure_to_jtd(str(src), str(out), record_type=record_type)
        return json.loads(out.read_text(encoding="utf-8"))

    def root_jtd(self, schema: dict[str, Any]) -> dict[str, Any]:
        if "definitions" in schema and "ref" in schema:
            return schema["definitions"][schema["ref"]]
        return schema

    def root_structure_definition(self, structure: dict[str, Any]) -> dict[str, Any]:
        ref = structure["$root"].removeprefix("#/definitions/")
        node: Any = structure["definitions"]
        for part in ref.split("/"):
            node = node[part]
        return node

    def fields(self, record: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {field["name"]: field for field in record["fields"]}

    # -- JTD -> Avro: full primitive range ------------------------------------

    def test_every_jtd_type_maps_to_exact_avro_type_and_annotation(self) -> None:
        for jtd_type, expected in JTD_TO_AVRO.items():
            with self.subTest(jtd_type=jtd_type):
                avro = self.jtd_to_avro({"type": jtd_type})
                self.assertEqual(avro, expected)

    def test_every_jtd_type_annotation_maps_back_to_exact_jtd_type(self) -> None:
        for jtd_type, avro in JTD_TO_AVRO.items():
            with self.subTest(jtd_type=jtd_type):
                self.assertEqual(self.avro_to_jtd(avro), {"type": jtd_type})
                self.assertEqual(AvroToJtdConverter().convert(avro), {"type": jtd_type})

    def test_raw_avro_primitive_fallback_mapping_to_jtd_is_pinned(self) -> None:
        for avro_type, jtd_type in RAW_AVRO_TO_JTD.items():
            with self.subTest(avro_type=avro_type):
                self.assertEqual(self.avro_to_jtd(avro_type), {"type": jtd_type})

    def test_avro_timestamp_millis_logical_type_maps_to_jtd_timestamp(self) -> None:
        schema = {"type": "long", "logicalType": "timestamp-millis"}
        self.assertEqual(self.avro_to_jtd(schema), {"type": "timestamp"})

    # -- JTD forms -------------------------------------------------------------

    def test_enum_symbols_are_sanitized_deduplicated_and_round_trip(self) -> None:
        jtd = {"enum": ["ready", "bad-value", "bad_value", "bad value", "123", "✓"]}
        avro = self.jtd_to_avro(jtd)
        self.assertEqual(avro["type"], "enum")
        self.assertEqual(avro["symbols"], ["ready", "bad_value", "bad_value_2", "bad_value_3", "_123", "Value"])
        self.assertEqual(
            avro["jtdEnumSymbols"],
            {"bad_value": "bad-value", "bad_value_2": "bad_value", "bad_value_3": "bad value", "_123": "123", "Value": "✓"},
        )
        self.assertEqual(self.avro_to_jtd(avro), jtd)

    def test_elements_values_and_nested_containers_map_to_array_and_map(self) -> None:
        jtd = {"elements": {"values": {"elements": {"type": "uint16"}}}}
        avro = self.jtd_to_avro(jtd)
        self.assertEqual(
            avro,
            {"type": "array", "items": {"type": "map", "values": {"type": "array", "items": {"type": "int", "jtdType": "uint16"}}}},
        )
        self.assertEqual(self.avro_to_jtd(avro), jtd)

    def test_properties_optional_properties_requiredness_and_defaults(self) -> None:
        jtd = {
            "properties": {"id": {"type": "string"}, "count": {"type": "int32"}},
            "optionalProperties": {"age": {"type": "uint8"}, "seen": {"type": "timestamp"}},
            "additionalProperties": False,
        }
        avro = self.jtd_to_avro(jtd)
        fields = self.fields(avro)
        self.assertEqual(fields["id"]["type"], {"type": "string", "jtdType": "string"})
        self.assertNotIn("default", fields["id"])
        self.assertEqual(fields["count"]["type"], {"type": "int", "jtdType": "int32"})
        self.assertNotIn("default", fields["count"])
        self.assertEqual(fields["age"]["type"], ["null", {"type": "int", "jtdType": "uint8"}])
        self.assertIsNone(fields["age"]["default"])
        self.assertEqual(fields["seen"]["type"], ["null", {"type": "long", "logicalType": "timestamp-millis", "jtdType": "timestamp"}])
        self.assertIsNone(fields["seen"]["default"])
        self.assertFalse(avro["jtdAdditionalProperties"])
        self.assertEqual(self.root_jtd(self.avro_to_jtd(avro)), jtd)

    @unittest.expectedFailure
    def test_required_nullable_property_round_trips_as_required_nullable(self) -> None:
        # BUG: Avro->JTD classifies any nullable field whose default is absent as
        # optionalProperties because field.get("default") is None for both absent
        # and explicit null defaults; required nullable JTD properties are lost.
        jtd = {"properties": {"count": {"type": "int32", "nullable": True}}}
        avro = self.jtd_to_avro(jtd)
        self.assertEqual(self.fields(avro)["count"]["type"], ["null", {"type": "int", "jtdType": "int32"}])
        self.assertNotIn("default", self.fields(avro)["count"])
        self.assertEqual(self.root_jtd(self.avro_to_jtd(avro)), jtd)

    def test_nullable_forms_wrap_in_null_union_and_round_trip(self) -> None:
        cases = [
            ({"type": "string", "nullable": True}, ["null", {"type": "string", "jtdType": "string"}]),
            ({"elements": {"type": "int8"}, "nullable": True}, ["null", {"type": "array", "items": {"type": "int", "jtdType": "int8"}}]),
            ({"values": {"type": "boolean"}, "nullable": True}, ["null", {"type": "map", "values": {"type": "boolean", "jtdType": "boolean"}}]),
        ]
        for jtd, avro_expected in cases:
            with self.subTest(jtd=jtd):
                avro = self.jtd_to_avro(jtd)
                self.assertEqual(avro, avro_expected)
                self.assertEqual(self.avro_to_jtd(avro), jtd)

    def test_discriminator_mapping_becomes_tagged_union_with_exact_metadata(self) -> None:
        jtd = {
            "discriminator": "kind",
            "mapping": {
                "car": {"properties": {"wheels": {"type": "uint8"}}},
                "boat-type": {"properties": {"draft": {"type": "float32"}}},
            },
        }
        avro = self.jtd_to_avro(jtd)
        self.assertIsInstance(avro, list)
        self.assertEqual([branch["name"] for branch in avro], ["jtdCar", "jtdBoat_Type"])
        by_tag = {branch["jtdMappingKey"]: branch for branch in avro}
        self.assertEqual(set(by_tag), {"car", "boat-type"})
        self.assertEqual(by_tag["car"]["jtdDiscriminator"], "kind")
        self.assertEqual(by_tag["car"]["fields"][0]["name"], "kind")
        self.assertEqual(by_tag["car"]["fields"][0]["type"]["symbols"], ["car"])
        self.assertEqual(by_tag["car"]["fields"][1]["type"], {"type": "int", "jtdType": "uint8"})
        self.assertEqual(by_tag["boat-type"]["fields"][0]["type"]["symbols"], ["boat_type"])
        self.assertEqual(by_tag["boat-type"]["fields"][0]["type"]["jtdEnumSymbols"], {"boat_type": "boat-type"})
        self.assertEqual(by_tag["boat-type"]["fields"][1]["type"], {"type": "float", "jtdType": "float32"})
        self.assertEqual(self.avro_to_jtd(avro), jtd)

    def test_ref_definitions_named_types_and_self_recursion(self) -> None:
        jtd = {
            "definitions": {
                "node": {
                    "properties": {"value": {"type": "int32"}},
                    "optionalProperties": {"next": {"ref": "node"}},
                }
            },
            "ref": "node",
        }
        avro = self.jtd_to_avro(jtd)
        self.assertEqual(len(avro), 1)
        node = avro[0]
        self.assertEqual(node["name"], "node")
        self.assertEqual(node["namespace"], NS)
        self.assertTrue(node["jtdRoot"])
        self.assertEqual(self.fields(node)["next"]["type"], ["null", f"{NS}.node"])
        self.assertIsNone(self.fields(node)["next"]["default"])
        self.assertEqual(
            self.avro_to_jtd(avro),
            {
                "definitions": {
                    f"{NS}.node": {
                        "properties": {"value": {"type": "int32"}},
                        "optionalProperties": {"next": {"ref": f"{NS}.node"}},
                    }
                },
                "ref": f"{NS}.node",
            },
        )

    def test_mutually_recursive_definitions_generate_parseable_avro(self) -> None:
        jtd = {
            "definitions": {
                "a": {"optionalProperties": {"b": {"ref": "b"}}},
                "b": {"optionalProperties": {"a": {"ref": "a"}}},
            },
            "ref": "a",
        }
        avro = JtdToAvroConverter(namespace=NS).convert(jtd, root_name="Root")
        self.assert_valid_avro(avro)

    # -- round-trip fidelity and lossy behavior --------------------------------

    def test_jtd_avro_jtd_round_trip_preserves_full_jtd_type_range_with_annotations(self) -> None:
        source = {
            "properties": {f"f_{name}": {"type": name} for name in JTD_TO_AVRO},
            "optionalProperties": {"maybe_int8": {"type": "int8"}, "maybe_timestamp": {"type": "timestamp"}},
            "additionalProperties": True,
        }
        avro = self.jtd_to_avro(source)
        self.assertEqual(self.root_jtd(self.avro_to_jtd(avro)), source)

    def test_jtd_avro_jtd_lossy_behavior_when_jtd_annotations_are_absent(self) -> None:
        avro = {
            "type": "record",
            "name": "RawPrims",
            "namespace": NS,
            "fields": [
                {"name": "narrow_int8_without_jtdType", "type": "int"},
                {"name": "unsigned32_without_jtdType", "type": "long"},
                {"name": "float_without_jtdType", "type": "float"},
                {"name": "timestamp_logical", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            ],
        }
        jtd = self.root_jtd(self.avro_to_jtd(avro))
        self.assertEqual(
            jtd["properties"],
            {
                "narrow_int8_without_jtdType": {"type": "int32"},
                "unsigned32_without_jtdType": {"type": "uint32"},
                "float_without_jtdType": {"type": "float32"},
                "timestamp_logical": {"type": "timestamp"},
            },
        )

    # -- Structure bridge ------------------------------------------------------

    def test_jtd_to_structure_preserves_full_jtd_type_range(self) -> None:
        source = {"properties": {name: {"type": name} for name in JTD_TO_AVRO}}
        structure = self.jtd_to_structure(source)
        root = self.root_structure_definition(structure)
        self.assertEqual(root["required"], list(JTD_TO_AVRO))
        for name, expected in JTD_TO_STRUCTURE.items():
            with self.subTest(name=name):
                self.assertEqual(root["properties"][name], expected)

    def test_structure_to_jtd_pins_full_range_and_lossy_logical_special_types(self) -> None:
        structure = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/schemas/Root",
            "name": "Root",
            "$root": "#/definitions/Root",
            "definitions": {
                "Root": {
                    "name": "Root",
                    "type": "object",
                    "required": list(STRUCTURE_TO_JTD_LOSSY),
                    "properties": {name: {"type": name} for name in STRUCTURE_TO_JTD_LOSSY},
                }
            },
        }
        jtd = self.root_jtd(self.structure_to_jtd(structure))
        self.assertEqual(jtd["properties"], STRUCTURE_TO_JTD_LOSSY)

    def test_jtd_structure_jtd_round_trip_pins_timestamp_loss(self) -> None:
        source = {"properties": {"when": {"type": "timestamp"}, "id": {"type": "string"}}}
        structure = self.jtd_to_structure(source)
        jtd = self.root_jtd(self.structure_to_jtd(structure, record_type="jtd"))
        self.assertEqual(jtd["properties"], {"when": {"type": "string"}, "id": {"type": "string"}})

    # -- adversarial / error paths -------------------------------------------

    def test_missing_input_files_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            convert_jtd_to_avro(str(self.tmp / "missing.jtd.json"), str(self.tmp / "out.avsc"))
        with self.assertRaises(FileNotFoundError):
            convert_avro_to_jtd(str(self.tmp / "missing.avsc"), str(self.tmp / "out.jtd.json"))
        with self.assertRaises(FileNotFoundError):
            convert_jtd_to_structure(str(self.tmp / "missing.jtd.json"), str(self.tmp / "out.struct.json"))
        with self.assertRaises(FileNotFoundError):
            convert_structure_to_jtd(str(self.tmp / "missing.struct.json"), str(self.tmp / "out.jtd.json"))

    def test_bad_json_raises_json_decode_error(self) -> None:
        bad = self.tmp / "bad.jtd.json"
        bad.write_text('{"type": "string"', encoding="utf-8")
        with self.assertRaises(json.JSONDecodeError):
            convert_jtd_to_avro(str(bad), str(self.tmp / "out.avsc"))

    def test_unknown_type_unknown_form_and_missing_ref_raise_clean_errors(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported JTD type 'uint64'"):
            self.jtd_to_avro({"type": "uint64"})
        with self.assertRaisesRegex(ValueError, "Unsupported or invalid JTD schema form"):
            self.jtd_to_avro({"notAForm": {"type": "string"}})
        with self.assertRaisesRegex(ValueError, "was not found in definitions"):
            self.jtd_to_avro({"ref": "Missing", "definitions": {}})

    def test_empty_schema_gracefully_maps_to_string_with_empty_schema_marker(self) -> None:
        avro = self.jtd_to_avro({})
        self.assertEqual(avro, {"type": "string", "jtdEmptySchema": True})
        self.assertEqual(self.avro_to_jtd(avro), {"type": "string"})

    def test_enum_duplicate_after_sanitization_collisions_are_suffixed(self) -> None:
        avro = self.jtd_to_avro({"enum": ["a-b", "a_b", "a b"]})
        self.assertEqual(avro["symbols"], ["a_b", "a_b_2", "a_b_3"])
        self.assertEqual(avro["jtdEnumSymbols"], {"a_b": "a-b", "a_b_2": "a_b", "a_b_3": "a b"})
        self.assertEqual(self.avro_to_jtd(avro), {"enum": ["a-b", "a_b", "a b"]})

    def test_unicode_non_ascii_enum_values_survive_via_jtd_enum_symbols(self) -> None:
        jtd = {"enum": ["café", "普通话", "😀"]}
        avro = self.jtd_to_avro(jtd)
        self.assertEqual(avro["symbols"], ["caf_", "___", "Value"])
        self.assertEqual(avro["jtdEnumSymbols"], {"caf_": "café", "___": "普通话", "Value": "😀"})
        self.assertEqual(self.avro_to_jtd(avro), jtd)

    def test_unicode_non_ascii_property_names_survive_round_trip(self) -> None:
        jtd = {"properties": {"café": {"type": "string"}, "普通话": {"type": "string"}}, "optionalProperties": {"emoji😀": {"type": "string"}}}
        avro = self.jtd_to_avro(jtd)
        self.assertEqual([field["name"] for field in avro["fields"]], ["caf_", "___", "emoji_"])
        self.assertEqual(self.root_jtd(self.avro_to_jtd(avro)), jtd)


if __name__ == "__main__":
    unittest.main()
