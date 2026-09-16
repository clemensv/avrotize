"""Full type-system range and adversarial coverage for Parquet converters.

This suite deliberately exercises every JSON Structure -> Parquet, Avro ->
Parquet, and Parquet -> Avro type-mapping branch in the Parquet converters,
including lossy round trips and adversarial/error inputs. It complements the
happy-path fixture coverage in test_structuretoparquet.py.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from avrotize.avrotoparquet import convert_avro_to_parquet
from avrotize.parquettoavro import convert_parquet_to_avro
from avrotize.structuretoparquet import convert_structure_to_parquet

ROOT = Path(__file__).resolve().parents[1]
NS = "example.parquet"


class ParquetTypeSystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    # -- helpers ---------------------------------------------------------------

    def structure_to_schema(
        self,
        schema: dict,
        record_type: str | None = None,
        *,
        emit_cloudevents: bool = False,
        output_format: str = "parquet",
    ):
        src = self.tmp / "schema.struct.json"
        src.write_text(json.dumps(schema), encoding="utf-8")
        suffix = ".schema.json" if output_format == "schema" else ".parquet"
        out = self.tmp / f"schema{suffix}"
        convert_structure_to_parquet(str(src), record_type, str(out), emit_cloudevents, output_format)
        if output_format == "schema":
            return json.loads(out.read_text(encoding="utf-8"))
        return pq.read_schema(out)

    def avro_to_schema(
        self,
        schema,
        record_type: str | None = None,
        *,
        emit_cloudevents: bool = False,
    ) -> pa.Schema:
        src = self.tmp / "schema.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "schema.parquet"
        convert_avro_to_parquet(str(src), record_type, str(out), emit_cloudevents)
        return pq.read_schema(out)

    def parquet_to_avro(self, schema: pa.Schema, name: str = "schema") -> dict:
        parquet_path = self.tmp / f"{name}.parquet"
        avro_path = self.tmp / f"{name}.avsc"
        pq.write_table(pa.Table.from_batches([], schema=schema), parquet_path)
        convert_parquet_to_avro(str(parquet_path), str(avro_path), namespace=NS)
        return json.loads(avro_path.read_text(encoding="utf-8"))

    def fields(self, schema: pa.Schema) -> dict[str, pa.Field]:
        return {field.name: field for field in schema}

    def avro_fields(self, schema: dict) -> dict:
        return {field["name"]: field for field in schema["fields"]}

    # -- JSON Structure -> Parquet: full primitive range -----------------------

    def test_json_structure_every_scalar_maps_to_expected_parquet_type(self):
        expected = {
            "f_null": pa.string(),
            "f_boolean": pa.bool_(),
            "f_string": pa.string(),
            "f_int8": pa.int8(),
            "f_uint8": pa.uint8(),
            "f_int16": pa.int16(),
            "f_uint16": pa.uint16(),
            "f_int32": pa.int32(),
            "f_uint32": pa.uint32(),
            "f_int64": pa.int64(),
            "f_uint64": pa.uint64(),
            "f_int128": pa.string(),
            "f_uint128": pa.string(),
            "f_integer": pa.int32(),
            "f_number": pa.float64(),
            "f_float8": pa.float32(),
            "f_float": pa.float32(),
            "f_float32": pa.float32(),
            "f_binary32": pa.float32(),
            "f_double": pa.float64(),
            "f_float64": pa.float64(),
            "f_binary64": pa.float64(),
            "f_binary": pa.binary(),
            "f_bytes": pa.binary(),
            "f_decimal": pa.decimal128(12, 4),
            "f_uuid": pa.string(),
            "f_uri": pa.string(),
            "f_jsonpointer": pa.string(),
            "f_any": pa.string(),
        }
        properties = {
            name: {"type": name.removeprefix("f_")}
            for name in expected
            if name not in {"f_decimal", "f_any"}
        }
        properties["f_decimal"] = {"type": "decimal", "precision": 12, "scale": 4}
        properties["f_any"] = {"type": "any"}
        schema = self.structure_to_schema({"type": "object", "properties": properties, "required": list(properties)})
        fields = self.fields(schema)
        for name, typ in expected.items():
            with self.subTest(field=name):
                self.assertEqual(fields[name].type, typ)
                self.assertFalse(fields[name].nullable)

    def test_json_structure_logical_temporal_types_map_to_expected_parquet_type(self):
        schema = self.structure_to_schema(
            {
                "type": "object",
                "properties": {
                    "f_date": {"type": "date"},
                    "f_time": {"type": "time"},
                    "f_datetime": {"type": "datetime"},
                    "f_timestamp": {"type": "timestamp"},
                    "f_duration": {"type": "duration"},
                },
                "required": ["f_date", "f_time", "f_datetime", "f_timestamp", "f_duration"],
            }
        )
        fields = self.fields(schema)
        self.assertEqual(fields["f_date"].type, pa.date32())
        self.assertEqual(fields["f_time"].type, pa.time64("us"))
        self.assertEqual(fields["f_datetime"].type, pa.timestamp("us"))
        self.assertEqual(fields["f_timestamp"].type, pa.timestamp("us"))
        self.assertEqual(fields["f_duration"].type, pa.int64())

    def test_json_structure_required_optional_and_explicit_nullability(self):
        schema = self.structure_to_schema(
            {
                "type": "object",
                "properties": {
                    "required_string": {"type": "string"},
                    "optional_string": {"type": "string"},
                    "required_nullable_string": {"type": ["null", "string"]},
                    "list_nullable": ["string", "null"],
                },
                "required": ["required_string", "required_nullable_string", "list_nullable"],
            }
        )
        fields = self.fields(schema)
        self.assertFalse(fields["required_string"].nullable)
        self.assertTrue(fields["optional_string"].nullable)
        self.assertTrue(fields["required_nullable_string"].nullable)
        self.assertTrue(fields["list_nullable"].nullable)
        self.assertEqual(fields["required_nullable_string"].type, pa.string())

    # -- JSON Structure -> Parquet: containers, choices, records ---------------

    def test_json_structure_containers_nested_objects_tuple_and_choice(self):
        schema = self.structure_to_schema(
            {
                "type": "object",
                "properties": {
                    "array_string": {"type": "array", "items": {"type": "string"}},
                    "set_int": {"type": "set", "items": {"type": "int32"}},
                    "map_double": {"type": "map", "values": {"type": "double"}},
                    "nested_arrays": {
                        "type": "array",
                        "items": {"type": "array", "items": {"type": "uint16"}},
                    },
                    "object_field": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}, "count": {"type": "int64"}},
                        "required": ["id"],
                    },
                    "open_object": {"type": "object"},
                    "tuple_field": {"type": "tuple", "items": [{"type": "string"}, {"type": "int32"}]},
                    "choice_list": {
                        "type": "choice",
                        "choices": [{"type": "string"}, {"type": "int32"}],
                    },
                    "choice_dict": {
                        "type": "choice",
                        "choices": {"text": {"type": "string"}, "flag": {"type": "boolean"}},
                    },
                    "union_list": [{"type": "string"}, {"type": "int64"}],
                },
                "required": [
                    "array_string",
                    "set_int",
                    "map_double",
                    "nested_arrays",
                    "object_field",
                    "open_object",
                    "tuple_field",
                    "choice_list",
                    "choice_dict",
                    "union_list",
                ],
            }
        )
        fields = self.fields(schema)
        self.assertEqual(fields["array_string"].type, pa.list_(pa.string()))
        self.assertEqual(fields["set_int"].type, pa.list_(pa.int32()))
        self.assertEqual(fields["map_double"].type, pa.map_(pa.string(), pa.float64()))
        self.assertEqual(fields["nested_arrays"].type, pa.list_(pa.list_(pa.uint16())))
        self.assertEqual(fields["object_field"].type, pa.struct([
            pa.field("id", pa.string(), nullable=False),
            pa.field("count", pa.int64(), nullable=True),
        ]))
        self.assertEqual(fields["open_object"].type, pa.map_(pa.string(), pa.string()))
        self.assertEqual(fields["tuple_field"].type, pa.struct([
            pa.field("field_0", pa.string(), nullable=False),
            pa.field("field_1", pa.int32(), nullable=False),
        ]))
        self.assertEqual(fields["choice_list"].type, pa.struct([
            pa.field("option_0", pa.string(), nullable=True),
            pa.field("option_1", pa.int32(), nullable=True),
        ]))
        self.assertEqual(fields["choice_dict"].type, pa.struct([
            pa.field("text", pa.string(), nullable=True),
            pa.field("flag", pa.bool_(), nullable=True),
        ]))
        self.assertEqual(fields["union_list"].type, pa.struct([
            pa.field("option_0", pa.string(), nullable=True),
            pa.field("option_1", pa.int64(), nullable=True),
        ]))

    def test_json_structure_extends_ref_record_type_cloudevents_and_schema_output(self):
        schema_doc = {
            "definitions": {
                "Base": {
                    "type": "object",
                    "properties": {"base_id": {"type": "string"}},
                    "required": ["base_id"],
                },
                "Event": {
                    "type": "object",
                    "properties": {
                        "event_time": {"type": "timestamp"},
                        "payload": {
                            "type": "object",
                            "$extends": "#/definitions/Base",
                            "properties": {"payload_value": {"type": "int32"}},
                            "required": ["payload_value"],
                        },
                    },
                    "required": ["event_time", "payload"],
                },
            }
        }
        schema = self.structure_to_schema(schema_doc, "Event", emit_cloudevents=True)
        fields = self.fields(schema)
        self.assertEqual(fields["event_time"].type, pa.timestamp("us"))
        self.assertEqual(fields["payload"].type, pa.struct([
            pa.field("base_id", pa.string(), nullable=False),
            pa.field("payload_value", pa.int32(), nullable=False),
        ]))
        self.assertFalse(fields["___type"].nullable)
        self.assertEqual(fields["___type"].type, pa.string())
        self.assertEqual(fields["___source"].type, pa.string())
        self.assertEqual(fields["___id"].type, pa.string())
        self.assertEqual(fields["___time"].type, pa.timestamp("us"))
        self.assertFalse(fields["___time"].nullable)
        self.assertEqual(fields["___subject"].type, pa.string())
        self.assertTrue(fields["___subject"].nullable)

        schema_json = self.structure_to_schema(schema_doc, "Event", output_format="schema")
        self.assertEqual(schema_json["type"], "struct")
        self.assertEqual(
            schema_json["fields"],
            [
                {"name": "event_time", "type": "timestamp[us]", "nullable": False},
                {
                    "name": "payload",
                    "type": "struct<base_id: string not null, payload_value: int32 not null>",
                    "nullable": False,
                },
            ],
        )

    @unittest.expectedFailure
    def test_json_structure_top_level_record_type_extends_includes_base_fields(self):
        # BUG: _resolve_root returns the selected definition directly and the
        # top-level conversion loop ignores $extends, so Base.base_id is dropped.
        schema = self.structure_to_schema(
            {
                "definitions": {
                    "Base": {
                        "type": "object",
                        "properties": {"base_id": {"type": "string"}},
                        "required": ["base_id"],
                    },
                    "Event": {
                        "type": "object",
                        "$extends": "#/definitions/Base",
                        "properties": {"event_time": {"type": "timestamp"}},
                        "required": ["event_time"],
                    },
                }
            },
            "Event",
        )
        self.assertIn("base_id", schema.names)

    # -- Avro -> Parquet: full primitive/logical/container range ----------------

    def test_avro_every_primitive_and_logical_type_maps_to_expected_parquet_type(self):
        schema = {
            "type": "record",
            "name": "AvroPrims",
            "namespace": NS,
            "fields": [
                {"name": "f_null", "type": "null"},
                {"name": "f_boolean", "type": "boolean"},
                {"name": "f_int", "type": "int"},
                {"name": "f_long", "type": "long"},
                {"name": "f_float", "type": "float"},
                {"name": "f_double", "type": "double"},
                {"name": "f_bytes", "type": "bytes"},
                {"name": "f_string", "type": "string"},
                {"name": "f_uuid", "type": {"type": "string", "logicalType": "uuid"}},
                {"name": "f_decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "f_date", "type": {"type": "int", "logicalType": "date"}},
                {"name": "f_time_millis", "type": {"type": "long", "logicalType": "time-millis"}},
                {"name": "f_time_micros", "type": {"type": "long", "logicalType": "time-micros"}},
                {"name": "f_ts_millis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "f_ts_micros", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                {"name": "f_enum", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "BLUE"]}},
                {"name": "f_fixed", "type": {"type": "fixed", "name": "Hash16", "size": 16}},
            ],
        }
        fields = self.fields(self.avro_to_schema(schema))
        expected = {
            "f_null": pa.string(),
            "f_boolean": pa.bool_(),
            "f_int": pa.int32(),
            "f_long": pa.int64(),
            "f_float": pa.float32(),
            "f_double": pa.float64(),
            "f_bytes": pa.binary(),
            "f_string": pa.string(),
            "f_uuid": pa.string(),
            "f_decimal": pa.decimal128(38, 18),
            "f_date": pa.date32(),
            "f_time_millis": pa.time64("ns"),
            "f_time_micros": pa.time64("ns"),
            "f_ts_millis": pa.timestamp("ns"),
            "f_ts_micros": pa.timestamp("ns"),
            "f_enum": pa.string(),
            "f_fixed": pa.string(),
        }
        for name, typ in expected.items():
            with self.subTest(field=name):
                self.assertEqual(fields[name].type, typ)
                self.assertTrue(fields[name].nullable)

    def test_avro_containers_records_named_types_and_unions_map_to_expected_parquet_type(self):
        schema = [
            {
                "type": "record",
                "name": "Child",
                "namespace": NS,
                "fields": [{"name": "child_id", "type": "string"}],
            },
            {
                "type": "record",
                "name": "AvroContainers",
                "namespace": NS,
                "fields": [
                    {"name": "array_int", "type": {"type": "array", "items": "int"}},
                    {"name": "map_long", "type": {"type": "map", "values": "long"}},
                    {
                        "name": "record_field",
                        "type": {
                            "type": "record",
                            "name": "Inner",
                            "fields": [{"name": "flag", "type": "boolean"}],
                        },
                    },
                    {"name": "named_child", "type": f"{NS}.Child"},
                    {"name": "nullable_string", "type": ["null", "string"]},
                    {"name": "choice_scalar", "type": ["string", "int"]},
                    {"name": "choice_complex", "type": ["null", {"type": "array", "items": "string"}, {"type": "map", "values": "int"}]},
                    {"name": "empty_union", "type": []},
                ],
            },
        ]
        fields = self.fields(self.avro_to_schema(schema, "AvroContainers"))
        self.assertEqual(fields["array_int"].type, pa.list_(pa.int32()))
        self.assertEqual(fields["map_long"].type, pa.map_(pa.string(), pa.int64()))
        self.assertEqual(fields["record_field"].type, pa.struct({"flag": pa.bool_()}))
        self.assertEqual(fields["named_child"].type, pa.struct({"child_id": pa.string()}))
        self.assertEqual(fields["nullable_string"].type, pa.string())
        self.assertEqual(fields["choice_scalar"].type, pa.struct([
            pa.field("stringValue", pa.string()),
            pa.field("intValue", pa.int32()),
        ]))
        self.assertEqual(fields["choice_complex"].type, pa.struct([
            pa.field("ArrayValue", pa.list_(pa.string())),
            pa.field("MapValue", pa.map_(pa.string(), pa.int32())),
        ]))
        self.assertEqual(fields["empty_union"].type, pa.string())

    def test_avro_record_type_selection_and_cloudevents_columns(self):
        schema = [
            {"type": "record", "name": "First", "namespace": NS, "fields": [{"name": "a", "type": "int"}]},
            {"type": "record", "name": "Second", "namespace": NS, "fields": [{"name": "b", "type": "string"}]},
        ]
        fields = self.fields(self.avro_to_schema(schema, f"{NS}.Second", emit_cloudevents=True))
        self.assertNotIn("a", fields)
        self.assertEqual(fields["b"].type, pa.string())
        self.assertEqual(fields["___type"].type, pa.string())
        self.assertEqual(fields["___source"].type, pa.string())
        self.assertEqual(fields["___id"].type, pa.string())
        self.assertEqual(fields["___time"].type, pa.timestamp("ns"))
        self.assertEqual(fields["___subject"].type, pa.string())

    # -- Parquet -> Avro: full primitive/logical/container range ----------------

    def test_parquet_every_primitive_and_logical_type_maps_to_expected_avro_type(self):
        schema = pa.schema([
            pa.field("f_int8", pa.int8()),
            pa.field("f_int16", pa.int16()),
            pa.field("f_int32", pa.int32()),
            pa.field("f_int64", pa.int64()),
            pa.field("f_uint8", pa.uint8()),
            pa.field("f_uint16", pa.uint16()),
            pa.field("f_uint32", pa.uint32()),
            pa.field("f_uint64", pa.uint64()),
            pa.field("f_float32", pa.float32()),
            pa.field("f_float64", pa.float64()),
            pa.field("f_boolean", pa.bool_()),
            pa.field("f_binary", pa.binary()),
            pa.field("f_string", pa.string()),
            pa.field("f_timestamp_us", pa.timestamp("us")),
            pa.field("f_timestamp_ns", pa.timestamp("ns")),
            pa.field("f_date32", pa.date32()),
            pa.field("f_date64", pa.date64()),
            pa.field("f_decimal", pa.decimal128(9, 3)),
            pa.field("f_time64", pa.time64("us")),
            pa.field("f_unknown", pa.duration("us")),
        ])
        fields = self.avro_fields(self.parquet_to_avro(schema, "parquet_prims"))
        expected = {
            "f_int8": "int",
            "f_int16": "int",
            "f_int32": "int",
            "f_int64": "long",
            "f_uint8": "int",
            "f_uint16": "int",
            "f_uint32": "long",
            "f_uint64": "long",
            "f_float32": "float",
            "f_float64": "double",
            "f_boolean": "boolean",
            "f_binary": "bytes",
            "f_string": "string",
            "f_timestamp_us": {"type": "long", "logicalType": "timestamp-millis"},
            "f_timestamp_ns": {"type": "long", "logicalType": "timestamp-millis"},
            "f_date32": {"type": "int", "logicalType": "date"},
            "f_date64": {"type": "int", "logicalType": "date"},
            "f_decimal": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 3},
            "f_time64": "string",
            "f_unknown": "string",
        }
        for name, typ in expected.items():
            with self.subTest(field=name):
                self.assertEqual(fields[name]["type"], typ)

    def test_parquet_containers_and_structs_map_to_expected_avro_type(self):
        schema = pa.schema([
            pa.field("array_string", pa.list_(pa.string())),
            pa.field("nested_array", pa.list_(pa.list_(pa.int32()))),
            pa.field("map_double", pa.map_(pa.string(), pa.float64())),
            pa.field("struct_field", pa.struct([
                pa.field("id", pa.string()),
                pa.field("counts", pa.list_(pa.int64())),
            ])),
        ])
        fields = self.avro_fields(self.parquet_to_avro(schema, "containers"))
        self.assertEqual(fields["array_string"]["type"], {"type": "array", "items": "string"})
        self.assertEqual(fields["nested_array"]["type"], {"type": "array", "items": {"type": "array", "items": "int"}})
        self.assertEqual(fields["map_double"]["type"], {"type": "map", "values": "double"})
        self.assertEqual(
            fields["struct_field"]["type"],
            {
                "type": "record",
                "name": "struct_fieldType",
                "namespace": NS,
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "counts", "type": {"type": "array", "items": "long"}},
                ],
            },
        )

    def test_parquet_metadata_description_is_preserved_as_avro_doc(self):
        schema = pa.schema([pa.field("documented", pa.string(), metadata={b"description": "caf\u00e9 \u2764"})])
        fields = self.avro_fields(self.parquet_to_avro(schema, "docs"))
        self.assertEqual(fields["documented"]["doc"], "caf\u00e9 \u2764")

    # -- round-trip fidelity / lossy behavior ----------------------------------

    def test_avro_parquet_avro_round_trip_pins_lossy_logical_and_union_behavior(self):
        avro_schema = {
            "type": "record",
            "name": "RoundTrip",
            "namespace": NS,
            "fields": [
                {"name": "decimal_field", "type": {"type": "bytes", "logicalType": "decimal", "precision": 7, "scale": 2}},
                {"name": "timestamp_micros", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                {"name": "time_micros", "type": {"type": "long", "logicalType": "time-micros"}},
                {"name": "choice", "type": ["string", "int"]},
            ],
        }
        parquet_schema = self.avro_to_schema(avro_schema)
        round_tripped = self.parquet_to_avro(parquet_schema, "round_trip")
        fields = self.avro_fields(round_tripped)
        self.assertEqual(fields["decimal_field"]["type"], {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 18})
        self.assertEqual(fields["timestamp_micros"]["type"], {"type": "long", "logicalType": "timestamp-millis"})
        self.assertEqual(fields["time_micros"]["type"], "string")
        self.assertEqual(
            fields["choice"]["type"],
            {
                "type": "record",
                "name": "choiceType",
                "namespace": NS,
                "fields": [{"name": "stringValue", "type": "string"}, {"name": "intValue", "type": "int"}],
            },
        )

    def test_json_structure_parquet_avro_round_trip_pins_lossy_unsigned_set_and_temporal_behavior(self):
        struct_schema = {
            "type": "object",
            "properties": {
                "u8": {"type": "uint8"},
                "u32": {"type": "uint32"},
                "u64": {"type": "uint64"},
                "set_values": {"type": "set", "items": {"type": "string"}},
                "ts": {"type": "timestamp"},
                "time_value": {"type": "time"},
            },
            "required": ["u8", "u32", "u64", "set_values", "ts", "time_value"],
        }
        parquet_schema = self.structure_to_schema(struct_schema)
        round_tripped = self.parquet_to_avro(parquet_schema, "struct_round_trip")
        fields = self.avro_fields(round_tripped)
        self.assertEqual(fields["u8"]["type"], "int")
        self.assertEqual(fields["u32"]["type"], "long")
        self.assertEqual(fields["u64"]["type"], "long")
        self.assertEqual(fields["set_values"]["type"], {"type": "array", "items": "string"})
        self.assertEqual(fields["ts"]["type"], {"type": "long", "logicalType": "timestamp-millis"})
        self.assertEqual(fields["time_value"]["type"], "string")

    # -- adversarial / error paths ---------------------------------------------

    def test_missing_input_files_raise_documented_exceptions(self):
        with self.assertRaises(FileNotFoundError):
            convert_structure_to_parquet(str(self.tmp / "missing.struct.json"), None, str(self.tmp / "out.parquet"))
        with self.assertRaises(FileNotFoundError):
            convert_avro_to_parquet(str(self.tmp / "missing.avsc"), None, str(self.tmp / "out.parquet"))
        with self.assertRaises(FileNotFoundError):
            convert_parquet_to_avro(str(self.tmp / "missing.parquet"), str(self.tmp / "out.avsc"))

    def test_malformed_json_inputs_raise_jsondecodeerror(self):
        bad_struct = self.tmp / "bad.struct.json"
        bad_struct.write_text("{not json", encoding="utf-8")
        with self.assertRaises(json.JSONDecodeError):
            convert_structure_to_parquet(str(bad_struct), None, str(self.tmp / "bad.parquet"))

        bad_avro = self.tmp / "bad.avsc"
        bad_avro.write_text("{not json", encoding="utf-8")
        with self.assertRaises(json.JSONDecodeError):
            convert_avro_to_parquet(str(bad_avro), None, str(self.tmp / "bad.parquet"))

    def test_invalid_top_level_schemas_raise_systemexit(self):
        src = self.tmp / "not_object.struct.json"
        src.write_text(json.dumps({"type": "array", "items": {"type": "string"}}), encoding="utf-8")
        with self.assertRaises(SystemExit):
            convert_structure_to_parquet(str(src), None, str(self.tmp / "out.parquet"))

        avro_src = self.tmp / "not_record.avsc"
        avro_src.write_text(json.dumps("string"), encoding="utf-8")
        with self.assertRaises(SystemExit):
            convert_avro_to_parquet(str(avro_src), None, str(self.tmp / "out.parquet"))

    def test_unknown_types_empty_records_and_empty_unions_degrade_to_string(self):
        schema = self.structure_to_schema(
            {
                "type": "object",
                "properties": {
                    "unknown": {"type": "made_up"},
                    "empty_union": [],
                    "empty_choice": {"type": "choice", "choices": "bad"},
                },
            }
        )
        fields = self.fields(schema)
        self.assertEqual(fields["unknown"].type, pa.string())
        self.assertEqual(fields["empty_union"].type, pa.string())
        self.assertEqual(fields["empty_choice"].type, pa.string())

        avro_fields = self.fields(self.avro_to_schema({
            "type": "record",
            "name": "Fallbacks",
            "fields": [
                {"name": "empty_record", "type": {"type": "record", "name": "Empty", "fields": []}},
                {"name": "unknown_named", "type": "does.not.Exist"},
            ],
        }))
        self.assertEqual(avro_fields["empty_record"].type, pa.string())
        self.assertEqual(avro_fields["unknown_named"].type, pa.string())

    def test_reserved_duplicate_and_unicode_column_names_are_preserved_or_cleanly_rejected(self):
        schema = self.structure_to_schema(
            {
                "type": "object",
                "properties": {
                    "class": {"type": "string"},
                    "name": {"type": "int32"},
                    "na\u00efve": {"type": "string"},
                    "\u503c": {"type": "double"},
                },
                "required": ["class", "name", "na\u00efve", "\u503c"],
            }
        )
        self.assertEqual(schema.names, ["class", "name", "na\u00efve", "\u503c"])
        self.assertEqual(self.fields(schema)["\u503c"].type, pa.float64())

        duplicate_schema = pa.schema([pa.field("dup", pa.int32()), pa.field("dup", pa.string())])
        with self.assertRaises((pa.ArrowInvalid, OSError)):
            self.parquet_to_avro(duplicate_schema, "duplicates")

    def test_unicode_field_values_do_not_affect_schema_conversion(self):
        schema = self.structure_to_schema(
            {
                "type": "object",
                "properties": {
                    "label": {"type": "string", "default": "caf\u00e9 \u2764"},
                    "emoji_\U0001f680": {"type": "uuid"},
                },
                "required": ["label", "emoji_\U0001f680"],
            }
        )
        fields = self.fields(schema)
        self.assertEqual(fields["label"].type, pa.string())
        self.assertEqual(fields["emoji_\U0001f680"].type, pa.string())
        self.assertEqual(schema.names, ["label", "emoji_\U0001f680"])


if __name__ == "__main__":
    unittest.main()
