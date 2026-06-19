import json
import os
import sys
import tempfile
from os import path, getcwd
from fastavro.schema import load_schema
from jsoncomparison import NO_DIFF, Compare
import pytest
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch


class TestJsonsToAvro(unittest.TestCase):

    def validate_avro_schema(self, avro_file_path):
        load_schema(avro_file_path)

    def create_avro_from_jsons(self, jsons_path, avro_path = '', avro_ref_path = '', namespace = "com.test.example", split_top_level_records = False):
        cwd = getcwd()
        if jsons_path.startswith("http"):
            jsons_full_path = jsons_path
        else:
            jsons_full_path = path.join(cwd, "test", "jsons", jsons_path)
        if not avro_path:
            avro_path = jsons_path.replace(".jsons", ".avsc").replace(".json", ".avsc")
        if not avro_ref_path:
            # '-ref' appended to the avsc base file name
            avro_ref_full_path = path.join(cwd, "test", "jsons", avro_path.replace(".avsc", "-ref.avsc"))
        else:
            avro_ref_full_path = path.join(cwd, "test", "jsons", "addlprops1-ref.avsc")
        avro_full_path = path.join(tempfile.gettempdir(), "avrotize", avro_path)
        dir = os.path.dirname(avro_full_path) if not split_top_level_records else avro_full_path
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)

        convert_jsons_to_avro(jsons_full_path, avro_full_path, namespace, split_top_level_records=split_top_level_records)
        if not split_top_level_records:
            self.validate_avro_schema(avro_full_path)

            if os.path.exists(avro_ref_full_path):
                with open(avro_ref_full_path, "r") as ref:
                    expected = json.loads(ref.read())
                with open(avro_full_path, "r") as ref:
                    actual = json.loads(ref.read())
                diff = Compare().check(actual, expected)
                assert diff == NO_DIFF

        


    def test_convert_rootarray_jsons_to_avro(self):
        self.create_avro_from_jsons("rootarray.jsons", "rootarray.avsc")
        
    def test_convert_arraydef_jsons_to_avro(self):
        self.create_avro_from_jsons("arraydef.json", "arraydef.avsc")
        
    def test_convert_usingrefs_jsons_to_avro(self):
        self.create_avro_from_jsons("usingrefs.json", "usingrefs.avsc")
        
    def test_convert_anyof_jsons_to_avro(self):
        self.create_avro_from_jsons("anyof.json", "anyof.avsc")

    def test_convert_circularrefs_jsons_to_avro(self):
        self.create_avro_from_jsons("circularrefs.json", "circularrefs.avsc")        

    def test_convert_address_jsons_to_avro(self):
        self.create_avro_from_jsons("address.jsons", "address.avsc")

    def test_convert_movie_jsons_to_avro(self):
        self.create_avro_from_jsons("movie.jsons", "movie.avsc")

    def test_convert_person_jsons_to_avro(self):
        self.create_avro_from_jsons("person.jsons", "person.avsc")

    def test_convert_employee_jsons_to_avro(self):
        self.create_avro_from_jsons("employee.jsons", "employee.avsc")

    def test_convert_azurestorage_jsons_to_avro(self):
        self.create_avro_from_jsons("azurestorage.jsons", "azurestorage.avsc", namespace="Microsoft.Azure.Storage")

    def test_convert_azurestorage_jsons_to_avro_split(self):
        self.create_avro_from_jsons("azurestorage.jsons", "azurestorage-schemas", namespace="Microsoft.Azure.Storage", split_top_level_records=True)

    def test_convert_azurestorage_remote_jsons_to_avro(self):
        jsons_path = "https://raw.githubusercontent.com:443/Azure/azure-rest-api-specs/master/specification/eventgrid/data-plane/Microsoft.Storage/stable/2018-01-01/Storage.json"
        self.create_avro_from_jsons(jsons_path, "azurestorage.avsc", namespace="Microsoft.Azure.Storage")

    def test_convert_azurestorage_remote_deeplink_jsons_to_avro(self):
        jsons_path = "https://raw.githubusercontent.com:443/Azure/azure-rest-api-specs/master/specification/eventgrid/data-plane/Microsoft.Storage/stable/2018-01-01/Storage.json#/definitions/StorageLifecyclePolicyCompletedEventData"
        self.create_avro_from_jsons(jsons_path, "azurestoragedeep.avsc", namespace="Microsoft.Azure.Storage")

    def test_convert_addlprops1_jsons_to_avro(self):
        self.create_avro_from_jsons("addlprops1.json", "addlprops1.avsc")

    def test_convert_addlprops2_jsons_to_avro(self):
        self.create_avro_from_jsons("addlprops2.json", "addlprops2.avsc")

    def test_convert_addlprops3_jsons_to_avro(self):
        self.create_avro_from_jsons("addlprops3.json", "addlprops3.avsc")
        
    def test_convert_patternprops1_jsons_to_avro(self):
        self.create_avro_from_jsons("patternprops1.json", "patternprops1.avsc")

    def test_convert_patternprops2_jsons_to_avro(self):
        self.create_avro_from_jsons("patternprops2.json", "patternprops2.avsc")
    
    def test_convert_composition_jsons_to_avro(self):
        self.create_avro_from_jsons("composition.json", "composition.avsc")

    def test_convert_avro_avsc_jsons_to_avro(self):
        self.create_avro_from_jsons("avro-avsc.json", "avro-avsc.avsc")

    def test_convert_clouidify_jsons_to_avro(self):
        self.create_avro_from_jsons("cloudify.json", "cloudify.avsc")

    def test_convert_databricks_asset_bundles_to_avro(self):
        self.create_avro_from_jsons("databricks-asset-bundles.json", "databricks-asset-bundles.avsc")
        
    def test_convert_jfrog_pipelines_to_avro(self):
        self.create_avro_from_jsons("jfrog-pipelines.json", "jfrog-pipelines.avsc")

    def test_convert_kubernetes_definitions_jsons_to_avro(self):
        self.create_avro_from_jsons("kubernetes-definitions.json", "kubernetes-definitions.avsc")

    def test_convert_travis_jsons_to_avro(self):
        self.create_avro_from_jsons("travis.json", "travis.avsc")
    
    def test_convert_discriminated_union_simple_to_avro(self):
        self.create_avro_from_jsons("discriminated-union-simple.json", "discriminated-union-simple.avsc")
    
    def test_convert_discriminated_union_nested_to_avro(self):
        self.create_avro_from_jsons("discriminated-union-nested.json", "discriminated-union-nested.avsc")
    
    def test_convert_discriminated_union_array_complex_to_avro(self):
        self.create_avro_from_jsons("discriminated-union-array-complex.json", "discriminated-union-array-complex.avsc")
    
    def test_convert_optional_nested_union_to_avro(self):
        self.create_avro_from_jsons("optional-nested-union.json", "optional-nested-union.avsc")
    
    def test_convert_oneof_with_title_to_avro(self):
        self.create_avro_from_jsons("oneof-with-title.json", "oneof-with-title.avsc")

    def test_convert_conditional_schema_patterns_to_avro(self):
        """Test if/then/else conditional schema patterns conversion."""
        self.create_avro_from_jsons("conditional-schema-patterns.json", "conditional-schema-patterns.avsc")


class TestFormatLogicalTypes(unittest.TestCase):
    """Issue #337: JSON Schema string ``format`` values must convert to the
    correct Avro logical types instead of being collapsed to a bare base type.

    Notably ``date-time`` must become ``{"type": "long", "logicalType":
    "timestamp-millis"}`` — a bare ``int``/``long`` loses both the 64-bit width
    (silent overflow) and the temporal semantics.
    """

    def _convert(self, json_schema: dict) -> dict:
        """Convert an in-memory JSON Schema to an Avro schema dict.

        Validates fastavro-parseability as a side effect (raises on invalid).
        """
        tmpdir = tempfile.mkdtemp()
        json_path = os.path.join(tmpdir, "in.json")
        avro_path = os.path.join(tmpdir, "out.avsc")
        with open(json_path, "w", encoding="utf-8") as handle:
            json.dump(json_schema, handle)
        convert_jsons_to_avro(json_path, avro_path, "com.test.example")
        load_schema(avro_path)  # fastavro validity gate
        with open(avro_path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    @staticmethod
    def _field(record: dict, name: str) -> dict:
        return next(f for f in record["fields"] if f["name"] == name)

    @staticmethod
    def _schema(properties: dict, required=None) -> dict:
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "Event",
            "properties": properties,
        }
        if required is not None:
            schema["required"] = required
        return schema

    def test_date_time_maps_to_timestamp_millis(self):
        """The core issue: required date-time -> long/timestamp-millis."""
        record = self._convert(self._schema(
            {"createdTimestamp": {"type": "string", "format": "date-time"}},
            required=["createdTimestamp"],
        ))
        field = self._field(record, "createdTimestamp")
        self.assertEqual(field["type"], {"type": "long", "logicalType": "timestamp-millis"})

    def test_date_time_is_long_not_int(self):
        """Regression guard against the 32-bit overflow: base must be long."""
        record = self._convert(self._schema(
            {"ts": {"type": "string", "format": "date-time"}},
            required=["ts"],
        ))
        field_type = self._field(record, "ts")["type"]
        self.assertIsInstance(field_type, dict)
        self.assertEqual(field_type["type"], "long")
        self.assertNotEqual(field_type["type"], "int")

    def test_date_maps_to_date_logical_type(self):
        """date -> int/date (correct: Avro date is days-since-epoch in an int)."""
        record = self._convert(self._schema(
            {"birthDate": {"type": "string", "format": "date"}},
            required=["birthDate"],
        ))
        field = self._field(record, "birthDate")
        self.assertEqual(field["type"], {"type": "int", "logicalType": "date"})

    def test_time_maps_to_time_millis(self):
        """time -> int/time-millis."""
        record = self._convert(self._schema(
            {"alarm": {"type": "string", "format": "time"}},
            required=["alarm"],
        ))
        field = self._field(record, "alarm")
        self.assertEqual(field["type"], {"type": "int", "logicalType": "time-millis"})

    def test_uuid_maps_to_uuid_logical_type(self):
        """uuid -> string/uuid (preserved, not flattened to bare string)."""
        record = self._convert(self._schema(
            {"id": {"type": "string", "format": "uuid"}},
            required=["id"],
        ))
        field = self._field(record, "id")
        self.assertEqual(field["type"], {"type": "string", "logicalType": "uuid"})

    def test_optional_date_time_is_nullable_logical_type(self):
        """An optional date-time becomes ['null', {long, timestamp-millis}]."""
        record = self._convert(self._schema(
            {"updatedTimestamp": {"type": "string", "format": "date-time"}},
        ))
        field_type = self._field(record, "updatedTimestamp")["type"]
        self.assertIsInstance(field_type, list)
        self.assertIn("null", field_type)
        self.assertIn({"type": "long", "logicalType": "timestamp-millis"}, field_type)

    def test_array_of_date_time(self):
        """An array of date-time keeps the logical type on its items."""
        record = self._convert(self._schema(
            {"timestamps": {
                "type": "array",
                "items": {"type": "string", "format": "date-time"},
            }},
            required=["timestamps"],
        ))
        field_type = self._field(record, "timestamps")["type"]
        self.assertEqual(field_type["type"], "array")
        self.assertEqual(field_type["items"], {"type": "long", "logicalType": "timestamp-millis"})

    def test_multiple_temporal_formats_together(self):
        """A schema mixing several formats converts each one correctly."""
        record = self._convert(self._schema(
            {
                "created": {"type": "string", "format": "date-time"},
                "day": {"type": "string", "format": "date"},
                "at": {"type": "string", "format": "time"},
                "ref": {"type": "string", "format": "uuid"},
            },
            required=["created", "day", "at", "ref"],
        ))
        self.assertEqual(self._field(record, "created")["type"],
                         {"type": "long", "logicalType": "timestamp-millis"})
        self.assertEqual(self._field(record, "day")["type"],
                         {"type": "int", "logicalType": "date"})
        self.assertEqual(self._field(record, "at")["type"],
                         {"type": "int", "logicalType": "time-millis"})
        self.assertEqual(self._field(record, "ref")["type"],
                         {"type": "string", "logicalType": "uuid"})

    def test_fastavro_roundtrip_timestamp_millis(self):
        """Data written/read through the converted schema preserves the instant."""
        from datetime import datetime, timezone
        import io
        from fastavro import writer, reader
        from fastavro.schema import parse_schema

        record_schema = self._convert(self._schema(
            {
                "id": {"type": "string"},
                "createdTimestamp": {"type": "string", "format": "date-time"},
            },
            required=["id", "createdTimestamp"],
        ))
        parsed = parse_schema(record_schema)

        moment = datetime(2021, 6, 4, 12, 30, 45, tzinfo=timezone.utc)
        datum = {"id": "evt-1", "createdTimestamp": moment}
        buffer = io.BytesIO()
        writer(buffer, parsed, [datum])
        buffer.seek(0)
        out = list(reader(buffer))
        self.assertEqual(out[0]["createdTimestamp"], moment)
