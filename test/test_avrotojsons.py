import json
import os
import sys
import tempfile
from os import path, getcwd
from avrotize.avrotojsons import convert_avro_to_json_schema
from jsoncomparison import NO_DIFF, Compare
import unittest


class TestAvroToJsons(unittest.TestCase):

    def validate_json_schema(self, json_file_path):
        with open(json_file_path, 'r') as file:
            json.load(file)  # Ensure the JSON schema is valid

    def create_json_from_avro(self, avro_path, json_path='', naming_mode='default'):
        cwd = getcwd()
        if not json_path:
            json_path = avro_path.replace(".avsc", ".json")
        avro_full_path = path.join(cwd, "test", "avsc", avro_path)
        json_full_path = path.join(tempfile.gettempdir(), "avrotize", json_path)
        dir = os.path.dirname(json_full_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)

        convert_avro_to_json_schema(avro_full_path, json_full_path, naming_mode=naming_mode)
        self.validate_json_schema(json_full_path)

        # Compare with reference JSON schema if available
        json_ref_path = avro_full_path.replace(".avsc", "-ref.json")
        if os.path.exists(json_ref_path):
            with open(json_ref_path, "r") as ref:
                expected = json.loads(ref.read())
            with open(json_full_path, "r") as actual_file:
                actual = json.loads(actual_file.read())
            diff = Compare().check(actual, expected)
            assert diff == NO_DIFF

    def test_convert_address_nn_avro_to_json(self):
        self.create_json_from_avro("address-nn.avsc", "address-nn.json")

    def test_convert_address_avro_to_json(self):
        self.create_json_from_avro("address.avsc", "address.json")

    def test_convert_complexunion_avro_to_json(self):
        self.create_json_from_avro("complexunion.avsc", "complexunion.json")

    def test_convert_enumfield_ordinals_avro_to_json(self):
        self.create_json_from_avro("enumfield-ordinals.avsc", "enumfield-ordinals.json")

    def test_convert_enumfield_avro_to_json(self):
        self.create_json_from_avro("enumfield.avsc", "enumfield.json")

    def test_convert_feeditem_avro_to_json(self):
        self.create_json_from_avro("feeditem.avsc", "feeditem.json")

    def test_convert_fileblob_avro_to_json(self):
        self.create_json_from_avro("fileblob.avsc", "fileblob.json")

    def test_convert_northwind_avro_to_json(self):
        self.create_json_from_avro("northwind.avsc", "northwind.json")

    def test_convert_primitiveunion_avro_to_json(self):
        self.create_json_from_avro("primitiveunion.avsc", "primitiveunion.json")

    def test_convert_telemetry_avro_to_json(self):
        self.create_json_from_avro("telemetry.avsc", "telemetry.json")

    def test_convert_twotypeunion_avro_to_json(self):
        self.create_json_from_avro("twotypeunion.avsc", "twotypeunion.json")

    def test_convert_typemapunion_avro_to_json(self):
        self.create_json_from_avro("typemapunion.avsc", "typemapunion.json")

    def test_convert_typemapunion2_avro_to_json(self):
        self.create_json_from_avro("typemapunion2.avsc", "typemapunion2.json")