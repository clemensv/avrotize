import json
import os
import sys
from os import path, getcwd
from fastavro.schema import load_schema
from jsoncomparison import NO_DIFF, Compare
import pytest
from avrotize.kstructtoavro import convert_kafka_struct_to_avro_schema

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch


class TestKStructToAvro(unittest.TestCase):

    def validate_avro_schema(self, avro_file_path):
        load_schema(avro_file_path)

    def create_avro_from_kstruct(self, kstruct_path, avro_path = '', avro_ref_path = '', namespace = "com.test.example"):
        cwd = getcwd()
        if kstruct_path.startswith("http"):
            jsons_full_path = kstruct_path
        else:
            jsons_full_path = path.join(cwd, "test", "kstruct", kstruct_path)
        if not avro_path:
            avro_path = kstruct_path.replace(".json", ".avsc")
        if not avro_ref_path:
            # '-ref' appended to the avsc base file name
            avro_ref_full_path = path.join(cwd, "test", "kstruct", avro_path.replace(".avsc", "-ref.avsc"))
        else:
            avro_ref_full_path = path.join(cwd, "test", "kstruct", "addlprops1-ref.avsc")
        avro_full_path = path.join(cwd, "test", "tmp", avro_path)
        dir = os.path.dirname(avro_full_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)

        convert_kafka_struct_to_avro_schema(jsons_full_path, avro_full_path)
        self.validate_avro_schema(avro_full_path)

        if os.path.exists(avro_ref_full_path):
            with open(avro_ref_full_path, "r") as ref:
                expected = json.loads(ref.read())
            with open(avro_full_path, "r") as ref:
                actual = json.loads(ref.read())
            diff = Compare().check(actual, expected)
            assert diff == NO_DIFF

    def test_convert_cardata_json_to_avro(self):
        self.create_avro_from_kstruct("cardata.json", "cardata.avsc")

    def test_convert_players_json_to_avro(self):
        self.create_avro_from_kstruct("players.json", "players.avsc")