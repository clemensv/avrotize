import os
import sys
from os import path, getcwd

import pytest

from avrotize.avrotoproto import convert_avro_to_proto
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch

class TestAvroToProto(unittest.TestCase):
    def test_convert_address_avsc_to_tsql(self):
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        proto_path = os.path.join(cwd, "test", "tmp", "address.proto")
        dir = os.path.dirname(proto_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_avro_to_proto(avro_path, proto_path)           

    def test_convert_telemetry_avsc_to_tsql(self):
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        proto_path = os.path.join(cwd, "test", "tmp", "telemetry.proto")
        dir = os.path.dirname(proto_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_avro_to_proto(avro_path, proto_path)

    def test_convert_jfrog_pipelines_jsons_to_avro_to_proto(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(cwd, "test", "tmp", "jfrog-pipelines.avsc")
        proto_path = path.join(cwd, "test", "tmp", "jfrog-pipelines-proto")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_proto(avro_path, proto_path)
