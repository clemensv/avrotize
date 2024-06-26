import os
import sys
import tempfile
from os import path, getcwd

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize import convert_proto_to_avro

class TestProtoToAvro(unittest.TestCase):
    def test_convert_proto_to_avro(self):
        cwd = getcwd()        
        proto_path = path.join(cwd, "test", "gtfsrt", "gtfsrt.proto")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "gtfsrt.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_proto_to_avro(proto_path, avro_path)           

    def test_convert_proto_with_import_to_avro(self):
        cwd = getcwd()        
        proto_path = path.join(cwd, "test", "proto", "user.proto")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "user.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_proto_to_avro(proto_path, avro_path)
        
    def test_convert_proto_within_oneof_to_avro(self):
        cwd = getcwd()        
        proto_path = path.join(cwd, "test", "proto", "oneoftest.proto")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "oneoftest.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_proto_to_avro(proto_path, avro_path)

if __name__ == '__main__':
    unittest.main()