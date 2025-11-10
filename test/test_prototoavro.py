import os
import sys
import tempfile
from os import path, getcwd
import json

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize import convert_proto_to_avro
from jsoncomparison import NO_DIFF, Compare

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
    
    def test_convert_proto_with_buf_style_import(self):
        """Test proto conversion with buf-style imports using proto_root parameter."""
        cwd = getcwd()
        proto_path = path.join(cwd, "test", "proto_buf_test", "proto", "foo", "bar", "bizz.proto")
        proto_root = path.join(cwd, "test", "proto_buf_test", "proto", "foo")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "bizz.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_proto_to_avro(proto_path, avro_path, proto_root=proto_root)
        
        # Verify the output file was created
        self.assertTrue(os.path.exists(avro_path))

    def test_convert_proto3_enum_to_avro(self):
        """Test converting a proto3 file with a top-level enum to Avro schema."""
        cwd = getcwd()        
        proto_path = path.join(cwd, "test", "proto", "enum_proto3.proto")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "enum_proto3.avsc")
        avro_ref_path = path.join(cwd, "test", "proto", "enum_proto3-ref.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_proto_to_avro(proto_path, avro_path, "Avrotest")
        
        # Compare against reference file
        with open(avro_ref_path, "r") as ref:
            expected = json.load(ref)
        with open(avro_path, "r") as actual_file:
            actual = json.load(actual_file)
        diff = Compare().check(actual, expected)
        assert diff == NO_DIFF

if __name__ == '__main__':
    unittest.main()