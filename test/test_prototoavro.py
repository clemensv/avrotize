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

    def test_convert_proto3_enum_to_avro(self):
        """Test converting a proto3 file with a top-level enum to Avro schema."""
        cwd = getcwd()        
        proto_path = path.join(cwd, "test", "proto", "enum_proto3.proto")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "enum_proto3.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_proto_to_avro(proto_path, avro_path, "Avrotest")
        
        # Verify the schema was created correctly
        import json
        with open(avro_path, 'r') as f:
            schema = json.load(f)
        
        # Check that we have exactly one enum
        self.assertEqual(len(schema), 1)
        self.assertEqual(schema[0]['type'], 'enum')
        self.assertEqual(schema[0]['name'], 'Corpus')
        self.assertEqual(schema[0]['namespace'], 'Avrotest')
        
        # Check that all enum values are present
        expected_symbols = [
            'CORPUS_UNSPECIFIED', 'CORPUS_UNIVERSAL', 'CORPUS_WEB', 
            'CORPUS_IMAGES', 'CORPUS_LOCAL', 'CORPUS_NEWS', 
            'CORPUS_PRODUCTS', 'CORPUS_VIDEO'
        ]
        self.assertEqual(schema[0]['symbols'], expected_symbols)
        
        # Check ordinals
        self.assertEqual(schema[0]['ordinals']['CORPUS_UNSPECIFIED'], 0)
        self.assertEqual(schema[0]['ordinals']['CORPUS_UNIVERSAL'], 1)
        self.assertEqual(schema[0]['ordinals']['CORPUS_WEB'], 2)
        self.assertEqual(schema[0]['ordinals']['CORPUS_IMAGES'], 3)
        self.assertEqual(schema[0]['ordinals']['CORPUS_LOCAL'], 4)
        self.assertEqual(schema[0]['ordinals']['CORPUS_NEWS'], 5)
        self.assertEqual(schema[0]['ordinals']['CORPUS_PRODUCTS'], 6)
        self.assertEqual(schema[0]['ordinals']['CORPUS_VIDEO'], 7)

if __name__ == '__main__':
    unittest.main()