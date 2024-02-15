import os
import sys
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
        avro_path = path.join(cwd, "test", "tmp", "gtfsrt.avsc")
        
        convert_proto_to_avro(proto_path, avro_path)           

if __name__ == '__main__':
    unittest.main()