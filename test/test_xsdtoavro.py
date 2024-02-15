import os
import sys
from os import path, getcwd

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize.xsdtoavro import convert_xsd_to_avro

class TestXsdToAvro(unittest.TestCase):
    def test_convert_address_jsons_to_avro(self):
        cwd = os.getcwd()        
        xsd_path = os.path.join(cwd, "test", "xsd", "crmdata.xsd")
        avro_path = os.path.join(cwd, "test", "tmp", "crmdata.avsc")
        
        convert_xsd_to_avro(xsd_path, avro_path)           
