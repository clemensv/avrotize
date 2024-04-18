import os
import sys
from os import path, getcwd
from fastavro.schema import load_schema

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize.xsdtoavro import convert_xsd_to_avro

class TestXsdToAvro(unittest.TestCase):

    def validate_avro_schema(self, avro_file_path):
        load_schema(avro_file_path)

    def test_convert_crmdata_xsd_to_avro(self):
        cwd = os.getcwd()        
        xsd_path = os.path.join(cwd, "test", "xsd", "crmdata.xsd")
        avro_path = os.path.join(cwd, "test", "tmp", "crmdata.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_xsd_to_avro(xsd_path, avro_path)           
        self.validate_avro_schema(avro_path)

    def test_convert_iso20022_xsd_to_avro1(self):
        cwd = os.getcwd()        
        xsd_path = os.path.join(cwd, "test", "xsd", "acmt.003.001.08.xsd")
        avro_path = os.path.join(cwd, "test", "tmp", "acmt.003.001.08.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_xsd_to_avro(xsd_path, avro_path)
        self.validate_avro_schema(avro_path)

    def test_convert_iso20022_xsd_to_avro2(self):
        cwd = os.getcwd()        
        xsd_path = os.path.join(cwd, "test", "xsd", "admi.017.001.01.xsd")
        avro_path = os.path.join(cwd, "test", "tmp", "admi.017.001.01.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_xsd_to_avro(xsd_path, avro_path)
        self.validate_avro_schema(avro_path)

    
