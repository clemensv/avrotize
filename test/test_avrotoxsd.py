import os
import sys
from os import path, getcwd

from test.test_jsontoavro import TestJsonsToAvro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize.avrotoxsd import convert_avro_to_xsd


class TestAvroToXsd(unittest.TestCase):
    
    def create_xsd_from_avro(self, avro_path, xsd_path = '', xsd_ref_path = '', namespace = "com.test.example"):
        cwd = getcwd()
        if avro_path.startswith("http"):
            avro_full_path = avro_path
        else:
            avro_full_path = path.join(cwd, "test", "avsc", avro_path)
        if not xsd_path:
            xsd_path = avro_path.replace(".avsc", ".xsd")
        if not xsd_ref_path:
            # '-ref' appended to the avsc base file name
            avro_ref_full_path = path.join(cwd, "test", "avsc", xsd_path.replace(".xsd", "-ref.xsd"))
        else:
            avro_ref_full_path = path.join(cwd, "test", "avsc", xsd_ref_path)
        xsd_full_path = path.join(cwd, "test", "tmp", xsd_path)
        dir = os.path.dirname(avro_full_path)
        if not os.path.exists(dir):
            os.makedirs(dir)

        convert_avro_to_xsd(avro_full_path, xsd_full_path)
        
    def create_xsd_from_jsons(self, jsons_path, xsd_path = '', xsd_ref_path = '', namespace = "com.test.example"):
        cwd = getcwd()
        j2a = TestJsonsToAvro()        
        avro_path = path.join(cwd, "test", "tmp", jsons_path.replace(".jsons", ".avsc").replace(".json", ".avsc"))
        j2a.create_avro_from_jsons(jsons_path, avro_path)            
        self.create_xsd_from_avro(avro_path, xsd_path, xsd_ref_path, namespace)
        
    
    def test_convert_address_avsc_to_xsd(self):
        self.create_xsd_from_avro("address.avsc")

    def test_convert_telemetry_avsc_to_xsd(self):
        self.create_xsd_from_avro("telemetry.avsc")
        
    def test_convert_azurestorage_json_to_xsd(self):
        self.create_xsd_from_jsons("azurestorage.jsons")

    
