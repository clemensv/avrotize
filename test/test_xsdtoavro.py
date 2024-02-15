from os import getcwd, path
import unittest
from unittest.mock import patch
from avrotize.xsdtoavro import convert_xsd_to_avro

class TestXsdToAvro(unittest.TestCase):
    def test_convert_address_jsons_to_avro(self):
        cwd = getcwd()        
        xsd_path = path.join(cwd, "test", "xsd", "crmdata.xsd")
        avro_path = path.join(cwd, "test", "tmp", "crmdata.avsc")
        
        convert_xsd_to_avro(xsd_path, avro_path)           
