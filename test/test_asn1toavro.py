import os
import sys
from os import path, getcwd

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize.asn1toavro import convert_asn1_to_avro

class TestAsn1ToAvro(unittest.TestCase):
    def test_convert_address_asn_to_avro(self):
        cwd = os.getcwd()        
        asn1_path = os.path.join(cwd, "test", "asn1", "person.asn")
        avro_path = os.path.join(cwd, "test", "tmp", "personasn.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_asn1_to_avro(asn1_path, avro_path)           

    def test_convert_movie_asn_to_avro(self):
        cwd = os.getcwd()        
        asn1_path = os.path.join(cwd, "test", "asn1", "movie.asn")
        avro_path = os.path.join(cwd, "test", "tmp", "movieasn.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        convert_asn1_to_avro(asn1_path, avro_path)
