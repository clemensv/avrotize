import os
import sys
from os import path, getcwd
from fastavro.schema import load_schema

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize.asn1toavro import convert_asn1_to_avro


class TestAsn1ToAvro(unittest.TestCase):

    def validate_avro_schema(self, avro_file_path):
        load_schema(avro_file_path)

    def test_convert_address_asn_to_avro(self):
        cwd = os.getcwd()        
        asn1_path = os.path.join(cwd, "test", "asn1", "person.asn")
        avro_path = os.path.join(cwd, "test", "tmp", "personasn.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_asn1_to_avro(asn1_path, avro_path)           
        self.validate_avro_schema(avro_path)    

    def test_convert_movie_asn_to_avro(self):
        cwd = os.getcwd()        
        asn1_path = os.path.join(cwd, "test", "asn1", "movie.asn")
        avro_path = os.path.join(cwd, "test", "tmp", "movieasn.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        convert_asn1_to_avro(asn1_path, avro_path)
        self.validate_avro_schema(avro_path)    

    def test_convert_ldap3_asn_to_avro(self):
        cwd = os.getcwd()        
        asn1_path = os.path.join(cwd, "test", "asn1", "ldap3.asn")
        avro_path = os.path.join(cwd, "test", "tmp", "ldapasn.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        convert_asn1_to_avro(asn1_path, avro_path)
        self.validate_avro_schema(avro_path)    



