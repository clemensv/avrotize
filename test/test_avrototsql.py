import os
import sys
from os import path, getcwd

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize.avrototsql import convert_avro_to_tsql

class TestAvroToTSQL(unittest.TestCase):
    def test_convert_address_avsc_to_tsql(self):
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        kql_path = os.path.join(cwd, "test", "tmp", "address.sql")
        dir = os.path.dirname(kql_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_avro_to_tsql(avro_path, None, kql_path, False)           

    def test_convert_telemetry_avsc_to_tsql(self):
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        kql_path = os.path.join(cwd, "test", "tmp", "telemetry.sql")
        dir = os.path.dirname(kql_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_avro_to_tsql(avro_path, None, kql_path, True)

    
