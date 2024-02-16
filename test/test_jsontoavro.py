import os
import sys
from os import path, getcwd

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize.jsonstoavro import convert_jsons_to_avro

class TestJsonsToAvro(unittest.TestCase):
    def test_convert_address_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "address.jsons")
        avro_path = path.join(cwd, "test", "tmp", "address.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")           

    def test_convert_movie_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "movie.jsons")
        avro_path = path.join(cwd, "test", "tmp", "movie.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")

    def test_convert_person_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "person.jsons")
        avro_path = path.join(cwd, "test", "tmp", "person.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")

    def test_convert_employee_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "employee.jsons")
        avro_path = path.join(cwd, "test", "tmp", "employee.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")

    def test_convert_azurestorage_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "azurestorage.jsons")
        avro_path = path.join(cwd, "test", "tmp", "azurestorage.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "microsoft.azure.storage")

    def test_convert_azurestorage_remote_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = "https://raw.githubusercontent.com:443/Azure/azure-rest-api-specs/master/specification/eventgrid/data-plane/Microsoft.Storage/stable/2018-01-01/Storage.json"
        avro_path = path.join(cwd, "test", "tmp", "azurestorage.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "microsoft.azure.storage")

    def test_convert_azurestorage_remote_deeplink_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = "https://raw.githubusercontent.com:443/Azure/azure-rest-api-specs/master/specification/eventgrid/data-plane/Microsoft.Storage/stable/2018-01-01/Storage.json#/definitions/StorageLifecyclePolicyCompletedEventData"
        avro_path = path.join(cwd, "test", "tmp", "azurestoragedeep.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "microsoft.azure.storage")

    def test_convert_addlprops1_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "addlprops1.json")
        avro_path = path.join(cwd, "test", "tmp", "addlprops1.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")           

    def test_convert_addlprops2_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "addlprops2.json")
        avro_path = path.join(cwd, "test", "tmp", "addlprops2.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")

    def test_convert_patternprops1_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "patternprops1.json")
        avro_path = path.join(cwd, "test", "tmp", "patternprops1.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")

    def test_convert_patternprops2_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "patternprops2.json")
        avro_path = path.join(cwd, "test", "tmp", "patternprops2.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")




if __name__ == '__main__':
    unittest.main()