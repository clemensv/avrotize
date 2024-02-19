import os
import sys
from os import path, getcwd
import avro.schema
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch


class TestJsonsToAvro(unittest.TestCase):

    def validate_avro_schema(self, avro_file_path):
        avro.schema.parse(open(avro_file_path, "rb").read())
                

    def test_convert_address_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "address.jsons")
        avro_path = path.join(cwd, "test", "tmp", "address.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")           
        self.validate_avro_schema(avro_path)

    def test_convert_movie_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "movie.jsons")
        avro_path = path.join(cwd, "test", "tmp", "movie.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")
        self.validate_avro_schema(avro_path)

    def test_convert_person_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "person.jsons")
        avro_path = path.join(cwd, "test", "tmp", "person.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")
        self.validate_avro_schema(avro_path)

    def test_convert_employee_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "employee.jsons")
        avro_path = path.join(cwd, "test", "tmp", "employee.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")
        self.validate_avro_schema(avro_path)

    def test_convert_azurestorage_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "azurestorage.jsons")
        avro_path = path.join(cwd, "test", "tmp", "azurestorage.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "microsoft.azure.storage")
        self.validate_avro_schema(avro_path)

    def test_convert_azurestorage_remote_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = "https://raw.githubusercontent.com:443/Azure/azure-rest-api-specs/master/specification/eventgrid/data-plane/Microsoft.Storage/stable/2018-01-01/Storage.json"
        avro_path = path.join(cwd, "test", "tmp", "azurestorage.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "microsoft.azure.storage")
        self.validate_avro_schema(avro_path)

    def test_convert_azurestorage_remote_deeplink_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = "https://raw.githubusercontent.com:443/Azure/azure-rest-api-specs/master/specification/eventgrid/data-plane/Microsoft.Storage/stable/2018-01-01/Storage.json#/definitions/StorageLifecyclePolicyCompletedEventData"
        avro_path = path.join(cwd, "test", "tmp", "azurestoragedeep.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "microsoft.azure.storage")
        self.validate_avro_schema(avro_path)

    def test_convert_addlprops1_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "addlprops1.json")
        avro_path = path.join(cwd, "test", "tmp", "addlprops1.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")           
        self.validate_avro_schema(avro_path)

    def test_convert_addlprops2_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "addlprops2.json")
        avro_path = path.join(cwd, "test", "tmp", "addlprops2.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")
        self.validate_avro_schema(avro_path)

    def test_convert_patternprops1_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "patternprops1.json")
        avro_path = path.join(cwd, "test", "tmp", "patternprops1.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")
        self.validate_avro_schema(avro_path)

    def test_convert_patternprops2_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "patternprops2.json")
        avro_path = path.join(cwd, "test", "tmp", "patternprops2.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")
        self.validate_avro_schema(avro_path)

    # TODO: is causing a recursion error
    # def test_convert_avro_avsc_jsons_to_avro(self):
    #     cwd = getcwd()        
    #     jsons_path = path.join(cwd, "test", "jsons", "avro-avsc.json")
    #     avro_path = path.join(cwd, "test", "tmp", "avro-avsc.json.avsc")
    #     dir = os.path.dirname(avro_path)
    #     if not os.path.exists(dir):
    #         os.makedirs(dir)
        
    #     convert_jsons_to_avro(jsons_path, avro_path)
        #self.validate_avro_schema(avro_path)

    def test_convert_clouidify_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "cloudify.json")
        avro_path = path.join(cwd, "test", "tmp", "cloudify.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        #self.validate_avro_schema(avro_path)

    def test_convert_databricks_asset_bundles_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "databricks-asset-bundles.json")
        avro_path = path.join(cwd, "test", "tmp", "databricks-asset-bundles.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        convert_jsons_to_avro(jsons_path, avro_path)
        self.validate_avro_schema(avro_path)
    
    def test_convert_jfrog_pipelines_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(cwd, "test", "tmp", "jfrog-pipelines.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        convert_jsons_to_avro(jsons_path, avro_path)
        #self.validate_avro_schema(avro_path)

    def test_convert_kubernetes_definitions_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "kubernetes-definitions.json")
        avro_path = path.join(cwd, "test", "tmp", "kubernetes-definitions.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir)
        convert_jsons_to_avro(jsons_path, avro_path)
        #self.validate_avro_schema(avro_path)


        




if __name__ == '__main__':
    unittest.main()