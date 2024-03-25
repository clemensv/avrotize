import json
import os
import sys
from os import path, getcwd
from fastavro.schema import load_schema
from jsoncomparison import NO_DIFF, Compare
import pytest
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch


class TestJsonsToAvro(unittest.TestCase):

    def validate_avro_schema(self, avro_file_path):
        load_schema(avro_file_path)

    def create_avro_from_jsons(self, jsons_path, avro_path = '', avro_ref_path = '', namespace = "com.test.example", split_top_level_records = False):
        cwd = getcwd()
        if jsons_path.startswith("http"):
            jsons_full_path = jsons_path
        else:
            jsons_full_path = path.join(cwd, "test", "jsons", jsons_path)
        if not avro_path:
            avro_path = jsons_path.replace(".jsons", ".avsc").replace(".json", ".avsc")
        if not avro_ref_path:
            # '-ref' appended to the avsc base file name
            avro_ref_full_path = path.join(cwd, "test", "jsons", avro_path.replace(".avsc", "-ref.avsc"))
        else:
            avro_ref_full_path = path.join(cwd, "test", "jsons", "addlprops1-ref.avsc")
        avro_full_path = path.join(cwd, "test", "tmp", avro_path)
        dir = os.path.dirname(avro_full_path) if not split_top_level_records else avro_full_path
        if not os.path.exists(dir):
            os.makedirs(dir)

        convert_jsons_to_avro(jsons_full_path, avro_full_path, namespace, split_top_level_records=split_top_level_records)
        if not split_top_level_records:
            self.validate_avro_schema(avro_full_path)

            if os.path.exists(avro_ref_full_path):
                with open(avro_ref_full_path, "r") as ref:
                    expected = json.loads(ref.read())
                with open(avro_full_path, "r") as ref:
                    actual = json.loads(ref.read())
                diff = Compare().check(actual, expected)
                assert diff == NO_DIFF

        


    def test_convert_rootarray_jsons_to_avro(self):
        self.create_avro_from_jsons("rootarray.jsons", "rootarray.avsc")
        
    def test_convert_arraydef_jsons_to_avro(self):
        self.create_avro_from_jsons("arraydef.json", "arraydef.avsc")
        
    def test_convert_usingrefs_jsons_to_avro(self):
        self.create_avro_from_jsons("usingrefs.json", "usingrefs.avsc")
        
    def test_convert_anyof_jsons_to_avro(self):
        self.create_avro_from_jsons("anyof.json", "anyof.avsc")

    def test_convert_circularrefs_jsons_to_avro(self):
        self.create_avro_from_jsons("circularrefs.json", "circularrefs.avsc")        

    def test_convert_address_jsons_to_avro(self):
        self.create_avro_from_jsons("address.jsons", "address.avsc")

    def test_convert_movie_jsons_to_avro(self):
        self.create_avro_from_jsons("movie.jsons", "movie.avsc")

    def test_convert_person_jsons_to_avro(self):
        self.create_avro_from_jsons("person.jsons", "person.avsc")

    def test_convert_employee_jsons_to_avro(self):
        self.create_avro_from_jsons("employee.jsons", "employee.avsc")

    def test_convert_azurestorage_jsons_to_avro(self):
        self.create_avro_from_jsons("azurestorage.jsons", "azurestorage.avsc", namespace="Microsoft.Azure.Storage")

    def test_convert_azurestorage_jsons_to_avro_split(self):
        self.create_avro_from_jsons("azurestorage.jsons", "azurestorage-schemas", namespace="Microsoft.Azure.Storage", split_top_level_records=True)

    def test_convert_azurestorage_remote_jsons_to_avro(self):
        jsons_path = "https://raw.githubusercontent.com:443/Azure/azure-rest-api-specs/master/specification/eventgrid/data-plane/Microsoft.Storage/stable/2018-01-01/Storage.json"
        self.create_avro_from_jsons(jsons_path, "azurestorage.avsc", namespace="Microsoft.Azure.Storage")

    def test_convert_azurestorage_remote_deeplink_jsons_to_avro(self):
        jsons_path = "https://raw.githubusercontent.com:443/Azure/azure-rest-api-specs/master/specification/eventgrid/data-plane/Microsoft.Storage/stable/2018-01-01/Storage.json#/definitions/StorageLifecyclePolicyCompletedEventData"
        self.create_avro_from_jsons(jsons_path, "azurestoragedeep.avsc", namespace="Microsoft.Azure.Storage")

    def test_convert_addlprops1_jsons_to_avro(self):
        self.create_avro_from_jsons("addlprops1.json", "addlprops1.avsc")

    def test_convert_addlprops2_jsons_to_avro(self):
        self.create_avro_from_jsons("addlprops2.json", "addlprops2.avsc")

    def test_convert_addlprops3_jsons_to_avro(self):
        self.create_avro_from_jsons("addlprops3.json", "addlprops3.avsc")
        
    def test_convert_patternprops1_jsons_to_avro(self):
        self.create_avro_from_jsons("patternprops1.json", "patternprops1.avsc")

    def test_convert_patternprops2_jsons_to_avro(self):
        self.create_avro_from_jsons("patternprops2.json", "patternprops2.avsc")
    
    def test_convert_composition_jsons_to_avro(self):
        self.create_avro_from_jsons("composition.json", "composition.avsc")

    def test_convert_avro_avsc_jsons_to_avro(self):
        self.create_avro_from_jsons("avro-avsc.json", "avro-avsc.avsc")

    def test_convert_clouidify_jsons_to_avro(self):
        self.create_avro_from_jsons("cloudify.json", "cloudify.avsc")

    def test_convert_databricks_asset_bundles_to_avro(self):
        self.create_avro_from_jsons("databricks-asset-bundles.json", "databricks-asset-bundles.avsc")
        
    def test_convert_jfrog_pipelines_to_avro(self):
        self.create_avro_from_jsons("jfrog-pipelines.json", "jfrog-pipelines.avsc")

    def test_convert_kubernetes_definitions_jsons_to_avro(self):
        self.create_avro_from_jsons("kubernetes-definitions.json", "kubernetes-definitions.avsc")

    def test_convert_travis_jsons_to_avro(self):
        self.create_avro_from_jsons("travis.json", "travis.avsc")
