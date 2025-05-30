from distutils.dir_util import copy_tree
from unittest.mock import patch
import unittest
import os
import shutil
import subprocess
import sys
import tempfile
from os import path, getcwd

import pytest

from avrotize.avrotocsharp import convert_avro_to_csharp
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestAvroToCSharp(unittest.TestCase):
    
    def run_convert_avsc_to_csharp(self, avsc_name, system_text_json_annotation=False, newtonsoft_json_annotation=False, avro_annotation=False, system_xml_annotation=False, pascal_properties=False):
        """ Test converting an avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", avsc_name + ".avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", avsc_name + "-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_avro_to_csharp(avro_path, cs_path, pascal_properties=pascal_properties, system_text_json_annotation=system_text_json_annotation, newtonsoft_json_annotation=newtonsoft_json_annotation, avro_annotation=avro_annotation, system_xml_annotation=system_xml_annotation)
        assert subprocess.check_call(
            ['dotnet', 'test'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0
    
    def test_convert_address_avsc_to_csharp(self):
        """ Test converting an address.avsc file to C# """
        self.run_convert_avsc_to_csharp("address")
   
    def test_convert_address_avsc_to_csharp_annotated(self):
        for system_text_json_annotation in [True, False]:
            for newtonsoft_json_annotation in [True, False]:
                for avro_annotation in [True, False]:
                    for pascal_properties in [True, False]:
                        for system_xml_annotation in [True, False]:
                            self.run_convert_avsc_to_csharp("address", system_text_json_annotation=system_text_json_annotation, newtonsoft_json_annotation=newtonsoft_json_annotation, avro_annotation=avro_annotation, pascal_properties=pascal_properties, system_xml_annotation=system_xml_annotation)

                                        
    def test_convert_enumfield_avsc_to_csharp_annotated(self):
        """ Test converting an enumfield.avsc file to C# """
        for system_text_json_annotation in [True, False]:
            for newtonsoft_json_annotation in [True, False]:
                for avro_annotation in [True, False]:
                    for pascal_properties in [True, False]:
                        for system_xml_annotation in [True, False]:
                            self.run_convert_avsc_to_csharp("enumfield", system_text_json_annotation=system_text_json_annotation, newtonsoft_json_annotation=newtonsoft_json_annotation, avro_annotation=avro_annotation, pascal_properties=pascal_properties, system_xml_annotation=system_xml_annotation)


    def test_convert_telemetry_avsc_to_csharp_annotated(self):
        """ Test converting an telemetry.avsc file to C# """
        for system_text_json_annotation in [True, False]:
            for newtonsoft_json_annotation in [True, False]:
                for avro_annotation in [True, False]:
                    for pascal_properties in [True, False]:
                        for system_xml_annotation in [True, False]:
                            self.run_convert_avsc_to_csharp("telemetry", system_text_json_annotation=system_text_json_annotation, newtonsoft_json_annotation=newtonsoft_json_annotation, avro_annotation=avro_annotation, pascal_properties=pascal_properties, system_xml_annotation=system_xml_annotation)
        
    def test_convert_address_nn_avsc_to_csharp(self):
        """ Test converting an address.nn.avsc file to C# """
        self.run_convert_avsc_to_csharp("address-nn")
        
    def test_convert_twotypeunion_ann_avsc_to_csharp(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "twotypeunion.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "twotypeunion-cs")
        cs_test_path = os.path.join(tempfile.gettempdir(), "avrotize", "twotypeunion-cs-test")
        test_csproj = os.path.join(cwd, "test", "cs", "twotypeunion")
        if os.path.exists(cs_test_path):
            shutil.rmtree(cs_test_path, ignore_errors=True)
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_test_path, exist_ok=True)
        os.makedirs(cs_path, exist_ok=True)
        
        copy_tree(test_csproj, cs_test_path)

        convert_avro_to_csharp(avro_path, cs_path, base_namespace="TwoTypeUnion", system_text_json_annotation=True, avro_annotation=True, pascal_properties=True)
        assert subprocess.check_call(
            ['dotnet', 'run', '--force'], cwd=cs_test_path, stdout=sys.stdout, stderr=sys.stderr) == 0
        
    def run_test_convert_twotypeunion_avsc_to_csharp(self, system_text_json_annotation=True, newtonsoft_json_annotation=True, avro_annotation=True, pascal_properties=True, system_xml_annotation=True):
        """ Test converting a twotypeunion.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "twotypeunion.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "twotypeunion-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_avro_to_csharp(avro_path, cs_path, base_namespace="TwoTypeUnion", system_text_json_annotation=system_text_json_annotation, newtonsoft_json_annotation=newtonsoft_json_annotation, avro_annotation=avro_annotation, pascal_properties=pascal_properties, system_xml_annotation=system_xml_annotation)
        assert subprocess.check_call(
            ['dotnet', 'build'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0
        
    def test_convert_twotypeunion_avsc_to_csharp_annotated(self):
        """ Test converting a twotypeunion.avsc file to C# """
        for system_text_json_annotation in [True, False]:
            for newtonsoft_json_annotation in [True, False]:
                for avro_annotation in [True, False]:
                    for pascal_properties in [True, False]:
                        for system_xml_annotation in [True, False]:
                            self.run_test_convert_twotypeunion_avsc_to_csharp(system_text_json_annotation=system_text_json_annotation, newtonsoft_json_annotation=newtonsoft_json_annotation, avro_annotation=avro_annotation, pascal_properties=pascal_properties, system_xml_annotation=system_xml_annotation)
        
        
    def test_convert_typemapunion_avsc_to_csharp(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "typemapunion.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize",  "typemapunion-cs")
        cs_test_path = os.path.join(tempfile.gettempdir(), "avrotize", "typemapunion-cs-test")
        test_csproj = os.path.join(cwd, "test", "cs", "typemapunion")
        
        if os.path.exists(cs_test_path):
            shutil.rmtree(cs_test_path, ignore_errors=True)
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_test_path, exist_ok=True)
        os.makedirs(cs_path, exist_ok=True)
        
        copy_tree(test_csproj, cs_test_path)

        convert_avro_to_csharp(avro_path, cs_path, base_namespace="TypeMapUnion", system_text_json_annotation=True, pascal_properties=True)
        assert subprocess.check_call(
            ['dotnet', 'run', '--force'], cwd=cs_test_path, stdout=sys.stdout, stderr=sys.stderr) == 0
        
    def test_convert_typemapunion2_avsc_to_csharp(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "typemapunion2.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "typemapunion2-cs")
        #test_csproj = os.path.join(cwd, "test", "cs", "typemapunion2")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_avro_to_csharp(avro_path, cs_path, base_namespace="TypeNameUnion", system_text_json_annotation=True, pascal_properties=True)
        assert subprocess.check_call(['dotnet', 'build', '--force'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0

    def test_convert_primitiveunion_avsc_to_csharp(self):
        """ Test converting an primitiveunion.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "primitiveunion.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "primitiveunion-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_avro_to_csharp(avro_path, cs_path, system_text_json_annotation=True, avro_annotation=True, pascal_properties=True)
        assert subprocess.check_call(['dotnet', 'build', '--force'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0


    def test_convert_jfrog_pipelines_jsons_to_avro_to_csharp(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        cs_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_csharp(avro_path, cs_path)
        assert subprocess.check_call(
            ['dotnet', 'build'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0

    def test_convert_jfrog_pipelines_jsons_to_avro_to_csharp_annotated(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        cs_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-cs-ann")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_csharp(avro_path, cs_path, pascal_properties=True, avro_annotation=True,
                               system_text_json_annotation=True, newtonsoft_json_annotation=True, system_xml_annotation=True)
        assert subprocess.check_call(['dotnet', 'build'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0
