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
    def test_convert_address_avsc_to_csharp(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_avro_to_csharp(avro_path, cs_path)
        assert subprocess.check_call(
            ['dotnet', 'build'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0

    def test_convert_address_avsc_to_csharp_avro_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-cs-avro")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_avro_to_csharp(avro_path, cs_path, avro_annotation=True)
        assert subprocess.check_call(
            ['dotnet', 'build'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0

    def test_convert_address_avsc_to_csharp_system_text_json_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-cs-stj")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_avro_to_csharp(
            avro_path, cs_path, system_text_json_annotation=True, pascal_properties=True)
        assert subprocess.check_call(
            ['dotnet', 'build'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0
        
    def test_convert_enumfield_avsc_to_csharp_system_text_json_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "enumfield.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "enumfield-cs-stj")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_avro_to_csharp(
            avro_path, cs_path, system_text_json_annotation=True, pascal_properties=True)
        assert subprocess.check_call(
            ['dotnet', 'build'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0

    def test_convert_address_avsc_to_csharp_newtonsoft_json_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-cs-nj")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_avro_to_csharp(
            avro_path, cs_path, newtonsoft_json_annotation=True, pascal_properties=True)
        assert subprocess.check_call(
            ['dotnet', 'build'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0

    def test_convert_telemetry_avsc_to_csharp(self):
        """ Test converting a telemetry.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_avro_to_csharp(avro_path, cs_path)
        assert subprocess.check_call(
            ['dotnet', 'build'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0
        
    def test_convert_twotypeunion_avsc_to_csharp(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "twotypeunion.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "twotypeunion-cs-test", "twotypeunion-cs")
        cs_test_path = os.path.join(tempfile.gettempdir(), "avrotize", "twotypeunion-cs-test")
        test_csproj = os.path.join(cwd, "test", "cs", "twotypeunion")
        if os.path.exists(cs_test_path):
            shutil.rmtree(cs_test_path, ignore_errors=True)
        os.makedirs(cs_test_path, exist_ok=True)
        os.makedirs(cs_path, exist_ok=True)
        
        copy_tree(test_csproj, cs_test_path)

        convert_avro_to_csharp(avro_path, cs_path, system_text_json_annotation=True, pascal_properties=True)
        assert subprocess.check_call(
            ['dotnet', 'run', '--force'], cwd=cs_test_path, stdout=sys.stdout, stderr=sys.stderr) == 0
        
    def test_convert_typemapunion_avsc_to_csharp(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "typemapunion.avsc")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize",  "typemapunion-cs-test", "typemapunion-cs")
        cs_test_path = os.path.join(tempfile.gettempdir(), "avrotize", "typemapunion-cs-test")
        test_csproj = os.path.join(cwd, "test", "cs", "typemapunion")
        
        if os.path.exists(cs_test_path):
            shutil.rmtree(cs_test_path, ignore_errors=True)
        os.makedirs(cs_test_path, exist_ok=True)
        os.makedirs(cs_path, exist_ok=True)
        
        copy_tree(test_csproj, cs_test_path)

        convert_avro_to_csharp(avro_path, cs_path, system_text_json_annotation=True, pascal_properties=True)
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

        convert_avro_to_csharp(avro_path, cs_path, system_text_json_annotation=True, pascal_properties=True)
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
                               system_text_json_annotation=True, newtonsoft_json_annotation=True)
        assert subprocess.check_call(['dotnet', 'build'], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr) == 0
