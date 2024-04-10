from unittest.mock import patch
import unittest
import os
import shutil
import subprocess
import sys
from os import path, getcwd

import pytest

from avrotize.avrotojava import convert_avro_to_java
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestAvroToJava(unittest.TestCase):
    def test_convert_address_avsc_to_java(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        java_path = os.path.join(cwd, "test", "tmp", "address-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path)
        os.makedirs(java_path)

        convert_avro_to_java(avro_path, java_path)
        assert subprocess.check_call(
            ['mvn', 'package'], cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_address_avsc_to_java_avro_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        java_path = os.path.join(cwd, "test", "tmp", "address-java-avro")
        if os.path.exists(java_path):
            shutil.rmtree(java_path)
        os.makedirs(java_path)

        convert_avro_to_java(avro_path, java_path, avro_annotation=True)
        assert subprocess.check_call(
            ['mvn', 'package'], cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_address_avsc_to_java_jackson_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        java_path = os.path.join(cwd, "test", "tmp", "address-java-jackson")
        if os.path.exists(java_path):
            shutil.rmtree(java_path)
        os.makedirs(java_path)

        convert_avro_to_java(avro_path, java_path,
                             jackson_annotation=True, pascal_properties=True)
        assert subprocess.check_call(
            ['mvn', 'package'], cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_telemetry_avsc_to_java(self):
        """ Test converting a telemetry.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        java_path = os.path.join(cwd, "test", "tmp", "telemetry-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path)
        os.makedirs(java_path)

        convert_avro_to_java(avro_path, java_path)
        assert subprocess.check_call(
            ['mvn', 'package'], cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_jfrog_pipelines_jsons_to_avro_to_java(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(cwd, "test", "tmp", "jfrog-pipelines.avsc")
        java_path = path.join(cwd, "test", "tmp", "jfrog-pipelines-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path)
        os.makedirs(java_path)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_java(avro_path, java_path)
        assert subprocess.check_call(
            ['mvn', 'package'], cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_jfrog_pipelines_jsons_to_avro_to_java_annotated(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(cwd, "test", "tmp", "jfrog-pipelines.avsc")
        java_path = path.join(cwd, "test", "tmp", "jfrog-pipelines-java-ann")
        if os.path.exists(java_path):
            shutil.rmtree(java_path)
        os.makedirs(java_path)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_java(avro_path, java_path, pascal_properties=True,
                             avro_annotation=True, jackson_annotation=True)
        assert subprocess.check_call(
            ['mvn', 'package'], cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
