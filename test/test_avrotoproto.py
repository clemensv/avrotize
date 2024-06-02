""" Test Avro to Proto conversion """

import os
import sys
import tempfile
from os import path, getcwd

import pytest

from avrotize.avrotoproto import convert_avro_to_proto
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch

class TestAvroToProto(unittest.TestCase):
    """ Test Avro to Proto conversion"""
    def test_convert_address_avsc_to_proto(self):
        """ Test converting an Avro schema to Proto"""
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "address.proto")
        dir = os.path.dirname(proto_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_proto(avro_path, proto_path)           
        
    def test_convert_address_nn_avsc_to_proto(self):
        """ Test converting an Avro schema to Proto"""
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "address-nn.avsc")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-nn.proto")
        dir = os.path.dirname(proto_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_proto(avro_path, proto_path)           


    def test_convert_telemetry_avsc_to_proto(self):
        """ Test converting an Avro schema to Proto"""
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry.proto")
        dir = os.path.dirname(proto_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_avro_to_proto(avro_path, proto_path)

    def test_convert_jfrog_pipelines_jsons_to_avro_to_proto(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        proto_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-proto")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_proto(avro_path, proto_path)
