import os
import json
import sys
import tempfile
from os import path, getcwd
from fastavro.schema import load_schema

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch
from avrotize.xsdtoavro import convert_xsd_to_avro

class TestXsdToAvro(unittest.TestCase):

    def validate_avro_schema(self, avro_file_path):
        load_schema(avro_file_path)

    def test_numeric_namespace_segments_are_valid_avro_names(self):
        xml_namespace = "urn:example:payments:sample.001.02"
        with tempfile.TemporaryDirectory() as directory:
            xsd_path = path.join(directory, "sample.001.02.xsd")
            avro_path = path.join(directory, "sample.avsc")
            with open(xsd_path, "w", encoding="utf-8") as xsd:
                xsd.write(f"""<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
                    targetNamespace="{xml_namespace}">
                    <xs:element name="Document" type="xs:string"/>
                </xs:schema>""")
            for override, expected in ((None, "sample._001._02"),
                                       ("Example.2026.Release", "Example._2026.Release")):
                with self.subTest(namespace=override):
                    convert_xsd_to_avro(xsd_path, avro_path, namespace=override)
                    with open(avro_path, encoding="utf-8") as avro:
                        schema = json.load(avro)
                    self.assertEqual(schema["namespace"], expected)
                    self.assertEqual(schema["xmlns"], xml_namespace)
                    self.validate_avro_schema(avro_path)

    def test_convert_crmdata_xsd_to_avro(self):
        cwd = os.getcwd()        
        xsd_path = os.path.join(cwd, "test", "xsd", "crmdata.xsd")
        avro_path = os.path.join(tempfile.gettempdir(), "avrotize", "crmdata.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_xsd_to_avro(xsd_path, avro_path)           
        self.validate_avro_schema(avro_path)

    def test_convert_iso20022_xsd_to_avro1(self):
        cwd = os.getcwd()        
        xsd_path = os.path.join(cwd, "test", "xsd", "acmt.003.001.08.xsd")
        avro_path = os.path.join(tempfile.gettempdir(), "avrotize", "acmt.003.001.08.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_xsd_to_avro(xsd_path, avro_path)
        self.validate_avro_schema(avro_path)

    def test_convert_iso20022_xsd_to_avro2(self):
        cwd = os.getcwd()        
        xsd_path = os.path.join(cwd, "test", "xsd", "admi.017.001.01.xsd")
        avro_path = os.path.join(tempfile.gettempdir(), "avrotize", "admi.017.001.01.avsc")
        dir = os.path.dirname(avro_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)
        
        convert_xsd_to_avro(xsd_path, avro_path)
        self.validate_avro_schema(avro_path)

    
