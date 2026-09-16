import json
import os
import sys
import tempfile
import unittest

from fastavro.schema import load_schema

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import asn1tools

from avrotize.avrotoasn1 import convert_avro_to_asn1
from avrotize.asn1toavro import convert_asn1_to_avro


class TestAvroToAsn1(unittest.TestCase):
    """Tests for converting Avrotize (Avro) schema to ASN.1 modules."""

    def _tmp(self, name):
        out_dir = os.path.join(tempfile.gettempdir(), "avrotize")
        os.makedirs(out_dir, exist_ok=True)
        return os.path.join(out_dir, name)

    def convert_and_validate(self, avsc_name):
        """Convert an avsc fixture to ASN.1 and assert the module compiles."""
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", avsc_name)
        asn1_path = self._tmp(avsc_name.replace(".avsc", ".asn1"))
        convert_avro_to_asn1(avro_path, asn1_path)
        # asn1tools compiling the output is a strong syntactic + structural validity check.
        asn1tools.compile_files([asn1_path])
        return asn1_path

    def convert_and_roundtrip(self, avsc_name):
        """Convert avsc -> ASN.1 -> Avro and assert the resulting Avro schema is valid."""
        asn1_path = self.convert_and_validate(avsc_name)
        back_path = self._tmp(avsc_name.replace(".avsc", "_from_asn1.avsc"))
        convert_asn1_to_avro(asn1_path, back_path)
        load_schema(back_path)

    def test_convert_address_avsc_to_asn1(self):
        self.convert_and_roundtrip("address.avsc")

    def test_convert_address_nn_avsc_to_asn1(self):
        self.convert_and_roundtrip("address-nn.avsc")

    def test_convert_northwind_avsc_to_asn1(self):
        self.convert_and_roundtrip("northwind.avsc")

    def test_convert_telemetry_avsc_to_asn1(self):
        self.convert_and_roundtrip("telemetry.avsc")

    def test_convert_twotypeunion_avsc_to_asn1(self):
        self.convert_and_validate("twotypeunion.avsc")

    def test_convert_enumfield_ordinals_avsc_to_asn1(self):
        self.convert_and_validate("enumfield-ordinals.avsc")

    def test_full_type_system_mapping(self):
        """Pin the full Avro -> ASN.1 type mapping and assert every construct is emitted."""
        schema = [
            {"type": "enum", "name": "Color", "namespace": "test",
             "symbols": ["RED", "GREEN", "BLUE"]},
            {"type": "fixed", "name": "Md5", "namespace": "test", "size": 16},
            {"type": "record", "name": "AllTypes", "namespace": "test", "fields": [
                {"name": "i", "type": "int"},
                {"name": "l", "type": "long"},
                {"name": "f", "type": "float"},
                {"name": "d", "type": "double"},
                {"name": "b", "type": "boolean"},
                {"name": "by", "type": "bytes"},
                {"name": "s", "type": "string"},
                {"name": "n", "type": "null"},
                {"name": "color", "type": "test.Color"},
                {"name": "hash", "type": "test.Md5"},
                {"name": "arr", "type": {"type": "array", "items": "string"}},
                {"name": "mp", "type": {"type": "map", "values": "long"}},
                {"name": "opt", "type": ["null", "string"]},
                {"name": "ch", "type": ["int", "string", "boolean"]},
                {"name": "dt", "type": {"type": "int", "logicalType": "date"}},
                {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "tm", "type": {"type": "int", "logicalType": "time-millis"}},
                {"name": "dec", "type": {"type": "bytes", "logicalType": "decimal",
                                         "precision": 10, "scale": 2}},
                {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}},
            ]},
        ]
        avro_path = self._tmp("alltypes_asn1.avsc")
        with open(avro_path, "w", encoding="utf-8") as handle:
            json.dump(schema, handle)
        asn1_path = self._tmp("alltypes_asn1.asn1")
        convert_avro_to_asn1(avro_path, asn1_path)

        # Must be a valid ASN.1 module.
        asn1tools.compile_files([asn1_path])

        with open(asn1_path, "r", encoding="utf-8") as handle:
            text = handle.read()

        self.assertIn("Color ::= ENUMERATED", text)
        self.assertIn("red(0)", text)
        self.assertIn("green(1)", text)
        self.assertIn("blue(2)", text)
        self.assertIn("Md5 ::= OCTET STRING (SIZE(16))", text)
        self.assertIn("AllTypes ::= SEQUENCE", text)
        self.assertIn("i INTEGER", text)
        self.assertIn("l INTEGER", text)
        self.assertIn("f REAL", text)
        self.assertIn("d REAL", text)
        self.assertIn("b BOOLEAN", text)
        self.assertIn("by OCTET STRING", text)
        self.assertIn("s UTF8String", text)
        self.assertIn("n NULL", text)
        self.assertIn("color Color", text)
        self.assertIn("hash Md5", text)
        self.assertIn("arr SEQUENCE OF UTF8String", text)
        self.assertIn("mp SEQUENCE OF SEQUENCE { key UTF8String, value INTEGER }", text)
        self.assertIn("opt UTF8String OPTIONAL", text)
        self.assertIn("ch CHOICE {", text)
        self.assertIn("dt DATE", text)
        self.assertIn("ts DATE-TIME", text)
        self.assertIn("tm TIME-OF-DAY", text)
        self.assertIn("dec REAL", text)
        self.assertIn("uid UTF8String", text)


if __name__ == "__main__":
    unittest.main()
