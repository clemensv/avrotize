import json
import os
import tempfile
import unittest
from os import path, getcwd
from avrotize.avrotojstruct import convert_avro_to_json_structure
from jsoncomparison import NO_DIFF, Compare
from tools.json_structure_schema_validator import JSONStructureSchemaCoreValidator


class TestAvroToJsonStructure(unittest.TestCase):
    """Tests converting Avro schemas to JSON Structure (*.struct.json)"""

    def _validate_json(self, json_file_path: str) -> None:
        """Ensure generated file contains valid JSON and conforms to JSON Structure Core."""
        with open(json_file_path, "r", encoding="utf-8") as file:
            source_text = file.read()
            try:
                doc = json.loads(source_text)
            except json.JSONDecodeError as e:
                self.fail(f"Invalid JSON in {json_file_path}: {e}")
                return # Should not be reached if self.fail works as expected

        validator = JSONStructureSchemaCoreValidator(allow_dollar=False, allow_import=True) # allow_import might be needed depending on test cases
        errors = validator.validate(doc, source_text)
        if errors:
            error_messages = "\n".join(errors)
            self.fail(f"JSON Structure validation failed for {json_file_path}:\n{error_messages}")

    def _convert_and_validate(
        self, avro_file: str, struct_file: str | None = None, naming_mode: str = "default"
    ) -> None:
        """
        Helper that converts an Avro schema to JSON Structure, validates the
        produced JSON, and compares it with a reference file if present.
        """
        cwd = getcwd()
        if struct_file is None:
            struct_file = avro_file.replace(".avsc", ".struct.json")

        avro_full_path = path.join(cwd, "test", "avsc", avro_file)
        struct_full_path = path.join(tempfile.gettempdir(), "avrotize", struct_file)

        # Ensure destination directory exists
        os.makedirs(os.path.dirname(struct_full_path), exist_ok=True)

        # Perform conversion
        convert_avro_to_json_structure(avro_full_path, struct_full_path, naming_mode=naming_mode)

        # Validate JSON is syntactically correct
        self._validate_json(struct_full_path)

        # If a reference file exists, compare generated output against it
        ref_path = avro_full_path.replace(".avsc", "-ref.struct.json")
        if os.path.exists(ref_path):
            with open(ref_path, "r", encoding="utf-8") as ref_file:
                expected = json.load(ref_file)
            with open(struct_full_path, "r", encoding="utf-8") as gen_file:
                actual = json.load(gen_file)

            diff = Compare().check(actual, expected)
            self.assertEqual(diff, NO_DIFF, f"Differences found for {avro_file}")

    # One test per Avro fixture (mirrors test_avrotojsons.py)
    def test_address_nn(self):
        self._convert_and_validate("address-nn.avsc", "address-nn.struct.json")

    def test_address(self):
        self._convert_and_validate("address.avsc", "address.struct.json")

    def test_complexunion(self):
        self._convert_and_validate("complexunion.avsc", "complexunion.struct.json")

    def test_enumfield_ordinals(self):
        self._convert_and_validate("enumfield-ordinals.avsc", "enumfield-ordinals.struct.json")

    def test_enumfield(self):
        self._convert_and_validate("enumfield.avsc", "enumfield.struct.json")

    def test_feeditem(self):
        self._convert_and_validate("feeditem.avsc", "feeditem.struct.json")

    def test_fileblob(self):
        self._convert_and_validate("fileblob.avsc", "fileblob.struct.json")

    def test_northwind(self):
        self._convert_and_validate("northwind.avsc", "northwind.struct.json")

    def test_primitiveunion(self):
        self._convert_and_validate("primitiveunion.avsc", "primitiveunion.struct.json")

    def test_telemetry(self):
        self._convert_and_validate("telemetry.avsc", "telemetry.struct.json")

    def test_twotypeunion(self):
        self._convert_and_validate("twotypeunion.avsc", "twotypeunion.struct.json")

    def test_typemapunion(self):
        self._convert_and_validate("typemapunion.avsc", "typemapunion.struct.json")

    def test_typemapunion2(self):
        self._convert_and_validate("typemapunion2.avsc", "typemapunion2.struct.json")