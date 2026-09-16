"""Tests for local TMSL validation."""

import json
import os
import sys
import tempfile
import unittest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.avrototsml import convert_avro_to_tmsl
from avrotize.tmslvalidate import validate_tmsl_file


class TestTmslValidate(unittest.TestCase):
    """Test cases for TMSL validation."""

    def test_validate_generated_tmsl(self):
        """Generated TMSL from a2tsml should validate locally."""
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        tmsl_path = os.path.join(tempfile.gettempdir(), "avrotize", "address.validate.tmsl.json")
        os.makedirs(os.path.dirname(tmsl_path), exist_ok=True)

        convert_avro_to_tmsl(avro_path, None, tmsl_path)

        errors = validate_tmsl_file(tmsl_path)
        self.assertEqual([], errors)

    def test_rejects_invalid_datatype(self):
        """Invalid column dataType should fail validation."""
        tmsl_doc = {
            "createOrReplace": {
                "object": {"database": "db"},
                "database": {
                    "name": "db",
                    "compatibilityLevel": 1605,
                    "model": {
                        "culture": "en-US",
                        "tables": [
                            {
                                "name": "T1",
                                "columns": [
                                    {
                                        "name": "c1",
                                        "dataType": "notAType",
                                        "sourceColumn": "c1",
                                    }
                                ],
                            }
                        ],
                    },
                },
            }
        }

        tmsl_path = os.path.join(tempfile.gettempdir(), "avrotize", "invalid-datatype.tmsl.json")
        os.makedirs(os.path.dirname(tmsl_path), exist_ok=True)
        with open(tmsl_path, "w", encoding="utf-8") as f:
            json.dump(tmsl_doc, f, indent=2)

        errors = validate_tmsl_file(tmsl_path)
        self.assertTrue(any("dataType" in error for error in errors))

    def test_rejects_unexpected_property(self):
        """Unexpected property should fail validation for strict object shapes."""
        tmsl_doc = {
            "createOrReplace": {
                "object": {"database": "db"},
                "database": {
                    "name": "db",
                    "compatibilityLevel": 1605,
                    "model": {
                        "culture": "en-US",
                        "tables": [
                            {
                                "name": "T1",
                                "columns": [
                                    {
                                        "name": "c1",
                                        "dataType": "string",
                                        "sourceColumn": "c1",
                                        "unexpected": True,
                                    }
                                ],
                            }
                        ],
                    },
                },
            }
        }

        tmsl_path = os.path.join(tempfile.gettempdir(), "avrotize", "invalid-extra-prop.tmsl.json")
        os.makedirs(os.path.dirname(tmsl_path), exist_ok=True)
        with open(tmsl_path, "w", encoding="utf-8") as f:
            json.dump(tmsl_doc, f, indent=2)

        errors = validate_tmsl_file(tmsl_path)
        self.assertTrue(any("Unexpected property 'unexpected'" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
