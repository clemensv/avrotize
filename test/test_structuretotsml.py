"""Tests for JSON Structure to TMSL conversion."""

import json
import os
import sys
import tempfile
import unittest

# Ensure the project root is in the system path for imports
current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretotsml import convert_structure_to_tmsl


class TestStructureToTmsl(unittest.TestCase):
    """Test cases for JSON Structure to TMSL conversion."""

    def test_convert_basic_types_structure_to_tmsl(self):
        """Test converting basic-types.struct.json to a TMSL schema."""
        cwd = os.getcwd()
        structure_path = os.path.join(cwd, "test", "struct", "basic-types.struct.json")
        tmsl_path = os.path.join(tempfile.gettempdir(), "avrotize", "basic-types.tmsl.json")
        os.makedirs(os.path.dirname(tmsl_path), exist_ok=True)

        convert_structure_to_tmsl(structure_path, tmsl_path)

        self.assertTrue(os.path.exists(tmsl_path))
        with open(tmsl_path, "r", encoding="utf-8") as f:
            tmsl = json.load(f)

        self.assertIn("createOrReplace", tmsl)
        table = tmsl["createOrReplace"]["database"]["model"]["tables"][0]
        self.assertIn("columns", table)
        self.assertGreater(len(table["columns"]), 0)


if __name__ == "__main__":
    unittest.main()
