"""
Tests for JSON Structure to Java conversion
"""
import unittest
import os
import shutil
import subprocess
import sys
import tempfile

from avrotize.structuretojava import convert_structure_to_java

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToJava(unittest.TestCase):
    """Test cases for JSON Structure to Java conversion"""

    def run_convert_struct_to_java(
        self,
        struct_name,
        jackson_annotation=True,
        pascal_properties=False,
        package_name=None,
    ):
        """Test converting a JSON Structure file to Java and building with Maven"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", struct_name + ".struct.json")
        java_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        if package_name is None:
            package_name = struct_name.replace('-', '_').replace('.', '_').lower()

        convert_structure_to_java(
            struct_path,
            java_path,
            package_name=package_name,
            pascal_properties=pascal_properties,
            jackson_annotation=jackson_annotation,
        )
        
        # Verify Maven build runs successfully
        assert (
            subprocess.check_call(
                ["mvn", "package", "-B"],
                cwd=java_path,
                stdout=sys.stdout,
                stderr=sys.stderr,
            )
            == 0
        )

    def test_convert_address_struct_to_java(self):
        """Test converting address JSON Structure to Java"""
        self.run_convert_struct_to_java("address-ref")

    def test_convert_address_struct_to_java_pascal_properties(self):
        """Test converting address JSON Structure to Java with PascalCase properties"""
        self.run_convert_struct_to_java("address-ref", pascal_properties=True)
    
    def test_convert_userprofile_struct_to_java(self):
        """Test converting userprofile JSON Structure to Java"""
        self.run_convert_struct_to_java("userprofile-ref")
    
    def test_convert_feeditem_struct_to_java(self):
        """Test converting feeditem JSON Structure to Java"""
        self.run_convert_struct_to_java("feeditem-ref")


if __name__ == "__main__":
    unittest.main()
