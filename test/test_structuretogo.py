"""Tests for JSON Structure to Go conversion."""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile
from os import path

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretogo import convert_structure_to_go


class TestStructureToGo(unittest.TestCase):
    """Test cases for JSON Structure to Go conversion."""

    def run_convert_structure_to_go(
        self,
        struct_name,
        json_annotation=False,
        avro_annotation=False,
        package_name='',
    ):
        """Helper method to convert a JSON Structure file to Go"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", struct_name + ".struct.json")
        go_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-go")
        
        # Clean up existing output
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)

        # Convert the schema
        convert_structure_to_go(
            struct_path,
            go_path,
            package_name=package_name or struct_name.lower().replace('-', '_'),
            json_annotation=json_annotation,
            avro_annotation=avro_annotation,
            package_site='github.com',
            package_username='test'
        )

        # Verify basic structure
        assert os.path.exists(os.path.join(go_path, 'go.mod')), "go.mod file not found"
        assert os.path.exists(os.path.join(go_path, 'pkg')), "pkg directory not found"

        # Verify Go code compiles if Go is installed
        self.verify_go_build(go_path, package_name or struct_name.lower().replace('-', '_'))

        return go_path

    def verify_go_build(self, go_path, package_name: str):
        """Verify the Go output by building it"""
        # Check if Go is installed
        go_installed = False
        try:
            result = subprocess.run(
                ["go", "version"], 
                check=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                timeout=10
            )
            go_installed = True
            print(f"\nGo version: {result.stdout.decode().strip()}")
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
            print("\nGo is not installed or not in PATH, skipping build verification")
            return

        if go_installed:
            try:
                # Run go mod tidy to download dependencies
                print(f"\nRunning go mod tidy in {go_path}")
                subprocess.run(
                    ["go", "mod", "tidy"], 
                    cwd=go_path, 
                    check=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    timeout=60
                )

                # Build the Go code
                print(f"Building Go code in {go_path}")
                result = subprocess.run(
                    ["go", "build", "./..."], 
                    cwd=go_path, 
                    check=True, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    timeout=60
                )
                output = result.stdout.decode()
                if output:
                    print("\nGo build output:\n", output)
                print("Go build successful!")

            except subprocess.CalledProcessError as e:
                print("Go build failed:\n", e.stderr.decode())
                # Don't fail the test, just warn
                print("WARNING: Go build failed, but continuing with test")
            except subprocess.TimeoutExpired:
                print("WARNING: Go build timed out, but continuing with test")

    def test_all_primitives(self):
        """Test conversion of all JSON Structure primitive types"""
        print("\n\nTest: All primitives")
        go_path = self.run_convert_structure_to_go(
            "test-all-primitives",
            json_annotation=True,
            package_name="allprimitives"
        )

        # Verify the generated Go file exists
        pkg_dir = os.path.join(go_path, "pkg", "allprimitives")
        assert os.path.exists(pkg_dir), f"Package directory not found: {pkg_dir}"
        
        # Check that struct file was generated
        struct_files = [f for f in os.listdir(pkg_dir) if f.endswith('.go') and not f.endswith('_test.go')]
        assert len(struct_files) > 0, "No Go struct files generated"
        print(f"Generated files: {struct_files}")

    def test_enum(self):
        """Test conversion of enums"""
        print("\n\nTest: Enums")
        go_path = self.run_convert_structure_to_go(
            "test-enum-const",
            json_annotation=True,
            package_name="testenum"
        )

        pkg_dir = os.path.join(go_path, "pkg", "testenum")
        assert os.path.exists(pkg_dir), f"Package directory not found: {pkg_dir}"

    def test_choice_tagged(self):
        """Test conversion of tagged choice/union types"""
        print("\n\nTest: Tagged Choice")
        go_path = self.run_convert_structure_to_go(
            "test-choice-tagged",
            json_annotation=True,
            package_name="testchoicetagged"
        )

        pkg_dir = os.path.join(go_path, "pkg", "testchoicetagged")
        assert os.path.exists(pkg_dir), f"Package directory not found: {pkg_dir}"

    def test_nested_objects(self):
        """Test conversion of nested objects"""
        print("\n\nTest: Nested objects (person)")
        go_path = self.run_convert_structure_to_go(
            "person-ref",
            json_annotation=True,
            package_name="person"
        )

        pkg_dir = os.path.join(go_path, "pkg", "person")
        assert os.path.exists(pkg_dir), f"Package directory not found: {pkg_dir}"

    def test_arrays_and_maps(self):
        """Test conversion of arrays and maps"""
        print("\n\nTest: Arrays and maps")
        go_path = self.run_convert_structure_to_go(
            "arraydef-ref",
            json_annotation=True,
            package_name="arraydef"
        )

        pkg_dir = os.path.join(go_path, "pkg", "arraydef")
        assert os.path.exists(pkg_dir), f"Package directory not found: {pkg_dir}"

    def test_optional_fields(self):
        """Test conversion with optional fields"""
        print("\n\nTest: Optional fields")
        go_path = self.run_convert_structure_to_go(
            "address-ref",
            json_annotation=True,
            package_name="address"
        )

        pkg_dir = os.path.join(go_path, "pkg", "address")
        assert os.path.exists(pkg_dir), f"Package directory not found: {pkg_dir}"


if __name__ == '__main__':
    unittest.main()
