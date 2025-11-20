"""Tests for JSON Structure to Rust conversion."""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretorust import convert_structure_to_rust


class TestStructureToRust(unittest.TestCase):
    """Test cases for JSON Structure to Rust conversion."""

    def run_convert_struct_to_rust(
        self,
        struct_name,
        serde_annotation=False,
        package_name='',
    ):
        """Helper method to convert a JSON Structure file to Rust"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "avsc", struct_name + "-ref.struct.json")
        rust_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-rust")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)

        convert_structure_to_rust(
            struct_path,
            rust_path,
            package_name=package_name if package_name else struct_name,
            serde_annotation=serde_annotation
        )

        # Verify basic structure
        assert os.path.exists(os.path.join(rust_path, 'src'))
        assert os.path.exists(os.path.join(rust_path, 'Cargo.toml'))
        assert os.path.exists(os.path.join(rust_path, 'src', 'lib.rs'))

        # Build the package and run the embedded unit tests
        try:
            # First build the package
            build_result = subprocess.run(
                ['cargo', 'build', '--release'],
                cwd=rust_path,
                capture_output=True,
                text=True,
                timeout=180
            )
            if build_result.returncode != 0:
                print(f"\nCargo build failed for {struct_name}:")
                print(build_result.stdout)
                print(build_result.stderr)
                self.fail(f"Cargo build failed for {struct_name}")
            
            # Then run the unit tests
            test_result = subprocess.run(
                ['cargo', 'test', '--release'],
                cwd=rust_path,
                capture_output=True,
                text=True,
                timeout=180
            )
            if test_result.returncode != 0:
                print(f"\nCargo test output for {struct_name}:")
                print(test_result.stdout)
                print(test_result.stderr)
                self.fail(f"Cargo tests failed for {struct_name}")
                
        except subprocess.TimeoutExpired:
            self.fail(f"Cargo build/test timed out for {struct_name}")
        except FileNotFoundError:
            self.skipTest("Cargo not found - skipping Rust build and tests")

    def test_convert_address_struct_to_rust(self):
        """Test converting an address JSON Structure file to Rust"""
        self.run_convert_struct_to_rust("address", serde_annotation=True)

    def test_convert_address_struct_to_rust_no_serde(self):
        """Test converting an address JSON Structure file to Rust without serde"""
        self.run_convert_struct_to_rust("address", serde_annotation=False)

    def test_convert_telemetry_struct_to_rust(self):
        """Test converting a telemetry JSON Structure file to Rust"""
        self.run_convert_struct_to_rust("telemetry", serde_annotation=True)

    def test_convert_telemetry_struct_to_rust_no_serde(self):
        """Test converting a telemetry JSON Structure file to Rust without serde"""
        self.run_convert_struct_to_rust("telemetry", serde_annotation=False)


if __name__ == '__main__':
    unittest.main()
