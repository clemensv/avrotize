"""
Tests for JSON Structure to C++ conversion
"""
import unittest
import os
import shutil
import subprocess
import sys
import tempfile

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretocpp import convert_structure_to_cpp


class TestStructureToCpp(unittest.TestCase):
    """Test cases for JSON Structure to C++ conversion"""

    def run_convert_struct_to_cpp(
        self,
        struct_name,
        json_annotation=True,
        namespace=None,
        build_and_test=False,
    ):
        """Test converting a JSON Structure file to C++"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", struct_name + ".struct.json")
        cpp_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-cpp")
        if os.path.exists(cpp_path):
            shutil.rmtree(cpp_path, ignore_errors=True)
        os.makedirs(cpp_path, exist_ok=True)

        kwargs = {
            "json_annotation": json_annotation,
        }
        if namespace is not None:
            kwargs["namespace"] = namespace

        convert_structure_to_cpp(struct_path, cpp_path, **kwargs)
        
        # Verify files were generated
        include_dir = os.path.join(cpp_path, "include")
        self.assertTrue(os.path.exists(include_dir), f"Include directory not found at {include_dir}")
        
        # Check for CMakeLists.txt
        cmake_file = os.path.join(cpp_path, "CMakeLists.txt")
        self.assertTrue(os.path.exists(cmake_file), f"CMakeLists.txt not found at {cmake_file}")
        
        # Check for vcpkg.json
        vcpkg_file = os.path.join(cpp_path, "vcpkg.json")
        self.assertTrue(os.path.exists(vcpkg_file), f"vcpkg.json not found at {vcpkg_file}")
        
        # Optionally build and test the generated code
        if build_and_test:
            self._build_and_test_cpp(cpp_path, struct_name)
        
        return cpp_path

    def _build_and_test_cpp(self, cpp_path, test_name):
        """
        Build and test the generated C++ code.
        This requires CMake and a C++ compiler (g++/clang++) to be available.
        Note: Building with vcpkg can be time-consuming on first run.
        """
        # Check if cmake is available
        try:
            result = subprocess.run(
                ["cmake", "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode != 0:
                self.skipTest("CMake not available")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("CMake not available")
        
        # Check if a C++ compiler is available
        try:
            result = subprocess.run(
                ["g++", "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode != 0:
                # Try clang++
                result = subprocess.run(
                    ["clang++", "--version"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode != 0:
                    self.skipTest("C++ compiler not available")
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self.skipTest("C++ compiler not available")
        
        print(f"\n[{test_name}] Building generated C++ code...")
        print(f"  Output directory: {cpp_path}")
        
        # Run CMake configure
        try:
            # Note: vcpkg bootstrap can take several minutes on first run
            # We set a reasonable timeout
            result = subprocess.run(
                ["cmake", "-B", "build", "-S", ".", "-DCMAKE_BUILD_TYPE=Release"],
                cwd=cpp_path,
                capture_output=True,
                text=True,
                timeout=300  # 5 minutes for vcpkg setup
            )
            if result.returncode != 0:
                print(f"  CMake configure failed:")
                print(result.stderr)
                self.skipTest(f"CMake configure failed: {result.stderr[:200]}")
        except subprocess.TimeoutExpired:
            self.skipTest("CMake configure timed out (vcpkg setup can be slow on first run)")
        
        # Run CMake build
        try:
            result = subprocess.run(
                ["cmake", "--build", "build", "--config", "Release"],
                cwd=cpp_path,
                capture_output=True,
                text=True,
                timeout=180  # 3 minutes for build
            )
            if result.returncode != 0:
                print(f"  CMake build failed:")
                print(result.stderr)
                self.fail(f"CMake build failed: {result.stderr[:200]}")
        except subprocess.TimeoutExpired:
            self.fail("CMake build timed out")
        
        print(f"  ✓ Build successful")
        
        # Run tests if they exist
        test_executable = os.path.join(cpp_path, "build", "runUnitTests")
        if os.path.exists(test_executable):
            try:
                result = subprocess.run(
                    [test_executable],
                    cwd=cpp_path,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if result.returncode != 0:
                    print(f"  Tests failed:")
                    print(result.stdout)
                    print(result.stderr)
                    self.fail(f"Unit tests failed: {result.stderr[:200]}")
                else:
                    print(f"  ✓ All tests passed")
            except subprocess.TimeoutExpired:
                self.fail("Test execution timed out")

    def test_convert_address_struct_to_cpp(self):
        """Test converting address-ref.struct.json to C++"""
        self.run_convert_struct_to_cpp("address-ref", json_annotation=True, namespace="address")

    def test_convert_address_struct_to_cpp_no_json(self):
        """Test converting address-ref.struct.json to C++ without JSON annotation"""
        self.run_convert_struct_to_cpp("address-ref", json_annotation=False, namespace="address")

    def test_convert_arraydef_struct_to_cpp(self):
        """Test converting arraydef-ref.struct.json to C++"""
        self.run_convert_struct_to_cpp("arraydef-ref", json_annotation=True, namespace="arraydef")

    def test_convert_addlprops1_struct_to_cpp(self):
        """Test converting addlprops1-ref.struct.json to C++"""
        self.run_convert_struct_to_cpp("addlprops1-ref", json_annotation=True, namespace="addlprops1")

    def test_convert_multiple_types_struct_to_cpp(self):
        """Test converting a schema with multiple types"""
        # Create a simple test schema
        import json
        test_schema = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "TestTypes",
            "properties": {
                "stringField": {"type": "string"},
                "intField": {"type": "int32"},
                "boolField": {"type": "boolean"},
                "floatField": {"type": "float"},
                "dateField": {"type": "date"},
                "uuidField": {"type": "uuid"},
                "binaryField": {"type": "binary"},
                "arrayField": {"type": "array", "items": {"type": "string"}},
                "mapField": {"type": "map", "values": {"type": "int32"}}
            },
            "required": ["stringField", "intField"]
        }
        
        cwd = os.getcwd()
        struct_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_types.struct.json")
        cpp_path = os.path.join(tempfile.gettempdir(), "avrotize", "test_types-cpp")
        
        # Write test schema
        os.makedirs(os.path.dirname(struct_path), exist_ok=True)
        with open(struct_path, 'w') as f:
            json.dump(test_schema, f)
        
        # Clean up output directory
        if os.path.exists(cpp_path):
            shutil.rmtree(cpp_path, ignore_errors=True)
        os.makedirs(cpp_path, exist_ok=True)
        
        # Convert
        convert_structure_to_cpp(struct_path, cpp_path, namespace="testtypes", json_annotation=True)
        
        # Verify files were generated
        include_dir = os.path.join(cpp_path, "include")
        self.assertTrue(os.path.exists(include_dir), f"Include directory not found at {include_dir}")

    @unittest.skip("Build test disabled by default - requires vcpkg setup which can be slow")
    def test_build_address_cpp_code(self):
        """
        Test that demonstrates building and testing generated C++ code.
        Skipped by default as it requires CMake, C++ compiler, and vcpkg setup.
        
        To run this test: pytest test/test_structuretocpp.py::TestStructureToCpp::test_build_address_cpp_code -v
        """
        self.run_convert_struct_to_cpp(
            "address-ref",
            json_annotation=True,
            namespace="address",
            build_and_test=True
        )


if __name__ == '__main__':
    unittest.main()
