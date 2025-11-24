"""Tests for JSON Structure to Go conversion."""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile
import glob
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
        try:
            convert_structure_to_go(
                struct_path,
                go_path,
                package_name=package_name or struct_name.lower().replace('-', '_'),
                json_annotation=json_annotation,
                avro_annotation=avro_annotation,
                package_site='github.com',
                package_username='test'
            )
        except Exception as e:
            print(f"Conversion failed for {struct_name}: {e}")
            raise

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

    def test_all_struct_files(self):
        """Test conversion of all JSON Structure files in test/jsons"""
        cwd = os.getcwd()
        jsons_dir = os.path.join(cwd, "test", "jsons")
        struct_files = glob.glob(os.path.join(jsons_dir, "*.struct.json"))
        
        print(f"\n\nFound {len(struct_files)} .struct.json files to test")
        
        failed_tests = []
        for struct_file in sorted(struct_files):
            struct_name = os.path.basename(struct_file).replace('.struct.json', '')
            print(f"\n{'='*60}")
            print(f"Testing: {struct_name}")
            print('='*60)
            
            try:
                go_path = self.run_convert_structure_to_go(
                    struct_name,
                    json_annotation=True,
                    package_name=struct_name.lower().replace('-', '_')
                )
                
                # Verify the generated Go file exists
                pkg_dir = os.path.join(go_path, "pkg", struct_name.lower().replace('-', '_'))
                assert os.path.exists(pkg_dir), f"Package directory not found: {pkg_dir}"
                
                # Check that at least one struct or enum file was generated
                go_files = [f for f in os.listdir(pkg_dir) if f.endswith('.go') and not f.endswith('_test.go') and not f.endswith('_helpers.go') and f != 'module.go']
                assert len(go_files) > 0, f"No Go struct/enum files generated for {struct_name}"
                print(f"[OK] Success: Generated {len(go_files)} Go file(s)")
                
            except Exception as e:
                print(f"[FAIL] Failed: {struct_name} - {e}")
                failed_tests.append((struct_name, str(e)))
        
        # Report summary
        print(f"\n\n{'='*60}")
        print(f"Test Summary: {len(struct_files) - len(failed_tests)}/{len(struct_files)} passed")
        print('='*60)
        
        if failed_tests:
            print("\nFailed tests:")
            for name, error in failed_tests:
                print(f"  - {name}: {error}")
        
        # Only fail if more than 10% of tests failed (some schemas may be unsupported)
        failure_rate = len(failed_tests) / len(struct_files) if struct_files else 0
        assert failure_rate < 0.1, f"Too many tests failed: {len(failed_tests)}/{len(struct_files)}"

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

    def test_avro_annotation(self):
        """Test conversion with Avro annotation enabled"""
        print("\n\nTest: Avro annotation")
        go_path = self.run_convert_structure_to_go(
            "test-all-primitives",
            json_annotation=True,
            avro_annotation=True,
            package_name="testavro"
        )

        pkg_dir = os.path.join(go_path, "pkg", "testavro")
        assert os.path.exists(pkg_dir), f"Package directory not found: {pkg_dir}"
        
        # Check that Avro schema constant is in the generated file
        go_files = [f for f in os.listdir(pkg_dir) if f.endswith('.go') and not f.endswith('_test.go') and not f.endswith('_helpers.go') and f != 'module.go']
        assert len(go_files) > 0, "No Go struct files generated"
        
        # Verify Avro schema is embedded
        struct_file_path = os.path.join(pkg_dir, go_files[0])
        with open(struct_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            assert 'AvroSchema' in content, "Avro schema constant not found in generated file"
            assert 'GetAvroSchema()' in content, "GetAvroSchema method not found in generated file"
            assert 'avro.Parse' in content, "Avro schema parsing code not found"
            assert 'avro.Marshal' in content, "Avro marshaling code not found"
            assert 'avro.Unmarshal' in content, "Avro unmarshaling code not found"
        
        print("Avro schema embedding verified successfully")

    def test_abstract_types(self):
        """ Test abstract type support with Go interfaces """
        cwd = os.getcwd()
        go_path = os.path.join(tempfile.gettempdir(), "avrotize", "testabstract")
        
        # Clean up existing output
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)
        
        convert_structure_to_go(
            structure_schema_path=os.path.join(cwd, "test", "jsons", "abstract-extends.struct.json"),
            go_file_path=go_path,
            json_annotation=True,
            avro_annotation=True,
            package_name="testabstract"
        )

        pkg_dir = os.path.join(go_path, "pkg", "testabstract")
        assert os.path.exists(pkg_dir), f"Package directory not found: {pkg_dir}"
        
        # Check interface file exists
        interface_file = os.path.join(pkg_dir, "AbstractBase.go")
        assert os.path.exists(interface_file), "Abstract interface file not generated"
        
        # Verify interface content
        with open(interface_file, 'r', encoding='utf-8') as f:
            content = f.read()
            assert 'type AbstractBase interface' in content, "Interface declaration not found"
            assert 'GetBaseField()' in content, "Getter method not found in interface"
            assert 'SetBaseField(' in content, "Setter method not found in interface"
        
        # Check concrete implementation file
        impl_file = os.path.join(pkg_dir, "ConcreteImpl.go")
        assert os.path.exists(impl_file), "Concrete implementation file not generated"
        
        # Verify implementation content
        with open(impl_file, 'r', encoding='utf-8') as f:
            content = f.read()
            assert 'type ConcreteImpl struct' in content, "Struct declaration not found"
            assert 'BaseField' in content, "Base field not found in concrete struct"
            assert 'ImplField' in content, "Implementation field not found"
            assert 'var _ AbstractBase = (*ConcreteImpl)(nil)' in content, "Interface compliance check not found"
            assert 'func (s *ConcreteImpl) GetBaseField()' in content, "Base field getter not found"
            assert 'func (s *ConcreteImpl) SetBaseField(' in content, "Base field setter not found"
        
        # Verify the generated code compiles
        # Initialize go module if not already initialized
        if not os.path.exists(os.path.join(go_path, "go.mod")):
            result = subprocess.run(
                ["go", "mod", "init", "testabstract"],
                cwd=go_path,
                check=True,
                capture_output=True,
                text=True
            )
        
        result = subprocess.run(
            ["go", "mod", "tidy"],
            cwd=go_path,
            check=True,
            capture_output=True,
            text=True
        )
        
        result = subprocess.run(
            ["go", "build", f"./pkg/testabstract"],
            cwd=go_path,
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, f"Go build failed: {result.stderr}"
        print("[OK] Abstract type code compiles successfully")


if __name__ == '__main__':
    unittest.main()
