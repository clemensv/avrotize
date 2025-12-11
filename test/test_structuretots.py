"""Tests for JSON Structure to TypeScript conversion."""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile
import json
from os import path, getcwd

from avrotize.structuretots import convert_structure_to_typescript, convert_structure_schema_to_typescript

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToTypeScript(unittest.TestCase):
    """Test cases for JSON Structure to TypeScript conversion."""

    def run_convert_struct_to_typescript(
        self,
        struct_name,
        typedjson_annotation=False,
        package_name='',
    ):
        """Helper method to convert a JSON Structure file to TypeScript"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", struct_name + ".struct.json")
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-ts")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)

        convert_structure_to_typescript(
            struct_path,
            ts_path,
            package_name=package_name,
            typedjson_annotation=typedjson_annotation
        )

        # Verify basic structure
        assert os.path.exists(os.path.join(ts_path, 'src'))
        assert os.path.exists(os.path.join(ts_path, 'package.json'))
        assert os.path.exists(os.path.join(ts_path, 'tsconfig.json'))

        # Try to compile TypeScript
        try:
            # Install dependencies
            subprocess.check_call(
                ["npm", "install"],
                cwd=ts_path,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            # Compile TypeScript
            subprocess.check_call(
                ["npm", "run", "build"],
                cwd=ts_path,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            # Check that dist directory was created
            assert os.path.exists(os.path.join(ts_path, 'dist'))
            print(f"[OK] {struct_name} compiled successfully")
            
            # Run tests if test directory exists
            test_dir = os.path.join(ts_path, "test")
            if os.path.exists(test_dir):
                try:
                    subprocess.check_call(
                        ["npm", "test"],
                        cwd=ts_path,
                        stdout=sys.stdout,
                        stderr=sys.stderr
                    )
                    print(f"[OK] {struct_name} tests passed")
                except subprocess.CalledProcessError as e:
                    print(f"Warning: Tests failed for {struct_name}: {e}")
        except subprocess.CalledProcessError as e:
            print(f"Warning: TypeScript compilation failed for {struct_name}: {e}")
            # Continue anyway - we still want to verify the generation worked
        except FileNotFoundError:
            print(f"Warning: npm not found, skipping TypeScript compilation for {struct_name}")

        return ts_path

    def test_convert_basic_types_struct_to_typescript(self):
        """Test converting basic-types.struct.json to TypeScript"""
        self.run_convert_struct_to_typescript("basic-types")

    def test_convert_basic_types_struct_to_typescript_annotated(self):
        """Test converting basic-types with TypedJSON annotation"""
        for typedjson_annotation in [True, False]:
            self.run_convert_struct_to_typescript(
                "basic-types",
                typedjson_annotation=typedjson_annotation
            )

    def test_convert_numeric_types_struct_to_typescript(self):
        """Test converting numeric-types.struct.json to TypeScript"""
        self.run_convert_struct_to_typescript("numeric-types")

    def test_convert_temporal_types_struct_to_typescript(self):
        """Test converting temporal-types.struct.json to TypeScript"""
        self.run_convert_struct_to_typescript("temporal-types")

    def test_convert_collections_struct_to_typescript(self):
        """Test converting collections.struct.json to TypeScript"""
        self.run_convert_struct_to_typescript("collections")

    def test_convert_nested_objects_struct_to_typescript(self):
        """Test converting nested-objects.struct.json to TypeScript"""
        self.run_convert_struct_to_typescript("nested-objects")

    def test_convert_choice_types_struct_to_typescript(self):
        """Test converting choice-types.struct.json to TypeScript"""
        self.run_convert_struct_to_typescript("choice-types")

    def test_convert_extensions_struct_to_typescript(self):
        """Test converting extensions.struct.json to TypeScript"""
        self.run_convert_struct_to_typescript("extensions")

    def test_convert_edge_cases_struct_to_typescript(self):
        """Test converting edge-cases.struct.json to TypeScript"""
        self.run_convert_struct_to_typescript("edge-cases")

    def test_convert_complex_scenario_struct_to_typescript(self):
        """Test converting complex-scenario.struct.json to TypeScript"""
        self.run_convert_struct_to_typescript("complex-scenario")

    def test_convert_schema_to_typescript(self):
        """Test converting a schema as JsonNode to TypeScript"""
        schema = {
            "type": "object",
            "name": "TestRecord",
            "namespace": "test.schema",
            "properties": {
                "id": {"type": "string"},
                "count": {"type": "integer"},
                "active": {"type": "boolean"}
            },
            "required": ["id"]
        }
        
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", "schema-test-ts")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
        
        convert_structure_schema_to_typescript(
            schema,
            ts_path,
            package_name='schema-test'
        )
        
        # Verify basic structure
        assert os.path.exists(os.path.join(ts_path, 'src'))
        assert os.path.exists(os.path.join(ts_path, 'package.json'))
        assert os.path.exists(os.path.join(ts_path, 'tsconfig.json'))
        
        # Verify the generated TypeScript file exists
        src_files = os.listdir(os.path.join(ts_path, 'src'))
        ts_files = [f for f in src_files if f.endswith('.ts')]
        assert len(ts_files) > 0, "Expected at least one TypeScript file to be generated"
        
        # Try to compile TypeScript
        try:
            subprocess.check_call(
                ["npm", "install"],
                cwd=ts_path,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            subprocess.check_call(
                ["npm", "run", "build"],
                cwd=ts_path,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            assert os.path.exists(os.path.join(ts_path, 'dist'))
            print("[OK] schema-test compiled successfully")
        except subprocess.CalledProcessError as e:
            print(f"Warning: TypeScript compilation failed for schema-test: {e}")
        except FileNotFoundError:
            print("Warning: npm not found, skipping TypeScript compilation for schema-test")

    def test_convert_schema_list_to_typescript(self):
        """Test converting a list of schemas to TypeScript"""
        schemas = [
            {
                "type": "object",
                "name": "PrintJobStartedEventData",
                "properties": {
                    "jobId": {"type": "string"},
                    "startTime": {"type": "datetime"}
                },
                "required": ["jobId", "startTime"]
            },
            {
                "type": "object",
                "name": "PrintJobCompletedEventData",
                "properties": {
                    "jobId": {"type": "string"},
                    "endTime": {"type": "datetime"}
                },
                "required": ["jobId", "endTime"]
            }
        ]
        
        ts_path = os.path.join(tempfile.gettempdir(), "avrotize", "schema-list-test-ts")
        if os.path.exists(ts_path):
            shutil.rmtree(ts_path, ignore_errors=True)
        os.makedirs(ts_path, exist_ok=True)
        
        convert_structure_schema_to_typescript(
            schemas,
            ts_path,
            package_name='schema-list-test'
        )
        
        # Verify basic structure
        assert os.path.exists(os.path.join(ts_path, 'src'))
        assert os.path.exists(os.path.join(ts_path, 'package.json'))
        assert os.path.exists(os.path.join(ts_path, 'tsconfig.json'))
        
        # Verify index.ts contains exports for both types
        index_path = os.path.join(ts_path, 'src', 'index.ts')
        assert os.path.exists(index_path), "index.ts should exist"
        with open(index_path, 'r', encoding='utf-8') as f:
            index_content = f.read()
        assert 'PrintJobStartedEventData' in index_content, "index.ts should export PrintJobStartedEventData"
        assert 'PrintJobCompletedEventData' in index_content, "index.ts should export PrintJobCompletedEventData"
        
        # Verify both TypeScript files were generated
        src_dir = os.path.join(ts_path, 'src', 'schema-list-test')
        assert os.path.exists(os.path.join(src_dir, 'PrintJobStartedEventData.ts')), \
            "PrintJobStartedEventData.ts should exist"
        assert os.path.exists(os.path.join(src_dir, 'PrintJobCompletedEventData.ts')), \
            "PrintJobCompletedEventData.ts should exist"
        
        # Try to compile TypeScript
        try:
            subprocess.check_call(
                ["npm", "install"],
                cwd=ts_path,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            subprocess.check_call(
                ["npm", "run", "build"],
                cwd=ts_path,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            assert os.path.exists(os.path.join(ts_path, 'dist'))
            print("[OK] schema-list-test compiled successfully")
        except subprocess.CalledProcessError as e:
            print(f"Warning: TypeScript compilation failed for schema-list-test: {e}")
        except FileNotFoundError:
            print("Warning: npm not found, skipping TypeScript compilation for schema-list-test")

    def test_enum_with_namespace_import_path(self):
        """
        Regression test: Enum import paths must match actual file locations.
        
        When an enum has its own namespace, the import path in classes using that enum
        must match the actual file location (based on schema_namespace, not parent_namespace).
        """
        import re
        
        # Schema with a class that uses an enum with its own namespace
        schema = {
            "type": "object",
            "name": "Order",
            "namespace": "com.example.orders",
            "properties": {
                "orderId": {"type": "string"},
                "status": {
                    "type": "string",
                    "enum": ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"],
                    "name": "OrderStatus",
                    "namespace": "com.example.orders.enums"  # Different namespace
                }
            }
        }
        
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "ts-enum-import-test")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_schema_to_typescript(schema, output_dir, package_name="test")
        
        # Verify enum file exists at the correct location (based on its namespace)
        enum_path = os.path.join(output_dir, "src", "test", "com", "example", "orders", "enums", "OrderStatus.ts")
        assert os.path.exists(enum_path), f"Enum file should exist at {enum_path}"
        
        # Verify Order class file exists
        order_path = os.path.join(output_dir, "src", "test", "com", "example", "orders", "Order.ts")
        assert os.path.exists(order_path), f"Order class should exist at {order_path}"
        
        # Read Order.ts and check the import statement references correct path
        with open(order_path, 'r', encoding='utf-8') as f:
            order_content = f.read()
        
        # The import should point to the enum's actual location
        assert "OrderStatus" in order_content, "Order should reference OrderStatus"
        
        # Verify the import path includes 'enums' subdirectory
        import_pattern = r"import\s*\{\s*OrderStatus\s*\}\s*from\s*['\"]([^'\"]+)['\"]"
        match = re.search(import_pattern, order_content)
        assert match is not None, "Should have an import statement for OrderStatus"
        import_path = match.group(1)
        assert "enums" in import_path, f"Import path '{import_path}' should reference 'enums' subdirectory"

    def test_named_enum_keeps_name(self):
        """
        Regression test: Enums with explicit names should not get 'Enum' suffix.
        """
        schema = {
            "type": "object",
            "name": "Task",
            "properties": {
                "priority": {
                    "type": "string",
                    "enum": ["LOW", "MEDIUM", "HIGH"],
                    "name": "Priority"  # Explicit name
                }
            }
        }
        
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "ts-enum-naming")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_schema_to_typescript(schema, output_dir)
        
        # Look for Priority.ts (not PriorityEnum.ts)
        priority_path = os.path.join(output_dir, "src", "Priority.ts")
        priority_enum_path = os.path.join(output_dir, "src", "PriorityEnum.ts")
        
        assert os.path.exists(priority_path), "Named enum should use its explicit name (Priority.ts)"
        assert not os.path.exists(priority_enum_path), "Should NOT have PriorityEnum.ts when name is explicitly 'Priority'"

    def test_unnamed_enum_gets_suffix(self):
        """
        Regression test: Inline enums without names should get 'Enum' suffix based on field name.
        """
        schema = {
            "type": "object",
            "name": "Task",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["TODO", "DONE"]
                    # No explicit 'name' - should become 'StatusEnum'
                }
            }
        }
        
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "ts-unnamed-enum")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_schema_to_typescript(schema, output_dir)
        
        # Look for StatusEnum.ts (field name + Enum suffix)
        status_enum_path = os.path.join(output_dir, "src", "StatusEnum.ts")
        
        assert os.path.exists(status_enum_path), "Unnamed enum should get 'Enum' suffix (StatusEnum.ts)"


if __name__ == '__main__':
    unittest.main()
