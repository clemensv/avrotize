"""
Tests for JSON Structure to C# conversion
"""
import unittest
import os
import shutil
import subprocess
import sys
import tempfile

import pytest

from avrotize.structuretocsharp import convert_structure_to_csharp

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToCSharp(unittest.TestCase):
    """Test cases for JSON Structure to C# conversion"""

    def run_convert_struct_to_csharp(
        self,
        struct_name,
        system_text_json_annotation=False,
        newtonsoft_json_annotation=False,
        system_xml_annotation=False,
        pascal_properties=False,
        base_namespace=None,
        project_name=None,
    ):
        """Test converting a JSON Structure file to C#"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", struct_name + ".struct.json")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", struct_name + "-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        kwargs = {
            "pascal_properties": pascal_properties,
            "system_text_json_annotation": system_text_json_annotation,
            "newtonsoft_json_annotation": newtonsoft_json_annotation,
            "system_xml_annotation": system_xml_annotation,
        }
        if base_namespace is not None:
            kwargs["base_namespace"] = base_namespace
        if project_name is not None:
            kwargs["project_name"] = project_name

        convert_structure_to_csharp(struct_path, cs_path, **kwargs)
        
        # Verify dotnet test runs successfully
        assert (
            subprocess.check_call(
                ["dotnet", "test"], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr
            )
            == 0
        )

    def test_convert_address_struct_to_csharp(self):
        """Test converting address JSON Structure to C#"""
        self.run_convert_struct_to_csharp("address-ref")

    def test_convert_address_struct_to_csharp_annotated(self):
        """Test converting address with all annotation combinations"""
        for system_text_json_annotation in [True, False]:
            for newtonsoft_json_annotation in [True, False]:
                for pascal_properties in [True, False]:
                    for system_xml_annotation in [True, False]:
                        self.run_convert_struct_to_csharp(
                            "address-ref",
                            system_text_json_annotation=system_text_json_annotation,
                            newtonsoft_json_annotation=newtonsoft_json_annotation,
                            pascal_properties=pascal_properties,
                            system_xml_annotation=system_xml_annotation,
                        )

    def test_convert_addlprops1_struct_to_csharp(self):
        """Test converting additionalProperties example to C#"""
        self.run_convert_struct_to_csharp("addlprops1-ref")

    def test_convert_addlprops2_struct_to_csharp(self):
        """Test converting additionalProperties typed example to C#"""
        self.run_convert_struct_to_csharp("addlprops2-ref")

    def test_convert_addlprops3_struct_to_csharp(self):
        """Test converting additionalProperties complex example to C#"""
        self.run_convert_struct_to_csharp("addlprops3-ref")

    def test_convert_movie_struct_to_csharp(self):
        """Test converting movie JSON Structure to C#"""
        self.run_convert_struct_to_csharp("movie-ref")

    def test_convert_movie_struct_to_csharp_annotated(self):
        """Test converting movie with various annotations"""
        for system_text_json_annotation in [True, False]:
            for newtonsoft_json_annotation in [True, False]:
                for pascal_properties in [True, False]:
                    self.run_convert_struct_to_csharp(
                        "movie-ref",
                        system_text_json_annotation=system_text_json_annotation,
                        newtonsoft_json_annotation=newtonsoft_json_annotation,
                        pascal_properties=pascal_properties,
                    )

    def test_convert_primitives_struct_to_csharp(self):
        """Test converting all primitive types to C#"""
        self.run_convert_struct_to_csharp(
            "test-all-primitives", 
            system_text_json_annotation=True,
            pascal_properties=True
        )

    def test_convert_enum_const_struct_to_csharp(self):
        """Test converting enum and const keywords to C#"""
        self.run_convert_struct_to_csharp(
            "test-enum-const",
            system_text_json_annotation=True,
            pascal_properties=True
        )

    def test_convert_tuple_struct_to_csharp(self):
        """Test converting tuple types to C#"""
        self.run_convert_struct_to_csharp(
            "test-tuple",
            system_text_json_annotation=True,
            pascal_properties=True
        )

    def test_convert_choice_tagged_struct_to_csharp(self):
        """Test converting tagged choice types to C#"""
        self.run_convert_struct_to_csharp(
            "test-choice-tagged",
            system_text_json_annotation=True,
            pascal_properties=True
        )

    def test_convert_choice_inline_struct_to_csharp(self):
        """Test converting inline choice types with $extends to C#"""
        self.run_convert_struct_to_csharp(
            "test-choice-inline",
            system_text_json_annotation=True,
            pascal_properties=True
        )

    def test_convert_with_namespace(self):
        """Test conversion with custom namespace"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "address-ref.struct.json")
        cs_path = os.path.join(
            tempfile.gettempdir(), "avrotize", "address-namespace-cs"
        )
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        convert_structure_to_csharp(
            struct_path,
            cs_path,
            base_namespace="MyCustomNamespace",
            project_name="AddressProject",
            system_text_json_annotation=True,
            pascal_properties=True,
        )

        # Verify the namespace is correct in generated file
        generated_files = [
            f for f in os.listdir(cs_path) if f.endswith(".cs") and f != "Program.cs"
        ]
        assert len(generated_files) > 0

        with open(os.path.join(cs_path, generated_files[0]), "r", encoding="utf-8") as f:
            content = f.read()
            assert "namespace MyCustomNamespace" in content

        # Verify project builds
        assert (
            subprocess.check_call(
                ["dotnet", "test"], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr
            )
            == 0
        )

    def test_primitive_types(self):
        """Test conversion of various primitive types"""
        # Create a test structure with all primitive types
        cwd = os.getcwd()
        cs_path = os.path.join(
            tempfile.gettempdir(), "avrotize", "primitives-test-cs"
        )
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        # Create a test JSON Structure file
        test_struct = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "PrimitiveTypes",
            "properties": {
                "stringField": {"type": "string"},
                "boolField": {"type": "boolean"},
                "int32Field": {"type": "int32"},
                "int64Field": {"type": "int64"},
                "floatField": {"type": "float"},
                "doubleField": {"type": "double"},
                "dateField": {"type": "date"},
                "timeField": {"type": "time"},
                "datetimeField": {"type": "datetime"},
                "uuidField": {"type": "uuid"},
                "binaryField": {"type": "binary"},
            },
        }

        import json
        test_file = os.path.join(cs_path, "test.struct.json")
        with open(test_file, "w", encoding="utf-8") as f:
            json.dump(test_struct, f, indent=2)

        convert_structure_to_csharp(
            test_file,
            cs_path,
            system_text_json_annotation=True,
            pascal_properties=True,
        )

        # Verify the file contains expected type mappings
        generated_files = [
            f for f in os.listdir(cs_path) if f.endswith(".cs") and f != "Program.cs"
        ]
        assert len(generated_files) > 0

        with open(os.path.join(cs_path, generated_files[0]), "r", encoding="utf-8") as f:
            content = f.read()
            assert "string StringField" in content or "string stringField" in content
            assert "bool BoolField" in content or "bool boolField" in content
            assert "int Int32Field" in content or "int int32Field" in content
            assert "long Int64Field" in content or "long int64Field" in content
            assert "float FloatField" in content or "float floatField" in content
            assert "double DoubleField" in content or "double doubleField" in content
            assert "DateOnly DateField" in content or "DateOnly dateField" in content
            assert "TimeOnly TimeField" in content or "TimeOnly timeField" in content
            assert (
                "DateTimeOffset DatetimeField" in content
                or "DateTimeOffset datetimeField" in content
            )
            assert "Guid UuidField" in content or "Guid uuidField" in content
            assert "byte[] BinaryField" in content or "byte[] binaryField" in content

    def test_array_type(self):
        """Test conversion of array types"""
        cwd = os.getcwd()
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "array-test-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        test_struct = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "ArrayTest",
            "properties": {
                "stringArray": {"type": "array", "items": {"type": "string"}},
                "intArray": {"type": "array", "items": {"type": "int32"}},
            },
        }

        import json
        test_file = os.path.join(cs_path, "test.struct.json")
        with open(test_file, "w", encoding="utf-8") as f:
            json.dump(test_struct, f, indent=2)

        convert_structure_to_csharp(
            test_file, cs_path, system_text_json_annotation=True
        )

        generated_files = [
            f for f in os.listdir(cs_path) if f.endswith(".cs") and f != "Program.cs"
        ]
        assert len(generated_files) > 0

        with open(os.path.join(cs_path, generated_files[0]), "r", encoding="utf-8") as f:
            content = f.read()
            assert "List<string>" in content
            assert "List<int>" in content

    def test_map_type(self):
        """Test conversion of map types"""
        cwd = os.getcwd()
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "map-test-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        test_struct = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "MapTest",
            "properties": {
                "stringMap": {
                    "type": "map",
                    "keys": {"type": "string"},
                    "values": {"type": "string"},
                },
                "intMap": {
                    "type": "map",
                    "keys": {"type": "string"},
                    "values": {"type": "int32"},
                },
            },
        }

        import json
        test_file = os.path.join(cs_path, "test.struct.json")
        with open(test_file, "w", encoding="utf-8") as f:
            json.dump(test_struct, f, indent=2)

        convert_structure_to_csharp(
            test_file, cs_path, system_text_json_annotation=True
        )

        generated_files = [
            f for f in os.listdir(cs_path) if f.endswith(".cs") and f != "Program.cs"
        ]
        assert len(generated_files) > 0

        with open(os.path.join(cs_path, generated_files[0]), "r", encoding="utf-8") as f:
            content = f.read()
            assert "Dictionary<string, string>" in content
            assert "Dictionary<string, int>" in content

    def test_set_type(self):
        """Test conversion of set types"""
        cwd = os.getcwd()
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "set-test-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)

        test_struct = {
            "$schema": "https://json-structure.org/meta/extended/v0/#",
            "type": "object",
            "name": "SetTest",
            "properties": {
                "stringSet": {"type": "set", "items": {"type": "string"}},
                "intSet": {"type": "set", "items": {"type": "int32"}},
            },
        }

        import json
        test_file = os.path.join(cs_path, "test.struct.json")
        with open(test_file, "w", encoding="utf-8") as f:
            json.dump(test_struct, f, indent=2)

        convert_structure_to_csharp(
            test_file, cs_path, system_text_json_annotation=True
        )

        generated_files = [
            f for f in os.listdir(cs_path) if f.endswith(".cs") and f != "Program.cs"
        ]
        assert len(generated_files) > 0

        with open(os.path.join(cs_path, generated_files[0]), "r", encoding="utf-8") as f:
            content = f.read()
            assert "HashSet<string>" in content
            assert "HashSet<int>" in content


if __name__ == "__main__":
    pytest.main([__file__])
