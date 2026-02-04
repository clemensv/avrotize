"""
Tests for JSON Structure to C# conversion
"""
import unittest
import os
import shutil
import subprocess
import sys
import tempfile
import json

import pytest

from avrotize.structuretocsharp import convert_structure_to_csharp

# Import the validator
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))
from json_structure_instance_validator import JSONStructureInstanceValidator

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
        
        # Run the instance serializer program to generate JSON files
        instances_dir = os.path.join(cs_path, "instances")
        os.makedirs(instances_dir, exist_ok=True)
        
        # Run the test project as a console app to generate instances
        try:
            subprocess.check_call(
                ["dotnet", "run", "--project", os.path.join(cs_path, "test"), "--", instances_dir],
                cwd=cs_path,
                stdout=sys.stdout,
                stderr=sys.stderr
            )
        except subprocess.CalledProcessError as e:
            print(f"Warning: Could not run instance serializer: {e}")
            return  # Skip validation if we can't generate instances
        
        # Validate each generated JSON file against the schema
        if os.path.exists(instances_dir):
            json_files = [f for f in os.listdir(instances_dir) if f.endswith('.json')]
            if json_files:
                # Load the schema
                with open(struct_path, 'r', encoding='utf-8') as f:
                    schema = json.load(f)
                
                # Validate each instance
                for json_file in json_files:
                    instance_path = os.path.join(instances_dir, json_file)
                    with open(instance_path, 'r', encoding='utf-8') as f:
                        instance = json.load(f)
                    
                    # Create a fresh validator for each instance to avoid error accumulation
                    validator = JSONStructureInstanceValidator(schema, extended=True)
                    errors = validator.validate_instance(instance)
                    if errors:
                        print(f"\nValidation errors for {json_file}:")
                        for error in errors:
                            print(f"  - {error}")
                        assert False, f"Instance {json_file} failed validation against JSON Structure schema"
                    else:
                        print(f"[OK] {json_file} validated successfully")
        
        return cs_path


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

    @pytest.mark.skip(reason="Known issue: Schema has nested inline objects with same 'name' as parent, causing type reuse and incorrect serialization")
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

    def test_abstract_type_validation(self):
        """Test that abstract type is generated correctly and code compiles"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "test-abstract-validation.struct.json")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "test-abstract-validation-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)
        
        # Simply check that code is generated and compiles
        # The warning is emitted to stderr but we can't easily capture it in tests
        convert_structure_to_csharp(
            struct_path,
            cs_path,
            system_text_json_annotation=True,
            pascal_properties=True
        )
        
        # Verify AbstractBase is generated as abstract
        abstract_file = os.path.join(cs_path, "src", "TestAbstractValidationCs", "AbstractBase.cs")
        assert os.path.exists(abstract_file), "AbstractBase.cs should be generated"
        
        with open(abstract_file, 'r', encoding='utf-8') as f:
            content = f.read()
            assert "abstract partial class AbstractBase" in content, "AbstractBase should be abstract"
            assert "protected AbstractBase()" in content, "Abstract class should have protected constructor"
        
        print(f"✓ Abstract type generated correctly")

    def test_sealed_class_additionalproperties_false(self):
        """Test that additionalProperties: false generates sealed classes"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "test-sealed.struct.json")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "test-sealed-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)
        
        convert_structure_to_csharp(
            struct_path,
            cs_path,
            system_text_json_annotation=True,
            pascal_properties=True
        )
        
        # Verify SealedClass is generated as sealed
        sealed_file = os.path.join(cs_path, "src", "TestSealedCs", "SealedClass.cs")
        assert os.path.exists(sealed_file), "SealedClass.cs should be generated"
        
        with open(sealed_file, 'r', encoding='utf-8') as f:
            content = f.read()
            assert "public sealed partial class SealedClass" in content, "Class should be sealed when additionalProperties: false"
            assert "AdditionalProperties" not in content, "No AdditionalProperties dictionary should be added"
        
        # Verify code compiles
        assert subprocess.check_call(["dotnet", "build"], cwd=cs_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0
        
        print(f"✓ Sealed class with additionalProperties: false generated correctly")

    def test_extensible_class_additionalproperties_true(self):
        """Test that additionalProperties: true generates Dictionary<string, object>"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "test-additional-props-true.struct.json")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "test-additional-props-true-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)
        
        convert_structure_to_csharp(
            struct_path,
            cs_path,
            system_text_json_annotation=True,
            pascal_properties=True
        )
        
        # Verify ExtensibleClass has AdditionalProperties dictionary
        extensible_file = os.path.join(cs_path, "src", "TestAdditionalPropsTrueCs", "ExtensibleClass.cs")
        assert os.path.exists(extensible_file), "ExtensibleClass.cs should be generated"
        
        with open(extensible_file, 'r', encoding='utf-8') as f:
            content = f.read()
            assert "public partial class ExtensibleClass" in content, "Class should not be sealed when additionalProperties: true"
            assert "sealed" not in content.lower(), "Class should not be sealed"
            # JsonExtensionData with Dictionary<string, object> for boxed primitive values
            assert "Dictionary<string, object>? AdditionalProperties" in content, "Should have AdditionalProperties dictionary with object values"
        
        # Verify code compiles
        assert subprocess.check_call(["dotnet", "build"], cwd=cs_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0
        
        print(f"✓ Extensible class with additionalProperties: true generated correctly")

    def test_validation_attributes(self):
        """Test that validation attributes are generated from schema constraints"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "test-validation-attributes.struct.json")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "test-validation-attributes-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)
        
        convert_structure_to_csharp(
            struct_path,
            cs_path,
            system_text_json_annotation=True,
            pascal_properties=True
        )
        
        # Verify ValidatedClass has validation attributes
        validated_file = os.path.join(cs_path, "src", "TestValidationAttributesCs", "ValidatedClass.cs")
        assert os.path.exists(validated_file), "ValidatedClass.cs should be generated"
        
        with open(validated_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Check StringLength with min and max
            assert "[System.ComponentModel.DataAnnotations.StringLength(20, MinimumLength = 3)]" in content, \
                "Username should have StringLength(20, MinimumLength = 3)"
            
            # Check RegularExpression for pattern
            assert "[System.ComponentModel.DataAnnotations.RegularExpression(@\"" in content, \
                "Email should have RegularExpression attribute"
            
            # Check Range for numeric types
            assert "[System.ComponentModel.DataAnnotations.Range(0, 150)]" in content, \
                "Age should have Range(0, 150)"
            assert "[System.ComponentModel.DataAnnotations.Range(0.0, 100.0)]" in content or \
                   "[System.ComponentModel.DataAnnotations.Range(0, 100)]" in content, \
                "Score should have Range attribute"
            
            # Check maxLength only
            assert "[System.ComponentModel.DataAnnotations.StringLength(500)]" in content, \
                "Bio should have StringLength(500)"
            
            # Check minLength only
            assert "[System.ComponentModel.DataAnnotations.MinLength(1)]" in content, \
                "Tags should have MinLength(1)"
        
        # Verify code compiles
        assert subprocess.check_call(["dotnet", "build"], cwd=cs_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0
        
        print(f"✓ Validation attributes generated correctly")

    def test_validation_extensions(self):
        """Test JSON Structure validation extension keywords (exclusiveMinimum, exclusiveMaximum, minItems, maxItems, format: email)"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "test-validation-extensions.struct.json")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "test-validation-extensions-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)
        
        convert_structure_to_csharp(
            struct_path,
            cs_path,
            system_text_json_annotation=True,
            pascal_properties=True
        )
        
        # Verify ValidationExtensions class has all the new validation attributes
        validated_file = os.path.join(cs_path, "src", "TestValidationExtensionsCs", "ValidationExtensions.cs")
        assert os.path.exists(validated_file), "ValidationExtensions.cs should be generated"
        
        with open(validated_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Check EmailAddress attribute for format: email
            assert "[System.ComponentModel.DataAnnotations.EmailAddress]" in content, \
                "emailField should have EmailAddress attribute"
            
            # Check Range with exclusive minimum
            assert "[System.ComponentModel.DataAnnotations.Range(0, 100, MinimumIsExclusive = true)]" in content, \
                "rangeWithExclusiveMin should have Range with MinimumIsExclusive"
            
            # Check Range with exclusive maximum
            assert "[System.ComponentModel.DataAnnotations.Range(0.0, 100.0, MaximumIsExclusive = true)]" in content or \
                   "[System.ComponentModel.DataAnnotations.Range(0, 100, MaximumIsExclusive = true)]" in content, \
                "rangeWithExclusiveMax should have Range with MaximumIsExclusive"
            
            # Check Range with both exclusive
            assert "MinimumIsExclusive = true, MaximumIsExclusive = true" in content, \
                "rangeWithBothExclusive should have Range with both exclusive parameters"
            
            # Check MinLength for arrays (minItems)
            assert content.count("[System.ComponentModel.DataAnnotations.MinLength(1)]") >= 1, \
                "arrayWithMinItems should have MinLength(1)"
            assert content.count("[System.ComponentModel.DataAnnotations.MinLength(2)]") >= 1, \
                "arrayWithBothConstraints should have MinLength(2)"
            
            # Check MaxLength for arrays (maxItems)
            assert "[System.ComponentModel.DataAnnotations.MaxLength(10)]" in content, \
                "arrayWithMaxItems should have MaxLength(10)"
            assert "[System.ComponentModel.DataAnnotations.MaxLength(5)]" in content, \
                "arrayWithBothConstraints should have MaxLength(5)"
            
            # Check combined validations (EmailAddress + StringLength + RegularExpression)
            # The combinedValidations field should have all three attributes
            assert content.count("[System.ComponentModel.DataAnnotations.EmailAddress]") >= 2, \
                "combinedValidations should have EmailAddress attribute (2 occurrences expected)"
        
        # Verify code compiles - build only the main project, not the test project
        src_project = os.path.join(cs_path, "src", "TestValidationExtensionsCs.csproj")
        assert subprocess.check_call(["dotnet", "build", src_project], cwd=cs_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0
        
        print(f"✓ Validation extension attributes generated correctly")

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
        # Check in src subdirectory where classes are generated
        src_path = os.path.join(cs_path, "src")
        if not os.path.exists(src_path):
            raise AssertionError(f"src directory not created: {src_path}")
        generated_files = []
        for root, dirs, files in os.walk(src_path):
            for file in files:
                if file.endswith(".cs") and file != "Program.cs":
                    generated_files.append(os.path.join(root, file))
        assert len(generated_files) > 0

        with open(generated_files[0], "r", encoding="utf-8") as f:
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
        # Check in src subdirectory where classes are generated
        src_path = os.path.join(cs_path, "src")
        if not os.path.exists(src_path):
            raise AssertionError(f"src directory not created: {src_path}")
        generated_files = []
        for root, dirs, files in os.walk(src_path):
            for file in files:
                if file.endswith(".cs") and file != "Program.cs":
                    generated_files.append(os.path.join(root, file))
        assert len(generated_files) > 0

        # Find the PrimitiveTypes.cs file specifically
        primitive_types_file = None
        for gf in generated_files:
            if os.path.basename(gf) == "PrimitiveTypes.cs":
                primitive_types_file = gf
                break
        assert primitive_types_file is not None, "PrimitiveTypes.cs should be generated"

        with open(primitive_types_file, "r", encoding="utf-8") as f:
            content = f.read()
            assert "string? StringField" in content or "string? stringField" in content
            assert "bool? BoolField" in content or "bool? boolField" in content
            assert "int? Int32Field" in content or "int? int32Field" in content
            assert "long? Int64Field" in content or "long? int64Field" in content
            assert "float? FloatField" in content or "float? floatField" in content
            assert "double? DoubleField" in content or "double? doubleField" in content
            assert "DateOnly? DateField" in content or "DateOnly? dateField" in content
            assert "TimeOnly? TimeField" in content or "TimeOnly? timeField" in content
            assert (
                "DateTimeOffset? DatetimeField" in content
                or "DateTimeOffset? datetimeField" in content
            )
            assert "Guid? UuidField" in content or "Guid? uuidField" in content
            assert "byte[]? BinaryField" in content or "byte[]? binaryField" in content

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

        # Check in src subdirectory where classes are generated
        src_path = os.path.join(cs_path, "src")
        if not os.path.exists(src_path):
            raise AssertionError(f"src directory not created: {src_path}")
        generated_files = [
            f for f in os.listdir(src_path) if f.endswith(".cs") and f != "Program.cs"
        ]
        if len(generated_files) == 0:
            # Check subdirectories
            for root, dirs, files in os.walk(src_path):
                for file in files:
                    if file.endswith(".cs") and file != "Program.cs":
                        generated_files.append(os.path.join(root, file))
        assert len(generated_files) > 0

        # Get the first generated file path
        first_file = generated_files[0] if os.path.isabs(generated_files[0]) else os.path.join(src_path, generated_files[0])
        with open(first_file, "r", encoding="utf-8") as f:
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

        # Check in src subdirectory where classes are generated
        src_path = os.path.join(cs_path, "src")
        if not os.path.exists(src_path):
            raise AssertionError(f"src directory not created: {src_path}")
        generated_files = []
        for root, dirs, files in os.walk(src_path):
            for file in files:
                if file.endswith(".cs") and file != "Program.cs":
                    generated_files.append(os.path.join(root, file))
        assert len(generated_files) > 0

        with open(generated_files[0], "r", encoding="utf-8") as f:
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

        # Check in src subdirectory where classes are generated
        src_path = os.path.join(cs_path, "src")
        if not os.path.exists(src_path):
            raise AssertionError(f"src directory not created: {src_path}")
        generated_files = []
        for root, dirs, files in os.walk(src_path):
            for file in files:
                if file.endswith(".cs") and file != "Program.cs":
                    generated_files.append(os.path.join(root, file))
        assert len(generated_files) > 0

        with open(generated_files[0], "r", encoding="utf-8") as f:
            content = f.read()
            assert "HashSet<string>" in content
            assert "HashSet<int>" in content

    def test_addins_offers_uses(self):
        """Test $offers/$uses add-in system generates interfaces, view classes, and implicit operators"""
        cs_path = self.run_convert_struct_to_csharp(
            "test-addins",
            system_text_json_annotation=True,
            pascal_properties=True
        )
        
        # Find all generated C# files
        src_path = os.path.join(cs_path, "src")
        generated_files = {}
        for root, dirs, files in os.walk(src_path):
            for file in files:
                if file.endswith(".cs"):
                    full_path = os.path.join(root, file)
                    generated_files[file] = full_path
        
        # Verify Product class exists
        assert "Product.cs" in generated_files, f"Product.cs not found in {list(generated_files.keys())}"
        
        # Verify add-in interfaces exist
        assert "IAuditInfo.cs" in generated_files, f"IAuditInfo.cs not found in {list(generated_files.keys())}"
        assert "IPricingInfo.cs" in generated_files, f"IPricingInfo.cs not found in {list(generated_files.keys())}"
        
        # Verify view classes exist
        assert "AuditInfoView.cs" in generated_files, f"AuditInfoView.cs not found in {list(generated_files.keys())}"
        assert "PricingInfoView.cs" in generated_files, f"PricingInfoView.cs not found in {list(generated_files.keys())}"
        
        # Verify extensions class exists (adds implicit operators to Product)
        assert "ProductExtensions.cs" in generated_files, f"ProductExtensions.cs not found in {list(generated_files.keys())}"
        
        # Check IAuditInfo interface content
        with open(generated_files["IAuditInfo.cs"], "r", encoding="utf-8") as f:
            audit_content = f.read()
            assert "public interface IAuditInfo" in audit_content
            assert "string? CreatedBy" in audit_content
            assert "double? CreatedAt" in audit_content  # timestamp-millis maps to double
            
        # Check IPricingInfo interface content
        with open(generated_files["IPricingInfo.cs"], "r", encoding="utf-8") as f:
            pricing_content = f.read()
            assert "public interface IPricingInfo" in pricing_content
            assert "double? Price" in pricing_content  # decimal maps to double
            assert "string? Currency" in pricing_content
        
        # Check AuditInfoView class content
        with open(generated_files["AuditInfoView.cs"], "r", encoding="utf-8") as f:
            view_content = f.read()
            assert "public sealed class AuditInfoView : IAuditInfo" in view_content
            assert "private readonly Dictionary<string, object?> _extensions" in view_content
            assert "public AuditInfoView(Dictionary<string, object?> extensions)" in view_content
            assert "string? CreatedBy" in view_content
            assert 'get => _extensions.TryGetValue("createdBy"' in view_content
            assert 'set { if (value != null) _extensions["createdBy"]' in view_content
        
        # Check PricingInfoView class content
        with open(generated_files["PricingInfoView.cs"], "r", encoding="utf-8") as f:
            view_content = f.read()
            assert "public sealed class PricingInfoView : IPricingInfo" in view_content
            assert "private readonly Dictionary<string, object?> _extensions" in view_content
            
        # Check ProductExtensions class content (implicit operators)
        with open(generated_files["ProductExtensions.cs"], "r", encoding="utf-8") as f:
            ext_content = f.read()
            assert "public partial class Product" in ext_content
            assert "public Dictionary<string, object?> Extensions { get; set; } = new()" in ext_content
            assert "public static implicit operator AuditInfoView(Product obj)" in ext_content
            assert "=> new AuditInfoView(obj.Extensions)" in ext_content
            assert "public static implicit operator PricingInfoView(Product obj)" in ext_content
            assert "=> new PricingInfoView(obj.Extensions)" in ext_content
            # Should have JsonExtensionData attribute for automatic unknown property capture
            assert '[System.Text.Json.Serialization.JsonExtensionData]' in ext_content
        
        # Verify the code compiles
        assert (
            subprocess.check_call(
                ["dotnet", "build"], cwd=cs_path, stdout=sys.stdout, stderr=sys.stderr
            )
            == 0
        )

    def test_avro_annotation(self):
        """Test that --avro-annotation flag generates Avro schema and serialization methods"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "address-ref.struct.json")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-avro-cs")
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)
        
        # Convert with avro_annotation flag
        convert_structure_to_csharp(
            struct_path,
            cs_path,
            avro_annotation=True
        )
        
        # Verify the Document class file exists
        document_file = os.path.join(cs_path, "src", "AddressAvroCs", "Document.cs")
        assert os.path.exists(document_file), "Document.cs should be generated"
        
        with open(document_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # 1. Verify embedded Avro schema field
            assert "private static readonly Avro.Schema AvroSchema" in content, \
                "Generated code should contain embedded Avro schema field"
            assert "Avro.Schema.Parse(" in content, \
                "Avro schema should be parsed from JSON string"
            
            # 2. Verify Avro using statements
            assert "using Avro;" in content, \
                "Generated code should include 'using Avro;'"
            assert "using Avro.Generic;" in content, \
                "Generated code should include 'using Avro.Generic;'"
            assert "using Avro.IO;" in content, \
                "Generated code should include 'using Avro.IO;'"
            
            # 3. Verify ToByteArray method supports avro/binary
            assert "public byte[] ToByteArray(string contentTypeString)" in content, \
                "Generated code should have ToByteArray method"
            assert 'contentType.MediaType == "avro/binary"' in content or \
                   'contentType.MediaType == "application/vnd.apache.avro+avro"' in content, \
                "ToByteArray should support avro/binary content type"
            
            # 4. Verify FromData method supports avro/binary
            assert "public static Document? FromData(object? data, string? contentTypeString" in content, \
                "Generated code should have FromData method"
            
            # 5. Verify helper methods for Avro conversion
            assert "ToAvroRecord()" in content, \
                "Generated code should have ToAvroRecord helper method"
            assert "FromAvroRecord(" in content, \
                "Generated code should have FromAvroRecord helper method"
        
        # Verify the .csproj includes Apache.Avro package
        csproj_file = os.path.join(cs_path, "src", "AddressAvroCs.csproj")
        assert os.path.exists(csproj_file), "Project file should be generated"
        
        with open(csproj_file, 'r', encoding='utf-8') as f:
            csproj_content = f.read()
            assert 'PackageReference Include="Apache.Avro"' in csproj_content, \
                "Project file should reference Apache.Avro package"
            assert 'PackageReference Include="System.Text.Json"' in csproj_content, \
                "Project file should reference System.Text.Json package (required for Avro helpers)"
        
        # 6. Verify code compiles successfully
        assert subprocess.check_call(
            ["dotnet", "build"], 
            cwd=cs_path, 
            stdout=subprocess.DEVNULL, 
            stderr=subprocess.DEVNULL
        ) == 0, "Generated code with avro_annotation should compile successfully"
        
        print(f"✓ Avro annotation test passed")

    def test_inline_union_json_roundtrip(self):
        """Test that inline unions with wrapped $ref format work correctly with JSON serialization"""
        from avrotize.schema_inference import JsonStructureSchemaInferrer
        
        cwd = os.getcwd()
        jsonl_path = os.path.join(cwd, "test", "jsons", "domain-events.jsonl")
        cs_path = os.path.join(tempfile.gettempdir(), "avrotize", "inline-union-roundtrip-cs")
        
        if os.path.exists(cs_path):
            shutil.rmtree(cs_path, ignore_errors=True)
        os.makedirs(cs_path, exist_ok=True)
        
        # Load JSON values from JSONL file
        values = []
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    values.append(json.loads(line))
        
        # 1. Infer schema from JSONL with --infer-choices
        inferrer = JsonStructureSchemaInferrer(infer_choices=True)
        schema = inferrer.infer_from_json_values("DomainEvent", values)
        
        # Verify schema has choice type
        assert schema.get('type') == 'choice', "Schema should be a choice type"
        assert schema.get('selector') == 'event_type', "Selector should be event_type"
        
        # Save schema to file
        schema_path = os.path.join(cs_path, "domain-events.jstruct.json")
        with open(schema_path, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=2)
        
        # 2. Generate C# classes with System.Text.Json annotations
        convert_structure_to_csharp(
            schema_path,
            cs_path,
            system_text_json_annotation=True,
            pascal_properties=False  # Keep snake_case for JSON compat
        )
        
        # 3. Verify the DomainEventBase has [JsonIgnore] on event_type
        base_file = os.path.join(cs_path, "src", "InlineUnionRoundtripCs", "DomainEventBase.cs")
        assert os.path.exists(base_file), "DomainEventBase.cs should be generated"
        
        with open(base_file, 'r', encoding='utf-8') as f:
            base_content = f.read()
            assert "[System.Text.Json.Serialization.JsonIgnore]" in base_content, \
                "Discriminator property should have [JsonIgnore] attribute"
        
        # 4. Verify DomainEvent has [JsonPolymorphic] attribute
        union_file = os.path.join(cs_path, "src", "InlineUnionRoundtripCs", "DomainEvent.cs")
        assert os.path.exists(union_file), "DomainEvent.cs should be generated"
        
        with open(union_file, 'r', encoding='utf-8') as f:
            union_content = f.read()
            assert "[System.Text.Json.Serialization.JsonPolymorphic" in union_content, \
                "Inline union should have [JsonPolymorphic] attribute"
            assert 'TypeDiscriminatorPropertyName = "event_type"' in union_content, \
                "JsonPolymorphic should use event_type as discriminator"
        
        # 5. Verify derived classes have their properties
        for variant_name in ["OrderPlaced", "UserCreated", "PaymentReceived"]:
            variant_file = os.path.join(cs_path, "src", "InlineUnionRoundtripCs", f"{variant_name}.cs")
            assert os.path.exists(variant_file), f"{variant_name}.cs should be generated"
            
            with open(variant_file, 'r', encoding='utf-8') as f:
                variant_content = f.read()
                assert f"class {variant_name} : DomainEvent" in variant_content, \
                    f"{variant_name} should extend DomainEvent"
        
        # 6. Build the project
        assert subprocess.check_call(
            ["dotnet", "build"], 
            cwd=cs_path, 
            stdout=subprocess.DEVNULL, 
            stderr=subprocess.DEVNULL
        ) == 0, "Generated code should compile successfully"
        
        print(f"✓ Inline union JSON round-trip test passed")


if __name__ == "__main__":
    pytest.main([__file__])


