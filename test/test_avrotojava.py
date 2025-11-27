"""Test the avrotize.avrotojava module."""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile
from os import path, getcwd
from distutils.dir_util import copy_tree

import pytest

from avrotize.avrotojava import convert_avro_to_java
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestAvroToJava(unittest.TestCase):
    def test_convert_address_avsc_to_java(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        java_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_avro_to_java(avro_path, java_path, package_name="address.java")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_address_avsc_to_java_avro_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        java_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-java-avro")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_avro_to_java(avro_path, java_path, avro_annotation=True, package_name="address.java.avro")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_address_avsc_to_java_jackson_annotation(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        java_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-java-jackson")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_avro_to_java(avro_path, java_path,
                             jackson_annotation=True, pascal_properties=True, package_name="address.java.jackson")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_telemetry_avsc_to_java(self):
        """ Test converting a telemetry.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        java_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_avro_to_java(avro_path, java_path, package_name="telemetry.java.avro")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
        
    def test_convert_twotypeunion_avsc_to_java(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "twotypeunion.avsc")
        java_codegen_path = os.path.join(tempfile.gettempdir(), "avrotize", "twotypeunion", "twotypeunion-java")
        java_test_path = os.path.join(tempfile.gettempdir(), "avrotize", "twotypeunion")
        test_path = os.path.join(cwd, "test", "java", "twotypeunion")
        if os.path.exists(java_test_path):
            shutil.rmtree(java_test_path, ignore_errors=True)
        os.makedirs(java_test_path, exist_ok=True)

        convert_avro_to_java(avro_path, java_codegen_path, jackson_annotation=True, avro_annotation=True, package_name="twotypeunion")
        copy_tree(test_path, java_test_path)
        assert subprocess.check_call(
            "mvn clean test -B", cwd=java_test_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
        
    def test_convert_complexunion_avsc_to_java(self):
        """ Test converting an address.avsc file to C# """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "complexunion.avsc")
        java_codegen_path = os.path.join(tempfile.gettempdir(), "avrotize", "complexunion", "complexunion-java")
        java_test_path = os.path.join(tempfile.gettempdir(), "avrotize", "complexunion")
        test_path = os.path.join(cwd, "test", "java", "complexunion")
        if os.path.exists(java_test_path):
            shutil.rmtree(java_test_path, ignore_errors=True)
        os.makedirs(java_test_path, exist_ok=True)

        convert_avro_to_java(avro_path, java_codegen_path, jackson_annotation=True, avro_annotation=True, package_name="complexunion")
        copy_tree(test_path, java_test_path)
        assert subprocess.check_call(
            "mvn clean test -B", cwd=java_test_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_jfrog_pipelines_jsons_to_avro_to_java(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        java_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_java(avro_path, java_path)
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_jfrog_pipelines_jsons_to_avro_to_java_annotated(self):
        """ Test converting a jfrog-pipelines.json file to C# """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        java_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-java-ann")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_java(avro_path, java_path, pascal_properties=True,
                             avro_annotation=True, jackson_annotation=True, package_name="jfrog.pipelines.java.avro")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
    
    def test_convert_discriminated_union_simple_jsons_to_avro_to_java(self):
        """ Test converting a simple discriminated union JSON Schema to Java """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "discriminated-union-simple.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "discriminated-union-simple.avsc")
        java_path = path.join(tempfile.gettempdir(), "avrotize", "discriminated-union-simple-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_java(avro_path, java_path, pascal_properties=True,
                             avro_annotation=True, jackson_annotation=True, package_name="discriminated.union.simple")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
    
    def test_convert_discriminated_union_nested_jsons_to_avro_to_java(self):
        """ Test converting a nested discriminated union JSON Schema to Java """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "discriminated-union-nested.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "discriminated-union-nested.avsc")
        java_path = path.join(tempfile.gettempdir(), "avrotize", "discriminated-union-nested-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_java(avro_path, java_path, pascal_properties=True,
                             avro_annotation=True, jackson_annotation=True, package_name="discriminated.union.nested")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
    
    def test_convert_discriminated_union_array_complex_jsons_to_avro_to_java(self):
        """ Test converting a complex array union (non-discriminated) JSON Schema to Java """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "discriminated-union-array-complex.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "discriminated-union-array-complex.avsc")
        java_path = path.join(tempfile.gettempdir(), "avrotize", "discriminated-union-array-complex-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_java(avro_path, java_path, pascal_properties=True,
                             avro_annotation=True, jackson_annotation=True, package_name="complex.array.union")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
    
    def test_convert_optional_nested_union_jsons_to_avro_to_java(self):
        """ Test converting optional nested union JSON Schema to Java """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "optional-nested-union.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "optional-nested-union.avsc")
        java_path = path.join(tempfile.gettempdir(), "avrotize", "optional-nested-union-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_java(avro_path, java_path, pascal_properties=True,
                             avro_annotation=True, jackson_annotation=True, package_name="optional.nested.union")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
    
    def test_convert_oneof_with_title_jsons_to_avro_to_java(self):
        """ Test converting a oneOf with title JSON Schema to Java """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "oneof-with-title.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "oneof-with-title.avsc")
        java_path = path.join(tempfile.gettempdir(), "avrotize", "oneof-with-title-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_java(avro_path, java_path, pascal_properties=True,
                             avro_annotation=True, jackson_annotation=True, package_name="oneof.with.title")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
    
    def test_convert_restricted_name_avsc_to_java(self):
        """ Test converting an avsc file with a restricted name in the namespace to Java """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "restricted-namespace.avsc")
        java_path = os.path.join(tempfile.gettempdir(), "avrotize", "restricted-namespace-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

    def test_convert_enum_lower_avsc_to_java(self):
        """ Test converting an avsc file with an enum value that is lowercase to Java """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "enumfield-lower.avsc")
        java_path = os.path.join(tempfile.gettempdir(), "avrotize", "enumfield-lower-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_avro_to_java(avro_path, java_path, package_name="enumfield.lower.java")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
    
    def test_fixed_field_avsc_to_java(self):
        """ Test converting an avsc file with a fixed field to Java """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "fixed-field.avsc")
        java_path = os.path.join(tempfile.gettempdir(), "avrotize", "fixed-field-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_avro_to_java(avro_path, java_path, package_name="fixed.field.java")
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_enum_constants_use_screaming_case(self):
        """ Test that Java enum constants are generated in SCREAMING_CASE """
        import json
        
        # Create test schema with mixed-case symbols
        schema = {
            "type": "enum",
            "name": "SwitchSource",
            "symbols": ["PhysicalSwitch", "AppSwitch", "VoiceSwitch"]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc', delete=False) as f:
            json.dump(schema, f)
            schema_file = f.name
        
        try:
            java_path = os.path.join(tempfile.gettempdir(), "avrotize", "enum-screaming-case")
            if os.path.exists(java_path):
                shutil.rmtree(java_path, ignore_errors=True)
            os.makedirs(java_path, exist_ok=True)
            
            convert_avro_to_java(schema_file, java_path, package_name="test.enums")
            
            # Read generated enum file
            enum_file = os.path.join(java_path, 'src', 'main', 'java', 'test', 'enums', 'SwitchSource.java')
            with open(enum_file, 'r') as f:
                content = f.read()
            
            # Verify SCREAMING_CASE constants are present
            assert 'PHYSICALSWITCH' in content, f"Expected 'PHYSICALSWITCH' in enum"
            assert 'APPSWITCH' in content, f"Expected 'APPSWITCH' in enum"
            assert 'VOICESWITCH' in content, f"Expected 'VOICESWITCH' in enum"
            
            # Read generated test file
            test_file = os.path.join(java_path, 'src', 'test', 'java', 'test', 'enums', 'SwitchSourceTest.java')
            with open(test_file, 'r') as f:
                test_content = f.read()
            
            # Verify test uses SCREAMING_CASE
            assert 'valueOf("PHYSICALSWITCH")' in test_content, "Test should use SCREAMING_CASE"
        finally:
            os.unlink(schema_file)

    def test_enum_with_reserved_words_use_screaming_case(self):
        """ Test that Java reserved word enum symbols are prefixed with underscore and use SCREAMING_CASE """
        import json
        
        # Create test schema with Java reserved words as symbols
        schema = {
            "type": "enum",
            "name": "ReservedEnum",
            "symbols": ["true", "false", "class"]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc', delete=False) as f:
            json.dump(schema, f)
            schema_file = f.name
        
        try:
            java_path = os.path.join(tempfile.gettempdir(), "avrotize", "enum-reserved-words")
            if os.path.exists(java_path):
                shutil.rmtree(java_path, ignore_errors=True)
            os.makedirs(java_path, exist_ok=True)
            
            convert_avro_to_java(schema_file, java_path, package_name="test.reserved")
            
            # Read generated enum file
            enum_file = os.path.join(java_path, 'src', 'main', 'java', 'test', 'reserved', 'ReservedEnum.java')
            with open(enum_file, 'r') as f:
                content = f.read()
            
            # Verify reserved words are prefixed with underscore and uppercased
            assert '_TRUE' in content, f"Expected '_TRUE' for reserved word 'true'"
            assert '_FALSE' in content, f"Expected '_FALSE' for reserved word 'false'"
            assert '_CLASS' in content, f"Expected '_CLASS' for reserved word 'class'"
        finally:
            os.unlink(schema_file)
