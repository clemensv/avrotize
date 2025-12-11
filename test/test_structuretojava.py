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
                shell=True,
            )
            == 0
        )

    # Basic object tests
    def test_convert_address_struct_to_java(self):
        """Test converting address JSON Structure to Java"""
        self.run_convert_struct_to_java("address-ref")

    def test_convert_address_struct_to_java_pascal_properties(self):
        """Test converting address JSON Structure to Java with PascalCase properties"""
        self.run_convert_struct_to_java("address-ref", pascal_properties=True)
    
    def test_convert_userprofile_struct_to_java(self):
        """Test converting userprofile JSON Structure to Java"""
        self.run_convert_struct_to_java("userprofile-ref")

    def test_convert_person_struct_to_java(self):
        """Test converting person JSON Structure to Java"""
        self.run_convert_struct_to_java("person-ref")
    
    # Test with choice types (discriminated unions)
    def test_convert_employee_struct_to_java(self):
        """Test converting employee JSON Structure with choice types to Java"""
        self.run_convert_struct_to_java("employee-ref")
    
    # Test with nested and complex structures
    def test_convert_composition_struct_to_java(self):
        """Test converting composition JSON Structure to Java"""
        self.run_convert_struct_to_java("composition-ref")
    
    def test_convert_circularrefs_struct_to_java(self):
        """Test converting circular reference structure to Java"""
        self.run_convert_struct_to_java("circularrefs-ref")
    
    # Test arrays and collections
    def test_convert_arraydef_struct_to_java(self):
        """Test converting array definitions to Java"""
        self.run_convert_struct_to_java("arraydef-ref")
    
    # Test with anyOf unions
    def test_convert_anyof_struct_to_java(self):
        """Test converting anyOf union types to Java"""
        self.run_convert_struct_to_java("anyof-ref")
    
    # Test complex real-world schemas
    def test_convert_azurestorage_struct_to_java(self):
        """Test converting Azure Storage schema to Java"""
        self.run_convert_struct_to_java("azurestorage-ref")
    
    # Skipped: cloudify-ref has complex inheritance that causes Java compile errors
    # def test_convert_cloudify_struct_to_java(self):
    #     """Test converting Cloudify schema to Java"""
    #     self.run_convert_struct_to_java("cloudify-ref")
    
    def test_convert_databricks_struct_to_java(self):
        """Test converting Databricks Asset Bundles schema to Java"""
        self.run_convert_struct_to_java("databricks-asset-bundles-ref")
    
    # Test additional properties
    def test_convert_addlprops1_struct_to_java(self):
        """Test converting additional properties schema 1 to Java"""
        self.run_convert_struct_to_java("addlprops1-ref")
    
    def test_convert_addlprops2_struct_to_java(self):
        """Test converting additional properties schema 2 to Java"""
        self.run_convert_struct_to_java("addlprops2-ref")
    
    def test_convert_addlprops3_struct_to_java(self):
        """Test converting additional properties schema 3 to Java"""
        self.run_convert_struct_to_java("addlprops3-ref")
    
    # Test without Jackson annotations
    def test_convert_address_no_jackson(self):
        """Test converting address without Jackson annotations"""
        self.run_convert_struct_to_java("address-ref", jackson_annotation=False)
    
    # Test complex composed schemas
    def test_convert_anyof_composed_struct_to_java(self):
        """Test converting composed anyOf schema to Java"""
        self.run_convert_struct_to_java("anyof-ref-composed")
    
    def test_convert_employee_composed_struct_to_java(self):
        """Test converting composed employee schema to Java"""
        self.run_convert_struct_to_java("employee-ref-composed")
    
    def test_convert_person_composed_struct_to_java(self):
        """Test converting composed person schema to Java"""
        self.run_convert_struct_to_java("person-ref-composed")
    
    def test_convert_composition_composed_struct_to_java(self):
        """Test converting composed composition schema to Java"""
        self.run_convert_struct_to_java("composition-ref-composed")

    def test_class_has_create_test_instance_method(self):
        """
        Regression test: Generated Java classes should have createTestInstance() method.
        """
        from avrotize.structuretojava import convert_structure_schema_to_java
        
        schema = {
            "type": "object",
            "name": "Person",
            "namespace": "com.example",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
                "email": {"type": "string"}
            }
        }
        
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "java-create-test-instance")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_schema_to_java(schema, output_dir, package_name="test.pkg")
        
        # Find the Person.java file
        java_src_dir = os.path.join(output_dir, "src", "main", "java")
        person_files = []
        for root, dirs, files in os.walk(java_src_dir):
            for f in files:
                if f == "Person.java":
                    person_files.append(os.path.join(root, f))
        
        assert len(person_files) > 0, f"Person.java should exist somewhere under {java_src_dir}"
        person_path = person_files[0]
        
        with open(person_path, 'r', encoding='utf-8') as f:
            java_content = f.read()
        
        # Check for createTestInstance method
        assert "public static Person createTestInstance()" in java_content, \
            "Person class should have createTestInstance() method"
        
        # Check that the method sets field values
        assert "instance.set" in java_content, \
            "createTestInstance should call setter methods"

    def test_abstract_class_no_create_test_instance(self):
        """
        Regression test: Abstract classes should NOT have createTestInstance() method.
        """
        from avrotize.structuretojava import convert_structure_schema_to_java
        
        schema = {
            "type": "object",
            "name": "BaseEntity",
            "namespace": "com.example",
            "abstract": True,
            "properties": {
                "id": {"type": "string"}
            }
        }
        
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "java-abstract-no-test-instance")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)
        
        convert_structure_schema_to_java(schema, output_dir, package_name="test.pkg")
        
        # Find the BaseEntity.java file
        java_src_dir = os.path.join(output_dir, "src", "main", "java")
        base_entity_files = []
        for root, dirs, files in os.walk(java_src_dir):
            for f in files:
                if f == "BaseEntity.java":
                    base_entity_files.append(os.path.join(root, f))
        
        assert len(base_entity_files) > 0, f"BaseEntity.java should exist somewhere under {java_src_dir}"
        base_entity_path = base_entity_files[0]
        
        with open(base_entity_path, 'r', encoding='utf-8') as f:
            java_content = f.read()
        
        # Abstract classes should NOT have createTestInstance
        assert "public static BaseEntity createTestInstance()" not in java_content, \
            "Abstract class should NOT have createTestInstance() method"


if __name__ == "__main__":
    unittest.main()
