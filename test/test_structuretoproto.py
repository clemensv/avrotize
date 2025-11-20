""" Test JSON Structure to Protocol Buffers conversion """

import os
import sys
import tempfile
import shutil

import pytest

from avrotize.structuretoproto import convert_structure_to_proto

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest


class TestStructureToProto(unittest.TestCase):
    """ Test JSON Structure to Protocol Buffers conversion """

    def test_convert_address_struct_to_proto(self):
        """ Test converting a JSON Structure address schema to Proto """
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "address-ref.struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-struct-proto")
        
        if os.path.exists(proto_path):
            shutil.rmtree(proto_path, ignore_errors=True)
        os.makedirs(proto_path, exist_ok=True)
        
        convert_structure_to_proto(struct_path, proto_path)
        
        # Verify proto file was created
        proto_files = [f for f in os.listdir(proto_path) if f.endswith('.proto')]
        assert len(proto_files) > 0, "No .proto files were generated"
        
        # Read and verify proto content
        proto_file_path = os.path.join(proto_path, proto_files[0])
        with open(proto_file_path, 'r', encoding='utf-8') as f:
            proto_content = f.read()
        
        # Check for expected proto3 syntax
        assert 'syntax = "proto3"' in proto_content
        assert 'message' in proto_content or 'enum' in proto_content

    def test_convert_simple_object_to_proto(self):
        """ Test converting a simple JSON Structure object to Proto """
        cwd = os.getcwd()
        # Use a simple test schema
        struct_path = os.path.join(cwd, "test", "jsons", "address-ref.struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "simple-struct-proto")
        
        if os.path.exists(proto_path):
            shutil.rmtree(proto_path, ignore_errors=True)
        os.makedirs(proto_path, exist_ok=True)
        
        convert_structure_to_proto(struct_path, proto_path, naming_mode='pascal')
        
        # Verify proto file was created
        proto_files = [f for f in os.listdir(proto_path) if f.endswith('.proto')]
        assert len(proto_files) > 0

    def test_convert_with_different_naming_modes(self):
        """ Test converting with different naming conventions """
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "jsons", "address-ref.struct.json")
        
        for naming_mode in ['pascal', 'camel', 'snake']:
            proto_path = os.path.join(tempfile.gettempdir(), "avrotize", f"naming-{naming_mode}-proto")
            
            if os.path.exists(proto_path):
                shutil.rmtree(proto_path, ignore_errors=True)
            os.makedirs(proto_path, exist_ok=True)
            
            convert_structure_to_proto(struct_path, proto_path, naming_mode=naming_mode)
            
            # Verify proto file was created
            proto_files = [f for f in os.listdir(proto_path) if f.endswith('.proto')]
            assert len(proto_files) > 0, f"No .proto files generated for {naming_mode} naming mode"

    def test_convert_with_primitives(self):
        """ Test converting schemas with various primitive types """
        cwd = os.getcwd()
        # Find a schema with different types
        test_schemas = [
            "address-ref.struct.json",
            "arraydef-ref.struct.json",
        ]
        
        for schema_name in test_schemas:
            struct_path = os.path.join(cwd, "test", "jsons", schema_name)
            if not os.path.exists(struct_path):
                continue
                
            proto_path = os.path.join(tempfile.gettempdir(), "avrotize", 
                                     f"{os.path.splitext(schema_name)[0]}-proto")
            
            if os.path.exists(proto_path):
                shutil.rmtree(proto_path, ignore_errors=True)
            os.makedirs(proto_path, exist_ok=True)
            
            convert_structure_to_proto(struct_path, proto_path)
            
            # Verify proto file was created
            proto_files = [f for f in os.listdir(proto_path) if f.endswith('.proto')]
            assert len(proto_files) > 0, f"No .proto files generated for {schema_name}"
            
            # Read and verify basic structure
            proto_file_path = os.path.join(proto_path, proto_files[0])
            with open(proto_file_path, 'r', encoding='utf-8') as f:
                proto_content = f.read()
            
            assert 'syntax = "proto3"' in proto_content
            assert 'package' in proto_content


if __name__ == '__main__':
    unittest.main()
