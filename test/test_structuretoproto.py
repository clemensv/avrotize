""" Test JSON Structure to Proto conversion """

import os
import sys
import tempfile
from os import path, getcwd
import filecmp

import pytest

from avrotize.structuretoproto import convert_structure_to_proto
from avrotize import convert_proto_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch

class TestStructureToProto(unittest.TestCase):
    """ Test JSON Structure to Proto conversion"""
    
    def validate_proto_file(self, proto_file):
        """Validate a proto file by converting it to Avro (validates proto syntax)"""
        if not os.path.exists(proto_file):
            self.fail(f"Proto file does not exist: {proto_file}")
        
        # Try to convert proto to avro - if this succeeds, the proto is valid
        try:
            avro_output = os.path.join(tempfile.gettempdir(), "avrotize", "validation", os.path.basename(proto_file).replace('.proto', '.avsc'))
            os.makedirs(os.path.dirname(avro_output), exist_ok=True)
            
            # Get the directory containing the proto file for imports
            proto_root = os.path.dirname(proto_file)
            convert_proto_to_avro(proto_file, avro_output, proto_root=proto_root)
            
            # If we get here, conversion was successful
            return True
        except Exception as e:
            self.fail(f"Proto validation failed for {proto_file}: {str(e)}")
    
    def compare_proto_files(self, generated_file, reference_file):
        """Compare two proto files, ignoring whitespace differences"""
        if not os.path.exists(generated_file):
            self.fail(f"Generated file does not exist: {generated_file}")
        if not os.path.exists(reference_file):
            self.fail(f"Reference file does not exist: {reference_file}")
        
        # Read and normalize both files (remove extra whitespace, sort lines for comparison)
        with open(generated_file, 'r') as f:
            generated_content = f.read().strip()
        with open(reference_file, 'r') as f:
            reference_content = f.read().strip()
        
        # Simple comparison - in a real scenario you might want more sophisticated comparison
        if generated_content != reference_content:
            print(f"\n--- Generated ({generated_file}) ---")
            print(generated_content)
            print(f"\n--- Reference ({reference_file}) ---")
            print(reference_content)
            self.fail("Generated proto file does not match reference")
        
        # Validate the proto file by converting to Avro
        self.validate_proto_file(generated_file)
    
    def test_convert_basic_types_struct_to_proto(self):
        """ Test converting basic types JSON Structure to Proto"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "basic-types.struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "basic-types")
        ref_proto_path = os.path.join(cwd, "test", "struct", "basic-types.struct-ref.proto")
        
        dir_path = os.path.dirname(proto_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_structure_to_proto(struct_path, proto_path)
        
        # Check that the generated file matches the reference
        generated_file = os.path.join(proto_path, "basic_types.proto")
        self.compare_proto_files(generated_file, ref_proto_path)
    
    def test_convert_numeric_types_struct_to_proto(self):
        """ Test converting numeric types JSON Structure to Proto"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "numeric-types.struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "numeric-types")
        ref_proto_path = os.path.join(cwd, "test", "struct", "numeric-types.struct-ref.proto")
        
        dir_path = os.path.dirname(proto_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_structure_to_proto(struct_path, proto_path)
        
        # Check that the generated file matches the reference
        generated_file = os.path.join(proto_path, "numeric_types.proto")
        self.compare_proto_files(generated_file, ref_proto_path)
    
    def test_convert_temporal_types_struct_to_proto(self):
        """ Test converting temporal types JSON Structure to Proto"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "temporal-types.struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "temporal-types")
        ref_proto_path = os.path.join(cwd, "test", "struct", "temporal-types.struct-ref.proto")
        
        dir_path = os.path.dirname(proto_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_structure_to_proto(struct_path, proto_path)
        
        # Check that the generated file matches the reference
        generated_file = os.path.join(proto_path, "temporal_types.proto")
        self.compare_proto_files(generated_file, ref_proto_path)
    
    def test_convert_collections_struct_to_proto(self):
        """ Test converting collections JSON Structure to Proto"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "collections.struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "collections")
        ref_proto_path = os.path.join(cwd, "test", "struct", "collections.struct-ref.proto")
        
        dir_path = os.path.dirname(proto_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_structure_to_proto(struct_path, proto_path)
        
        # Check that the generated file matches the reference
        generated_file = os.path.join(proto_path, "collections.proto")
        self.compare_proto_files(generated_file, ref_proto_path)
    
    def test_convert_choice_types_struct_to_proto(self):
        """ Test converting choice types JSON Structure to Proto"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "choice-types.struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "choice-types")
        ref_proto_path = os.path.join(cwd, "test", "struct", "choice-types.struct-ref.proto")
        
        dir_path = os.path.dirname(proto_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_structure_to_proto(struct_path, proto_path)
        
        # Check that the generated file matches the reference
        generated_file = os.path.join(proto_path, "choice_types.proto")
        self.compare_proto_files(generated_file, ref_proto_path)
    
    def test_convert_advanced_features_struct_to_proto(self):
        """ Test converting advanced features JSON Structure to Proto"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "advanced-features.struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "advanced-features")
        ref_proto_path = os.path.join(cwd, "test", "struct", "advanced-features.struct-ref.proto")
        
        dir_path = os.path.dirname(proto_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_structure_to_proto(struct_path, proto_path)
        
        # Check that the generated file matches the reference
        generated_file = os.path.join(proto_path, "advanced_features.proto")
        self.compare_proto_files(generated_file, ref_proto_path)
    
    def test_convert_inheritance_struct_to_proto(self):
        """ Test converting inheritance JSON Structure to Proto"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "inheritance.struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "inheritance")
        ref_proto_path = os.path.join(cwd, "test", "struct", "inheritance.struct-ref.proto")
        
        dir_path = os.path.dirname(proto_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_structure_to_proto(struct_path, proto_path)
        
        # Check that the generated file matches the reference
        generated_file = os.path.join(proto_path, "inheritance.proto")
        self.compare_proto_files(generated_file, ref_proto_path)
    
    def test_convert_tuple_test_struct_to_proto(self):
        """ Test converting tuple test JSON Structure to Proto"""
        cwd = os.getcwd()
        struct_path = os.path.join(cwd, "test", "struct", "tuple-test.struct.json")
        proto_path = os.path.join(tempfile.gettempdir(), "avrotize", "tuple-test")
        ref_proto_path = os.path.join(cwd, "test", "struct", "tuple-test.struct-ref.proto")
        
        dir_path = os.path.dirname(proto_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        convert_structure_to_proto(struct_path, proto_path)
        
        # Check that the generated file matches the reference
        generated_file = os.path.join(proto_path, "tuple_test.proto")
        self.compare_proto_files(generated_file, ref_proto_path)

if __name__ == '__main__':
    unittest.main()
