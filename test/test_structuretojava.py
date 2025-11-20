"""Test the avrotize.structuretojava module."""

import unittest
import os
import shutil
import subprocess
import sys
import tempfile
from os import path, getcwd

import pytest

from avrotize.structuretojava import convert_structure_to_java

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestStructureToJava(unittest.TestCase):
    def test_convert_simple_structure_to_java(self):
        """Test converting a simple JSON Structure schema to Java"""
        cwd = os.getcwd()
        # Use an existing structure schema from test/jsons
        structure_path = os.path.join(cwd, "test", "jsons", "anyof.structure.json")
        java_path = os.path.join(tempfile.gettempdir(), "avrotize", "anyof-structure-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_structure_to_java(structure_path, java_path, package_name="anyof.structure.java")
        
        # Check that files were generated
        src_dir = os.path.join(java_path, "src", "main", "java")
        assert os.path.exists(src_dir), f"Source directory not found: {src_dir}"
        
        # Try to build with Maven
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_jfrog_structure_to_java(self):
        """Test converting JFrog pipelines structure to Java"""
        cwd = os.getcwd()
        structure_path = os.path.join(cwd, "test", "jsons", "jfrog-pipelines.structure.json")
        java_path = os.path.join(tempfile.gettempdir(), "avrotize", "jfrog-structure-java")
        if os.path.exists(java_path):
            shutil.rmtree(java_path, ignore_errors=True)
        os.makedirs(java_path, exist_ok=True)

        convert_structure_to_java(structure_path, java_path, package_name="jfrog.pipelines.java")
        
        # Check that files were generated
        src_dir = os.path.join(java_path, "src", "main", "java")
        assert os.path.exists(src_dir), f"Source directory not found: {src_dir}"
        
        # Try to build with Maven
        assert subprocess.check_call(
            "mvn package -B", cwd=java_path, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0


if __name__ == '__main__':
    unittest.main()
