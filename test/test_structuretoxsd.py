import os
import sys
import tempfile
from os import path, getcwd
import xmlunittest

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretoxsd import convert_structure_to_xsd


class TestStructureToXsd(xmlunittest.XmlTestCase):
    
    def create_xsd_from_structure(self, structure_path, xsd_path='', xsd_ref_path=''):
        cwd = getcwd()
        if structure_path.startswith("http"):
            structure_full_path = structure_path
        else:
            structure_full_path = path.join(cwd, "test", "avsc", structure_path)
        
        if not xsd_path:
            xsd_path = structure_path.replace(".struct.json", ".xsd")
        
        if not xsd_ref_path:
            # '-ref' appended to the structure base file name
            xsd_ref_full_path = path.join(cwd, "test", "avsc", xsd_path.replace(".xsd", "-struct-ref.xsd"))
        else:
            xsd_ref_full_path = path.join(cwd, "test", "avsc", xsd_ref_path)
        
        xsd_full_path = path.join(tempfile.gettempdir(), "avrotize", xsd_path)
        dir_path = os.path.dirname(xsd_full_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)

        convert_structure_to_xsd(structure_full_path, xsd_full_path)
        
        # Validate the xsd was created
        self.assertTrue(os.path.exists(xsd_full_path), f"XSD file was not created at {xsd_full_path}")
        
        with open(xsd_full_path, 'r') as file:
            xsd_full = file.read()
        
        # If reference file exists, compare
        if os.path.exists(xsd_ref_full_path):
            with open(xsd_ref_full_path, 'r') as file:
                xsd_ref = file.read()
            self.assertXmlEquivalentOutputs(xsd_full, xsd_ref)
        
        return xsd_full_path
    
    def test_convert_address_struct_to_xsd(self):
        """Test converting a JSON Structure schema to XSD"""
        self.create_xsd_from_structure("address-ref.struct.json")
    
    def test_convert_northwind_struct_to_xsd(self):
        """Test converting a JSON Structure schema to XSD"""
        self.create_xsd_from_structure("northwind-ref.struct.json")
    
    def test_convert_telemetry_struct_to_xsd(self):
        """Test converting a JSON Structure schema to XSD"""
        self.create_xsd_from_structure("telemetry-ref.struct.json")
    
    def test_convert_enumfield_struct_to_xsd(self):
        """Test converting a JSON Structure schema with enums to XSD"""
        self.create_xsd_from_structure("enumfield-ref.struct.json")
    
    def test_convert_twotypeunion_struct_to_xsd(self):
        """Test converting a JSON Structure schema with unions to XSD"""
        self.create_xsd_from_structure("twotypeunion-ref.struct.json")


if __name__ == '__main__':
    import unittest
    unittest.main()
