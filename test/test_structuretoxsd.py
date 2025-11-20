import os
import sys
import tempfile
from os import path, getcwd
import xmlschema
import xmlunittest
import lxml.etree as ET


current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretoxsd import convert_structure_to_xsd


class TestStructureToXsd(xmlunittest.XmlTestCase):
    
    def create_xsd_from_structure(self, struct_name, xsd_path = '', xsd_ref_path = '', namespace = ''):
        cwd = getcwd()
        struct_full_path = path.join(cwd, "test", "struct", struct_name + ".struct.json")
        if not xsd_path:
            xsd_path = struct_name + ".xsd"
        if not xsd_ref_path:
            # '-ref' appended to the struct base file name
            xsd_ref_full_path = path.join(cwd, "test", "struct", struct_name + "-ref.xsd")
        else:
            xsd_ref_full_path = path.join(cwd, "test", "struct", xsd_ref_path)
        xsd_full_path = path.join(tempfile.gettempdir(), "avrotize", xsd_path)
        dir = os.path.dirname(xsd_full_path)
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)

        convert_structure_to_xsd(struct_full_path, xsd_full_path, namespace)
        
        # validate the xsd being correct
        #xmlschema.XMLSchema(xsd_full_path)
        
        # load both the result and the reference as strings
        with open(xsd_full_path, 'r') as file:
            xsd_full = file.read()
        
        if os.path.exists(xsd_ref_full_path):
            with open(xsd_ref_full_path, 'r') as file:
                xsd_ref = file.read()
            self.assertXmlEquivalentOutputs(xsd_full, xsd_ref)
        else:
            # If reference doesn't exist, just validate the generated XSD is well-formed
            try:
                xmlschema.XMLSchema(xsd_full_path)
                print(f"Warning: No reference file found at {xsd_ref_full_path}. Generated XSD is well-formed.")
            except Exception as e:
                self.fail(f"Generated XSD is not well-formed: {e}")
    
    def test_convert_basic_types_struct_to_xsd(self):
        """ Test converting basic types JSON Structure to XSD"""
        self.create_xsd_from_structure("basic-types")
        
    def test_convert_collections_struct_to_xsd(self):
        """ Test converting collections JSON Structure to XSD"""
        self.create_xsd_from_structure("collections")
        
    def test_convert_choice_types_struct_to_xsd(self):
        """ Test converting choice types JSON Structure to XSD"""
        self.create_xsd_from_structure("choice-types")
        
    def test_convert_nested_objects_struct_to_xsd(self):
        """ Test converting nested objects JSON Structure to XSD"""
        self.create_xsd_from_structure("nested-objects")
        
    def test_convert_numeric_types_struct_to_xsd(self):
        """ Test converting numeric types JSON Structure to XSD"""
        self.create_xsd_from_structure("numeric-types")
        
    def test_convert_temporal_types_struct_to_xsd(self):
        """ Test converting temporal types JSON Structure to XSD"""
        self.create_xsd_from_structure("temporal-types")
        
    def test_convert_validation_constraints_struct_to_xsd(self):
        """ Test converting validation constraints JSON Structure to XSD"""
        self.create_xsd_from_structure("validation-constraints")
        
    def test_convert_edge_cases_struct_to_xsd(self):
        """ Test converting edge cases JSON Structure to XSD"""
        self.create_xsd_from_structure("edge-cases")
        
    def test_convert_extensions_struct_to_xsd(self):
        """ Test converting extensions JSON Structure to XSD"""
        self.create_xsd_from_structure("extensions")
        
    def test_convert_complex_scenario_struct_to_xsd(self):
        """ Test converting complex scenario JSON Structure to XSD"""
        self.create_xsd_from_structure("complex-scenario")
