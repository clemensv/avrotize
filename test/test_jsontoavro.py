import json
import os
import sys
import tempfile
from os import path, getcwd
from fastavro.schema import load_schema
from jsoncomparison import NO_DIFF, Compare
import pytest
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

import unittest
from unittest.mock import patch


class TestJsonsToAvro(unittest.TestCase):

    def validate_avro_schema(self, avro_file_path):
        load_schema(avro_file_path)

    def create_avro_from_jsons(self, jsons_path, avro_path = '', avro_ref_path = '', namespace = "com.test.example", split_top_level_records = False):
        cwd = getcwd()
        if jsons_path.startswith("http"):
            jsons_full_path = jsons_path
        else:
            jsons_full_path = path.join(cwd, "test", "jsons", jsons_path)
        if not avro_path:
            avro_path = jsons_path.replace(".jsons", ".avsc").replace(".json", ".avsc")
        if not avro_ref_path:
            # '-ref' appended to the avsc base file name
            avro_ref_full_path = path.join(cwd, "test", "jsons", avro_path.replace(".avsc", "-ref.avsc"))
        else:
            avro_ref_full_path = path.join(cwd, "test", "jsons", "addlprops1-ref.avsc")
        avro_full_path = path.join(tempfile.gettempdir(), "avrotize", avro_path)
        dir = os.path.dirname(avro_full_path) if not split_top_level_records else avro_full_path
        if not os.path.exists(dir):
            os.makedirs(dir, exist_ok=True)

        convert_jsons_to_avro(jsons_full_path, avro_full_path, namespace, split_top_level_records=split_top_level_records)
        if not split_top_level_records:
            self.validate_avro_schema(avro_full_path)

            if os.path.exists(avro_ref_full_path):
                with open(avro_ref_full_path, "r") as ref:
                    expected = json.loads(ref.read())
                with open(avro_full_path, "r") as ref:
                    actual = json.loads(ref.read())
                diff = Compare().check(actual, expected)
                assert diff == NO_DIFF

        


    def test_convert_rootarray_jsons_to_avro(self):
        self.create_avro_from_jsons("rootarray.jsons", "rootarray.avsc")
        
    def test_convert_arraydef_jsons_to_avro(self):
        self.create_avro_from_jsons("arraydef.json", "arraydef.avsc")
        
    def test_convert_usingrefs_jsons_to_avro(self):
        self.create_avro_from_jsons("usingrefs.json", "usingrefs.avsc")
        
    def test_convert_anyof_jsons_to_avro(self):
        self.create_avro_from_jsons("anyof.json", "anyof.avsc")

    def test_convert_circularrefs_jsons_to_avro(self):
        self.create_avro_from_jsons("circularrefs.json", "circularrefs.avsc")        

    def test_convert_address_jsons_to_avro(self):
        self.create_avro_from_jsons("address.jsons", "address.avsc")

    def test_convert_movie_jsons_to_avro(self):
        self.create_avro_from_jsons("movie.jsons", "movie.avsc")

    def test_convert_person_jsons_to_avro(self):
        self.create_avro_from_jsons("person.jsons", "person.avsc")

    def test_convert_employee_jsons_to_avro(self):
        self.create_avro_from_jsons("employee.jsons", "employee.avsc")

    def test_convert_azurestorage_jsons_to_avro(self):
        self.create_avro_from_jsons("azurestorage.jsons", "azurestorage.avsc", namespace="Microsoft.Azure.Storage")

    def test_convert_azurestorage_jsons_to_avro_split(self):
        self.create_avro_from_jsons("azurestorage.jsons", "azurestorage-schemas", namespace="Microsoft.Azure.Storage", split_top_level_records=True)

    def test_convert_azurestorage_remote_jsons_to_avro(self):
        jsons_path = "https://raw.githubusercontent.com:443/Azure/azure-rest-api-specs/master/specification/eventgrid/data-plane/Microsoft.Storage/stable/2018-01-01/Storage.json"
        self.create_avro_from_jsons(jsons_path, "azurestorage.avsc", namespace="Microsoft.Azure.Storage")

    def test_convert_azurestorage_remote_deeplink_jsons_to_avro(self):
        jsons_path = "https://raw.githubusercontent.com:443/Azure/azure-rest-api-specs/master/specification/eventgrid/data-plane/Microsoft.Storage/stable/2018-01-01/Storage.json#/definitions/StorageLifecyclePolicyCompletedEventData"
        self.create_avro_from_jsons(jsons_path, "azurestoragedeep.avsc", namespace="Microsoft.Azure.Storage")

    def test_convert_addlprops1_jsons_to_avro(self):
        self.create_avro_from_jsons("addlprops1.json", "addlprops1.avsc")

    def test_convert_addlprops2_jsons_to_avro(self):
        self.create_avro_from_jsons("addlprops2.json", "addlprops2.avsc")

    def test_convert_addlprops3_jsons_to_avro(self):
        self.create_avro_from_jsons("addlprops3.json", "addlprops3.avsc")
        
    def test_convert_patternprops1_jsons_to_avro(self):
        self.create_avro_from_jsons("patternprops1.json", "patternprops1.avsc")

    def test_convert_patternprops2_jsons_to_avro(self):
        self.create_avro_from_jsons("patternprops2.json", "patternprops2.avsc")
    
    def test_convert_composition_jsons_to_avro(self):
        self.create_avro_from_jsons("composition.json", "composition.avsc")

    def test_convert_avro_avsc_jsons_to_avro(self):
        self.create_avro_from_jsons("avro-avsc.json", "avro-avsc.avsc")

    def test_convert_clouidify_jsons_to_avro(self):
        self.create_avro_from_jsons("cloudify.json", "cloudify.avsc")

    def test_convert_databricks_asset_bundles_to_avro(self):
        self.create_avro_from_jsons("databricks-asset-bundles.json", "databricks-asset-bundles.avsc")
        
    def test_convert_jfrog_pipelines_to_avro(self):
        self.create_avro_from_jsons("jfrog-pipelines.json", "jfrog-pipelines.avsc")

    def test_convert_kubernetes_definitions_jsons_to_avro(self):
        self.create_avro_from_jsons("kubernetes-definitions.json", "kubernetes-definitions.avsc")

    def test_convert_travis_jsons_to_avro(self):
        self.create_avro_from_jsons("travis.json", "travis.avsc")
    
    def test_convert_discriminated_union_simple_to_avro(self):
        self.create_avro_from_jsons("discriminated-union-simple.json", "discriminated-union-simple.avsc")
    
    def test_convert_discriminated_union_nested_to_avro(self):
        self.create_avro_from_jsons("discriminated-union-nested.json", "discriminated-union-nested.avsc")
    
    def test_convert_discriminated_union_array_complex_to_avro(self):
        self.create_avro_from_jsons("discriminated-union-array-complex.json", "discriminated-union-array-complex.avsc")
    
    def test_convert_optional_nested_union_to_avro(self):
        self.create_avro_from_jsons("optional-nested-union.json", "optional-nested-union.avsc")
    
    def test_convert_oneof_with_title_to_avro(self):
        self.create_avro_from_jsons("oneof-with-title.json", "oneof-with-title.avsc")

    def test_convert_conditional_schema_patterns_to_avro(self):
        """Test if/then/else conditional schema patterns conversion."""
        self.create_avro_from_jsons("conditional-schema-patterns.json", "conditional-schema-patterns.avsc")


class TestInlineEnumScoping(unittest.TestCase):
    """Regression tests for issue #338: inline enum fields that share a property
    name across multiple sibling schemas must each produce a distinct Avro enum
    with its own symbols (no shared/collapsed enum)."""

    def _convert(self, json_schema: dict) -> list:
        tmp_dir = os.path.join(tempfile.gettempdir(), "avrotize", "enum338")
        os.makedirs(tmp_dir, exist_ok=True)
        in_path = os.path.join(tmp_dir, "in.json")
        out_path = os.path.join(tmp_dir, "out.avsc")
        with open(in_path, "w", encoding="utf-8") as f:
            json.dump(json_schema, f)
        if os.path.exists(out_path):
            os.remove(out_path)
        convert_jsons_to_avro(in_path, out_path, "com.test.example")
        load_schema(out_path)  # must be valid Avro
        with open(out_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _collect_enums(self, node, found=None):
        """Recursively collect every Avro enum definition as
        (fullname, frozenset(symbols))."""
        if found is None:
            found = []
        if isinstance(node, list):
            for item in node:
                self._collect_enums(item, found)
        elif isinstance(node, dict):
            if node.get("type") == "enum" and "symbols" in node:
                ns = node.get("namespace", "")
                fullname = f"{ns}.{node['name']}" if ns else node["name"]
                found.append((fullname, frozenset(node["symbols"])))
            for value in node.values():
                self._collect_enums(value, found)
        return found

    def test_sibling_definitions_enums_are_distinct(self):
        schema = {
            "$defs": {
                "Order": {
                    "type": "object",
                    "properties": {"type": {"type": "string", "enum": ["Express"]}},
                },
                "Shipment": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["Air", "Ground", "Sea", "Rail"],
                        }
                    },
                },
            }
        }
        enums = self._collect_enums(self._convert(schema))
        symbol_sets = [symbols for _, symbols in enums]
        # Both distinct enums must be present with their own symbols.
        self.assertIn(frozenset(["Express"]), symbol_sets)
        self.assertIn(frozenset(["Air", "Ground", "Sea", "Rail"]), symbol_sets)
        # No two enums may share a fully-qualified name (collision guard).
        fullnames = [name for name, _ in enums]
        self.assertEqual(len(fullnames), len(set(fullnames)),
                         f"Enum fullnames collide: {fullnames}")
        # The Shipment enum must NOT be silently collapsed to the Order values.
        shipment_enum = next((s for _, s in enums
                              if s == frozenset(["Air", "Ground", "Sea", "Rail"])), None)
        self.assertIsNotNone(shipment_enum,
                             "Shipment inline enum lost its symbols (issue #338)")

    def test_inline_nested_object_enums_are_distinct(self):
        schema = {
            "type": "object",
            "properties": {
                "order": {
                    "type": "object",
                    "properties": {"type": {"type": "string", "enum": ["Express"]}},
                },
                "shipment": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["Air", "Ground", "Sea", "Rail"],
                        }
                    },
                },
            },
        }
        enums = self._collect_enums(self._convert(schema))
        symbol_sets = [symbols for _, symbols in enums]
        self.assertIn(frozenset(["Express"]), symbol_sets)
        self.assertIn(frozenset(["Air", "Ground", "Sea", "Rail"]), symbol_sets)
        fullnames = [name for name, _ in enums]
        self.assertEqual(len(fullnames), len(set(fullnames)),
                         f"Enum fullnames collide: {fullnames}")
