import os
import shutil
import subprocess
import sys
import tempfile
from os import path, getcwd
import pytest
import unittest
from unittest.mock import patch

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


from avrotize.avrotorust import (
    convert_avro_schema_to_rust,
    convert_avro_to_rust,
)
from avrotize.common import generic_type
from avrotize.jsonstoavro import convert_jsons_to_avro
import pytest


class TestAvroToRust(unittest.TestCase):
    
    # Timeout in seconds for cargo commands
    CARGO_TIMEOUT = 300
    
    def run_convert_to_rust(self, name:str, avro_annotation:bool=False, serde_annotation:bool=False):
        """ Test converting an avsc file to Rust and compiling/testing it """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{name}.avsc")
        rust_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{name}-rs{'' if not avro_annotation else '-avro'}{'' if not serde_annotation else '-serde'}")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)        
        convert_avro_to_rust(avro_path, rust_path, package_name=name, avro_annotation=avro_annotation, serde_annotation=serde_annotation)
        assert subprocess.check_call(
            ['cargo', 'test'], cwd=rust_path, stdout=sys.stdout, stderr=sys.stderr, timeout=self.CARGO_TIMEOUT) == 0
        return rust_path

    def assert_module_scoped_schemas(self, rust_path: str, expected_modules: set[str]):
        """Assert each expected generated module owns one module-scoped SCHEMA."""
        schema_modules = set()
        for root, _, files in os.walk(os.path.join(rust_path, "src")):
            for file_name in files:
                if not file_name.endswith(".rs"):
                    continue
                file_path = os.path.join(root, file_name)
                with open(file_path, "r", encoding="utf-8") as generated_file:
                    source = generated_file.read()
                if "pub static ref SCHEMA" not in source:
                    continue
                relative_path = os.path.relpath(file_path, os.path.join(rust_path, "src"))
                schema_modules.add(relative_path.replace(os.sep, "/"))
                self.assertEqual(1, source.count("pub static ref SCHEMA"))
                self.assertIn("\nlazy_static! {\n", source)
        self.assertEqual(expected_modules, schema_modules)
        
    def test_convert_address_avsc_to_rust(self):
        """ Test converting an address.avsc file to Rust """
        self.run_convert_to_rust("address", True, True)
        self.run_convert_to_rust("address", True, False)
        self.run_convert_to_rust("address", False, True)
        self.run_convert_to_rust("address", False, False)
        
    def test_convert_twotypeunion_avsc_to_rust(self):
        """ Test converting an twotypeunion.avsc file to Rust """
        self.run_convert_to_rust("twotypeunion", True, True)
        self.run_convert_to_rust("twotypeunion", True, False)
        self.run_convert_to_rust("twotypeunion", False, True)
        self.run_convert_to_rust("twotypeunion", False, False)
    
    def test_convert_typemapunion_avsc_to_rust(self):
        """ Test converting an twotypeunion.avsc file to Rust """
        # Skip avro_annotation=True combinations - apache-avro library has limitations
        # with maps that have union value types (returns "Can only encode value type Map as one of [Map]")
        self.run_convert_to_rust("typemapunion", False, True)
        self.run_convert_to_rust("typemapunion", False, False)
    

    def test_convert_telemetry_avsc_to_rust(self):
        """ Test converting a telemetry.avsc file to Rust """
        self.run_convert_to_rust("telemetry", True, True)
        self.run_convert_to_rust("telemetry", True, False)
        self.run_convert_to_rust("telemetry", False, True)
        self.run_convert_to_rust("telemetry", False, False)

    def test_convert_enum_avro_annotations_to_rust(self):
        """Compile an enum-only crate with module-scoped Avro schema support."""
        rust_path = self.run_convert_to_rust("rust-enum-annotation", True, True)
        self.assert_module_scoped_schemas(
            rust_path,
            {"issue406/enum_only/status.rs"},
        )
        avro_only_path = self.run_convert_to_rust("rust-enum-annotation", True, False)
        self.assert_module_scoped_schemas(
            avro_only_path,
            {"issue406/enum_only/status.rs"},
        )

    def test_convert_union_avro_annotations_to_rust(self):
        """Compile a union-bearing crate with Avro binary round-trip coverage."""
        rust_path = self.run_convert_to_rust("rust-union-annotation", True, True)
        self.assert_module_scoped_schemas(
            rust_path,
            {
                "issue406/union_only/numericunion.rs",
                "issue406/union_only/arrayvaluesunion.rs",
                "issue406/union_only/mapvaluesunion.rs",
                "issue406/union_only/nestedholder.rs",
                "issue406/union_only/nullableunion.rs",
                "issue406/union_only/unionholder.rs",
                "issue406/union_only/valueunion.rs",
            },
        )
        union_holder_path = os.path.join(
            rust_path,
            "src",
            "issue406",
            "union_only",
            "unionholder.rs",
        )
        with open(union_holder_path, "r", encoding="utf-8") as generated_file:
            avro_source = generated_file.read()
        self.assertIn(
            "pub nullable: Option<crate::issue406::union_only::nullableunion::NullableUnion>",
            avro_source,
        )

        legacy_field = (
            "pub nullable: "
            "crate::issue406::union_only::nullableunion::NullableUnion"
        )
        for serde_annotation in (False, True):
            compatible_path = self.run_convert_to_rust(
                "rust-union-annotation",
                False,
                serde_annotation,
            )
            compatible_holder_path = os.path.join(
                compatible_path,
                "src",
                "issue406",
                "union_only",
                "unionholder.rs",
            )
            with open(
                compatible_holder_path,
                "r",
                encoding="utf-8",
            ) as generated_file:
                compatible_source = generated_file.read()
            self.assertIn(legacy_field, compatible_source)
            self.assertNotIn("pub nullable: Option<", compatible_source)

    def test_convert_multitype_avro_annotations_to_rust(self):
        """Compile records, enums, and unions that each define SCHEMA in one crate."""
        rust_path = self.run_convert_to_rust("rust-multitype-annotations", True, True)
        self.assert_module_scoped_schemas(
            rust_path,
            {
                "issue406/multitype/alternate.rs",
                "issue406/multitype/composite.rs",
                "issue406/multitype/choiceunion.rs",
                "issue406/multitype/inlinekind.rs",
                "issue406/multitype/simple.rs",
                "issue406/multitype/standalone.rs",
                "issue406/multitype/valueunion.rs",
                "issue406/multitype/wrapper.rs",
            },
        )

    def test_convert_anyvalue_reference_to_rust_default(self):
        """Keep AnyValue mapped to serde_json::Value even when it is indexed."""
        rust_path = self.run_convert_to_rust(
            "rust-anyvalue-reference",
            False,
            False,
        )
        holder_path = os.path.join(
            rust_path,
            "src",
            "issue406",
            "anyvalue",
            "anyvalueholder.rs",
        )
        with open(holder_path, "r", encoding="utf-8") as generated_file:
            source = generated_file.read()
        self.assertIn("pub payload: serde_json::Value", source)
        self.assertNotIn("crate::avrotize::anyvalue::AnyValue", source)

    def test_convert_generic_anyvalue_union_to_rust(self):
        """Compile Avro decoding for the generic AnyValue union."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-generic-anyvalue-rs-avro-serde",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        schema = {
            "type": "record",
            "name": "GenericHolder",
            "namespace": "issue406.generic",
            "fields": [
                {
                    "name": "payload",
                    "type": generic_type(),
                }
            ],
        }
        convert_avro_schema_to_rust(
            schema,
            rust_path,
            package_name="rust-generic-anyvalue",
            avro_annotation=True,
            serde_annotation=True,
        )
        assert subprocess.check_call(
            ['cargo', 'test'],
            cwd=rust_path,
            stdout=sys.stdout,
            stderr=sys.stderr,
            timeout=self.CARGO_TIMEOUT,
        ) == 0

    def test_convert_jfrog_pipelines_jsons_to_avro_to_rust(self):
        """ Test converting a jfrog-pipelines.json file to Rust """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        rust_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-rs")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_rust(avro_path, rust_path, package_name="jfrog_pipelines")
        assert subprocess.check_call(
            ['cargo', 'test'], cwd=rust_path, stdout=sys.stdout, stderr=sys.stderr, timeout=self.CARGO_TIMEOUT) == 0

    @pytest.mark.skip(reason="jfrog-pipelines has deeply nested structurally identical union variants (Auto1/Auto2) that cannot round-trip with serde untagged unions")
    def test_convert_jfrog_pipelines_jsons_to_avro_to_rust_typed_json(self):
        """ Test converting a jfrog-pipelines.json file to Rust """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        rust_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-rs-typed-json")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_rust(avro_path, rust_path, package_name="jfrog_pipelines", serde_annotation=True)
        assert subprocess.check_call(
            ['cargo', 'test'], cwd=rust_path, stdout=sys.stdout, stderr=sys.stderr, timeout=self.CARGO_TIMEOUT) == 0

    @pytest.mark.skip(reason="jfrog-pipelines has deeply nested structurally identical union variants (Auto1/Auto2) that cannot round-trip with serde untagged unions")
    def test_convert_jfrog_pipelines_jsons_to_avro_to_rust_avro_annotations(self):
        """ Test converting a jfrog-pipelines.json file to Rust """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        rust_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-rs-avro")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_rust(avro_path, rust_path, package_name="jfrog_pipelines", avro_annotation=True)
        assert subprocess.check_call(
            ['cargo', 'test'], cwd=rust_path, stdout=sys.stdout, stderr=sys.stderr, timeout=self.CARGO_TIMEOUT) == 0
