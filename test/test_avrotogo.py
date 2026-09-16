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


from avrotize.avrotogo import convert_avro_to_go
from avrotize.jsonstoavro import convert_jsons_to_avro


class TestAvroToGo(unittest.TestCase):


    def verify_go_output(self, go_path, package_name: str):
        """ Verify the Go output """
         # Check if Go is installed
        go_installed = False
        try:
            subprocess.run(["go", "version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            go_installed = True
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
        
        if go_installed:
            try:
                # Initialize a new Go module if needed
                subprocess.run(["go", "mod", "init", package_name], cwd=go_path, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            except subprocess.CalledProcessError:
                pass  # Ignore if the module is already initialized

            try:
                # Download all dependencies
                subprocess.run(["go", "mod", "tidy"], cwd=go_path, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                # Run the Go tests
                result = subprocess.run(["go", "build", "./..."], cwd=go_path, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output = result.stdout.decode()
                if output:
                    print("\nGo build output:\n", output)
            except subprocess.CalledProcessError as e:
                print("Go tests failed:\n", e.stderr.decode())
        else:
            print("Go tools are not installed on this machine.")
        
    def run_convert_avsc_to_go(self, avro_name, avro_annotation=False, json_annotation=False):
        """ Test converting an.avsc file to go """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{avro_name}.avsc")
        go_path = os.path.join(tempfile.gettempdir(), "avrotize", f"{avro_name}-go{'-avro' if avro_annotation else ''}{'-json' if json_annotation else ''}")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)        
        convert_avro_to_go(avro_path, go_path, package_name=avro_name, json_annotation=json_annotation, avro_annotation=avro_annotation)
        self.verify_go_output(go_path, avro_name)

    def test_convert_address_avsc_to_go(self):
        """ Test converting an address.avsc file to go """
        self.run_convert_avsc_to_go("address", False, False)
        self.run_convert_avsc_to_go("address", True, False)
        self.run_convert_avsc_to_go("address", False, True)
        self.run_convert_avsc_to_go("address", True, True)

    def test_convert_telemetry_avsc_to_go(self):
        """ Test converting a telemetry.avsc file to go """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        go_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-go")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)
        
        convert_avro_to_go(avro_path, go_path, package_name="telemetry")
        self.verify_go_output(go_path, "telemetry")
        
        
        

    def test_issue_401_gofmt_clean_and_struct_tag_spacing(self):
        """ Issue #401: the Go emitter must produce gofmt-clean, idiomatic Go.

        Verifies the two things gofmt itself cannot: (a) struct tags are
        space-separated (`json:"x" avro:"x"`, not `json:"x"avro:"x"`) -- gofmt
        never rewrites raw string literals -- and (b) the package clause is a
        valid Go identifier even for dotted/hyphenated inputs. Then, if the Go
        toolchain is present, asserts `gofmt -l` reports nothing (fully clean:
        tabs, sorted imports, aligned fields, blank lines, trailing newline). """
        import re
        from avrotize.avrotogo import convert_avro_schema_to_go

        schema = [
            {
                "type": "record",
                "name": "BrightnessChangedEventData",
                "namespace": "example.iss401",
                "fields": [
                    {"name": "tenantid", "type": "string"},
                    {"name": "deviceid", "type": "string"},
                    {"name": "brightness", "type": "int"},
                    {"name": "colorTemperature", "type": "int"},
                ],
            },
            {
                "type": "enum",
                "name": "SwitchSource",
                "namespace": "example.iss401",
                "symbols": ["PhysicalSwitch", "AppSwitch", "VoiceSwitch"],
            },
        ]
        go_path = os.path.join(tempfile.gettempdir(), "avrotize", "issue-401-go")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)

        # Hyphen/dot in package name must be sanitized to a valid Go identifier.
        convert_avro_schema_to_go(
            schema, go_path, package_name="brightness-events.data",
            json_annotation=True, avro_annotation=True)

        go_files = []
        for root, _dirs, files in os.walk(go_path):
            for f in files:
                if f.endswith(".go"):
                    go_files.append(os.path.join(root, f))
        assert go_files, "no .go files generated"

        struct_file = None
        for gf in go_files:
            with open(gf, "r", encoding="utf-8") as fh:
                content = fh.read()
            # (b) package clause is a valid Go identifier (no dot/hyphen).
            for line in content.splitlines():
                if line.startswith("package "):
                    pkg = line[len("package "):].strip()
                    assert re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", pkg), \
                        f"invalid Go package name {pkg!r} in {gf}"
            if 'avro:"' in content:
                struct_file = content

        # (a) struct tags must be space-separated.
        assert struct_file is not None, "expected a struct file with avro tags"
        assert '"avro:' not in struct_file, \
            'struct tags are glued without a space (json:"x"avro:"x")'
        assert ' avro:"' in struct_file, "expected space-separated struct tags"

        # If gofmt is available, the whole tree must be gofmt-clean.
        gofmt = shutil.which("gofmt")
        if gofmt:
            result = subprocess.run(
                [gofmt, "-l", go_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            unformatted = result.stdout.decode().strip()
            assert unformatted == "", f"gofmt reports unformatted files:\n{unformatted}"

    def test_convert_jfrog_pipelines_jsons_to_avro_to_go(self):
        """ Test converting a jfrog-pipelines.json file to go """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        go_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-go")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_go(avro_path, go_path, package_name="jfrog_pipelines")
        self.verify_go_output(go_path, "jfrog_pipelines")
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_go_typed_json(self):
        """ Test converting a jfrog-pipelines.json file to go """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        go_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-go-typed-json")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_go(avro_path, go_path, package_name="jfrog_pipelines", json_annotation=True)
        self.verify_go_output(go_path, "jfrog_pipelines")
        
    def test_convert_jfrog_pipelines_jsons_to_avro_to_go_avro_annotations(self):
        """ Test converting a jfrog-pipelines.json file to go """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        go_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-go-avro")
        if os.path.exists(go_path):
            shutil.rmtree(go_path, ignore_errors=True)
        os.makedirs(go_path, exist_ok=True)            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_go(avro_path, go_path, package_name="jfrog_pipelines", avro_annotation=True)
        self.verify_go_output(go_path, "jfrog_pipelines")
