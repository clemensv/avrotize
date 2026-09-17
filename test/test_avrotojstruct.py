import json
import os
import tempfile
import unittest
import sys
from os import path, getcwd
from pathlib import Path
from avrotize.avrotojstruct import convert_avro_to_json_structure
from jsoncomparison import NO_DIFF, Compare

# Add the JSON Structure validator to the path
validator_path = Path(__file__).parent.parent / "tools" / "primer-and-samples" / "samples" / "py"
if validator_path.exists():
    sys.path.insert(0, str(validator_path))

try:
    from json_structure_schema_validator import JSONStructureSchemaCoreValidator
    VALIDATOR_AVAILABLE = True
except ImportError:
    print("Warning: JSON Structure validator not available. Schema validation will be skipped.")
    VALIDATOR_AVAILABLE = False
    JSONStructureSchemaCoreValidator = None


class TestAvroToJsonStructure(unittest.TestCase):
    """Tests converting Avro schemas to JSON Structure (*.struct.json)"""

    def _assert_refs_wrapped(self, node, parent_key=None, in_type_list=False, path="#") -> None:
        """Assert every ``$ref`` is a JSON Structure type reference, i.e. it is the
        value of a ``type`` keyword or a member of a ``type`` union array. A bare
        ``$ref`` used directly as a property/items/values/choice value is a Core
        spec violation (spec 3.3.6 / 3.7.1) that older avrotize builds emitted."""
        if isinstance(node, dict):
            if "$ref" in node and not (parent_key == "type" or in_type_list):
                self.fail(
                    f"Bare $ref not wrapped under `type` at {path}: {node!r}. "
                    f"Expected {{'type': {{'$ref': ...}}}}."
                )
            for key, value in node.items():
                if key == "type" and isinstance(value, list):
                    for i, member in enumerate(value):
                        self._assert_refs_wrapped(
                            member, parent_key=None, in_type_list=True,
                            path=f"{path}/type[{i}]",
                        )
                else:
                    self._assert_refs_wrapped(
                        value, parent_key=key, in_type_list=False,
                        path=f"{path}/{key}",
                    )
        elif isinstance(node, list):
            for i, member in enumerate(node):
                self._assert_refs_wrapped(
                    member, parent_key=parent_key, in_type_list=in_type_list,
                    path=f"{path}[{i}]",
                )

    def _validate_json(self, json_file_path: str) -> None:
        """Ensure generated file contains valid JSON and conforms to JSON Structure Core."""
        with open(json_file_path, "r", encoding="utf-8") as file:
            source_text = file.read()
            try:
                doc = json.loads(source_text)
            except json.JSONDecodeError as e:
                self.fail(f"Invalid JSON in {json_file_path}: {e}")
                return # Should not be reached if self.fail works as expected

        # Structural guard: type references MUST be wrapped under `type`.
        self._assert_refs_wrapped(doc)

        if VALIDATOR_AVAILABLE and JSONStructureSchemaCoreValidator is not None:
            validator = JSONStructureSchemaCoreValidator(allow_dollar=False, allow_import=True) # allow_import might be needed depending on test cases
            errors = validator.validate(doc, source_text)
            if errors:
                error_messages = "\n".join(errors)
                self.fail(f"JSON Structure validation failed for {json_file_path}:\n{error_messages}")
        else:
            print(f"Warning: Skipping JSON Structure validation for {json_file_path} (validator not available)")

    def _convert_and_validate(
        self, avro_file: str, struct_file: str | None = None, naming_mode: str = "default"
    ) -> None:
        """
        Helper that converts an Avro schema to JSON Structure, validates the
        produced JSON, and compares it with a reference file if present.
        """
        cwd = getcwd()
        if struct_file is None:
            struct_file = avro_file.replace(".avsc", ".struct.json")

        avro_full_path = path.join(cwd, "test", "avsc", avro_file)
        struct_full_path = path.join(tempfile.gettempdir(), "avrotize", struct_file)

        # Ensure destination directory exists
        os.makedirs(os.path.dirname(struct_full_path), exist_ok=True)

        # Perform conversion
        convert_avro_to_json_structure(avro_full_path, struct_full_path, naming_mode=naming_mode)

        # Validate JSON is syntactically correct
        self._validate_json(struct_full_path)

        # If a reference file exists, compare generated output against it
        ref_path = avro_full_path.replace(".avsc", "-ref.struct.json")
        if os.path.exists(ref_path):
            with open(ref_path, "r", encoding="utf-8") as ref_file:
                expected = json.load(ref_file)
            with open(struct_full_path, "r", encoding="utf-8") as gen_file:
                actual = json.load(gen_file)

            diff = Compare().check(actual, expected)
            self.assertEqual(diff, NO_DIFF, f"Differences found for {avro_file}")

    # One test per Avro fixture (mirrors test_avrotojsons.py)
    def test_address_nn(self):
        self._convert_and_validate("address-nn.avsc", "address-nn.struct.json")

    def test_address(self):
        self._convert_and_validate("address.avsc", "address.struct.json")
        structure_path = path.join(tempfile.gettempdir(), "avrotize", "address.struct.json")
        with open(structure_path, "r", encoding="utf-8") as structure_file:
            structure = json.load(structure_file)
        self.assertEqual(structure["$schema"], "https://json-structure.org/meta/extended/v0/#")
        self.assertEqual(structure["$uses"], ["JSONStructureValidation"])

    def test_complexunion(self):
        self._convert_and_validate("complexunion.avsc", "complexunion.struct.json")

    def test_enumfield_ordinals(self):
        self._convert_and_validate("enumfield-ordinals.avsc", "enumfield-ordinals.struct.json")

    def test_enumfield(self):
        self._convert_and_validate("enumfield.avsc", "enumfield.struct.json")

    def test_feeditem(self):
        self._convert_and_validate("feeditem.avsc", "feeditem.struct.json")

    def test_fileblob(self):
        self._convert_and_validate("fileblob.avsc", "fileblob.struct.json")

    def test_northwind(self):
        self._convert_and_validate("northwind.avsc", "northwind.struct.json")
        avro_path = path.join(getcwd(), "test", "avsc", "northwind.avsc")
        structure_path = path.join(tempfile.gettempdir(), "avrotize", "northwind.struct.json")
        with open(avro_path, "r", encoding="utf-8") as avro_file:
            source_types = {schema["name"] for schema in json.load(avro_file)}
        with open(structure_path, "r", encoding="utf-8") as structure_file:
            structure = json.load(structure_file)

        self.assertEqual(structure.get("type"), "null")
        self.assertNotIn("$root", structure)
        self.assertEqual(structure["$schema"], "https://json-structure.org/meta/core/v0/#")
        self.assertNotIn("$uses", structure)
        self.assertEqual(set(structure["definitions"]["Northwind"]), source_types)

    def test_dotted_names_preserve_reference_resolution(self):
        self._convert_and_validate("rust-named-reference-resolution.avsc")
        structure_path = path.join(
            tempfile.gettempdir(), "avrotize", "rust-named-reference-resolution.struct.json"
        )
        with open(structure_path, "r", encoding="utf-8") as structure_file:
            structure = json.load(structure_file)

        self.assertIn("Item", structure["definitions"]["Left"])
        holder = structure["definitions"]["Right"]["Holder"]
        self.assertEqual(holder["properties"]["local"]["type"]["$ref"], "#/definitions/Right/Item")
        self.assertEqual(holder["properties"]["foreign"]["type"]["$ref"], "#/definitions/Left/Item")

    def test_reserved_definition_name_uses_avro_altname(self):
        source_path = path.join(getcwd(), "test", "jsons", "discriminated-union-simple-ref.avsc")
        structure_path = path.join(tempfile.gettempdir(), "avrotize", "reserved-name.struct.json")
        convert_avro_to_json_structure(source_path, structure_path)
        with open(structure_path, "r", encoding="utf-8") as structure_file:
            structure = json.load(structure_file)

        self.assertEqual(structure["$schema"], "https://json-structure.org/meta/extended/v0/#")
        self.assertEqual(
            structure["$uses"],
            ["JSONStructureAlternateNames", "JSONStructureValidation"],
        )
        escaped = structure["definitions"]["com"]["test"]["example"]["Shape_types"]["type_"]
        self.assertEqual(escaped["name"], "type_")
        self.assertEqual(escaped["altnames"], {"avro": "type"})
        triangle = structure["definitions"]["com"]["test"]["example"]["Shape_types"]["Triangle"]
        self.assertEqual(
            triangle["properties"]["type"]["type"]["$ref"],
            "#/definitions/com/test/example/Shape_types/type_",
        )

    def test_named_type_namespace_collision_preserves_nested_definition(self):
        source_path = path.join(
            getcwd(), "test", "db", "postgres-test_postgres_heterogeneous_json-ref.avsc"
        )
        structure_path = path.join(tempfile.gettempdir(), "avrotize", "nested-name.struct.json")
        convert_avro_to_json_structure(source_path, structure_path)
        with open(structure_path, "r", encoding="utf-8") as structure_file:
            structure = json.load(structure_file)

        mixed = structure["definitions"]["com"]["example"]["mixed"]
        self.assertIn("child", mixed["mixed_json_"]["dataTypes"])
        child_ref = mixed["mixed_jsonTypes"]["data"]["properties"]["child"]["type"]["$ref"]
        self.assertEqual(
            child_ref,
            "#/definitions/com/example/mixed/mixed_json_/dataTypes/child",
        )

    def test_primitiveunion(self):
        self._convert_and_validate("primitiveunion.avsc", "primitiveunion.struct.json")

    def test_telemetry(self):
        self._convert_and_validate("telemetry.avsc", "telemetry.struct.json")

    def test_twotypeunion(self):
        self._convert_and_validate("twotypeunion.avsc", "twotypeunion.struct.json")

    def test_typemapunion(self):
        self._convert_and_validate("typemapunion.avsc", "typemapunion.struct.json")

    def test_typemapunion2(self):
        self._convert_and_validate("typemapunion2.avsc", "typemapunion2.struct.json")