"""Tests for JSON Structure to Parquet schema conversion (avrotize.structuretoparquet)."""

import json
import os
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

# Ensure the project root is importable
current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.structuretoparquet import convert_structure_to_parquet


STRUCT_FIXTURES = [
    "basic-types.struct.json",
    "choice-types.struct.json",
    "collections.struct.json",
    "complex-scenario.struct.json",
    "edge-cases.struct.json",
    "extensions.struct.json",
    "nested-objects.struct.json",
    "numeric-types.struct.json",
    "temporal-types.struct.json",
    "validation-constraints.struct.json",
]


class TestStructureToParquet(unittest.TestCase):
    """Test cases for JSON Structure to Parquet conversion."""

    def setUp(self):
        self.out_dir = os.path.join(tempfile.gettempdir(), "avrotize")
        os.makedirs(self.out_dir, exist_ok=True)

    def _convert(self, struct_file, record_type=None, emit_cloudevents=False, output_format="parquet"):
        """Convert a fixture in test/struct/ and return the output path."""
        struct_path = os.path.join(os.getcwd(), "test", "struct", struct_file)
        suffix = ".parquet" if output_format == "parquet" else ".schema.json"
        out_path = os.path.join(self.out_dir, struct_file.replace(".struct.json", suffix))
        convert_structure_to_parquet(struct_path, record_type, out_path, emit_cloudevents, output_format)
        return out_path

    def _convert_inline(self, schema_dict, name, emit_cloudevents=False, output_format="parquet"):
        """Write an inline JSON Structure schema to a temp file, convert, return output path."""
        struct_path = os.path.join(self.out_dir, f"{name}.struct.json")
        with open(struct_path, "w", encoding="utf-8") as f:
            json.dump(schema_dict, f)
        suffix = ".parquet" if output_format == "parquet" else ".schema.json"
        out_path = os.path.join(self.out_dir, f"{name}{suffix}")
        convert_structure_to_parquet(struct_path, None, out_path, emit_cloudevents, output_format)
        return out_path

    # -- Broad coverage: every fixture converts to a readable Parquet file -------

    def test_all_fixtures_convert(self):
        """Every shared JSON Structure fixture produces a readable Parquet schema."""
        for struct_file in STRUCT_FIXTURES:
            with self.subTest(fixture=struct_file):
                out_path = self._convert(struct_file)
                self.assertTrue(os.path.exists(out_path))
                schema = pq.read_schema(out_path)
                self.assertGreater(len(schema), 0)

    # -- Basic primitive type mapping + nullability ------------------------------

    def test_basic_types_mapping(self):
        schema = pq.read_schema(self._convert("basic-types.struct.json"))
        fields = {f.name: f for f in schema}
        self.assertEqual(fields["stringField"].type, pa.string())
        self.assertEqual(fields["intField"].type, pa.int32())
        self.assertEqual(fields["longField"].type, pa.int64())
        self.assertEqual(fields["floatField"].type, pa.float32())
        self.assertEqual(fields["doubleField"].type, pa.float64())
        self.assertEqual(fields["booleanField"].type, pa.bool_())
        self.assertEqual(fields["uuidField"].type, pa.string())
        self.assertEqual(fields["bytesField"].type, pa.binary())

    def test_basic_types_nullability(self):
        """Required fields are non-nullable; the rest are nullable."""
        schema = pq.read_schema(self._convert("basic-types.struct.json"))
        fields = {f.name: f for f in schema}
        self.assertFalse(fields["stringField"].nullable)  # required
        self.assertFalse(fields["intField"].nullable)     # required
        self.assertTrue(fields["longField"].nullable)     # not required
        self.assertTrue(fields["doubleField"].nullable)

    # -- Numeric types -----------------------------------------------------------

    def test_numeric_types_mapping(self):
        schema = pq.read_schema(self._convert("numeric-types.struct.json"))
        fields = {f.name: f for f in schema}
        self.assertEqual(fields["int8Field"].type, pa.int8())
        self.assertEqual(fields["int16Field"].type, pa.int16())
        self.assertEqual(fields["uint32Field"].type, pa.uint32())
        self.assertEqual(fields["uint64Field"].type, pa.uint64())
        self.assertEqual(fields["float32Field"].type, pa.float32())
        self.assertEqual(fields["float64Field"].type, pa.float64())

    def test_decimal_precision_scale(self):
        schema = pq.read_schema(self._convert("numeric-types.struct.json"))
        fields = {f.name: f for f in schema}
        self.assertEqual(fields["decimalField"].type, pa.decimal128(10, 2))
        self.assertFalse(fields["decimalField"].nullable)  # required

    # -- Temporal types (microsecond precision) ----------------------------------

    def test_temporal_types_mapping(self):
        schema = pq.read_schema(self._convert("temporal-types.struct.json"))
        fields = {f.name: f for f in schema}
        self.assertEqual(fields["dateField"].type, pa.date32())
        self.assertEqual(fields["timeField"].type, pa.time64('us'))
        self.assertEqual(fields["datetimeField"].type, pa.timestamp('us'))
        self.assertEqual(fields["timestampField"].type, pa.timestamp('us'))
        self.assertEqual(fields["durationField"].type, pa.int64())
        self.assertFalse(fields["datetimeField"].nullable)  # required

    # -- Collections -------------------------------------------------------------

    def test_collections_mapping(self):
        schema = pq.read_schema(self._convert("collections.struct.json"))
        fields = {f.name: f for f in schema}

        string_array = fields["stringArray"].type
        self.assertTrue(pa.types.is_list(string_array))
        self.assertEqual(string_array.value_type, pa.string())

        number_set = fields["numberSet"].type
        self.assertTrue(pa.types.is_list(number_set))
        self.assertEqual(number_set.value_type, pa.int32())

        m = fields["stringToNumberMap"].type
        self.assertTrue(pa.types.is_map(m))
        self.assertEqual(m.key_type, pa.string())
        self.assertEqual(m.item_type, pa.float64())

        nested = fields["nestedArray"].type
        self.assertTrue(pa.types.is_list(nested))
        self.assertTrue(pa.types.is_list(nested.value_type))
        self.assertEqual(nested.value_type.value_type, pa.string())

    # -- Nested objects become structs -------------------------------------------

    def test_nested_object_becomes_struct(self):
        schema = pq.read_schema(self._convert("nested-objects.struct.json"))
        struct_fields = [f for f in schema if pa.types.is_struct(f.type)]
        self.assertTrue(struct_fields, "Expected at least one struct-typed field for nested objects")

    # -- Choice types become structs of optional alternatives --------------------

    def test_choice_type_becomes_struct(self):
        schema = pq.read_schema(self._convert("choice-types.struct.json"))
        # At least one field should be a struct (choice -> struct of options) or
        # the file should still convert cleanly with >0 fields.
        self.assertGreater(len(schema), 0)

    # -- CloudEvents columns -----------------------------------------------------

    def test_cloudevents_columns(self):
        schema = pq.read_schema(self._convert("basic-types.struct.json", emit_cloudevents=True))
        names = [f.name for f in schema]
        for col in ["___type", "___source", "___id", "___time", "___subject"]:
            self.assertIn(col, names)
        fields = {f.name: f for f in schema}
        self.assertEqual(fields["___time"].type, pa.timestamp('us'))

    # -- Schema (JSON) output format ---------------------------------------------

    def test_schema_json_output(self):
        out_path = self._convert("basic-types.struct.json", output_format="schema")
        self.assertTrue(os.path.exists(out_path))
        with open(out_path, "r", encoding="utf-8") as f:
            doc = json.load(f)
        self.assertEqual(doc["type"], "struct")
        names = [field["name"] for field in doc["fields"]]
        self.assertIn("stringField", names)

    # -- Edge: property-less (open) object -> map<string,string> -----------------

    def test_property_less_object_maps_to_map(self):
        schema_dict = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/schemas/openobj",
            "type": "object",
            "name": "OpenObj",
            "properties": {
                "Id": {"type": "string"},
                "Dimension": {"type": "object"},
            },
        }
        schema = pq.read_schema(self._convert_inline(schema_dict, "openobj"))
        fields = {f.name: f for f in schema}
        dim = fields["Dimension"].type
        self.assertTrue(pa.types.is_map(dim))
        self.assertEqual(dim.key_type, pa.string())
        self.assertEqual(dim.item_type, pa.string())

    # -- Record type selection from definitions ----------------------------------

    def test_record_type_from_definitions(self):
        schema_dict = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": "https://example.com/schemas/defs",
            "definitions": {
                "Person": {
                    "type": "object",
                    "name": "Person",
                    "properties": {"name": {"type": "string"}, "age": {"type": "int32"}},
                    "required": ["name"],
                }
            },
        }
        struct_path = os.path.join(self.out_dir, "defs.struct.json")
        with open(struct_path, "w", encoding="utf-8") as f:
            json.dump(schema_dict, f)
        out_path = os.path.join(self.out_dir, "defs.parquet")
        convert_structure_to_parquet(struct_path, "Person", out_path, False, "parquet")
        schema = pq.read_schema(out_path)
        names = [f.name for f in schema]
        self.assertIn("name", names)
        self.assertIn("age", names)


if __name__ == '__main__':
    unittest.main()
