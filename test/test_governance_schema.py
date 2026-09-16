"""Tests for the standard-library JSON Schema subset validator."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools import governance_schema

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIR = REPO_ROOT / ".github" / "governance" / "schemas"


class TypeTests(unittest.TestCase):
    def test_type_mismatch_is_reported(self) -> None:
        self.assertTrue(governance_schema.validate("x", {"type": "integer"}))

    def test_boolean_is_not_an_integer(self) -> None:
        self.assertTrue(governance_schema.validate(True, {"type": "integer"}))

    def test_integer_is_a_number(self) -> None:
        self.assertEqual(governance_schema.validate(3, {"type": "number"}), [])

    def test_type_union_accepts_either(self) -> None:
        schema = {"type": ["string", "null"]}
        self.assertEqual(governance_schema.validate(None, schema), [])
        self.assertEqual(governance_schema.validate("a", schema), [])
        self.assertTrue(governance_schema.validate(5, schema))

    def test_null_type(self) -> None:
        self.assertEqual(governance_schema.validate(None, {"type": "null"}), [])


class ObjectTests(unittest.TestCase):
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["a", "b"],
        "properties": {
            "a": {"type": "string"},
            "b": {"type": "object", "required": ["c"], "properties": {"c": {"type": "integer"}}},
        },
    }

    def test_valid_object(self) -> None:
        self.assertEqual(governance_schema.validate({"a": "x", "b": {"c": 1}}, self.schema), [])

    def test_missing_required_property(self) -> None:
        errors = governance_schema.validate({"a": "x"}, self.schema)
        self.assertTrue(any("missing required property 'b'" in error for error in errors))

    def test_missing_nested_required_property(self) -> None:
        errors = governance_schema.validate({"a": "x", "b": {}}, self.schema)
        self.assertTrue(any("$.b: missing required property 'c'" in error for error in errors))

    def test_unexpected_property(self) -> None:
        errors = governance_schema.validate({"a": "x", "b": {"c": 1}, "z": 1}, self.schema)
        self.assertTrue(any("unexpected properties ['z']" in error for error in errors))

    def test_nested_type_error_path(self) -> None:
        errors = governance_schema.validate({"a": "x", "b": {"c": "no"}}, self.schema)
        self.assertTrue(any(error.startswith("$.b.c:") for error in errors))

    def test_additional_properties_schema(self) -> None:
        schema = {"type": "object", "properties": {}, "additionalProperties": {"type": "string"}}
        self.assertEqual(governance_schema.validate({"x": "ok"}, schema), [])
        self.assertTrue(governance_schema.validate({"x": 1}, schema))


class ArrayTests(unittest.TestCase):
    schema = {"type": "array", "minItems": 1, "maxItems": 2, "uniqueItems": True, "items": {"type": "string"}}

    def test_valid_array(self) -> None:
        self.assertEqual(governance_schema.validate(["a"], self.schema), [])

    def test_item_type_is_checked(self) -> None:
        errors = governance_schema.validate(["a", 2], self.schema)
        self.assertTrue(any("$[1]" in error for error in errors))

    def test_min_items(self) -> None:
        self.assertTrue(governance_schema.validate([], self.schema))

    def test_max_items(self) -> None:
        self.assertTrue(governance_schema.validate(["a", "b", "c"], self.schema))

    def test_unique_items(self) -> None:
        self.assertTrue(governance_schema.validate(["a", "a"], self.schema))


class ScalarConstraintTests(unittest.TestCase):
    def test_const(self) -> None:
        self.assertEqual(governance_schema.validate(1, {"const": 1}), [])
        self.assertTrue(governance_schema.validate(2, {"const": 1}))

    def test_enum(self) -> None:
        self.assertTrue(governance_schema.validate("z", {"enum": ["a", "b"]}))

    def test_pattern(self) -> None:
        schema = {"type": "string", "pattern": "^[0-9a-f]{4}$"}
        self.assertEqual(governance_schema.validate("abcd", schema), [])
        self.assertTrue(governance_schema.validate("abcz", schema))

    def test_length_bounds(self) -> None:
        schema = {"type": "string", "minLength": 2, "maxLength": 3}
        self.assertTrue(governance_schema.validate("a", schema))
        self.assertTrue(governance_schema.validate("abcd", schema))
        self.assertEqual(governance_schema.validate("ab", schema), [])

    def test_numeric_bounds(self) -> None:
        schema = {"type": "integer", "minimum": 1, "maximum": 3}
        self.assertTrue(governance_schema.validate(0, schema))
        self.assertTrue(governance_schema.validate(4, schema))
        self.assertEqual(governance_schema.validate(2, schema), [])

    def test_exclusive_bounds(self) -> None:
        schema = {"type": "number", "exclusiveMinimum": 0, "exclusiveMaximum": 1}
        self.assertTrue(governance_schema.validate(0, schema))
        self.assertEqual(governance_schema.validate(0.5, schema), [])


class CompositionTests(unittest.TestCase):
    def test_ref_resolution(self) -> None:
        schema = {
            "$defs": {"digest": {"type": "string", "pattern": "^[0-9a-f]{4}$"}},
            "type": "object",
            "properties": {"d": {"$ref": "#/$defs/digest"}},
        }
        self.assertEqual(governance_schema.validate({"d": "abcd"}, schema), [])
        self.assertTrue(governance_schema.validate({"d": "zzzz"}, schema))

    def test_unresolvable_ref_raises(self) -> None:
        with self.assertRaises(governance_schema.SchemaError):
            governance_schema.validate({"d": "x"}, {"type": "object", "properties": {"d": {"$ref": "#/$defs/nope"}}})

    def test_any_of(self) -> None:
        schema = {"anyOf": [{"type": "string"}, {"type": "integer"}]}
        self.assertEqual(governance_schema.validate(1, schema), [])
        self.assertTrue(governance_schema.validate([], schema))

    def test_one_of_requires_exactly_one(self) -> None:
        schema = {"oneOf": [{"type": "integer"}, {"type": "number"}]}
        self.assertTrue(governance_schema.validate(1, schema))

    def test_all_of(self) -> None:
        schema = {"allOf": [{"type": "string"}, {"minLength": 2}]}
        self.assertEqual(governance_schema.validate("ab", schema), [])
        self.assertTrue(governance_schema.validate("a", schema))


class SchemaHygieneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory(dir=str(Path(__file__).resolve().parent))
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)

    def test_unsupported_keyword_is_rejected(self) -> None:
        path = self.root / "schema.json"
        path.write_text(json.dumps({"type": "object", "patternProperties": {"^a": {}}}), encoding="utf-8")
        with self.assertRaises(governance_schema.SchemaError):
            governance_schema.load_schema(path)

    def test_corrupt_schema_is_rejected(self) -> None:
        path = self.root / "schema.json"
        path.write_text("{ nope", encoding="utf-8")
        with self.assertRaises(governance_schema.SchemaError):
            governance_schema.load_schema(path)

    def test_all_checked_in_schemas_use_the_supported_subset(self) -> None:
        for path in sorted(SCHEMA_DIR.glob("*.json")):
            with self.subTest(schema=path.name):
                governance_schema.load_schema(path)

    def test_validate_or_raise_reports_every_error(self) -> None:
        path = self.root / "schema.json"
        path.write_text(
            json.dumps({"type": "object", "required": ["a", "b"], "properties": {}}),
            encoding="utf-8",
        )
        with self.assertRaises(governance_schema.SchemaError) as context:
            governance_schema.validate_or_raise({}, path, "record")
        self.assertIn("'a'", str(context.exception))
        self.assertIn("'b'", str(context.exception))


if __name__ == "__main__":
    unittest.main()
