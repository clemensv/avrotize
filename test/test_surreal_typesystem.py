"""Full type-system range and adversarial coverage for SurrealQL converters.

This suite deliberately exercises every mapping branch in
avrotize/surrealtoavro.py and avrotize/avrotosurreal.py. It complements the
happy-path fixture tests in test_surrealtoavro.py and test_avrotosurreal.py.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fastavro.schema import parse_schema

from avrotize.avrotosurreal import convert_avro_to_surrealql
from avrotize.surrealtoavro import convert_surrealql_to_avro

ROOT = Path(__file__).resolve().parents[1]
NS = "example.surreal"


def normalize_surrealql(text: str) -> list[str]:
    """Normalize SurrealQL statements for structural assertions."""
    return sorted(" ".join(stmt.strip().rstrip(";").split()) for stmt in text.split(";") if stmt.strip())


class SurrealTypeSystemTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(dir=ROOT)
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    # -- helpers ---------------------------------------------------------------

    def surreal_to_avro(self, surrealql: str, namespace: str | None = NS):
        src = self.tmp / "schema.surql"
        src.write_text(surrealql, encoding="utf-8")
        out = self.tmp / "schema.avsc"
        convert_surrealql_to_avro(str(src), str(out), namespace=namespace)
        schema = json.loads(out.read_text(encoding="utf-8"))
        parse_schema(schema)  # every generated schema must be valid Avro
        records = schema if isinstance(schema, list) else [schema]
        by_name = {record["name"]: record for record in records}
        return schema, by_name

    def avro_to_surreal(self, schema, record_type: str | None = None) -> str:
        src = self.tmp / "schema.avsc"
        src.write_text(json.dumps(schema), encoding="utf-8")
        out = self.tmp / "schema.surql"
        convert_avro_to_surrealql(str(src), str(out), record_type=record_type)
        return out.read_text(encoding="utf-8")

    def field(self, by_name, record: str, name: str):
        return next(field for field in by_name[record]["fields"] if field["name"] == name)

    # -- SurrealQL -> Avro: full primitive/special range -----------------------

    def test_every_surrealql_type_maps_to_expected_avro(self):
        surrealql = """
        DEFINE TABLE type_range SCHEMAFULL;
        DEFINE FIELD f_bool ON TABLE type_range TYPE bool;
        DEFINE FIELD f_int ON TABLE type_range TYPE int;
        DEFINE FIELD f_float ON TABLE type_range TYPE float;
        DEFINE FIELD f_decimal ON TABLE type_range TYPE decimal;
        DEFINE FIELD f_number ON TABLE type_range TYPE number;
        DEFINE FIELD f_string ON TABLE type_range TYPE string;
        DEFINE FIELD f_datetime ON TABLE type_range TYPE datetime;
        DEFINE FIELD f_duration ON TABLE type_range TYPE duration;
        DEFINE FIELD f_uuid ON TABLE type_range TYPE uuid;
        DEFINE FIELD f_bytes ON TABLE type_range TYPE bytes;
        DEFINE FIELD f_any ON TABLE type_range TYPE any;
        DEFINE FIELD f_object ON TABLE type_range TYPE object;
        DEFINE FIELD f_array ON TABLE type_range TYPE array;
        DEFINE FIELD f_record ON TABLE type_range TYPE record<person>;
        DEFINE FIELD f_geometry ON TABLE type_range TYPE geometry;
        DEFINE FIELD f_set ON TABLE type_range TYPE set<string>;
        DEFINE FIELD f_option ON TABLE type_range TYPE option<int>;
        """
        _, by_name = self.surreal_to_avro(surrealql)
        expected = {
            "f_bool": "boolean",
            "f_int": "long",
            "f_float": "double",
            "f_decimal": {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 9},
            "f_number": "double",
            "f_string": "string",
            "f_datetime": {"type": "long", "logicalType": "timestamp-millis"},
            "f_duration": "string",
            "f_uuid": {"type": "string", "logicalType": "uuid"},
            "f_bytes": "bytes",
            "f_any": "string",
            "f_object": {"type": "record", "name": "type_range_f_object", "fields": []},
            "f_array": {"type": "array", "items": "string"},
            "f_geometry": "string",
            "f_set": {"type": "array", "items": "string"},
            "f_option": ["null", "long"],
        }
        for name, avro_type in expected.items():
            with self.subTest(field=name):
                self.assertEqual(self.field(by_name, "type_range", name)["type"], avro_type)
        self.assertEqual(self.field(by_name, "type_range", "f_option")["default"], None)
        record_ref = self.field(by_name, "type_range", "f_record")["type"]
        self.assertEqual(record_ref["type"], "string")
        self.assertEqual(record_ref["surrealType"], "record<person>")
        self.assertEqual(record_ref["doc"], "SurrealDB record references are represented as record id strings.")

    def test_comments_preserve_field_doc_but_assert_default_permissions_are_dropped(self):
        surrealql = """
        DEFINE TABLE account SCHEMAFULL PERMISSIONS NONE;
        DEFINE FIELD status ON TABLE account TYPE string DEFAULT 'new' ASSERT $value != NONE COMMENT 'status doc' PERMISSIONS FULL;
        """
        _, by_name = self.surreal_to_avro(surrealql)
        status = self.field(by_name, "account", "status")
        self.assertEqual(status, {"name": "status", "type": "string", "doc": "status doc"})

    # -- containers and table/field semantics ---------------------------------

    def test_arrays_sets_nested_arrays_and_option_containers(self):
        surrealql = """
        DEFINE TABLE containers SCHEMAFULL;
        DEFINE FIELD tags ON TABLE containers TYPE array<string>;
        DEFINE FIELD scores ON TABLE containers TYPE set<int>;
        DEFINE FIELD matrix ON TABLE containers TYPE array<array<int>>;
        DEFINE FIELD optional_tags ON TABLE containers TYPE option<array<string>>;
        DEFINE FIELD maybe_set ON TABLE containers TYPE option<set<uuid>>;
        """
        _, by_name = self.surreal_to_avro(surrealql)
        self.assertEqual(self.field(by_name, "containers", "tags")["type"], {"type": "array", "items": "string"})
        self.assertEqual(self.field(by_name, "containers", "scores")["type"], {"type": "array", "items": "long"})
        self.assertEqual(
            self.field(by_name, "containers", "matrix")["type"],
            {"type": "array", "items": {"type": "array", "items": "long"}},
        )
        self.assertEqual(self.field(by_name, "containers", "optional_tags")["type"], ["null", {"type": "array", "items": "string"}])
        self.assertEqual(
            self.field(by_name, "containers", "maybe_set")["type"],
            ["null", {"type": "array", "items": {"type": "string", "logicalType": "uuid"}}],
        )

    def test_schemafull_and_schemaless_tables_are_pinned(self):
        surrealql = """
        DEFINE TABLE strict SCHEMAFULL;
        DEFINE FIELD name ON TABLE strict TYPE string;
        DEFINE TABLE flex SCHEMALESS;
        DEFINE FIELD payload ON TABLE flex FLEXIBLE TYPE object;
        """
        _, by_name = self.surreal_to_avro(surrealql)
        self.assertNotIn("doc", by_name["strict"])
        self.assertEqual(by_name["flex"]["doc"], "SurrealDB SCHEMALESS table; Avro captures declared fields only.")
        self.assertEqual(self.field(by_name, "flex", "payload")["type"], {"type": "record", "name": "flex_payload", "fields": []})

    def test_dotted_paths_array_item_paths_and_nested_objects(self):
        surrealql = """
        DEFINE TABLE person SCHEMAFULL;
        DEFINE FIELD profile ON TABLE person TYPE object;
        DEFINE FIELD profile.address.city ON TABLE person TYPE string;
        DEFINE FIELD profile.address.zip ON TABLE person TYPE int;
        DEFINE FIELD phones ON TABLE person TYPE array<object>;
        DEFINE FIELD phones[*].kind ON TABLE person TYPE string;
        DEFINE FIELD phones[*].number ON TABLE person TYPE string;
        """
        _, by_name = self.surreal_to_avro(surrealql)
        profile = self.field(by_name, "person", "profile")["type"]
        self.assertEqual(profile["type"], "record")
        address = next(field for field in profile["fields"] if field["name"] == "address")["type"]
        self.assertEqual(address["name"], "person_profile_address")
        self.assertEqual({field["name"]: field["type"] for field in address["fields"]}, {"city": "string", "zip": "long"})
        phones = self.field(by_name, "person", "phones")["type"]
        self.assertEqual(phones["type"], "array")
        self.assertEqual({field["name"]: field["type"] for field in phones["items"]["fields"]}, {"kind": "string", "number": "string"})

    # -- recursion -------------------------------------------------------------

    def test_self_referential_record_link_and_nested_recursion_are_valid_avro(self):
        surrealql = """
        DEFINE TABLE node SCHEMAFULL;
        DEFINE FIELD id ON TABLE node TYPE record<node>;
        DEFINE FIELD parent ON TABLE node TYPE option<record<node>>;
        DEFINE FIELD metadata ON TABLE node TYPE object;
        DEFINE FIELD metadata.child.value ON TABLE node TYPE int;
        """
        schema, by_name = self.surreal_to_avro(surrealql)
        parse_schema(schema)
        self.assertEqual(self.field(by_name, "node", "id")["type"]["surrealType"], "record<node>")
        self.assertEqual(self.field(by_name, "node", "parent")["type"][1]["surrealType"], "record<node>")
        child = self.field(by_name, "node", "metadata")["type"]["fields"][0]["type"]
        self.assertEqual(child["name"], "node_metadata_child")
        self.assertEqual(child["fields"], [{"name": "value", "type": "long"}])

    # -- Avro -> SurrealQL: full primitive/special range -----------------------

    def test_every_avro_primitive_and_logical_type_maps_to_surrealql(self):
        schema = {
            "type": "record",
            "name": "AvroRange",
            "namespace": NS,
            "fields": [
                {"name": "f_null", "type": "null"},
                {"name": "f_bool", "type": "boolean"},
                {"name": "f_int", "type": "int"},
                {"name": "f_long", "type": "long"},
                {"name": "f_float", "type": "float"},
                {"name": "f_double", "type": "double"},
                {"name": "f_bytes", "type": "bytes"},
                {"name": "f_string", "type": "string"},
                {"name": "f_datetime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "f_uuid", "type": {"type": "string", "logicalType": "uuid"}},
                {"name": "f_decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "f_array", "type": {"type": "array", "items": "int"}},
                {"name": "f_nested_array", "type": {"type": "array", "items": {"type": "array", "items": "string"}}},
                {"name": "f_map", "type": {"type": "map", "values": "string"}},
                {"name": "f_nullable", "type": ["null", "string"], "default": None},
                {"name": "f_union", "type": ["string", "int"]},
                {"name": "f_enum", "type": {"type": "enum", "name": "Choice", "symbols": ["A", "B"]}},
                {"name": "f_fixed", "type": {"type": "fixed", "name": "Fixed4", "size": 4}},
            ],
        }
        text = self.avro_to_surreal(schema)
        expected = {
            "DEFINE TABLE AvroRange SCHEMAFULL;",
            "DEFINE FIELD f_null ON TABLE AvroRange TYPE option<string>;",
            "DEFINE FIELD f_bool ON TABLE AvroRange TYPE bool;",
            "DEFINE FIELD f_int ON TABLE AvroRange TYPE int;",
            "DEFINE FIELD f_long ON TABLE AvroRange TYPE int;",
            "DEFINE FIELD f_float ON TABLE AvroRange TYPE float;",
            "DEFINE FIELD f_double ON TABLE AvroRange TYPE number;",
            "DEFINE FIELD f_bytes ON TABLE AvroRange TYPE bytes;",
            "DEFINE FIELD f_string ON TABLE AvroRange TYPE string;",
            "DEFINE FIELD f_datetime ON TABLE AvroRange TYPE datetime;",
            "DEFINE FIELD f_uuid ON TABLE AvroRange TYPE uuid;",
            "DEFINE FIELD f_decimal ON TABLE AvroRange TYPE decimal;",
            "DEFINE FIELD f_array ON TABLE AvroRange TYPE array<int>;",
            "DEFINE FIELD f_nested_array ON TABLE AvroRange TYPE array<array<string>>;",
            "DEFINE FIELD f_map ON TABLE AvroRange TYPE object;",
            "DEFINE FIELD f_nullable ON TABLE AvroRange TYPE option<string>;",
            "DEFINE FIELD f_union ON TABLE AvroRange TYPE string;",
            "DEFINE FIELD f_enum ON TABLE AvroRange TYPE enum;",
            "DEFINE FIELD f_fixed ON TABLE AvroRange TYPE fixed;",
        }
        self.assertEqual(set(normalize_surrealql(text)), {stmt.rstrip(";") for stmt in expected})

    def test_avro_records_and_array_records_emit_dotted_paths(self):
        schema = {
            "type": "record",
            "name": "Person",
            "namespace": NS,
            "fields": [
                {
                    "name": "address",
                    "type": {"type": "record", "name": "Address", "fields": [{"name": "city", "type": "string"}]},
                },
                {
                    "name": "phones",
                    "type": {
                        "type": "array",
                        "items": {"type": "record", "name": "Phone", "fields": [{"name": "number", "type": "string"}]},
                    },
                },
                {
                    "name": "maybe_object",
                    "type": ["null", {"type": "record", "name": "MaybeObject", "fields": [{"name": "x", "type": "int"}]}],
                    "default": None,
                },
            ],
        }
        text = self.avro_to_surreal(schema)
        self.assertIn("DEFINE FIELD address ON TABLE Person TYPE object;", text)
        self.assertIn("DEFINE FIELD address.city ON TABLE Person TYPE string;", text)
        self.assertIn("DEFINE FIELD phones ON TABLE Person TYPE array<object>;", text)
        self.assertIn("DEFINE FIELD phones[*].number ON TABLE Person TYPE string;", text)
        self.assertIn("DEFINE FIELD maybe_object ON TABLE Person TYPE option<object>;", text)
        self.assertIn("DEFINE FIELD maybe_object.x ON TABLE Person TYPE int;", text)

    def test_record_type_filter_and_missing_record_type(self):
        schema = [
            {"type": "record", "name": "A", "namespace": NS, "fields": [{"name": "x", "type": "string"}]},
            {"type": "record", "name": "B", "namespace": NS, "fields": [{"name": "y", "type": "string"}]},
        ]
        text = self.avro_to_surreal(schema, record_type=f"{NS}.B")
        self.assertNotIn("DEFINE TABLE A", text)
        self.assertIn("DEFINE TABLE B SCHEMAFULL;", text)
        with self.assertRaises(ValueError):
            self.avro_to_surreal(schema, record_type="Missing")

    # -- round-trip fidelity ---------------------------------------------------

    def test_round_trip_pins_lossy_surrealql_to_avro_to_surrealql_mappings(self):
        surrealql = """
        DEFINE TABLE roundtrip SCHEMAFULL PERMISSIONS NONE;
        DEFINE FIELD f_float ON TABLE roundtrip TYPE float ASSERT $value > 0;
        DEFINE FIELD f_number ON TABLE roundtrip TYPE number;
        DEFINE FIELD f_duration ON TABLE roundtrip TYPE duration;
        DEFINE FIELD f_any ON TABLE roundtrip TYPE any;
        DEFINE FIELD f_set ON TABLE roundtrip TYPE set<string>;
        DEFINE FIELD f_geometry ON TABLE roundtrip TYPE geometry;
        DEFINE FIELD f_record ON TABLE roundtrip TYPE record<user>;
        DEFINE FIELD f_option ON TABLE roundtrip TYPE option<int>;
        """
        schema, _ = self.surreal_to_avro(surrealql)
        text = self.avro_to_surreal(schema)
        pinned_lossy = {
            "DEFINE TABLE roundtrip SCHEMAFULL",
            "DEFINE FIELD f_float ON TABLE roundtrip TYPE number",
            "DEFINE FIELD f_number ON TABLE roundtrip TYPE number",
            "DEFINE FIELD f_duration ON TABLE roundtrip TYPE string",
            "DEFINE FIELD f_any ON TABLE roundtrip TYPE string",
            "DEFINE FIELD f_set ON TABLE roundtrip TYPE array<string>",
            "DEFINE FIELD f_geometry ON TABLE roundtrip TYPE string",
            "DEFINE FIELD f_record ON TABLE roundtrip TYPE string",
            "DEFINE FIELD f_option ON TABLE roundtrip TYPE option<int>",
        }
        self.assertEqual(set(normalize_surrealql(text)), pinned_lossy)

    def test_round_trip_preserves_non_lossy_core_structure(self):
        surrealql = """
        DEFINE TABLE stable SCHEMAFULL;
        DEFINE FIELD name ON TABLE stable TYPE string;
        DEFINE FIELD active ON TABLE stable TYPE bool;
        DEFINE FIELD count ON TABLE stable TYPE int;
        DEFINE FIELD created ON TABLE stable TYPE datetime;
        DEFINE FIELD ext_id ON TABLE stable TYPE uuid;
        DEFINE FIELD price ON TABLE stable TYPE decimal;
        DEFINE FIELD payload ON TABLE stable TYPE bytes;
        DEFINE FIELD tags ON TABLE stable TYPE array<string>;
        DEFINE FIELD nested ON TABLE stable TYPE object;
        DEFINE FIELD nested.value ON TABLE stable TYPE int;
        """
        schema, _ = self.surreal_to_avro(surrealql)
        text = self.avro_to_surreal(schema)
        self.assertEqual(
            set(normalize_surrealql(text)),
            {
                "DEFINE TABLE stable SCHEMAFULL",
                "DEFINE FIELD name ON TABLE stable TYPE string",
                "DEFINE FIELD active ON TABLE stable TYPE bool",
                "DEFINE FIELD count ON TABLE stable TYPE int",
                "DEFINE FIELD created ON TABLE stable TYPE datetime",
                "DEFINE FIELD ext_id ON TABLE stable TYPE uuid",
                "DEFINE FIELD price ON TABLE stable TYPE decimal",
                "DEFINE FIELD payload ON TABLE stable TYPE bytes",
                "DEFINE FIELD tags ON TABLE stable TYPE array<string>",
                "DEFINE FIELD nested ON TABLE stable TYPE object",
                "DEFINE FIELD nested.value ON TABLE stable TYPE int",
            },
        )

    # -- adversarial / error paths --------------------------------------------

    def test_missing_surrealql_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            convert_surrealql_to_avro(str(self.tmp / "missing.surql"), str(self.tmp / "out.avsc"), namespace=NS)

    def test_missing_avro_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            convert_avro_to_surrealql(str(self.tmp / "missing.avsc"), str(self.tmp / "out.surql"))

    def test_empty_surrealql_raises_clean_valueerror(self):
        with self.assertRaisesRegex(ValueError, "No supported SurrealQL"):
            self.surreal_to_avro("")

    def test_malformed_define_statement_without_supported_table_or_field_raises(self):
        with self.assertRaisesRegex(ValueError, "No supported SurrealQL"):
            self.surreal_to_avro("DEFINE TABLE ; DEFINE FIELD ON TYPE;")

    def test_malformed_field_on_existing_table_is_ignored_gracefully(self):
        _, by_name = self.surreal_to_avro("DEFINE TABLE t SCHEMAFULL; DEFINE FIELD ON t TYPE int;")
        self.assertEqual(by_name["t"]["fields"], [])

    def test_duplicate_after_sanitization_is_rejected_or_deduplicated(self):
        schema, by_name = self.surreal_to_avro(
            "DEFINE TABLE t SCHEMAFULL; DEFINE FIELD `a-b` ON TABLE t TYPE string; DEFINE FIELD a_b ON TABLE t TYPE int;"
        )
        names = [field["name"] for field in by_name["t"]["fields"]]
        self.assertEqual(len(names), len(set(names)), schema)

    def test_unicode_identifiers_do_not_confuse_table_parser(self):
        _, by_name = self.surreal_to_avro(
            "DEFINE TABLE `café` SCHEMAFULL; DEFINE FIELD `naïve` ON TABLE `café` TYPE string COMMENT 'héllo';"
        )
        self.assertEqual(set(by_name), {"caf_"})
        self.assertEqual(self.field(by_name, "caf_", "na_ve")["doc"], "héllo")


if __name__ == "__main__":
    unittest.main()
