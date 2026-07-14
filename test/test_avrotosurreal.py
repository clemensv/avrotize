"""Tests for Avrotize Schema to SurrealQL schema conversion."""

import json
import unittest
from pathlib import Path

from avrotize.avrotosurreal import AvroToSurrealQLConverter, convert_avro_to_surrealql
from avrotize.surrealtoavro import SurrealToAvroConverter


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "test" / "surrealql" / "person.surql"


def normalize_surrealql(text: str) -> list[str]:
    """Normalize SurrealQL statements for structural assertions."""
    return sorted(" ".join(stmt.strip().rstrip(";").split()).lower() for stmt in text.split(";") if stmt.strip())


class TestAvroToSurrealQL(unittest.TestCase):
    """Validate Avro to SurrealQL mappings."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.avro_schema = SurrealToAvroConverter(namespace="example.surreal").convert_schema(FIXTURE.read_text(encoding="utf-8"))
        cls.surrealql = AvroToSurrealQLConverter().convert_schema(cls.avro_schema)

    def test_emits_define_table_statements(self) -> None:
        self.assertIn("DEFINE TABLE person SCHEMAFULL;", self.surrealql)
        self.assertIn("DEFINE TABLE order SCHEMAFULL;", self.surrealql)

    def test_emits_scalar_field_statements(self) -> None:
        self.assertIn("DEFINE FIELD name ON TABLE person TYPE string;", self.surrealql)
        self.assertIn("DEFINE FIELD score ON TABLE person TYPE number;", self.surrealql)
        self.assertIn("DEFINE FIELD active ON TABLE person TYPE bool;", self.surrealql)

    def test_emits_logical_type_field_statements(self) -> None:
        self.assertIn("DEFINE FIELD created_at ON TABLE person TYPE datetime;", self.surrealql)
        self.assertIn("DEFINE FIELD external_id ON TABLE person TYPE uuid;", self.surrealql)
        self.assertIn("DEFINE FIELD price ON TABLE person TYPE decimal;", self.surrealql)

    def test_nullable_union_emits_option(self) -> None:
        self.assertIn("DEFINE FIELD age ON TABLE person TYPE option<int>;", self.surrealql)

    def test_nested_records_emit_dotted_paths(self) -> None:
        self.assertIn("DEFINE FIELD address ON TABLE person TYPE object;", self.surrealql)
        self.assertIn("DEFINE FIELD address.city ON TABLE person TYPE string;", self.surrealql)
        self.assertIn("DEFINE FIELD address.zip ON TABLE person TYPE int;", self.surrealql)

    def test_array_record_items_emit_bracket_paths(self) -> None:
        self.assertIn("DEFINE FIELD phones ON TABLE person TYPE array<object>;", self.surrealql)
        self.assertIn("DEFINE FIELD phones[*].kind ON TABLE person TYPE string;", self.surrealql)
        self.assertIn("DEFINE FIELD phones[*].number ON TABLE person TYPE string;", self.surrealql)

    def test_record_type_filter(self) -> None:
        text = AvroToSurrealQLConverter().convert_schema(self.avro_schema, record_type="person")
        self.assertIn("DEFINE TABLE person SCHEMAFULL;", text)
        self.assertNotIn("DEFINE TABLE order SCHEMAFULL;", text)

    def test_convert_function_writes_surrealql_file(self) -> None:
        out_dir = ROOT / "test" / "surrealql" / "generated"
        out_dir.mkdir(exist_ok=True)
        avro_path = out_dir / "person.avsc"
        out_path = out_dir / "person.surql"
        try:
            avro_path.write_text(json.dumps(self.avro_schema), encoding="utf-8")
            convert_avro_to_surrealql(str(avro_path), str(out_path), record_type="person")
            self.assertIn("DEFINE TABLE person SCHEMAFULL;", out_path.read_text(encoding="utf-8"))
        finally:
            for path in (avro_path, out_path):
                if path.exists():
                    path.unlink()
            if out_dir.exists() and not any(out_dir.iterdir()):
                out_dir.rmdir()

    def test_round_trip_preserves_table_and_field_structure(self) -> None:
        round_trip = SurrealToAvroConverter(namespace="example.surreal").convert_schema(self.surrealql)
        second_surrealql = AvroToSurrealQLConverter().convert_schema(round_trip)
        self.assertEqual(normalize_surrealql(self.surrealql), normalize_surrealql(second_surrealql))


if __name__ == "__main__":
    unittest.main()
