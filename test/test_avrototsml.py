"""Tests for Avro to TMSL conversion."""

import json
import os
import sys
import tempfile
import unittest

# Ensure the project root is in the system path for imports
current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

from avrotize.avrototsml import convert_avro_to_tmsl


class TestAvroToTmsl(unittest.TestCase):
    """Test cases for Avro to TMSL conversion."""

    def _load_reference(self, file_name: str):
        cwd = os.getcwd()
        ref_path = os.path.join(cwd, "test", "tsml", file_name)
        with open(ref_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def test_convert_address_avsc_to_tmsl(self):
        """Test converting address.avsc to a TMSL schema."""
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        tmsl_path = os.path.join(tempfile.gettempdir(), "avrotize", "address.tmsl.json")
        os.makedirs(os.path.dirname(tmsl_path), exist_ok=True)

        convert_avro_to_tmsl(avro_path, None, tmsl_path)

        self.assertTrue(os.path.exists(tmsl_path))
        with open(tmsl_path, "r", encoding="utf-8") as f:
            tmsl = json.load(f)

        self.assertIn("createOrReplace", tmsl)
        expected = self._load_reference("address-ref.tmsl.json")
        self.assertEqual(expected, tmsl)
        database = tmsl["createOrReplace"]["database"]
        self.assertIn("model", database)
        self.assertIn("tables", database["model"])
        self.assertEqual(len(database["model"]["tables"]), 1)

        table = database["model"]["tables"][0]
        columns = table.get("columns", [])
        column_names = {column["name"] for column in columns}
        self.assertIn("streetAddress", column_names)
        self.assertIn("postOfficeBox", column_names)

        nullable_column = next(column for column in columns if column["name"] == "postOfficeBox")
        self.assertTrue(nullable_column.get("isNullable", False))

    def test_convert_with_cloudevents_columns(self):
        """Test conversion with CloudEvents columns enabled."""
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        tmsl_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-ce.tmsl.json")
        os.makedirs(os.path.dirname(tmsl_path), exist_ok=True)

        convert_avro_to_tmsl(avro_path, None, tmsl_path, emit_cloudevents_columns=True)

        with open(tmsl_path, "r", encoding="utf-8") as f:
            tmsl = json.load(f)

        columns = tmsl["createOrReplace"]["database"]["model"]["tables"][0]["columns"]
        column_names = {column["name"] for column in columns}
        self.assertIn("___type", column_names)
        self.assertIn("___source", column_names)
        self.assertIn("___id", column_names)
        self.assertIn("___time", column_names)
        self.assertIn("___subject", column_names)

    def test_convert_with_relationships_from_foreign_keys(self):
        """Test relationship generation from Avro foreignKeys metadata."""
        avro_schema = [
            {
                "type": "record",
                "name": "Customer",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"}
                ],
                "unique": ["id"],
                "altnames": {"sql": "public.customers"}
            },
            {
                "type": "record",
                "name": "Order",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "customer_id", "type": "long"}
                ],
                "unique": ["id"],
                "foreignKeys": [
                    {
                        "name": "fk_orders_customers",
                        "columns": ["customer_id"],
                        "referencedTable": "Customer",
                        "referencedColumns": ["id"],
                        "referencedTableSql": "public.customers"
                    }
                ],
                "altnames": {"sql": "public.orders"}
            }
        ]

        avro_path = os.path.join(tempfile.gettempdir(), "avrotize", "relational.avsc")
        tmsl_path = os.path.join(tempfile.gettempdir(), "avrotize", "relational.tmsl.json")
        os.makedirs(os.path.dirname(avro_path), exist_ok=True)

        with open(avro_path, "w", encoding="utf-8") as f:
            json.dump(avro_schema, f, indent=2)

        convert_avro_to_tmsl(avro_path, None, tmsl_path)

        with open(tmsl_path, "r", encoding="utf-8") as f:
            tmsl = json.load(f)

        expected = self._load_reference("relational-ref.tmsl.json")
        self.assertEqual(expected, tmsl)

        model = tmsl["createOrReplace"]["database"]["model"]
        self.assertIn("relationships", model)
        self.assertEqual(1, len(model["relationships"]))

        relationship = model["relationships"][0]
        self.assertEqual("Order", relationship["fromTable"])
        self.assertEqual("customer_id", relationship["fromColumn"])
        self.assertEqual("Customer", relationship["toTable"])
        self.assertEqual("id", relationship["toColumn"])


if __name__ == "__main__":
    unittest.main()
