import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

from fastavro import parse_schema

from avrotize.avrotosmithy import convert_avro_to_smithy, convert_avro_to_smithy_schema
from avrotize.jstructtosmithy import convert_json_structure_to_smithy
from avrotize.smithytoavro import convert_smithy_to_avro, convert_smithy_to_avro_schema
from avrotize.smithytojstruct import convert_smithy_to_json_structure


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "test" / "smithy"
MODEL = FIXTURE_DIR / "model.smithy"
OUT_DIR = FIXTURE_DIR / "generated"


class TestSmithyToAvro(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if OUT_DIR.exists():
            shutil.rmtree(OUT_DIR)
        OUT_DIR.mkdir(parents=True)

    @classmethod
    def tearDownClass(cls):
        if OUT_DIR.exists():
            shutil.rmtree(OUT_DIR)

    def convert(self, text):
        return convert_smithy_to_avro_schema(text)

    def by_name(self, schema, name):
        return next(item for item in schema if isinstance(item, dict) and item.get("name") == name)

    def test_import_resolves_to_worktree(self):
        import avrotize
        self.assertTrue(str(avrotize.__file__).startswith(str(ROOT)))

    def test_namespace_maps_to_avro_namespace(self):
        schema = self.convert('namespace com.example\nstructure Foo { name: String }')
        self.assertEqual(self.by_name(schema, "Foo")["namespace"], "com.example")

    def test_all_simple_shapes_are_mapped(self):
        smithy = '''namespace com.example
structure Scalars {
    blob: Blob
    bool: Boolean
    text: String
    byteValue: Byte
    shortValue: Short
    intValue: Integer
    longValue: Long
    floatValue: Float
    doubleValue: Double
    bigInteger: BigInteger
    bigDecimal: BigDecimal
    timestamp: Timestamp
    document: Document
}'''
        record = self.by_name(self.convert(smithy), "Scalars")
        fields = {field["name"]: field["type"][1] for field in record["fields"]}
        self.assertEqual(fields["blob"], "bytes")
        self.assertEqual(fields["bool"], "boolean")
        self.assertEqual(fields["text"], "string")
        self.assertEqual(fields["byteValue"], "int")
        self.assertEqual(fields["shortValue"], "int")
        self.assertEqual(fields["intValue"], "int")
        self.assertEqual(fields["longValue"], "long")
        self.assertEqual(fields["floatValue"], "float")
        self.assertEqual(fields["doubleValue"], "double")
        self.assertEqual(fields["bigInteger"], "string")
        self.assertEqual(fields["bigDecimal"], "double")
        self.assertEqual(fields["timestamp"], {"type": "long", "logicalType": "timestamp-millis"})
        self.assertEqual(fields["document"], "string")

    def test_required_and_optional_nullability(self):
        record = self.by_name(self.convert('namespace com.example\nstructure Foo { @required id: String, nick: String }'), "Foo")
        fields = {field["name"]: field for field in record["fields"]}
        self.assertEqual(fields["id"]["type"], "string")
        self.assertEqual(fields["nick"]["type"], ["null", "string"])
        self.assertIsNone(fields["nick"]["default"])

    def test_default_trait_is_preserved(self):
        record = self.by_name(self.convert('namespace com.example\nstructure Foo { @default("guest") nick: String }'), "Foo")
        field = record["fields"][0]
        self.assertEqual(field["type"], ["string", "null"])
        self.assertEqual(field["default"], "guest")
        parse_schema(record)

    def test_documentation_trait_maps_to_doc(self):
        record = self.by_name(self.convert('namespace com.example\n@documentation("Doc")\nstructure Foo { @documentation("Field doc") name: String }'), "Foo")
        self.assertEqual(record["doc"], "Doc")
        self.assertEqual(record["fields"][0]["doc"], "Field doc")

    def test_enum_and_int_enum(self):
        schema = self.convert('namespace com.example\nenum Color { RED, GREEN }\nintEnum Status { OK = 0, ERROR = 9 }')
        color = self.by_name(schema, "Color")
        status = self.by_name(schema, "Status")
        self.assertEqual(color["type"], "enum")
        self.assertEqual(color["symbols"], ["RED", "GREEN"])
        self.assertEqual(status["ordinals"], {"OK": 0, "ERROR": 9})

    def test_union_shape_preserves_member_names(self):
        payload = self.by_name(self.convert('namespace com.example\nunion Payload { text: String, count: Integer }'), "Payload")
        self.assertEqual(payload["smithyShape"], "union")
        self.assertEqual([field["name"] for field in payload["fields"]], ["text", "count"])
        self.assertEqual(payload["fields"][0]["type"], ["null", "string"])

    def test_list_shape_maps_to_avro_array(self):
        names = self.by_name(self.convert('namespace com.example\nlist Names { member: String }'), "Names")
        self.assertEqual(names["smithyShape"], "list")
        self.assertEqual(names["fields"][0]["type"], {"type": "array", "items": "string"})

    def test_map_shape_maps_to_avro_map(self):
        tags = self.by_name(self.convert('namespace com.example\nmap Tags { key: String, value: Integer }'), "Tags")
        self.assertEqual(tags["smithyShape"], "map")
        self.assertEqual(tags["fields"][0]["type"], {"type": "map", "values": "int"})

    def test_nested_structures_and_references(self):
        schema = self.convert('namespace com.example\nstructure Address { city: String }\nstructure Person { @required address: Address }')
        parse_schema(schema)
        person = self.by_name(schema, "Person")
        self.assertEqual(person["fields"][0]["type"], "Address")

    def test_named_list_and_map_references_inline_as_array_and_map(self):
        schema = self.convert('namespace com.example\nlist Names { member: String }\nmap Scores { key: String, value: Integer }\nstructure Foo { names: Names, scores: Scores }')
        foo = self.by_name(schema, "Foo")
        fields = {field["name"]: field["type"][1] for field in foo["fields"]}
        self.assertEqual(fields["names"], {"type": "array", "items": "string"})
        self.assertEqual(fields["scores"], {"type": "map", "values": "int"})

    def test_timestamp_logical_type_is_valid(self):
        schema = self.convert('namespace com.example\nstructure Event { @required time: Timestamp }')
        parse_schema(schema)
        event = self.by_name(schema, "Event")
        self.assertEqual(event["fields"][0]["type"], {"type": "long", "logicalType": "timestamp-millis"})

    def test_out_of_scope_service_and_operation_are_skipped(self):
        schema = self.convert('namespace com.example\nstructure Foo { name: String }\nservice S { operations: [Get] }\noperation Get { input: Foo, output: Foo }')
        self.assertEqual([item["name"] for item in schema], ["Foo"])
        parse_schema(schema)

    def test_metadata_and_apply_are_skipped_without_consuming_shapes(self):
        schema = self.convert('namespace com.example\nmetadata foo = "bar"\napply Foo @deprecated\nstructure Foo { name: String }')
        self.assertEqual([item["name"] for item in schema], ["Foo"])
        parse_schema(schema)

    def test_fixture_avro_is_valid(self):
        output = OUT_DIR / "model.avsc"
        convert_smithy_to_avro(str(MODEL), str(output))
        schema = json.loads(output.read_text(encoding="utf-8"))
        parse_schema(schema)
        self.assertIn("Person", [item["name"] for item in schema])

    def test_avro_to_smithy_emits_parseable_smithy(self):
        avro = [{"type": "record", "name": "Person", "namespace": "com.example", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": ["null", "int"], "default": None}]}]
        text = convert_avro_to_smithy_schema(avro)
        self.assertIn('$version: "2.0"', text)
        self.assertIn("namespace com.example", text)
        self.assertIn("@required", text)
        reparsed = convert_smithy_to_avro_schema(text)
        self.assertEqual(self.by_name(reparsed, "Person")["fields"][0]["type"], "string")

    def test_avro_enum_array_map_union_to_smithy(self):
        avro = [
            {"type": "enum", "name": "Color", "namespace": "com.example", "symbols": ["RED", "GREEN"]},
            {"type": "record", "name": "Foo", "namespace": "com.example", "fields": [
                {"name": "colors", "type": {"type": "array", "items": "Color"}},
                {"name": "tags", "type": {"type": "map", "values": "string"}},
                {"name": "choice", "type": ["string", "int"]},
            ]},
        ]
        text = convert_avro_to_smithy_schema(avro)
        self.assertIn("enum Color", text)
        self.assertIn("list FooColorsList", text)
        self.assertIn("map FooTagsMap", text)
        self.assertIn("union FooChoiceUnion", text)

    def test_round_trip_preserves_data_shapes(self):
        first = json.loads((OUT_DIR / "roundtrip1.avsc").read_text(encoding="utf-8")) if (OUT_DIR / "roundtrip1.avsc").exists() else None
        source_avro = convert_smithy_to_avro_schema(MODEL.read_text(encoding="utf-8"))
        smithy_text = convert_avro_to_smithy_schema(source_avro)
        second_avro = convert_smithy_to_avro_schema(smithy_text)
        self.assertEqual({item["name"] for item in source_avro}, {item["name"] for item in second_avro})
        self.assertEqual(self.by_name(second_avro, "Payload")["smithyShape"], "union")
        parse_schema(second_avro)

    def test_smithy_to_json_structure_bridge(self):
        output = OUT_DIR / "model.struct.json"
        convert_smithy_to_json_structure(str(MODEL), str(output))
        data = json.loads(output.read_text(encoding="utf-8"))
        self.assertIsInstance(data, dict)
        self.assertTrue(data)

    def test_json_structure_to_smithy_bridge(self):
        struct_path = OUT_DIR / "bridge.struct.json"
        smithy_path = OUT_DIR / "bridge.smithy"
        convert_smithy_to_json_structure(str(MODEL), str(struct_path))
        convert_json_structure_to_smithy(str(struct_path), str(smithy_path), namespace="com.example.bridge")
        text = smithy_path.read_text(encoding="utf-8")
        self.assertIn("namespace com.example.bridge", text)
        self.assertIn("structure", text)

    def test_cli_smoke_smithy2a(self):
        output = OUT_DIR / "cli.avsc"
        result = subprocess.run(
            [sys.executable, "-m", "avrotize", "smithy2a", str(MODEL), "--out", str(output)],
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        parse_schema(json.loads(output.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
