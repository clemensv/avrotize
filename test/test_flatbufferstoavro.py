import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from fastavro import parse_schema

from avrotize.avrotoflatbuffers import convert_avro_to_flatbuffers
from avrotize.flatbufferstoavro import FlatBuffersParser, convert_flatbuffers_to_avro
from avrotize.flatbufferstojstruct import convert_flatbuffers_to_json_structure
from avrotize.jstructtoflatbuffers import convert_json_structure_to_flatbuffers


ROOT = Path(__file__).resolve().parents[1]
FBS_DIR = ROOT / "test" / "fbs"


class TestFlatBuffersConverters(unittest.TestCase):
    def convert_fixture(self, fixture: str):
        tmp = tempfile.TemporaryDirectory(dir=FBS_DIR)
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name) / "schema.avsc"
        convert_flatbuffers_to_avro(str(FBS_DIR / fixture), str(out))
        with out.open(encoding="utf-8") as f:
            schema = json.load(f)
        parse_schema(schema)
        return schema, Path(tmp.name)

    @staticmethod
    def by_name(schema):
        return {item["name"]: item for item in schema}

    def test_imports_worktree_package(self):
        import avrotize
        self.assertTrue(str(Path(avrotize.__file__).resolve()).startswith(str(ROOT)))

    def test_namespace_maps_to_avro_namespace(self):
        schema, _ = self.convert_fixture("monster.fbs")
        self.assertEqual(self.by_name(schema)["Monster"]["namespace"], "Example.Game")

    def test_table_maps_to_avro_record(self):
        schema, _ = self.convert_fixture("monster.fbs")
        monster = self.by_name(schema)["Monster"]
        self.assertEqual(monster["type"], "record")
        self.assertIn("fields", monster)

    def test_struct_maps_to_record_with_layout_doc(self):
        schema, _ = self.convert_fixture("monster.fbs")
        vec3 = self.by_name(schema)["Vec3"]
        self.assertEqual(vec3["type"], "record")
        self.assertIn("fixed inline memory layout", vec3["doc"])

    def test_enum_symbols_and_explicit_ordinals(self):
        schema, _ = self.convert_fixture("monster.fbs")
        color = self.by_name(schema)["Color"]
        self.assertEqual(color["symbols"], ["Red", "Green", "Blue"])
        self.assertEqual(color["ordinals"], {"Red": 1, "Green": 2, "Blue": 4})

    def test_required_field_is_not_nullable(self):
        schema, _ = self.convert_fixture("monster.fbs")
        fields = {f["name"]: f for f in self.by_name(schema)["Monster"]["fields"]}
        self.assertEqual(fields["name"]["type"], "string")
        self.assertTrue(fields["name"]["fbsRequired"])

    def test_optional_field_is_nullable_with_null_default(self):
        schema, _ = self.convert_fixture("monster.fbs")
        fields = {f["name"]: f for f in self.by_name(schema)["Monster"]["fields"]}
        self.assertEqual(fields["mana"]["type"], ["null", "int"])
        self.assertIsNone(fields["mana"]["default"])

    def test_field_defaults_are_preserved_as_fbs_default(self):
        schema, _ = self.convert_fixture("monster.fbs")
        fields = {f["name"]: f for f in self.by_name(schema)["Monster"]["fields"]}
        self.assertEqual(fields["hp"]["fbsDefault"], 100)
        self.assertEqual(fields["alive"]["fbsDefault"], True)

    def test_ubyte_vector_maps_to_bytes(self):
        schema, _ = self.convert_fixture("monster.fbs")
        fields = {f["name"]: f for f in self.by_name(schema)["Monster"]["fields"]}
        self.assertEqual(fields["inventory"]["type"], ["null", "bytes"])

    def test_vector_of_table_maps_to_array(self):
        schema, _ = self.convert_fixture("monster.fbs")
        fields = {f["name"]: f for f in self.by_name(schema)["Monster"]["fields"]}
        self.assertEqual(fields["weapons"]["type"][1], {"type": "array", "items": "Example.Game.Weapon"})

    def test_nested_table_reference_is_qualified(self):
        schema, _ = self.convert_fixture("monster.fbs")
        fields = {f["name"]: f for f in self.by_name(schema)["Monster"]["fields"]}
        self.assertEqual(fields["pos"]["type"], "Example.Game.Vec3")

    def test_root_type_annotation_is_preserved(self):
        schema, _ = self.convert_fixture("monster.fbs")
        self.assertTrue(self.by_name(schema)["Monster"]["root_type"])

    def test_all_required_scalars_map_to_avro_primitives(self):
        schema, _ = self.convert_fixture("scalars.fbs")
        fields = {f["name"]: f["type"] for f in self.by_name(schema)["Scalars"]["fields"]}
        self.assertEqual(fields, {
            "b": "boolean", "i8": "int", "u8": "int", "i16": "int", "u16": "int",
            "i32": "int", "u32": "long", "i64": "long", "u64": "long",
            "f32": "float", "f64": "double", "s": "string",
        })

    def test_flatbuffers_union_maps_to_avro_union(self):
        schema, _ = self.convert_fixture("union.fbs")
        holder = self.by_name(schema)["Holder"]
        item = next(f for f in holder["fields"] if f["name"] == "item")
        self.assertEqual(item["type"], ["null", "Example.Union.Sword", "Example.Union.Spell"])

    def test_avro_to_flatbuffers_emits_parseable_schema(self):
        schema, tmp = self.convert_fixture("monster.fbs")
        avro_path = tmp / "schema.avsc"
        fbs_path = tmp / "schema.fbs"
        with avro_path.open("w", encoding="utf-8") as f:
            json.dump(schema, f)
        convert_avro_to_flatbuffers(str(avro_path), str(fbs_path))
        parsed = FlatBuffersParser(fbs_path.read_text(encoding="utf-8")).parse()
        self.assertEqual(parsed.namespace, "Example.Game")
        self.assertEqual(parsed.root_type, "Monster")
        self.assertIn("Monster", [t.name for t in parsed.tables])

    def test_avro_to_flatbuffers_maps_arrays_and_enums(self):
        schema, tmp = self.convert_fixture("monster.fbs")
        avro_path = tmp / "schema.avsc"
        fbs_path = tmp / "schema.fbs"
        avro_path.write_text(json.dumps(schema), encoding="utf-8")
        convert_avro_to_flatbuffers(str(avro_path), str(fbs_path))
        text = fbs_path.read_text(encoding="utf-8")
        self.assertIn("enum Color : int { Red = 1, Green = 2, Blue = 4 }", text)
        self.assertIn("weapons: [Weapon];", text)
        self.assertIn("inventory: [ubyte];", text)

    def test_round_trip_preserves_names_and_root(self):
        schema, tmp = self.convert_fixture("monster.fbs")
        avro_path = tmp / "schema.avsc"
        fbs_path = tmp / "schema.fbs"
        roundtrip_path = tmp / "roundtrip.avsc"
        avro_path.write_text(json.dumps(schema), encoding="utf-8")
        convert_avro_to_flatbuffers(str(avro_path), str(fbs_path))
        convert_flatbuffers_to_avro(str(fbs_path), str(roundtrip_path))
        roundtrip = json.loads(roundtrip_path.read_text(encoding="utf-8"))
        self.assertEqual(set(self.by_name(schema)), set(self.by_name(roundtrip)))
        self.assertTrue(self.by_name(roundtrip)["Monster"].get("root_type"))

    def test_flatbuffers_to_json_structure_bridge(self):
        _, tmp = self.convert_fixture("monster.fbs")
        out = tmp / "schema.struct.json"
        convert_flatbuffers_to_json_structure(str(FBS_DIR / "monster.fbs"), str(out))
        structure = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(structure["name"], "Monster")
        self.assertIn("$root", structure)
        self.assertIn("definitions", structure)

    def test_json_structure_to_flatbuffers_bridge(self):
        _, tmp = self.convert_fixture("monster.fbs")
        struct_path = tmp / "schema.struct.json"
        fbs_path = tmp / "from_structure.fbs"
        convert_flatbuffers_to_json_structure(str(FBS_DIR / "monster.fbs"), str(struct_path))
        convert_json_structure_to_flatbuffers(str(struct_path), str(fbs_path))
        parsed = FlatBuffersParser(fbs_path.read_text(encoding="utf-8")).parse()
        self.assertEqual(parsed.root_type, "Monster")
        self.assertIn("Monster", [t.name for t in parsed.tables])

    def test_cli_fbs2a_smoke(self):
        with tempfile.TemporaryDirectory(dir=FBS_DIR) as tmp:
            out = Path(tmp) / "cli.avsc"
            result = subprocess.run(
                [sys.executable, "-m", "avrotize", "fbs2a", str(FBS_DIR / "monster.fbs"), "--out", str(out)],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            parse_schema(json.loads(out.read_text(encoding="utf-8")))

    def test_commands_json_is_valid_and_contains_flatbuffers_commands(self):
        commands = json.loads((ROOT / "avrotize" / "commands.json").read_text(encoding="utf-8"))
        by_command = {cmd["command"]: cmd for cmd in commands}
        for command in ["fbs2a", "a2fbs", "fbs2s", "s2fbs"]:
            self.assertIn(command, by_command)
        self.assertEqual(by_command["fbs2a"]["extensions"], [".fbs"])


if __name__ == "__main__":
    unittest.main()
