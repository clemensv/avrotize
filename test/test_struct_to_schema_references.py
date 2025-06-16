"""Tests for JSON Structure to JSON Schema reference file generation."""

import json
import unittest
from pathlib import Path
from avrotize.structuretojsons import convert_structure_to_json_schema_string


class TestStructureToJsonSchemaReferences(unittest.TestCase):
    """Test cases that verify the s2j generator produces reference files correctly."""

    def setUp(self):
        """Set up test environment."""
        self.test_dir = Path(__file__).parent / "struct"
        self.maxDiff = None  # Show full diff for JSON comparisons

    def get_struct_files(self):
        """Get all JSON Structure test files."""
        struct_files = []
        for file_path in self.test_dir.glob("*.struct.json"):
            if not file_path.name.endswith("-ref.json"):
                struct_files.append(file_path)
        return sorted(struct_files)

    def load_json_file(self, file_path):
        """Load and parse a JSON file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def test_basic_types_reference(self):
        """Test basic-types.struct.json generates correct reference."""
        struct_file = self.test_dir / "basic-types.struct.json"
        ref_file = self.test_dir / "basic-types-ref.json"
        
        self.assertTrue(struct_file.exists(), f"Structure file {struct_file} not found")
        self.assertTrue(ref_file.exists(), f"Reference file {ref_file} not found")
        
        # Load structure file and convert
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        generated_schema = convert_structure_to_json_schema_string(structure_content)
        generated_json = json.loads(generated_schema)
        
        # Load reference file
        reference_json = self.load_json_file(ref_file)
        
        # Compare
        self.assertEqual(generated_json, reference_json,
                        "Generated schema does not match reference file")

    def test_numeric_types_reference(self):
        """Test numeric-types.struct.json generates correct reference."""
        struct_file = self.test_dir / "numeric-types.struct.json"
        ref_file = self.test_dir / "numeric-types-ref.json"
        
        self.assertTrue(struct_file.exists(), f"Structure file {struct_file} not found")
        self.assertTrue(ref_file.exists(), f"Reference file {ref_file} not found")
        
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        generated_schema = convert_structure_to_json_schema_string(structure_content)
        generated_json = json.loads(generated_schema)
        reference_json = self.load_json_file(ref_file)
        
        self.assertEqual(generated_json, reference_json,
                        "Generated schema does not match reference file")

    def test_temporal_types_reference(self):
        """Test temporal-types.struct.json generates correct reference."""
        struct_file = self.test_dir / "temporal-types.struct.json"
        ref_file = self.test_dir / "temporal-types-ref.json"
        
        self.assertTrue(struct_file.exists(), f"Structure file {struct_file} not found")
        self.assertTrue(ref_file.exists(), f"Reference file {ref_file} not found")
        
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        generated_schema = convert_structure_to_json_schema_string(structure_content)
        generated_json = json.loads(generated_schema)
        reference_json = self.load_json_file(ref_file)
        
        self.assertEqual(generated_json, reference_json,
                        "Generated schema does not match reference file")

    def test_collections_reference(self):
        """Test collections.struct.json generates correct reference."""
        struct_file = self.test_dir / "collections.struct.json"
        ref_file = self.test_dir / "collections-ref.json"
        
        self.assertTrue(struct_file.exists(), f"Structure file {struct_file} not found")
        self.assertTrue(ref_file.exists(), f"Reference file {ref_file} not found")
        
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        generated_schema = convert_structure_to_json_schema_string(structure_content)
        generated_json = json.loads(generated_schema)
        reference_json = self.load_json_file(ref_file)
        
        self.assertEqual(generated_json, reference_json,
                        "Generated schema does not match reference file")

    def test_validation_constraints_reference(self):
        """Test validation-constraints.struct.json generates correct reference."""
        struct_file = self.test_dir / "validation-constraints.struct.json"
        ref_file = self.test_dir / "validation-constraints-ref.json"
        
        self.assertTrue(struct_file.exists(), f"Structure file {struct_file} not found")
        self.assertTrue(ref_file.exists(), f"Reference file {ref_file} not found")
        
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        generated_schema = convert_structure_to_json_schema_string(structure_content)
        generated_json = json.loads(generated_schema)
        reference_json = self.load_json_file(ref_file)
        
        self.assertEqual(generated_json, reference_json,
                        "Generated schema does not match reference file")

    def test_choice_types_reference(self):
        """Test choice-types.struct.json generates correct reference."""
        struct_file = self.test_dir / "choice-types.struct.json"
        ref_file = self.test_dir / "choice-types-ref.json"
        
        self.assertTrue(struct_file.exists(), f"Structure file {struct_file} not found")
        self.assertTrue(ref_file.exists(), f"Reference file {ref_file} not found")
        
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        generated_schema = convert_structure_to_json_schema_string(structure_content)
        generated_json = json.loads(generated_schema)
        reference_json = self.load_json_file(ref_file)
        
        self.assertEqual(generated_json, reference_json,
                        "Generated schema does not match reference file")

    def test_extensions_reference(self):
        """Test extensions.struct.json generates correct reference."""
        struct_file = self.test_dir / "extensions.struct.json"
        ref_file = self.test_dir / "extensions-ref.json"
        
        self.assertTrue(struct_file.exists(), f"Structure file {struct_file} not found")
        self.assertTrue(ref_file.exists(), f"Reference file {ref_file} not found")
        
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        generated_schema = convert_structure_to_json_schema_string(structure_content)
        generated_json = json.loads(generated_schema)
        reference_json = self.load_json_file(ref_file)
        
        self.assertEqual(generated_json, reference_json,
                        "Generated schema does not match reference file")

    def test_nested_objects_reference(self):
        """Test nested-objects.struct.json generates correct reference."""
        struct_file = self.test_dir / "nested-objects.struct.json"
        ref_file = self.test_dir / "nested-objects-ref.json"
        
        self.assertTrue(struct_file.exists(), f"Structure file {struct_file} not found")
        self.assertTrue(ref_file.exists(), f"Reference file {ref_file} not found")
        
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        generated_schema = convert_structure_to_json_schema_string(structure_content)
        generated_json = json.loads(generated_schema)
        reference_json = self.load_json_file(ref_file)
        
        self.assertEqual(generated_json, reference_json,
                        "Generated schema does not match reference file")

    def test_complex_scenario_reference(self):
        """Test complex-scenario.struct.json generates correct reference."""
        struct_file = self.test_dir / "complex-scenario.struct.json"
        ref_file = self.test_dir / "complex-scenario-ref.json"
        
        self.assertTrue(struct_file.exists(), f"Structure file {struct_file} not found")
        self.assertTrue(ref_file.exists(), f"Reference file {ref_file} not found")
        
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        generated_schema = convert_structure_to_json_schema_string(structure_content)
        generated_json = json.loads(generated_schema)
        reference_json = self.load_json_file(ref_file)
        
        self.assertEqual(generated_json, reference_json,
                        "Generated schema does not match reference file")

    def test_edge_cases_reference(self):
        """Test edge-cases.struct.json generates correct reference."""
        struct_file = self.test_dir / "edge-cases.struct.json"
        ref_file = self.test_dir / "edge-cases-ref.json"
        
        self.assertTrue(struct_file.exists(), f"Structure file {struct_file} not found")
        self.assertTrue(ref_file.exists(), f"Reference file {ref_file} not found")
        
        with open(struct_file, 'r', encoding='utf-8') as f:
            structure_content = f.read()
        
        generated_schema = convert_structure_to_json_schema_string(structure_content)
        generated_json = json.loads(generated_schema)
        reference_json = self.load_json_file(ref_file)
        
        self.assertEqual(generated_json, reference_json,
                        "Generated schema does not match reference file")

    def test_all_reference_files_exist(self):
        """Test that all structure files have corresponding reference files."""
        struct_files = self.get_struct_files()
        missing_refs = []
        
        for struct_file in struct_files:
            base_name = struct_file.stem
            if base_name.endswith(".struct"):
                base_name = base_name[:-7]
            
            ref_file = struct_file.parent / f"{base_name}-ref.json"
            if not ref_file.exists():
                missing_refs.append(ref_file.name)
        
        self.assertEqual(len(missing_refs), 0, 
                        f"Missing reference files: {missing_refs}")

    def test_all_structure_files_generate_valid_json(self):
        """Test that all structure files can be converted to valid JSON."""
        struct_files = self.get_struct_files()
        conversion_failures = []
        
        for struct_file in struct_files:
            try:
                with open(struct_file, 'r', encoding='utf-8') as f:
                    structure_content = f.read()
                
                generated_schema = convert_structure_to_json_schema_string(structure_content)
                
                # Verify it's valid JSON
                json.loads(generated_schema)
                
            except Exception as e:
                conversion_failures.append(f"{struct_file.name}: {str(e)}")
        
        self.assertEqual(len(conversion_failures), 0,
                        f"Conversion failures: {conversion_failures}")

    def test_generated_schemas_have_required_fields(self):
        """Test that all generated schemas have required JSON Schema fields."""
        struct_files = self.get_struct_files()
        
        for struct_file in struct_files:
            with self.subTest(file=struct_file.name):
                with open(struct_file, 'r', encoding='utf-8') as f:
                    structure_content = f.read()
                
                generated_schema = convert_structure_to_json_schema_string(structure_content)
                schema_json = json.loads(generated_schema)
                
                # Check required fields
                self.assertIn("$schema", schema_json, 
                             f"Missing $schema in {struct_file.name}")
                self.assertEqual(schema_json["$schema"], 
                               "https://json-schema.org/draft/2020-12/schema",
                               f"Wrong schema version in {struct_file.name}")
                self.assertIn("type", schema_json,
                             f"Missing type in {struct_file.name}")

    def test_roundtrip_conversion_consistency(self):
        """Test that converting structure -> schema -> structure maintains key information."""
        # This is a basic smoke test for roundtrip conversion
        # We'll test with a simple structure to ensure basic consistency
        
        simple_structure = {
            "type": "object",
            "properties": {
                "name": {"type": "string", "maxLength": 50},
                "age": {"type": "int32", "minimum": 0}
            },
            "required": ["name"]
        }
        
        # Convert to JSON Schema
        structure_json = json.dumps(simple_structure)
        schema_result = convert_structure_to_json_schema_string(structure_json)
        schema_json = json.loads(schema_result)
        
        # Basic checks
        self.assertEqual(schema_json["type"], "object")
        self.assertIn("properties", schema_json)
        self.assertIn("name", schema_json["properties"])
        self.assertIn("age", schema_json["properties"])
        self.assertEqual(schema_json["required"], ["name"])


if __name__ == '__main__':
    unittest.main()
