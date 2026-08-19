import glob
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


from avrotize.avrotorust import (
    AvroToRust,
    convert_avro_schema_to_rust,
    convert_avro_to_rust,
)
from avrotize.common import generic_type
from avrotize.jsonstoavro import convert_jsons_to_avro
import pytest


class TestAvroToRust(unittest.TestCase):
    
    # Timeout in seconds for cargo commands
    CARGO_TIMEOUT = 300

    def test_avro_union_path_identity_is_typed_and_bounded(self):
        """Keep structural union identities distinct and filesystem-safe."""
        converter = AvroToRust()
        array_path = [
            ('record', 'n.' + ('R' * 80)),
            ('field', 'items'),
            ('array', 'items'),
        ]
        real_field_path = [
            ('record', 'n.' + ('R' * 80)),
            ('field', 'itemsArrayItems'),
        ]
        branch_map_path = array_path + [
            ('branch', '1'),
            ('map', 'values'),
        ]
        identities = {
            converter.union_name_from_path(array_path),
            converter.union_name_from_path(real_field_path),
            converter.union_name_from_path(branch_map_path),
        }
        self.assertEqual(3, len(identities))
        self.assertTrue(all(len(identity) < 100 for identity in identities))

    def test_recursive_named_type_shape_signature(self):
        """Bound structural JSON matching for recursive named records."""
        converter = AvroToRust()
        def recursive_node(name):
            return {
                "type": "record",
                "name": name,
                "namespace": "issue406.recursive",
                "fields": [
                    {
                        "name": "children",
                        "type": {
                            "type": "array",
                            "items": name,
                        },
                    }
                ],
            }

        node_a = recursive_node("NodeA")
        node_b = recursive_node("NodeB")
        converter.index_avro_named_types([node_a, node_b])
        signature_a = converter.get_json_shape_signature(
            node_a,
            "issue406.recursive",
        )
        signature_b = converter.get_json_shape_signature(
            node_b,
            "issue406.recursive",
        )
        self.assertIn(
            "('ref', 0)",
            str(signature_a),
        )
        self.assertEqual(signature_a, signature_b)

    def test_named_type_resolution_requires_current_namespace(self):
        """Do not resolve unqualified names from unrelated namespaces."""
        converter = AvroToRust()
        left = {
            "type": "record",
            "name": "Item",
            "namespace": "Left",
            "fields": [],
        }
        converter.index_avro_named_types(left)
        self.assertIsNone(
            converter.resolve_avro_named_type("Item", "Right")
        )
        self.assertIs(
            left,
            converter.resolve_avro_named_type("Left.Item", "Right"),
        )

    def test_logical_fixed_mapping_is_preserved(self):
        """Keep the existing decimal fixed mapping while fixing plain fixed."""
        converter = AvroToRust()
        self.assertEqual(
            "f64",
            converter.convert_avro_type_to_rust(
                "amount",
                {
                    "type": "fixed",
                    "name": "Amount",
                    "size": 8,
                    "logicalType": "decimal",
                    "precision": 12,
                    "scale": 2,
                },
                "issue406.fixed",
            ),
        )

    def test_case_distinct_avro_names_fail_before_output(self):
        """Reject Avro names that normalize to one Rust module path."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-case-collision",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        schema = [
            {
                "type": "record",
                "name": "Item",
                "namespace": "Foo",
                "fields": [],
            },
            {
                "type": "record",
                "name": "Item",
                "namespace": "foo",
                "fields": [],
            },
        ]
        with self.assertRaisesRegex(
            ValueError,
            r"exact path collision.*Foo\.Item.*foo\.Item",
        ):
            convert_avro_schema_to_rust(
                schema,
                rust_path,
                package_name="rust-case-collision",
                avro_annotation=True,
            )
        self.assertFalse(os.path.exists(rust_path))

        type_case_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-type-case-collision",
        )
        if os.path.exists(type_case_path):
            shutil.rmtree(type_case_path, ignore_errors=True)
        with self.assertRaisesRegex(
            ValueError,
            r"exact path collision.*foo\.ITEM.*foo\.Item",
        ):
            convert_avro_schema_to_rust(
                [
                    {
                        "type": "record",
                        "name": "Item",
                        "namespace": "foo",
                        "fields": [],
                    },
                    {
                        "type": "record",
                        "name": "ITEM",
                        "namespace": "foo",
                        "fields": [],
                    },
                ],
                type_case_path,
                package_name="rust-type-case-collision",
                avro_annotation=True,
            )
        self.assertFalse(os.path.exists(type_case_path))

    def test_rust_type_file_directory_conflict_fails_before_output(self):
        """Reject a Rust module path required as both file and directory."""
        schemas = [
            {
                "type": "record",
                "name": "B",
                "namespace": "a",
                "fields": [],
            },
            {
                "type": "record",
                "name": "C",
                "namespace": "a.b",
                "fields": [],
            },
        ]
        for index, schema in enumerate((schemas, list(reversed(schemas)))):
            rust_path = os.path.join(
                tempfile.gettempdir(),
                "avrotize",
                f"rust-prefix-collision-{index}",
            )
            if os.path.exists(rust_path):
                shutil.rmtree(rust_path, ignore_errors=True)
            with self.assertRaisesRegex(
                ValueError,
                r"both a file and directory.*a\.B.*a\.b\.C",
            ):
                convert_avro_schema_to_rust(
                    schema,
                    rust_path,
                    package_name="rust-prefix-collision",
                    avro_annotation=True,
                )
            self.assertFalse(os.path.exists(rust_path))

    def test_reserved_rust_module_paths_fail_before_output(self):
        """Reject named types that collide with generated mod.rs/lib.rs."""
        schemas = [
            {
                "type": "record",
                "name": "Mod",
                "namespace": "n",
                "fields": [],
            },
            {
                "type": "record",
                "name": "Lib",
                "fields": [],
            },
            {
                "type": "record",
                "name": "NestedMod",
                "namespace": "n.mod",
                "fields": [],
            },
            {
                "type": "record",
                "name": "NestedLib",
                "namespace": "lib.n",
                "fields": [],
            },
        ]
        cases = [[schema] for schema in schemas]
        cases.extend((schemas, list(reversed(schemas))))
        ordered_cases = []
        for schema in cases:
            ordered_cases.extend((schema, list(reversed(schema))))
        for index, schema in enumerate(ordered_cases):
            rust_path = os.path.join(
                tempfile.gettempdir(),
                "avrotize",
                f"rust-reserved-path-{index}",
            )
            if os.path.exists(rust_path):
                shutil.rmtree(rust_path, ignore_errors=True)
            with self.assertRaisesRegex(
                ValueError,
                r"(generation plan has an exact path collision|"
                r"generation plan requires the same path|"
                r"generated Rust (mod|lib)\.rs)",
            ):
                convert_avro_schema_to_rust(
                    schema,
                    rust_path,
                    package_name="rust-reserved-path",
                    avro_annotation=True,
                )
            self.assertFalse(os.path.exists(rust_path))

    def test_generated_union_and_alias_paths_fail_before_output(self):
        """Preflight generated union and legacy alias path collisions."""
        converter = AvroToRust()
        union_name = converter.union_name_from_path([
            ('record', 'n.Carrier'),
            ('field', 'choice'),
        ])
        cases = [
            [
                {
                    "type": "record",
                    "name": union_name,
                    "namespace": "n",
                    "fields": [],
                },
                {
                    "type": "record",
                    "name": "Carrier",
                    "namespace": "n",
                    "fields": [
                        {
                            "name": "choice",
                            "type": ["long", "int"],
                        }
                    ],
                },
            ],
            [
                {
                    "type": "record",
                    "name": "FooUnion",
                    "namespace": "n",
                    "fields": [],
                },
                {
                    "type": "record",
                    "name": "Carrier",
                    "namespace": "n",
                    "fields": [
                        {
                            "name": "foo",
                            "type": ["long", "int"],
                        }
                    ],
                },
            ],
        ]
        for index, schema in enumerate(cases):
            rust_path = os.path.join(
                tempfile.gettempdir(),
                "avrotize",
                f"rust-planned-path-collision-{index}",
            )
            if os.path.exists(rust_path):
                shutil.rmtree(rust_path, ignore_errors=True)
            with self.assertRaisesRegex(
                ValueError,
                r"generation plan has an exact path collision",
            ):
                convert_avro_schema_to_rust(
                    schema,
                    rust_path,
                    package_name="rust-planned-path-collision",
                    avro_annotation=True,
                )
            self.assertFalse(os.path.exists(rust_path))

    def test_generation_plan_matches_annotation_mode(self):
        """Plan hash unions only for Avro and legacy names otherwise."""
        converter = AvroToRust()
        hash_name = converter.union_name_from_path([
            ('record', 'n.Carrier'),
            ('field', 'choice'),
        ])
        hash_schema = [
            {
                "type": "record",
                "name": hash_name,
                "namespace": "n",
                "fields": [],
            },
            {
                "type": "record",
                "name": "Carrier",
                "namespace": "n",
                "fields": [
                    {
                        "name": "choice",
                        "type": ["long", "int"],
                    }
                ],
            },
        ]
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-plan-non-avro-hash",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        convert_avro_schema_to_rust(
            hash_schema,
            rust_path,
            package_name="rust-plan-non-avro-hash",
            avro_annotation=False,
        )
        self.assertTrue(os.path.exists(rust_path))

        collision_schema = [
            {
                "type": "record",
                "name": record_name,
                "namespace": "n",
                "fields": [
                    {
                        "name": "choice",
                        "type": ["long", "int"],
                    }
                ],
            }
            for record_name in ("One", "Two")
        ]
        collision_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-plan-non-avro-collision",
        )
        if os.path.exists(collision_path):
            shutil.rmtree(collision_path, ignore_errors=True)
        convert_avro_schema_to_rust(
            collision_schema,
            collision_path,
            package_name="rust-plan-non-avro-collision",
            avro_annotation=False,
        )
        self.assertTrue(os.path.exists(collision_path))

        symbol_collision_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-plan-symbol-collision",
        )
        if os.path.exists(symbol_collision_path):
            shutil.rmtree(symbol_collision_path, ignore_errors=True)
        with self.assertRaisesRegex(
            ValueError,
            r"exact path collision.*FooBarUnion.*FoobarUnion",
        ):
            convert_avro_schema_to_rust(
                {
                    "type": "record",
                    "name": "Carrier",
                    "namespace": "n",
                    "fields": [
                        {
                            "name": "foo_bar",
                            "type": ["long", "int"],
                        },
                        {
                            "name": "foobar",
                            "type": ["long", "int"],
                        },
                    ],
                },
                symbol_collision_path,
                package_name="rust-plan-symbol-collision",
                avro_annotation=False,
            )
        self.assertFalse(os.path.exists(symbol_collision_path))

    def test_generated_alias_is_replaced_on_regeneration(self):
        """Atomically replace stale generated aliases after a moved field."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-alias-regeneration",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)

        def schema(record_name):
            return {
                "type": "record",
                "name": record_name,
                "namespace": "n",
                "fields": [
                    {
                        "name": "choice",
                        "type": ["long", "int"],
                    }
                ],
            }

        convert_avro_schema_to_rust(
            schema("First"),
            rust_path,
            package_name="rust-alias-regeneration",
            avro_annotation=True,
        )
        alias_path = os.path.join(
            rust_path,
            "src",
            "n",
            "choiceunion.rs",
        )
        with open(alias_path, "r", encoding="utf-8") as alias_file:
            first_content = alias_file.read()
        convert_avro_schema_to_rust(
            schema("Second"),
            rust_path,
            package_name="rust-alias-regeneration",
            avro_annotation=True,
        )
        with open(alias_path, "r", encoding="utf-8") as alias_file:
            second_content = alias_file.read()
        self.assertNotEqual(first_content, second_content)
        self.assertNotIn(".tmp", os.listdir(os.path.dirname(alias_path)))

        with open(alias_path, "w", encoding="utf-8") as alias_file:
            alias_file.write(
                first_content + "\n// user-owned customization\n"
            )
        with self.assertRaisesRegex(
            ValueError,
            r"Existing file conflicts with planned legacy union alias",
        ):
            convert_avro_schema_to_rust(
                schema("Third"),
                rust_path,
                package_name="rust-alias-regeneration",
                avro_annotation=True,
            )

    def test_output_parent_path_case_is_preserved(self):
        """Never lowercase caller-provided output directory components."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "AvrotizeCaseSensitiveParent",
            "GeneratedRust",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(
                os.path.dirname(rust_path),
                ignore_errors=True,
            )
        convert_avro_schema_to_rust(
            {
                "type": "record",
                "name": "Carrier",
                "namespace": "n",
                "fields": [
                    {
                        "name": "choice",
                        "type": ["long", "int"],
                    }
                ],
            },
            rust_path,
            package_name="rust-case-sensitive-parent",
            avro_annotation=True,
        )
        self.assertTrue(os.path.isfile(os.path.join(rust_path, "Cargo.toml")))
        union_files = glob.glob(
            os.path.join(rust_path, "src", "n", "unionpath*.rs")
        )
        self.assertEqual(1, len(union_files))

        root_schemas = [
            {
                "type": "record",
                "name": "B",
                "fields": [],
            },
            {
                "type": "record",
                "name": "C",
                "namespace": "b",
                "fields": [],
            },
        ]
        for index, schema in enumerate(
            (root_schemas, list(reversed(root_schemas)))
        ):
            rust_path = os.path.join(
                tempfile.gettempdir(),
                "avrotize",
                f"rust-root-prefix-collision-{index}",
            )
            if os.path.exists(rust_path):
                shutil.rmtree(rust_path, ignore_errors=True)
            with self.assertRaisesRegex(
                ValueError,
                r"both a file and directory.*named type B.*named type b\.C",
            ):
                convert_avro_schema_to_rust(
                    schema,
                    rust_path,
                    package_name="rust-root-prefix-collision",
                    avro_annotation=True,
                )
            self.assertFalse(os.path.exists(rust_path))

    def test_rust_module_output_is_hash_seed_deterministic(self):
        """Sort generated module declarations independently of hash seed."""
        outputs = []
        fixture = os.path.join(
            os.getcwd(),
            "test",
            "avsc",
            "rust-multitype-annotations.avsc",
        )
        for seed in ("1", "8675309"):
            rust_path = os.path.join(
                tempfile.gettempdir(),
                "avrotize",
                f"rust-determinism-{seed}",
            )
            if os.path.exists(rust_path):
                shutil.rmtree(rust_path, ignore_errors=True)
            env = os.environ.copy()
            env["PYTHONHASHSEED"] = seed
            subprocess.check_call(
                [
                    sys.executable,
                    "-c",
                    (
                        "from avrotize.avrotorust import convert_avro_to_rust; "
                        f"convert_avro_to_rust(r'{fixture}', r'{rust_path}', "
                        "package_name='rust-determinism', "
                        "avro_annotation=True, serde_annotation=True)"
                    ),
                ],
                env=env,
            )
            module_files = {}
            for root, _, files in os.walk(os.path.join(rust_path, "src")):
                for file_name in files:
                    if file_name not in ("mod.rs", "lib.rs"):
                        continue
                    file_path = os.path.join(root, file_name)
                    relative = os.path.relpath(
                        file_path,
                        os.path.join(rust_path, "src"),
                    )
                    with open(file_path, "rb") as module_file:
                        module_files[relative] = module_file.read()
            outputs.append(module_files)
        self.assertEqual(outputs[0], outputs[1])
    
    def run_convert_to_rust(
        self,
        name: str,
        avro_annotation: bool = False,
        serde_annotation: bool = False,
        xml_annotation: bool = False,
    ):
        """ Test converting an avsc file to Rust and compiling/testing it """
        cwd = os.getcwd()        
        avro_path = os.path.join(cwd, "test", "avsc", f"{name}.avsc")
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            f"{name}-rs"
            f"{'' if not avro_annotation else '-avro'}"
            f"{'' if not serde_annotation else '-serde'}"
            f"{'' if not xml_annotation else '-xml'}",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)        
        convert_avro_to_rust(
            avro_path,
            rust_path,
            package_name=name,
            avro_annotation=avro_annotation,
            serde_annotation=serde_annotation,
            xml_annotation=xml_annotation,
        )
        assert subprocess.check_call(
            ['cargo', 'test'], cwd=rust_path, stdout=sys.stdout, stderr=sys.stderr, timeout=self.CARGO_TIMEOUT) == 0
        return rust_path

    def assert_module_scoped_schemas(
        self,
        rust_path: str,
        expected_modules: set[str] | None = None,
        expected_count: int | None = None,
        required_modules: set[str] | None = None,
    ):
        """Assert each expected generated module owns one module-scoped SCHEMA."""
        schema_modules = set()
        for root, _, files in os.walk(os.path.join(rust_path, "src")):
            for file_name in files:
                if not file_name.endswith(".rs"):
                    continue
                file_path = os.path.join(root, file_name)
                with open(file_path, "r", encoding="utf-8") as generated_file:
                    source = generated_file.read()
                if "pub static ref SCHEMA" not in source:
                    continue
                relative_path = os.path.relpath(file_path, os.path.join(rust_path, "src"))
                schema_modules.add(relative_path.replace(os.sep, "/"))
                self.assertEqual(1, source.count("pub static ref SCHEMA"))
                self.assertIn("\nlazy_static! {\n", source)
        if expected_modules is not None:
            self.assertEqual(expected_modules, schema_modules)
        if expected_count is not None:
            self.assertEqual(expected_count, len(schema_modules))
        if required_modules is not None:
            self.assertTrue(required_modules <= schema_modules)
        return schema_modules
        
    def test_convert_address_avsc_to_rust(self):
        """ Test converting an address.avsc file to Rust """
        self.run_convert_to_rust("address", True, True)
        self.run_convert_to_rust("address", True, False)
        self.run_convert_to_rust("address", False, True)
        self.run_convert_to_rust("address", False, False)

    def test_convert_root_types_to_rust(self):
        """Compile root records and enums in every annotation mode."""
        for avro_annotation, serde_annotation in (
            (False, False),
            (False, True),
            (True, False),
            (True, True),
        ):
            rust_path = self.run_convert_to_rust(
                "rust-root-types",
                avro_annotation,
                serde_annotation,
            )
            with open(
                os.path.join(rust_path, "src", "lib.rs"),
                "r",
                encoding="utf-8",
            ) as lib_file:
                source = lib_file.read()
            self.assertIn("pub mod rootrecord;", source)
            self.assertIn("pub mod rootenum;", source)
            if avro_annotation:
                self.assertIn("pub mod choiceunion;", source)
                integration_dir = os.path.join(rust_path, "tests")
                os.makedirs(integration_dir, exist_ok=True)
                with open(
                    os.path.join(
                        integration_dir,
                        "legacy_root_union_api.rs",
                    ),
                    "w",
                    encoding="utf-8",
                ) as legacy_test:
                    legacy_test.write(
                        "use rust_root_types::{\n"
                        "    choiceunion::ChoiceUnion,\n"
                        "    rootrecord::RootRecord,\n"
                        "};\n\n"
                        "#[test]\n"
                        "fn legacy_root_union_api_compiles() {\n"
                        "    let record = RootRecord {\n"
                        "        choice: ChoiceUnion::String("
                        "\"value\".into()),\n"
                        "        ..Default::default()\n"
                        "    };\n"
                        "    assert!(matches!("
                        "record.choice, ChoiceUnion::String(_)));\n"
                        "}\n"
                    )
                assert subprocess.check_call(
                    ['cargo', 'test'],
                    cwd=rust_path,
                    stdout=sys.stdout,
                    stderr=sys.stderr,
                    timeout=self.CARGO_TIMEOUT,
                ) == 0
                convert_avro_to_rust(
                    os.path.join(
                        os.getcwd(),
                        "test",
                        "avsc",
                        "rust-root-types.avsc",
                    ),
                    rust_path,
                    package_name="rust-root-types",
                    avro_annotation=True,
                    serde_annotation=serde_annotation,
                )
                with open(
                    os.path.join(rust_path, "src", "lib.rs"),
                    "r",
                    encoding="utf-8",
                ) as regenerated_lib:
                    self.assertIn(
                        "pub mod choiceunion;",
                        regenerated_lib.read(),
                    )
        
    def test_convert_twotypeunion_avsc_to_rust(self):
        """ Test converting an twotypeunion.avsc file to Rust """
        self.run_convert_to_rust("twotypeunion", True, True)
        self.run_convert_to_rust("twotypeunion", True, False)
        self.run_convert_to_rust("twotypeunion", False, True)
        self.run_convert_to_rust("twotypeunion", False, False)
    
    def test_convert_typemapunion_avsc_to_rust(self):
        """ Test converting an twotypeunion.avsc file to Rust """
        # Skip avro_annotation=True combinations - apache-avro library has limitations
        # with maps that have union value types (returns "Can only encode value type Map as one of [Map]")
        self.run_convert_to_rust("typemapunion", False, True)
        self.run_convert_to_rust("typemapunion", False, False)
    

    def test_convert_telemetry_avsc_to_rust(self):
        """ Test converting a telemetry.avsc file to Rust """
        self.run_convert_to_rust("telemetry", True, True)
        self.run_convert_to_rust("telemetry", True, False)
        self.run_convert_to_rust("telemetry", False, True)
        self.run_convert_to_rust("telemetry", False, False)

    def test_convert_enum_avro_annotations_to_rust(self):
        """Compile an enum-only crate with module-scoped Avro schema support."""
        rust_path = self.run_convert_to_rust("rust-enum-annotation", True, True)
        self.assert_module_scoped_schemas(
            rust_path,
            {"issue406/enum_only/status.rs"},
        )
        avro_only_path = self.run_convert_to_rust("rust-enum-annotation", True, False)
        self.assert_module_scoped_schemas(
            avro_only_path,
            {"issue406/enum_only/status.rs"},
        )

    def test_convert_union_avro_annotations_to_rust(self):
        """Compile a union-bearing crate with Avro binary round-trip coverage."""
        rust_path = self.run_convert_to_rust("rust-union-annotation", True, True)
        self.assert_module_scoped_schemas(
            rust_path,
            expected_count=17,
            required_modules={
                "issue406/union_only/nestedholder.rs",
                "issue406/union_only/unionholder.rs",
            },
        )
        union_holder_path = os.path.join(
            rust_path,
            "src",
            "issue406",
            "union_only",
            "unionholder.rs",
        )
        with open(union_holder_path, "r", encoding="utf-8") as generated_file:
            avro_source = generated_file.read()
        self.assertRegex(
            avro_source,
            r"pub nullable: crate::issue406::union_only::"
            r"union[a-z0-9]+::Union[A-Za-z0-9]+,",
        )
        self.assertIn(
            "pub optional_array: Vec<String>",
            avro_source,
        )
        self.assertIn(
            "pub optional_map: std::collections::HashMap<String, String>",
            avro_source,
        )
        self.assertRegex(
            avro_source,
            r"pub optional_inline_record: crate::issue406::union_only::"
            r"optionalinlinerecord::OptionalInlineRecord",
        )
        self.assertIn(
            "pub optional_fixed: Vec<u8>",
            avro_source,
        )
        legacy_alias_path = os.path.join(
            rust_path,
            "src",
            "issue406",
            "union_only",
            "nullableunion.rs",
        )
        self.assertTrue(os.path.exists(legacy_alias_path))
        integration_dir = os.path.join(rust_path, "tests")
        os.makedirs(integration_dir, exist_ok=True)
        with open(
            os.path.join(integration_dir, "legacy_union_api.rs"),
            "w",
            encoding="utf-8",
        ) as legacy_test:
            legacy_test.write(
                "use rust_union_annotation::issue406::union_only::{\n"
                "    nullfirstoption1union::NullFirstoption1Union,\n"
                "    nulllastoption0union::NullLastoption0Union,\n"
                "    nullmiddleoption0union::NullMiddleoption0Union,\n"
                "    nullableunion::NullableUnion,\n"
                "    unionholder::UnionHolder,\n"
                "};\n\n"
                "#[test]\n"
                "fn legacy_avro_union_api_compiles() {\n"
                "    let holder = UnionHolder {\n"
                "        nullable: NullableUnion::String(\"value\".into()),\n"
                "        ..Default::default()\n"
                "    };\n"
                "    assert!(matches!(holder.nullable, NullableUnion::String(_)));\n"
                "    let _ = NullFirstoption1Union::default();\n"
                "    let _ = NullMiddleoption0Union::default();\n"
                "    let _ = NullLastoption0Union::default();\n"
                "}\n"
            )
        assert subprocess.check_call(
            ['cargo', 'test'],
            cwd=rust_path,
            stdout=sys.stdout,
            stderr=sys.stderr,
            timeout=self.CARGO_TIMEOUT,
        ) == 0

        legacy_field = (
            "pub nullable: "
            "crate::issue406::union_only::nullableunion::NullableUnion"
        )
        for serde_annotation in (False, True):
            compatible_path = self.run_convert_to_rust(
                "rust-union-annotation",
                False,
                serde_annotation,
            )
            compatible_holder_path = os.path.join(
                compatible_path,
                "src",
                "issue406",
                "union_only",
                "unionholder.rs",
            )
            with open(
                compatible_holder_path,
                "r",
                encoding="utf-8",
            ) as generated_file:
                compatible_source = generated_file.read()
            self.assertIn(legacy_field, compatible_source)
            self.assertNotIn("pub nullable: Option<", compatible_source)

    def test_convert_multitype_avro_annotations_to_rust(self):
        """Compile records, enums, and unions that each define SCHEMA in one crate."""
        rust_path = self.run_convert_to_rust("rust-multitype-annotations", True, True)
        schema_modules = self.assert_module_scoped_schemas(
            rust_path,
            expected_count=28,
            required_modules={
                "issue406/multitype/alternate.rs",
                "issue406/multitype/collisionone.rs",
                "issue406/multitype/collisiontwo.rs",
                "issue406/multitype/composite.rs",
                "issue406/multitype/foo.rs",
                "issue406/multitype/foobar.rs",
                "issue406/multitype/inlinekind.rs",
                "issue406/multitype/simple.rs",
                "issue406/multitype/standalone.rs",
                "issue406/multitype/syntheticpaths.rs",
                "issue406/multitype/twina.rs",
                "issue406/multitype/twinb.rs",
                "issue406/multitype/twinholder.rs",
                "issue406/multitype/wrapper.rs",
            },
        )
        union_modules = {
            module for module in schema_modules
            if os.path.basename(module).startswith("union")
        }
        self.assertEqual(14, len(union_modules))

    def test_convert_named_reference_resolution_to_rust(self):
        """Resolve dotted names and duplicate short names by namespace."""
        rust_path = self.run_convert_to_rust(
            "rust-named-reference-resolution",
            True,
            True,
        )
        holder_path = os.path.join(
            rust_path,
            "src",
            "right",
            "holder.rs",
        )
        with open(holder_path, "r", encoding="utf-8") as generated_file:
            source = generated_file.read()
        self.assertIn("pub local: crate::right::item::Item", source)
        self.assertIn("pub foreign: crate::left::item::Item", source)
        self.run_convert_to_rust(
            "rust-named-reference-resolution",
            True,
            True,
            True,
        )

    def test_convert_anyvalue_reference_to_rust_default(self):
        """Keep AnyValue mapped to serde_json::Value even when it is indexed."""
        rust_path = self.run_convert_to_rust(
            "rust-anyvalue-reference",
            False,
            False,
        )
        holder_path = os.path.join(
            rust_path,
            "src",
            "issue406",
            "anyvalue",
            "anyvalueholder.rs",
        )
        with open(holder_path, "r", encoding="utf-8") as generated_file:
            source = generated_file.read()
        self.assertIn("pub payload: serde_json::Value", source)
        self.assertNotIn("crate::avrotize::anyvalue::AnyValue", source)

    def test_convert_generic_anyvalue_union_to_rust(self):
        """Compile Avro decoding for the generic AnyValue union."""
        for serde_annotation in (True, False):
            rust_path = os.path.join(
                tempfile.gettempdir(),
                "avrotize",
                "rust-generic-anyvalue-rs-avro"
                f"{'-serde' if serde_annotation else ''}",
            )
            if os.path.exists(rust_path):
                shutil.rmtree(rust_path, ignore_errors=True)
            schema = {
                "type": "record",
                "name": "GenericHolder",
                "namespace": "issue406.generic",
                "fields": [
                    {
                        "name": "payload",
                        "type": generic_type(),
                    }
                ],
            }
            convert_avro_schema_to_rust(
                schema,
                rust_path,
                package_name="rust-generic-anyvalue",
                avro_annotation=True,
                serde_annotation=serde_annotation,
            )
            assert subprocess.check_call(
                ['cargo', 'test'],
                cwd=rust_path,
                stdout=sys.stdout,
                stderr=sys.stderr,
                timeout=self.CARGO_TIMEOUT,
            ) == 0

    def test_convert_jfrog_pipelines_jsons_to_avro_to_rust(self):
        """ Test converting a jfrog-pipelines.json file to Rust """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        rust_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-rs")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_rust(avro_path, rust_path, package_name="jfrog_pipelines")
        assert subprocess.check_call(
            ['cargo', 'test'], cwd=rust_path, stdout=sys.stdout, stderr=sys.stderr, timeout=self.CARGO_TIMEOUT) == 0

    @pytest.mark.skip(reason="jfrog-pipelines has deeply nested structurally identical union variants (Auto1/Auto2) that cannot round-trip with serde untagged unions")
    def test_convert_jfrog_pipelines_jsons_to_avro_to_rust_typed_json(self):
        """ Test converting a jfrog-pipelines.json file to Rust """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        rust_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-rs-typed-json")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_rust(avro_path, rust_path, package_name="jfrog_pipelines", serde_annotation=True)
        assert subprocess.check_call(
            ['cargo', 'test'], cwd=rust_path, stdout=sys.stdout, stderr=sys.stderr, timeout=self.CARGO_TIMEOUT) == 0

    @pytest.mark.skip(reason="jfrog-pipelines has deeply nested structurally identical union variants (Auto1/Auto2) that cannot round-trip with serde untagged unions")
    def test_convert_jfrog_pipelines_jsons_to_avro_to_rust_avro_annotations(self):
        """ Test converting a jfrog-pipelines.json file to Rust """
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        rust_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-rs-avro")
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        os.makedirs(rust_path, exist_ok=True)
            
        
        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_rust(avro_path, rust_path, package_name="jfrog_pipelines", avro_annotation=True)
        assert subprocess.check_call(
            ['cargo', 'test'], cwd=rust_path, stdout=sys.stdout, stderr=sys.stderr, timeout=self.CARGO_TIMEOUT) == 0
