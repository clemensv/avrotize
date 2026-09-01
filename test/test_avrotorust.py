import glob
import os
import re
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
    JsonSignature,
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
        self.assertEqual(2, signature_a.node_count)
        self.assertEqual(2, signature_a.edge_count)
        self.assertEqual(signature_a, signature_b)
        self.assertEqual(hash(signature_a), hash(signature_b))

    def test_recursive_union_shape_signature_is_bounded(self):
        """Keep recursive union edges as graph references during construction."""
        converter = AvroToRust()
        node = {
            "type": "record",
            "name": "Node",
            "namespace": "issue484.recursive_union",
            "fields": [
                {
                    "name": "children",
                    "type": {
                        "type": "array",
                        "items": ["null", "Node"],
                    },
                }
            ],
        }
        converter.index_avro_named_types(node)

        signature = converter.get_json_shape_signature(
            "Node",
            "issue484.recursive_union",
        )
        self.assertEqual(2, signature.node_count)
        self.assertEqual(2, signature.edge_count)
        self.assertLess(len(repr(signature)), 256)

    def test_nullable_record_signatures_model_realizable_shapes(self):
        """Model absent, null, and present nullable record field shapes."""
        converter = AvroToRust()
        optional_record = {
            "type": "record",
            "name": "OptionalString",
            "namespace": "issue484.shapes",
            "fields": [
                {
                    "name": "x",
                    "type": ["null", "string"],
                    "default": None,
                }
            ],
        }
        required_record = {
            "type": "record",
            "name": "RequiredString",
            "namespace": "issue484.shapes",
            "fields": [{"name": "x", "type": "string"}],
        }
        converter.index_avro_named_types([
            optional_record,
            required_record,
        ])

        optional_match = converter.get_json_match_signature(
            optional_record,
            "issue484.shapes",
        )
        optional_shape = converter.get_json_shape_signature(
            optional_record,
            "issue484.shapes",
        )
        required_match = converter.get_json_match_signature(
            required_record,
            "issue484.shapes",
        )
        required_shape = converter.get_json_shape_signature(
            required_record,
            "issue484.shapes",
        )
        absent_shape = ("record", ())
        null_shape = ("record", (("x", "null"),))

        self.assertTrue(converter.json_match_accepts_shape(
            optional_match,
            absent_shape,
        ))
        self.assertTrue(converter.json_match_accepts_shape(
            optional_match,
            null_shape,
        ))
        self.assertTrue(converter.json_match_accepts_shape(
            optional_match,
            required_shape,
        ))
        self.assertTrue(converter.json_match_accepts_shape(
            required_match,
            optional_shape,
        ))
        self.assertFalse(converter.json_match_accepts_shape(
            required_match,
            absent_shape,
        ))
        self.assertFalse(converter.json_match_accepts_shape(
            required_match,
            null_shape,
        ))

    def test_optional_record_union_overlap_is_order_independent(self):
        """Detect optional overlaps, including nested records, in either order."""
        converter = AvroToRust()
        records = [
            {
                "type": "record",
                "name": "OptionalString",
                "namespace": "issue484.overlap",
                "fields": [
                    {
                        "name": "x",
                        "type": ["null", "string"],
                        "default": None,
                    }
                ],
            },
            {
                "type": "record",
                "name": "RequiredString",
                "namespace": "issue484.overlap",
                "fields": [{"name": "x", "type": "string"}],
            },
            {
                "type": "record",
                "name": "RequiredInteger",
                "namespace": "issue484.overlap",
                "fields": [{"name": "x", "type": "long"}],
            },
            {
                "type": "record",
                "name": "RequiredNull",
                "namespace": "issue484.overlap",
                "fields": [{"name": "x", "type": "null"}],
            },
            {
                "type": "record",
                "name": "Empty",
                "namespace": "issue484.overlap",
                "fields": [],
            },
            {
                "type": "record",
                "name": "NestedOptionalString",
                "namespace": "issue484.overlap",
                "fields": [
                    {
                        "name": "value",
                        "type": ["null", "string"],
                        "default": None,
                    }
                ],
            },
            {
                "type": "record",
                "name": "NestedRequiredString",
                "namespace": "issue484.overlap",
                "fields": [{"name": "value", "type": "string"}],
            },
            {
                "type": "record",
                "name": "OptionalOuter",
                "namespace": "issue484.overlap",
                "fields": [
                    {
                        "name": "nested",
                        "type": "NestedOptionalString",
                    }
                ],
            },
            {
                "type": "record",
                "name": "RequiredOuter",
                "namespace": "issue484.overlap",
                "fields": [
                    {
                        "name": "nested",
                        "type": "NestedRequiredString",
                    }
                ],
            },
        ]
        converter.index_avro_named_types(records)

        for branches in (
            ["OptionalString", "RequiredString"],
            ["RequiredString", "OptionalString"],
            ["OptionalString", "RequiredNull"],
            ["RequiredNull", "OptionalString"],
            ["OptionalString", "Empty"],
            ["Empty", "OptionalString"],
            ["OptionalOuter", "RequiredOuter"],
            ["RequiredOuter", "OptionalOuter"],
        ):
            self.assertFalse(converter.is_json_round_trip_safe(
                branches,
                "issue484.overlap",
            ))

        for branches in (
            ["OptionalString", "RequiredInteger"],
            ["RequiredInteger", "OptionalString"],
        ):
            self.assertTrue(converter.is_json_round_trip_safe(
                branches,
                "issue484.overlap",
            ))

    def test_named_references_preserve_nested_union_ambiguity(self):
        """Propagate nested ambiguity through named record references."""
        converter = AvroToRust()
        optional_record = {
            "type": "record",
            "name": "OptionalString",
            "namespace": "issue484.named",
            "fields": [
                {
                    "name": "x",
                    "type": ["null", "string"],
                    "default": None,
                }
            ],
        }
        required_record = {
            "type": "record",
            "name": "RequiredString",
            "namespace": "issue484.named",
            "fields": [{"name": "x", "type": "string"}],
        }
        inner = {
            "type": "record",
            "name": "Inner",
            "namespace": "issue484.named",
            "fields": [
                {
                    "name": "choice",
                    "type": ["OptionalString", "RequiredString"],
                }
            ],
        }
        outer = {
            "type": "record",
            "name": "Outer",
            "namespace": "issue484.named",
            "fields": [{"name": "inner", "type": "Inner"}],
        }
        converter.index_avro_named_types([
            optional_record,
            required_record,
            inner,
            outer,
        ])

        self.assertFalse(converter.is_json_round_trip_safe(
            inner,
            "issue484.named",
        ))
        self.assertFalse(converter.is_json_round_trip_safe(
            "Inner",
            "issue484.named",
        ))
        self.assertFalse(converter.is_json_round_trip_safe(
            outer,
            "issue484.named",
        ))

    def test_optional_record_matchers_overlap_non_record_json_kinds(self):
        """Mirror serde_json indexing for all-nullable record predicates."""
        converter = AvroToRust()
        optional_record = {
            "type": "record",
            "name": "OptionalString",
            "namespace": "issue484.cross_kind",
            "fields": [
                {
                    "name": "x",
                    "type": ["null", "string"],
                    "default": None,
                }
            ],
        }
        converter.index_avro_named_types(optional_record)
        optional_match = converter.get_json_match_signature(
            optional_record,
            "issue484.cross_kind",
        )

        for shape in ("string", ("array", "string"), "null"):
            self.assertFalse(converter.json_match_accepts_shape(
                optional_match,
                shape,
            ))
        self.assertTrue(converter.json_match_accepts_shape(
            optional_match,
            ("map", "string"),
        ))
        required_record = {
            "type": "record",
            "name": "RequiredString",
            "namespace": "issue484.cross_kind",
            "fields": [{"name": "x", "type": "string"}],
        }
        converter.index_avro_named_types(required_record)
        required_match = converter.get_json_match_signature(
            required_record,
            "issue484.cross_kind",
        )
        self.assertFalse(converter.json_match_accepts_shape(
            required_match,
            "string",
        ))

    def test_nullable_named_record_uses_generated_predicate_semantics(self):
        """Do not add null to a named record emitted as a bare Rust field."""
        converter = AvroToRust()
        child = {
            "type": "record",
            "name": "Child",
            "namespace": "issue484.named_nullable",
            "fields": [{"name": "value", "type": "string"}],
        }
        named_container = {
            "type": "record",
            "name": "NamedContainer",
            "namespace": "issue484.named_nullable",
            "fields": [
                {
                    "name": "child",
                    "type": ["null", "Child"],
                    "default": None,
                }
            ],
        }
        optional_container = {
            "type": "record",
            "name": "OptionalStringContainer",
            "namespace": "issue484.named_nullable",
            "fields": [
                {
                    "name": "child",
                    "type": ["null", "string"],
                    "default": None,
                }
            ],
        }
        converter.index_avro_named_types([
            child,
            named_container,
            optional_container,
        ])

        named_match = converter.get_json_match_signature(
            named_container,
            "issue484.named_nullable",
        )
        named_shape = converter.get_json_shape_signature(
            named_container,
            "issue484.named_nullable",
        )
        optional_match = converter.get_json_match_signature(
            optional_container,
            "issue484.named_nullable",
        )
        optional_shape = converter.get_json_shape_signature(
            optional_container,
            "issue484.named_nullable",
        )
        self.assertFalse(converter.json_match_accepts_shape(
            named_match,
            optional_shape,
        ))
        self.assertFalse(converter.json_match_accepts_shape(
            optional_match,
            named_shape,
        ))

    def test_json_shape_overlap_comparison_is_memoized(self):
        """Keep nested union comparison work bounded by unique signature pairs."""
        match_signature = "string"
        shape_signature = "boolean"
        for _ in range(11):
            shape_signature = (
                "union",
                (shape_signature,) * 4,
            )

        original = AvroToRust.json_match_accepts_shape
        comparison_count = 0

        def counted(match, shape, memo=None):
            nonlocal comparison_count
            comparison_count += 1
            if memo is None:
                return original(match, shape)
            return original(match, shape, memo)

        with patch.object(
            AvroToRust,
            "json_match_accepts_shape",
            side_effect=counted,
        ):
            self.assertFalse(AvroToRust.json_match_accepts_shape(
                match_signature,
                shape_signature,
            ))
        self.assertLess(comparison_count, 200)

    def test_named_json_safety_traversal_is_memoized(self):
        """Evaluate repeated named-reference DAG edges once per conversion."""
        converter = AvroToRust()
        records = [
            {
                "type": "record",
                "name": "Node0",
                "namespace": "issue484.safety_dag",
                "fields": [{"name": "value", "type": "string"}],
            }
        ]
        for index in range(1, 18):
            records.append({
                "type": "record",
                "name": f"Node{index}",
                "namespace": "issue484.safety_dag",
                "fields": [
                    {"name": "left", "type": f"Node{index - 1}"},
                    {"name": "right", "type": f"Node{index - 1}"},
                ],
            })
        converter.index_avro_named_types(records)

        with patch.object(
            converter,
            "resolve_avro_named_type",
            wraps=converter.resolve_avro_named_type,
        ) as resolve:
            self.assertTrue(converter.is_json_round_trip_safe(
                "Node17",
                "issue484.safety_dag",
            ))
        self.assertLess(resolve.call_count, 100)

    def test_json_signature_construction_is_structurally_bounded(self):
        """Build each signature graph once per named node and schema edge."""
        converter = AvroToRust()
        records = [
            {
                "type": "record",
                "name": "Node0",
                "namespace": "issue484.signature_dag",
                "fields": [{"name": "value", "type": "string"}],
            }
        ]
        for index in range(1, 19):
            records.append({
                "type": "record",
                "name": f"Node{index}",
                "namespace": "issue484.signature_dag",
                "fields": [
                    {"name": "left", "type": f"Node{index - 1}"},
                    {"name": "right", "type": f"Node{index - 1}"},
                ],
            })
        converter.index_avro_named_types(records)

        signatures = (
            converter.get_json_match_signature(
                "Node18",
                "issue484.signature_dag",
            ),
            converter.get_json_shape_signature(
                "Node18",
                "issue484.signature_dag",
            ),
            converter.get_json_default_shape_signature(
                "Node18",
                "issue484.signature_dag",
            ),
        )
        for signature in signatures:
            self.assertEqual(20, signature.node_count)
            self.assertEqual(37, signature.edge_count)
            self.assertLess(len(repr(signature)), 4096)

    def test_deep_json_signature_construction_is_iterative(self):
        """Build and canonicalize signatures beyond Python's recursion limit."""
        converter = AvroToRust()
        records = [
            {
                "type": "record",
                "name": "Node0000",
                "namespace": "issue484.deep_signature",
                "fields": [{"name": "value", "type": "string"}],
            }
        ]
        for index in range(1, 1101):
            records.append({
                "type": "record",
                "name": f"Node{index:04d}",
                "namespace": "issue484.deep_signature",
                "fields": [{
                    "name": "next",
                    "type": f"Node{index - 1:04d}",
                }],
            })
        converter.index_avro_named_types(records)

        for builder in (
            converter.get_json_match_signature,
            converter.get_json_shape_signature,
            converter.get_json_default_shape_signature,
        ):
            signature = builder(
                "Node1100",
                "issue484.deep_signature",
            )
            self.assertEqual(1102, signature.node_count)
            self.assertEqual(1101, signature.edge_count)
            self.assertEqual(
                signature.node_count,
                signature.canonical_visit_count,
            )
            self.assertLess(len(repr(signature)), 50000)

    def test_cyclic_signature_refinement_is_predecessor_driven(self):
        """Refine a deep SCC by visiting predecessor edges, not whole depths."""
        node_count = 4096
        nodes = [
            (
                "record",
                ((
                    "marker" if index == 0 else "next",
                    (index + 1) % node_count,
                ),),
            )
            for index in range(node_count)
        ]

        signature = JsonSignature(0, nodes)

        self.assertEqual(node_count, signature.canonical_visit_count)
        self.assertLess(
            signature.canonical_refinement_count,
            6 * signature.edge_count,
        )

    def test_anonymous_cyclic_union_signatures_are_bounded(self):
        """Memoize anonymous union identity before structural inspection."""
        recursive_union = []
        recursive_union.extend(["string", recursive_union])
        converter = AvroToRust()

        for builder in (
            converter.get_json_match_signature,
            converter.get_json_shape_signature,
            converter.get_json_default_shape_signature,
        ):
            signature = builder(recursive_union, "")
            self.assertLessEqual(signature.node_count, 2)
            self.assertLessEqual(signature.edge_count, 2)
            self.assertLess(len(repr(signature)), 128)

    def test_named_json_safety_worklist_is_linear_on_reverse_chain(self):
        """Re-evaluate only named dependents whose safety can change."""
        converter = AvroToRust()
        schemas = [
            {
                "type": "enum",
                "name": "Tag",
                "namespace": "issue484.safety_chain",
                "symbols": ["ONE", "TWO"],
            },
            {
                "type": "record",
                "name": "Node0000",
                "namespace": "issue484.safety_chain",
                "fields": [{
                    "name": "choice",
                    "type": ["string", "Tag"],
                }],
            },
        ]
        for index in range(1, 2001):
            schemas.append({
                "type": "record",
                "name": f"Node{index:04d}",
                "namespace": "issue484.safety_chain",
                "fields": [{
                    "name": "next",
                    "type": f"Node{index - 1:04d}",
                }],
            })
        converter.index_avro_named_types(schemas)

        with patch.object(
            converter,
            "evaluate_json_round_trip_safe",
            wraps=converter.evaluate_json_round_trip_safe,
        ) as evaluate:
            self.assertFalse(converter.is_json_round_trip_safe(
                "Node2000",
                "issue484.safety_chain",
            ))
        self.assertLess(evaluate.call_count, 5000)

    def test_named_json_safety_handles_namespaced_sccs(self):
        """Propagate unsafe SCCs without conflating duplicate short names."""
        converter = AvroToRust()
        schemas = [
            {
                "type": "record",
                "name": "Optional",
                "namespace": "issue484.unsafe",
                "fields": [{
                    "name": "x",
                    "type": ["null", "string"],
                    "default": None,
                }],
            },
            {
                "type": "record",
                "name": "Required",
                "namespace": "issue484.unsafe",
                "fields": [{"name": "x", "type": "string"}],
            },
            {
                "type": "record",
                "name": "Node",
                "namespace": "issue484.safe",
                "fields": [{"name": "next", "type": "Peer"}],
            },
            {
                "type": "record",
                "name": "Peer",
                "namespace": "issue484.safe",
                "fields": [{"name": "next", "type": "Node"}],
            },
            {
                "type": "record",
                "name": "Node",
                "namespace": "issue484.unsafe",
                "fields": [{"name": "next", "type": "Peer"}],
            },
            {
                "type": "record",
                "name": "Peer",
                "namespace": "issue484.unsafe",
                "fields": [
                    {"name": "next", "type": "Node"},
                    {
                        "name": "choice",
                        "type": ["Optional", "Required"],
                    },
                ],
            },
        ]
        converter.index_avro_named_types(schemas)

        self.assertTrue(converter.is_json_round_trip_safe(
            "Node",
            "issue484.safe",
        ))
        self.assertTrue(converter.is_json_round_trip_safe(
            "Peer",
            "issue484.safe",
        ))
        self.assertFalse(converter.is_json_round_trip_safe(
            "Node",
            "issue484.unsafe",
        ))
        self.assertFalse(converter.is_json_round_trip_safe(
            "Peer",
            "issue484.unsafe",
        ))

    def test_recursive_union_overlap_requires_a_concrete_match(self):
        """Do not infer overlap solely by revisiting a recursive pair."""
        converter = AvroToRust()
        schemas = [
            {
                "type": "record",
                "name": "A",
                "namespace": "issue484.recursive_disjoint",
                "fields": [{
                    "name": "value",
                    "type": ["A", "string"],
                }],
            },
            {
                "type": "record",
                "name": "B",
                "namespace": "issue484.recursive_disjoint",
                "fields": [{
                    "name": "value",
                    "type": ["B", "long"],
                }],
            },
        ]
        converter.index_avro_named_types(schemas)

        self.assertFalse(converter.json_match_accepts_shape(
            converter.get_json_match_signature(
                "A",
                "issue484.recursive_disjoint",
            ),
            converter.get_json_shape_signature(
                "B",
                "issue484.recursive_disjoint",
            ),
        ))
        self.assertTrue(converter.is_json_round_trip_safe(
            ["A", "B"],
            "issue484.recursive_disjoint",
        ))

    def test_json_signature_hashing_is_structurally_bounded(self):
        """Avoid quadratic equality scans when signatures enter sets."""
        converter = AvroToRust()
        signatures = [
            converter.get_json_match_signature(
                {
                    "type": "record",
                    "name": f"Record{index}",
                    "namespace": "issue484.signature_hash",
                    "fields": [{
                        "name": f"value{index}",
                        "type": "string",
                    }],
                },
                "issue484.signature_hash",
            )
            for index in range(1000)
        ]
        original = JsonSignature.__eq__
        comparison_count = 0

        def counted(left, right):
            nonlocal comparison_count
            comparison_count += 1
            return original(left, right)

        with patch.object(JsonSignature, "__eq__", counted):
            self.assertEqual(1000, len(set(signatures)))
        self.assertLess(comparison_count, 100)

    def test_json_signature_equality_ignores_incidental_sharing(self):
        """Canonicalize shared and duplicated equivalent named subgraphs."""
        converter = AvroToRust()
        schemas = [
            {
                "type": "record",
                "name": "Leaf",
                "namespace": "issue484.sharing",
                "fields": [{"name": "value", "type": "string"}],
            },
            {
                "type": "record",
                "name": "Leaf1",
                "namespace": "issue484.sharing",
                "fields": [{"name": "value", "type": "string"}],
            },
            {
                "type": "record",
                "name": "Leaf2",
                "namespace": "issue484.sharing",
                "fields": [{"name": "value", "type": "string"}],
            },
            {
                "type": "record",
                "name": "Shared",
                "namespace": "issue484.sharing",
                "fields": [
                    {"name": "left", "type": "Leaf"},
                    {"name": "right", "type": "Leaf"},
                ],
            },
            {
                "type": "record",
                "name": "Duplicated",
                "namespace": "issue484.sharing",
                "fields": [
                    {"name": "left", "type": "Leaf1"},
                    {"name": "right", "type": "Leaf2"},
                ],
            },
        ]
        converter.index_avro_named_types(schemas)

        shared = converter.get_json_match_signature(
            "Shared",
            "issue484.sharing",
        )
        duplicated = converter.get_json_match_signature(
            "Duplicated",
            "issue484.sharing",
        )
        self.assertEqual(shared, duplicated)
        self.assertEqual(hash(shared), hash(duplicated))

    def test_json_signature_equality_merges_equivalent_recursive_sccs(self):
        """Canonicalize shared and duplicated recursive named subgraphs."""
        converter = AvroToRust()

        def recursive_leaf(name):
            return {
                "type": "record",
                "name": name,
                "namespace": "issue484.cyclic_sharing",
                "fields": [{
                    "name": "children",
                    "type": {"type": "array", "items": name},
                }],
            }

        schemas = [
            recursive_leaf("Leaf"),
            recursive_leaf("Leaf1"),
            recursive_leaf("Leaf2"),
            {
                "type": "record",
                "name": "Shared",
                "namespace": "issue484.cyclic_sharing",
                "fields": [
                    {"name": "left", "type": "Leaf"},
                    {"name": "right", "type": "Leaf"},
                ],
            },
            {
                "type": "record",
                "name": "Duplicated",
                "namespace": "issue484.cyclic_sharing",
                "fields": [
                    {"name": "left", "type": "Leaf1"},
                    {"name": "right", "type": "Leaf2"},
                ],
            },
        ]
        converter.index_avro_named_types(schemas)

        shared = converter.get_json_shape_signature(
            "Shared",
            "issue484.cyclic_sharing",
        )
        duplicated = converter.get_json_shape_signature(
            "Duplicated",
            "issue484.cyclic_sharing",
        )
        self.assertEqual(shared, duplicated)
        self.assertEqual(hash(shared), hash(duplicated))

    def test_json_signature_equality_preserves_scc_boundaries(self):
        """Do not fold an acyclic wrapper into its recursive child SCC."""
        wrapped_cycle = JsonSignature(0, [
            ("record", (("left", 1), ("right", 1))),
            ("record", (("left", 2), ("right", 2))),
            ("record", (("left", 1), ("right", 1))),
        ])
        self_recursive = JsonSignature(0, [
            ("record", (("left", 0), ("right", 0))),
        ])

        self.assertNotEqual(wrapped_cycle, self_recursive)

    def test_decimal_fixed_signatures_follow_generated_f64(self):
        """Model decimal fixed and bytes by their generated Rust number shape."""
        converter = AvroToRust()
        decimal_fixed = {
            "type": "fixed",
            "name": "Amount",
            "namespace": "issue484.decimal",
            "size": 8,
            "logicalType": "decimal",
            "precision": 12,
            "scale": 2,
        }
        converter.index_avro_named_types(decimal_fixed)

        for builder in (
            converter.get_json_match_signature,
            converter.get_json_shape_signature,
            converter.get_json_default_shape_signature,
        ):
            self.assertEqual(
                builder("Amount", "issue484.decimal"),
                builder("double", "issue484.decimal"),
            )
        self.assertFalse(converter.is_json_round_trip_safe(
            ["Amount", "double"],
            "issue484.decimal",
        ))

    def test_json_match_signatures_include_xml_serde_aliases(self):
        """Model aliases accepted by combined JSON and XML generated types."""
        converter = AvroToRust()
        converter.serde_annotation = True
        converter.xml_annotation = True
        schemas = [
            {
                "type": "enum",
                "name": "First",
                "namespace": "issue484.aliases",
                "symbols": ["ALPHA"],
                "altenums": {"xml": {"ALPHA": "BETA"}},
            },
            {
                "type": "enum",
                "name": "Second",
                "namespace": "issue484.aliases",
                "symbols": ["BETA"],
                "altenums": {"xml": {"BETA": "GAMMA"}},
            },
            {
                "type": "record",
                "name": "Renamed",
                "namespace": "issue484.aliases",
                "fields": [{
                    "name": "original",
                    "type": "string",
                    "altnames": {"xml": "shared"},
                }],
            },
            {
                "type": "record",
                "name": "Original",
                "namespace": "issue484.aliases",
                "fields": [{
                    "name": "shared",
                    "type": "string",
                }],
            },
        ]
        converter.index_avro_named_types(schemas)

        self.assertTrue(converter.json_match_accepts_shape(
            converter.get_json_match_signature(
                "First",
                "issue484.aliases",
            ),
            converter.get_json_shape_signature(
                "Second",
                "issue484.aliases",
            ),
        ))
        self.assertTrue(converter.json_match_accepts_shape(
            converter.get_json_match_signature(
                "Renamed",
                "issue484.aliases",
            ),
            converter.get_json_shape_signature(
                "Original",
                "issue484.aliases",
            ),
        ))

    def test_json_union_subset_records_reject_ambiguity(self):
        """Reject a record branch also accepted by a subset-field matcher."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-json-subset-record-union",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        schema = [
            {
                "type": "record",
                "name": "Base",
                "namespace": "n",
                "fields": [{"name": "value", "type": "string"}],
            },
            {
                "type": "record",
                "name": "Extended",
                "namespace": "n",
                "fields": [
                    {"name": "value", "type": "string"},
                    {"name": "detail", "type": "string"},
                ],
            },
            {
                "type": "record",
                "name": "Holder",
                "namespace": "n",
                "fields": [
                    {"name": "choice", "type": ["Base", "Extended"]}
                ],
            },
        ]
        convert_avro_schema_to_rust(
            schema,
            rust_path,
            package_name="rust-json-subset-record-union",
            serde_annotation=True,
        )
        assert subprocess.check_call(
            ['cargo', 'test'],
            cwd=rust_path,
            stdout=sys.stdout,
            stderr=sys.stderr,
            timeout=self.CARGO_TIMEOUT,
        ) == 0

    def test_optional_record_union_rejects_generated_json_ambiguity(self):
        """Compile the issue 484 fixture and exercise fail-closed JSON matching."""
        rust_path = self.run_convert_to_rust(
            "rust-optional-record-union",
            serde_annotation=True,
        )
        union_files = glob.glob(os.path.join(
            rust_path,
            "src",
            "issue484",
            "optional_record_union",
            "unionpath*.rs",
        ))
        self.assertEqual(1, len(union_files))
        with open(union_files[0], "r", encoding="utf-8") as union_file:
            union_source = union_file.read()
        self.assertIn(
            "fn test_rejects_ambiguous_json_",
            union_source,
        )
        self.assertEqual(
            2,
            union_source.count(
                "Skip JSON round-trip: structurally identical to an earlier "
                "variant."
            ),
        )

    def test_union_candidate_matching_drops_probe_values(self):
        """Probe near-limit JSON and XML payloads with one live value at a time."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-union-candidate-memory",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        candidate_count = 16
        records = [
            {
                "type": "record",
                "name": f"Record{index}",
                "namespace": "issue484.candidate_memory",
                "fields": [
                    {"name": "blob", "type": "string"},
                    {"name": f"marker{index}", "type": "string"},
                ],
            }
            for index in range(candidate_count)
        ]
        records.append({
            "type": "record",
            "name": "Holder",
            "namespace": "issue484.candidate_memory",
            "fields": [{
                "name": "choice",
                "type": [
                    f"Record{index}"
                    for index in range(candidate_count)
                ],
            }],
        })
        convert_avro_schema_to_rust(
            records,
            rust_path,
            package_name="rust-union-candidate-memory",
            serde_annotation=True,
            xml_annotation=True,
        )
        union_files = glob.glob(os.path.join(
            rust_path,
            "src",
            "issue484",
            "candidate_memory",
            "unionpath*.rs",
        ))
        self.assertEqual(1, len(union_files))
        with open(union_files[0], encoding="utf-8") as generated_file:
            source = generated_file.read()
        union_type = re.search(
            r"pub enum (\w+)",
            source,
        ).group(1)
        self.assertNotIn("let candidate_", source)
        self.assertEqual(
            candidate_count,
            source.count("json_candidate_matches::<"),
        )
        self.assertEqual(
            candidate_count,
            source.count("&& xml_candidate_matches::<"),
        )
        self.assertEqual(
            2 * candidate_count,
            len(re.findall(r"\.map\(\w+::Record\d+\)", source)),
        )

        # Keep all probes eligible in this test-only crate so each failed
        # candidate allocates and then drops the same near-limit payload.
        source = re.sub(
            r"<[^>\n]*Record\d+>::is_xml_shape\(&shape_node\)",
            "true",
            source,
        )
        with open(
            union_files[0],
            "w",
            encoding="utf-8",
        ) as union_file:
            union_file.write(source)
            union_file.write(
                "\n\n#[cfg(test)]\n"
                "mod candidate_lifetime_regression {\n"
                "    use super::*;\n"
                "    use crate::issue484::candidate_memory::{\n"
                "        holder::Holder,\n"
                "        record0::Record0,\n"
                "    };\n\n"
                "#[test]\n"
                "    fn candidate_probe_values_are_sequential() {\n"
                "        let record = Record0 {\n"
                "            blob: \"x\".repeat(15 * 1024 * 1024),\n"
                "            marker0: \"selected\".into(),\n"
                "        };\n"
                f"        let value = {union_type}::Record0(record);\n\n"
                "        let xml = crate::xml_support::with_xml_union_probe(\n"
                "            || quick_xml::se::to_string(&Holder {\n"
                "                choice: value.clone(),\n"
                "            })\n"
                "        ).unwrap();\n"
                "        let json = serde_json::to_value(&value).unwrap();\n"
                "        reset_candidate_test_counters();\n"
                f"        let json_result: {union_type} =\n"
                "            serde_json::from_value(json).unwrap();\n"
                "        assert_eq!(value, json_result);\n"
                f"        assert_eq!(({candidate_count}, 0, 1, 0, 1), "
                "candidate_test_counts());\n\n"
                "        reset_candidate_test_counters();\n"
                "        let xml_result: Holder =\n"
                "            quick_xml::de::from_str(&xml).unwrap();\n"
                "        assert_eq!(value, xml_result.choice);\n"
                f"        assert_eq!((0, {candidate_count}, 0, 1, 1), "
                "candidate_test_counts());\n"
                "    }\n"
                "}\n"
            )
        assert subprocess.check_call(
            [
                'cargo',
                'test',
                '--lib',
                'candidate_probe_values_are_sequential',
            ],
            cwd=rust_path,
            stdout=sys.stdout,
            stderr=sys.stderr,
            timeout=self.CARGO_TIMEOUT,
        ) == 0

    def test_optional_record_union_edge_cases_compile(self):
        """Exercise generated direct, named, nested, and cross-kind unions."""
        self.run_convert_to_rust(
            "rust-optional-record-union-edge-cases",
            serde_annotation=True,
        )

    def test_nullable_named_record_union_xml_is_not_rejected(self):
        """Keep bare nullable named-record fields usable with XML annotations."""
        self.run_convert_to_rust(
            "rust-nullable-named-record-union",
            serde_annotation=True,
            xml_annotation=True,
        )

    def test_partial_record_xml_ambiguity_uses_concrete_value(self):
        """Allow A(None) but reject overlapping A(Some) in either union order."""
        rust_path = self.run_convert_to_rust(
            "rust-partial-xml-record-union",
            serde_annotation=True,
            xml_annotation=True,
        )
        integration_dir = os.path.join(rust_path, "tests")
        os.makedirs(integration_dir, exist_ok=True)
        with open(
            os.path.join(integration_dir, "partial_xml_ambiguity.rs"),
            "w",
            encoding="utf-8",
        ) as integration_test:
            integration_test.write(
                "use rust_partial_xml_record_union::issue484::partial_xml::{\n"
                "    a::A,\n"
                "    forwardunion::ForwardUnion,\n"
                "    reverseunion::ReverseUnion,\n"
                "};\n\n"
                "#[test]\n"
                "fn optional_record_xml_ambiguity_uses_the_value_shape() {\n"
                "    let forward_none = ForwardUnion::A(A { x: None });\n"
                "    let forward_xml = quick_xml::se::to_string("
                "&forward_none).unwrap();\n"
                "    let forward_round_trip: ForwardUnion = "
                "quick_xml::de::from_str(&forward_xml).unwrap();\n"
                "    assert_eq!(forward_none, forward_round_trip);\n"
                "    let reverse_none = ReverseUnion::A(A { x: None });\n"
                "    let reverse_xml = quick_xml::se::to_string("
                "&reverse_none).unwrap();\n"
                "    let reverse_round_trip: ReverseUnion = "
                "quick_xml::de::from_str(&reverse_xml).unwrap();\n"
                "    assert_eq!(reverse_none, reverse_round_trip);\n"
                "    let forward_some = ForwardUnion::A(A {\n"
                "        x: Some(\"overlap\".into()),\n"
                "    });\n"
                "    let forward_error = quick_xml::se::to_string("
                "&forward_some).unwrap_err();\n"
                "    assert!(forward_error.to_string().contains("
                "\"ambiguous XML union value\"));\n"
                "    let reverse_some = ReverseUnion::A(A {\n"
                "        x: Some(\"overlap\".into()),\n"
                "    });\n"
                "    let reverse_error = quick_xml::se::to_string("
                "&reverse_some).unwrap_err();\n"
                "    assert!(reverse_error.to_string().contains("
                "\"ambiguous XML union value\"));\n"
                "}\n"
            )
        assert subprocess.check_call(
            ['cargo', 'test', '--test', 'partial_xml_ambiguity'],
            cwd=rust_path,
            stdout=sys.stdout,
            stderr=sys.stderr,
            timeout=self.CARGO_TIMEOUT,
        ) == 0

    def test_xml_union_serialization_uses_normalized_variant_identity(self):
        """Reject XML values that normalize into a different union variant."""
        fixture_name = "rust-xml-union-lexical-normalization"
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            f"{fixture_name}-rs-serde-xml",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        convert_avro_to_rust(
            os.path.join(os.getcwd(), "test", "avsc", f"{fixture_name}.avsc"),
            rust_path,
            package_name=fixture_name,
            serde_annotation=True,
            xml_annotation=True,
        )
        integration_dir = os.path.join(rust_path, "tests")
        os.makedirs(integration_dir, exist_ok=True)
        with open(
            os.path.join(integration_dir, "xml_lexical_normalization.rs"),
            "w",
            encoding="utf-8",
        ) as integration_test:
            integration_test.write(
                "use rust_xml_union_lexical_normalization::issue484::xml_lexical::{\n"
                "    actualoverlapunion::ActualOverlapUnion,\n"
                "    alternateenumunion::AlternateEnumUnion,\n"
                "    attributefield::AttributeField,\n"
                "    attributefieldunion::AttributeFieldUnion,\n"
                "    attributeelementunion::AttributeElementUnion,\n"
                "    boolforwardunion::BoolForwardUnion,\n"
                "    boolreverseunion::BoolReverseUnion,\n"
                "    collectionholder::CollectionHolder,\n"
                "    collectionunion::CollectionUnion,\n"
                "    enumforwardunion::EnumForwardUnion,\n"
                "    enumreverseunion::EnumReverseUnion,\n"
                "    elementfield::ElementField,\n"
                "    innertext::InnerText,\n"
                "    innerunion::InnerUnion,\n"
                "    intlongforwardunion::IntLongForwardUnion,\n"
                "    intlongreverseunion::IntLongReverseUnion,\n"
                "    nestedchoice::NestedChoice,\n"
                "    nestedunion::NestedUnion,\n"
                "    narrowforwardunion::NarrowForwardUnion,\n"
                "    narrowrecordforwardunion::NarrowRecordForwardUnion,\n"
                "    narrowrecordreverseunion::NarrowRecordReverseUnion,\n"
                "    narrowreverseunion::NarrowReverseUnion,\n"
                "    numericforwardunion::NumericForwardUnion,\n"
                "    numericreverseunion::NumericReverseUnion,\n"
                "    optionalstring::OptionalString,\n"
                "    optionalstringforwardunion::OptionalStringForwardUnion,\n"
                "    optionalstringreverseunion::OptionalStringReverseUnion,\n"
                "    renamedfield::RenamedField,\n"
                "    renamedfieldunion::RenamedFieldUnion,\n"
                "    requiredlong::RequiredLong,\n"
                "    taga::TagA,\n"
                "    tagb::TagB,\n"
                "    textboolforwardunion::TextBoolForwardUnion,\n"
                "    textboolreverseunion::TextBoolReverseUnion,\n"
                "    wiretag::WireTag,\n"
                "};\n"
                "use serde::{Deserialize, Serialize};\n"
                "use std::collections::HashMap;\n\n"
                "#[derive(Deserialize, Serialize)]\n"
                "#[serde(rename = \"Root\")]\n"
                "struct Root<T> {\n"
                "    value: T,\n"
                "}\n\n"
                "fn assert_ambiguous<T: Serialize>(value: T) {\n"
                "    let error = quick_xml::se::to_string(&Root { value })\n"
                "        .unwrap_err();\n"
                "    assert!(error.to_string().contains(\"ambiguous XML union value\"));\n"
                "}\n\n"
                "#[test]\n"
                "fn normalized_lexical_values_preserve_selected_variant_identity() {\n"
                "    assert_ambiguous(NumericForwardUnion::OptionalString(\n"
                "        OptionalString { x: Some(\"42\".into()) },\n"
                "    ));\n"
                "    assert_ambiguous(NumericReverseUnion::OptionalString(\n"
                "        OptionalString { x: Some(\"-7\".into()) },\n"
                "    ));\n"
                "    assert_ambiguous(BoolForwardUnion::OptionalString(\n"
                "        OptionalString { x: Some(\"true\".into()) },\n"
                "    ));\n"
                "    assert_ambiguous(BoolReverseUnion::OptionalString(\n"
                "        OptionalString { x: Some(\"false\".into()) },\n"
                "    ));\n"
                "    assert_ambiguous(TextBoolForwardUnion::String(\"1\".into()));\n"
                "    assert_ambiguous(TextBoolReverseUnion::String(\"0\".into()));\n"
                "    assert_ambiguous(NumericForwardUnion::OptionalString(\n"
                "        OptionalString { x: Some(\" 42 \".into()) },\n"
                "    ));\n"
                "    assert_ambiguous(ActualOverlapUnion::OptionalString(\n"
                "        OptionalString { x: Some(\"text\".into()) },\n"
                "    ));\n"
                "    assert_ambiguous(NarrowForwardUnion::String(\"42\".into()));\n"
                "    assert_ambiguous(NarrowReverseUnion::String(\"42\".into()));\n\n"
                "    assert_ambiguous(NumericForwardUnion::RequiredLong(\n"
                "        RequiredLong { x: 42 },\n"
                "    ));\n\n"
                "    let none = NumericForwardUnion::OptionalString(\n"
                "        OptionalString { x: None },\n"
                "    );\n"
                "    let none_xml = quick_xml::se::to_string(&none).unwrap();\n"
                "    let none_round_trip: NumericForwardUnion =\n"
                "        quick_xml::de::from_str(&none_xml).unwrap();\n"
                "    assert_eq!(none, none_round_trip);\n\n"
                "    let null_text = NumericReverseUnion::OptionalString(\n"
                "        OptionalString { x: Some(\"null\".into()) },\n"
                "    );\n"
                "    let null_xml = quick_xml::se::to_string(&null_text).unwrap();\n"
                "    let null_round_trip: NumericReverseUnion =\n"
                "        quick_xml::de::from_str(&null_xml).unwrap();\n"
                "    assert_eq!(null_text, null_round_trip);\n"
                "    let overflow = Root {\n"
                "        value: NarrowForwardUnion::String(\"2147483648\".into()),\n"
                "    };\n"
                "    let overflow_xml = quick_xml::se::to_string(&overflow).unwrap();\n"
                "    let overflow_round_trip: Root<NarrowForwardUnion> =\n"
                "        quick_xml::de::from_str(&overflow_xml).unwrap();\n"
                "    assert_eq!(overflow.value, overflow_round_trip.value);\n"
                "}\n\n"
                "#[test]\n"
                "fn concrete_json_matching_preserves_variant_identity() {\n"
                "    let forward = IntLongForwardUnion::I64(2_147_483_648);\n"
                "    let json = serde_json::to_vec(&forward).unwrap();\n"
                "    let recovered: IntLongForwardUnion =\n"
                "        serde_json::from_slice(&json).unwrap();\n"
                "    assert_eq!(forward, recovered);\n\n"
                "    let reverse = IntLongReverseUnion::I64(2_147_483_648);\n"
                "    let json = serde_json::to_vec(&reverse).unwrap();\n"
                "    let recovered: IntLongReverseUnion =\n"
                "        serde_json::from_slice(&json).unwrap();\n"
                "    assert_eq!(reverse, recovered);\n\n"
                "    assert!(serde_json::from_str::<IntLongForwardUnion>(\"42\").is_err());\n"
                "    assert!(serde_json::from_str::<IntLongReverseUnion>(\"42\").is_err());\n\n"
                "    let alpha = EnumForwardUnion::TagA(TagA::ALPHA);\n"
                "    let recovered: EnumForwardUnion = serde_json::from_slice(\n"
                "        &serde_json::to_vec(&alpha).unwrap(),\n"
                "    ).unwrap();\n"
                "    assert_eq!(alpha, recovered);\n"
                "    let beta = EnumReverseUnion::TagB(TagB::BETA);\n"
                "    let recovered: EnumReverseUnion = serde_json::from_slice(\n"
                "        &serde_json::to_vec(&beta).unwrap(),\n"
                "    ).unwrap();\n"
                "    assert_eq!(beta, recovered);\n\n"
                "    let string_value = OptionalStringForwardUnion::String(\n"
                "        \"hello\".into(),\n"
                "    );\n"
                "    let recovered: OptionalStringForwardUnion = serde_json::from_slice(\n"
                "        &serde_json::to_vec(&string_value).unwrap(),\n"
                "    ).unwrap();\n"
                "    assert_eq!(string_value, recovered);\n"
                "    let record_value = OptionalStringReverseUnion::OptionalString(\n"
                "        OptionalString { x: None },\n"
                "    );\n"
                "    let recovered: OptionalStringReverseUnion = serde_json::from_slice(\n"
                "        &serde_json::to_vec(&record_value).unwrap(),\n"
                "    ).unwrap();\n"
                "    assert_eq!(record_value, recovered);\n"
                "}\n\n"
                "#[test]\n"
                "fn concrete_xml_matching_preserves_variant_identity() {\n"
                "    let forward = Root {\n"
                "        value: IntLongForwardUnion::I64(2_147_483_648),\n"
                "    };\n"
                "    let xml = quick_xml::se::to_string(&forward).unwrap();\n"
                "    let recovered: Root<IntLongForwardUnion> =\n"
                "        quick_xml::de::from_str(&xml).unwrap();\n"
                "    assert_eq!(forward.value, recovered.value);\n"
                "    let reverse = Root {\n"
                "        value: IntLongReverseUnion::I64(2_147_483_648),\n"
                "    };\n"
                "    let xml = quick_xml::se::to_string(&reverse).unwrap();\n"
                "    let recovered: Root<IntLongReverseUnion> =\n"
                "        quick_xml::de::from_str(&xml).unwrap();\n"
                "    assert_eq!(reverse.value, recovered.value);\n\n"
                "    let spaced: Root<IntLongForwardUnion> =\n"
                "        quick_xml::de::from_str(\n"
                "            \"<Root><value> 2147483648 </value></Root>\"\n"
                "        ).unwrap();\n"
                "    assert_eq!(\n"
                "        IntLongForwardUnion::I64(2_147_483_648),\n"
                "        spaced.value\n"
                "    );\n"
                "    assert!(quick_xml::de::from_str::<Root<TextBoolForwardUnion>>(\n"
                "        \"<Root><value>1</value></Root>\"\n"
                "    ).is_err());\n"
                "    assert!(quick_xml::de::from_str::<Root<TextBoolReverseUnion>>(\n"
                "        \"<Root><value>0</value></Root>\"\n"
                "    ).is_err());\n\n"
                "    let non_xml_space: Root<NarrowForwardUnion> =\n"
                "        quick_xml::de::from_str(\n"
                "            \"<Root><value>&#xA0;42&#xA0;</value></Root>\"\n"
                "        ).unwrap();\n"
                "    assert_eq!(\n"
                "        NarrowForwardUnion::String(\"\\u{a0}42\\u{a0}\".into()),\n"
                "        non_xml_space.value\n"
                "    );\n\n"
                "    let nested_forward = Root {\n"
                "        value: NarrowRecordForwardUnion::OptionalString(\n"
                "            OptionalString { x: Some(\"2147483648\".into()) },\n"
                "        ),\n"
                "    };\n"
                "    let xml = quick_xml::se::to_string(&nested_forward).unwrap();\n"
                "    let recovered: Root<NarrowRecordForwardUnion> =\n"
                "        quick_xml::de::from_str(&xml).unwrap();\n"
                "    assert_eq!(nested_forward.value, recovered.value);\n"
                "    let nested_reverse = Root {\n"
                "        value: NarrowRecordReverseUnion::OptionalString(\n"
                "            OptionalString { x: Some(\"2147483648\".into()) },\n"
                "        ),\n"
                "    };\n"
                "    let xml = quick_xml::se::to_string(&nested_reverse).unwrap();\n"
                "    let recovered: Root<NarrowRecordReverseUnion> =\n"
                "        quick_xml::de::from_str(&xml).unwrap();\n"
                "    assert_eq!(nested_reverse.value, recovered.value);\n\n"
                "    let alpha = Root {\n"
                "        value: EnumForwardUnion::TagA(TagA::ALPHA),\n"
                "    };\n"
                "    let xml = quick_xml::se::to_string(&alpha).unwrap();\n"
                "    let recovered: Root<EnumForwardUnion> =\n"
                "        quick_xml::de::from_str(&xml).unwrap();\n"
                "    assert_eq!(alpha.value, recovered.value);\n"
                "    let beta = Root {\n"
                "        value: EnumReverseUnion::TagB(TagB::BETA),\n"
                "    };\n"
                "    let xml = quick_xml::se::to_string(&beta).unwrap();\n"
                "    let recovered: Root<EnumReverseUnion> =\n"
                "        quick_xml::de::from_str(&xml).unwrap();\n"
                "    assert_eq!(beta.value, recovered.value);\n"
                "}\n\n"
                "#[test]\n"
                "fn exact_probe_uses_xml_metadata_and_collection_shapes() {\n"
                "    let renamed_xml = quick_xml::se::to_string(&Root {\n"
                "        value: RenamedFieldUnion::RenamedField(RenamedField {\n"
                "            value: \"text\".into(),\n"
                "        }),\n"
                "    }).unwrap();\n"
                "    assert!(renamed_xml.contains(\"<wireValue>text</wireValue>\"));\n\n"
                "    let renamed_round_trip: Root<RenamedFieldUnion> =\n"
                "        quick_xml::de::from_str(&renamed_xml).unwrap();\n"
                "    assert!(matches!(\n"
                "        renamed_round_trip.value,\n"
                "        RenamedFieldUnion::RenamedField(_)\n"
                "    ));\n\n"
                "    let attribute_xml = quick_xml::se::to_string(&Root {\n"
                "        value: AttributeFieldUnion::AttributeField(AttributeField {\n"
                "            value: \"text\".into(),\n"
                "        }),\n"
                "    }).unwrap();\n"
                "    assert!(attribute_xml.contains(\"value=\\\"text\\\"\"));\n\n"
                "    let attribute_round_trip: Root<AttributeFieldUnion> =\n"
                "        quick_xml::de::from_str(&attribute_xml).unwrap();\n"
                "    assert!(matches!(\n"
                "        attribute_round_trip.value,\n"
                "        AttributeFieldUnion::AttributeField(_)\n"
                "    ));\n\n"
                "    let attribute_variant = Root {\n"
                "        value: AttributeElementUnion::AttributeField(AttributeField {\n"
                "            value: \"attribute\".into(),\n"
                "        }),\n"
                "    };\n"
                "    let xml = quick_xml::se::to_string(&attribute_variant).unwrap();\n"
                "    let recovered: Root<AttributeElementUnion> =\n"
                "        quick_xml::de::from_str(&xml).unwrap();\n"
                "    assert_eq!(attribute_variant.value, recovered.value);\n"
                "    let element_variant = Root {\n"
                "        value: AttributeElementUnion::ElementField(ElementField {\n"
                "            value: \"element\".into(),\n"
                "        }),\n"
                "    };\n"
                "    let xml = quick_xml::se::to_string(&element_variant).unwrap();\n"
                "    let recovered: Root<AttributeElementUnion> =\n"
                "        quick_xml::de::from_str(&xml).unwrap();\n"
                "    assert_eq!(element_variant.value, recovered.value);\n\n"
                "    let enum_xml = quick_xml::se::to_string(&Root {\n"
                "        value: AlternateEnumUnion::WireTag(WireTag::FIRST),\n"
                "    }).unwrap();\n"
                "    assert!(enum_xml.contains(\">wire-first<\"));\n\n"
                "    let enum_round_trip: Root<AlternateEnumUnion> =\n"
                "        quick_xml::de::from_str(&enum_xml).unwrap();\n"
                "    assert!(matches!(\n"
                "        enum_round_trip.value,\n"
                "        AlternateEnumUnion::WireTag(WireTag::FIRST)\n"
                "    ));\n\n"
                "    let vec_holder = CollectionHolder {\n"
                "        collection: CollectionUnion::VecString(vec![\n"
                "            \"one\".into(), \"two\".into(),\n"
                "        ]),\n"
                "    };\n"
                "    let vec_xml = vec_holder.to_byte_array(\"application/xml\").unwrap();\n"
                "    let vec_round_trip = CollectionHolder::from_data(\n"
                "        &vec_xml, \"application/xml\",\n"
                "    ).unwrap();\n"
                "    assert_eq!(vec_holder, vec_round_trip);\n\n"
                "    let map_holder = CollectionHolder {\n"
                "        collection: CollectionUnion::HashMapStringString(\n"
                "            HashMap::from([(\"key\".into(), \"value\".into())]),\n"
                "        ),\n"
                "    };\n"
                "    let map_xml = map_holder.to_byte_array(\"application/xml\").unwrap();\n"
                "    let map_round_trip = CollectionHolder::from_data(\n"
                "        &map_xml, \"application/xml\",\n"
                "    ).unwrap();\n"
                "    assert_eq!(map_holder, map_round_trip);\n\n"
                "    let nested_xml = quick_xml::se::to_string(&Root {\n"
                "        value: NestedUnion::NestedChoice(NestedChoice {\n"
                "            inner: InnerUnion::InnerText(InnerText {\n"
                "                text: \"nested\".into(),\n"
                "            }),\n"
                "        }),\n"
                "    }).unwrap();\n"
                "    assert!(nested_xml.contains(\"<text>nested</text>\"));\n"
                "    let nested_round_trip: Root<NestedUnion> =\n"
                "        quick_xml::de::from_str(&nested_xml).unwrap();\n"
                "    assert!(matches!(\n"
                "        nested_round_trip.value,\n"
                "        NestedUnion::NestedChoice(_)\n"
                "    ));\n"
                "}\n"
            )
        assert subprocess.check_call(
            ['cargo', 'test', '--test', 'xml_lexical_normalization'],
            cwd=rust_path,
            stdout=sys.stdout,
            stderr=sys.stderr,
            timeout=self.CARGO_TIMEOUT,
        ) == 0

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

    def test_xml_support_path_conflicts_fail_before_output(self):
        """Reserve the generated XML helper module before writes."""
        conflict = {
            "type": "record",
            "name": "Carrier",
            "namespace": "xml_support",
            "fields": [],
        }
        safe = {
            "type": "record",
            "name": "Other",
            "namespace": "n",
            "fields": [],
        }
        cases = (
            [conflict],
            [safe, conflict],
            [conflict, safe],
        )
        for index, schema in enumerate(cases):
            rust_path = os.path.join(
                tempfile.gettempdir(),
                "avrotize",
                f"rust-xml-support-conflict-{index}",
            )
            if os.path.exists(rust_path):
                shutil.rmtree(rust_path, ignore_errors=True)
            with self.assertRaisesRegex(
                ValueError,
                r"(xml_support|xmlsupport)",
            ):
                convert_avro_schema_to_rust(
                    schema,
                    rust_path,
                    package_name="rust-xml-support-conflict",
                    xml_annotation=True,
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
        with self.assertRaisesRegex(
            ValueError,
            r"exact path collision.*generated union.*named type",
        ):
            convert_avro_schema_to_rust(
                hash_schema,
                rust_path,
                package_name="rust-plan-non-avro-hash",
                avro_annotation=False,
            )
        self.assertFalse(os.path.exists(rust_path))

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

        different_schema_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-plan-owner-different-unions",
        )
        if os.path.exists(different_schema_path):
            shutil.rmtree(different_schema_path, ignore_errors=True)
        different_schema = [
            {
                "type": "record",
                "name": "One",
                "namespace": "n",
                "fields": [
                    {
                        "name": "choice",
                        "type": ["long", "int"],
                    }
                ],
            },
            {
                "type": "record",
                "name": "Two",
                "namespace": "n",
                "fields": [
                    {
                        "name": "choice",
                        "type": ["string", "boolean"],
                    }
                ],
            },
        ]
        convert_avro_schema_to_rust(
            different_schema,
            different_schema_path,
            package_name="rust-plan-owner-different-unions",
            avro_annotation=False,
            serde_annotation=True,
        )
        self.assertFalse(
            os.path.exists(
                os.path.join(
                    different_schema_path,
                    "src",
                    "n",
                    "choiceunion.rs",
                )
            )
        )
        union_files = glob.glob(
            os.path.join(
                different_schema_path,
                "src",
                "n",
                "unionpath*.rs",
            )
        )
        self.assertEqual(2, len(union_files))
        assert subprocess.check_call(
            ['cargo', 'test'],
            cwd=different_schema_path,
            stdout=sys.stdout,
            stderr=sys.stderr,
            timeout=self.CARGO_TIMEOUT,
        ) == 0

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

    def test_legacy_union_file_migrates_and_ambiguous_alias_is_removed(self):
        """Migrate old union modules and remove newly ambiguous aliases."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-legacy-union-migration",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        alias_directory = os.path.join(rust_path, "src", "n")
        os.makedirs(alias_directory, exist_ok=True)
        alias_path = os.path.join(alias_directory, "choiceunion.rs")
        with open(alias_path, "w", encoding="utf-8") as legacy_file:
            legacy_file.write(
                "pub enum ChoiceUnion { I64(i64), I32(i32) }\n"
                "impl Default for ChoiceUnion { fn default() -> Self {"
                " ChoiceUnion::I64(0) } }\n"
                "impl ChoiceUnion { pub fn is_json_match(_: &()) -> bool"
                " { true } }\n"
                "#[cfg(test)] impl ChoiceUnion {"
                " pub fn generate_random_instance() -> Self {"
                " ChoiceUnion::I64(0) } }\n"
                "#[test] fn test_union_variants_choiceunion() {}\n"
            )
        first_schema = {
            "type": "record",
            "name": "One",
            "namespace": "n",
            "fields": [
                {"name": "choice", "type": ["long", "int"]}
            ],
        }
        convert_avro_schema_to_rust(
            first_schema,
            rust_path,
            package_name="rust-legacy-union-migration",
            serde_annotation=True,
        )
        with open(alias_path, "r", encoding="utf-8") as alias_file:
            self.assertTrue(alias_file.read().startswith(
                "pub type ChoiceUnion = crate::"
            ))

        second_schema = [
            first_schema,
            {
                "type": "record",
                "name": "Two",
                "namespace": "n",
                "fields": [
                    {
                        "name": "choice",
                        "type": ["string", "boolean"],
                    }
                ],
            },
        ]
        convert_avro_schema_to_rust(
            second_schema,
            rust_path,
            package_name="rust-legacy-union-migration",
            serde_annotation=True,
        )
        self.assertFalse(os.path.exists(alias_path))

    def test_stale_alias_does_not_delete_new_named_type(self):
        """Keep a current named type that occupies a stale alias path."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-alias-to-named",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        first_schema = {
            "type": "record",
            "name": "Carrier",
            "namespace": "n",
            "fields": [
                {"name": "choice", "type": ["long", "int"]}
            ],
        }
        convert_avro_schema_to_rust(
            first_schema,
            rust_path,
            package_name="rust-alias-to-named",
            serde_annotation=True,
        )
        alias_path = os.path.join(
            rust_path,
            "src",
            "n",
            "choiceunion.rs",
        )
        self.assertTrue(os.path.isfile(alias_path))

        second_schema = {
            "type": "record",
            "name": "ChoiceUnion",
            "namespace": "n",
            "fields": [
                {"name": "value", "type": "string"}
            ],
        }
        convert_avro_schema_to_rust(
            second_schema,
            rust_path,
            package_name="rust-alias-to-named",
            serde_annotation=True,
        )
        with open(alias_path, "r", encoding="utf-8") as named_file:
            self.assertIn(
                "pub struct ChoiceUnion",
                named_file.read(),
            )

    def test_shared_outer_union_registers_descendant_aliases(self):
        """Register nested legacy aliases for every shared outer owner."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-shared-outer-union",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        nested_union = [
            {
                "type": "array",
                "items": ["long", "int"],
            },
            "string",
        ]
        schema = {
            "type": "record",
            "name": "Carrier",
            "namespace": "n",
            "fields": [
                {"name": "foo", "type": nested_union},
                {"name": "bar", "type": nested_union},
            ],
        }
        convert_avro_schema_to_rust(
            schema,
            rust_path,
            package_name="rust-shared-outer-union",
            serde_annotation=True,
        )
        for alias_name in (
            "foooption0union.rs",
            "baroption0union.rs",
        ):
            self.assertTrue(
                os.path.isfile(
                    os.path.join(
                        rust_path,
                        "src",
                        "n",
                        alias_name,
                    )
                )
            )

    def test_union_sharing_normalizes_rust_namespace(self):
        """Match planning and generation namespace normalization."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-union-namespace-normalization",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        schema = [
            {
                "type": "record",
                "name": "One",
                "namespace": "N",
                "fields": [
                    {"name": "choice", "type": ["long", "int"]}
                ],
            },
            {
                "type": "record",
                "name": "Two",
                "namespace": "n",
                "fields": [
                    {"name": "choice", "type": ["long", "int"]}
                ],
            },
        ]
        convert_avro_schema_to_rust(
            schema,
            rust_path,
            package_name="rust-union-namespace-normalization",
            serde_annotation=True,
        )
        union_files = glob.glob(
            os.path.join(rust_path, "src", "n", "unionpath*.rs")
        )
        self.assertEqual(1, len(union_files))
        self.assertTrue(
            os.path.isfile(
                os.path.join(
                    rust_path,
                    "src",
                    "n",
                    "choiceunion.rs",
                )
            )
        )

    def test_nullable_union_identity_preserves_trailing_null_schema(self):
        """Do not reuse a non-null union target for a nullable source schema."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-nullable-union-identity",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        schema = {
            "type": "record",
            "name": "Carrier",
            "namespace": "n",
            "fields": [
                {"name": "plain", "type": ["string", "long"]},
                {
                    "name": "nullable",
                    "type": ["string", "long", "null"],
                },
            ],
        }
        convert_avro_schema_to_rust(
            schema,
            rust_path,
            package_name="rust-nullable-union-identity",
            avro_annotation=True,
        )
        union_files = glob.glob(
            os.path.join(rust_path, "src", "n", "unionpath*.rs")
        )
        self.assertEqual(2, len(union_files))
        generated_sources = []
        for union_file in union_files:
            with open(union_file, "r", encoding="utf-8") as generated_file:
                generated_sources.append(generated_file.read())
        nullable_sources = [
            source for source in generated_sources
            if "pub static ref SOURCE_SCHEMA" in source
        ]
        self.assertEqual(1, len(nullable_sources))
        self.assertIn(
            "pub fn to_nullable_byte_array(",
            nullable_sources[0],
        )
        self.assertIn(
            "pub fn from_nullable_data(",
            nullable_sources[0],
        )

    def test_recursive_shared_union_metadata_is_complete(self):
        """Round-trip recursive shared unions after metadata completion."""
        schema = {
            "type": "record",
            "name": "RecursiveHolder",
            "namespace": "issue406.recursive_shared",
            "fields": [
                {
                    "name": "root",
                    "type": [
                        {
                            "type": "record",
                            "name": "Branch",
                            "fields": [
                                {"name": "value", "type": "string"},
                                {
                                    "name": "children",
                                    "type": {
                                        "type": "array",
                                        "items": ["Branch", "string"],
                                    },
                                },
                            ],
                        },
                        "string",
                    ],
                }
            ],
        }
        for serde_annotation in (False, True):
            rust_path = os.path.join(
                tempfile.gettempdir(),
                "avrotize",
                "rust-recursive-shared"
                f"{'-serde' if serde_annotation else ''}",
            )
            if os.path.exists(rust_path):
                shutil.rmtree(rust_path, ignore_errors=True)
            convert_avro_schema_to_rust(
                schema,
                rust_path,
                package_name="recursive-shared",
                avro_annotation=True,
                serde_annotation=serde_annotation,
            )
            union_files = glob.glob(
                os.path.join(
                    rust_path,
                    "src",
                    "issue406",
                    "recursive_shared",
                    "unionpath*.rs",
                )
            )
            self.assertEqual(1, len(union_files))
            with open(
                union_files[0],
                "r",
                encoding="utf-8",
            ) as union_file:
                source = union_file.read()
            self.assertRegex(
                source,
                r"from_avro_branch[\s\S]+0 => Ok\([\s\S]+1 => Ok\(",
            )
            self.assertNotIn("match self {\n        })", source)
            holder_path = os.path.join(
                rust_path,
                "src",
                "issue406",
                "recursive_shared",
                "recursiveholder.rs",
            )
            with open(
                holder_path,
                "a",
                encoding="utf-8",
            ) as holder_file:
                holder_file.write(
                    "\n#[cfg(test)]\n"
                    "mod recursive_depth_test {\n"
                    "    use super::*;\n"
                    "    use apache_avro::types::Value;\n\n"
                    "    #[test]\n"
                    "    fn recursive_union_depth_is_bounded() {\n"
                    "        let mut branch = Value::Record(vec![\n"
                    "            (\"value\".into(), Value::String("
                    "\"leaf\".into())),\n"
                    "            (\"children\".into(), Value::Array(vec![])),\n"
                    "        ]);\n"
                    "        for _ in 0..130 {\n"
                    "            branch = Value::Record(vec![\n"
                    "                (\"value\".into(), Value::String("
                    "\"node\".into())),\n"
                    "                (\"children\".into(), Value::Array(vec![\n"
                    "                    Value::Union(0, Box::new(branch)),\n"
                    "                ])),\n"
                    "            ]);\n"
                    "        }\n"
                    "        let value = Value::Record(vec![\n"
                    "            (\"root\".into(), "
                    "Value::Union(0, Box::new(branch))),\n"
                    "        ]);\n"
                    "        let error = RecursiveHolder::from_avro_value("
                    "&value).unwrap_err();\n"
                    "        assert!(error.to_string().contains("
                    "\"nesting depth\"));\n"
                    "    }\n"
                    "}\n"
                )

            integration_dir = os.path.join(rust_path, "tests")
            os.makedirs(integration_dir, exist_ok=True)
            with open(
                os.path.join(integration_dir, "recursive_union.rs"),
                "w",
                encoding="utf-8",
            ) as integration_test:
                integration_test.write(
                    "use recursive_shared::issue406::recursive_shared::{\n"
                    "    branch::Branch,\n"
                    "    childrenunion::ChildrenUnion,\n"
                    "    recursiveholder::RecursiveHolder,\n"
                    "    rootunion::RootUnion,\n"
                    "};\n\n"
                    "#[test]\n"
                    "fn recursive_union_round_trip() {\n"
                    "    let branch = Branch {\n"
                    "        value: \"root\".into(),\n"
                    "        children: vec!["
                    "ChildrenUnion::String(\"leaf\".into())],\n"
                    "    };\n"
                    "    let instance = RecursiveHolder {\n"
                    "        root: RootUnion::Branch(branch),\n"
                    "    };\n"
                    "    let bytes = instance.to_byte_array("
                    "\"avro/binary\").unwrap();\n"
                    "    let decoded = RecursiveHolder::from_data("
                    "&bytes, \"avro/binary\").unwrap();\n"
                    "    assert_eq!(instance, decoded);\n"
                    "}\n"
                )
            assert subprocess.check_call(
                ['cargo', 'test'],
                cwd=rust_path,
                stdout=sys.stdout,
                stderr=sys.stderr,
                timeout=self.CARGO_TIMEOUT,
            ) == 0

    def test_same_converter_generates_complete_independent_outputs(self):
        """Reset run-scoped caches between outputs on one converter."""
        schema = {
            "type": "record",
            "name": "Carrier",
            "namespace": "issue406.reuse",
            "fields": [
                {
                    "name": "choice",
                    "type": ["long", "int"],
                }
            ],
        }
        converter = AvroToRust()
        converter.base_package = "rust-reuse"
        converter.avro_annotation = True
        converter.serde_annotation = True
        for index in range(2):
            rust_path = os.path.join(
                tempfile.gettempdir(),
                "avrotize",
                f"rust-reuse-{index}",
            )
            if os.path.exists(rust_path):
                shutil.rmtree(rust_path, ignore_errors=True)
            converter.convert_schema(schema, rust_path)
            union_files = glob.glob(
                os.path.join(
                    rust_path,
                    "src",
                    "issue406",
                    "reuse",
                    "unionpath*.rs",
                )
            )
            self.assertEqual(1, len(union_files))
            assert subprocess.check_call(
                ['cargo', 'test'],
                cwd=rust_path,
                stdout=sys.stdout,
                stderr=sys.stderr,
                timeout=self.CARGO_TIMEOUT,
            ) == 0

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

    def test_generated_paths_cannot_escape_output(self):
        """Reject traversal components before creating output."""
        rust_path = os.path.join(
            tempfile.gettempdir(),
            "avrotize",
            "rust-path-escape",
        )
        if os.path.exists(rust_path):
            shutil.rmtree(rust_path, ignore_errors=True)
        with self.assertRaisesRegex(
            ValueError,
            r"invalid generated Rust path component",
        ):
            convert_avro_schema_to_rust(
                {
                    "type": "record",
                    "name": "Carrier",
                    "namespace": "../escape",
                    "fields": [],
                },
                rust_path,
                package_name="rust-path-escape",
                avro_annotation=True,
            )
        self.assertFalse(os.path.exists(rust_path))

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
        integration_dir = os.path.join(rust_path, "tests")
        os.makedirs(integration_dir, exist_ok=True)
        with open(
            os.path.join(integration_dir, "gzip_limit.rs"),
            "w",
            encoding="utf-8",
        ) as gzip_test:
            gzip_test.write(
                "use rust_enum_annotation::issue406::enum_only::"
                "status::Status;\n"
                "use flate2::write::GzEncoder;\n"
                "use std::io::Write;\n"
                "#[test]\n"
                "fn gzip_limit_is_enforced() {\n"
                "    let mut encoder = GzEncoder::new("
                "Vec::new(), flate2::Compression::default());\n"
                "    encoder.write_all(&vec![0u8; 16 * 1024 * 1024 + 1])"
                ".unwrap();\n"
                "    let bomb = encoder.finish().unwrap();\n"
                "    let error = Status::from_data("
                "&bomb, \"avro/binary+gzip\").unwrap_err();\n"
                "    assert!(error.to_string().contains(\"size limit\"));\n"
                "}\n"
            )
        assert subprocess.check_call(
            ['cargo', 'test', '--test', 'gzip_limit'],
            cwd=rust_path,
            stdout=sys.stdout,
            stderr=sys.stderr,
            timeout=self.CARGO_TIMEOUT,
        ) == 0

    def test_convert_union_avro_annotations_to_rust(self):
        """Compile a union-bearing crate with Avro binary round-trip coverage."""
        rust_path = self.run_convert_to_rust("rust-union-annotation", True, True)
        self.assert_module_scoped_schemas(
            rust_path,
            expected_count=12,
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
        self.assertRegex(
            avro_source,
            r"pub optional_named_record: crate::issue406::union_only::"
            r"optionalinlinerecord::OptionalInlineRecord",
        )
        self.assertRegex(
            avro_source,
            r"pub optional_qualified_named_record: "
            r"crate::issue406::union_only::"
            r"optionalinlinerecord::OptionalInlineRecord",
        )
        self.assertRegex(
            avro_source,
            r"pub optional_named_enum: crate::issue406::union_only::"
            r"optionalnamedenum::OptionalNamedEnum",
        )
        self.assertRegex(
            avro_source,
            r"pub optional_named_enum_ref: "
            r"crate::issue406::union_only::"
            r"optionalnamedenum::OptionalNamedEnum",
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
                "use flate2::write::GzEncoder;\n"
                "use std::io::Write;\n\n"
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
                "\n#[test]\n"
                "fn gzip_limit_is_enforced() {\n"
                "    let mut encoder = GzEncoder::new(\n"
                "        Vec::new(), flate2::Compression::default());\n"
                "    encoder.write_all(&vec![0u8; 16 * 1024 * 1024 + 1])"
                ".unwrap();\n"
                "    let bomb = encoder.finish().unwrap();\n"
                "    let error = UnionHolder::from_data(\n"
                "        &bomb, \"avro/binary+gzip\").unwrap_err();\n"
                "    assert!(error.to_string().contains(\"size limit\"));\n"
                "    let error = NullableUnion::from_nullable_data(\n"
                "        &bomb, \"avro/binary+gzip\").unwrap_err();\n"
                "    assert!(error.to_string().contains(\"size limit\"));\n"
                "}\n"
            )
        assert subprocess.check_call(
            ['cargo', 'test'],
            cwd=rust_path,
            stdout=sys.stdout,
            stderr=sys.stderr,
            timeout=self.CARGO_TIMEOUT,
        ) == 0

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
            self.assertRegex(
                compatible_source,
                r"pub nullable: crate::issue406::union_only::"
                r"unionpath[a-z0-9]+::UnionPath[A-Za-z0-9]+,",
            )
            self.assertNotIn("pub nullable: Option<", compatible_source)
            self.assertTrue(
                os.path.exists(
                    os.path.join(
                        compatible_path,
                        "src",
                        "issue406",
                        "union_only",
                        "nullableunion.rs",
                    )
                )
            )

    def test_convert_multitype_avro_annotations_to_rust(self):
        """Compile records, enums, and unions that each define SCHEMA in one crate."""
        rust_path = self.run_convert_to_rust("rust-multitype-annotations", True, True)
        schema_modules = self.assert_module_scoped_schemas(
            rust_path,
            expected_count=20,
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
        self.assertEqual(6, len(union_modules))
        self.run_convert_to_rust(
            "rust-multitype-annotations",
            True,
            False,
        )
        self.run_convert_to_rust(
            "rust-multitype-annotations",
            False,
            True,
        )
        self.run_convert_to_rust(
            "rust-multitype-annotations",
            False,
            False,
        )

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
