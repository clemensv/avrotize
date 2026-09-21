from unittest.mock import patch
import unittest
import json
import os
import sys
import tempfile
from os import path, getcwd
import time

import pytest

from avrotize.structuretographql import convert_structure_to_graphql

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)

# Import GraphQL validation library if available
try:
    from graphql import parse, build_schema, validate_schema
    GRAPHQL_VALIDATION_AVAILABLE = True
except ImportError:
    GRAPHQL_VALIDATION_AVAILABLE = False
    print("Warning: graphql-core not installed. GraphQL syntax validation will be skipped.")
    print("Install with: pip install graphql-core")


def test_convert_address_struct_to_graphql():
    """Test converting address.struct.json to address.graphql"""
    convert_case("address")


def test_convert_telemetry_struct_to_graphql():
    """Test converting telemetry.struct.json to telemetry.graphql"""
    convert_case("telemetry")


def test_convert_primitives_struct_to_graphql():
    """Test converting primitives.struct.json to primitives.graphql - tests all JSON primitive types"""
    convert_case("primitives")


def test_convert_extendedprimitives_struct_to_graphql():
    """Test converting extendedprimitives.struct.json to extendedprimitives.graphql - tests all extended primitive types"""
    convert_case("extendedprimitives")


def test_convert_compounds_struct_to_graphql():
    """Test converting compounds.struct.json to compounds.graphql - tests compound types (array, set, map, nested objects)"""
    convert_case("compounds")


def test_convert_enums_struct_to_graphql():
    """Test converting enums.struct.json to enums.graphql - tests enum types"""
    convert_case("enums")


def test_convert_enumfield_struct_to_graphql():
    """Test converting enumfield.struct.json to enumfield.graphql - tests enum field references"""
    convert_case("enumfield")


def test_convert_complexunion_struct_to_graphql():
    """Test converting complexunion.struct.json to complexunion.graphql - tests complex union types"""
    convert_case("complexunion")


def test_convert_northwind_struct_to_graphql():
    """Test converting northwind.struct.json to northwind.graphql - tests real-world complex schema"""
    convert_case("northwind")


def test_logical_types_take_precedence_over_wire_types(tmp_path):
    """Logical types take precedence over their numeric wire types."""
    structure_path = tmp_path / "logical-types.struct.json"
    graphql_path = tmp_path / "logical-types.graphql"
    structure_path.write_text(json.dumps({
        "name": "Event",
        "type": "object",
        "properties": {
            "day": {"type": "int32", "logicalType": "date"},
            "created": {"type": "int64", "logicalType": "timestampMillis"},
            "amount": {"type": "binary", "logicalType": "decimal"},
        },
    }), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    assert "scalar Date\n" in graphql
    assert "scalar DateTime\n" in graphql
    assert "scalar Decimal\n" in graphql
    assert "day: Date" in graphql
    assert "created: DateTime" in graphql
    assert "amount: Decimal" in graphql


def test_logical_type_definition_reference_uses_scalar(tmp_path):
    structure_path = tmp_path / "logical-ref.struct.json"
    graphql_path = tmp_path / "logical-ref.graphql"
    structure_path.write_text(json.dumps({
        "name": "Event",
        "type": "object",
        "properties": {"created": {"type": {"$ref": "#/definitions/Instant"}}},
        "definitions": {"Instant": {"name": "Instant", "type": "int64", "logicalType": "timestampMillis"}},
    }), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    assert "scalar DateTime\n" in graphql
    assert "created: DateTime" in graphql
    assert "created: Instant" not in graphql


def test_logical_ref_in_later_document_uses_scalar(tmp_path):
    structure_path = tmp_path / "multi.struct.json"
    graphql_path = tmp_path / "multi.graphql"
    structure_path.write_text(json.dumps([
        {
            "name": "First",
            "type": "object",
            "properties": {},
            "definitions": {"Instant": {"type": "int32", "logicalType": "date"}},
        },
        {
            "name": "Second",
            "type": "object",
            "properties": {"created": {"type": {"$ref": "#/definitions/Instant"}}},
            "definitions": {"Instant": {"type": "int64", "logicalType": "timestampMillis"}},
        },
    ]), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    assert "created: DateTime" in graphql


def test_logical_ref_in_later_document_type_union_uses_owner(tmp_path):
    structure_path = tmp_path / "multi-union.struct.json"
    graphql_path = tmp_path / "multi-union.graphql"
    structure_path.write_text(json.dumps([
        {"definitions": {"Instant": {"type": "int32", "logicalType": "date"}}},
        {
            "name": "Second",
            "type": "object",
            "properties": {"created": {"type": ["null", {"$ref": "#/definitions/Instant"}]}},
            "definitions": {"Instant": {"type": "int64", "logicalType": "timestampMillis"}},
        },
    ]), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    assert "created: DateTime" in graphql


def test_unnamed_object_definition_uses_reference_name(tmp_path):
    structure_path = tmp_path / "unnamed.struct.json"
    graphql_path = tmp_path / "unnamed.graphql"
    structure_path.write_text(json.dumps({
        "name": "Parent",
        "type": "object",
        "properties": {"child": {"type": {"$ref": "#/definitions/Child"}}},
        "definitions": {
            "Child": {"type": "object", "properties": {"value": {"type": "string"}}},
        },
    }), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    assert "type Child" in graphql
    assert "child: Child" in graphql
    assert "type UnnamedType" not in graphql


def test_definition_key_overrides_embedded_name_consistently(tmp_path):
    structure_path = tmp_path / "renamed.struct.json"
    graphql_path = tmp_path / "renamed.graphql"
    structure_path.write_text(json.dumps({
        "name": "Parent",
        "type": "object",
        "properties": {"child": {"type": {"$ref": "#/definitions/Child"}}},
        "definitions": {
            "Child": {"name": "LegacyChild", "type": "object", "properties": {}},
        },
    }), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    assert "type Child" in graphql
    assert "child: Child" in graphql
    assert "LegacyChild" not in graphql


def test_external_ref_uses_referenced_schema_name(tmp_path):
    structure_path = tmp_path / "external.struct.json"
    graphql_path = tmp_path / "external.graphql"
    structure_path.write_text(json.dumps([
        {
            "$id": "https://example.com/customer.json",
            "name": "Customer",
            "type": "object",
            "properties": {},
        },
        {
            "name": "Order",
            "type": "object",
            "properties": {"customer": {"type": {"$ref": "https://example.com/customer.json"}}},
        },
    ]), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    assert "type Customer" in graphql
    assert "customer: Customer" in graphql
    assert "customer.json" not in graphql


def test_recursive_local_ref_terminates(tmp_path):
    structure_path = tmp_path / "recursive.struct.json"
    graphql_path = tmp_path / "recursive.graphql"
    structure_path.write_text(json.dumps({
        "$root": "#/definitions/Node",
        "definitions": {
            "Node": {
                "type": "object",
                "properties": {"next": {"type": {"$ref": "#/definitions/Node"}}},
            },
        },
    }), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    assert graphql.count("type Node {") == 1
    assert "next: Node" in graphql


def test_root_pointer_key_overrides_embedded_name_without_duplicate(tmp_path):
    structure_path = tmp_path / "canonical-root.struct.json"
    graphql_path = tmp_path / "canonical-root.graphql"
    structure_path.write_text(json.dumps({
        "$root": "#/definitions/Canonical",
        "definitions": {
            "Canonical": {"name": "Legacy", "type": "object", "properties": {}},
        },
    }), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    assert graphql.count("type Canonical {") == 1
    assert "Legacy" not in graphql


def test_relative_external_ref_resolves_against_owner_id(tmp_path):
    structure_path = tmp_path / "relative-external.struct.json"
    graphql_path = tmp_path / "relative-external.graphql"
    structure_path.write_text(json.dumps([
        {
            "$id": "https://example.com/schemas/order.json",
            "namespace": "sales",
            "name": "Order",
            "type": "object",
            "properties": {"customer": {"type": {"$ref": "customer.json"}}},
        },
        {
            "$id": "https://example.com/schemas/customer.json",
            "namespace": "crm",
            "name": "Customer",
            "type": "object",
            "properties": {},
        },
    ]), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    build_schema(graphql)
    assert "customer: Customer" in graphql
    assert graphql.count("type Customer {") == 1


def test_absolute_document_ref_follows_target_root(tmp_path):
    structure_path = tmp_path / "rooted-external.struct.json"
    graphql_path = tmp_path / "rooted-external.graphql"
    structure_path.write_text(json.dumps([
        {
            "$id": "https://example.com/schemas/order.json",
            "namespace": "sales",
            "name": "Order",
            "type": "object",
            "properties": {
                "customer": {"type": {"$ref": "https://example.com/schemas/customer.json"}},
            },
        },
        {
            "$id": "https://example.com/schemas/customer.json",
            "namespace": "crm",
            "$root": "#/definitions/CanonicalCustomer",
            "definitions": {
                "CanonicalCustomer": {"name": "LegacyCustomer", "type": "object", "properties": {}},
            },
        },
    ]), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    build_schema(graphql)
    assert "customer: CanonicalCustomer" in graphql
    assert graphql.count("type CanonicalCustomer {") == 1
    assert "LegacyCustomer" not in graphql


def test_same_simple_name_in_different_namespaces_is_disambiguated(tmp_path):
    structure_path = tmp_path / "namespace-collision.struct.json"
    graphql_path = tmp_path / "namespace-collision.graphql"
    structure_path.write_text(json.dumps({
        "$root": "#/definitions/app/Envelope",
        "definitions": {
            "app": {
                "Envelope": {
                    "type": "object",
                    "properties": {
                        "seller": {"type": {"$ref": "#/definitions/sales/Party"}},
                        "buyer": {"type": {"$ref": "#/definitions/crm/Party"}},
                    },
                },
            },
            "sales": {
                "Party": {"type": "object", "properties": {"salesId": {"type": "string"}}},
            },
            "crm": {
                "Party": {"type": "object", "properties": {"crmId": {"type": "string"}}},
            },
        },
    }), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    build_schema(graphql)
    assert "type sales_Party {" in graphql
    assert "type crm_Party {" in graphql
    assert "seller: sales_Party" in graphql
    assert "buyer: crm_Party" in graphql
    assert "type Party {" not in graphql


def test_nested_definition_namespace_precedes_document_namespace(tmp_path):
    structure_path = tmp_path / "nested-namespace.struct.json"
    graphql_path = tmp_path / "nested-namespace.graphql"
    structure_path.write_text(json.dumps({
        "namespace": "owner",
        "$root": "#/definitions/app/Envelope",
        "definitions": {
            "app": {
                "Envelope": {
                    "type": "object",
                    "properties": {
                        "seller": {"type": {"$ref": "#/definitions/sales/Party"}},
                        "buyer": {"type": {"$ref": "#/definitions/crm/Party"}},
                    },
                },
            },
            "sales": {
                "Party": {"type": "object", "properties": {"salesId": {"type": "string"}}},
            },
            "crm": {
                "Party": {"type": "object", "properties": {"crmId": {"type": "string"}}},
            },
        },
    }), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    build_schema(graphql)
    assert "type sales_Party {" in graphql
    assert "type crm_Party {" in graphql
    assert "seller: sales_Party" in graphql
    assert "buyer: crm_Party" in graphql


def test_raw_qualified_identity_survives_graphql_name_sanitization(tmp_path):
    structure_path = tmp_path / "qualified-identity.struct.json"
    graphql_path = tmp_path / "qualified-identity.graphql"
    structure_path.write_text(json.dumps({
        "$root": "#/definitions/app/Envelope",
        "definitions": {
            "app": {
                "Envelope": {
                    "type": "object",
                    "properties": {
                        "dotted": {"type": {"$ref": "#/definitions/a/b/Party"}},
                        "underscored": {"type": {"$ref": "#/definitions/a_b/Party"}},
                    },
                },
            },
            "a": {
                "b": {
                    "Party": {"type": "object", "properties": {"dottedId": {"type": "string"}}},
                },
            },
            "a_b": {
                "Party": {"type": "object", "properties": {"underscoredId": {"type": "string"}}},
            },
        },
    }), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    build_schema(graphql)
    assert "type a_b_Party {" in graphql
    assert "type a_b_Party_2 {" in graphql
    assert "dotted: a_b_Party" in graphql
    assert "underscored: a_b_Party_2" in graphql


def test_three_way_document_namespace_collisions_are_allocated_together(tmp_path):
    structure_path = tmp_path / "three-way-collision.struct.json"
    graphql_path = tmp_path / "three-way-collision.graphql"
    documents = [
        {
            "$id": f"https://example.com/{namespace}.json",
            "namespace": namespace,
            "name": "Party",
            "type": "object",
            "properties": {f"{namespace}Id": {"type": "string"}},
        }
        for namespace in ("gamma", "alpha", "beta")
    ]
    documents.append({
        "$id": "https://example.com/envelope.json",
        "name": "Envelope",
        "type": "object",
        "properties": {
            namespace: {"type": {"$ref": f"https://example.com/{namespace}.json"}}
            for namespace in ("gamma", "alpha", "beta")
        },
    })
    structure_path.write_text(json.dumps(documents), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    schema = build_schema(graphql)
    for namespace in ("gamma", "alpha", "beta"):
        type_name = f"{namespace}_Party"
        assert schema.get_type(type_name) is not None
        assert str(schema.get_type("Envelope").fields[namespace].type) == type_name


def test_document_id_distinguishes_unqualified_type_names_and_references(tmp_path):
    structure_path = tmp_path / "document-identity.struct.json"
    graphql_path = tmp_path / "document-identity.graphql"
    structure_path.write_text(json.dumps([
        {
            "$id": "https://example.com/sales.json",
            "name": "Party",
            "type": "object",
            "properties": {"salesId": {"type": "string"}},
        },
        {
            "$id": "https://example.com/crm.json",
            "name": "Party",
            "type": "object",
            "properties": {"crmId": {"type": "string"}},
        },
        {
            "$id": "https://example.com/envelope.json",
            "name": "Envelope",
            "type": "object",
            "properties": {
                "seller": {"type": {"$ref": "https://example.com/sales.json"}},
                "buyer": {"type": {"$ref": "https://example.com/crm.json"}},
            },
        },
    ]), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    schema = build_schema(graphql)
    assert "salesId" in schema.get_type("sales_Party").fields
    assert "crmId" in schema.get_type("crm_Party").fields
    assert str(schema.get_type("Envelope").fields["seller"].type) == "sales_Party"
    assert str(schema.get_type("Envelope").fields["buyer"].type) == "crm_Party"


def test_emitted_scalar_names_are_reserved_for_types_and_references(tmp_path):
    structure_path = tmp_path / "scalar-collisions.struct.json"
    graphql_path = tmp_path / "scalar-collisions.graphql"
    scalar_types = {
        "JSON": "any",
        "Date": "date",
        "DateTime": "datetime",
        "Time": "time",
        "Duration": "duration",
        "Decimal": "decimal",
        "UUID": "uuid",
    }
    definitions = {
        name: (
            {"type": "object", "properties": {"value": {"type": "string"}}}
            if index % 2 == 0
            else {"enum": ["VALUE"]}
        )
        for index, name in enumerate(scalar_types)
    }
    properties = {
        f"{name.lower()}Type": {"type": {"$ref": f"#/definitions/{name}"}}
        for name in scalar_types
    }
    properties.update(
        {
            f"{name.lower()}Value": {"type": structure_type}
            for name, structure_type in scalar_types.items()
        }
    )
    structure_path.write_text(json.dumps({
        "name": "Envelope",
        "type": "object",
        "properties": properties,
        "definitions": definitions,
    }), encoding="utf-8")

    convert_structure_to_graphql(str(structure_path), str(graphql_path))

    graphql = graphql_path.read_text(encoding="utf-8")
    build_schema(graphql)
    for scalar_name in scalar_types:
        assert f"scalar {scalar_name}\n" in graphql
        assert f"{scalar_name}_2 {{" in graphql
        assert f"{scalar_name.lower()}Type: {scalar_name}_2" in graphql


def validate_graphql_syntax(graphql_content: str, file_name: str):
    """
    Validate that the generated GraphQL is syntactically correct.
    
    Args:
        graphql_content: The GraphQL schema content to validate
        file_name: Name of the file being validated (for error messages)
    
    Raises:
        AssertionError: If GraphQL syntax is invalid
    """
    if not GRAPHQL_VALIDATION_AVAILABLE:
        return
    
    try:
        # Parse the GraphQL schema to verify syntax
        # This validates that the SDL (Schema Definition Language) is syntactically correct
        document = parse(graphql_content)
        
        # We don't use build_schema() or validate_schema() because:
        # 1. Our generated schemas are type definitions only (no Query/Mutation root types)
        # 2. They're meant to be valid SDL that can be combined/extended, not standalone executable schemas
        # 3. Parsing alone validates syntax which is what we need
        
        # Verify we got a valid document with definitions
        if not document or not document.definitions:
            raise AssertionError(f"GraphQL document for {file_name} has no definitions")
            
    except Exception as e:
        if isinstance(e, AssertionError):
            raise
        raise AssertionError(f"GraphQL syntax validation failed for {file_name}: {str(e)}")


def convert_case(file_base_name: str):
    """Convert a JSON Structure schema to GraphQL schema"""
    cwd = os.getcwd()
    struct_path = os.path.join(cwd, "test", "avsc", file_base_name+"-ref.struct.json")
    graphql_path = os.path.join(tempfile.gettempdir(
    ), "avrotize", file_base_name+".graphql")
    graphql_ref_path = os.path.join(cwd, "test", "avsc", file_base_name+"-ref.graphql")
    dir_name = os.path.dirname(graphql_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    convert_structure_to_graphql(
        struct_path, graphql_path)
    
    # Read the generated GraphQL content
    with open(graphql_path, 'r', encoding="utf-8") as f:
        generated_content = f.read()
    
    # Validate GraphQL syntax
    validate_graphql_syntax(generated_content, file_base_name + ".graphql")
    
    # Compare with reference if it exists
    if os.path.exists(graphql_ref_path):
        with open(graphql_ref_path, 'r', encoding="utf-8") as file2:
            reference_content = file2.read()
        
        # Also validate reference GraphQL
        validate_graphql_syntax(reference_content, file_base_name + "-ref.graphql")
        
        assert generated_content == reference_content, f"Generated GraphQL does not match reference.\nGenerated:\n{generated_content}\n\nExpected:\n{reference_content}"
