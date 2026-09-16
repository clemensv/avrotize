"""Deterministic, standard-library JSON Schema subset validator for governance records.

The governance toolchain must validate its own records without adding runtime
dependencies to workflows. This module implements a recursive validator for the
JSON Schema subset actually used by the checked-in governance schemas:

``$ref`` (local ``#/$defs/...`` pointers), ``type`` (single or union),
``const``, ``enum``, ``properties``, ``required``, ``additionalProperties``
(boolean or schema), ``items``, ``minItems``, ``maxItems``, ``uniqueItems``,
``minLength``, ``maxLength``, ``pattern``, ``minimum``, ``maximum``,
``exclusiveMinimum``, ``exclusiveMaximum``, ``allOf``, ``anyOf``, and ``oneOf``.

Unknown keywords are ignored deliberately: an unsupported keyword must never be
silently treated as satisfied by a checked-in schema, so schemas in this
repository are restricted to the subset above and
:func:`assert_supported_schema` enforces that restriction.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

__all__ = [
    "SchemaError",
    "assert_supported_schema",
    "load_schema",
    "validate",
    "validate_or_raise",
]

_TYPE_NAMES = {
    "object": dict,
    "array": list,
    "string": str,
    "boolean": bool,
    "null": type(None),
}

SUPPORTED_KEYWORDS = frozenset(
    {
        "$schema",
        "$id",
        "$ref",
        "$defs",
        "title",
        "description",
        "examples",
        "default",
        "type",
        "const",
        "enum",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minLength",
        "maxLength",
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "allOf",
        "anyOf",
        "oneOf",
    }
)


class SchemaError(RuntimeError):
    """Raised when a schema itself is unusable (corrupt or unsupported)."""


def load_schema(path: Path) -> dict[str, Any]:
    """Load and structurally check a schema file. Corrupt schema is a hard failure."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SchemaError(f"cannot load schema {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise SchemaError(f"schema {path} is not a JSON object")
    assert_supported_schema(document, path.as_posix())
    return document


def assert_supported_schema(schema: Any, origin: str, pointer: str = "#") -> None:
    """Reject schema keywords this validator does not implement."""
    if isinstance(schema, bool):
        return
    if not isinstance(schema, dict):
        raise SchemaError(f"{origin}{pointer}: schema node must be an object or boolean")
    unsupported = sorted(set(schema) - SUPPORTED_KEYWORDS)
    if unsupported:
        raise SchemaError(f"{origin}{pointer}: unsupported schema keywords {unsupported}")
    for key in ("properties", "$defs"):
        node = schema.get(key)
        if node is None:
            continue
        if not isinstance(node, dict):
            raise SchemaError(f"{origin}{pointer}/{key}: must be an object")
        for name, child in node.items():
            assert_supported_schema(child, origin, f"{pointer}/{key}/{name}")
    for key in ("items", "additionalProperties"):
        node = schema.get(key)
        if node is None or isinstance(node, bool):
            continue
        assert_supported_schema(node, origin, f"{pointer}/{key}")
    for key in ("allOf", "anyOf", "oneOf"):
        node = schema.get(key)
        if node is None:
            continue
        if not isinstance(node, list) or not node:
            raise SchemaError(f"{origin}{pointer}/{key}: must be a non-empty array")
        for index, child in enumerate(node):
            assert_supported_schema(child, origin, f"{pointer}/{key}/{index}")


def _resolve_ref(ref: str, root: dict[str, Any], path: str) -> dict[str, Any]:
    if not ref.startswith("#/"):
        raise SchemaError(f"{path}: only local '#/...' references are supported, got {ref!r}")
    node: Any = root
    for token in ref[2:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        if not isinstance(node, dict) or token not in node:
            raise SchemaError(f"{path}: unresolvable reference {ref!r}")
        node = node[token]
    if not isinstance(node, dict):
        raise SchemaError(f"{path}: reference {ref!r} does not point at a schema object")
    return node


def _type_matches(value: Any, type_name: str) -> bool:
    if type_name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    expected = _TYPE_NAMES.get(type_name)
    if expected is None:
        raise SchemaError(f"unsupported type name {type_name!r}")
    if expected is bool:
        return isinstance(value, bool)
    if expected is dict:
        return isinstance(value, dict)
    if expected is list:
        return isinstance(value, list)
    if expected is str:
        return isinstance(value, str)
    return isinstance(value, expected)


def validate(instance: Any, schema: dict[str, Any], root: dict[str, Any] | None = None, path: str = "$") -> list[str]:
    """Validate ``instance`` against ``schema``; return a list of human-readable errors."""
    if root is None:
        root = schema
    if isinstance(schema, bool):
        return [] if schema else [f"{path}: schema forbids any value"]
    if not isinstance(schema, dict):
        raise SchemaError(f"{path}: schema node must be an object")

    if "$ref" in schema:
        target = _resolve_ref(str(schema["$ref"]), root, path)
        return validate(instance, target, root, path)

    errors: list[str] = []

    declared_type = schema.get("type")
    if declared_type is not None:
        names = declared_type if isinstance(declared_type, list) else [declared_type]
        if not any(_type_matches(instance, str(name)) for name in names):
            errors.append(
                f"{path}: expected type {'|'.join(str(n) for n in names)}, got {type(instance).__name__}"
            )
            return errors

    if "const" in schema and instance != schema["const"]:
        errors.append(f"{path}: expected const {schema['const']!r}, got {instance!r}")
    if "enum" in schema and instance not in schema["enum"]:
        errors.append(f"{path}: value {instance!r} is not one of {schema['enum']!r}")

    if isinstance(instance, str):
        min_length = schema.get("minLength")
        if isinstance(min_length, int) and len(instance) < min_length:
            errors.append(f"{path}: string shorter than minLength {min_length}")
        max_length = schema.get("maxLength")
        if isinstance(max_length, int) and len(instance) > max_length:
            errors.append(f"{path}: string longer than maxLength {max_length}")
        pattern = schema.get("pattern")
        if isinstance(pattern, str) and re.search(pattern, instance) is None:
            errors.append(f"{path}: string does not match pattern {pattern!r}")

    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        for keyword, comparison, message in (
            ("minimum", lambda v, b: v >= b, "below minimum"),
            ("maximum", lambda v, b: v <= b, "above maximum"),
            ("exclusiveMinimum", lambda v, b: v > b, "not above exclusiveMinimum"),
            ("exclusiveMaximum", lambda v, b: v < b, "not below exclusiveMaximum"),
        ):
            bound = schema.get(keyword)
            if isinstance(bound, (int, float)) and not isinstance(bound, bool) and not comparison(instance, bound):
                errors.append(f"{path}: value {instance!r} is {message} {bound!r}")

    if isinstance(instance, list):
        min_items = schema.get("minItems")
        if isinstance(min_items, int) and len(instance) < min_items:
            errors.append(f"{path}: array shorter than minItems {min_items}")
        max_items = schema.get("maxItems")
        if isinstance(max_items, int) and len(instance) > max_items:
            errors.append(f"{path}: array longer than maxItems {max_items}")
        if schema.get("uniqueItems") is True:
            seen: list[str] = [json.dumps(item, sort_keys=True) for item in instance]
            if len(set(seen)) != len(seen):
                errors.append(f"{path}: array items are not unique")
        item_schema = schema.get("items")
        if item_schema is not None:
            for index, item in enumerate(instance):
                errors.extend(validate(item, item_schema, root, f"{path}[{index}]"))

    if isinstance(instance, dict):
        required = schema.get("required")
        if isinstance(required, list):
            for name in required:
                if name not in instance:
                    errors.append(f"{path}: missing required property {name!r}")
        properties = schema.get("properties")
        properties = properties if isinstance(properties, dict) else {}
        for name, value in instance.items():
            if name in properties:
                errors.extend(validate(value, properties[name], root, f"{path}.{name}"))
        additional = schema.get("additionalProperties")
        if additional is False:
            extra = sorted(set(instance) - set(properties))
            if extra:
                errors.append(f"{path}: unexpected properties {extra}")
        elif isinstance(additional, dict):
            for name, value in instance.items():
                if name not in properties:
                    errors.extend(validate(value, additional, root, f"{path}.{name}"))

    for keyword in ("allOf", "anyOf", "oneOf"):
        subschemas = schema.get(keyword)
        if not isinstance(subschemas, list):
            continue
        results = [validate(instance, sub, root, path) for sub in subschemas]
        satisfied = sum(1 for result in results if not result)
        if keyword == "allOf":
            for result in results:
                errors.extend(result)
        elif keyword == "anyOf" and satisfied == 0:
            errors.append(f"{path}: value does not satisfy any anyOf branch")
        elif keyword == "oneOf" and satisfied != 1:
            errors.append(f"{path}: value satisfies {satisfied} oneOf branches, expected exactly 1")

    return errors


def validate_or_raise(instance: Any, schema_path: Path, label: str) -> None:
    """Validate and raise :class:`SchemaError` with all errors when invalid."""
    schema = load_schema(schema_path)
    errors = validate(instance, schema)
    if errors:
        raise SchemaError(f"{label} failed schema validation ({schema_path.name}): " + "; ".join(errors))
