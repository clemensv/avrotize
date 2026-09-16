"""Convert a JSON Structure schema to a Parquet schema.

This mirrors :mod:`avrotize.structuretoiceberg` (JSON Structure traversal) and
:mod:`avrotize.avrotoparquet` (PyArrow/Parquet emission). It produces an empty
Parquet file that carries the derived schema, which is the same convention used
by the Avro-to-Parquet converter (a Parquet file's footer stores the schema).

Type mapping follows the JSON Structure -> Iceberg converter for consistency
across avrotize's columnar targets, using PyArrow types instead of Iceberg
types. Temporal types use microsecond precision (the JSON Structure / Avro
``*-micros`` convention and Parquet's native ``TIMESTAMP``/``TIME`` units).
"""

import json
import sys
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | bool | int | None


class StructureToParquetConverter:
    """Convert a JSON Structure schema to a Parquet (PyArrow) schema."""

    def __init__(self: 'StructureToParquetConverter'):
        self.schema_doc: Optional[Dict[str, Any]] = None
        self.definitions: Dict[str, Any] = {}

    def get_fullname(self, namespace: str, name: str) -> str:
        """Get the full name of a record type."""
        return f"{namespace}.{name}" if namespace else name

    def convert_structure_to_parquet(
        self,
        structure_schema_path: str,
        structure_record_type: Optional[str],
        parquet_file_path: str,
        emit_cloudevents_columns: bool = False,
        output_format: str = "parquet",
    ) -> None:
        """Convert a JSON Structure schema to a Parquet schema.

        Args:
            structure_schema_path: Path to the JSON Structure schema file.
            structure_record_type: Record type to convert (or None for the root).
            parquet_file_path: Path to write the Parquet file (or JSON schema when
                ``output_format`` is ``"schema"``).
            emit_cloudevents_columns: Whether to add CloudEvents columns.
            output_format: ``"parquet"`` (default) writes an empty Parquet file
                carrying the schema; ``"schema"`` writes the schema as JSON.
        """
        if not structure_schema_path:
            print("Please specify the JSON Structure schema file")
            sys.exit(1)
        with open(structure_schema_path, "r", encoding="utf-8") as f:
            schema = json.loads(f.read())

        self.schema_doc = schema
        if "definitions" in schema:
            self.definitions = schema["definitions"]

        schema = self._resolve_root(schema, structure_record_type)

        properties = schema.get("properties", {})
        required = schema.get("required", [])

        parquet_fields: List[pa.Field] = []
        for prop_name, prop_schema in properties.items():
            is_required = prop_name in required
            field_type = self.convert_structure_type_to_parquet_type(prop_schema)
            nullable = (not is_required) or self._is_nullable(prop_schema)
            parquet_fields.append(pa.field(prop_name, field_type, nullable=nullable))

        if emit_cloudevents_columns:
            parquet_fields.extend([
                pa.field("___type", pa.string(), nullable=False),
                pa.field("___source", pa.string(), nullable=False),
                pa.field("___id", pa.string(), nullable=False),
                pa.field("___time", pa.timestamp('us'), nullable=False),
                pa.field("___subject", pa.string(), nullable=True),
            ])

        parquet_schema = pa.schema(parquet_fields)

        if output_format == "schema":
            with open(parquet_file_path, "w", encoding="utf-8") as f:
                json.dump(self._schema_to_json(parquet_schema), f, indent=2)
        else:
            table = pa.Table.from_batches([], schema=parquet_schema)
            pq.write_table(table, parquet_file_path)

    def _resolve_root(self, schema: Dict[str, Any], record_type: Optional[str]) -> Dict[str, Any]:
        """Resolve the top-level object schema to convert."""
        if schema.get("type") == "object":
            return schema
        if "$ref" in schema:
            return self.resolve_ref(schema["$ref"])
        if record_type and "definitions" in schema:
            if record_type in schema["definitions"]:
                return schema["definitions"][record_type]
            print(f"No record type {record_type} found in the JSON Structure schema definitions")
            sys.exit(1)
        print("Expected a JSON Structure schema with type 'object' at the top level")
        sys.exit(1)

    def resolve_ref(self, ref: str) -> Dict[str, Any]:
        """Resolve a local ``#/...`` reference within the schema document."""
        if not ref.startswith("#/"):
            raise ValueError(f"Only local references are supported, got: {ref}")
        current: Any = self.schema_doc
        for part in ref[2:].split("/"):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                raise ValueError(f"Could not resolve reference: {ref}")
        return current

    def _is_nullable(self, structure_type: Any) -> bool:
        """Whether a property type explicitly allows null."""
        if isinstance(structure_type, list):
            return "null" in structure_type
        if isinstance(structure_type, dict):
            t = structure_type.get("type")
            if isinstance(t, list):
                return "null" in t
        return False

    def convert_structure_type_to_parquet_type(self, structure_type: Any) -> pa.DataType:
        """Convert a JSON Structure type to a PyArrow type."""
        # Resolve references
        if isinstance(structure_type, dict) and "$ref" in structure_type:
            return self.convert_structure_type_to_parquet_type(self.resolve_ref(structure_type["$ref"]))

        # Union expressed as a JSON list (e.g. ["string", "null"])
        if isinstance(structure_type, list):
            non_null = [t for t in structure_type if t != "null"]
            if len(non_null) == 1:
                return self.convert_structure_type_to_parquet_type(non_null[0])
            if len(non_null) > 1:
                return pa.struct([
                    pa.field(f"option_{i}", self.convert_structure_type_to_parquet_type(t), nullable=True)
                    for i, t in enumerate(non_null)
                ])
            return pa.string()

        if isinstance(structure_type, dict):
            type_name = structure_type.get("type")

            if isinstance(type_name, list):
                return self.convert_structure_type_to_parquet_type(type_name)

            if type_name in ("array", "set"):
                items = structure_type.get("items", {"type": "string"})
                return pa.list_(self.convert_structure_type_to_parquet_type(items))

            if type_name == "map":
                values = structure_type.get("values", {"type": "string"})
                return pa.map_(pa.string(), self.convert_structure_type_to_parquet_type(values))

            if type_name == "tuple":
                items = structure_type.get("items", [])
                return pa.struct([
                    pa.field(f"field_{i}", self.convert_structure_type_to_parquet_type(item), nullable=False)
                    for i, item in enumerate(items)
                ])

            if type_name == "object":
                return self._object_to_struct(structure_type)

            if type_name == "choice":
                return self._choice_to_struct(structure_type)

            if type_name == "any":
                return pa.string()

            if type_name:
                return self.map_scalar_type(type_name, structure_type)

        elif isinstance(structure_type, str):
            return self.map_scalar_type(structure_type, {})

        return pa.string()

    def _object_to_struct(self, structure_type: Dict[str, Any]) -> pa.DataType:
        """Convert a JSON Structure object (with optional ``$extends``) to a struct."""
        fields: List[pa.Field] = []
        if "$extends" in structure_type:
            base = self.resolve_ref(structure_type["$extends"])
            base_props = base.get("properties", {})
            base_required = base.get("required", [])
            for name, sub in base_props.items():
                fields.append(pa.field(
                    name,
                    self.convert_structure_type_to_parquet_type(sub),
                    nullable=(name not in base_required) or self._is_nullable(sub),
                ))
        properties = structure_type.get("properties", {})
        required = structure_type.get("required", [])
        for name, sub in properties.items():
            fields.append(pa.field(
                name,
                self.convert_structure_type_to_parquet_type(sub),
                nullable=(name not in required) or self._is_nullable(sub),
            ))
        if not fields:
            # An open/property-less object: a map of arbitrary string values.
            return pa.map_(pa.string(), pa.string())
        return pa.struct(fields)

    def _choice_to_struct(self, structure_type: Dict[str, Any]) -> pa.DataType:
        """Convert a JSON Structure choice (union) to a struct of optional alternatives."""
        choices = structure_type.get("choices", [])
        if isinstance(choices, list):
            return pa.struct([
                pa.field(f"option_{i}", self.convert_structure_type_to_parquet_type(c), nullable=True)
                for i, c in enumerate(choices)
            ])
        if isinstance(choices, dict):
            return pa.struct([
                pa.field(name, self.convert_structure_type_to_parquet_type(sub), nullable=True)
                for name, sub in choices.items()
            ])
        return pa.string()

    def map_scalar_type(self, type_name: str, type_schema: Dict[str, Any]) -> pa.DataType:
        """Map a JSON Structure scalar type to a PyArrow type."""
        if type_name == "decimal":
            precision = type_schema.get("precision", 38)
            scale = type_schema.get("scale", 18)
            return pa.decimal128(precision, scale)

        type_mapping: Dict[str, pa.DataType] = {
            'null': pa.string(),
            'boolean': pa.bool_(),
            'string': pa.string(),
            'int8': pa.int8(),
            'uint8': pa.uint8(),
            'int16': pa.int16(),
            'uint16': pa.uint16(),
            'int32': pa.int32(),
            'uint32': pa.uint32(),
            'int64': pa.int64(),
            'uint64': pa.uint64(),
            'int128': pa.string(),   # No native 128-bit integer in Parquet
            'uint128': pa.string(),
            'integer': pa.int32(),
            'number': pa.float64(),
            'float8': pa.float32(),
            'float': pa.float32(),
            'float32': pa.float32(),
            'binary32': pa.float32(),
            'double': pa.float64(),
            'float64': pa.float64(),
            'binary64': pa.float64(),
            'binary': pa.binary(),
            'bytes': pa.binary(),
            'date': pa.date32(),
            'time': pa.time64('us'),
            'datetime': pa.timestamp('us'),
            'timestamp': pa.timestamp('us'),
            'duration': pa.int64(),   # Stored as microseconds
            'uuid': pa.string(),
            'uri': pa.string(),
            'jsonpointer': pa.string(),
        }
        return type_mapping.get(type_name, pa.string())

    def _schema_to_json(self, schema: pa.Schema) -> Dict[str, Any]:
        """Serialize a PyArrow schema to a JSON-friendly structure (for inspection)."""
        return {
            "type": "struct",
            "fields": [
                {"name": field.name, "type": str(field.type), "nullable": field.nullable}
                for field in schema
            ],
        }


def convert_structure_to_parquet(
    structure_schema_path,
    structure_record_type,
    parquet_file_path,
    emit_cloudevents_columns=False,
    output_format="parquet",
):
    """Convert a JSON Structure schema to a Parquet schema."""
    converter = StructureToParquetConverter()
    converter.convert_structure_to_parquet(
        structure_schema_path,
        structure_record_type,
        parquet_file_path,
        emit_cloudevents_columns,
        output_format,
    )
