"""Convert Avrotize Schema records to SurrealQL schema definitions."""

import json
from typing import Dict, List

JsonNode = Dict[str, "JsonNode"] | List["JsonNode"] | str | bool | int | float | None


class AvroToSurrealQLConverter:
    """Converter for Avrotize Schema records to SurrealQL DEFINE statements."""

    def convert_schema(self, schema: JsonNode, record_type: str | None = None) -> str:
        """Convert an Avrotize Schema document to SurrealQL text."""
        records = self._record_schemas(schema)
        if record_type:
            records = [record for record in records if record.get("name") == record_type or self._qualified_name(record) == record_type]
            if not records:
                raise ValueError(f"Record type {record_type} not found in Avro schema")
        lines: List[str] = []
        for record in records:
            lines.append(f"DEFINE TABLE {record['name']} SCHEMAFULL;")
            for statement in self._field_statements(record, str(record["name"])):
                lines.append(statement)
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    def _field_statements(self, record: Dict[str, JsonNode], table_name: str, prefix: str = "") -> List[str]:
        statements: List[str] = []
        for field in record.get("fields", []):
            if not isinstance(field, dict):
                continue
            field_name = str(field["name"])
            path = f"{prefix}.{field_name}" if prefix else field_name
            field_type = field["type"]
            nullable, base_type = self._unwrap_nullable(field_type)
            if isinstance(base_type, dict) and base_type.get("type") == "record":
                statements.append(self._field_statement(path, table_name, "option<object>" if nullable else "object", field))
                statements.extend(self._field_statements(base_type, table_name, path))
            elif isinstance(base_type, dict) and base_type.get("type") == "array" and isinstance(base_type.get("items"), dict) and base_type["items"].get("type") == "record":
                surreal_type = "array<object>"
                if nullable:
                    surreal_type = f"option<{surreal_type}>"
                statements.append(self._field_statement(path, table_name, surreal_type, field))
                statements.extend(self._array_item_statements(base_type["items"], table_name, f"{path}[*]"))
            else:
                surreal_type = self._avro_type_to_surreal(field_type)
                statements.append(self._field_statement(path, table_name, surreal_type, field))
        return statements

    def _array_item_statements(self, record: Dict[str, JsonNode], table_name: str, prefix: str) -> List[str]:
        statements: List[str] = []
        for field in record.get("fields", []):
            if not isinstance(field, dict):
                continue
            path = f"{prefix}.{field['name']}"
            field_type = field["type"]
            nullable, base_type = self._unwrap_nullable(field_type)
            if isinstance(base_type, dict) and base_type.get("type") == "record":
                statements.append(self._field_statement(path, table_name, "option<object>" if nullable else "object", field))
                statements.extend(self._field_statements(base_type, table_name, path))
            else:
                statements.append(self._field_statement(path, table_name, self._avro_type_to_surreal(field_type), field))
        return statements

    @staticmethod
    def _field_statement(path: str, table_name: str, surreal_type: str, field: Dict[str, JsonNode]) -> str:
        statement = f"DEFINE FIELD {path} ON TABLE {table_name} TYPE {surreal_type}"
        if "doc" in field and field["doc"]:
            escaped = str(field["doc"]).replace("'", "\\'")
            statement += f" COMMENT '{escaped}'"
        return statement + ";"

    def _avro_type_to_surreal(self, avro_type: JsonNode) -> str:
        nullable, base_type = self._unwrap_nullable(avro_type)
        surreal_type = self._non_nullable_avro_type_to_surreal(base_type)
        return f"option<{surreal_type}>" if nullable else surreal_type

    def _non_nullable_avro_type_to_surreal(self, avro_type: JsonNode) -> str:
        if isinstance(avro_type, str):
            mapping = {
                "string": "string",
                "boolean": "bool",
                "int": "int",
                "long": "int",
                "float": "float",
                "double": "number",
                "bytes": "bytes",
                "null": "option<string>",
            }
            return mapping.get(avro_type, avro_type)
        if isinstance(avro_type, dict):
            type_name = avro_type.get("type")
            logical_type = avro_type.get("logicalType")
            if logical_type == "timestamp-millis":
                return "datetime"
            if logical_type == "uuid":
                return "uuid"
            if logical_type == "decimal":
                return "decimal"
            if type_name == "array":
                return f"array<{self._avro_type_to_surreal(avro_type.get('items', 'string'))}>"
            if type_name == "record":
                return "object"
            if type_name == "map":
                return "object"
            if isinstance(type_name, str):
                return self._non_nullable_avro_type_to_surreal(type_name)
        return "string"

    @staticmethod
    def _unwrap_nullable(avro_type: JsonNode) -> tuple[bool, JsonNode]:
        if isinstance(avro_type, list):
            non_null = [item for item in avro_type if item != "null"]
            if len(non_null) == 1:
                return True, non_null[0]
        return False, avro_type

    @staticmethod
    def _record_schemas(schema: JsonNode) -> List[Dict[str, JsonNode]]:
        if isinstance(schema, list):
            return [item for item in schema if isinstance(item, dict) and item.get("type") == "record"]
        if isinstance(schema, dict) and schema.get("type") == "record":
            return [schema]
        return []

    @staticmethod
    def _qualified_name(record: Dict[str, JsonNode]) -> str:
        namespace = record.get("namespace")
        return f"{namespace}.{record['name']}" if namespace else str(record["name"])


def convert_avro_to_surrealql(avro_schema_path: str, surrealql_file_path: str, record_type: str | None = None):
    """Convert an Avrotize Schema file to a SurrealQL schema file."""
    with open(avro_schema_path, "r", encoding="utf-8") as avro_file:
        schema = json.load(avro_file)
    surrealql = AvroToSurrealQLConverter().convert_schema(schema, record_type=record_type)
    with open(surrealql_file_path, "w", encoding="utf-8") as surrealql_file:
        surrealql_file.write(surrealql)
