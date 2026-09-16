"""Convert an Avro schema to a Tabular Model Scripting Language (TMSL) schema."""

import json
import sys
from typing import Any, Dict, List, Optional, Tuple

JsonNode = Dict[str, "JsonNode"] | List["JsonNode"] | str | bool | int | float | None


class AvroToTmslConverter:
    """Class to convert Avro schema to TMSL schema."""

    def __init__(self: "AvroToTmslConverter") -> None:
        self.named_type_cache: Dict[str, Dict[str, Any]] = {}

    def get_fullname(self, namespace: str, name: str) -> str:
        """Get fully-qualified type name."""
        return f"{namespace}.{name}" if namespace else name

    def cache_named_types(self, avro_type: JsonNode, namespace: str = "") -> None:
        """Cache named Avro types for reference resolution."""
        if isinstance(avro_type, list):
            for item in avro_type:
                self.cache_named_types(item, namespace)
            return

        if isinstance(avro_type, dict):
            current_namespace = str(avro_type.get("namespace", namespace))
            type_name = avro_type.get("name")
            if isinstance(type_name, str):
                fullname = self.get_fullname(current_namespace, type_name)
                self.named_type_cache[fullname] = avro_type
                self.named_type_cache[type_name] = avro_type

            avro_kind = avro_type.get("type")
            if avro_kind == "record":
                for field in avro_type.get("fields", []):
                    if isinstance(field, dict) and "type" in field:
                        self.cache_named_types(field["type"], current_namespace)
            elif avro_kind == "array":
                self.cache_named_types(avro_type.get("items"), current_namespace)
            elif avro_kind == "map":
                self.cache_named_types(avro_type.get("values"), current_namespace)

    def map_avro_type_to_tmsl(self, avro_type: JsonNode) -> Tuple[str, bool]:
        """Map an Avro field type to a TMSL data type and nullability."""
        if isinstance(avro_type, list):
            non_null_types = [item for item in avro_type if item != "null"]
            nullable = len(non_null_types) != len(avro_type)
            if not non_null_types:
                return "string", True
            if len(non_null_types) == 1:
                mapped_type, _ = self.map_avro_type_to_tmsl(non_null_types[0])
                return mapped_type, True if nullable else False
            return "variant", True

        if isinstance(avro_type, dict):
            avro_kind = avro_type.get("type")

            if avro_kind == "record":
                return "variant", False
            if avro_kind in ["array", "map"]:
                return "variant", False
            if avro_kind == "enum":
                return "string", False
            if avro_kind == "fixed":
                return "binary", False

            logical_type = avro_type.get("logicalType")
            if logical_type in ["timestamp-millis", "timestamp-micros", "date", "time-millis", "time-micros"]:
                return "dateTime", False
            if logical_type == "decimal":
                return "decimal", False

            if isinstance(avro_kind, (str, dict, list)):
                return self.map_avro_type_to_tmsl(avro_kind)
            return "string", False

        if isinstance(avro_type, str):
            if avro_type in ["boolean"]:
                return "boolean", False
            if avro_type in ["int", "long"]:
                return "int64", False
            if avro_type in ["float", "double"]:
                return "double", False
            if avro_type == "bytes":
                return "binary", False
            if avro_type in ["string", "null"]:
                return "string", avro_type == "null"

            referenced = self.named_type_cache.get(avro_type)
            if referenced is not None:
                return self.map_avro_type_to_tmsl(referenced)

            return "variant", False

        return "string", False

    def resolve_root_record(self, schema: JsonNode, avro_record_type: Optional[str]) -> Dict[str, Any]:
        """Resolve the root record from a schema document."""
        if isinstance(schema, dict):
            if schema.get("type") != "record":
                print("Expected an Avro schema with a root type of 'record'")
                sys.exit(1)
            return schema

        if isinstance(schema, list):
            if avro_record_type:
                for candidate in schema:
                    if not isinstance(candidate, dict):
                        continue
                    if candidate.get("type") != "record":
                        continue
                    record_name = str(candidate.get("name", ""))
                    namespace = str(candidate.get("namespace", ""))
                    fullname = self.get_fullname(namespace, record_name)
                    if avro_record_type in [record_name, fullname]:
                        return candidate
                print(f"No top-level record type {avro_record_type} found in the Avro schema")
                sys.exit(1)

            for candidate in schema:
                if isinstance(candidate, dict) and candidate.get("type") == "record":
                    return candidate

            print("Expected at least one Avro 'record' schema in the schema list")
            sys.exit(1)

        print("Expected an Avro schema as a JSON object or a list of schema records")
        sys.exit(1)

    def resolve_records(self, schema: JsonNode, avro_record_type: Optional[str]) -> List[Dict[str, Any]]:
        """Resolve one or more record schemas from the input document."""
        if isinstance(schema, dict):
            return [self.resolve_root_record(schema, avro_record_type)]

        if isinstance(schema, list):
            if avro_record_type:
                return [self.resolve_root_record(schema, avro_record_type)]

            records = [item for item in schema if isinstance(item, dict) and item.get("type") == "record"]
            if records:
                return records

        print("Expected one or more Avro 'record' schemas")
        sys.exit(1)

    def build_table(self, record: Dict[str, Any], emit_cloudevents_columns: bool) -> Dict[str, Any]:
        """Build a TMSL table object from an Avro record."""
        table_name = str(record.get("name", "Table"))
        unique_columns = set(str(column) for column in record.get("unique", []) if isinstance(column, str))

        columns: List[Dict[str, Any]] = []
        for field in record.get("fields", []):
            if not isinstance(field, dict):
                continue
            field_name = str(field.get("name", ""))
            if not field_name:
                continue
            data_type, nullable = self.map_avro_type_to_tmsl(field.get("type"))
            column: Dict[str, Any] = {
                "name": field_name,
                "dataType": data_type,
                "sourceColumn": field_name,
            }
            if field_name in unique_columns:
                column["isKey"] = True
            if nullable:
                column["isNullable"] = True
            columns.append(column)

        if emit_cloudevents_columns:
            columns.extend([
                {"name": "___type", "dataType": "string", "sourceColumn": "___type", "isNullable": True},
                {"name": "___source", "dataType": "string", "sourceColumn": "___source", "isNullable": True},
                {"name": "___id", "dataType": "string", "sourceColumn": "___id", "isNullable": True},
                {"name": "___time", "dataType": "dateTime", "sourceColumn": "___time", "isNullable": True},
                {"name": "___subject", "dataType": "string", "sourceColumn": "___subject", "isNullable": True},
            ])

        return {
            "name": table_name,
            "columns": columns,
        }

    def _record_sql_identifier(self, record: Dict[str, Any]) -> str | None:
        """Get SQL table identifier from Avro altnames metadata, if available."""
        altnames = record.get("altnames")
        if isinstance(altnames, dict):
            sql_name = altnames.get("sql")
            if isinstance(sql_name, str) and sql_name:
                return sql_name
        return None

    def build_relationships(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build TMSL relationships from Avro foreignKeys metadata."""
        table_by_name = {
            str(record.get("name")): str(record.get("name"))
            for record in records
            if isinstance(record.get("name"), str)
        }
        table_by_sql = {}
        for record in records:
            record_name = record.get("name")
            if not isinstance(record_name, str) or not record_name:
                continue
            sql_identifier = self._record_sql_identifier(record)
            if sql_identifier:
                table_by_sql[sql_identifier] = record_name

        relationships: List[Dict[str, Any]] = []
        relationship_names: set[str] = set()

        for record in records:
            from_table = record.get("name")
            if not isinstance(from_table, str) or not from_table:
                continue

            foreign_keys = record.get("foreignKeys")
            if not isinstance(foreign_keys, list):
                continue

            for fk in foreign_keys:
                if not isinstance(fk, dict):
                    continue

                columns = fk.get("columns")
                referenced_columns = fk.get("referencedColumns")
                if not isinstance(columns, list) or not isinstance(referenced_columns, list):
                    continue
                if len(columns) != len(referenced_columns) or len(columns) == 0:
                    continue

                target_table = None
                referenced_table_sql = fk.get("referencedTableSql")
                if isinstance(referenced_table_sql, str):
                    target_table = table_by_sql.get(referenced_table_sql)

                if not target_table:
                    referenced_table = fk.get("referencedTable")
                    if isinstance(referenced_table, str):
                        target_table = table_by_name.get(referenced_table)

                if not target_table:
                    continue

                for from_column, to_column in zip(columns, referenced_columns):
                    if not isinstance(from_column, str) or not isinstance(to_column, str):
                        continue

                    relationship_name = f"{from_table}_{from_column}_to_{target_table}_{to_column}"
                    if relationship_name in relationship_names:
                        continue

                    relationships.append(
                        {
                            "name": relationship_name,
                            "fromTable": from_table,
                            "fromColumn": from_column,
                            "toTable": target_table,
                            "toColumn": to_column,
                        }
                    )
                    relationship_names.add(relationship_name)

        return relationships

    def build_tmsl_schema(
        self,
        avro_schema: JsonNode,
        avro_record_type: Optional[str] = None,
        database_name: str = "",
        compatibility_level: int = 1605,
        emit_cloudevents_columns: bool = False,
    ) -> Dict[str, Any]:
        """Build a TMSL JSON document from an Avro schema document."""
        self.cache_named_types(avro_schema)
        records = self.resolve_records(avro_schema, avro_record_type)

        tables = [self.build_table(record, emit_cloudevents_columns) for record in records]
        first_table_name = str(tables[0].get("name", "Database")) if tables else "Database"
        database = database_name or first_table_name
        relationships = self.build_relationships(records)

        model: Dict[str, Any] = {
            "culture": "en-US",
            "tables": tables,
        }
        if relationships:
            model["relationships"] = relationships

        return {
            "createOrReplace": {
                "object": {"database": database},
                "database": {
                    "name": database,
                    "compatibilityLevel": compatibility_level,
                    "model": model,
                },
            }
        }

    def convert_avro_to_tmsl(
        self,
        avro_schema_path: str,
        avro_record_type: Optional[str],
        tmsl_file_path: str,
        database_name: str = "",
        compatibility_level: int = 1605,
        emit_cloudevents_columns: bool = False,
    ) -> None:
        """Convert an Avro schema file to a TMSL JSON file."""
        if not avro_schema_path:
            print("Please specify the avro schema file")
            sys.exit(1)

        with open(avro_schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)

        tmsl_schema = self.build_tmsl_schema(
            schema,
            avro_record_type=avro_record_type,
            database_name=database_name,
            compatibility_level=compatibility_level,
            emit_cloudevents_columns=emit_cloudevents_columns,
        )

        with open(tmsl_file_path, "w", encoding="utf-8") as f:
            json.dump(tmsl_schema, f, indent=2)


def convert_avro_to_tmsl(
    avro_schema_path: str,
    avro_record_type: Optional[str],
    tmsl_file_path: str,
    database_name: str = "",
    compatibility_level: int = 1605,
    emit_cloudevents_columns: bool = False,
) -> None:
    """Convert an Avro schema file to a TMSL JSON file."""
    converter = AvroToTmslConverter()
    converter.convert_avro_to_tmsl(
        avro_schema_path,
        avro_record_type,
        tmsl_file_path,
        database_name,
        compatibility_level,
        emit_cloudevents_columns,
    )
