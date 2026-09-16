"""Validate Tabular Model Scripting Language (TMSL) scripts locally.

Validation rules in this module are aligned with Microsoft TMSL object definitions:
- Tabular Model Scripting Language (TMSL) Reference
- CreateOrReplace command (TMSL)
- Database object (TMSL)
- Tables object (TMSL)

This validator performs structural/schema-like checks only; semantic validation
still requires execution against an XMLA endpoint.
"""

import json
from typing import Any, Dict, List


ALLOWED_TMSL_COMMANDS = {
    "create",
    "createOrReplace",
    "alter",
    "delete",
    "refresh",
    "mergePartitions",
    "sequence",
}

ALLOWED_READ_WRITE_MODES = {"readWrite", "readOnly", "readOnlyExclusive"}
ALLOWED_MODEL_DEFAULT_MODES = {"import", "directQuery", "default"}
ALLOWED_ALIGNMENT = {"default", "left", "right", "center"}
ALLOWED_SUMMARIZE_BY = {"default", "none", "sum", "min", "max", "count", "average", "distinctCount"}
ALLOWED_COLUMN_TYPES = {"data", "calculated", "rowNumber", "calculatedTableColumn"}
ALLOWED_COLUMN_DATA_TYPES = {
    "automatic",
    "string",
    "int64",
    "double",
    "dateTime",
    "decimal",
    "boolean",
    "binary",
    "unknown",
    "variant",
}


class TmslValidationError(Exception):
    """Raised when TMSL validation fails."""


class TmslValidator:
    """Local structural validator for TMSL scripts."""

    def __init__(self) -> None:
        self.errors: List[str] = []

    def _err(self, path: str, message: str) -> None:
        self.errors.append(f"{path}: {message}")

    def _expect_type(self, value: Any, expected_type: type, path: str) -> bool:
        if not isinstance(value, expected_type):
            self._err(path, f"Expected {expected_type.__name__}, got {type(value).__name__}")
            return False
        return True

    def _check_allowed_keys(self, obj: Dict[str, Any], allowed_keys: set[str], path: str) -> None:
        for key in obj.keys():
            if key not in allowed_keys:
                self._err(path, f"Unexpected property '{key}'")

    def _validate_annotation(self, annotation: Any, path: str) -> None:
        if not self._expect_type(annotation, dict, path):
            return
        self._check_allowed_keys(annotation, {"name", "value"}, path)
        if not isinstance(annotation.get("name"), str) or not annotation.get("name"):
            self._err(f"{path}.name", "Annotation name must be a non-empty string")
        if "value" in annotation:
            value = annotation["value"]
            if isinstance(value, str):
                return
            if isinstance(value, list) and all(isinstance(item, str) for item in value):
                return
            self._err(f"{path}.value", "Annotation value must be a string or an array of strings")

    def _validate_annotations(self, annotations: Any, path: str) -> None:
        if not self._expect_type(annotations, list, path):
            return
        for i, annotation in enumerate(annotations):
            self._validate_annotation(annotation, f"{path}[{i}]")

    def _validate_column(self, column: Any, path: str) -> None:
        if not self._expect_type(column, dict, path):
            return

        allowed_keys = {
            "name",
            "dataType",
            "dataCategory",
            "description",
            "isHidden",
            "isUnique",
            "isKey",
            "isNullable",
            "alignment",
            "tableDetailPosition",
            "isDefaultLabel",
            "isDefaultImage",
            "summarizeBy",
            "type",
            "formatString",
            "isAvailableInMdx",
            "keepUniqueRows",
            "displayOrdinal",
            "sourceProviderType",
            "displayFolder",
            "sourceColumn",
            "sortByColumn",
            "isNameInferred",
            "isDataTypeInferred",
            "columnOriginTable",
            "columnOriginColumn",
            "expression",
            "annotations",
        }
        self._check_allowed_keys(column, allowed_keys, path)

        if not isinstance(column.get("name"), str) or not column.get("name"):
            self._err(f"{path}.name", "Column name must be a non-empty string")

        data_type = column.get("dataType")
        if not isinstance(data_type, str) or data_type not in ALLOWED_COLUMN_DATA_TYPES:
            self._err(
                f"{path}.dataType",
                "Column dataType must be one of: " + ", ".join(sorted(ALLOWED_COLUMN_DATA_TYPES)),
            )

        if "alignment" in column and column["alignment"] not in ALLOWED_ALIGNMENT:
            self._err(f"{path}.alignment", "Invalid alignment value")

        if "summarizeBy" in column and column["summarizeBy"] not in ALLOWED_SUMMARIZE_BY:
            self._err(f"{path}.summarizeBy", "Invalid summarizeBy value")

        if "type" in column and column["type"] not in ALLOWED_COLUMN_TYPES:
            self._err(f"{path}.type", "Invalid column type value")

        if "annotations" in column:
            self._validate_annotations(column["annotations"], f"{path}.annotations")

    def _validate_table(self, table: Any, path: str) -> None:
        if not self._expect_type(table, dict, path):
            return

        allowed_keys = {
            "name",
            "dataCategory",
            "description",
            "isHidden",
            "partitions",
            "annotations",
            "columns",
            "measures",
            "hierarchies",
        }
        self._check_allowed_keys(table, allowed_keys, path)

        if not isinstance(table.get("name"), str) or not table.get("name"):
            self._err(f"{path}.name", "Table name must be a non-empty string")

        columns = table.get("columns")
        if columns is None:
            self._err(f"{path}.columns", "Missing 'columns' collection")
        elif self._expect_type(columns, list, f"{path}.columns"):
            for i, column in enumerate(columns):
                self._validate_column(column, f"{path}.columns[{i}]")

        if "annotations" in table:
            self._validate_annotations(table["annotations"], f"{path}.annotations")

    def _validate_model(self, model: Any, path: str) -> None:
        if not self._expect_type(model, dict, path):
            return

        # Per TMSL reference schema, model has additionalProperties: false.
        allowed_keys = {
            "name",
            "description",
            "storageLocation",
            "defaultMode",
            "defaultDataView",
            "culture",
            "collation",
            "annotations",
            "tables",
            "relationships",
            "dataSources",
            "perspectives",
            "cultures",
            "roles",
            "functions",
        }
        self._check_allowed_keys(model, allowed_keys, path)

        if "defaultMode" in model and model["defaultMode"] not in ALLOWED_MODEL_DEFAULT_MODES:
            self._err(f"{path}.defaultMode", "Invalid defaultMode value")

        if "culture" in model and not isinstance(model["culture"], str):
            self._err(f"{path}.culture", "culture must be a string")

        tables = model.get("tables")
        if tables is None:
            self._err(f"{path}.tables", "Missing 'tables' collection")
        elif self._expect_type(tables, list, f"{path}.tables"):
            for i, table in enumerate(tables):
                self._validate_table(table, f"{path}.tables[{i}]")

        if "annotations" in model:
            self._validate_annotations(model["annotations"], f"{path}.annotations")

    def _validate_database(self, database: Any, path: str) -> None:
        if not self._expect_type(database, dict, path):
            return

        # Database schema also uses additionalProperties: false.
        allowed_keys = {
            "name",
            "id",
            "description",
            "compatibilityLevel",
            "readWriteMode",
            "model",
            "annotations",
        }
        self._check_allowed_keys(database, allowed_keys, path)

        if not isinstance(database.get("name"), str) or not database.get("name"):
            self._err(f"{path}.name", "Database name must be a non-empty string")

        compatibility_level = database.get("compatibilityLevel")
        if compatibility_level is not None:
            if not isinstance(compatibility_level, int):
                self._err(f"{path}.compatibilityLevel", "compatibilityLevel must be an integer")
            elif compatibility_level < 1200:
                self._err(f"{path}.compatibilityLevel", "compatibilityLevel must be 1200 or higher for TMSL")

        if "readWriteMode" in database and database["readWriteMode"] not in ALLOWED_READ_WRITE_MODES:
            self._err(f"{path}.readWriteMode", "Invalid readWriteMode value")

        if "model" in database:
            self._validate_model(database["model"], f"{path}.model")

        if "annotations" in database:
            self._validate_annotations(database["annotations"], f"{path}.annotations")

    def _validate_create_or_replace(self, command: Any, path: str) -> None:
        if not self._expect_type(command, dict, path):
            return

        allowed_keys = {"object", "database", "dataSource", "table", "partition", "role"}
        self._check_allowed_keys(command, allowed_keys, path)

        target = command.get("object")
        if not self._expect_type(target, dict, f"{path}.object"):
            return
        if not isinstance(target.get("database"), str) or not target.get("database"):
            self._err(f"{path}.object.database", "Target database must be a non-empty string")

        if "database" in command:
            self._validate_database(command["database"], f"{path}.database")

    def validate_tmsl(self, document: Any) -> List[str]:
        """Validate a parsed TMSL document and return a list of errors."""
        self.errors = []

        if not self._expect_type(document, dict, "$"):
            return self.errors

        command_keys = [key for key in document.keys() if key in ALLOWED_TMSL_COMMANDS]
        if len(command_keys) != 1:
            self._err("$", "TMSL must contain exactly one top-level command")
            return self.errors

        command_name = command_keys[0]
        for key in document.keys():
            if key != command_name:
                self._err("$", f"Unexpected top-level property '{key}'")

        if command_name == "createOrReplace":
            self._validate_create_or_replace(document[command_name], f"$.{command_name}")

        return self.errors

    def validate_file(self, tmsl_file_path: str) -> List[str]:
        """Validate a TMSL JSON file and return a list of errors."""
        with open(tmsl_file_path, "r", encoding="utf-8") as f:
            document = json.load(f)
        return self.validate_tmsl(document)


def validate_tmsl_file(tmsl_file_path: str) -> List[str]:
    """Validate a TMSL JSON file and return a list of errors."""
    validator = TmslValidator()
    return validator.validate_file(tmsl_file_path)


def validate_tmsl(tmsl_file_path: str, quiet: bool = False) -> None:
    """CLI command entrypoint for local TMSL validation."""
    errors = validate_tmsl_file(tmsl_file_path)

    if not quiet:
        if errors:
            for error in errors:
                print(f"✗ {error}")
            print(f"\nValidation summary: {len(errors)} error(s)")
        else:
            print("✓ Valid TMSL")

    if errors:
        exit(1)
