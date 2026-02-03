"""Validates JSON instances against Avro schemas.

This module implements JSON validation against Avro schemas according to
the Avrotize Schema Specification (avrotize-schema.md). It validates:
- Primitive types: null, boolean, int, long, float, double, bytes, string
- Logical types: decimal, uuid, date, time, timestamp, duration
- Complex types: record, enum, array, map, fixed
- Type unions
"""

import base64
import re
from typing import Any, Dict, List, Tuple, Union

# Type alias for Avro schema
AvroSchema = Union[str, Dict[str, Any], List[Any]]


class AvroValidationError(Exception):
    """Exception raised when JSON instance doesn't match Avro schema."""

    def __init__(self, message: str, path: str = "#"):
        self.message = message
        self.path = path
        super().__init__(f"{message} at {path}")


class AvroValidator:
    """Validates JSON instances against Avro schemas."""

    # RFC 3339 patterns for logical type validation
    UUID_PATTERN = re.compile(
        r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    )
    DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    TIME_PATTERN = re.compile(r'^\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?$')
    DATETIME_PATTERN = re.compile(
        r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?$'
    )
    DURATION_PATTERN = re.compile(
        r'^P(\d+Y)?(\d+M)?(\d+D)?(T(\d+H)?(\d+M)?(\d+(\.\d+)?S)?)?$'
    )
    DECIMAL_PATTERN = re.compile(r'^[+-]?\d+(\.\d+)?$')

    # Int32 and Int64 bounds
    INT32_MIN = -(2**31)
    INT32_MAX = 2**31 - 1
    INT64_MIN = -(2**63)
    INT64_MAX = 2**63 - 1

    def __init__(self, schema: AvroSchema):
        """Initialize the validator with an Avro schema.

        Args:
            schema: The Avro schema to validate against
        """
        self.schema = schema
        self.named_types: Dict[str, Dict[str, Any]] = {}
        self._collect_named_types(schema, '')

    def _collect_named_types(self, schema: AvroSchema, current_namespace: str) -> None:
        """Collects all named types from the schema for reference resolution.

        Args:
            schema: The schema to scan
            current_namespace: The current namespace context
        """
        if isinstance(schema, dict):
            schema_type = schema.get('type')
            if schema_type in ('record', 'enum', 'fixed'):
                namespace = schema.get('namespace', current_namespace)
                name = schema.get('name', '')
                if namespace:
                    fullname = f"{namespace}.{name}"
                else:
                    fullname = name
                self.named_types[fullname] = schema
                self.named_types[name] = schema  # Also store short name

                # Recurse into record fields
                if schema_type == 'record':
                    for field in schema.get('fields', []):
                        self._collect_named_types(field.get('type', 'null'), namespace)

            elif schema_type == 'array':
                self._collect_named_types(schema.get('items', 'null'), current_namespace)
            elif schema_type == 'map':
                self._collect_named_types(schema.get('values', 'null'), current_namespace)

        elif isinstance(schema, list):
            # Type union
            for item in schema:
                self._collect_named_types(item, current_namespace)

    def validate(self, instance: Any) -> None:
        """Validates a JSON instance against the schema.

        Args:
            instance: The JSON value to validate

        Raises:
            AvroValidationError: If the instance doesn't match the schema
        """
        self._validate(instance, self.schema, "#")

    def _validate(self, instance: Any, schema: AvroSchema, path: str) -> None:
        """Internal validation method.

        Args:
            instance: The JSON value to validate
            schema: The schema to validate against
            path: JSON pointer path for error messages

        Raises:
            AvroValidationError: If validation fails
        """
        if isinstance(schema, str):
            self._validate_primitive_or_reference(instance, schema, path)
        elif isinstance(schema, dict):
            self._validate_complex(instance, schema, path)
        elif isinstance(schema, list):
            self._validate_union(instance, schema, path)
        else:
            raise AvroValidationError(f"Invalid schema type: {type(schema)}", path)

    def _validate_primitive_or_reference(
        self, instance: Any, schema: str, path: str
    ) -> None:
        """Validates against a primitive type or named type reference.

        Args:
            instance: The JSON value to validate
            schema: The primitive type name or named type reference
            path: JSON pointer path for error messages
        """
        # Check if it's a named type reference
        if schema in self.named_types:
            self._validate_complex(instance, self.named_types[schema], path)
            return

        # Primitive type validation
        if schema == 'null':
            if instance is not None:
                raise AvroValidationError(f"Expected null, got {type(instance).__name__}", path)

        elif schema == 'boolean':
            if not isinstance(instance, bool):
                raise AvroValidationError(f"Expected boolean, got {type(instance).__name__}", path)

        elif schema == 'int':
            if not isinstance(instance, int) or isinstance(instance, bool):
                raise AvroValidationError(f"Expected int, got {type(instance).__name__}", path)
            if not (self.INT32_MIN <= instance <= self.INT32_MAX):
                raise AvroValidationError(
                    f"Integer {instance} out of int32 range [{self.INT32_MIN}, {self.INT32_MAX}]",
                    path
                )

        elif schema == 'long':
            if not isinstance(instance, int) or isinstance(instance, bool):
                raise AvroValidationError(f"Expected long, got {type(instance).__name__}", path)
            if not (self.INT64_MIN <= instance <= self.INT64_MAX):
                raise AvroValidationError(
                    f"Integer {instance} out of int64 range [{self.INT64_MIN}, {self.INT64_MAX}]",
                    path
                )

        elif schema == 'float':
            if not isinstance(instance, (int, float)) or isinstance(instance, bool):
                raise AvroValidationError(f"Expected float, got {type(instance).__name__}", path)

        elif schema == 'double':
            if not isinstance(instance, (int, float)) or isinstance(instance, bool):
                raise AvroValidationError(f"Expected double, got {type(instance).__name__}", path)

        elif schema == 'bytes':
            # In JSON, bytes are represented as strings with unicode escapes
            if not isinstance(instance, str):
                raise AvroValidationError(f"Expected bytes (string), got {type(instance).__name__}", path)

        elif schema == 'string':
            if not isinstance(instance, str):
                raise AvroValidationError(f"Expected string, got {type(instance).__name__}", path)

        else:
            raise AvroValidationError(f"Unknown primitive type: {schema}", path)

    def _validate_complex(self, instance: Any, schema: Dict[str, Any], path: str) -> None:
        """Validates against a complex type schema.

        Args:
            instance: The JSON value to validate
            schema: The complex type schema
            path: JSON pointer path for error messages
        """
        schema_type = schema.get('type')

        if schema_type == 'record':
            self._validate_record(instance, schema, path)
        elif schema_type == 'enum':
            self._validate_enum(instance, schema, path)
        elif schema_type == 'array':
            self._validate_array(instance, schema, path)
        elif schema_type == 'map':
            self._validate_map(instance, schema, path)
        elif schema_type == 'fixed':
            self._validate_fixed(instance, schema, path)
        elif schema_type in ('null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'):
            # Complex form of primitive type, possibly with logical type
            logical_type = schema.get('logicalType')
            if logical_type:
                self._validate_logical_type(instance, schema, logical_type, path)
            else:
                self._validate_primitive_or_reference(instance, schema_type, path)
        else:
            raise AvroValidationError(f"Unknown complex type: {schema_type}", path)

    def _validate_record(self, instance: Any, schema: Dict[str, Any], path: str) -> None:
        """Validates a record type.

        Args:
            instance: The JSON value to validate
            schema: The record schema
            path: JSON pointer path for error messages
        """
        if not isinstance(instance, dict):
            raise AvroValidationError(
                f"Expected object for record '{schema.get('name')}', got {type(instance).__name__}",
                path
            )

        fields = schema.get('fields', [])
        field_names = set()

        for field in fields:
            field_name = field.get('name')
            field_names.add(field_name)

            # Check for altnames (JSON encoding)
            json_name = field_name
            altnames = field.get('altnames', {})
            if 'json' in altnames:
                json_name = altnames['json']

            if json_name in instance:
                field_path = f"{path}/{json_name}"
                self._validate(instance[json_name], field.get('type', 'null'), field_path)
            elif field_name in instance:
                field_path = f"{path}/{field_name}"
                self._validate(instance[field_name], field.get('type', 'null'), field_path)
            elif 'default' not in field:
                # Check if the field type allows null
                field_type = field.get('type', 'null')
                if not self._type_allows_null(field_type):
                    raise AvroValidationError(
                        f"Missing required field '{field_name}'",
                        path
                    )

    def _type_allows_null(self, schema: AvroSchema) -> bool:
        """Check if a type allows null values."""
        if schema == 'null':
            return True
        if isinstance(schema, list):
            return 'null' in schema or any(
                (isinstance(s, dict) and s.get('type') == 'null') for s in schema
            )
        return False

    def _validate_enum(self, instance: Any, schema: Dict[str, Any], path: str) -> None:
        """Validates an enum type.

        Args:
            instance: The JSON value to validate
            schema: The enum schema
            path: JSON pointer path for error messages
        """
        if not isinstance(instance, str):
            raise AvroValidationError(
                f"Expected string for enum '{schema.get('name')}', got {type(instance).__name__}",
                path
            )

        symbols = schema.get('symbols', [])
        
        # Check direct symbol match
        if instance in symbols:
            return

        # Check altsymbols for JSON encoding
        altsymbols = schema.get('altsymbols', {}).get('json', {})
        for symbol, alt_value in altsymbols.items():
            if instance == alt_value:
                return

        raise AvroValidationError(
            f"'{instance}' is not a valid symbol for enum '{schema.get('name')}'. Valid symbols: {symbols}",
            path
        )

    def _validate_array(self, instance: Any, schema: Dict[str, Any], path: str) -> None:
        """Validates an array type.

        Args:
            instance: The JSON value to validate
            schema: The array schema
            path: JSON pointer path for error messages
        """
        if not isinstance(instance, list):
            raise AvroValidationError(
                f"Expected array, got {type(instance).__name__}",
                path
            )

        items_schema = schema.get('items', 'null')
        for i, item in enumerate(instance):
            item_path = f"{path}/{i}"
            self._validate(item, items_schema, item_path)

    def _validate_map(self, instance: Any, schema: Dict[str, Any], path: str) -> None:
        """Validates a map type.

        Args:
            instance: The JSON value to validate
            schema: The map schema
            path: JSON pointer path for error messages
        """
        if not isinstance(instance, dict):
            raise AvroValidationError(
                f"Expected object for map, got {type(instance).__name__}",
                path
            )

        values_schema = schema.get('values', 'null')
        for key, value in instance.items():
            if not isinstance(key, str):
                raise AvroValidationError(
                    f"Map keys must be strings, got {type(key).__name__}",
                    path
                )
            value_path = f"{path}/{key}"
            self._validate(value, values_schema, value_path)

    def _validate_fixed(self, instance: Any, schema: Dict[str, Any], path: str) -> None:
        """Validates a fixed type.

        Args:
            instance: The JSON value to validate
            schema: The fixed schema
            path: JSON pointer path for error messages
        """
        if not isinstance(instance, str):
            raise AvroValidationError(
                f"Expected string for fixed '{schema.get('name')}', got {type(instance).__name__}",
                path
            )

        size = schema.get('size', 0)
        # In JSON, fixed values are represented as unicode escape sequences
        # Each byte is represented as a unicode character
        if len(instance) != size:
            raise AvroValidationError(
                f"Fixed '{schema.get('name')}' requires exactly {size} bytes, got {len(instance)}",
                path
            )

    def _validate_union(self, instance: Any, schema: List[Any], path: str) -> None:
        """Validates against a type union.

        Args:
            instance: The JSON value to validate
            schema: The union schema (list of types)
            path: JSON pointer path for error messages
        """
        errors = []
        for union_type in schema:
            try:
                self._validate(instance, union_type, path)
                return  # Validation succeeded for this type
            except AvroValidationError as e:
                errors.append(str(e))

        # None of the union types matched
        type_names = [self._get_type_name(t) for t in schema]
        raise AvroValidationError(
            f"Value doesn't match any type in union {type_names}",
            path
        )

    def _get_type_name(self, schema: AvroSchema) -> str:
        """Gets a human-readable name for a schema type."""
        if isinstance(schema, str):
            return schema
        elif isinstance(schema, dict):
            schema_type = schema.get('type', 'unknown')
            name = schema.get('name')
            if name:
                return f"{schema_type}:{name}"
            return schema_type
        elif isinstance(schema, list):
            return f"union[{', '.join(self._get_type_name(t) for t in schema)}]"
        return 'unknown'

    def _validate_logical_type(
        self, instance: Any, schema: Dict[str, Any], logical_type: str, path: str
    ) -> None:
        """Validates a logical type.

        Args:
            instance: The JSON value to validate
            schema: The schema with logical type
            logical_type: The logical type name
            path: JSON pointer path for error messages
        """
        base_type = schema.get('type')

        if logical_type == 'decimal':
            self._validate_decimal(instance, schema, path)

        elif logical_type == 'uuid':
            if base_type != 'string':
                raise AvroValidationError(f"uuid logical type requires string base type", path)
            if not isinstance(instance, str):
                raise AvroValidationError(f"Expected string for uuid, got {type(instance).__name__}", path)
            if not self.UUID_PATTERN.match(instance):
                raise AvroValidationError(f"Invalid UUID format: {instance}", path)

        elif logical_type == 'date':
            if base_type == 'int':
                if not isinstance(instance, int) or isinstance(instance, bool):
                    raise AvroValidationError(f"Expected int for date, got {type(instance).__name__}", path)
            elif base_type == 'string':
                if not isinstance(instance, str):
                    raise AvroValidationError(f"Expected string for date, got {type(instance).__name__}", path)
                if not self.DATE_PATTERN.match(instance):
                    raise AvroValidationError(f"Invalid date format (expected YYYY-MM-DD): {instance}", path)
            else:
                raise AvroValidationError(f"date logical type requires int or string base type", path)

        elif logical_type in ('time-millis', 'time-micros'):
            if base_type in ('int', 'long'):
                if not isinstance(instance, int) or isinstance(instance, bool):
                    raise AvroValidationError(f"Expected int for {logical_type}, got {type(instance).__name__}", path)
            elif base_type == 'string':
                if not isinstance(instance, str):
                    raise AvroValidationError(f"Expected string for {logical_type}, got {type(instance).__name__}", path)
                if not self.TIME_PATTERN.match(instance):
                    raise AvroValidationError(f"Invalid time format: {instance}", path)
            else:
                raise AvroValidationError(f"{logical_type} logical type requires int, long, or string base type", path)

        elif logical_type in ('timestamp-millis', 'timestamp-micros', 'local-timestamp-millis', 'local-timestamp-micros'):
            if base_type == 'long':
                if not isinstance(instance, int) or isinstance(instance, bool):
                    raise AvroValidationError(f"Expected long for {logical_type}, got {type(instance).__name__}", path)
            elif base_type == 'string':
                if not isinstance(instance, str):
                    raise AvroValidationError(f"Expected string for {logical_type}, got {type(instance).__name__}", path)
                if not self.DATETIME_PATTERN.match(instance):
                    raise AvroValidationError(f"Invalid datetime format: {instance}", path)
            else:
                raise AvroValidationError(f"{logical_type} logical type requires long or string base type", path)

        elif logical_type == 'duration':
            if base_type == 'fixed':
                self._validate_fixed(instance, schema, path)
            elif base_type == 'string':
                if not isinstance(instance, str):
                    raise AvroValidationError(f"Expected string for duration, got {type(instance).__name__}", path)
                if not self.DURATION_PATTERN.match(instance):
                    raise AvroValidationError(f"Invalid duration format: {instance}", path)
            else:
                raise AvroValidationError(f"duration logical type requires fixed or string base type", path)

        else:
            # Unknown logical type - fall back to base type validation
            self._validate_primitive_or_reference(instance, base_type, path)

    def _validate_decimal(self, instance: Any, schema: Dict[str, Any], path: str) -> None:
        """Validates a decimal logical type.

        Args:
            instance: The JSON value to validate
            schema: The decimal schema
            path: JSON pointer path for error messages
        """
        base_type = schema.get('type')

        if base_type == 'string':
            if not isinstance(instance, str):
                raise AvroValidationError(f"Expected string for decimal, got {type(instance).__name__}", path)
            if not self.DECIMAL_PATTERN.match(instance):
                raise AvroValidationError(f"Invalid decimal format: {instance}", path)
        elif base_type == 'bytes':
            if not isinstance(instance, str):
                raise AvroValidationError(f"Expected bytes (string) for decimal, got {type(instance).__name__}", path)
        elif base_type == 'fixed':
            self._validate_fixed(instance, schema, path)
        else:
            raise AvroValidationError(f"decimal logical type requires bytes, fixed, or string base type", path)


def validate_json_against_avro(instance: Any, schema: AvroSchema) -> List[str]:
    """Validates a JSON instance against an Avro schema.

    Args:
        instance: The JSON value to validate
        schema: The Avro schema

    Returns:
        List of validation error messages (empty if valid)
    """
    validator = AvroValidator(schema)
    try:
        validator.validate(instance)
        return []
    except AvroValidationError as e:
        return [str(e)]
