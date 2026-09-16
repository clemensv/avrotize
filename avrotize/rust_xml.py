from typing import Any


def xml_wire_name(name: str, schema_obj: Any) -> str:
    """Resolve an XML local name, honoring ``altnames.xml``."""
    if isinstance(schema_obj, dict):
        altnames = schema_obj.get("altnames")
        if isinstance(altnames, dict) and "xml" in altnames:
            return altnames["xml"]
    return name


def xml_enum_wire_value(value: Any, enum_schema: Any) -> str:
    """Resolve an XML enum value, honoring ``altenums.xml``."""
    if isinstance(enum_schema, dict):
        altenums = enum_schema.get("altenums")
        if isinstance(altenums, dict):
            xml_values = altenums.get("xml")
            if isinstance(xml_values, dict) and str(value) in xml_values:
                return xml_values[str(value)]
    return str(value)
