# encoding: utf-8
"""
json_structure_schema_validator.py

A validation tool for the experimental JSON Structure Core specification
published by C. Vasters (Microsoft) in February 2025.
This version optionally supports the JSON Structure JSONStructureImport extension via the
--allowimport flag and allows passing a mapping of URI to filenames via the
--importmap option.

Usage:
    python json_structure_schema_validator.py [--metaschema] [--allowimport] [--importmap URI=filename ...] <path_to_json_file>

The --metaschema parameter allows '$' in property names.
The --allowimport parameter enables processing of the $import and $importdefs keywords.
If a URI mapping is provided via --importmap, the external schema file is loaded from the given path.
"""

import sys
import json
import re
from urllib.parse import urlparse


class JSONStructureSchemaCoreValidator:
    """
    Validates JSON Structure Core documents for conformance with the specification.
    Provides error messages annotated with estimated line and column numbers.
    Optionally supports the JSON Structure JSONStructureImport extension if allow_import is True.
    """

    ABSOLUTE_URI_REGEX = re.compile(r'^[a-zA-Z][a-zA-Z0-9+\-.]*://')
    MAP_KEY_REGEX = re.compile(r'^[A-Za-z0-9._-]+$')
    RESERVED_KEYWORDS = {
        "definitions", "$extends", "$id", "$ref", "$root", "$schema", "$uses",
        "$offers", "abstract", "additionalProperties", "const", "default",
        "description", "enum", "examples", "format", "items", "maxLength",
        "name", "precision", "properties", "required", "scale", "type",
        "values"
    }
    PRIMITIVE_TYPES = {
        "string", "number", "boolean", "null", "int32", "uint32", "int64",
        "uint64", "int128", "uint128", "float", "double", "decimal", "date",
        "datetime", "time", "duration", "uuid", "uri", "binary", "jsonpointer",
        "any"
    }
    COMPOUND_TYPES = {"object", "array", "set", "map", "tuple", "choice"}

    def __init__(self, allow_dollar=False, allow_import=False, import_map=None):
        """
        Initializes a validator instance.
        :param allow_dollar: Boolean flag to allow '$' in property names.
        :param allow_import: Boolean flag to enable processing of $import/$importdefs.
        :param import_map: Dictionary mapping URI to local filenames.
        """
        self.errors = []
        self.doc = None
        self.source_text = None
        self.allow_import = allow_import
        self.import_map = import_map if import_map is not None else {}
        if allow_dollar:
            self.identifier_regex = re.compile(r'^[A-Za-z_$][A-Za-z0-9_$]*$')
        else:
            self.identifier_regex = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

    def validate(self, doc, source_text=None):
        """
        Main entry point for validating a JSON Structure Core document.
        :param doc: A dict parsed from JSON.
        :param source_text: The original JSON text for computing line and column.
        :return: List of error messages, empty if valid.
        """
        self.errors = []
        self.doc = doc
        self.source_text = source_text

        if not isinstance(doc, dict):
            self._err("Root of the document must be a JSON object.", "#")
            return self.errors

        # Process $import and $importdefs keywords recursively.
        self._process_imports(doc, "#")

        self._check_required_top_level_keywords(doc, "#")
        if "$schema" in doc:
            self._check_is_absolute_uri(doc["$schema"], "$schema", "#/$schema")
        if "$id" in doc:
            self._check_is_absolute_uri(doc["$id"], "$id", "#/$id")
        if "type" in doc and "$root" in doc:
            self._err("Document cannot have both 'type' at root and '$root' at the same time.", "#")
        if "type" in doc:
            self._validate_schema(doc, is_root=True, path="#", name_in_namespace=None)
        if "$root" in doc:
            self._check_json_pointer(doc["$root"], self.doc, "$root")
        if "definitions" in doc:
            if not isinstance(doc["definitions"], dict):
                self._err("definitions must be an object.", "#/definitions")
            else:
                self._validate_namespace(doc["definitions"], "#/definitions")
        if "$offers" in doc:
            self._check_offers(doc["$offers"], "#/$offers")
        return self.errors

    def _check_required_top_level_keywords(self, obj, location):
        """
        Ensures $schema and $id are present at the root level.
        """
        if "$schema" not in obj:
            self._err("Missing required '$schema' keyword at root.", location)
        if "$id" not in obj:
            self._err("Missing required '$id' keyword at root.", location)

    def _check_is_absolute_uri(self, value, keyword_name, location):
        """
        Checks if a given string is an absolute URI.
        """
        if not isinstance(value, str):
            self._err(f"'{keyword_name}' must be a string.", location)
            return
        if not self.ABSOLUTE_URI_REGEX.search(value):
            self._err(f"'{keyword_name}' must be an absolute URI.", location)

    def _process_imports(self, obj, path):
        """
        Recursively processes $import and $importdefs keywords.
        If allow_import is False, an error is reported.
        Otherwise, external schemas are fetched and their definitions merged into the current object.
        This merging is done in-place so that imported definitions appear as if they were defined locally.
        """
        if isinstance(obj, dict):
            # Process import keywords at current level.
            for key in list(obj.keys()):
                if key in ("$import", "$importdefs"):
                    if not self.allow_import:
                        self._err(f"JSONStructureImport keyword '{key}' encountered but allow_import not enabled.", f"{path}/{key}")
                        continue
                    uri = obj[key]
                    if not isinstance(uri, str):
                        self._err(f"JSONStructureImport keyword '{key}' value must be a string URI.", f"{path}/{key}")
                        continue
                    if not self.ABSOLUTE_URI_REGEX.search(uri):
                        self._err(f"JSONStructureImport keyword '{key}' value must be an absolute URI.", f"{path}/{key}")
                        continue
                    external = self._fetch_external_schema(uri)
                    if external is None:
                        self._err(f"Unable to fetch external schema from {uri}.", f"{path}/{key}")
                        continue
                    if key == "$import":
                        imported_defs = {}
                        # JSONStructureImport root type if available.
                        if "type" in external and "name" in external:
                            imported_defs[external["name"]] = external
                        # Also import definitions from definitions if available.
                        if "definitions" in external and isinstance(external["definitions"], dict):
                            imported_defs.update(external["definitions"])
                    else:  # $importdefs
                        if "definitions" in external and isinstance(external["definitions"], dict):
                            imported_defs = external["definitions"]
                        else:
                            imported_defs = {}
                    # Merge imported definitions directly into the current object.
                    for k, v in imported_defs.items():
                        if k not in obj:
                            obj[k] = v
                    del obj[key]
            # Recurse into all values.
            for key, value in obj.items():
                self._process_imports(value, f"{path}/{key}")
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                self._process_imports(item, f"{path}[{idx}]")

    def _merge_definitions(self, local, imported, path):
        """
        Merges imported definitions into the local namespace.
        Local definitions take precedence over imported definitions.
        (This method is not used in the new merging approach.)
        """
        for key, value in imported.items():
            if key not in local:
                local[key] = value

    def _fetch_external_schema(self, uri):
        """
        Fetches an external schema from a URI.
        If a mapping is provided and contains the URI, loads from the given file.
        Otherwise, uses a simulated lookup.
        """
        if uri in self.import_map:
            try:
                with open(self.import_map[uri], "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self._err(f"Failed to load imported schema from {self.import_map[uri]}: {e}", "#/import")
                return None
        # Simulated external schemas for testing purposes.
        EXTERNAL_SCHEMAS = {
            "https://example.com/people.json": {
                "$schema": "https://json-structure.org/meta/core/v0/#",
                "$id": "https://example.com/people.json",
                "name": "Person",
                "type": "object",
                "properties": {
                    "firstName": {"type": "string"},
                    "lastName": {"type": "string"},
                    "address": {"$ref": "#/definitions/Address"}
                },
                "definitions": {
                    "Address": {
                        "name": "Address",
                        "type": "object",
                        "properties": {
                            "street": {"type": "string"},
                            "city": {"type": "string"}
                        }
                    }
                }
            },
            "https://example.com/address.json": {
                "$schema": "https://json-structure.org/meta/core/v0/#",
                "$id": "https://example.com/address.json",
                "name": "Address",
                "type": "object",
                "properties": {
                    "street": {"type": "string"},
                    "city": {"type": "string"}
                }
            }
        }
        return EXTERNAL_SCHEMAS.get(uri)

    def _validate_namespace(self, obj, path):
        """
        Recursively validates objects in definitions as either a namespace or a schema.
        """
        if not isinstance(obj, dict):
            self._err(f"{path} must be an object.", path)
            return
        for k, v in obj.items():
            subpath = f"{path}/{k}"
            if isinstance(v, dict) and ("type" in v or "$ref" in v):
                self._validate_schema(v, is_root=False, path=subpath, name_in_namespace=k)
            else:
                if not isinstance(v, dict):
                    self._err(f"{subpath} is not a valid namespace or schema object.", subpath)
                else:
                    self._validate_namespace(v, subpath)

    def _validate_schema(self, schema_obj, is_root=False, path="", name_in_namespace=None):
        """
        Validates an individual schema object.
        """
        if not isinstance(schema_obj, dict):
            self._err(f"{path} must be an object to be a schema.", path)
            return
        if is_root and "type" in schema_obj and "name" not in schema_obj:
            if not isinstance(schema_obj["type"], list):
                self._err("Root schema with 'type' must have a 'name' property.", path)
        if "name" in schema_obj:
            if not isinstance(schema_obj["name"], str):
                self._err(f"'name' must be a string.", path + "/name")
            else:
                if not self.identifier_regex.match(schema_obj["name"]):
                    self._err(f"'name' must match the identifier pattern.", path + "/name")
        if "abstract" in schema_obj:
            if not isinstance(schema_obj["abstract"], bool):
                self._err(f"'abstract' keyword must be boolean.", path + "/abstract")
        if "$extends" in schema_obj:
            if not isinstance(schema_obj["$extends"], str):
                self._err(f"'$extends' must be a JSON pointer string.", path + "/$extends")
            else:
                self._check_json_pointer(schema_obj["$extends"], self.doc, path + "/$extends")
        if "type" not in schema_obj and "$ref" not in schema_obj:
            self._err("Missing required 'type' or '$ref' in schema object.", path)
            return
        if "type" in schema_obj and "$ref" in schema_obj:
            self._err("Cannot have both 'type' and '$ref'.", path)
            return
        if "$ref" in schema_obj:
            if not isinstance(schema_obj["$ref"], str):
                self._err("'$ref' must be a string.", path + "/$ref")
            else:
                self._check_json_pointer(schema_obj["$ref"], self.doc, path + "/$ref")
            return
        if "type" in schema_obj:
            tval = schema_obj["type"]
            if isinstance(tval, list):
                if not tval:
                    self._err("Type union cannot be empty.", path + "/type")
                else:
                    for idx, union_item in enumerate(tval):
                        self._check_union_type_item(union_item, f"{path}/type[{idx}]")
            elif isinstance(tval, dict):
                if "$ref" not in tval:
                    if "type" in tval or "properties" in tval:
                        self._validate_schema(tval, is_root=False, path=f"{path}/type(inline)")
                    else:
                        self._err("Type dict must have '$ref' or be a valid schema object.", path + "/type")
                else:
                    self._check_json_pointer(tval["$ref"], self.doc, path + "/type/$ref")
            else:
                if not isinstance(tval, str):
                    self._err("Type must be a string, list, or object with $ref.", path + "/type")
                else:
                    if (tval not in self.PRIMITIVE_TYPES and
                        tval not in self.COMPOUND_TYPES):
                        self._err(f"Type '{tval}' is not a recognized primitive or compound type.", path + "/type")
                    else:
                        if tval == "any":
                            pass
                        elif tval == "object":
                            self._check_object_schema(schema_obj, path)
                        elif tval == "array":
                            self._check_array_schema(schema_obj, path)
                        elif tval == "set":
                            self._check_set_schema(schema_obj, path)
                        elif tval == "map":
                            self._check_map_schema(schema_obj, path)
                        elif tval == "tuple":
                            self._check_tuple_schema(schema_obj, path)
                        elif tval == "choice":
                            self._check_choice_schema(schema_obj, path)
                        else:
                            self._check_primitive_schema(schema_obj, path)
        if "required" in schema_obj:
            if "type" in schema_obj and isinstance(schema_obj["type"], str):
                if schema_obj["type"] != "object":
                    self._err("'required' can only appear in an object schema.", path + "/required")
        if "additionalProperties" in schema_obj:
            if "type" in schema_obj and isinstance(schema_obj["type"], str):
                if schema_obj["type"] != "object":
                    self._err("'additionalProperties' can only appear in an object schema.", path + "/additionalProperties")
        if "enum" in schema_obj:
            if not isinstance(schema_obj["enum"], list):
                self._err("Enum must be an array.", path + "/enum")
            if "type" in schema_obj and isinstance(schema_obj["type"], str):
                if schema_obj["type"] in self.COMPOUND_TYPES:
                    self._err("'enum' cannot be used with compound types.", path + "/enum")
        if "const" in schema_obj:
            if "type" in schema_obj and isinstance(schema_obj["type"], str):
                if schema_obj["type"] in self.COMPOUND_TYPES:
                    self._err("'const' cannot be used with compound types.", path + "/const")

    def _check_union_type_item(self, union_item, path):
        """
        Checks one item in a union's type array.
        """
        if isinstance(union_item, str):
            if (union_item not in self.PRIMITIVE_TYPES and
                union_item not in self.COMPOUND_TYPES):
                self._err(f"'{union_item}' not recognized as a valid type name.", path)
            if union_item in self.COMPOUND_TYPES:
                self._err(f"Inline compound type '{union_item}' is not permitted in a union. Must use a $ref.", path)
        elif isinstance(union_item, dict):
            if "$ref" not in union_item:
                self._err("Inline compound definitions not allowed in union. Must be a $ref.", path)
            else:
                self._check_json_pointer(union_item["$ref"], self.doc, path + "/$ref")
        else:
            self._err("Union item must be a string or an object with $ref.", path)

    def _check_object_schema(self, obj, path):
        """
        Checks constraints specific to an 'object' type.
        If the object extends another type via '$extends', 'properties' is optional.
        """
        if "properties" not in obj and "$extends" not in obj:
            self._err("Object type must have 'properties' if not extending another type.", path + "/properties")
        elif "properties" in obj:
            props = obj["properties"]
            if not isinstance(props, dict):
                self._err("Properties must be an object.", path + "/properties")
            else:
                for prop_name, prop_schema in props.items():
                    if not self.identifier_regex.match(prop_name):
                        self._err(f"Property key '{prop_name}' does not match the identifier pattern.", path + f"/properties/{prop_name}")
                    if isinstance(prop_schema, dict):
                        self._validate_schema(prop_schema, is_root=False, path=f"{path}/properties/{prop_name}")
                    else:
                        self._err(f"Property '{prop_name}' must be an object (a schema).", path + f"/properties/{prop_name}")

    def _check_array_schema(self, obj, path):
        """
        Checks constraints for an 'array' type.
        """
        if "items" not in obj:
            self._err("Array type must have 'items'.", path + "/items")
        else:
            items_schema = obj["items"]
            if not isinstance(items_schema, dict):
                self._err("'items' must be an object (a schema).", path + "/items")
            else:
                self._validate_schema(items_schema, is_root=False, path=path + "/items")

    def _check_set_schema(self, obj, path):
        """
        Checks constraints for a 'set' type.
        """
        if "items" not in obj:
            self._err("Set type must have 'items'.", path + "/items")
        else:
            items_schema = obj["items"]
            if not isinstance(items_schema, dict):
                self._err("'items' must be an object (a schema).", path + "/items")
            else:
                self._validate_schema(items_schema, is_root=False, path=path + "/items")

    def _check_map_schema(self, obj, path):
        """
        Checks constraints for a 'map' type.
        """
        if "values" not in obj:
            self._err("Map type must have 'values'.", path + "/values")
        else:
            values_schema = obj["values"]
            if not isinstance(values_schema, dict):
                self._err("'values' must be an object (a schema).", path + "/values")
            else:
                self._validate_schema(values_schema, is_root=False, path=path + "/values")

    def _check_tuple_schema(self, obj, path):
        """
        Checks constraints for a 'tuple' type.
        A valid tuple schema must:
          - Include a 'name' attribute.
          - Have a 'properties' object where each key is a valid identifier.
          - Include a 'tuple' keyword that is an array of strings defining the order.
          - Ensure that every element in the 'tuple' array corresponds to a property in 'properties'.
        """
        # Check that 'name' is present.
        if "name" not in obj:
            self._err("Tuple type must include a 'name' attribute.", path + "/name")
        
        # Validate properties.
        if "properties" not in obj:
            self._err("Tuple type must have 'properties'.", path + "/properties")
        else:
            props = obj["properties"]
            if not isinstance(props, dict):
                self._err("'properties' must be an object.", path + "/properties")
            else:
                for prop_name, prop_schema in props.items():
                    if not self.identifier_regex.match(prop_name):
                        self._err(f"Tuple property key '{prop_name}' does not match the identifier pattern.", path + f"/properties/{prop_name}")
                    if isinstance(prop_schema, dict):
                        self._validate_schema(prop_schema, is_root=False, path=f"{path}/properties/{prop_name}")
                    else:
                        self._err(f"Tuple property '{prop_name}' must be an object (a schema).", path + f"/properties/{prop_name}")
        
        # Check that the 'tuple' keyword is present.
        if "tuple" not in obj:
            self._err("Tuple type must include the 'tuple' keyword defining the order of elements.", path + "/tuple")
        else:
            tuple_order = obj["tuple"]
            if not isinstance(tuple_order, list):
                self._err("'tuple' keyword must be an array of strings.", path + "/tuple")
            else:
                for idx, element in enumerate(tuple_order):
                    if not isinstance(element, str):
                        self._err(f"Element at index {idx} in 'tuple' array must be a string.", path + f"/tuple[{idx}]")
                    elif "properties" in obj and isinstance(obj["properties"], dict) and element not in obj["properties"]:
                        self._err(f"Element '{element}' in 'tuple' does not correspond to any property in 'properties'.", path + f"/tuple[{idx}]")

    def _check_choice_schema(self, obj, path):
        """
        Checks constraints for a 'choice' type (tagged or inline union).
        """
        if "choices" not in obj:
            self._err("Choice type must have 'choices'.", path + "/choices")
        else:
            choices = obj["choices"]
            if not isinstance(choices, dict):
                self._err("'choices' must be an object (map).", path + "/choices")
            else:
                for name, choice_schema in choices.items():
                    if not isinstance(name, str):
                        self._err(f"Choice key '{name}' must be a string.", path + f"/choices/{name}")
                    if isinstance(choice_schema, dict):
                        self._validate_schema(choice_schema, is_root=False, path=f"{path}/choices/{name}")
                    else:
                        self._err(f"Choice value for '{name}' must be an object (schema).", path + f"/choices/{name}")
        if "selector" in obj and not isinstance(obj.get("selector"), str):
            self._err("'selector' must be a string.", path + "/selector")

    def _check_primitive_schema(self, obj, path):
        """
        Checks constraints for a recognized primitive type.
        Additional annotation checks can be added here.
        """
        pass

    def _check_json_pointer(self, pointer, doc, path):
        """
        Validates that the pointer is a valid JSON Pointer within the same document.
        """
        if not isinstance(pointer, str):
            self._err("JSON Pointer must be a string.", path)
            return
        if not pointer.startswith("#"):
            self._err("JSON Pointer must start with '#' when referencing the same document.", path)
            return
        parts = pointer.split("/")
        cur = doc
        if pointer == "#":
            return
        for i, p in enumerate(parts):
            if i == 0:
                continue
            p = p.replace("~1", "/").replace("~0", "~")
            if isinstance(cur, dict):
                if p in cur:
                    cur = cur[p]
                else:
                    self._err(f"JSON Pointer segment '/{p}' not found.", path)
                    return
            else:
                self._err(f"JSON Pointer segment '/{p}' not applicable to non-object.", path)
                return

    def _check_offers(self, offers, path):
        """
        Validates the structure of the $offers map.
        """
        if not isinstance(offers, dict):
            self._err("$offers must be an object.", path)
            return
        for addin_name, addin_val in offers.items():
            if not isinstance(addin_name, str):
                self._err("$offers keys must be strings.", path)
            if isinstance(addin_val, str):
                self._check_json_pointer(addin_val, self.doc, f"{path}/{addin_name}")
            elif isinstance(addin_val, list):
                for idx, pointer in enumerate(addin_val):
                    if not isinstance(pointer, str):
                        self._err(f"$offers/{addin_name}[{idx}] must be a string (JSON Pointer).", f"{path}/{addin_name}[{idx}]")
                    else:
                        self._check_json_pointer(pointer, self.doc, f"{path}/{addin_name}[{idx}]")
            else:
                self._err(f"$offers/{addin_name} must be a string or array of strings.", f"{path}/{addin_name}")

    def _locate(self, pointer):
        """
        Heuristically locates the first occurrence of the JSON pointer path in the source text.
        Returns a tuple (line, column) if found, or None.
        """
        if not self.source_text or not pointer.startswith("#"):
            return None
        parts = pointer[1:].split("/")
        pos = 0
        for part in parts:
            part = part.replace("~1", "/").replace("~0", "~")
            pattern = f'"{part}"'
            found = self.source_text.find(pattern, pos)
            if found == -1:
                return None
            pos = found + len(pattern)
        line = self.source_text.count("\n", 0, pos) + 1
        last_newline = self.source_text.rfind("\n", 0, pos)
        col = pos - last_newline if last_newline != -1 else pos + 1
        return (line, col)

    def _err(self, message, location="#"):
        """
        Appends an error message with line and column information (if available)
        to the validation errors.
        """
        loc = self._locate(location)
        if loc:
            line, col = loc
            full_msg = f"{message} (Line: {line}, Column: {col})"
        else:
            full_msg = f"{message} (Location: {location}, line/column unknown)"
        self.errors.append(full_msg)


def validate_json_structure_schema_core(schema_document, source_text=None, allow_dollar=False, allow_import=False, import_map=None):
    """
    Validates the provided schema_document dict against the JSON Structure Core specification.
    :param schema_document: Parsed JSON Structure document.
    :param source_text: Original JSON text.
    :param allow_dollar: Allow '$' in property names.
    :param allow_import: Enable processing of $import/$importdefs keywords.
    :param import_map: Dictionary mapping URI to local filenames.
    :return: List of error strings; empty if valid.
    """
    validator = JSONStructureSchemaCoreValidator(allow_dollar=allow_dollar, allow_import=allow_import, import_map=import_map)
    return validator.validate(schema_document, source_text)


def main():
    """
    Command line entry point.
    Expects [--metaschema] [--allowimport] [--importmap URI=filename ...] and <path_to_json_file> as arguments.
    Prints errors with line and column information if found, otherwise prints "Schema is valid."
    """
    args = sys.argv[1:]
    allow_dollar = False
    allow_import = False
    import_map = {}

    # Process flags.
    while args and args[0].startswith("--"):
        arg = args.pop(0)
        if arg == "--metaschema":
            allow_dollar = True
        elif arg == "--allowimport":
            allow_import = True
        elif arg.startswith("--importmap"):
            if "=" in arg:
                _, mapping_str = arg.split("--importmap=", 1)
            else:
                if not args:
                    print("Missing value for --importmap")
                    sys.exit(1)
                mapping_str = args.pop(0)
            parts = mapping_str.split("=", 1)
            if len(parts) != 2:
                print("Invalid --importmap format. Expected format: URI=filename")
                sys.exit(1)
            uri, filename = parts
            import_map[uri] = filename
        else:
            print(f"Unknown flag {arg}")
            sys.exit(1)

    if len(args) < 1:
        print("Usage: python json_structure_schema_validator.py [--metaschema] [--allowimport] [--importmap URI=filename ...] <path_to_json_file>")
        sys.exit(1)
    file_path = args[0]
    try:
        with open(file_path, "r", encoding="utf-8") as file_in:
            source_text = file_in.read()
            data = json.loads(source_text)
    except (FileNotFoundError, json.JSONDecodeError) as ex:
        if hasattr(ex, 'lineno') and hasattr(ex, 'colno'):
            print(f"Error reading JSON file: {ex.msg} (Line: {ex.lineno}, Column: {ex.colno})")
        else:
            print(f"Error reading JSON file: {ex}")
        sys.exit(1)
    errors = validate_json_structure_schema_core(data, source_text, allow_dollar=allow_dollar, allow_import=allow_import, import_map=import_map)
    if errors:
        print("Schema is invalid:")
        for err in errors:
            print(" -", err)
        sys.exit(1)
    print("Schema is valid.")


if __name__ == "__main__":
    main()
