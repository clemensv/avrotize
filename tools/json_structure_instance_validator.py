# encoding: utf-8
"""
json_structure_instance_validator.py

Validates JSON document instances against JSON Structures conforming to the experimental
JSON Structure Core specification (C. Vasters, Microsoft, February 2025), including
all constructs from the core spec: primitive types, compound types, abstract types,
$extends, $offers, $uses, as well as the JSON Structure JSONStructureImport extensions ($import and $importdefs).

Additionally, if the instance provides a "$uses" clause containing "JSONStructureConditionalComposition" and/or "JSONStructureValidation",
the corresponding conditional composition and validation addin constraints are enforced.
Extensions such as "JSONStructureAlternateNames" or "JSONStructureUnits" are generally ignored for validation.

Furthermore, when the root schema’s "$schema" equals 
    "https://json-structure.org/meta/extended/v0/#"
all addins (i.e. all keys offered in "$offers") are automatically enabled.

This implementation also supports extended validation keywords as defined in the 
"JSON Structure JSONStructureValidation" spec, including numeric, string, array, object constraints,
and the "has" keyword.

Usage:
    python json_structure_instance_validator.py <schema_file> <instance_file>

The code sections are annotated with references to the metaschema constructs.
"""

import sys
import json
import re
import datetime
import uuid
from urllib.parse import urlparse

# Regular expressions for date, datetime, time and JSON pointer.
_DATE_REGEX = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_DATETIME_REGEX = re.compile(
    r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:\d{2})$'
)
_TIME_REGEX = re.compile(r'^\d{2}:\d{2}:\d{2}(?:\.\d+)?$')
_JSONPOINTER_REGEX = re.compile(r'^#(\/[^\/]+)*$')


class JSONStructureInstanceValidator:
    """
    Validator for JSON document instances against full JSON Structure Core schemas.
    Implements all core constructs including abstract types, $extends, $offers, $uses,
    the JSONStructureImport extension ($import and $importdefs), as well as conditional composition
    (allOf, anyOf, oneOf, not, if/then/else) and validation addin constraints.
    """
    ABSOLUTE_URI_REGEX = re.compile(r'^[a-zA-Z][a-zA-Z0-9+\-.]*://')

    def __init__(self, root_schema, allow_import=False, import_map=None, extended=False):
        """
        Initializes the validator.
        :param root_schema: The JSON Structure (as dict).
        :param allow_import: Enables processing of $import/$importdefs.
        :param import_map: Dict mapping URIs to local filenames.
        """
        self.root_schema = root_schema
        self.errors = []
        self.allow_import = allow_import
        self.import_map = import_map if import_map is not None else {}
        self.extended = extended
        self.enabled_extensions = set()
        # Process $import and $importdefs if enabled. [Metaschema: JSONStructureImport extension constructs]
        if self.allow_import:
            self._process_imports(self.root_schema, "#")
        self._detect_enabled_extensions()

    def _detect_enabled_extensions(self):
        schema_uri = self.root_schema.get("$schema", "")
        uses = self.root_schema.get("$uses", [])
        if "extended" in schema_uri or "validation" in schema_uri:
            self.enabled_extensions.add("JSONStructureConditionalComposition")
            self.enabled_extensions.add("JSONStructureValidation")
        if isinstance(uses, list):
            for ext in uses:
                self.enabled_extensions.add(ext)

    def validate_instance(self, instance, schema=None, path="#", meta=None):
        """
        Validates an instance against a schema.
        Resolves $ref, processes $extends, rejects abstract schemas, applies $uses,
        then validates based on type. Finally, conditionally validates conditional composition
        and validation addin constraints.
        :param instance: The JSON instance.
        :param schema: The schema to validate against (defaults to root schema).
        :param path: JSON Pointer for error reporting.
        :return: List of error messages.
        """
        if schema is None:
            schema = self.root_schema

        # --- Automatically enable all addins if using extended metaschema ---
        # Only do this if $uses is present; otherwise, do NOT auto-enable addins (per spec)
        if self.root_schema.get("$schema") == "https://json-structure.org/meta/extended/v0/#":
            if "$uses" in schema:
                all_addins = [
                    "JSONStructureConditionalComposition",
                    "JSONStructureValidation",
                    "JSONStructureUnits",
                    "JSONStructureAlternateNames"
                ]
                schema.setdefault("$uses", [])
                for addin in all_addins:
                    if addin not in schema["$uses"]:
                        schema["$uses"].append(addin)
            # If $uses is not present, do not auto-enable addins; enforcement is handled later

        if isinstance(instance, dict) and "$uses" in instance and self.root_schema.get("$schema") == "https://json-structure.org/meta/validation/v0/#":
            # Automatically enable the JSONStructureValidation addin.
            schema.setdefault("$uses", [])
            if "JSONStructureValidation" in instance["$uses"] and not "JSONStructureValidation" in schema["$uses"]:
                schema["$uses"].append("JSONStructureValidation")
            if "JSONStructureConditionalComposition" in instance["$uses"] and not "JSONStructureConditionalComposition" in schema["$uses"]:
                schema["$uses"].append("JSONStructureConditionalComposition")
            # [Metaschema: JSONStructureValidation metaschema automatically enables JSONStructureValidation addin]

        # the core schema https://json-structure.org/meta/validation/v0/# has no JSONStructureConditionalComposition or JSONStructureValidation addins
        # an instance referencing these addins will be rejected
        if isinstance(instance, dict) and "$uses" in instance and self.root_schema.get("$schema") == "https://json-structure.org/meta/core/v0/#":
            if "JSONStructureValidation" in instance["$uses"] or "JSONStructureConditionalComposition" in instance["$uses"]:
                self.errors.append(
                    f"Instance at {path} references JSONStructureConditionalComposition or JSONStructureValidation addins but the schema does not support them")

        # Resolve $ref at schema level. [Metaschema: TypeReference]
        if "$ref" in schema:
            ref = schema["$ref"]
            resolved = self._resolve_ref(ref)
            if resolved is None:
                self.errors.append(f"Cannot resolve $ref {ref} at {path}")
                return self.errors
            return self.validate_instance(instance, resolved, path)
        
        hasConditionals = False
        if "$uses" in self.root_schema:
            if "JSONStructureConditionalComposition" in self.root_schema["$uses"]:
                hasConditionals = self._validate_conditionals(schema, instance, path)        # Handle schemas that are only conditional composition at the root (no 'type')
        conditional_keywords = ("allOf", "anyOf", "oneOf", "not", "if", "then", "else")
        has_conditionals_at_root = any(k in schema for k in conditional_keywords)
        if not schema.get("type") and has_conditionals_at_root:
            schema_uri = self.root_schema.get("$schema", "")
            is_validation = schema_uri.endswith("/validation/v0/#")
            is_extended = schema_uri.endswith("/extended/v0/#")
            
            # Check extended metaschema enforcement first
            if is_extended and not (isinstance(self.root_schema, dict) and "$uses" in self.root_schema and "JSONStructureConditionalComposition" in self.root_schema["$uses"]):
                self.errors.append(
                    "Conditional composition is not enabled: $uses must include 'JSONStructureConditionalComposition' for the extended metaschema")
                return self.errors
            
            enable_conditional = (
                self.extended or
                is_validation or
                (isinstance(self.root_schema, dict) and "$uses" in self.root_schema and (
                    "JSONStructureConditionalComposition" in self.root_schema["$uses"] or "JSONStructureValidation" in self.root_schema["$uses"]
                ))
            )
            
            if enable_conditional:
                self._validate_conditionals(schema, instance, path)
                return self.errors
            else:
                self.errors.append(f"Conditional composition keywords present at {path} but not enabled")
                return self.errors

        # Handle case where "type" is a dict with a $ref. [Metaschema: PrimitiveOrReference]
        schema_type = schema.get("type")
        if not schema_type:
            self.errors.append(f"Schema at {path} has no 'type'")
            return self.errors
            
        if isinstance(schema_type, dict):
            if "$ref" in schema_type:
                resolved = self._resolve_ref(schema_type["$ref"])
                if resolved is None:
                    self.errors.append(f"Cannot resolve $ref {schema_type['$ref']} at {path}/type")
                    return self.errors
                new_schema = dict(schema)
                new_schema["type"] = resolved.get("type")
                if "properties" in resolved:
                    merged_props = dict(resolved.get("properties"))
                    merged_props.update(new_schema.get("properties", {}))
                    new_schema["properties"] = merged_props
                schema = new_schema
                schema_type = schema.get("type")
            else:
                self.errors.append(f"Schema at {path} has invalid 'type'")
                return self.errors

        # Handle union types. [Metaschema: TypeUnion]
        if isinstance(schema_type, list):
            union_valid = False
            union_errors = []
            for t in schema_type:
                backup = list(self.errors)
                self.errors = []
                self.validate_instance(instance, {"type": t}, path)
                if not self.errors:
                    union_valid = True
                    break
                else:
                    union_errors.extend(self.errors)
                self.errors = backup
            if not union_valid:
                self.errors.append(f"Instance at {path} does not match any type in union: {union_errors}")
            return self.errors

        if not isinstance(schema_type, str):
            self.errors.append(f"Schema at {path} has invalid 'type'")
            return self.errors        # Process $extends. [Metaschema: $extends in ObjectType/TupleType]
        if schema_type != "choice" and "$extends" in schema:
            base = self._resolve_ref(schema["$extends"])
            if base is None:
                self.errors.append(f"Cannot resolve $extends {schema['$extends']} at {path}")
                return self.errors
            base_props = base.get("properties", {})
            derived_props = schema.get("properties", {})
            for key in base_props:
                if key in derived_props:
                    self.errors.append(
                        f"Property '{key}' is inherited via $extends and must not be redefined at {path}")
            merged = dict(base)
            merged.update(schema)
            # Properly merge properties: base properties + derived properties
            if "properties" in base or "properties" in schema:
                merged_props = dict(base_props)
                merged_props.update(derived_props)
                merged["properties"] = merged_props
            merged.pop("$extends", None)
            merged.pop("abstract", None)
            schema = merged

        # Reject abstract schemas. [Metaschema: abstract property]
        if schema.get("abstract") is True:
            self.errors.append(f"Abstract schema at {path} cannot be used for instance validation")
            return self.errors

        # Process $uses add-in. [Metaschema: $offers and $uses]
        if isinstance(instance, dict) and "$uses" in instance:
            schema = self._apply_uses(schema, instance)
            instance.pop("$uses")

        # --- Begin type-based validation ---
        # Primitive types.
        if schema_type == "any":
            pass
        elif schema_type == "string":
            if not isinstance(instance, str):
                self.errors.append(f"Expected string at {path}, got {type(instance).__name__}")
        elif schema_type == "number":
            if isinstance(instance, bool) or not isinstance(instance, (int, float)):
                self.errors.append(f"Expected number at {path}, got {type(instance).__name__}")
        elif schema_type == "boolean":
            if not isinstance(instance, bool):
                self.errors.append(f"Expected boolean at {path}, got {type(instance).__name__}")
        elif schema_type == "null":
            if instance is not None:
                self.errors.append(f"Expected null at {path}, got {type(instance).__name__}")
        elif schema_type == "int32":
            if not isinstance(instance, int):
                self.errors.append(f"Expected int32 at {path}, got {type(instance).__name__}")
            elif not (-2**31 <= instance <= 2**31 - 1):
                self.errors.append(f"int32 value at {path} out of range")
        elif schema_type == "uint32":
            if not isinstance(instance, int):
                self.errors.append(f"Expected uint32 at {path}, got {type(instance).__name__}")
            elif not (0 <= instance <= 2**32 - 1):
                self.errors.append(f"uint32 value at {path} out of range")
        elif schema_type == "int64":
            if not isinstance(instance, str):
                self.errors.append(f"Expected int64 as string at {path}, got {type(instance).__name__}")
            else:
                try:
                    value = int(instance)
                    if not (-2**63 <= value <= 2**63 - 1):
                        self.errors.append(f"int64 value at {path} out of range")
                except ValueError:
                    self.errors.append(f"Invalid int64 format at {path}")
        elif schema_type == "uint64":
            if not isinstance(instance, str):
                self.errors.append(f"Expected uint64 as string at {path}, got {type(instance).__name__}")
            else:
                try:
                    value = int(instance)
                    if not (0 <= value <= 2**64 - 1):
                        self.errors.append(f"uint64 value at {path} out of range")
                except ValueError:
                    self.errors.append(f"Invalid uint64 format at {path}")
        elif schema_type in ("float", "double"):
            if not isinstance(instance, (int, float)):
                self.errors.append(f"Expected {schema_type} at {path}, got {type(instance).__name__}")
        elif schema_type == "decimal":
            if not isinstance(instance, str):
                self.errors.append(f"Expected decimal as string at {path}, got {type(instance).__name__}")
            else:
                try:
                    float(instance)
                except ValueError:
                    self.errors.append(f"Invalid decimal format at {path}")
        elif schema_type == "date":
            if not isinstance(instance, str) or not _DATE_REGEX.match(instance):
                self.errors.append(f"Expected date (YYYY-MM-DD) at {path}")
        elif schema_type == "datetime":
            if not isinstance(instance, str) or not _DATETIME_REGEX.match(instance):
                self.errors.append(f"Expected datetime (RFC3339) at {path}")
        elif schema_type == "time":
            if not isinstance(instance, str) or not _TIME_REGEX.match(instance):
                self.errors.append(f"Expected time (HH:MM:SS) at {path}")
        elif schema_type == "duration":
            if not isinstance(instance, str):
                self.errors.append(f"Expected duration as string at {path}")
        elif schema_type == "uuid":
            if not isinstance(instance, str):
                self.errors.append(f"Expected uuid as string at {path}")
            else:
                try:
                    uuid.UUID(instance)
                except ValueError:
                    self.errors.append(f"Invalid uuid format at {path}")
        elif schema_type == "uri":
            if not isinstance(instance, str):
                self.errors.append(f"Expected uri as string at {path}")
            else:
                parsed = urlparse(instance)
                if not parsed.scheme:
                    self.errors.append(f"Invalid uri format at {path}")
        elif schema_type == "binary":
            if not isinstance(instance, str):
                self.errors.append(f"Expected binary (base64 string) at {path}")
        elif schema_type == "jsonpointer":
            if not isinstance(instance, str) or not _JSONPOINTER_REGEX.match(instance):
                self.errors.append(f"Expected JSON pointer format at {path}")
        # Compound types.
        elif schema_type == "object":
            # Validate schema: properties MUST have at least one entry if present,
            # unless the schema uses $extends (properties may be inherited)
            if "properties" in schema:
                props_def = schema["properties"]
                if not isinstance(props_def, dict) or (len(props_def) == 0 and "$extends" not in schema):
                    self.errors.append(f"Object schema at {path} has 'properties' but it is empty - properties MUST have at least one entry")
                    return self.errors
            
            if not isinstance(instance, dict):
                self.errors.append(f"Expected object at {path}, got {type(instance).__name__}")
            else:
                props = schema.get("properties", {})
                req = schema.get("required", [])
                for r in req:
                    if r not in instance:
                        self.errors.append(f"Missing required property '{r}' at {path}")
                for prop, prop_schema in props.items():
                    if prop in instance:
                        self.validate_instance(instance[prop], prop_schema, f"{path}/{prop}")
                if "additionalProperties" in schema:
                    addl = schema["additionalProperties"]
                    if addl is False:
                        for key in instance.keys():
                            if key not in props:
                                self.errors.append(f"Additional property '{key}' not allowed at {path}")
                    elif isinstance(addl, dict):
                        for key in instance.keys():
                            if key not in props:
                                self.validate_instance(instance[key], addl, f"{path}/{key}")
                # Extended object constraint: "has" keyword. [Metaschema: ObjectValidationAddIn]
                if "has" in schema:
                    has_schema = schema["has"]
                    valid = any(len(self.validate_instance(val, has_schema, f"{path}/{prop}")) == 0
                                for prop, val in instance.items())
                    if not valid:
                        self.errors.append(f"Object at {path} does not have any property satisfying 'has' schema")
                # dependencies (dependentRequired) validation
                if "dependentRequired" in schema and isinstance(schema["dependentRequired"], dict):
                    for prop_name, required_deps in schema["dependentRequired"].items():
                        if prop_name in instance and isinstance(required_deps, list):
                            for dep in required_deps:
                                if dep not in instance:
                                    self.errors.append(
                                        f"Property '{prop_name}' at {path} requires dependent property '{dep}'")
        elif schema_type == "array":
            if not isinstance(instance, list):
                self.errors.append(f"Expected array at {path}, got {type(instance).__name__}")
            else:
                items_schema = schema.get("items")
                if items_schema:
                    for idx, item in enumerate(instance):
                        self.validate_instance(item, items_schema, f"{path}[{idx}]")
        elif schema_type == "set":
            if not isinstance(instance, list):
                self.errors.append(f"Expected set (unique array) at {path}, got {type(instance).__name__}")
            else:
                serialized = [json.dumps(x, sort_keys=True) for x in instance]
                if len(serialized) != len(set(serialized)):
                    self.errors.append(f"Set at {path} contains duplicate items")
                items_schema = schema.get("items")
                if items_schema:
                    for idx, item in enumerate(instance):
                        self.validate_instance(item, items_schema, f"{path}[{idx}]")
        elif schema_type == "map":
            if not isinstance(instance, dict):
                self.errors.append(f"Expected map (object) at {path}, got {type(instance).__name__}")
            else:
                # Map keys MAY be any valid JSON string (no restrictions on key format)
                values_schema = schema.get("values")
                if values_schema:
                    for key, val in instance.items():
                        self.validate_instance(val, values_schema, f"{path}/{key}")
        elif schema_type == "tuple":
            if not isinstance(instance, list):
                self.errors.append(f"Expected tuple (array) at {path}, got {type(instance).__name__}")
            else:
                # Retrieve the tuple ordering
                order = schema.get("tuple")
                props = schema.get("properties", {})
                if order is None:
                    self.errors.append(f"Tuple schema at {path} is missing the required 'tuple' keyword for ordering")
                elif not isinstance(order, list):
                    self.errors.append(f"'tuple' keyword at {path} must be an array of property names")
                else:
                    # Verify each name in order exists in properties
                    for prop_name in order:
                        if prop_name not in props:
                            self.errors.append(f"Tuple order key '{prop_name}' at {path} not defined in properties")
                    expected_len = len(order)
                    if len(instance) != expected_len:
                        self.errors.append(f"Tuple at {path} length {len(instance)} does not equal expected {expected_len}")
                    else:
                        for idx, prop_name in enumerate(order):
                            prop_schema = props[prop_name]
                            self.validate_instance(instance[idx], prop_schema, f"{path}/{prop_name}")
        elif schema_type == "choice":
            if not isinstance(instance, dict):
                self.errors.append(f"Expected choice object at {path}, got {type(instance).__name__}")
            else:
                choices = schema.get("choices", {})
                extends = schema.get("$extends")
                selector = schema.get("selector")
                if extends is None:
                    # Tagged union: exactly one property matching a choice key
                    if len(instance) != 1:
                        self.errors.append(f"Tagged union at {path} must have a single property")
                    else:
                        key, value = next(iter(instance.items()))
                        if key not in choices:
                            self.errors.append(f"Property '{key}' at {path} not one of choices {list(choices.keys())}")
                        else:
                            self.validate_instance(value, choices[key], f"{path}/{key}")
                else:
                    # Inline union: must have selector property
                    if selector is None:
                        self.errors.append(f"Inline union at {path} missing 'selector' in schema")
                    else:
                        sel_val = instance.get(selector)
                        if not isinstance(sel_val, str):
                            self.errors.append(f"Selector '{selector}' at {path} must be a string")
                        elif sel_val not in choices:
                            self.errors.append(f"Selector '{sel_val}' at {path} not one of choices {list(choices.keys())}")
                        else:
                            # validate remaining properties against chosen variant
                            variant = choices[sel_val]
                            inst_copy = dict(instance)
                            inst_copy.pop(selector, None)
                            self.validate_instance(inst_copy, variant, path)
        else:
            self.errors.append(f"Unsupported type '{schema_type}' at {path}")

        # --- Enforce extended features if enabled ---
        # Only enable conditional composition if explicitly enabled via $uses or the validation metaschema
        schema_uri = self.root_schema.get("$schema", "")
        is_validation = schema_uri.endswith("/validation/v0/#")
        is_extended = schema_uri.endswith("/extended/v0/#")
        # Only enable if:
        # - self.extended is set (CLI override)
        # - validation metaschema (enables both addins by default)
        # - $uses in schema explicitly enables the addin
        enable_conditional = (
            self.extended or
            is_validation or
            (isinstance(schema, dict) and "$uses" in schema and (
                "JSONStructureConditionalComposition" in schema["$uses"] or "JSONStructureValidation" in schema["$uses"]
            ))
        )
        # If the schema is the extended metaschema and has any conditional composition keyword but does NOT have $uses, this is an error (per spec)
        conditional_keywords = ("allOf", "anyOf", "oneOf", "not", "if", "then", "else")
        if is_extended and any(k in schema for k in conditional_keywords):
            if not (isinstance(schema, dict) and "$uses" in schema and "JSONStructureConditionalComposition" in schema["$uses"]):
                self.errors.append(
                    "Conditional composition is not enabled: $uses must include 'JSONStructureConditionalComposition' for the extended metaschema")
                return self.errors
        if enable_conditional:
            # Conditional composition (allOf, anyOf, oneOf, not, if/then/else)
            if ("JSONStructureConditionalComposition" in self.enabled_extensions or
                is_validation or
                (isinstance(schema, dict) and "$uses" in schema and "JSONStructureConditionalComposition" in schema["$uses"])):
                self._validate_conditionals(schema, instance, path)
            # Validation keywords (min/max, pattern, etc.)
            if ("JSONStructureValidation" in self.enabled_extensions or
                is_validation or
                (isinstance(schema, dict) and "$uses" in schema and "JSONStructureValidation" in schema["$uses"])):
                self._validate_validation_addins(schema, instance, path)

        if "const" in schema:
            if instance != schema["const"]:
                self.errors.append(f"Value at {path} does not equal const {schema['const']}")
        if "enum" in schema:
            if instance not in schema["enum"]:
                self.errors.append(f"Value at {path} not in enum {schema['enum']}")
        return self.errors

    def validate(self, instance, schema=None):
        if schema is None:
            schema = self.root_schema
        errors = []
        # Extended: conditional composition
        if self.extended and "JSONStructureConditionalComposition" in self.enabled_extensions:
            for key in ("allOf", "anyOf", "oneOf"):
                if key in schema:
                    subschemas = schema[key]
                    if key == "allOf":
                        for idx, subschema in enumerate(subschemas):
                            errors += self.validate(instance, subschema)
                    elif key == "anyOf":
                        if not any(not self.validate(instance, subschema) for subschema in subschemas):
                            errors.append(f"Instance does not match anyOf at {key}")
                    elif key == "oneOf":
                        matches = sum(1 for subschema in subschemas if not self.validate(instance, subschema))
                        if matches != 1:
                            errors.append(f"Instance does not match exactly one subschema in oneOf at {key}")
            if "not" in schema:
                if not self.validate(instance, schema["not"]):
                    errors.append("Instance must not match 'not' subschema")
            if "if" in schema:
                if not self.validate(instance, schema["if"]):
                    if "else" in schema:
                        errors += self.validate(instance, schema["else"])
                else:
                    if "then" in schema:
                        errors += self.validate(instance, schema["then"])
        # Extended: validation keywords
        if self.extended and "JSONStructureValidation" in self.enabled_extensions:
            t = schema.get("type")
            if t == "string":
                if "minLength" in schema and isinstance(instance, str):
                    if len(instance) < schema["minLength"]:
                        errors.append(f"String shorter than minLength {schema['minLength']}")
                if "pattern" in schema and isinstance(instance, str):
                    import re
                    if not re.match(schema["pattern"], instance):
                        errors.append(f"String does not match pattern {schema['pattern']}")
            if t == "number" or t == "int32" or t == "float" or t == "double":
                if "minimum" in schema and instance < schema["minimum"]:
                    errors.append(f"Number less than minimum {schema['minimum']}")
                if "maximum" in schema and instance > schema["maximum"]:
                    errors.append(f"Number greater than maximum {schema['maximum']}")
                if "multipleOf" in schema and instance % schema["multipleOf"] != 0:
                    errors.append(f"Number not a multiple of {schema['multipleOf']}")
            if t == "array":
                if "minItems" in schema and len(instance) < schema["minItems"]:
                    errors.append(f"Array has fewer than minItems {schema['minItems']}")
                if "maxItems" in schema and len(instance) > schema["maxItems"]:
                    errors.append(f"Array has more than maxItems {schema['maxItems']}")
                if "uniqueItems" in schema and schema["uniqueItems"]:
                    if len(instance) != len(set(map(str, instance))):
                        errors.append("Array items are not unique")
        return errors

    def _validate_conditionals(self, schema, instance, path) -> bool:
        """
        Validates conditional composition keywords: allOf, anyOf, oneOf, not, if/then/else.
        [Metaschema: JSON Structure Conditional Composition]
        returns True if the instance contains any conditional composition keywords
        """
        hasConditionals = False
        if "allOf" in schema:
            hasConditionals = True
            subschemas = schema["allOf"]
            for idx, subschema in enumerate(subschemas):
                backup = list(self.errors)
                # Ensure subschema inherits validation context from parent
                enhanced_subschema = dict(subschema)
                if self.root_schema.get("$uses"):
                    if "$uses" not in enhanced_subschema:
                        enhanced_subschema["$uses"] = list(self.root_schema["$uses"])
                    else:
                        # Merge validation addins
                        for addin in self.root_schema["$uses"]:
                            if addin not in enhanced_subschema["$uses"]:
                                enhanced_subschema["$uses"].append(addin)
                self.validate_instance(instance, enhanced_subschema, f"{path}/allOf[{idx}]")
                if self.errors:
                    self.errors = backup + self.errors
        if "anyOf" in schema:
            hasConditionals = True
            subschemas = schema["anyOf"]
            valid = False
            errors_any = []
            for idx, subschema in enumerate(subschemas):
                backup = list(self.errors)
                self.errors = []
                # Ensure subschema inherits validation context from parent
                enhanced_subschema = dict(subschema)
                if self.root_schema.get("$uses"):
                    if "$uses" not in enhanced_subschema:
                        enhanced_subschema["$uses"] = list(self.root_schema["$uses"])
                    else:
                        # Merge validation addins
                        for addin in self.root_schema["$uses"]:
                            if addin not in enhanced_subschema["$uses"]:
                                enhanced_subschema["$uses"].append(addin)
                self.validate_instance(instance, enhanced_subschema, f"{path}/anyOf[{idx}]")
                if not self.errors:
                    valid = True
                    break
                else:
                    errors_any.append(f"anyOf[{idx}]: {self.errors}")
                self.errors = backup
            if not valid:
                self.errors.append(f"Instance at {path} does not satisfy anyOf: {errors_any}")
        if "oneOf" in schema:
            hasConditionals = True	
            subschemas = schema["oneOf"]
            valid_count = 0
            errors_one = []
            for idx, subschema in enumerate(subschemas):
                backup = list(self.errors)
                self.errors = []
                # Ensure subschema inherits validation context from parent
                enhanced_subschema = dict(subschema)
                if self.root_schema.get("$uses"):
                    if "$uses" not in enhanced_subschema:
                        enhanced_subschema["$uses"] = list(self.root_schema["$uses"])
                    else:
                        # Merge validation addins
                        for addin in self.root_schema["$uses"]:
                            if addin not in enhanced_subschema["$uses"]:
                                enhanced_subschema["$uses"].append(addin)
                self.validate_instance(instance, enhanced_subschema, f"{path}/oneOf[{idx}]")
                if not self.errors:
                    valid_count += 1
                else:
                    errors_one.append(f"oneOf[{idx}]: {self.errors}")
                self.errors = backup
            if valid_count != 1:
                self.errors.append(
                    f"Instance at {path} must match exactly one subschema in oneOf; matched {valid_count}. Details: {errors_one}")
        if "not" in schema:
            hasConditionals = True
            subschema = schema["not"]
            backup = list(self.errors)
            self.errors = []
            self.validate_instance(instance, subschema, f"{path}/not")
            if not self.errors:
                self.errors = backup + [f"Instance at {path} should not validate against 'not' schema"]
            else:
                self.errors = backup
        if "if" in schema:
            hasConditionals = True
            backup = list(self.errors)
            self.errors = []
            self.validate_instance(instance, schema["if"], f"{path}/if")
            if_valid = not self.errors
            self.errors = backup
            if if_valid:
                if "then" in schema:
                    self.validate_instance(instance, schema["then"], f"{path}/then")
            else:
                if "else" in schema:
                    self.validate_instance(instance, schema["else"], f"{path}/else")
        return hasConditionals

    def _validate_validation_addins(self, schema, instance, path):
        """
        Validates additional constraints defined by the JSONStructureValidation addins.
        [Metaschema: JSON Structure JSONStructureValidation]
        """
        # Numeric constraints.
        if schema.get("type") in ("number", "integer", "float", "double", "decimal", "int32", "uint32", "int64", "uint64", "int128", "uint128"):
            if "minimum" in schema:
                try:
                    if instance < schema["minimum"]:
                        self.errors.append(f"Value at {path} is less than minimum {schema['minimum']}")
                except Exception:
                    self.errors.append(f"Cannot compare value at {path} with minimum constraint")
            if "maximum" in schema:
                try:
                    if instance > schema["maximum"]:
                        self.errors.append(f"Value at {path} is greater than maximum {schema['maximum']}")
                except Exception:
                    self.errors.append(f"Cannot compare value at {path} with maximum constraint")
            if schema.get("exclusiveMinimum") is True:
                try:
                    if instance <= schema.get("minimum", float("-inf")):
                        self.errors.append(
                            f"Value at {path} is not greater than exclusive minimum {schema.get('minimum')}")
                except Exception:
                    self.errors.append(f"Cannot evaluate exclusiveMinimum constraint at {path}")
            if schema.get("exclusiveMaximum") is True:
                try:
                    if instance >= schema.get("maximum", float("inf")):                        self.errors.append(
                            f"Value at {path} is not less than exclusive maximum {schema.get('maximum')}")
                except Exception:
                    self.errors.append(f"Cannot evaluate exclusiveMaximum constraint at {path}")
            if "multipleOf" in schema:
                try:
                    # Handle floating point precision issues
                    multiple_of = schema["multipleOf"]
                    quotient = instance / multiple_of
                    # Check if the quotient is close to an integer within a small tolerance
                    if abs(quotient - round(quotient)) > 1e-10:
                        self.errors.append(f"Value at {path} is not a multiple of {schema['multipleOf']}")
                except Exception:
                    self.errors.append(f"Cannot evaluate multipleOf constraint at {path}")
        
        # String constraints.
        if schema.get("type") == "string":
            if "minLength" in schema:
                try:
                    if len(instance) < schema["minLength"]:
                        self.errors.append(f"String at {path} shorter than minLength {schema['minLength']}")
                except TypeError:
                    self.errors.append(f"Invalid minLength constraint at {path}")
            if "maxLength" in schema:
                try:
                    if len(instance) > schema["maxLength"]:
                        self.errors.append(f"String at {path} exceeds maxLength {schema['maxLength']}")
                except TypeError:
                    self.errors.append(f"Invalid maxLength constraint at {path}")
            if "pattern" in schema:
                try:
                    pattern = re.compile(schema["pattern"])
                    if not pattern.search(instance):
                        self.errors.append(f"String at {path} does not match pattern {schema['pattern']}")
                except (re.error, TypeError):
                    self.errors.append(f"Invalid pattern constraint at {path}")
            if "format" in schema:
                fmt = schema["format"]
                try:
                    if fmt == "email":
                        # Simple email validation
                        if "@" not in instance or not re.match(r'^[^@]+@[^@]+\.[^@]+$', instance):
                            self.errors.append(f"String at {path} does not match format email")
                    elif fmt == "ipv4":
                        # IPv4 validation
                        parts = instance.split('.')
                        if len(parts) != 4 or not all(0 <= int(part) <= 255 for part in parts):
                            self.errors.append(f"String at {path} does not match format ipv4")
                    elif fmt == "ipv6":
                        # Basic IPv6 validation
                        if not re.match(r'^[0-9a-fA-F:]+$', instance):
                            self.errors.append(f"String at {path} does not match format ipv6")
                    elif fmt == "uri":
                        parsed = urlparse(instance)
                        if not parsed.scheme:
                            self.errors.append(f"String at {path} does not match format uri")
                    elif fmt == "hostname":
                        if not re.match(r'^[a-zA-Z0-9.-]+$', instance):
                            self.errors.append(f"String at {path} does not match format hostname")
                    # Add more format validations as needed
                except (ValueError, TypeError):
                    self.errors.append(f"String at {path} does not match format {fmt}")        # Array constraints.
        if schema.get("type") == "array":
            if "minItems" in schema:
                if len(instance) < schema["minItems"]:
                    self.errors.append(f"Array at {path} has fewer items than minItems {schema['minItems']}")
            if "maxItems" in schema:
                if len(instance) > schema["maxItems"]:
                    self.errors.append(f"Array at {path} has more items than maxItems {schema['maxItems']}")
            if schema.get("uniqueItems") is True:
                serialized = [json.dumps(x, sort_keys=True) for x in instance]
                if len(serialized) != len(set(serialized)):
                    self.errors.append(f"Array at {path} does not have unique items")
            
            # contains validation
            if "contains" in schema:
                contains_schema = schema["contains"]
                matches = []
                for i, item in enumerate(instance):
                    temp_validator = JSONStructureInstanceValidator(contains_schema, import_map=self.import_map, allow_import=self.allow_import)
                    item_errors = temp_validator.validate_instance(item)
                    if not item_errors:
                        matches.append(i)
                
                if not matches:
                    self.errors.append(f"Array at {path} does not contain required element")
                
                # minContains validation
                if "minContains" in schema:
                    if len(matches) < schema["minContains"]:
                        self.errors.append(f"Array at {path} contains fewer than minContains {schema['minContains']} matching elements")
                
                # maxContains validation  
                if "maxContains" in schema:
                    if len(matches) > schema["maxContains"]:
                        self.errors.append(f"Array at {path} contains more than maxContains {schema['maxContains']} matching elements")
        # Object constraints.
        if schema.get("type") == "object":
            if "minProperties" in schema:
                if len(instance.keys()) < schema["minProperties"]:
                    self.errors.append(
                        f"Object at {path} has fewer properties than minProperties {schema['minProperties']}")
            if "maxProperties" in schema:
                if len(instance.keys()) > schema["maxProperties"]:
                    self.errors.append(
                        f"Object at {path} has more properties than maxProperties {schema['maxProperties']}")
            
            # patternProperties validation
            if "patternProperties" in schema and isinstance(schema["patternProperties"], dict):
                for pattern_str, pattern_schema in schema["patternProperties"].items():
                    try:
                        pattern = re.compile(pattern_str)
                        for prop_name, prop_value in instance.items():
                            if pattern.search(prop_name):
                                self.validate_instance(prop_value, pattern_schema, f"{path}/{prop_name}")
                    except re.error:
                        self.errors.append(f"Invalid regular expression '{pattern_str}' in patternProperties at {path}")
            
            # propertyNames validation
            if "propertyNames" in schema:
                property_names_schema = schema["propertyNames"]
                if not isinstance(property_names_schema, dict) or property_names_schema.get("type") != "string":
                    self.errors.append(f"propertyNames schema must be of type string at {path}")
                else:
                    for prop_name in instance.keys():
                        self.validate_instance(prop_name, property_names_schema, f"{path}/propertyName({prop_name})")
                        
            # dependencies (dependentRequired) validation
            if "dependentRequired" in schema and isinstance(schema["dependentRequired"], dict):
                for prop_name, required_deps in schema["dependentRequired"].items():
                    if prop_name in instance and isinstance(required_deps, list):
                        for dep in required_deps:
                            if dep not in instance:
                                self.errors.append(
                                    f"Property '{prop_name}' at {path} requires dependent property '{dep}'")        # Map constraints
        if schema.get("type") == "map":
            # minEntries and maxEntries validation
            if "minEntries" in schema:
                if len(instance) < schema["minEntries"]:
                    self.errors.append(f"Map at {path} has fewer than minEntries {schema['minEntries']}")
            if "maxEntries" in schema:
                if len(instance) > schema["maxEntries"]:
                    self.errors.append(f"Map at {path} has more than maxEntries {schema['maxEntries']}")
                    
            # patternKeys validation
            if "patternKeys" in schema and isinstance(schema["patternKeys"], dict):
                for pattern_str, pattern_schema in schema["patternKeys"].items():
                    try:
                        pattern = re.compile(pattern_str)
                        for key_name, key_value in instance.items():
                            if pattern.search(key_name):
                                self.validate_instance(key_value, pattern_schema, f"{path}/{key_name}")
                    except re.error:
                        self.errors.append(f"Invalid regular expression '{pattern_str}' in patternKeys at {path}")
            
            # keyNames validation
            if "keyNames" in schema:
                key_names_schema = schema["keyNames"]
                if not isinstance(key_names_schema, dict) or key_names_schema.get("type") != "string":
                    self.errors.append(f"keyNames schema must be of type string at {path}")
                else:
                    for key_name in instance.keys():
                        # Ensure validation addins are enabled for keyNames schema validation
                        keynames_validation_schema = dict(key_names_schema)
                        if "$uses" not in keynames_validation_schema:
                            keynames_validation_schema["$uses"] = ["JSONStructureValidation"]
                        elif "JSONStructureValidation" not in keynames_validation_schema["$uses"]:
                            keynames_validation_schema["$uses"].append("JSONStructureValidation")
                        
                        temp_validator = JSONStructureInstanceValidator(keynames_validation_schema, import_map=self.import_map, allow_import=self.allow_import)
                        key_errors = temp_validator.validate_instance(key_name)
                        if key_errors:
                            self.errors.append(f"Map key name '{key_name}' at {path} does not match keyNames constraint")

    def _resolve_ref(self, ref):
        """
        Resolves a $ref within the root schema using JSON Pointer syntax.
        [Metaschema: TypeReference]
        :param ref: A JSON pointer string starting with '#'.
        :return: The referenced schema object or None.
        """
        if not ref.startswith("#"):
            return None
        parts = ref.lstrip("#").split("/")
        target = self.root_schema
        for part in parts:
            if part == "":
                continue
            part = part.replace("~1", "/").replace("~0", "~")
            if isinstance(target, dict) and part in target:
                target = target[part]
            else:
                return None
        return target

    def _process_imports(self, obj, path):
        """
        Recursively processes $import and $importdefs keywords in the schema.
        [Metaschema: JSONStructureImport extension constructs]
        Merges imported definitions into the current object as if defined locally.
        Uses self.import_map if the URI is mapped to a local file.
        """
        if isinstance(obj, dict):
            for key in list(obj.keys()):
                if key in ("$import", "$importdefs"):
                    if not self.allow_import:
                        self.errors.append(
                            f"JSONStructureImport keyword '{key}' encountered but allow_import not enabled at {path}/{key}")
                        continue
                    uri = obj[key]
                    if not isinstance(uri, str):
                        self.errors.append(f"JSONStructureImport keyword '{key}' value must be a string URI at {path}/{key}")
                        continue
                    if not self.ABSOLUTE_URI_REGEX.search(uri):
                        self.errors.append(f"JSONStructureImport keyword '{key}' value must be an absolute URI at {path}/{key}")
                        continue
                    external = self._fetch_external_schema(uri)
                    if external is None:
                        self.errors.append(f"Unable to fetch external schema from {uri} at {path}/{key}")
                        continue
                    if key == "$import":
                        imported_defs = {}
                        if "type" in external and "name" in external:
                            imported_defs[external["name"]] = external
                        if "definitions" in external and isinstance(external["definitions"], dict):
                            imported_defs.update(external["definitions"])
                    else:  # $importdefs
                        if "definitions" in external and isinstance(external["definitions"], dict):
                            imported_defs = external["definitions"]
                        else:
                            imported_defs = {}
                    for k, v in imported_defs.items():
                        if k not in obj:
                            obj[k] = v
                    del obj[key]
            for k, v in obj.items():
                self._process_imports(v, f"{path}/{k}")
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                self._process_imports(item, f"{path}[{idx}]")

    def _fetch_external_schema(self, uri):
        """
        Fetches an external schema from a URI.
        [Metaschema: JSONStructureImport extension resolution]
        If the URI is in self.import_map, loads the schema from the specified file.
        Otherwise, uses a simulated lookup.
        """
        if uri in self.import_map:
            try:
                with open(self.import_map[uri], "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self.errors.append(f"Failed to load imported schema from {self.import_map[uri]}: {e}")
                return None
        SIMULATED_SCHEMAS = {
            "https://example.com/people.json": {
                "$schema": "https://json-structure.org/meta/core/v0/#",
                "$id": "https://example.com/people.json",
                "name": "Person",
                "type": "object",
                "properties": {
                    "firstName": {"type": "string"},
                    "lastName": {"type": "string"},
                    "address": {"$ref": "#/Address"}
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
            "https://example.com/importdefs.json": {
                "$schema": "https://json-structure.org/meta/core/v0/#",
                "$id": "https://example.com/importdefs.json",
                "definitions": {
                    "LibraryType": {
                        "name": "LibraryType",
                        "type": "string"
                    }
                }
            }
        }
        return SIMULATED_SCHEMAS.get(uri)

    def _apply_uses(self, schema, instance):
        """
        Applies add-in types to the effective schema if the instance declares "$uses".
        [Metaschema: $offers and $uses in SchemaDocument]
        The $uses keyword is expected to be an array of add-in names.
        The root schema must have a "$offers" mapping.
        :param schema: The current schema.
        :param instance: The instance dict containing "$uses".
        :return: The merged schema with add-ins applied.
        """
        uses = instance.get("$uses")
        if not uses:
            return schema
        if not isinstance(uses, list):
            uses = [uses]
        offers = self.root_schema.get("$offers", {})
        merged = dict(schema)
        merged.setdefault("properties", {})
        for use in [u for u in uses if not u in ["JSONStructureValidation", "JSONStructureConditionalComposition", "JSONStructureAlternateNames", "JSONStructureUnits"]]:
            if use not in offers:
                self.errors.append(f"Add-in '{use}' not offered in $offers")
                continue
            addin = offers[use]
            if isinstance(addin, list):
                for ref in addin:
                    resolved = self._resolve_ref(ref) if isinstance(ref, str) else ref
                    if isinstance(resolved, dict):
                        addin_props = resolved.get("properties", {})
                        for prop in addin_props:
                            if prop in merged["properties"]:
                                self.errors.append(
                                    f"Add-in property '{prop}' from add-in '{use}' conflicts with existing property")
                        merged["properties"].update(addin_props)
            elif isinstance(addin, dict) and "$ref" in addin:
                resolved = self._resolve_ref(addin["$ref"])
                if isinstance(resolved, dict):
                    addin_props = resolved.get("properties", {})
                    for prop in addin_props:
                        if prop in merged["properties"]:
                            self.errors.append(
                                f"Add-in property '{prop}' from add-in '{use}' conflicts with existing property")
                    merged["properties"].update(addin_props)
            elif isinstance(addin, dict):
                addin_props = addin.get("properties", {})
                for prop in addin_props:
                    if prop in merged["properties"]:
                        self.errors.append(
                            f"Add-in property '{prop}' from add-in '{use}' conflicts with existing property")
                merged["properties"].update(addin_props)
            else:
                self.errors.append(f"Invalid add-in definition for '{use}'")
        return merged


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('instance_file')
    parser.add_argument('schema_file')
    parser.add_argument('--extended', action='store_true')
    args = parser.parse_args()
    import json
    with open(args.schema_file, 'r', encoding='utf-8') as f:
        schema = json.load(f)
    with open(args.instance_file, 'r', encoding='utf-8') as f:
        instance = json.load(f)
    validator = JSONStructureInstanceValidator(schema, extended=args.extended)
    errors = validator.validate(instance)
    if errors:
        print("Instance is invalid:")
        for err in errors:
            print(" -", err)
        exit(1)
    print("Instance is valid.")


if __name__ == "__main__":
    main()
