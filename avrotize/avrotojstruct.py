import json
import uuid
from typing import Any, Dict, List, Union

from avrotize.common import is_any_value_type


class AvroToJsonStructure:
    """
    Convert (one or more) Avro schemas into a single JSON-Structure document.
    """

    def __init__(self, avro_encoding: bool = False) -> None:
        self.known_types: set[str] = set()
        self.reference_stack: set[str] = set()
        self.definition_paths: dict[str, str] = {}
        self.avro_encoding: bool = avro_encoding

    # ------------------------------------------------------------------ TOP-LEVEL

    def convert(
        self,
        avro_schema: Union[Dict[str, Any], List[Any]],
        namespace: str | None = None,
    ) -> Dict[str, Any]:
        """
        Entry-point: return a full JSON-Structure document for `avro_schema`.
        """

        # ------------- LIST (multiple root schemas) --------------------
        if isinstance(avro_schema, list):
            # Empty list – return a stub document
            if not avro_schema:
                anon_name = f"empty_list_{uuid.uuid4().hex[:8]}"
                return {
                    "$schema": "https://json-structure.org/meta/core/v0/#",
                    "$id": f"https://example.com/schemas/{anon_name}",
                    "name": anon_name,
                    "definitions": {},
                }

            first = avro_schema[0]
            if isinstance(first, dict):
                self.known_types.clear()
                self.reference_stack.clear()
                self._prepare_definition_paths(avro_schema, namespace)

                current_namespace, name = self.resolve_full_name(
                    first.get("name", f"AnonymousType_{uuid.uuid4().hex}"),
                    first.get("namespace", namespace),
                )
                fqn = self.get_fqn(current_namespace, name)
                doc: Dict[str, Any] = {
                    "$schema": "https://json-structure.org/meta/core/v0/#",
                    "$id": f"https://example.com/schemas/{fqn}",
                    "name": name,
                    "type": "null",
                    "definitions": {},
                }
                for schema in avro_schema:
                    if isinstance(schema, dict):
                        self.register_definition(schema, namespace, doc["definitions"])
                self._apply_extensions(doc)
                return doc

            # First element non-dict → return stub
            bad_name = f"invalid_list_root_{uuid.uuid4().hex[:8]}"
            return {
                "$schema": "https://json-structure.org/meta/core/v0/#",
                "$id": f"https://example.com/schemas/{bad_name}",
                "name": bad_name,
                "definitions": {},
            }

        # ------------- SINGLE SCHEMA -----------------------------------
        # Reset caches for each top-level conversion
        self.known_types.clear()
        self.reference_stack.clear()
        self._prepare_definition_paths(avro_schema, namespace)

        current_namespace, name = self.resolve_full_name(
            avro_schema.get("name", f"AnonymousType_{uuid.uuid4().hex}"),
            avro_schema.get("namespace", namespace),
        )
        fqn = self.get_fqn(current_namespace, name)

        doc: Dict[str, Any] = {
            "$schema": "https://json-structure.org/meta/core/v0/#",
            "$id": f"https://example.com/schemas/{fqn}",
            "name": name,
            "$root": f"#/definitions/{fqn}",
            "definitions": {},
        }

        # Build definitions – do NOT skip root
        self.register_definition(avro_schema, current_namespace, doc["definitions"])
        self._apply_extensions(doc)
        return doc

    def _apply_extensions(self, doc: Dict[str, Any]) -> None:
        uses = set(doc.get("$uses", []))
        if self._contains_key(doc, "default"):
            uses.add("JSONStructureValidation")
        if self._contains_key(doc, "altnames"):
            uses.add("JSONStructureAlternateNames")
        if uses:
            doc["$schema"] = "https://json-structure.org/meta/extended/v0/#"
            doc["$uses"] = sorted(uses)

    def _contains_key(self, value: Any, key: str) -> bool:
        if isinstance(value, dict):
            return key in value or any(self._contains_key(item, key) for item in value.values())
        if isinstance(value, list):
            return any(self._contains_key(item, key) for item in value)
        return False

    # ------------------------------------------------------------------ REGISTRATION

    def register_definition(
        self,
        avro_schema: Dict[str, Any],
        namespace: str | None,
        definitions: Dict[str, Any],
        is_root: bool = False,  # retained only for signature compatibility
    ) -> None:
        """
        Ensure `avro_schema` has an entry in `definitions`.
        """

        current_namespace, name = self.resolve_full_name(
            avro_schema.get("name", f"AnonymousType_{uuid.uuid4().hex}"),
            avro_schema.get("namespace", namespace),
        )
        fqn = self.get_fqn(current_namespace, name)

        if fqn in self.known_types:  # already built / in progress
            return

        self.known_types.add(fqn)

        created = self.build_type_definition(avro_schema, current_namespace, definitions)

        # Remove marker if nothing was actually created
        if fqn not in definitions and created is None:
            self.known_types.discard(fqn)

    # ------------------------------------------------------------------ BUILD TYPE

    def build_type_definition(self, avro_schema, namespace, definitions):
        if not isinstance(avro_schema, dict): # Should be a complex type dict
            return None

        avro_type = avro_schema.get("type")
        # Use the schema's own namespace if provided, otherwise fall back to the passed 'namespace'
        current_schema_namespace, name = self.resolve_full_name(
            avro_schema.get("name", f"AnonymousType_{uuid.uuid4().hex}"),
            avro_schema.get("namespace", namespace),
        )
        fqn = self.get_fqn(current_schema_namespace, name)

        if fqn in self.reference_stack:
            # Circular reference during the build of this specific definition.
            # Depending on JSON Structure spec, could return a $ref or handle as error.
            # For now, allowing it to proceed might lead to incomplete recursive definitions
            # if not handled carefully by $ref logic in resolve_avro_type.
            # However, known_types in register_definition should catch completed cycles.
            pass

        self.reference_stack.add(fqn)

        # This variable will hold the actual definition content (the value part of the key-value pair)
        type_definition_content = None

        if avro_type == "record":
            props = {"name": name, "type": "object", "properties": {}, "required": []}
            if "doc" in avro_schema:
                props["description"] = avro_schema["doc"]
            if isinstance(avro_schema.get("unique"), list):
                props["x-avrotize-unique"] = avro_schema["unique"]
            if isinstance(avro_schema.get("foreignKeys"), list):
                props["x-avrotize-foreignKeys"] = avro_schema["foreignKeys"]
            
            # Namespace for resolving field types within this record
            record_fields_namespace = avro_schema.get("namespace", namespace) 

            for field in avro_schema.get("fields", []):
                field_name = field["name"]
                field_type_schema = field["type"]
                
                resolved_field_type = self.resolve_avro_type(field_type_schema, record_fields_namespace, definitions)

                if "default" in field and field["default"] is not None:
                    # Optionality/nullability is conveyed by absence from ``required``;
                    # a redundant ``default: null`` is meaningless in JSON Structure Core
                    # (and would decorate a non-nullable type reference), so only concrete
                    # defaults are emitted.
                    resolved_field_type["default"] = self.encode_default_value(field["default"], resolved_field_type.get("type", "unknown"))
                
                if not self.is_nullable_union(field_type_schema):
                    props["required"].append(field_name)
                
                if "doc" in field:
                    resolved_field_type["description"] = field["doc"]
                
                props["properties"][field_name] = resolved_field_type
            type_definition_content = props
            
        elif avro_type == "enum":
            props = {"name": name, "type": "string", "enum": avro_schema["symbols"]}
            if "doc" in avro_schema:
                props["description"] = avro_schema["doc"]
            if "default" in avro_schema: # Avro enum default
                props["default"] = avro_schema["default"]
            type_definition_content = props

        elif avro_type == "fixed":
            props = {"name": name, "type": "binary", "byteLength": avro_schema["size"]} # Consider "maxLength" or custom prop
            if "doc" in avro_schema:
                props["description"] = avro_schema["doc"]
            type_definition_content = props
        
        elif isinstance(avro_type, str) and avro_schema.get("logicalType"):
            # This is a named type that is also a logical type, e.g. a named decimal
            props = self.resolve_logical_type(avro_schema["logicalType"], avro_schema)
            # Ensure name and description from the schema are part of the definition
            if "name" not in props: props["name"] = name 
            if "doc" in avro_schema and "description" not in props : props["description"] = avro_schema["doc"]
            type_definition_content = props
            
        elif isinstance(avro_type, (list, dict)) and not avro_schema.get("name"):
            # An anonymous complex type (array, map, union) is the schema itself.
            # It needs a generated name (which 'name' variable already holds).
            props = self.resolve_avro_type(avro_schema, current_schema_namespace, definitions)
            if "name" not in props: props["name"] = name # Ensure generated name is part of definition
            type_definition_content = props
        
        # else:
            # If avro_type is a primitive string (e.g. "string", "int") or a named type reference string,
            # it doesn't form a new entry in "definitions" by itself.
            # resolve_avro_type handles these cases by returning the primitive type object or a $ref.
            # So, type_definition_content remains None, and nothing is added to definitions here.

        # If a definition was constructed, add it to the definitions map with proper nesting.
        if type_definition_content is not None:
            parts = fqn.split('/')
            definition_name = parts[-1]
            if definition_name != name:
                type_definition_content["name"] = definition_name
                type_definition_content.setdefault("altnames", {})["avro"] = name
            current_level_dict = definitions
            for i, part_name in enumerate(parts):
                if i == len(parts) - 1: # Last part is the type name itself
                    current_level_dict[part_name] = type_definition_content
                else: # This is a namespace part
                    current_level_dict = current_level_dict.setdefault(part_name, {})
        
        self.reference_stack.remove(fqn)
        return type_definition_content # Return the definition object (or None)

    # ------------------------------------------------------------------ RESOLVE TYPE

    def resolve_avro_type(
        self,
        avro_type_schema: Any,
        context_namespace: str | None,
        definitions: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Convert any Avro type expression into a JSON-Structure node (or $ref).
        """

        # ------------------ STRING (primitive or reference) --------------
        if isinstance(avro_type_schema, str):
            if is_any_value_type(avro_type_schema):
                return {"type": "any"}
            if avro_type_schema in self.get_primitive_types():
                return {"type": self.get_primitive_types()[avro_type_schema]}
            # Named type reference
            if "." in avro_type_schema:
                ref_namespace, ref_name = self.resolve_full_name(
                    avro_type_schema, context_namespace
                )
                ref_fqn = self.get_fqn(ref_namespace, ref_name)
            else:
                ref_fqn = self.get_fqn(context_namespace, self.clean_name(avro_type_schema))
            # JSON Structure Core requires a type reference to be the value of the
            # ``type`` keyword (``{"type": {"$ref": ...}}``); a bare ``{"$ref": ...}``
            # is only permitted inside a type-union array. See spec 3.3.6 / 3.7.1.
            return {"type": {"$ref": f"#/definitions/{ref_fqn}"}}

        # ------------------ UNION ----------------------------------------
        if isinstance(avro_type_schema, list):
            if not self.avro_encoding and "null" in avro_type_schema:
                non_null = [t for t in avro_type_schema if t != "null"]
                if len(non_null) == 1:
                    # Optional short-form
                    return self.resolve_avro_type(non_null[0], context_namespace, definitions)

            choices: Dict[str, Any] = {}
            for member in avro_type_schema:
                if isinstance(member, str):
                    key = self.clean_name(member)
                elif isinstance(member, dict) and member.get("name"):
                    key = self.clean_name(member["name"])
                else:
                    key = f"anonymous_{uuid.uuid4().hex[:8]}"
                choices[key] = self.resolve_avro_type(member, context_namespace, definitions)

            return {"type": "choice", "choices": choices}

        # ------------------ DICT (complex inline) ------------------------
        if isinstance(avro_type_schema, dict):
            category = avro_type_schema.get("type")
            inline_ns = avro_type_schema.get("namespace", context_namespace)

            if category in ("record", "enum", "fixed"):
                # Ensure definition exists then reference it
                self.register_definition(avro_type_schema, inline_ns, definitions)
                ref_namespace, ref_name = self.resolve_full_name(
                    avro_type_schema["name"], inline_ns
                )
                ref_fqn = self.get_fqn(ref_namespace, ref_name)
                # Wrap the reference under ``type`` (see note above) so the emitted
                # property/items/values/choice node is a valid JSON Structure schema.
                return {"type": {"$ref": f"#/definitions/{ref_fqn}"}}

            if category == "array":
                return {
                    "type": "array",
                    "items": self.resolve_avro_type(
                        avro_type_schema["items"], inline_ns, definitions
                    ),
                }

            if category == "map":
                return {
                    "type": "map",
                    "values": self.resolve_avro_type(
                        avro_type_schema["values"], inline_ns, definitions
                    ),
                }

            logical_type = avro_type_schema.get("logicalType")
            if logical_type:
                return self.resolve_logical_type(logical_type, avro_type_schema)

            jtd_type = avro_type_schema.get("jtdType")
            if jtd_type:
                jtd_mapping = {
                    "boolean": "boolean",
                    "float32": "float",
                    "float64": "double",
                    "int8": "int8",
                    "uint8": "uint8",
                    "int16": "int16",
                    "uint16": "uint16",
                    "int32": "int32",
                    "uint32": "uint32",
                    "string": "string",
                }
                if jtd_type in jtd_mapping:
                    return {"type": jtd_mapping[jtd_type]}

            if category in self.get_primitive_types():
                return {"type": self.get_primitive_types()[category]}

        raise ValueError(f"Unsupported Avro type schema: {avro_type_schema}")

    # ------------------------------------------------------------------ HELPERS

    def is_nullable_union(self, avro_field_type_schema: Any) -> bool:
        return isinstance(avro_field_type_schema, list) and "null" in avro_field_type_schema

    def encode_default_value(self, value: Any, json_structure_type: str) -> Any:
        # Minimal – pass through. Extend for binary/base64 etc. if needed.
        return value

    def resolve_logical_type(self, logical_type: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Very small logical-type mapping demo. Extend as required.
        """
        mapping = {
            "timestamp-micros": {"type": "int64", "logicalType": "timestampMicros"},
            "timestamp-millis": {"type": "int64", "logicalType": "timestampMillis"},
            "date": {"type": "int32", "logicalType": "date"},
            "uuid": {"type": "string", "format": "uuid"},
            # Avrotize Schema rfc3339-* string temporal family -> JSON Structure native
            # temporal types (round-trips with jstructtoavro). See issue #335.
            "rfc3339-date": {"type": "date"},
            "rfc3339-time-millis": {"type": "time"},
            "rfc3339-time-micros": {"type": "time"},
            "rfc3339-timestamp-millis": {"type": "datetime"},
            "rfc3339-timestamp-micros": {"type": "datetime"},
            "rfc3339-local-timestamp-millis": {"type": "datetime"},
            "rfc3339-local-timestamp-micros": {"type": "datetime"},
            "rfc3339-duration": {"type": "duration"},
        }
        return mapping.get(logical_type, {"type": "string"})

    def clean_name(self, name: str) -> str:
        return name.replace(".", "_")

    def resolve_full_name(self, name: str, namespace: str | None) -> tuple[str | None, str]:
        if "." in name:
            namespace, name = name.rsplit(".", 1)
        return namespace, self.clean_name(name)

    def _prepare_definition_paths(
        self, avro_schema: Any, namespace: str | None = None
    ) -> None:
        full_names: set[str] = set()

        def collect(node: Any, current_namespace: str | None) -> None:
            if isinstance(node, list):
                for item in node:
                    collect(item, current_namespace)
                return
            if not isinstance(node, dict):
                return

            category = node.get("type")
            inline_namespace = node.get("namespace", current_namespace)
            if category in ("record", "enum", "fixed") and node.get("name"):
                type_namespace, type_name = self.resolve_full_name(
                    node["name"], inline_namespace
                )
                raw_fqn = self.get_raw_fqn(type_namespace, type_name)
                full_names.add(raw_fqn)
                if category == "record":
                    for field in node.get("fields", []):
                        collect(field.get("type"), type_namespace)
                return

            if isinstance(category, (dict, list)):
                collect(category, inline_namespace)
            elif category == "array":
                collect(node.get("items"), inline_namespace)
            elif category == "map":
                collect(node.get("values"), inline_namespace)

        collect(avro_schema, namespace)
        reserved_names = {"type", "definitions"}
        self.definition_paths.clear()
        for full_name in full_names:
            parts = full_name.split("/")
            encoded_parts = []
            for index, part in enumerate(parts):
                prefix = "/".join(parts[: index + 1])
                if index < len(parts) - 1 and prefix in full_names:
                    part += "_"
                elif index == len(parts) - 1 and part in reserved_names:
                    part += "_"
                encoded_parts.append(part)
            self.definition_paths[full_name] = "/".join(encoded_parts)

    @staticmethod
    def get_raw_fqn(namespace: str | None, name: str) -> str:
        if namespace:
            return f"{namespace.replace('.', '/')}/{name}"
        return name

    def get_fqn(self, namespace: str | None, name: str) -> str:
        raw_fqn = self.get_raw_fqn(namespace, name)
        return self.definition_paths.get(raw_fqn, raw_fqn)

    @staticmethod
    def get_primitive_types() -> Dict[str, str]:
        return {
            "string": "string",
            "boolean": "boolean",
            "int": "int32",
            "long": "int64",
            "float": "float",
            "double": "double",
            "bytes": "binary",
            "null": "null",
        }


# ---------------------------------------------------------------------- CLI HELPER

def convert_avro_to_json_structure(
    avro_schema_file: str,
    json_structure_file: str,
    naming_mode: str = "default",
    avro_encoding: bool = False,
) -> None:
    """
    Convenience wrapper: read Avro schema from file and write JSON-Structure out.
    """

    converter = AvroToJsonStructure(avro_encoding=avro_encoding)

    with open(avro_schema_file, "r", encoding="utf-8") as f:
        avro_schema = json.load(f)

    json_structure = converter.convert(avro_schema)

    with open(json_structure_file, "w", encoding="utf-8") as f:
        json.dump(json_structure, f, indent=4)
