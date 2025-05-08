import json
import uuid
from typing import Any, Dict, List, Union


class AvroToJsonStructure:
    """
    Convert (one or more) Avro schemas into a single JSON-Structure document.
    """

    def __init__(self, avro_encoding: bool = False) -> None:
        self.known_types: set[str] = set()
        self.reference_stack: set[str] = set()
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

            # TEMPORARY: process only first element
            first = avro_schema[0]
            if isinstance(first, dict):
                return self.convert(first, namespace)

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

        current_namespace = avro_schema.get("namespace", namespace)
        name = self.clean_name(
            avro_schema.get("name", f"AnonymousType_{uuid.uuid4().hex}")
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
        return doc

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

        current_namespace = avro_schema.get("namespace", namespace)
        name = self.clean_name(
            avro_schema.get("name", f"AnonymousType_{uuid.uuid4().hex}")
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
        current_schema_namespace = avro_schema.get("namespace", namespace)
        name = self.clean_name(avro_schema.get("name", f"AnonymousType_{uuid.uuid4().hex}"))
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
            
            # Namespace for resolving field types within this record
            record_fields_namespace = avro_schema.get("namespace", namespace) 

            for field in avro_schema.get("fields", []):
                field_name = field["name"]
                field_type_schema = field["type"]
                
                resolved_field_type = self.resolve_avro_type(field_type_schema, record_fields_namespace, definitions)

                if "default" in field:
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
            if avro_type_schema in self.get_primitive_types():
                return {"type": self.get_primitive_types()[avro_type_schema]}
            # Named type reference
            if "." in avro_type_schema:
                ref_fqn = avro_type_schema.replace(".", "/")
            else:
                ref_fqn = self.get_fqn(context_namespace, self.clean_name(avro_type_schema))
            return {"$ref": f"#/definitions/{ref_fqn}"}

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
                ref_name = self.clean_name(avro_type_schema["name"])
                ref_fqn = self.get_fqn(inline_ns, ref_name)
                return {"$ref": f"#/definitions/{ref_fqn}"}

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
        }
        return mapping.get(logical_type, {"type": "string"})

    def clean_name(self, name: str) -> str:
        return name.replace(".", "_")

    def get_fqn(self, namespace: str | None, name: str) -> str:
        if namespace:
            return f"{namespace.replace('.', '/')}/{name}"
        return name

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
