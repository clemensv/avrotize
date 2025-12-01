"""Converts a JSON Structure schema to a Kusto table schema."""

import json
import sys
from typing import Any, List, Optional, Dict, Union
from avrotize.common import build_flat_type_dict, inline_avro_references, strip_first_doc
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties


class StructureToKusto:
    """Converts a JSON Structure schema to a Kusto table schema."""

    def __init__(self):
        """Initializes a new instance of the StructureToKusto class."""
        self.schema_registry: Dict[str, Dict] = {}
        self.processed_types: set = set()  # Track processed types to avoid duplicates

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None, schema_doc: Optional[Dict] = None) -> Optional[Dict]:
        """Resolves a $ref to the actual schema definition"""
        # Check if it's an absolute URI reference (schema with $id)
        if not ref.startswith('#/'):
            # Try to resolve from schema registry
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None
        
        # Handle fragment-only references (internal to document)
        path = ref[2:].split('/')
        schema = context_schema if context_schema else schema_doc
        
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        
        return schema

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """Recursively registers schemas with $id keywords"""
        if not isinstance(schema, dict):
            return
        
        # Register this schema if it has an $id
        if '$id' in schema:
            schema_id = schema['$id']
            # Handle relative URIs
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                from urllib.parse import urljoin
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            base_uri = schema_id  # Update base URI for nested schemas
        
        # Recursively process definitions
        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)
        
        # Recursively process properties
        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)
        
        # Recursively process items, values, etc.
        for key in ['items', 'values', 'additionalProperties']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)

    def flatten_inheritance(self, schema: Dict, schema_doc: Dict) -> Dict:
        """
        Flattens inheritance by merging properties from $extends base type.
        Returns a new schema with all properties merged.
        """
        if '$extends' not in schema:
            return schema
        
        flattened = schema.copy()
        base_ref = schema['$extends']
        
        # Resolve the base schema
        base_schema = self.resolve_ref(base_ref, schema_doc, schema_doc)
        if not base_schema:
            return flattened
        
        # Recursively flatten the base (in case it also extends something)
        flattened_base = self.flatten_inheritance(base_schema, schema_doc)
        
        # Merge properties: base properties first, then derived (derived can override)
        base_props = flattened_base.get('properties', {})
        derived_props = schema.get('properties', {})
        
        merged_props = {}
        merged_props.update(base_props)
        merged_props.update(derived_props)
        
        flattened['properties'] = merged_props
        
        # Merge required fields
        base_required = flattened_base.get('required', [])
        derived_required = schema.get('required', [])
        if base_required or derived_required:
            flattened['required'] = list(set(base_required + derived_required))
        
        # Add comment about flattened inheritance
        base_name = flattened_base.get('name', 'base type')
        orig_desc = flattened.get('description', '')
        if orig_desc:
            flattened['description'] = f"{orig_desc} (flattened from {base_name})"
        else:
            flattened['description'] = f"Flattened from {base_name}"
        
        # Remove $extends as it's now flattened
        if '$extends' in flattened:
            del flattened['$extends']
        
        return flattened

    def is_concrete_type(self, schema: Dict) -> bool:
        """Check if a type is concrete (not abstract)."""
        return not schema.get('abstract', False)

    def find_all_object_types(self, schema: Dict, schema_doc: Dict) -> List[Dict]:
        """
        Find all concrete object types in the schema, including those in definitions.
        Filters out abstract types and includes flattened versions of types with inheritance.
        """
        object_types = []
        
        def process_schema(s: Dict, path: str = ""):
            if not isinstance(s, dict):
                return
            
            # Check if this is an object type
            if s.get('type') == 'object':
                # Only include concrete types
                if self.is_concrete_type(s):
                    # Flatten inheritance if present
                    flattened = self.flatten_inheritance(s, schema_doc)
                    object_types.append(flattened)
            
            # Recursively process definitions
            if 'definitions' in s:
                for def_name, def_schema in s['definitions'].items():
                    if isinstance(def_schema, dict):
                        # Handle nested definitions
                        if def_schema.get('type') == 'object':
                            process_schema(def_schema, f"{path}/{def_name}")
                        else:
                            # Recurse into nested namespaces
                            for nested_key, nested_val in def_schema.items():
                                if isinstance(nested_val, dict):
                                    process_schema(nested_val, f"{path}/{def_name}/{nested_key}")
        
        # Process top-level schema
        if isinstance(schema, dict):
            if '$root' in schema:
                root_ref = schema['$root']
                root_schema = self.resolve_ref(root_ref, schema, schema)
                if root_schema:
                    process_schema(root_schema)
            elif 'type' in schema and schema['type'] == 'object':
                process_schema(schema)
            
            # Always process definitions
            if 'definitions' in schema:
                process_schema(schema)
        
        elif isinstance(schema, list):
            for s in schema:
                if isinstance(s, dict):
                    process_schema(s)
        
        return object_types

    def convert_record_to_kusto(self, recordschema: dict, schema_doc: dict, emit_cloudevents_columns: bool, emit_cloudevents_dispatch_table: bool) -> List[str]:
        """Converts a JSON Structure object schema to a Kusto table schema."""
        # Get the name and fields of the top-level record
        table_name = recordschema.get("name", "UnnamedTable")
        
        # Handle properties from JSON Structure
        properties = recordschema.get("properties", {})
        
        # Create a StringBuilder to store the kusto statements
        kusto = []

        # Append the create table statement with the column names and types
        kusto.append(f".create-merge table [{table_name}] (")
        columns = []
        for prop_name, prop_schema in properties.items():
            column_name = prop_name
            # Skip const fields - they will be documented but not create columns
            if isinstance(prop_schema, dict) and 'const' in prop_schema:
                continue
            column_type = self.convert_structure_type_to_kusto_type(prop_schema, schema_doc)
            columns.append(f"   [{column_name}]: {column_type}")
        if emit_cloudevents_columns:
            columns.append("   [___type]: string")
            columns.append("   [___source]: string")
            columns.append("   [___id]: string")
            columns.append("   [___time]: datetime")
            columns.append("   [___subject]: string")
        kusto.append(",\n".join(columns))
        kusto.append(");")
        kusto.append("")

        # Add the doc string as table metadata
        if "description" in recordschema or "doc" in recordschema:
            doc_data = recordschema.get("description", recordschema.get("doc", ""))
            doc_data = (doc_data[:997] + "...") if len(doc_data) > 1000 else doc_data
            
            # Add notes about flattened features
            notes = []
            if '$extends' in recordschema:
                notes.append("Note: Properties from base types have been flattened into this table.")
            if recordschema.get('abstract', False):
                notes.append("Warning: Abstract type - should not be instantiated directly.")
            
            if notes:
                doc_data = doc_data + " " + " ".join(notes)
            
            doc_string = json.dumps(json.dumps({
                "description": doc_data
            }))
            kusto.append(
                f".alter table [{table_name}] docstring {doc_string};")
            kusto.append("")

        doc_string_statement = []
        for prop_name, prop_schema in properties.items():
            column_name = prop_name
            
            # Handle const fields - document them but note they're const
            if isinstance(prop_schema, dict) and 'const' in prop_schema:
                const_value = prop_schema['const']
                doc_data = prop_schema.get("description", prop_schema.get("doc", ""))
                if doc_data:
                    doc_data = f"{doc_data} (const value: {json.dumps(const_value)})"
                else:
                    doc_data = f"Constant field with value: {json.dumps(const_value)}"
                doc_content = {"description": doc_data}
                doc = json.dumps(json.dumps(doc_content))
                # Add as comment - const fields are not stored in table
                kusto.insert(len(kusto) - (2 if kusto and kusto[-1] == "" else 1), 
                           f"-- Const field '{column_name}' with value: {json.dumps(const_value)}")
                continue
            
            if "description" in prop_schema or "doc" in prop_schema:
                doc_data = prop_schema.get("description", prop_schema.get("doc", ""))
                if len(doc_data) > 900:
                    doc_data = (doc_data[:897] + "...")
                doc_content = {
                    "description": doc_data
                }
                # Include schema info for complex types
                if isinstance(prop_schema, dict) and 'type' in prop_schema and prop_schema['type'] in ['object', 'array', 'map', 'set', 'choice', 'tuple']:
                    if (len(json.dumps(prop_schema)) + len(doc_data)) > 900:
                        doc_content["schema"] = '{ "doc": "Schema too large to inline. Please refer to the JSON Structure schema for more details." }'
                    else:
                        doc_content["schema"] = prop_schema
                doc = json.dumps(json.dumps(doc_content))
                doc_string_statement.append(f"   [{column_name}]: {doc}")
        if doc_string_statement and emit_cloudevents_columns:
            doc_string_statement.extend([
                "   [___type] : 'Event type'",
                "   [___source]: 'Context origin/source of the event'",
                "   [___id]: 'Event identifier'",
                "   [___time]: 'Event generation time'",
                "   [___subject]: 'Context subject of the event'"
            ])
        if doc_string_statement:
            kusto.append(f".alter table [{table_name}] column-docstrings (")
            kusto.append(",\n".join(doc_string_statement))
            kusto.append(");")
            kusto.append("")

        # add the JSON mapping for the table
        kusto.append(
            f".create-or-alter table [{table_name}] ingestion json mapping \"{table_name}_json_flat\"")
        kusto.append("```\n[")
        if emit_cloudevents_columns:
            kusto.append("  {\"column\": \"___type\", \"path\": \"$.type\"},")
            kusto.append(
                "  {\"column\": \"___source\", \"path\": \"$.source\"},")
            kusto.append("  {\"column\": \"___id\", \"path\": \"$.id\"},")
            kusto.append("  {\"column\": \"___time\", \"path\": \"$.time\"},")
            kusto.append(
                "  {\"column\": \"___subject\", \"path\": \"$.subject\"},")
        for prop_name, prop_schema in properties.items():
            # Skip const fields in JSON mapping since they're not stored as columns
            if isinstance(prop_schema, dict) and 'const' in prop_schema:
                continue
            column_name = prop_name
            kusto.append(
                f"  {{\"column\": \"{column_name}\", \"path\": \"$.{prop_name}\"}},")
        kusto.append("]\n```\n\n")

        if emit_cloudevents_columns:
            kusto.append(
                f".create-or-alter table [{table_name}] ingestion json mapping \"{table_name}_json_ce_structured\"")
            kusto.append("```\n[")
            kusto.append("  {\"column\": \"___type\", \"path\": \"$.type\"},")
            kusto.append(
                "  {\"column\": \"___source\", \"path\": \"$.source\"},")
            kusto.append("  {\"column\": \"___id\", \"path\": \"$.id\"},")
            kusto.append("  {\"column\": \"___time\", \"path\": \"$.time\"},")
            kusto.append(
                "  {\"column\": \"___subject\", \"path\": \"$.subject\"},")
            for prop_name, prop_schema in properties.items():
                # Skip const fields in JSON mapping since they're not stored as columns
                if isinstance(prop_schema, dict) and 'const' in prop_schema:
                    continue
                column_name = prop_name
                kusto.append(
                    f"  {{\"column\": \"{column_name}\", \"path\": \"$.data.{prop_name}\"}},")
            kusto.append("]\n```\n\n")

        if emit_cloudevents_columns:
            kusto.append(
                f".drop materialized-view {table_name}Latest ifexists;")
            kusto.append("")
            kusto.append(
                f".create materialized-view with (backfill=true) {table_name}Latest on table {table_name} {{")
            kusto.append(
                f"    {table_name} | summarize arg_max(___time, *) by ___type, ___source, ___subject")
            kusto.append("}")
            kusto.append("")

        if emit_cloudevents_dispatch_table:
            namespace = recordschema.get("namespace", "")
            event_type = namespace + "." + table_name if namespace else table_name

            query = f"_cloudevents_dispatch | where (specversion == '1.0' and type == '{event_type}') | " + \
                    "project"                        
            for prop_name, prop_schema in properties.items():
                column_name = prop_name
                column_type = self.convert_structure_type_to_kusto_type(prop_schema, schema_doc)
                query += f"['{column_name}'] = to{column_type}(data.['{column_name}']),"
            query += "___type = type,___source = source,___id = ['id'],___time = ['time'],___subject = subject"

            # build an update policy for the table that gets triggered by updates to the dispatch table and extracts the event
            kusto.append(f".alter table [{table_name}] policy update")
            kusto.append("```")
            kusto.append("[{")
            kusto.append("  \"IsEnabled\": true,")
            kusto.append("  \"Source\": \"_cloudevents_dispatch\",")
            kusto.append(
                f"  \"Query\": \"{query}\",")
            kusto.append("  \"IsTransactional\": false,")
            kusto.append("  \"PropagateIngestionProperties\": true,")
            kusto.append("}]")
            kusto.append("```\n")

        return kusto

    def convert_structure_to_kusto_script(self, structure_schema_path, structure_record_type, emit_cloudevents_columns=False, emit_cloudevents_dispatch_table=False) -> str:
        """Converts a JSON Structure schema to a Kusto table schema."""
        if emit_cloudevents_dispatch_table:
            emit_cloudevents_columns = True
        schema_file = structure_schema_path
        if not schema_file:
            print("Please specify the JSON Structure schema file")
            sys.exit(1)
        with open(schema_file, "r", encoding="utf-8") as f:
            schema_json = f.read()

        # Parse the schema as a JSON object
        schema = json.loads(schema_json)

        # Register schema IDs for $ref resolution
        if isinstance(schema, dict):
            self.register_schema_ids(schema)
        
        # Handle root-level array of schemas
        if isinstance(schema, list):
            for s in schema:
                if isinstance(s, dict):
                    self.register_schema_ids(s)
        
        # Find the record(s) to convert
        record_schemas = []
        schema_doc = None
        
        if isinstance(schema, list):
            schema_doc = schema[0] if schema else {}
            if structure_record_type:
                record_schema = next(
                    (x for x in schema if isinstance(x, dict) and x.get("name") == structure_record_type), None)
                if record_schema is None:
                    print(
                        f"No record type {structure_record_type} found in the JSON Structure schema")
                    sys.exit(1)
                # Flatten inheritance if present
                record_schemas = [self.flatten_inheritance(record_schema, schema_doc)]
            else:
                # Find all concrete object types
                all_types = self.find_all_object_types(schema, schema_doc)
                if all_types:
                    record_schemas = all_types
                else:
                    # Fallback to first object type
                    record_schema = next(
                        (x for x in schema if isinstance(x, dict) and x.get("type") == "object"), None)
                    if record_schema:
                        record_schemas = [self.flatten_inheritance(record_schema, schema_doc)]
        elif isinstance(schema, dict):
            schema_doc = schema
            # Check for $root reference
            if '$root' in schema:
                root_ref = schema['$root']
                record_schema = self.resolve_ref(root_ref, schema, schema)
                if record_schema:
                    # Flatten inheritance
                    record_schemas = [self.flatten_inheritance(record_schema, schema_doc)]
            elif 'type' in schema and schema['type'] == 'object':
                # Flatten inheritance
                record_schemas = [self.flatten_inheritance(schema, schema_doc)]
            elif not structure_record_type:
                # Find all concrete object types in definitions
                all_types = self.find_all_object_types(schema, schema_doc)
                if all_types:
                    record_schemas = all_types
                else:
                    # Look for object types in definitions (old fallback logic)
                    if 'definitions' in schema:
                        defs = schema['definitions']
                        for def_key, def_val in defs.items():
                            if isinstance(def_val, dict):
                                # Navigate nested definitions
                                for nested_key, nested_val in def_val.items():
                                    if isinstance(nested_val, dict) and nested_val.get('type') == 'object':
                                        if structure_record_type and nested_val.get('name') == structure_record_type:
                                            record_schemas = [self.flatten_inheritance(nested_val, schema_doc)]
                                            break
                                        elif not structure_record_type and self.is_concrete_type(nested_val):
                                            record_schemas.append(self.flatten_inheritance(nested_val, schema_doc))
                                if record_schemas and structure_record_type:
                                    break
            else:
                # Look for specific record type in definitions
                if 'definitions' in schema:
                    defs = schema['definitions']
                    for def_key, def_val in defs.items():
                        if isinstance(def_val, dict):
                            for nested_key, nested_val in def_val.items():
                                if isinstance(nested_val, dict) and nested_val.get('name') == structure_record_type:
                                    record_schemas = [self.flatten_inheritance(nested_val, schema_doc)]
                                    break
                        if record_schemas:
                            break
        
        if not record_schemas:
            print("Expected a JSON Structure schema with a root object type or a $root reference")
            sys.exit(1)

        kusto_script = []

        if emit_cloudevents_dispatch_table:
            kusto_script.append(
                ".create-merge table [_cloudevents_dispatch] (")
            kusto_script.append("    [specversion]: string,")
            kusto_script.append("    [type]: string,")
            kusto_script.append("    [source]: string,")
            kusto_script.append("    [id]: string,")
            kusto_script.append("    [time]: datetime,")
            kusto_script.append("    [subject]: string,")
            kusto_script.append("    [datacontenttype]: string,")
            kusto_script.append("    [dataschema]: string,")
            kusto_script.append("    [data]: dynamic")
            kusto_script.append(");\n\n")
            kusto_script.append(
                ".create-or-alter table [_cloudevents_dispatch] ingestion json mapping \"_cloudevents_dispatch_json\"")
            kusto_script.append("```\n[")
            kusto_script.append(
                "  {\"column\": \"specversion\", \"path\": \"$.specversion\"},")
            kusto_script.append(
                "  {\"column\": \"type\", \"path\": \"$.type\"},")
            kusto_script.append(
                "  {\"column\": \"source\", \"path\": \"$.source\"},")
            kusto_script.append("  {\"column\": \"id\", \"path\": \"$.id\"},")
            kusto_script.append(
                "  {\"column\": \"time\", \"path\": \"$.time\"},")
            kusto_script.append(
                "  {\"column\": \"subject\", \"path\": \"$.subject\"},")
            kusto_script.append(
                "  {\"column\": \"datacontenttype\", \"path\": \"$.datacontenttype\"},")
            kusto_script.append(
                "  {\"column\": \"dataschema\", \"path\": \"$.dataschema\"},")
            kusto_script.append(
                "  {\"column\": \"data\", \"path\": \"$.data\"}")
            kusto_script.append("]\n```\n\n")

        # Convert each record schema to Kusto
        for record_schema in record_schemas:
            if not isinstance(record_schema, dict) or "type" not in record_schema or record_schema["type"] != "object":
                continue
            
            # Skip abstract types that somehow made it through
            if not self.is_concrete_type(record_schema):
                continue
                
            kusto_script.extend(self.convert_record_to_kusto(
                record_schema, schema_doc, emit_cloudevents_columns, emit_cloudevents_dispatch_table))
        
        # Join and clean up extra blank lines at the end
        result = "\n".join(kusto_script)
        # Remove trailing whitespace while preserving intentional blank lines
        return result.rstrip() + "\n" if result else ""

    def convert_structure_to_kusto_file(self, structure_schema_path, structure_record_type, kusto_file_path, emit_cloudevents_columns=False, emit_cloudevents_dispatch_table=False):
        """Converts a JSON Structure schema to a Kusto table schema."""
        script = self.convert_structure_to_kusto_script(
            structure_schema_path, structure_record_type, emit_cloudevents_columns, emit_cloudevents_dispatch_table)
        with open(kusto_file_path, "w", encoding="utf-8") as kusto_file:
            kusto_file.write(script)

    def convert_structure_type_to_kusto_type(self, structure_type: Union[str, dict, list], schema_doc: Optional[Dict] = None) -> str:
        """Converts a JSON Structure type to a Kusto type."""
        if isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            if len(non_null_types) == 0:
                return "dynamic"
            elif len(non_null_types) == 1:
                return self.convert_structure_type_to_kusto_type(non_null_types[0], schema_doc)
            else:
                # Multiple non-null types - use dynamic
                return "dynamic"
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], schema_doc, schema_doc)
                if ref_schema:
                    return self.convert_structure_type_to_kusto_type(ref_schema, schema_doc)
                return "dynamic"
            
            # Handle enum keyword
            if 'enum' in structure_type:
                # Enums map to string in Kusto
                return "string"
            
            # Handle type keyword
            if 'type' not in structure_type:
                return "dynamic"
            
            struct_type = structure_type['type']
            
            # Handle complex types
            if struct_type in ['object', 'array', 'set', 'map', 'choice', 'tuple']:
                return "dynamic"
            else:
                return self.map_primitive_type(struct_type)
        elif isinstance(structure_type, str):
            return self.map_primitive_type(structure_type)
        
        return "dynamic"

    def map_primitive_type(self, type_value: str) -> str:
        """Maps a JSON Structure primitive type to a Kusto scalar type."""
        mapping = {
            # JSON primitive types
            'null': 'dynamic',
            'boolean': 'bool',
            'string': 'string',
            'integer': 'int',
            'number': 'real',
            
            # Extended integer types
            'int8': 'int',
            'uint8': 'int',
            'int16': 'int',
            'uint16': 'int',
            'int32': 'int',
            'uint32': 'long',  # uint32 can exceed int range
            'int64': 'long',
            'uint64': 'long',
            'int128': 'decimal',  # Use decimal for very large integers
            'uint128': 'decimal',
            
            # Extended float types
            'float8': 'real',
            'float': 'real',
            'double': 'real',
            'binary32': 'real',
            'binary64': 'real',
            'decimal': 'decimal',
            
            # Binary
            'binary': 'dynamic',
            
            # Date/time types
            'date': 'datetime',
            'time': 'timespan',
            'datetime': 'datetime',
            'timestamp': 'datetime',
            'duration': 'timespan',
            
            # Other types
            'uuid': 'guid',
            'uri': 'string',
            'jsonpointer': 'string',
            'any': 'dynamic'
        }
        
        return mapping.get(type_value, 'dynamic')


def convert_structure_to_kusto_file(structure_schema_path, structure_record_type, kusto_file_path, emit_cloudevents_columns=False, emit_cloudevents_dispatch_table=False):
    """Converts a JSON Structure schema to a Kusto table schema."""
    structure_to_kusto = StructureToKusto()
    structure_to_kusto.convert_structure_to_kusto_file(
        structure_schema_path, structure_record_type, kusto_file_path, emit_cloudevents_columns, emit_cloudevents_dispatch_table)


def convert_structure_to_kusto_db(structure_schema_path, structure_record_type, kusto_uri, kusto_database, emit_cloudevents_columns=False, emit_cloudevents_dispatch_table=False, token_provider=None):
    """Converts a JSON Structure schema to a Kusto table schema."""
    structure_to_kusto = StructureToKusto()
    script = structure_to_kusto.convert_structure_to_kusto_script(
        structure_schema_path, structure_record_type, emit_cloudevents_columns, emit_cloudevents_dispatch_table)
    kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(
        kusto_uri) if not token_provider else KustoConnectionStringBuilder.with_token_provider(kusto_uri, token_provider)
    client = KustoClient(kcsb)
    for statement in script.split("\n\n"):
        if statement.strip():
            try:
                client.execute_mgmt(kusto_database, statement)
            except Exception as e:
                print(e)
                sys.exit(1)


def convert_structure_to_kusto(structure_schema_path, structure_record_type, kusto_file_path, kusto_uri, kusto_database, emit_cloudevents_columns=False, emit_cloudevents_dispatch_table=False, token_provider=None):
    """Converts a JSON Structure schema to a Kusto table schema."""
    if not kusto_uri and not kusto_database:
        convert_structure_to_kusto_file(
            structure_schema_path, structure_record_type, kusto_file_path, emit_cloudevents_columns, emit_cloudevents_dispatch_table)
    else:
        convert_structure_to_kusto_db(
            structure_schema_path, structure_record_type, kusto_uri, kusto_database, emit_cloudevents_columns, emit_cloudevents_dispatch_table, token_provider)
