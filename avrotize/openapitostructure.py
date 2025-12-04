"""
OpenAPI to JSON Structure converter.

This module converts OpenAPI 3.x documents to JSON Structure format by extracting
components.schemas and delegating to the existing JSON Schema to JSON Structure converter.
"""

# pylint: disable=line-too-long

import json
import os
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse

import requests

from avrotize.jsonstostructure import JsonToStructureConverter


class OpenApiToStructureConverter:
    """
    Converts OpenAPI 3.x documents to JSON Structure format.
    
    This converter extracts schema definitions from `components.schemas` in an OpenAPI
    document and converts them to JSON Structure format using the existing JSON Schema
    to JSON Structure conversion machinery.
    
    Attributes:
        root_namespace: The namespace for the root schema.
        root_class_name: The name of the root class.
        preserve_composition: Flag to preserve composition keywords.
        detect_inheritance: Flag to detect inheritance patterns.
        detect_discriminators: Flag to detect OpenAPI discriminator patterns.
        convert_empty_objects_to_maps: Flag to convert objects with only additionalProperties to maps.
        lift_inline_schemas: Flag to lift inline schemas from paths to definitions.
    """
    
    def __init__(self) -> None:
        """Initialize the OpenAPI to JSON Structure converter."""
        self.root_namespace = 'example.com'
        self.root_class_name = 'document'
        self.preserve_composition = True
        self.detect_inheritance = True
        self.detect_discriminators = True
        self.convert_empty_objects_to_maps = True
        self.lift_inline_schemas = False  # Optional: lift inline schemas from paths
        self.content_cache: Dict[str, str] = {}
    
    def fetch_content(self, url: str) -> str:
        """
        Fetch content from a URL or file path.
        
        Args:
            url: The URL or file path to fetch content from.
            
        Returns:
            The content as a string.
            
        Raises:
            requests.RequestException: If there is an error fetching from HTTP/HTTPS.
            FileNotFoundError: If the file does not exist.
        """
        if url in self.content_cache:
            return self.content_cache[url]
        
        parsed_url = urlparse(url)
        
        if parsed_url.scheme in ['http', 'https']:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            content = response.text
            self.content_cache[url] = content
            return content
        elif parsed_url.scheme == 'file' or not parsed_url.scheme:
            # Handle file URLs or local paths
            file_path = parsed_url.path if parsed_url.scheme == 'file' else url
            if os.name == 'nt' and file_path.startswith('/'):
                file_path = file_path[1:]
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                self.content_cache[url] = content
                return content
        else:
            raise ValueError(f"Unsupported URL scheme: {parsed_url.scheme}")
    
    def extract_schemas_from_openapi(self, openapi_doc: dict) -> dict:
        """
        Extract schemas from an OpenAPI document and convert to JSON Schema format.
        
        This method extracts all schema definitions from `components.schemas` and
        creates a consolidated JSON Schema document that can be processed by the
        existing JSON Schema to JSON Structure converter.
        
        Args:
            openapi_doc: The OpenAPI document as a dictionary.
            
        Returns:
            A JSON Schema document with definitions from the OpenAPI document.
        """
        json_schema: Dict[str, Any] = {
            "$schema": "http://json-schema.org/draft-07/schema#"
        }
        
        # Copy OpenAPI info to JSON Schema metadata
        if 'info' in openapi_doc:
            info = openapi_doc['info']
            if 'title' in info:
                json_schema['title'] = info['title']
            if 'description' in info:
                json_schema['description'] = info['description']
            if 'version' in info:
                json_schema['$id'] = f"https://{self.root_namespace}/schemas/{info.get('title', 'openapi').lower().replace(' ', '-')}/{info['version']}"
        
        # Extract components.schemas
        components = openapi_doc.get('components', {})
        schemas = components.get('schemas', {})
        
        if schemas:
            # Convert OpenAPI schemas to JSON Schema definitions
            json_schema['definitions'] = {}
            for schema_name, schema_def in schemas.items():
                # Process the schema to handle OpenAPI-specific keywords
                processed_schema = self._process_openapi_schema(schema_def)
                json_schema['definitions'][schema_name] = processed_schema
        
        # Optionally lift inline schemas from paths
        if self.lift_inline_schemas:
            self._lift_inline_schemas_from_paths(openapi_doc, json_schema)
        
        return json_schema
    
    def _process_openapi_schema(self, schema: Union[dict, Any]) -> Union[dict, Any]:
        """
        Process an OpenAPI schema to handle OpenAPI-specific keywords.
        
        This method converts OpenAPI-specific keywords to JSON Structure metadata
        annotations or handles them appropriately.
        
        Args:
            schema: The OpenAPI schema object.
            
        Returns:
            The processed schema with OpenAPI keywords handled.
        """
        if not isinstance(schema, dict):
            return schema
        
        processed: Dict[str, Any] = {}
        
        for key, value in schema.items():
            if key == 'nullable':
                # OpenAPI nullable is handled by making type a union with null
                # This is handled during conversion, store it for later processing
                processed['nullable'] = value
            elif key == 'readOnly':
                # Map to JSON Structure metadata annotation
                if 'x-metadata' not in processed:
                    processed['x-metadata'] = {}
                processed['x-metadata']['readOnly'] = value
            elif key == 'writeOnly':
                # Map to JSON Structure metadata annotation
                if 'x-metadata' not in processed:
                    processed['x-metadata'] = {}
                processed['x-metadata']['writeOnly'] = value
            elif key == 'deprecated':
                # Map to JSON Structure metadata annotation
                if 'x-metadata' not in processed:
                    processed['x-metadata'] = {}
                processed['x-metadata']['deprecated'] = value
            elif key == 'discriminator':
                # OpenAPI discriminator - process and convert mapping references
                if isinstance(value, dict):
                    processed_discriminator = dict(value)
                    if 'mapping' in processed_discriminator:
                        # Convert mapping references from OpenAPI to JSON Schema format
                        converted_mapping = {}
                        for key_name, ref_value in processed_discriminator['mapping'].items():
                            if isinstance(ref_value, str) and ref_value.startswith('#/components/schemas/'):
                                converted_mapping[key_name] = ref_value.replace('#/components/schemas/', '#/definitions/')
                            else:
                                converted_mapping[key_name] = ref_value
                        processed_discriminator['mapping'] = converted_mapping
                    processed['discriminator'] = processed_discriminator
                else:
                    processed['discriminator'] = value
            elif key == 'xml':
                # OpenAPI XML object - map to metadata
                if 'x-metadata' not in processed:
                    processed['x-metadata'] = {}
                processed['x-metadata']['xml'] = value
            elif key == 'externalDocs':
                # Map to JSON Structure metadata
                if 'x-metadata' not in processed:
                    processed['x-metadata'] = {}
                processed['x-metadata']['externalDocs'] = value
            elif key == 'example':
                # Map example to JSON Schema examples array
                if 'examples' not in processed:
                    processed['examples'] = []
                processed['examples'].append(value)
            elif key == 'examples':
                # OpenAPI examples object
                processed['examples'] = list(value.values()) if isinstance(value, dict) else value
            elif key == '$ref':
                # Handle OpenAPI references - convert to JSON Schema format
                ref = value
                if ref.startswith('#/components/schemas/'):
                    # Convert OpenAPI ref to JSON Schema ref
                    ref = ref.replace('#/components/schemas/', '#/definitions/')
                processed['$ref'] = ref
            elif key in ('properties', 'additionalProperties', 'patternProperties'):
                # Recursively process nested schemas
                if key == 'properties' and isinstance(value, dict):
                    processed[key] = {
                        prop_name: self._process_openapi_schema(prop_schema)
                        for prop_name, prop_schema in value.items()
                    }
                elif key == 'additionalProperties' and isinstance(value, dict):
                    processed[key] = self._process_openapi_schema(value)
                elif key == 'patternProperties' and isinstance(value, dict):
                    processed[key] = {
                        pattern: self._process_openapi_schema(prop_schema)
                        for pattern, prop_schema in value.items()
                    }
                else:
                    processed[key] = value
            elif key == 'items':
                # Recursively process array items
                if isinstance(value, dict):
                    processed[key] = self._process_openapi_schema(value)
                elif isinstance(value, list):
                    processed[key] = [self._process_openapi_schema(item) for item in value]
                else:
                    processed[key] = value
            elif key in ('allOf', 'anyOf', 'oneOf'):
                # Recursively process composition schemas
                if isinstance(value, list):
                    processed[key] = [self._process_openapi_schema(item) for item in value]
                else:
                    processed[key] = value
            elif key == 'not':
                # Recursively process negation schema
                processed[key] = self._process_openapi_schema(value)
            else:
                # Pass through all other keywords
                processed[key] = value
        
        # Handle nullable by converting to union with null
        if processed.get('nullable') is True:
            if 'type' in processed:
                current_type = processed['type']
                if isinstance(current_type, list):
                    if 'null' not in current_type:
                        processed['type'] = current_type + ['null']
                else:
                    processed['type'] = [current_type, 'null']
            # Remove the nullable keyword after processing
            del processed['nullable']
        elif 'nullable' in processed:
            del processed['nullable']
        
        return processed
    
    def _lift_inline_schemas_from_paths(self, openapi_doc: dict, json_schema: dict) -> None:
        """
        Optionally lift inline schemas from paths/operations into named definitions.
        
        This method extracts inline schemas from request bodies, responses, and parameters
        in the OpenAPI paths section and adds them to the definitions.
        
        Args:
            openapi_doc: The OpenAPI document.
            json_schema: The JSON Schema being built (modified in place).
        """
        if 'definitions' not in json_schema:
            json_schema['definitions'] = {}
        
        paths = openapi_doc.get('paths', {})
        
        for path, path_item in paths.items():
            if not isinstance(path_item, dict):
                continue
            
            for method in ['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace']:
                operation = path_item.get(method)
                if not isinstance(operation, dict):
                    continue
                
                operation_id = operation.get('operationId', f"{method}_{path.replace('/', '_')}")
                
                # Extract request body schemas
                request_body = operation.get('requestBody', {})
                if isinstance(request_body, dict):
                    content = request_body.get('content', {})
                    for media_type, media_type_obj in content.items():
                        if isinstance(media_type_obj, dict) and 'schema' in media_type_obj:
                            schema = media_type_obj['schema']
                            if isinstance(schema, dict) and '$ref' not in schema:
                                # Inline schema - lift to definitions
                                def_name = f"{operation_id}_Request"
                                processed = self._process_openapi_schema(schema)
                                json_schema['definitions'][def_name] = processed
                
                # Extract response schemas
                responses = operation.get('responses', {})
                for status_code, response in responses.items():
                    if isinstance(response, dict):
                        content = response.get('content', {})
                        for media_type, media_type_obj in content.items():
                            if isinstance(media_type_obj, dict) and 'schema' in media_type_obj:
                                schema = media_type_obj['schema']
                                if isinstance(schema, dict) and '$ref' not in schema:
                                    # Inline schema - lift to definitions
                                    def_name = f"{operation_id}_{status_code}_Response"
                                    processed = self._process_openapi_schema(schema)
                                    json_schema['definitions'][def_name] = processed
    
    def convert_openapi_to_structure(
        self, 
        openapi_doc: Union[dict, str], 
        base_uri: str = ''
    ) -> dict:
        """
        Convert an OpenAPI document to JSON Structure format.
        
        Args:
            openapi_doc: The OpenAPI document as a dictionary or JSON string.
            base_uri: The base URI for resolving references.
            
        Returns:
            The JSON Structure document.
            
        Raises:
            ValueError: If the input is not a valid OpenAPI document.
            TypeError: If the input type is not supported.
        """
        # Parse JSON string if needed
        if isinstance(openapi_doc, str):
            openapi_doc = json.loads(openapi_doc)
        
        if not isinstance(openapi_doc, dict):
            raise TypeError(f"Expected dict or str, got {type(openapi_doc)}")
        
        # Validate OpenAPI version
        openapi_version = openapi_doc.get('openapi', '')
        if not openapi_version.startswith('3.'):
            if 'swagger' in openapi_doc:
                raise ValueError(f"Swagger 2.x documents are not supported. Please convert to OpenAPI 3.x first.")
            if not openapi_version:
                raise ValueError("Not a valid OpenAPI document: missing 'openapi' version field")
            raise ValueError(f"Unsupported OpenAPI version: {openapi_version}. Only OpenAPI 3.x is supported.")
        
        # Extract schemas from OpenAPI and convert to JSON Schema format
        json_schema = self.extract_schemas_from_openapi(openapi_doc)
        
        # Use the JSON Schema to JSON Structure converter
        json_converter = JsonToStructureConverter()
        json_converter.root_namespace = self.root_namespace
        json_converter.root_class_name = self.root_class_name
        json_converter.preserve_composition = self.preserve_composition
        json_converter.detect_inheritance = self.detect_inheritance
        json_converter.detect_discriminators = self.detect_discriminators
        json_converter.convert_empty_objects_to_maps = self.convert_empty_objects_to_maps
        
        # Convert to JSON Structure
        structure_schema = json_converter.convert_json_schema_to_structure(json_schema, base_uri)
        
        return structure_schema
    
    def convert_openapi_file_to_structure(
        self,
        openapi_file_path: str,
        structure_file_path: Optional[str] = None
    ) -> dict:
        """
        Convert an OpenAPI file to JSON Structure format.
        
        Args:
            openapi_file_path: Path to the input OpenAPI file.
            structure_file_path: Optional path for the output JSON Structure file.
            
        Returns:
            The JSON Structure document.
        """
        # Read the OpenAPI file
        with open(openapi_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Detect format (JSON or YAML)
        try:
            openapi_doc = json.loads(content)
        except json.JSONDecodeError:
            # Try YAML
            try:
                import yaml
                openapi_doc = yaml.safe_load(content)
            except ImportError:
                raise ValueError("YAML support requires PyYAML. Install with: pip install pyyaml")
            except Exception as e:
                raise ValueError(f"Failed to parse OpenAPI document as JSON or YAML: {e}")
        
        # Convert to JSON Structure
        base_uri = f"file://{os.path.abspath(openapi_file_path)}"
        structure_schema = self.convert_openapi_to_structure(openapi_doc, base_uri)
        
        # Write output if path specified
        if structure_file_path:
            with open(structure_file_path, 'w', encoding='utf-8') as f:
                json.dump(structure_schema, f, indent=2)
        
        return structure_schema


def convert_openapi_to_structure(
    input_data: str,
    root_namespace: str = 'example.com'
) -> str:
    """
    Convert an OpenAPI document to JSON Structure format.
    
    Args:
        input_data: The OpenAPI document as a JSON string.
        root_namespace: The namespace for the root schema.
        
    Returns:
        The JSON Structure document as a JSON string.
    """
    converter = OpenApiToStructureConverter()
    converter.root_namespace = root_namespace
    
    result = converter.convert_openapi_to_structure(input_data)
    
    return json.dumps(result, indent=2)


def convert_openapi_to_structure_files(
    openapi_file_path: str,
    structure_schema_path: str,
    root_namespace: Optional[str] = None,
    preserve_composition: bool = True,
    detect_discriminators: bool = True,
    lift_inline_schemas: bool = False
) -> None:
    """
    Convert an OpenAPI file to JSON Structure format.
    
    Args:
        openapi_file_path: Path to the input OpenAPI file.
        structure_schema_path: Path to the output JSON Structure file.
        root_namespace: The namespace for the root schema.
        preserve_composition: Flag to preserve composition keywords.
        detect_discriminators: Flag to detect OpenAPI discriminator patterns.
        lift_inline_schemas: Flag to lift inline schemas from paths to definitions.
    """
    if root_namespace is None:
        root_namespace = 'example.com'
    
    converter = OpenApiToStructureConverter()
    converter.root_namespace = root_namespace
    converter.preserve_composition = preserve_composition
    converter.detect_discriminators = detect_discriminators
    converter.lift_inline_schemas = lift_inline_schemas
    
    converter.convert_openapi_file_to_structure(openapi_file_path, structure_schema_path)
