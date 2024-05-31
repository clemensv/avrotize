""" Avro Tools Module """

import json
import hashlib
import base64
from typing import Dict, List, cast

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | int | bool | None

def transform_to_pcf(schema_json: str) -> str:
    """
    Transforms an Avro schema into its Parsing Canonical Form (PCF).
    
    :param schema_json: The Avro schema as a JSON string.
    :return: The Parsing Canonical Form (PCF) as a JSON string.
    """
    schema = json.loads(schema_json)
    canonical_schema = canonicalize_schema(schema)
    return json.dumps(canonical_schema, separators=(',', ':'))

def avsc_to_pcf(schema_file: str) -> None:
    """ Convert an Avro schema file to its Parsing Canonical Form (PCF)."""
    with open(schema_file, 'r', encoding='utf-8') as file:
        schema = json.load(file)
        print(transform_to_pcf(json.dumps(schema)))

def canonicalize_schema(schema: JsonNode, namespace:str="") -> JsonNode:
    """
    Recursively processes the schema to convert it to the Parsing Canonical Form (PCF).
    
    :param schema: The Avro schema as a dictionary.
    :param namespace: The current namespace for resolving names.
    :return: The canonicalized schema as a dictionary.
    """
    if isinstance(schema, str):
        return schema
    elif isinstance(schema, dict):
        if 'type' in schema and isinstance(schema['type'], str):
            if schema['type'] in PRIMITIVE_TYPES:
                return schema['type']
            if '.' not in schema['type'] and namespace:
                schema['type'] = namespace + '.' + schema['type']
        
        if 'name' in schema and '.' not in cast(str,schema['name']) and namespace:
            schema['name'] = namespace + '.' + cast(str,schema['name'])

        canonical = {}
        for field in FIELD_ORDER:
            if field in schema:
                value = schema[field]
                if field == 'fields' and isinstance(value, list):
                    value = [canonicalize_schema(f, cast(str,schema.get('namespace', namespace))) for f in value]
                elif field == 'symbols' or field == 'items' or field == 'values':
                    value = canonicalize_schema(value, namespace)
                elif isinstance(value, dict):
                    value = canonicalize_schema(value, namespace)
                elif isinstance(value, list):
                    value = [canonicalize_schema(v, namespace) for v in value]
                elif isinstance(value, str):
                    value = normalize_string(value)
                elif isinstance(value, int):
                    value = normalize_integer(value)
                canonical[field] = value
        return canonical
    elif isinstance(schema, list):
        return [canonicalize_schema(s, namespace) for s in schema]
    raise ValueError("Invalid schema: " + str(schema))

def normalize_string(value):
    """
    Normalizes JSON string literals by replacing escaped characters with their UTF-8 equivalents.
    
    :param value: The string value to normalize.
    :return: The normalized string.
    """
    return value.encode('utf-8').decode('unicode_escape')

def normalize_integer(value):
    """
    Normalizes JSON integer literals by removing leading zeros.
    
    :param value: The integer value to normalize.
    :return: The normalized integer.
    """
    return int(value)

def fingerprint_sha256(schema_json):
    """
    Generates a SHA-256 fingerprint for the given Avro schema.
    
    :param schema_json: The Avro schema as a JSON string.
    :return: The SHA-256 fingerprint as a base64 string.
    """
    pcf = transform_to_pcf(schema_json)
    sha256_hash = hashlib.sha256(pcf.encode('utf-8')).digest()
    return base64.b64encode(sha256_hash).decode('utf-8')

def fingerprint_md5(schema_json):
    """
    Generates an MD5 fingerprint for the given Avro schema.
    
    :param schema_json: The Avro schema as a JSON string.
    :return: The MD5 fingerprint as a base64 string.
    """
    pcf = transform_to_pcf(schema_json)
    md5_hash = hashlib.md5(pcf.encode('utf-8')).digest()
    return base64.b64encode(md5_hash).decode('utf-8')

def fingerprint_rabin(schema_json):
    """
    Generates a 64-bit Rabin fingerprint for the given Avro schema.
    
    :param schema_json: The Avro schema as a JSON string.
    :return: The Rabin fingerprint as a base64 string.
    """
    pcf = transform_to_pcf(schema_json).encode('utf-8')
    fp = fingerprint64(pcf)
    return base64.b64encode(fp.to_bytes(8, 'big')).decode('utf-8')

def fingerprint64(buf):
    """
    Computes a 64-bit Rabin fingerprint.
    
    :param buf: The input byte buffer.
    :return: The 64-bit Rabin fingerprint.
    """
    if FP_TABLE is None:
        init_fp_table()
    fp = EMPTY
    for byte in buf:
        fp = (fp >> 8) ^ FP_TABLE[(fp ^ byte) & 0xff]
    return fp

def init_fp_table():
    """
    Initializes the fingerprint table for the Rabin fingerprint algorithm.
    """
    global FP_TABLE
    FP_TABLE = []
    for i in range(256):
        fp = i
        for _ in range(8):
            fp = (fp >> 1) ^ (EMPTY & -(fp & 1))
        FP_TABLE.append(fp)

PRIMITIVE_TYPES = {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}
FIELD_ORDER = ["name", "type", "fields", "symbols", "items", "values", "size"]

EMPTY = 0xc15d213aa4d7a795
FP_TABLE = None

class PCFSchemaResult:
    def __init__(self, pcf: str, sha256: str, md5: str, rabin: str) -> None:
        self.pcf = pcf
        self.sha256 = sha256
        self.md5 = md5
        self.rabin = rabin

def pcf_schema(schema_json):
    """
    Wrapper function to provide PCF transformation and fingerprinting.
    
    :param schema_json: The Avro schema as a JSON string.
    :return: An instance of the PCFSchemaResult class containing the PCF and fingerprints (SHA-256, MD5, and Rabin) as base64 strings.
    """
    pcf = transform_to_pcf(schema_json)
    return PCFSchemaResult(pcf, fingerprint_sha256(schema_json), fingerprint_md5(schema_json), fingerprint_rabin(schema_json))

