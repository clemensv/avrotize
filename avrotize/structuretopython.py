# pylint: disable=line-too-long

""" StructureToPython class for converting JSON Structure schema to Python classes """

import json
import os
import re
import random
from typing import Any, Dict, List, Set, Tuple, Union, Optional

from avrotize.common import pascal, process_template
from avrotize.jstructtoavro import JsonStructureToAvro

JsonNode = Dict[str, 'JsonNode'] | List['JsonNode'] | str | None

INDENT = '    '

# Python standard library modules that should not be shadowed by package names
PYTHON_STDLIB_MODULES = {
    'abc', 'aifc', 'argparse', 'array', 'ast', 'asynchat', 'asyncio', 'asyncore',
    'atexit', 'audioop', 'base64', 'bdb', 'binascii', 'binhex', 'bisect', 'builtins',
    'bz2', 'calendar', 'cgi', 'cgitb', 'chunk', 'cmath', 'cmd', 'code', 'codecs',
    'codeop', 'collections', 'colorsys', 'compileall', 'concurrent', 'configparser',
    'contextlib', 'contextvars', 'copy', 'copyreg', 'cProfile', 'crypt', 'csv',
    'ctypes', 'curses', 'dataclasses', 'datetime', 'dbm', 'decimal', 'difflib',
    'dis', 'distutils', 'doctest', 'email', 'encodings', 'enum', 'errno', 'faulthandler',
    'fcntl', 'filecmp', 'fileinput', 'fnmatch', 'fractions', 'ftplib', 'functools',
    'gc', 'getopt', 'getpass', 'gettext', 'glob', 'graphlib', 'grp', 'gzip',
    'hashlib', 'heapq', 'hmac', 'html', 'http', 'imaplib', 'imghdr', 'imp',
    'importlib', 'inspect', 'io', 'ipaddress', 'itertools', 'json', 'keyword',
    'lib2to3', 'linecache', 'locale', 'logging', 'lzma', 'mailbox', 'mailcap',
    'marshal', 'math', 'mimetypes', 'mmap', 'modulefinder', 'multiprocessing',
    'netrc', 'nis', 'nntplib', 'numbers', 'operator', 'optparse', 'os', 'ossaudiodev',
    'pathlib', 'pdb', 'pickle', 'pickletools', 'pipes', 'pkgutil', 'platform',
    'plistlib', 'poplib', 'posix', 'posixpath', 'pprint', 'profile', 'pstats',
    'pty', 'pwd', 'py_compile', 'pyclbr', 'pydoc', 'queue', 'quopri', 'random',
    're', 'readline', 'reprlib', 'resource', 'rlcompleter', 'runpy', 'sched',
    'secrets', 'select', 'selectors', 'shelve', 'shlex', 'shutil', 'signal',
    'site', 'smtpd', 'smtplib', 'sndhdr', 'socket', 'socketserver', 'spwd',
    'sqlite3', 'ssl', 'stat', 'statistics', 'string', 'stringprep', 'struct',
    'subprocess', 'sunau', 'symtable', 'sys', 'sysconfig', 'syslog', 'tabnanny',
    'tarfile', 'telnetlib', 'tempfile', 'termios', 'test', 'textwrap', 'threading',
    'time', 'timeit', 'tkinter', 'token', 'tokenize', 'trace', 'traceback',
    'tracemalloc', 'tty', 'turtle', 'turtledemo', 'types', 'typing', 'unicodedata',
    'unittest', 'urllib', 'uu', 'uuid', 'venv', 'warnings', 'wave', 'weakref',
    'webbrowser', 'winreg', 'winsound', 'wsgiref', 'xdrlib', 'xml', 'xmlrpc',
    'zipapp', 'zipfile', 'zipimport', 'zlib', 'zoneinfo',
}


def is_python_reserved_word(word: str) -> bool:
    """Checks if a word is a Python reserved word"""
    reserved_words = [
        'False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await',
        'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except',
        'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is',
        'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return',
        'try', 'while', 'with', 'yield', 'record', 'self', 'cls'
    ]
    return word in reserved_words


def safe_package_name(name: str) -> str:
    """Converts a name to a safe Python package name that won't shadow stdlib"""
    if name.lower() in PYTHON_STDLIB_MODULES:
        return f"{name}_types"
    return name


class StructureToPython:
    """ Converts JSON Structure schema to Python classes """

    def __init__(self, base_package: str = '', dataclasses_json_annotation=False, avro_annotation=False) -> None:
        self.base_package = base_package
        self.dataclasses_json_annotation = dataclasses_json_annotation
        self.avro_annotation = avro_annotation
        self.output_dir = os.getcwd()
        self.schema_doc: JsonNode = None
        self.generated_types: Dict[str, str] = {}
        self.generated_structure_types: Dict[str, Dict[str, Union[str, Dict, List]]] = {}
        self.generated_enum_symbols: Dict[str, List[str]] = {}
        self.type_dict: Dict[str, Dict] = {}
        self.definitions: Dict[str, Any] = {}
        self.schema_registry: Dict[str, Dict] = {}

    def get_qualified_name(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        return f"{namespace}.{name}" if namespace != '' else name

    def concat_namespace(self, namespace: str, name: str) -> str:
        """ Concatenates namespace and name with a dot separator """
        if namespace and name:
            return f"{namespace}.{name}"
        elif namespace:
            return namespace
        else:
            return name

    def map_primitive_to_python(self, structure_type: str) -> str:
        """ Maps JSON Structure primitive types to Python types """
        mapping = {
            'null': 'None',
            'boolean': 'bool',
            'string': 'str',
            'integer': 'int',
            'number': 'float',
            'int8': 'int',
            'uint8': 'int',
            'int16': 'int',
            'uint16': 'int',
            'int32': 'int',
            'uint32': 'int',
            'int64': 'int',
            'uint64': 'int',
            'int128': 'int',
            'uint128': 'int',
            'float8': 'float',
            'float': 'float',
            'double': 'float',
            'binary32': 'float',
            'binary64': 'float',
            'decimal': 'decimal.Decimal',
            'binary': 'bytes',
            'date': 'datetime.date',
            'time': 'datetime.time',
            'datetime': 'datetime.datetime',
            'timestamp': 'datetime.datetime',
            'duration': 'datetime.timedelta',
            'uuid': 'uuid.UUID',
            'uri': 'str',
            'jsonpointer': 'str',
            'any': 'typing.Any'
        }
        qualified_class_name = self.get_qualified_name(
            self.base_package.lower(), structure_type.lower())
        if qualified_class_name in self.generated_types:
            result = qualified_class_name
        else:
            result = mapping.get(structure_type, 'typing.Any')
        return result

    def is_python_primitive(self, type_name: str) -> bool:
        """ Checks if a type is a Python primitive type """
        return type_name in ['None', 'bool', 'int', 'float', 'str', 'bytes']

    def is_python_typing_struct(self, type_name: str) -> bool:
        """ Checks if a type is a Python typing type """
        return type_name.startswith('typing.Dict[') or type_name.startswith('typing.List[') or \
               type_name.startswith('typing.Optional[') or type_name.startswith('typing.Union[') or \
               type_name == 'typing.Any'

    def safe_identifier(self, name: str, class_name: str = '', fallback_prefix: str = 'field') -> str:
        """Converts a name to a safe Python identifier.
        
        Handles:
        - Reserved words (append _)
        - Numeric prefixes (prepend _)
        - Special characters (replace with _)
        - All-special-char names (use fallback_prefix)
        - Class name collision (append _)
        """
        import re
        # Replace invalid characters with underscores
        safe = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
        # Remove leading/trailing underscores from sanitization, but keep intentional ones
        safe = safe.strip('_') if safe != name else safe
        # If nothing left after removing special chars, use fallback
        if not safe or not re.match(r'^[a-zA-Z_]', safe):
            if safe and re.match(r'^[0-9]', safe):
                safe = '_' + safe  # Numeric prefix
            else:
                safe = fallback_prefix + '_' + (safe if safe else 'unnamed')
        # Handle reserved words
        if is_python_reserved_word(safe):
            safe = safe + '_'
        # Handle class name collision
        if class_name and safe == class_name:
            safe = safe + '_'
        return safe

    def safe_name(self, name: str) -> str:
        """Converts a name to a safe Python name (legacy wrapper)"""
        return self.safe_identifier(name)

    def pascal_type_name(self, ref: str) -> str:
        """Converts a reference to a type name"""
        return '_'.join([pascal(part) for part in ref.split('.')[-1].split('_')])

    def python_package_from_structure_type(self, namespace: str, type_name: str) -> str:
        """Gets the Python package from a type name"""
        type_name_package = '.'.join([part.lower() for part in type_name.split('.')]) if '.' in type_name else type_name.lower()
        if '.' in type_name:
            package = type_name_package
        else:
            namespace_package = '.'.join([part.lower() for part in namespace.split('.')]) if namespace else ''
            package = namespace_package + ('.' if namespace_package and type_name_package else '') + type_name_package
        if self.base_package:
            package = self.base_package + '.' + package
        return package

    def python_type_from_structure_type(self, type_name: str) -> str:
        """Gets the Python class from a type name"""
        return self.pascal_type_name(type_name)

    def python_fully_qualified_name_from_structure_type(self, namespace: str, type_name: str) -> str:
        """Gets the fully qualified Python class name from a Structure type."""
        package = self.python_package_from_structure_type(namespace, type_name)
        return package + ('.' if package else '') + self.python_type_from_structure_type(type_name)

    def strip_package_from_fully_qualified_name(self, fully_qualified_name: str) -> str:
        """Strips the package from a fully qualified name"""
        return fully_qualified_name.split('.')[-1]

    def resolve_ref(self, ref: str, context_schema: Optional[Dict] = None) -> Optional[Dict]:
        """ Resolves a $ref to the actual schema definition """
        if not ref.startswith('#/'):
            if ref in self.schema_registry:
                return self.schema_registry[ref]
            return None

        path = ref[2:].split('/')
        schema = context_schema if context_schema else self.schema_doc
        for part in path:
            if not isinstance(schema, dict) or part not in schema:
                return None
            schema = schema[part]
        return schema

    def register_schema_ids(self, schema: Dict, base_uri: str = '') -> None:
        """ Recursively registers schemas with $id keywords """
        if not isinstance(schema, dict):
            return

        if '$id' in schema:
            schema_id = schema['$id']
            if base_uri and not schema_id.startswith(('http://', 'https://', 'urn:')):
                from urllib.parse import urljoin
                schema_id = urljoin(base_uri, schema_id)
            self.schema_registry[schema_id] = schema
            base_uri = schema_id

        if 'definitions' in schema:
            for def_name, def_schema in schema['definitions'].items():
                if isinstance(def_schema, dict):
                    self.register_schema_ids(def_schema, base_uri)

        if 'properties' in schema:
            for prop_name, prop_schema in schema['properties'].items():
                if isinstance(prop_schema, dict):
                    self.register_schema_ids(prop_schema, base_uri)

        for key in ['items', 'values', 'additionalProperties']:
            if key in schema and isinstance(schema[key], dict):
                self.register_schema_ids(schema[key], base_uri)

    def convert_structure_type_to_python(self, class_name: str, field_name: str, 
                                        structure_type: JsonNode, parent_namespace: str, 
                                        import_types: Set[str]) -> str:
        """ Converts JSON Structure type to Python type """
        if isinstance(structure_type, str):
            python_type = self.map_primitive_to_python(structure_type)
            if python_type.startswith('datetime.') or python_type == 'decimal.Decimal' or python_type == 'uuid.UUID':
                import_types.add(python_type)
            return python_type
        elif isinstance(structure_type, list):
            # Handle type unions
            non_null_types = [t for t in structure_type if t != 'null']
            if len(non_null_types) == 1:
                inner_type = self.convert_structure_type_to_python(
                    class_name, field_name, non_null_types[0], parent_namespace, import_types)
                if 'null' in structure_type:
                    return f'typing.Optional[{inner_type}]'
                return inner_type
            else:
                union_types = [self.convert_structure_type_to_python(
                    class_name, field_name, t, parent_namespace, import_types) for t in non_null_types]
                return f"typing.Union[{', '.join(union_types)}]"
        elif isinstance(structure_type, dict):
            # Handle $ref
            if '$ref' in structure_type:
                ref_schema = self.resolve_ref(structure_type['$ref'], self.schema_doc)
                if ref_schema:
                    ref_path = structure_type['$ref'].split('/')
                    type_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    ref = self.generate_class_or_choice(ref_schema, ref_namespace, write_file=True, explicit_name=type_name)
                    import_types.add(ref)
                    return self.strip_package_from_fully_qualified_name(ref)
                return 'typing.Any'

            # Handle enum keyword
            if 'enum' in structure_type:
                enum_ref = self.generate_enum(structure_type, field_name, parent_namespace, write_file=True)
                import_types.add(enum_ref)
                return self.strip_package_from_fully_qualified_name(enum_ref)

            # Handle type keyword
            if 'type' not in structure_type:
                return 'typing.Any'

            struct_type = structure_type['type']

            # Handle complex types
            if struct_type == 'object':
                class_ref = self.generate_class(structure_type, parent_namespace, write_file=True)
                import_types.add(class_ref)
                return self.strip_package_from_fully_qualified_name(class_ref)
            elif struct_type == 'array':
                items_type = self.convert_structure_type_to_python(
                    class_name, field_name+'List', structure_type.get('items', {'type': 'any'}), 
                    parent_namespace, import_types)
                return f"typing.List[{items_type}]"
            elif struct_type == 'set':
                items_type = self.convert_structure_type_to_python(
                    class_name, field_name+'Set', structure_type.get('items', {'type': 'any'}), 
                    parent_namespace, import_types)
                return f"typing.Set[{items_type}]"
            elif struct_type == 'map':
                values_type = self.convert_structure_type_to_python(
                    class_name, field_name+'Map', structure_type.get('values', {'type': 'any'}), 
                    parent_namespace, import_types)
                return f"typing.Dict[str, {values_type}]"
            elif struct_type == 'choice':
                # Generate choice returns a Union type and populates import_types with the choice types
                return self.generate_choice(structure_type, parent_namespace, write_file=True, import_types=import_types)
            elif struct_type == 'tuple':
                tuple_ref = self.generate_tuple(structure_type, parent_namespace, write_file=True)
                import_types.add(tuple_ref)
                return self.strip_package_from_fully_qualified_name(tuple_ref)
            else:
                return self.convert_structure_type_to_python(class_name, field_name, struct_type, parent_namespace, import_types)
        return 'typing.Any'

    def generate_class_or_choice(self, structure_schema: Dict, parent_namespace: str, 
                                 write_file: bool = True, explicit_name: str = '') -> str:
        """ Generates a Class or Choice """
        struct_type = structure_schema.get('type', 'object')
        if struct_type == 'object':
            return self.generate_class(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'choice':
            return self.generate_choice(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        elif struct_type == 'tuple':
            return self.generate_tuple(structure_schema, parent_namespace, write_file, explicit_name=explicit_name)
        return 'typing.Any'

    def generate_class(self, structure_schema: Dict, parent_namespace: str, 
                      write_file: bool, explicit_name: str = '') -> str:
        """ Generates a Python dataclass from JSON Structure object type """
        import_types: Set[str] = set()

        # Get name and namespace
        class_name = pascal(explicit_name if explicit_name else structure_schema.get('name', 'UnnamedClass'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_package, schema_namespace).lower()
        package_name = self.python_package_from_structure_type(schema_namespace, class_name)
        python_qualified_name = self.python_fully_qualified_name_from_structure_type(schema_namespace, class_name)
        
        if python_qualified_name in self.generated_types:
            return python_qualified_name

        # Check if this is an abstract type
        is_abstract = structure_schema.get('abstract', False)

        # Handle inheritance ($extends)
        base_class = None
        if '$extends' in structure_schema:
            base_ref = structure_schema['$extends']
            if isinstance(self.schema_doc, dict):
                base_schema = self.resolve_ref(base_ref, self.schema_doc)
                if base_schema:
                    ref_path = base_ref.split('/')
                    base_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    base_class = self.generate_class(base_schema, ref_namespace, write_file=True, explicit_name=base_name)
                    import_types.add(base_class)

        # Generate properties
        properties = structure_schema.get('properties', {})
        required_props = structure_schema.get('required', [])

        fields = []
        for prop_name, prop_schema in properties.items():
            field_def = self.generate_field(prop_name, prop_schema, class_name, schema_namespace, 
                                           required_props, import_types)
            fields.append(field_def)

        # Get docstring
        doc = structure_schema.get('description', structure_schema.get('doc', class_name))

        # Generate field docstrings
        field_docstrings = [{
            'name': self.safe_name(field['name']),
            'original_name': field.get('json_name') or field['name'],
            'type': field['type'],
            'is_primitive': field['is_primitive'],
            'is_enum': field['is_enum'],
            'docstring': self.generate_field_docstring(field, schema_namespace),
            'test_value': self.generate_test_value(field),
            'source_type': field.get('source_type', 'string'),
        } for field in fields]

        # If avro_annotation is enabled, convert JSON Structure schema to Avro schema
        # This is embedded in the generated class for runtime Avro serialization
        avro_schema_json = ''
        if self.avro_annotation:
            # Use JsonStructureToAvro to convert the schema
            converter = JsonStructureToAvro()
            schema_copy = structure_schema.copy()
            avro_schema = converter.convert(schema_copy)
            avro_schema_json = json.dumps(avro_schema).replace('\\"', '\'').replace('"', '\\"')

        # Process template
        class_definition = process_template(
            "structuretopython/dataclass_core.jinja",
            class_name=class_name,
            docstring=doc,
            fields=field_docstrings,
            import_types=import_types,
            base_package=self.base_package,
            dataclasses_json_annotation=self.dataclasses_json_annotation,
            avro_annotation=self.avro_annotation,
            avro_schema_json=avro_schema_json,
            is_abstract=is_abstract,
            base_class=base_class,
        )

        if write_file:
            self.write_to_file(package_name, class_name, class_definition)
            self.generate_test_class(package_name, class_name, field_docstrings, import_types)

        self.generated_types[python_qualified_name] = 'class'
        self.generated_structure_types[python_qualified_name] = structure_schema
        return python_qualified_name

    def generate_field(self, prop_name: str, prop_schema: Dict, class_name: str, 
                      parent_namespace: str, required_props: List, import_types: Set[str]) -> Dict:
        """ Generates a field for a Python dataclass """
        # Sanitize field name for Python identifier validity
        field_name = self.safe_identifier(prop_name, class_name)
        # Track if we need a field_name annotation for JSON serialization
        needs_field_name_annotation = field_name != prop_name

        # Check if this is a const field
        if 'const' in prop_schema:
            # Const fields are treated as class variables with default values
            prop_type = self.convert_structure_type_to_python(
                class_name, field_name, prop_schema, parent_namespace, import_types)
            return {
                'name': field_name,
                'json_name': prop_name if needs_field_name_annotation else None,
                'type': prop_type,
                'is_primitive': self.is_python_primitive(prop_type) or self.is_python_typing_struct(prop_type),
                'is_enum': False,
                'is_const': True,
                'const_value': prop_schema['const'],
                'source_type': prop_schema.get('type', 'string')
            }

        # Determine if required
        is_required = prop_name in required_props if not isinstance(required_props, list) or \
                     len(required_props) == 0 or not isinstance(required_props[0], list) else \
                     any(prop_name in req_set for req_set in required_props)

        # Get property type
        prop_type = self.convert_structure_type_to_python(
            class_name, field_name, prop_schema, parent_namespace, import_types)

        # Add Optional if not required
        if not is_required and not prop_type.startswith('typing.Optional['):
            prop_type = f'typing.Optional[{prop_type}]'

        # Get source type from structure schema
        source_type = prop_schema.get('type', 'string') if isinstance(prop_schema.get('type'), str) else 'object'

        return {
            'name': field_name,
            'json_name': prop_name if needs_field_name_annotation else None,
            'type': prop_type,
            'is_primitive': self.is_python_primitive(prop_type) or self.is_python_typing_struct(prop_type),
            'is_enum': prop_type in self.generated_types and self.generated_types[prop_type] == 'enum',
            'is_const': False,
            'source_type': source_type
        }

    def generate_field_docstring(self, field: Dict, parent_namespace: str) -> str:
        """Generates a field docstring for a Python dataclass"""
        field_type = field['type']
        field_name = self.safe_name(field['name'])
        field_docstring = f"{field_name} ({field_type})"
        return field_docstring

    def generate_enum(self, structure_schema: Dict, field_name: str, parent_namespace: str, 
                     write_file: bool) -> str:
        """ Generates a Python enum from JSON Structure enum """
        # Generate enum name from field name if not provided
        class_name = pascal(structure_schema.get('name', field_name + 'Enum'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_package, schema_namespace).lower()
        package_name = self.python_package_from_structure_type(schema_namespace, class_name)
        python_qualified_name = self.python_fully_qualified_name_from_structure_type(schema_namespace, class_name)
        
        if python_qualified_name in self.generated_types:
            return python_qualified_name

        symbols = [symbol if not is_python_reserved_word(symbol) else symbol + "_" 
                  for symbol in structure_schema.get('enum', [])]

        doc = structure_schema.get('description', structure_schema.get('doc', f'A {class_name} enum.'))

        enum_definition = process_template(
            "structuretopython/enum_core.jinja",
            class_name=class_name,
            docstring=doc,
            symbols=symbols,
        )

        if write_file:
            self.write_to_file(package_name, class_name, enum_definition)
            self.generate_test_enum(package_name, class_name, symbols)

        self.generated_types[python_qualified_name] = 'enum'
        self.generated_enum_symbols[python_qualified_name] = symbols
        return python_qualified_name

    def generate_choice(self, structure_schema: Dict, parent_namespace: str, 
                       write_file: bool, explicit_name: str = '', import_types: Optional[Set[str]] = None) -> str:
        """ Generates a Python Union type from JSON Structure choice """
        choice_name = explicit_name if explicit_name else structure_schema.get('name', 'UnnamedChoice')
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        if import_types is None:
            import_types = set()
        
        # If the choice extends a base class, generate the base and derived classes first
        if '$extends' in structure_schema:
            base_ref = structure_schema['$extends']
            if isinstance(self.schema_doc, dict):
                base_schema = self.resolve_ref(base_ref, self.schema_doc)
                if base_schema:
                    # Generate the base class
                    ref_path = base_ref.split('/')
                    base_name = ref_path[-1]
                    ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                    self.generate_class(base_schema, ref_namespace, write_file=True, explicit_name=base_name)
        
        # Generate types for each choice
        choice_types = []
        choices = structure_schema.get('choices', {})
        
        for choice_key, choice_schema in choices.items():
            if isinstance(choice_schema, dict):
                if '$ref' in choice_schema:
                    # Resolve reference and generate the type
                    ref_schema = self.resolve_ref(choice_schema['$ref'], self.schema_doc if isinstance(self.schema_doc, dict) else None)
                    if ref_schema:
                        ref_path = choice_schema['$ref'].split('/')
                        ref_name = ref_path[-1]
                        ref_namespace = '.'.join(ref_path[2:-1]) if len(ref_path) > 3 else parent_namespace
                        qualified_name = self.generate_class(ref_schema, ref_namespace, write_file=True, explicit_name=ref_name)
                        import_types.add(qualified_name)
                        choice_types.append(qualified_name.split('.')[-1])
                elif 'type' in choice_schema:
                    # Generate inline type
                    python_type = self.convert_structure_type_to_python(choice_name, choice_key, choice_schema, schema_namespace, import_types)
                    choice_types.append(python_type)
        
        # Return Union type
        if len(choice_types) == 0:
            return 'typing.Any'
        elif len(choice_types) == 1:
            return choice_types[0]
        else:
            return f"typing.Union[{', '.join(choice_types)}]"

    def generate_tuple(self, structure_schema: Dict, parent_namespace: str, 
                      write_file: bool, explicit_name: str = '') -> str:
        """ Generates a Python Tuple type from JSON Structure tuple """
        # For now, return typing.Any as tuples need special handling
        return 'typing.Any'

    def generate_map_alias(self, structure_schema: Dict, parent_namespace: str,
                          write_file: bool) -> str:
        """ Generates a Python TypeAlias for a top-level map type """
        import_types: Set[str] = set()
        
        # Get name and namespace
        class_name = pascal(structure_schema.get('name', 'UnnamedMap'))
        schema_namespace = structure_schema.get('namespace', parent_namespace)
        namespace = self.concat_namespace(self.base_package, schema_namespace).lower()
        package_name = self.python_package_from_structure_type(schema_namespace, class_name)
        python_qualified_name = self.python_fully_qualified_name_from_structure_type(schema_namespace, class_name)
        
        if python_qualified_name in self.generated_types:
            return python_qualified_name
        
        # Get the value type
        values_schema = structure_schema.get('values', {'type': 'any'})
        values_type = self.convert_structure_type_to_python(
            class_name, 'Values', values_schema, schema_namespace, import_types)
        
        # Get docstring
        doc = structure_schema.get('description', structure_schema.get('doc', f'A {class_name} map type.'))
        
        # Generate the type alias module
        map_definition = process_template(
            "structuretopython/map_alias.jinja",
            class_name=class_name,
            docstring=doc,
            values_type=values_type,
            import_types=import_types,
            base_package=self.base_package
        )
        
        if write_file:
            self.write_to_file(package_name, class_name, map_definition)
        
        self.generated_types[python_qualified_name] = 'map'
        return python_qualified_name

    def generate_test_value(self, field: Dict) -> Any:
        """Generates a test value for a given field"""
        field_type = field['type']

        def generate_value(field_type: str):
            test_values = {
                'str': chr(39) + ''.join([chr(random.randint(97, 122)) for _ in range(0, 20)]) + chr(39),
                'bool': str(random.choice([True, False])),
                'int': f'int({random.randint(0, 100)})',
                'float': f'float({random.uniform(0, 100)})',
                'bytes': 'b"test_bytes"',
                'None': 'None',
                'datetime.date': 'datetime.date.today()',
                'datetime.datetime': 'datetime.datetime.now(datetime.timezone.utc)',
                'datetime.time': 'datetime.datetime.now(datetime.timezone.utc).time()',
                'decimal.Decimal': f'decimal.Decimal("{random.randint(0, 100)}.{random.randint(0, 100)}")',
                'datetime.timedelta': 'datetime.timedelta(days=1)',
                'uuid.UUID': 'uuid.uuid4()',
                'typing.Any': '{"test": "test"}'
            }

            def resolve(field_type: str) -> str:
                pattern = re.compile(r'^(?:typing\.)*(Optional|List|Dict|Union|Set)\[(.+)\]$')
                match = pattern.match(field_type)
                if not match:
                    return field_type
                outer_type, inner_type = match.groups()
                if outer_type == 'Optional':
                    return inner_type
                elif outer_type in ['List', 'Set']:
                    return resolve(inner_type)
                elif outer_type == 'Dict':
                    _, value_type = inner_type.split(',', 1)
                    return resolve(value_type.strip())
                elif outer_type == 'Union':
                    first_type = inner_type.split(',', 1)[0]
                    return resolve(first_type.strip())
                return field_type

            if field_type.startswith('typing.Optional['):
                field_type = resolve(field_type)

            if field_type.startswith('typing.List[') or field_type.startswith('typing.Set['):
                field_type = resolve(field_type)
                array_range = random.randint(1, 5)
                return f"[{', '.join([generate_value(field_type) for _ in range(array_range)])}]"
            elif field_type.startswith('typing.Dict['):
                field_type = resolve(field_type)
                dict_range = random.randint(1, 5)
                dict_data = {}
                for _ in range(dict_range):
                    dict_data[''.join([chr(random.randint(97, 122)) for _ in range(0, 20)])] = generate_value(field_type)
                return f"{{{', '.join([chr(39)+key+chr(39)+f': {value}' for key, value in dict_data.items()])}}}"
            elif field_type.startswith('typing.Union['):
                field_type = resolve(field_type)
                return generate_value(field_type)
            if field_type in test_values:
                return test_values[field_type]
            # Check if this is an enum type - use first symbol value
            # Look up by fully qualified name or by short name (class name only)
            enum_symbols = None
            if field_type in self.generated_enum_symbols:
                enum_symbols = self.generated_enum_symbols[field_type]
            else:
                # Try to find by short name (the field type might be just the class name)
                for qualified_name, symbols in self.generated_enum_symbols.items():
                    if qualified_name.endswith('.' + field_type) or qualified_name == field_type:
                        enum_symbols = symbols
                        break
            if enum_symbols:
                return f"{field_type.split('.')[-1]}.{enum_symbols[0]}"
            # For complex types, use None since fields are typically optional
            # This avoids needing to construct nested objects with required args
            return 'None'

        return generate_value(field_type)

    def generate_test_class(self, package_name: str, class_name: str, fields: List[Dict[str, str]], 
                           import_types: Set[str]) -> None:
        """Generates a unit test class for a Python dataclass"""
        test_class_name = f"Test_{class_name}"
        # Use a simpler file naming scheme based on class name only
        test_file_name = f"test_{class_name.lower()}"
        test_class_definition = process_template(
            "structuretopython/test_class.jinja",
            package_name=package_name,
            class_name=class_name,
            test_class_name=test_class_name,
            fields=fields,
            import_types=import_types,
            avro_annotation=self.avro_annotation,
            dataclasses_json_annotation=self.dataclasses_json_annotation
        )

        base_dir = os.path.join(self.output_dir, "tests")
        test_file_path = os.path.join(base_dir, f"{test_file_name}.py")
        if not os.path.exists(os.path.dirname(test_file_path)):
            os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
        with open(test_file_path, 'w', encoding='utf-8') as file:
            file.write(test_class_definition)

    def generate_test_enum(self, package_name: str, class_name: str, symbols: List[str]) -> None:
        """Generates a unit test class for a Python enum"""
        test_class_name = f"Test_{class_name}"
        # Use a simpler file naming scheme based on class name only
        test_file_name = f"test_{class_name.lower()}"
        test_class_definition = process_template(
            "structuretopython/test_enum.jinja",
            package_name=package_name,
            class_name=class_name,
            test_class_name=test_class_name,
            symbols=symbols
        )
        base_dir = os.path.join(self.output_dir, "tests")
        test_file_path = os.path.join(base_dir, f"{test_file_name}.py")
        if not os.path.exists(os.path.dirname(test_file_path)):
            os.makedirs(os.path.dirname(test_file_path), exist_ok=True)
        with open(test_file_path, 'w', encoding='utf-8') as file:
            file.write(test_class_definition)

    def write_to_file(self, package: str, class_name: str, python_code: str):
        """Writes a Python class to a file"""
        # The containing directory is the parent package (matches avrotopython.py)
        parent_package_name = '.'.join(package.split('.')[:-1])
        parent_package_path = os.sep.join(parent_package_name.split('.')).lower()
        directory_path = os.path.join(self.output_dir, "src", parent_package_path)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path, exist_ok=True)
        file_path = os.path.join(directory_path, f"{class_name.lower()}.py")

        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(python_code)

    def write_init_files(self):
        """Writes __init__.py files to the output directories"""
        def organize_generated_types():
            generated_types_tree = {}
            for generated_type, _ in self.generated_types.items():
                parts = generated_type.split('.')
                if len(parts) < 2:
                    continue
                class_name = parts[-1]
                module_name = parts[-2]
                package_parts = parts[:-2]
                current_node = generated_types_tree
                for part in package_parts:
                    if part not in current_node:
                        current_node[part] = {}
                    current_node = current_node[part]
                current_node[module_name] = class_name
            return generated_types_tree

        def collect_class_names(node):
            class_names = []
            for key, value in node.items():
                if isinstance(value, dict):
                    class_names.extend(collect_class_names(value))
                else:
                    class_names.append(value)
            return class_names

        def write_init_files_recursive(generated_types_tree, current_package: str):
            import_statements = []
            all_statement = []
            for package_or_module_name, content in generated_types_tree.items():
                if isinstance(content, dict):
                    class_names = collect_class_names(content)
                    if class_names:
                        import_statements.append(f"from .{package_or_module_name} import {', '.join(class_names)}")
                        all_statement.extend([f'"{name}"' for name in class_names])
                    write_init_files_recursive(content, current_package + ('.' if current_package else '') + package_or_module_name)
                else:
                    class_name = content
                    import_statements.append(f"from .{package_or_module_name} import {class_name}")
                    all_statement.append(f'"{class_name}"')
            if current_package and (import_statements or all_statement):
                package_path = os.path.join(self.output_dir, 'src', current_package.replace('.', os.sep).lower())
                init_file_path = os.path.join(package_path, '__init__.py')
                if not os.path.exists(package_path):
                    os.makedirs(package_path, exist_ok=True)
                with open(init_file_path, 'w', encoding='utf-8') as file:
                    file.write('\n'.join(import_statements) + '\n\n__all__ = [' + ', '.join(all_statement) + ']\n')

        write_init_files_recursive(organize_generated_types(), '')

    def write_pyproject_toml(self):
        """Writes pyproject.toml file to the output directory"""
        pyproject_content = process_template(
            "structuretopython/pyproject_toml.jinja",
            package_name=self.base_package.replace('_', '-'),
            dataclasses_json_annotation=self.dataclasses_json_annotation,
            avro_annotation=self.avro_annotation
        )
        with open(os.path.join(self.output_dir, 'pyproject.toml'), 'w', encoding='utf-8') as file:
            file.write(pyproject_content)

    def convert_schemas(self, structure_schemas: List, output_dir: str):
        """ Converts JSON Structure schemas to Python dataclasses"""
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

        # Register all schema IDs first
        for structure_schema in structure_schemas:
            self.register_schema_ids(structure_schema)

        for structure_schema in structure_schemas:
            self.schema_doc = structure_schema
            if 'definitions' in structure_schema:
                self.definitions = structure_schema['definitions']

            if 'enum' in structure_schema:
                self.generate_enum(structure_schema, structure_schema.get('name', 'Enum'), 
                                 structure_schema.get('namespace', ''), write_file=True)
            elif structure_schema.get('type') == 'object':
                self.generate_class(structure_schema, structure_schema.get('namespace', ''), write_file=True)
            elif structure_schema.get('type') == 'choice':
                self.generate_choice(structure_schema, structure_schema.get('namespace', ''), write_file=True)
            elif structure_schema.get('type') == 'map':
                self.generate_map_alias(structure_schema, structure_schema.get('namespace', ''), write_file=True)

        self.write_init_files()
        self.write_pyproject_toml()

    def convert(self, structure_schema_path: str, output_dir: str):
        """Converts JSON Structure schema to Python dataclasses"""
        with open(structure_schema_path, 'r', encoding='utf-8') as file:
            schema = json.load(file)
        if isinstance(schema, dict):
            schema = [schema]
        return self.convert_schemas(schema, output_dir)


def convert_structure_to_python(structure_schema_path, py_file_path, package_name='', dataclasses_json_annotation=False, avro_annotation=False):
    """Converts JSON Structure schema to Python dataclasses"""
    if not package_name:
        # Strip .json extension, then also strip .struct suffix if present (*.struct.json naming convention)
        base_name = os.path.splitext(os.path.basename(structure_schema_path))[0]
        if base_name.endswith('.struct'):
            base_name = base_name[:-7]  # Remove '.struct' suffix
        package_name = base_name.lower().replace('-', '_')
    package_name = safe_package_name(package_name)

    structure_to_python = StructureToPython(package_name, dataclasses_json_annotation=dataclasses_json_annotation, avro_annotation=avro_annotation)
    structure_to_python.convert(structure_schema_path, py_file_path)


def convert_structure_schema_to_python(structure_schema, py_file_path, package_name='', dataclasses_json_annotation=False):
    """Converts JSON Structure schema to Python dataclasses"""
    package_name = safe_package_name(package_name) if package_name else package_name
    structure_to_python = StructureToPython(package_name, dataclasses_json_annotation=dataclasses_json_annotation)
    if isinstance(structure_schema, dict):
        structure_schema = [structure_schema]
    structure_to_python.convert_schemas(structure_schema, py_file_path)
