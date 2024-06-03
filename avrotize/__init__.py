import importlib

mod = "avrotize"
class LazyLoader:
    """    
    Lazy loader for the avrotize functions to speed up startup time.    
    """
    def __init__(self, mappings):
        self._modules = {}
        self._mappings = mappings

    def _load_module(self, module_name):
        if module_name not in self._modules:
            self._modules[module_name] = importlib.import_module(module_name)
        return self._modules[module_name]

    def __getattr__(self, item):
        if item in self._mappings:
            module_name, func_name = self._mappings[item]
            module = self._load_module(module_name)
            return getattr(module, func_name)
        else:
            return self._load_module(f"{mod}.{item}")

# Define the functions and their corresponding module paths
_mappings = {
    "convert_proto_to_avro": (f"{mod}.prototoavro", "convert_proto_to_avro"),
    "convert_jsons_to_avro": (f"{mod}.jsonstoavro", "convert_jsons_to_avro"),
    "convert_kafka_struct_to_avro_schema": (f"{mod}.kstructtoavro", "convert_kafka_struct_to_avro_schema"),
    "convert_kusto_to_avro": (f"{mod}.kustotoavro", "convert_kusto_to_avro"),
    "convert_asn1_to_avro": (f"{mod}.asn1toavro", "convert_asn1_to_avro"),
    "convert_xsd_to_avro": (f"{mod}.xsdtoavro", "convert_xsd_to_avro"),
    "convert_avro_to_json_schema": (f"{mod}.avrotojsons", "convert_avro_to_json_schema"),
    "convert_avro_to_kusto_file": (f"{mod}.avrotokusto", "convert_avro_to_kusto_file"),
    "convert_avro_to_kusto_db": (f"{mod}.avrotokusto", "convert_avro_to_kusto_db"),
    "convert_avro_to_parquet": (f"{mod}.avrotoparquet", "convert_avro_to_parquet"),
    "convert_avro_to_proto": (f"{mod}.avrotoproto", "convert_avro_to_proto"),
    "convert_avro_to_sql": (f"{mod}.avrotodb", "convert_avro_to_sql"),
    "convert_avro_to_xsd": (f"{mod}.avrotoxsd", "convert_avro_to_xsd"),
    "convert_avro_to_java": (f"{mod}.avrotojava", "convert_avro_to_java"),
    "convert_avro_schema_to_java": (f"{mod}.avrotojava", "convert_avro_schema_to_java"),
    "convert_avro_to_csharp": (f"{mod}.avrotocsharp", "convert_avro_to_csharp"),
    "convert_avro_schema_to_csharp": (f"{mod}.avrotocsharp", "convert_avro_schema_to_csharp"),
    "convert_avro_to_python": (f"{mod}.avrotopython", "convert_avro_to_python"),
    "convert_avro_schema_to_python": (f"{mod}.avrotopython", "convert_avro_schema_to_python"),
    "convert_avro_to_typescript": (f"{mod}.avrotots", "convert_avro_to_typescript"),
    "convert_avro_schema_to_typescript": (f"{mod}.avrotots", "convert_avro_schema_to_typescript"),
    "convert_avro_to_javascript": (f"{mod}.avrotojs", "convert_avro_to_javascript"),
    "convert_avro_schema_to_javascript": (f"{mod}.avrotojs", "convert_avro_schema_to_javascript"),
    "convert_avro_to_markdown": (f"{mod}.avrotomd", "convert_avro_to_markdown"),
    "convert_avro_to_cpp": (f"{mod}.avrotocpp", "convert_avro_to_cpp"),
    "convert_avro_schema_to_cpp": (f"{mod}.avrotocpp", "convert_avro_schema_to_cpp"),
    "convert_avro_to_go": (f"{mod}.avrotogo", "convert_avro_to_go"),
    "convert_avro_schema_to_go": (f"{mod}.avrotogo", "convert_avro_schema_to_go"),
    "convert_avro_to_rust": (f"{mod}.avrotorust", "convert_avro_to_rust"),
    "convert_avro_schema_to_rust": (f"{mod}.avrotorust", "convert_avro_schema_to_rust"),
    "convert_avro_to_datapackage": (f"{mod}.avrotodatapackage", "convert_avro_to_datapackage"),
}

_lazy_loader = LazyLoader(_mappings)

def __getattr__(name):
    return getattr(_lazy_loader, name)
