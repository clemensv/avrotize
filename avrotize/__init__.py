# __init__.py

from .jsonstoavro import convert_jsons_to_avro
from .kustotoavro import convert_kusto_to_avro
from .kstructtoavro import convert_kafka_struct_to_avro_schema
from .asn1toavro import convert_asn1_to_avro
from .xsdtoavro import convert_xsd_to_avro
from .prototoavro import  convert_proto_to_avro
from .avrotojsons import convert_avro_to_json_schema
from .avrotokusto import convert_avro_to_kusto_file, convert_avro_to_kusto_db
from .avrotoparquet import convert_avro_to_parquet
from .avrotoproto import convert_avro_to_proto
from .avrotodb import convert_avro_to_sql
from .avrotoxsd import convert_avro_to_xsd
from .avrotojava import convert_avro_to_java, convert_avro_schema_to_java
from .avrotocsharp import convert_avro_to_csharp, convert_avro_schema_to_csharp
from .avrotopython import convert_avro_to_python, convert_avro_schema_to_python
from .avrotots import convert_avro_to_typescript, convert_avro_schema_to_typescript
from .avrotojs import convert_avro_to_javascript, convert_avro_schema_to_javascript
from .avrotomd import convert_avro_to_markdown
from .avrotocpp import convert_avro_to_cpp, convert_avro_schema_to_cpp
from .avrotogo import convert_avro_to_go, convert_avro_schema_to_go
from .avrotorust import convert_avro_to_rust, convert_avro_schema_to_rust
from .avrotodatapackage import convert_avro_to_datapackage

__all__ = [
    "convert_proto_to_avro",
    "convert_jsons_to_avro",
    "convert_kafka_struct_to_avro_schema",
    "convert_kusto_to_avro",
    "convert_asn1_to_avro",
    "convert_xsd_to_avro",
    "convert_proto_to_avro",
    "convert_avro_to_json_schema",
    "convert_avro_to_kusto_file",
    "convert_avro_to_kusto_db",
    "convert_avro_to_parquet",
    "convert_avro_to_proto",
    "convert_avro_to_sql",
    "convert_avro_to_xsd",
    "convert_avro_to_java",
    "convert_avro_schema_to_java",
    "convert_avro_to_csharp",
    "convert_avro_schema_to_csharp",
    "convert_avro_to_python",
    "convert_avro_schema_to_python",
    "convert_avro_to_typescript",
    "convert_avro_schema_to_typescript",
    "convert_avro_to_javascript",
    "convert_avro_schema_to_javascript",
    "convert_avro_to_markdown",
    "convert_avro_to_cpp",
    "convert_avro_schema_to_cpp",
    "convert_avro_to_go",
    "convert_avro_schema_to_go",
    "convert_avro_to_rust",
    "convert_avro_schema_to_rust",
    "convert_avro_to_datapackage"
]