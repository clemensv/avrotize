# __init__.py

from .jsonstoavro import convert_jsons_to_avro
from .kstructtoavro import convert_kafka_struct_to_avro_schema
from .asn1toavro import convert_asn1_to_avro
from .xsdtoavro import convert_xsd_to_avro
from .prototoavro import  convert_proto_to_avro
from .avrotojsons import convert_avro_to_json_schema
from .avrotokusto import convert_avro_to_kusto
from .avrotoparquet import convert_avro_to_parquet
from .avrotoproto import convert_avro_to_proto
from .avrototsql import convert_avro_to_tsql
from .avrotoxsd import convert_avro_to_xsd
from .avrotojava import convert_avro_to_java, convert_avro_schema_to_java
from .avrotocsharp import convert_avro_to_csharp, convert_avro_schema_to_csharp
from .avrotopython import convert_avro_to_python, convert_avro_schema_to_python
from .avrotots import convert_avro_to_typescript, convert_avro_schema_to_typescript
from .avrotojs import convert_avro_to_javascript, convert_avro_schema_to_javascript

__all__ = [
    "convert_proto_to_avro",
    "convert_jsons_to_avro",
    "convert_kafka_struct_to_avro_schema",
    "convert_asn1_to_avro",
    "convert_xsd_to_avro",
    "convert_proto_to_avro",
    "convert_avro_to_json_schema",
    "convert_avro_to_kusto",
    "convert_avro_to_parquet",
    "convert_avro_to_proto",
    "convert_avro_to_tsql",
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
    "convert_avro_schema_to_javascript"
]