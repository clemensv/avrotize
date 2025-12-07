"""Constants for the avrotize package.

These constants are now derived from central dependency files in avrotize/dependencies/
which are monitored by Dependabot. This ensures versions stay up-to-date.
"""

from avrotize.dependency_version import get_dependency_version

# Java/JVM dependencies
try:
    AVRO_VERSION = get_dependency_version('java', 'jdk21', 'org.apache.avro:avro')
except ValueError:
    AVRO_VERSION = '1.12.0'  # Fallback

try:
    JACKSON_VERSION = get_dependency_version('java', 'jdk21', 'com.fasterxml.jackson.core:jackson-core')
except ValueError:
    JACKSON_VERSION = '2.18.2'  # Fallback

JDK_VERSION = '21'

# C# dependencies  
try:
    CSHARP_AVRO_VERSION = get_dependency_version('cs', 'net90', 'Apache.Avro')
except ValueError:
    CSHARP_AVRO_VERSION = '1.12.0'

try:
    NEWTONSOFT_JSON_VERSION = get_dependency_version('cs', 'net90', 'Newtonsoft.Json')
except ValueError:
    NEWTONSOFT_JSON_VERSION = '13.0.3'

try:
    SYSTEM_TEXT_JSON_VERSION = get_dependency_version('cs', 'net90', 'System.Text.Json')
except ValueError:
    SYSTEM_TEXT_JSON_VERSION = '9.0.3'

try:
    SYSTEM_MEMORY_DATA_VERSION = get_dependency_version('cs', 'net90', 'System.Memory.Data')
except ValueError:
    SYSTEM_MEMORY_DATA_VERSION = '9.0.3'

try:
    PROTOBUF_NET_VERSION = get_dependency_version('cs', 'net90', 'protobuf-net')
except ValueError:
    PROTOBUF_NET_VERSION = '3.2.30'

try:
    NUNIT_VERSION = get_dependency_version('cs', 'net90', 'NUnit')
except ValueError:
    NUNIT_VERSION = '4.3.2'

try:
    NUNIT_ADAPTER_VERSION = get_dependency_version('cs', 'net90', 'NUnit3TestAdapter')
except ValueError:
    NUNIT_ADAPTER_VERSION = '5.0.0'

try:
    MSTEST_SDK_VERSION = get_dependency_version('cs', 'net90', 'Microsoft.NET.Test.Sdk')
except ValueError:
    MSTEST_SDK_VERSION = '17.13.0'

# Java test dependencies
try:
    JUNIT_VERSION = get_dependency_version('java', 'jdk21', 'org.junit.jupiter:junit-jupiter-api')
except ValueError:
    JUNIT_VERSION = '5.11.4'

try:
    MAVEN_SUREFIRE_VERSION = get_dependency_version('java', 'jdk21', 'org.apache.maven.plugins:maven-surefire-plugin')
except ValueError:
    MAVEN_SUREFIRE_VERSION = '3.5.2'

try:
    MAVEN_COMPILER_VERSION = get_dependency_version('java', 'jdk21', 'org.apache.maven.plugins:maven-compiler-plugin')
except ValueError:
    MAVEN_COMPILER_VERSION = '3.13.0'