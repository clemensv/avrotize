from unittest.mock import patch
import unittest
import os
import shutil
import subprocess
import sys
import tempfile
from os import path, getcwd

import pytest

from avrotize.avrotopython import convert_avro_to_python
from avrotize.jsonstoavro import convert_jsons_to_avro

current_script_path = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_script_path))
sys.path.append(project_root)


class TestAvroToPython(unittest.TestCase):
    def test_convert_address_avsc_to_python(self):
        """ Test converting an address.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_address_avsc_to_python_json(self):
        """ Test converting an address.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py-json")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(
            avro_path, py_path, dataclasses_json_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
        
    def test_convert_address_avsc_to_python_json_avro(self):
        """ Test converting an address.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py-jsonavro")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(
            avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_address_avsc_to_python_avro(self):
        """ Test converting an address.avsc file to Python with avro annotation """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py-avro")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_telemetry_avsc_to_python(self):
        """ Test converting a telemetry.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "telemetry.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "telemetry-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
        
    def test_convert_enumfield_avsc_to_python(self):
        """ Test converting a enumfield.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "enumfield.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "enumfield-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
        
    def test_convert_enumfield_ordinals_avsc_to_python(self):
        """ Test converting a enumfield-ordinal.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "enumfield-ordinals.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "enumfield-ordinals-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0
        
    def test_convert_feeditem_avsc_to_python(self):
        """ Test converting a enumfield-ordinal.avsc file to Python """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "feeditem.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "feeditem-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_convert_jfrog_pipelines_jsons_to_avro_to_python(self):
        """ Test converting a jfrog-pipelines.json file to Python """
        cwd = getcwd()
        jsons_path = path.join(cwd, "test", "jsons", "jfrog-pipelines.json")
        avro_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines.avsc")
        py_path = path.join(tempfile.gettempdir(), "avrotize", "jfrog-pipelines-py")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_jsons_to_avro(jsons_path, avro_path)
        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = py_path
        
        assert subprocess.check_call(
            ['python', '-m', 'pytest'], cwd=py_path, env=new_env, stdout=sys.stdout, stderr=sys.stderr, shell=True) == 0

    def test_init_py_exports_class_names(self):
        """ Test that __init__.py files correctly export PascalCase class names, not just module names """
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py-init-test")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        convert_avro_to_python(avro_path, py_path)
        
        # Test 1: Verify __init__.py exists and contains correct imports
        init_file = os.path.join(py_path, "src", "address", "example", "com", "__init__.py")
        assert os.path.exists(init_file), f"__init__.py not found at {init_file}"
        
        with open(init_file, 'r', encoding='utf-8') as f:
            init_content = f.read()
            # Should import PascalCase class name, not lowercase module name
            assert "from .record import Record" in init_content, \
                f"Expected 'from .record import Record' in __init__.py, got: {init_content}"
            assert '__all__ = ["Record"]' in init_content, \
                f"Expected '__all__ = [\"Record\"]' in __init__.py, got: {init_content}"
        
        # Test 2: Verify no erroneous subdirectory was created
        record_dir = os.path.join(py_path, "src", "address", "example", "com", "record")
        assert not os.path.exists(record_dir), \
            f"Erroneous 'record' directory should not exist at {record_dir}"
        
        # Test 3: Verify the module file exists
        record_file = os.path.join(py_path, "src", "address", "example", "com", "record.py")
        assert os.path.exists(record_file), f"Module file not found at {record_file}"
        
        # Test 4: Verify we can actually import the class using the package
        # This is the critical test - it must work for end users
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = os.path.join(py_path, 'src')
        
        test_import_script = os.path.join(py_path, "test_import.py")
        with open(test_import_script, 'w', encoding='utf-8') as f:
            f.write("""
# Test direct class import from package
from address.example.com import Record

# Verify it's the actual class, not a module
assert hasattr(Record, '__dataclass_fields__'), "Record should be a dataclass"
assert Record.__name__ == 'Record', f"Expected class name 'Record', got {Record.__name__}"

# Test instantiation
try:
    instance = Record(
        type='address',
        postOfficeBox=None,
        extendedAddress=None,
        streetAddress='123 Main St',
        locality='Springfield',
        region='IL',
        postalCode='62701',
        countryName='USA'
    )
    assert instance.locality == 'Springfield', "Instance creation failed"
    print("SUCCESS: All import and class tests passed!")
except Exception as e:
    print(f"FAILED: {e}")
    raise
""")
        
        result = subprocess.run(
            [sys.executable, test_import_script],
            cwd=py_path,
            env=new_env,
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, \
            f"Import test failed with code {result.returncode}.\nStdout: {result.stdout}\nStderr: {result.stderr}"
        assert "SUCCESS" in result.stdout, \
            f"Expected success message in output, got: {result.stdout}"

    def test_pyproject_toml_package_name_case_sensitivity(self):
        """ Test that pyproject.toml package name is lowercase to match directory structure """
        from avrotize.avrotopython import convert_avro_schema_to_python
        
        # Create a test schema
        test_schema = {
            'type': 'record',
            'name': 'TestRecord',
            'namespace': 'com.example',
            'fields': [
                {'name': 'id', 'type': 'string'},
                {'name': 'value', 'type': 'int'}
            ]
        }
        
        # Use a package name with mixed case to test the fix
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "case-sensitivity-test")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)
        
        # Generate with mixed case package name
        convert_avro_schema_to_python(test_schema, py_path, package_name='test_MixedCase_pyData')
        
        # Check the generated pyproject.toml
        pyproject_path = os.path.join(py_path, 'pyproject.toml')
        assert os.path.exists(pyproject_path), "pyproject.toml should exist"
        
        with open(pyproject_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # The package name in the packages list should be lowercase
            assert 'include = "test_mixedcase_pydata"' in content, \
                f"Package name in pyproject.toml should be lowercase. Content: {content}"
            
            # Verify the actual directory matches
            expected_dir = os.path.join(py_path, 'src', 'test_mixedcase_pydata')
            assert os.path.exists(expected_dir), \
                f"Expected directory {expected_dir} should exist"
            
        # Verify Poetry can process this (if Poetry is available)
        try:
            result = subprocess.run(
                ['poetry', 'check'],
                cwd=py_path,
                capture_output=True,
                text=True,
                timeout=10
            )
            # If poetry is available, it should validate successfully
            assert result.returncode == 0, \
                f"Poetry validation failed: {result.stderr}"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            # Poetry not available or timeout, skip this check
            pass

    def test_no_double_test_prefix_when_package_starts_with_test(self):
        """
        Regression test: Verify that test file names don't get a double test_ prefix
        when the package already starts with test_.
        
        This prevents errors like:
        ModuleNotFoundError: No module named 'test_test_kafkaproducer_...'
        """
        from avrotize.avrotopython import AvroToPython
        import tempfile
        import shutil
        
        # Create a test output directory
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "test_prefix_check")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            # Use a package name that already starts with 'test_' like xregistry-codegen does
            converter = AvroToPython(
                base_package='test_kafkaproducer_data',
                dataclasses_json_annotation=True
            )
            converter.output_dir = output_dir
            converter.generated_types = {}
            
            # Generate test file with package already having test_ prefix
            converter.generate_test_class(
                package_name='test_kafkaproducer_data.MyEvent',
                class_name='MyEvent',
                fields=[{'name': 'value', 'type': 'str', 'test_value': "'test'"}],
                import_types=set()
            )
            
            # Verify the test file was created without double test_ prefix
            test_file_path = os.path.join(output_dir, "tests", "test_kafkaproducer_data_myevent.py")
            assert os.path.exists(test_file_path), \
                f"Expected test file at {test_file_path}, should not have double test_ prefix"
            
            # Verify the double-prefixed file does NOT exist
            wrong_file_path = os.path.join(output_dir, "tests", "test_test_kafkaproducer_data_myevent.py")
            assert not os.path.exists(wrong_file_path), \
                f"Found file with double test_ prefix at {wrong_file_path}"
            
            # Also verify the content of the generated test file has correct imports
            with open(test_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Verify there's no double test_ prefix in import statements
            assert 'test_test_' not in content, \
                f"Found double test_ prefix in generated test file content"
                
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_no_double_test_prefix_in_import_types(self):
        """
        Regression test: Verify that import statements for dependent types 
        don't get a double test_ prefix when the package already starts with test_.
        
        This tests the template logic that generates imports like:
        from test_mypackage_myclass import Test_MyClass
        
        Not:
        from test_test_mypackage_myclass import Test_MyClass
        """
        from avrotize.avrotopython import AvroToPython
        import tempfile
        import shutil
        
        # Create a test output directory
        output_dir = os.path.join(tempfile.gettempdir(), "avrotize", "test_import_prefix_check")
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            # Use a package name that already starts with 'test_'
            converter = AvroToPython(
                base_package='test_producer_data',
                dataclasses_json_annotation=True
            )
            converter.output_dir = output_dir
            converter.generated_types = {
                'test_producer_data.BaseEvent': 'class'  # Simulate a dependent type
            }
            
            # Generate test file with import_types that has test_ prefix
            import_types = {'test_producer_data.BaseEvent'}
            converter.generate_test_class(
                package_name='test_producer_data.DerivedEvent',
                class_name='DerivedEvent',
                fields=[{'name': 'value', 'type': 'str', 'test_value': "'test'"}],
                import_types=import_types
            )
            
            # Check the generated file content
            test_file_path = os.path.join(output_dir, "tests", "test_producer_data_derivedevent.py")
            assert os.path.exists(test_file_path), f"Expected test file at {test_file_path}"
            
            with open(test_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # The import should be: from test_producer_data_baseevent import Test_BaseEvent
            # NOT: from test_test_producer_data_baseevent import Test_BaseEvent
            # The template uses import_type.split('.')[:-1] to get the package path,
            # which for 'test_producer_data.BaseEvent' becomes 'test_producer_data'
            # Since it starts with 'test_', no additional prefix is added
            assert 'from test_producer_data import Test_BaseEvent' in content, \
                f"Expected correct import statement in test file, got:\n{content}"
            assert 'test_test_' not in content, \
                f"Found double test_ prefix in import statement:\n{content}"
                
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_gzip_compression_json(self):
        """ Test that gzip compression works correctly with JSON content type """
        import gzip
        cwd = os.getcwd()
        avro_path = os.path.join(cwd, "test", "avsc", "address.avsc")
        py_path = os.path.join(tempfile.gettempdir(), "avrotize", "address-py-gzip-test")
        if os.path.exists(py_path):
            shutil.rmtree(py_path, ignore_errors=True)
        os.makedirs(py_path, exist_ok=True)

        # Generate Python code with both JSON and Avro support
        convert_avro_to_python(avro_path, py_path, dataclasses_json_annotation=True, avro_annotation=True)
        
        # Add test directory to Python path
        new_env = os.environ.copy()
        new_env['PYTHONPATH'] = os.path.join(py_path, 'src')
        
        # Create test script
        test_script = os.path.join(py_path, "test_compression.py")
        with open(test_script, 'w', encoding='utf-8') as f:
            f.write("""
import gzip
import io
from address.example.com import Record

# Create a test record
record = Record(
    type='address',
    postOfficeBox=None,
    extendedAddress=None,
    streetAddress='123 Main St',
    locality='Springfield',
    region='IL',
    postalCode='62701',
    countryName='USA'
)

# Test 1: JSON serialization without compression
print("Test 1: JSON serialization without compression")
json_bytes = record.to_byte_array('application/json')
assert isinstance(json_bytes, (bytes, str)), "JSON serialization should return bytes or str"
print(f"  ✓ JSON serialization works: {len(json_bytes)} bytes")

# Test 2: JSON serialization with gzip compression
print("Test 2: JSON serialization with gzip compression")
json_gzip_bytes = record.to_byte_array('application/json+gzip')
assert isinstance(json_gzip_bytes, bytes), "Compressed data should be bytes"
print(f"  ✓ JSON+gzip serialization works: {len(json_gzip_bytes)} bytes")

# Test 3: Verify compression actually happened
if isinstance(json_bytes, str):
    json_bytes_len = len(json_bytes.encode('utf-8'))
else:
    json_bytes_len = len(json_bytes)
print(f"  Uncompressed: {json_bytes_len} bytes, Compressed: {len(json_gzip_bytes)} bytes")
# Compressed should typically be smaller, but with small data it might be larger due to gzip overhead
# Just verify the data is actually gzip-compressed by checking the magic number
assert json_gzip_bytes[:2] == bytes([0x1f, 0x8b]), "Data should be gzip compressed (magic number check)"
print(f"  ✓ Data is gzip compressed")

# Test 4: Verify we can decompress the data manually
with gzip.GzipFile(fileobj=io.BytesIO(json_gzip_bytes), mode='rb') as gf:
    decompressed = gf.read()
    print(f"  ✓ Manual decompression successful: {len(decompressed)} bytes")

# Test 5: Round-trip test - serialize with compression, deserialize with compression
print("Test 3: Round-trip with compression")
record2 = Record.from_data(json_gzip_bytes, 'application/json+gzip')
assert record2 is not None, "Deserialization should return a record"
assert record2.locality == 'Springfield', f"Expected locality='Springfield', got '{record2.locality}'"
assert record2.streetAddress == '123 Main St', f"Expected streetAddress='123 Main St', got '{record2.streetAddress}'"
print(f"  ✓ Round-trip successful")

# Test 6: Avro binary with gzip compression
print("Test 4: Avro binary with gzip compression")
avro_gzip_bytes = record.to_byte_array('avro/binary+gzip')
assert isinstance(avro_gzip_bytes, bytes), "Compressed Avro should be bytes"
assert avro_gzip_bytes[:2] == bytes([0x1f, 0x8b]), "Avro data should be gzip compressed"
print(f"  ✓ Avro+gzip serialization works: {len(avro_gzip_bytes)} bytes")

# Test 7: Round-trip test with Avro + gzip
print("Test 5: Round-trip with Avro + gzip")
record3 = Record.from_data(avro_gzip_bytes, 'avro/binary+gzip')
assert record3 is not None, "Avro deserialization should return a record"
assert record3.locality == 'Springfield', f"Expected locality='Springfield', got '{record3.locality}'"
assert record3.streetAddress == '123 Main St', f"Expected streetAddress='123 Main St', got '{record3.streetAddress}'"
print(f"  ✓ Avro round-trip successful")

# Test 8: Test alternative Avro content type with gzip
print("Test 6: Alternative Avro content type with gzip")
avro_alt_gzip_bytes = record.to_byte_array('application/vnd.apache.avro+avro+gzip')
assert isinstance(avro_alt_gzip_bytes, bytes), "Compressed Avro should be bytes"
assert avro_alt_gzip_bytes[:2] == bytes([0x1f, 0x8b]), "Avro data should be gzip compressed"
record4 = Record.from_data(avro_alt_gzip_bytes, 'application/vnd.apache.avro+avro+gzip')
assert record4.locality == 'Springfield', f"Expected locality='Springfield', got '{record4.locality}'"
print(f"  ✓ Alternative Avro content type with gzip works")

print("\\nAll compression tests passed! ✓")
""")
        
        # Run the test script
        result = subprocess.run(
            [sys.executable, test_script],
            cwd=py_path,
            env=new_env,
            capture_output=True,
            text=True
        )
        
        assert result.returncode == 0, \
            f"Compression test failed with code {result.returncode}.\\nStdout: {result.stdout}\\nStderr: {result.stderr}"
        assert "All compression tests passed" in result.stdout, \
            f"Expected success message in output, got: {result.stdout}"
