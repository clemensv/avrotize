import argparse
import os
import unittest
import tempfile
from unittest.mock import patch
from avrotize.avrotize import main

def get_avsc():
    """Provides the Avro input file path."""
    return os.path.join(os.path.dirname(__file__), 'avsc', 'northwind.avsc')

def get_xsd():
    """Provides the XSD input file path."""
    return os.path.join(os.path.dirname(__file__), 'xsd', 'crmdata.xsd')

def get_proto():
    """Provides the Proto input file path."""
    return os.path.join(os.path.dirname(__file__), 'proto', 'user.proto')

def get_json():
    """Provides the JSON input file path."""
    return os.path.join(os.path.dirname(__file__), 'jsons', 'address.json')

def get_parquet():
    """Provides the Parquet input file path."""
    return os.path.join(os.path.dirname(__file__), 'parquet', 'address.parquet')

def get_asn1():
    """Provides the ASN.1 input file path."""
    return os.path.join(os.path.dirname(__file__), 'asn1', 'person.asn')

def get_kstruct():
    """Provides the Kafka Struct input file path."""
    return os.path.join(os.path.dirname(__file__), 'kstruct', 'cardata.json')

class TestMain(unittest.TestCase):

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command=None))
    def test_main_no_command(self, mock_parse_args):
        """Test main function with no command."""
        with patch('builtins.print') as mock_print:
            main()


    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='p2a', input=get_proto(), out=tempfile.gettempdir() + '/output.avsc'))
    def test_main_p2a_command(self, mock_parse_args):
        """Test main function with p2a command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.avsc')  # Add assertion for file existence
            
    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='a2p', input=get_avsc(), out=tempfile.gettempdir() + '/output.proto', naming='snake', allow_optional=True))
    def test_main_a2p_command(self, mock_parse_args):
        """Test main function with a2p command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.proto')  # Add assertion for file existence

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='j2a', input=get_json(), out=tempfile.gettempdir() + '/output.avsc', namespace='com.example', split_top_level_records=False))
    def test_main_j2a_command(self, mock_parse_args):
        """Test main function with j2a command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.avsc')  # Add assertion for file existence

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='a2j', input=get_avsc(), out=tempfile.gettempdir() + '/output.jsons', namespace='com.example', naming='snake'))
    def test_main_a2j_command(self, mock_parse_args):
        """Test main function with a2j command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.jsons')  # Add assertion for file existence
            
    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='x2a', input=get_xsd(), out=tempfile.gettempdir() + '/output.avsc', namespace='com.example'))
    def test_main_x2a_command(self, mock_parse_args):
        """Test main function with x2a command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.avsc')  # Add assertion for file existence
            
    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='a2x', input=get_avsc(), out=tempfile.gettempdir() + '/output.xsd'))
    def test_main_a2x_command(self, mock_parse_args):
        """Test main function with a2x command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.xsd')  # Add assertion for file existence

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='a2k', input=get_avsc(), out=tempfile.gettempdir() + '/output.kql', kusto_uri='', kusto_database='', record_type='', emit_cloudevents_columns=True, emit_cloudevents_dispatch=False))
    def test_main_a2k_command(self, mock_parse_args):
        """Test main function with a2k command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.kql')  # Add assertion for file existence

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='a2sql', input=get_avsc(), out=tempfile.gettempdir() + '/output.sql', dialect='mysql', emit_cloudevents_columns=True))
    def test_main_a2sql_command(self, mock_parse_args):
        """Test main function with a2sql command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.sql')  # Add assertion for file existence

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='a2mongo', input=get_avsc(), out=tempfile.gettempdir() + '/output.json', emit_cloudevents_columns=True))
    def test_main_a2mongo_command(self, mock_parse_args):
        """Test main function with a2mongo command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.json')  # Add assertion for file existence

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='a2pq', input=get_avsc(), out=tempfile.gettempdir() + '/output.parquet', record_type='Northwind.Order', emit_cloudevents_columns=True))
    def test_main_a2pq_command(self, mock_parse_args):
        """Test main function with a2pq command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.parquet')  # Add assertion for file existence

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='a2ib', input=get_avsc(), out=tempfile.gettempdir() + '/output.iceberg', record_type='Northwind.Order', emit_cloudevents_columns=True))
    def test_main_a2ib_command(self, mock_parse_args):
        """Test main function with a2ib command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.iceberg')  # Add assertion for file existence

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='pq2a', input=get_parquet(), out=tempfile.gettempdir() + '/output.avsc', namespace='com.example'))
    def test_main_pq2a_command(self, mock_parse_args):
        """Test main function with pq2a command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.avsc')  # Add assertion for file existence

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='asn2a', input=get_asn1(), out=tempfile.gettempdir() + '/output.avsc'))
    def test_main_asn2a_command(self, mock_parse_args):
        """Test main function with asn2a command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.avsc')  # Add assertion for file existence

    @patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(command='kstruct2a', input=get_kstruct(), out=tempfile.gettempdir() + '/output.avsc'))
    def test_main_kstruct2a_command(self, mock_parse_args):
        """Test main function with kstruct2a command."""
        main()
        assert os.path.exists(tempfile.gettempdir() + '/output.avsc')  # Add assertion for file existence

if __name__ == '__main__':
    unittest.main()
