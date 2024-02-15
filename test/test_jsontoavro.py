from os import getcwd, path
import unittest
from unittest.mock import patch
from avrotize.jsonstoavro import convert_jsons_to_avro

class TestJsonsToAvro(unittest.TestCase):
    def test_convert_address_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "address.jsons")
        avro_path = path.join(cwd, "test", "tmp", "address.avsc")
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")           

    def test_convert_movie_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "movie.jsons")
        avro_path = path.join(cwd, "test", "tmp", "movie.avsc")
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")

    def test_convert_person_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "person.jsons")
        avro_path = path.join(cwd, "test", "tmp", "person.avsc")
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")

    def test_convert_employee_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "employee.jsons")
        avro_path = path.join(cwd, "test", "tmp", "employee.avsc")
        
        convert_jsons_to_avro(jsons_path, avro_path, "example.com")

    def test_convert_azurestorage_jsons_to_avro(self):
        cwd = getcwd()        
        jsons_path = path.join(cwd, "test", "jsons", "azurestorage.jsons")
        avro_path = path.join(cwd, "test", "tmp", "azurestorage.avsc")
        
        convert_jsons_to_avro(jsons_path, avro_path, "microsoft.azure.storage")


if __name__ == '__main__':
    unittest.main()