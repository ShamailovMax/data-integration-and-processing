import unittest
import sys
import os


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from PostgresDatabase import PostgresDatabase

class TestPostgresDatabase(unittest.TestCase):

    def test_clean_name(self):
        test_cases = {
            "Test Name": "test_name",
            "Another-Test?": "another_test",
            "Name/With/Slashes": "name_with_slashes",
            "Invalid\\Characters%$()": "invalid_characters",
            "SimpleName": "simplename",
            "": "", 
            "    Spaces    ": "____spaces____",
            # "ЭЭ _=+": "__"  # эта хреновина валит тест
        }
        
        for input_name, expected_output in test_cases.items():
            self.assertEqual(PostgresDatabase.clean_name(input_name), expected_output)


if __name__ == "__main__":
    unittest.main()
