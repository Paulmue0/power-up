import unittest

# imports the function to test from the src package
from src import function

"""
This is a blueprint to code a unit test.
Use for each function an own python file in this folder
Name each test this way: test_name_of_original_function.py
"""

# Defines a new test case class, which inherits from unittest.TestCase


class TestFunction(unittest.TestCase):
    """
    Define test methods and describe what it will do
    Do not forget to load new data set with setup() and
    after tear it down when the test is finished
    """

    def setUp(self):
        self.__function = TestFunction(
            database='insert_test_dataset_path_here.json')

    def tearDown(self):
        del self.__function

    def test_(self):
        data = [x, y, z]
        result = function(data)
        self.assertTrue(result, True)


# test runner to execute code, check assertions and give test results
if __name__ == '__main__':
    unittest.main()

# TODO tests von parking_file-funktionen, ev-approximation-funktionen, Data Helper, ...
#  - Einlesen der Dateien (was wenn Dateien nicht vorhanden? Schlüssel nicht existieren? Werte gleich Null?)
#  - Projektion, Weiterverarbeitung, Transformation der Koordinaten, ...
#  (Nicht: Bubblesplit, weil fliegt raus!)
