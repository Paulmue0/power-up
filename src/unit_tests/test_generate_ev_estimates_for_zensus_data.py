from ev_approximation.ev_cache import Cache
from ev_approximation.ev import generate_ev_estimates_for_census_data
import os
import sys
import unittest

import numpy
import pyarrow

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

# imports the function ev_percent_for_municipality to test from ev.py + cache file


# Defines a new test case class, which inherits from unittest.TestCase
class TestGenerateEvEstimatesForZensusData(unittest.TestCase):
    def setUp(self):
        self.__generate_ev_estimates_for_zensus_data = TestGenerateEvEstimatesForZensusData(
            database='./datasets/generated/cleared_ev.parquet')

    def tearDown(self):
        del self.__generate_ev_estimates_for_zensus_data

    def test_no_file(self):
        # test the case if no file is found
        message = 'File not found'
        self.assertIsNone(TestGenerateEvEstimatesForZensusData, message)


# test runner to execute code, check assertions and give test results
if __name__ == '__main__':
    unittest.main()

# TODO:
#  - Solve ModuleNotFoundError: No module named 'ev_cache'
