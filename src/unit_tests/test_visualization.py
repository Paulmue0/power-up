'''
This test was discarded for the time being because the basing functions have to be changed


import os
import sys
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

# import the function create_ev_map to test from the visualization.py
from visualization.visualization import create_ev_map

# Defines a new test case class, which inherits from unittest.TestCase
class TestCreateEVMap(unittest.TestCase):
    def setUp(self):
        self.__create_ev_map = TestCreateEVMap(f"../datasets/DE_Grid_ETRS89-LAEA_{raster_size}.gpkg.zip!DE_Grid_ETRS89-LAEA_{raster_size}/geogitter/DE_Grid_ETRS89-LAEA_{raster_size}.gpkg")

    def tearDown(self):
        del self.__create_ev_map

    def test_no_file(self):
        #test the case if no file is found
        message = 'File not found'
        self.assertIsNone(TestCreateEVMap, message)

# test runner to execute code, check assertions and give test results
if __name__ == '__main__':
    unittest.main()

# TODO:
#  - Solve ImportError: cannot import name 'get_dummy_100km_zensus_data' from 'helper.data_helper'
'''
