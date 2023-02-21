import os
import sys
import numpy as np
import pandas as pd
import unittest
import pyproj
from pyproj import Transformer


# import pycrs

# from data_helper import create_parquet_with_4326_crs

class TestParquetWith4326Crs(unittest.TestCase):
    def setUp(self):
        self.__create_parquet_with_4326_crs = TestParquetWith4326Crs()

    def tearDown(self):
        del self.__create_parquet_with_4326_crs

    def test_no_file(self):
        # file-comparison
        self.input_df_EPSG_3035 = pd.read_parquet(
            './datasets/generated/100m_cleared.parquet')

        self.expected_output_df_EPSG_4326 = pd.read_parquet(
            './datasets/generated/100m_4326.parquet')

        transformer = Transformer.from_crs("EPSG:3035", "EPSG:4326")

        self.input_df_EPSG_3035['x_mp_100m'], self.input_df_EPSG_3035['y_mp_100m'] = transformer.transform(
            self.input_df_EPSG_3035['y_mp_100m'].values, self.input_df_EPSG_3035['x_mp_100m'].values)

        self.input_df_EPSG_3035.to_parquet('src/unit_tests/100m_4326-Testfile.parquet', index=False)
        asserted_input = pd.read_parquet('src/unit_tests/100m_4326-Testfile.parquet')

        pd.testing.assert_frame_equal(asserted_input, self.expected_output_df_EPSG_4326)


if __name__ == '__main__':
    unittest.main()
