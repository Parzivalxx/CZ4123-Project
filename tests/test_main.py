from unittest import TestCase
from convert_dataframe import read_output
import os
import pandas as pd


class TestMain(TestCase):
    """
    Create output for u2022913c before running unit test
    """
    def test_output(self):
        path = os.path.join('data', 'SingaporeWeather.csv')
        df_expected = read_output(path)
        path = os.path.join('results', 'ScanResult_u2022913c.csv')
        df_actual = pd.read_csv(path)
        df_actual.sort_values(['Date', 'Category'], ignore_index=True, inplace=True)
        assertFrameEqual(df_expected, df_actual)


def assertFrameEqual(df1, df2, **kwds):
    """ Assert that two dataframes are equal, ignoring ordering of columns"""
    from pandas.util.testing import assert_frame_equal
    return assert_frame_equal(
        df1.sort_index(axis=1),
        df2.sort_index(axis=1),
        check_names=True,
        **kwds
    )
