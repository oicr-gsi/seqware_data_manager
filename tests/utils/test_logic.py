import pandas as pd
from pandas.testing import assert_series_equal

from utils.logic import string_match


def test_match(shared_datadir):
    df = pd.read_csv(shared_datadir / 'data.tsv')
    assert_series_equal(string_match(df, 'Study Title', 'A'),
                        pd.Series(name='Study Title', data=[True, True, False, False]))


def test_wildcard(shared_datadir):
    df = pd.read_csv(shared_datadir / 'data.tsv')
    assert_series_equal(string_match(df, 'Study Title', '*'),
                        pd.Series(name='Study Title', data=[True, True, True, True]))


def test_regex(shared_datadir):
    df = pd.read_csv(shared_datadir / 'data.tsv')
    assert_series_equal(string_match(df, 'Sample Name', '^.*_1$'),
                        pd.Series(name='Sample Name', data=[True, False, True, True]))