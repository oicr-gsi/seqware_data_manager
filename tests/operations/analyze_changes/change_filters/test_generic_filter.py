import pandas as pd
from pandas.testing import assert_series_equal

import operations.analyze_changes.change_filters as filters


def test_generic_filter_changes(shared_datadir):
    data = pd.read_csv(shared_datadir / 'data.tsv', delimiter='\t', header=0)
    data.set_index('index', inplace=True)
    changes = pd.read_csv(shared_datadir / 'changes.tsv', delimiter='\t', header=0)
    changes.set_index('index', inplace=True)
    filter = {'Sample Name': '^ABC_.*$',
              'field': 'Study Title',
              'from': 'abc',
              'to': 'ABC'}
    actual = filters.generic_filter_changes(data, changes, filters=filter)
    expected = pd.Series(data=[True, False, True, False, False, False, False],
                         index=pd.Int64Index([0, 0, 1, 1, 2, 2, 3], name ='index'))
    assert_series_equal(actual, expected)
