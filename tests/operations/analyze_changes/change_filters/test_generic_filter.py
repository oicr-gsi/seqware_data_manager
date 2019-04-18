import pandas as pd
from pandas.testing import assert_series_equal

import operations.analyze_changes.change_filters as filters


def test_generic_filter_wildcard(shared_datadir):
    data = pd.read_csv(shared_datadir / 'data.tsv', delimiter='\t', header=0)
    data.set_index('index', inplace=True)
    changes = pd.read_csv(shared_datadir / 'changes.tsv', delimiter='\t', header=0)
    changes.set_index('index', inplace=True)
    filter = {'Sample Name': '*',
              'field': 'Study Title',
              'from': 'abc',
              'to': 'ABC'}
    actual = filters.generic_filter_changes(data, changes, filters=filter)
    expected = pd.Series(data=[True, False, False, False,
                               True, False, False, False,
                               False, False, False, False,
                               False, False, False],
                         index=pd.Int64Index([0, 0, 0, 0,
                                              1, 1, 1, 1,
                                              2, 2, 2, 2,
                                              3, 3, 3], name='index'))
    assert_series_equal(actual, expected)


def test_generic_filter_okay_regex(shared_datadir):
    data = pd.read_csv(shared_datadir / 'data.tsv', delimiter='\t', header=0)
    data.set_index('index', inplace=True)
    changes = pd.read_csv(shared_datadir / 'changes.tsv', delimiter='\t', header=0)
    changes.set_index('index', inplace=True)
    filter = {'Sample Name': '^ABC_.*$',
              'field': 'Study Title',
              'from': 'abc',
              'to': 'ABC'}
    actual = filters.generic_filter_changes(data, changes, filters=filter)
    expected = pd.Series(data=[True, False, False, False,
                               True, False, False, False,
                               False, False, False, False,
                               False, False, False],
                         index=pd.Int64Index([0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3], name='index'))
    assert_series_equal(actual, expected)


def test_generic_filter_bad_regex(shared_datadir):
    data = pd.read_csv(shared_datadir / 'data.tsv', delimiter='\t', header=0)
    data.set_index('index', inplace=True)
    changes = pd.read_csv(shared_datadir / 'changes.tsv', delimiter='\t', header=0)
    changes.set_index('index', inplace=True)
    filter = {'Sample Name': '^abc_.*$',
              'field': 'Study Title',
              'from': 'abc',
              'to': 'ABC'}
    actual = filters.generic_filter_changes(data, changes, filters=filter)
    expected = pd.Series(data=[False, False, False, False,
                               False, False, False, False,
                               False, False, False, False,
                               False, False, False],
                         index=pd.Int64Index([0, 0, 0, 0,
                                              1, 1, 1, 1,
                                              2, 2, 2, 2,
                                              3, 3, 3], name='index'))
    assert_series_equal(actual, expected)


def test_generic_filter_value_list(shared_datadir):
    data = pd.read_csv(shared_datadir / 'data.tsv', delimiter='\t', header=0)
    data.set_index('index', inplace=True)
    changes = pd.read_csv(shared_datadir / 'changes.tsv', delimiter='\t', header=0)
    changes.set_index('index', inplace=True)
    filter = {'Sample Name': ['ABC_123', 'ABC_111'],
              'field': 'Study Title',
              'from': 'abc',
              'to': 'ABC'}
    actual = filters.generic_filter_changes(data, changes, filters=filter)
    expected = pd.Series(data=[True, False, False, False,
                               False, False, False, False,
                               False, False, False, False,
                               False, False, False],
                         index=pd.Int64Index([0, 0, 0, 0,
                                              1, 1, 1, 1,
                                              2, 2, 2, 2,
                                              3, 3, 3], name='index'))
    assert_series_equal(actual, expected)


def test_generic_filter_value_list_with_missing(shared_datadir):
    data = pd.read_csv(shared_datadir / 'data.tsv', delimiter='\t', header=0)
    data.set_index('index', inplace=True)
    changes = pd.read_csv(shared_datadir / 'changes.tsv', delimiter='\t', header=0)
    changes.set_index('index', inplace=True)
    filter = {'Sample Name': ['ABC_123', 'does_not_exist'],
              'field': 'Study Title',
              'from': 'abc',
              'to': 'ABC'}
    actual = filters.generic_filter_changes(data, changes, filters=filter)
    expected = pd.Series(data=[True, False, False, False,
                               False, False, False, False,
                               False, False, False, False,
                               False, False, False],
                         index=pd.Int64Index([0, 0, 0, 0,
                                              1, 1, 1, 1,
                                              2, 2, 2, 2,
                                              3, 3, 3], name='index'))
    assert_series_equal(actual, expected)


def test_generic_filter_add_field(shared_datadir):
    data = pd.read_csv(shared_datadir / 'data.tsv', delimiter='\t', header=0)
    data.set_index('index', inplace=True)
    changes = pd.read_csv(shared_datadir / 'changes.tsv', delimiter='\t', header=0)
    changes.set_index('index', inplace=True)
    filter = {'Sample Name': '*',
              'field': 'New Field',
              'from': '',
              'to': '*'}
    actual = filters.generic_filter_changes(data, changes, filters=filter)
    expected = pd.Series(data=[False, False, True, False,
                               False, False, True, False,
                               False, False, True, False,
                               False, True, False],
                         index=pd.Int64Index([0, 0, 0, 0,
                                              1, 1, 1, 1,
                                              2, 2, 2, 2,
                                              3, 3, 3], name='index'))
    assert_series_equal(actual, expected)


def test_generic_filter_delete_field(shared_datadir):
    data = pd.read_csv(shared_datadir / 'data.tsv', delimiter='\t', header=0)
    data.set_index('index', inplace=True)
    changes = pd.read_csv(shared_datadir / 'changes.tsv', delimiter='\t', header=0)
    changes.set_index('index', inplace=True)
    filter = {'Sample Name': '*',
              'field': 'Old Field',
              'from': '*',
              'to': ''}
    actual = filters.generic_filter_changes(data, changes, filters=filter)
    expected = pd.Series(data=[False, False, False, True,
                               False, False, False, True,
                               False, False, False, True,
                               False, False, True],
                         index=pd.Int64Index([0, 0, 0, 0,
                                              1, 1, 1, 1,
                                              2, 2, 2, 2,
                                              3, 3, 3], name='index'))
    assert_series_equal(actual, expected)


def test_generic_filter_replace_field(shared_datadir):
    data = pd.read_csv(shared_datadir / 'data.tsv', delimiter='\t', header=0)
    data.set_index('index', inplace=True)
    changes = pd.read_csv(shared_datadir / 'changes.tsv', delimiter='\t', header=0)
    changes.set_index('index', inplace=True)
    filter = {'field': 'Other',
              'from': 'A⌀B',
              'to': 'A&B'}
    actual = filters.generic_filter_changes(data, changes, filters=filter)
    expected = pd.Series(data=[False, False, False, False,
                               False, False, False, False,
                               False, False, False, False,
                               True, False, False],
                         index=pd.Int64Index([0, 0, 0, 0,
                                              1, 1, 1, 1,
                                              2, 2, 2, 2,
                                              3, 3, 3], name='index'))
    assert_series_equal(actual, expected)
