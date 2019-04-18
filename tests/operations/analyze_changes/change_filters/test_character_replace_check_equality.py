import pandas as pd
from pandas.testing import assert_series_equal

import operations.analyze_changes.change_filters as filters


def test_character_replace_check_equality(shared_datadir):
    data = pd.read_csv(shared_datadir / 'data.tsv', delimiter='\t', header=0)
    data.set_index('index', inplace=True)
    changes = pd.read_csv(shared_datadir / 'changes.tsv', delimiter='\t', header=0)
    changes.set_index('index', inplace=True)
    config = {'field': 'Other',
              'to_characters_regex': '[;=&]',
              'replace_with_character': '⌀'}
    actual = filters.character_replace_check_equality(data, changes, config=config)
    expected = pd.Series(data=[False, False, False, False,
                               False, False, False, False,
                               False, False, False, False,
                               True, False, False],
                         index=pd.Int64Index([0, 0, 0, 0,
                                              1, 1, 1, 1,
                                              2, 2, 2, 2,
                                              3, 3, 3], name='index'))
    assert_series_equal(actual, expected)
