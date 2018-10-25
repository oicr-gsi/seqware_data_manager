import pandas as pd
from pandas.testing import assert_series_equal

from operations.analyze_changes.change_filters.grouping import grouping_logic_okay


def test_empty():
    data = pd.DataFrame({'type': ['W1', 'W2', 'W3', 'W4', 'W5'],
                         'id': [1, 2, 3, 4, 5],
                         'current_field': ['A', 'B', 'C', 'D', 'E'],
                         'new_field': ['A', 'B', 'C', 'D', 'E']})

    changes = pd.DataFrame({}, columns=['field', 'from', 'to'])

    allowed_changes = grouping_logic_okay(data, changes,
                                          group_by_type=['type'],
                                          group_by_field='id',
                                          current_group_fields=['current_field'],
                                          new_group_fields=['new_field'],
                                          allowed_field_changes=[''])
    assert len(allowed_changes) == 0


def test_single_record_group_rename():
    data = pd.DataFrame({'type': ['W1'],
                         'id': [1],
                         'current_field': ['A'],
                         'new_field': ['B']},
                        index=[0])

    changes = pd.DataFrame({'field': ['current_field'],
                            'from': ['A'],
                            'to': ['B']},
                           index=[0])

    actual = grouping_logic_okay(data, changes,
                                 group_by_type=['type'],
                                 group_by_field='id',
                                 current_group_fields=['current_field'],
                                 new_group_fields=['new_field'],
                                 allowed_field_changes=['current_field'])
    expected = pd.Series([True], index=[0], name='field')
    assert_series_equal(actual, expected)


def test_multiple_record_group_rename():
    data = pd.DataFrame({'type': ['W1', 'W1'],
                         'id': [1, 1],
                         'current_field': ['A', 'A'],
                         'new_field': ['B', 'B']},
                        index=[0, 1])

    changes = pd.DataFrame({'field': ['current_field', 'current_field'],
                            'from': ['A', 'A'],
                            'to': ['B', 'B']},
                           index=[0, 1])

    actual = grouping_logic_okay(data, changes,
                                 group_by_type=['type'],
                                 group_by_field='id',
                                 current_group_fields=['current_field'],
                                 new_group_fields=['new_field'],
                                 allowed_field_changes=['current_field'])
    expected = pd.Series([True, True], index=[0, 1], name='field')
    assert_series_equal(actual, expected)


def test_multiple_record_group_rename():
    data = pd.DataFrame({'type': ['W1', 'W1'],
                         'id': [1, 1],
                         'current_field': ['A', 'B'],
                         'new_field': ['A', 'C']},
                        index=[0, 1])

    changes = pd.DataFrame({'field': ['current_field'],
                            'from': ['B'],
                            'to': ['C']},
                           index=[1])

    actual = grouping_logic_okay(data, changes,
                                 group_by_type=['type'],
                                 group_by_field='id',
                                 current_group_fields=['current_field'],
                                 new_group_fields=['new_field'],
                                 allowed_field_changes=['current_field'])
    expected = pd.Series([True], index=[1], name='field')
    assert_series_equal(actual, expected)


def test_allowed_field_changes():
    data = pd.DataFrame({'type': ['W1'],
                         'id': [1],
                         'current_field_1': ['A'],
                         'current_field_2': [1],
                         'new_field_1': ['B'],
                         'new_field_2': [2]},
                        index=[0])

    changes = pd.DataFrame({'field': ['current_field_1', 'current_field_2'],
                            'from': ['A', 1],
                            'to': ['B', 2]},
                           index=[0, 0])

    actual = grouping_logic_okay(data, changes,
                                 group_by_type=['type'],
                                 group_by_field='id',
                                 current_group_fields=['current_field_1', 'current_field_2'],
                                 new_group_fields=['new_field_1', 'new_field_2'],
                                 allowed_field_changes=['current_field_1'])
    expected = pd.Series([True, False], index=[0, 0], name='field')
    assert_series_equal(actual, expected)


def test_group_split():
    data = pd.DataFrame({'type': ['W1', 'W1'],
                         'id': [1, 1],
                         'current_field': ['A', 'A'],
                         'new_field': ['B', 'C']},
                        index=[0, 1])

    changes = pd.DataFrame({'field': ['current_field', 'current_field'],
                            'from': ['A', 'A'],
                            'to': ['B', 'C']},
                           index=[0, 1])

    actual = grouping_logic_okay(data, changes,
                                 group_by_type=['type'],
                                 group_by_field='id',
                                 current_group_fields=['current_field'],
                                 new_group_fields=['new_field'],
                                 allowed_field_changes=['current_field'])
    expected = pd.Series([False, False], index=[0, 1], name='field')
    assert_series_equal(actual, expected)


def test_group_merge():
    data = pd.DataFrame({'type': ['W1', 'W1'],
                         'id': [1, 2],
                         'current_field': ['A', 'B'],
                         'new_field': ['C', 'C']},
                        index=[0, 1])

    changes = pd.DataFrame({'field': ['current_field', 'current_field'],
                            'from': ['A', 'B'],
                            'to': ['C', 'C']},
                           index=[0, 1])

    actual = grouping_logic_okay(data, changes,
                                 group_by_type=['type'],
                                 group_by_field='id',
                                 current_group_fields=['current_field'],
                                 new_group_fields=['new_field'],
                                 allowed_field_changes=['current_field'])
    expected = pd.Series([False, False], index=[0, 1], name='field')
    assert_series_equal(actual, expected)


def test_group_merge_different_types():
    data = pd.DataFrame({'type': ['W1', 'W1', 'W2', 'W3'],
                         'id': [1, 2, 3, 4],
                         'current_field': ['A', 'B', 'A', 'A'],
                         'new_field': ['C', 'C', 'C', 'C'], },
                        index=[0, 1, 2, 3])

    changes = pd.DataFrame({'field': ['current_field', 'current_field', 'current_field', 'current_field'],
                            'from': ['A', 'B', 'A', 'A'],
                            'to': ['C', 'C', 'C', 'C']},
                           index=[0, 1, 2, 3])

    actual = grouping_logic_okay(data, changes,
                                 group_by_type=['type'],
                                 group_by_field='id',
                                 current_group_fields=['current_field'],
                                 new_group_fields=['new_field'],
                                 allowed_field_changes=['current_field'])
    expected = pd.Series([False, False, True, True], index=[0, 1, 2, 3], name='field')
    assert_series_equal(actual, expected)


def test_swap():
    data = pd.DataFrame({'type': ['W1', 'W1', 'W1', 'W1'],
                         'id': [1, 1, 2, 2],
                         'current_field': ['A', 'A', 'B', 'B'],
                         'new_field': ['A', 'B', 'A', 'B']},
                        index=[0, 1, 2, 3])

    changes = pd.DataFrame({'field': ['current_field', 'current_field'],
                            'from': ['A', 'B'],
                            'to': ['B', 'A']},
                           index=[1, 2])

    actual = grouping_logic_okay(data, changes,
                                 group_by_type=['type'],
                                 group_by_field='id',
                                 current_group_fields=['current_field'],
                                 new_group_fields=['new_field'],
                                 allowed_field_changes=['current_field'])
    expected = pd.Series([False, False], index=[1, 2], name='field')
    assert_series_equal(actual, expected)


def test_multiple_grouping_types():
    data = pd.DataFrame({'type': ['W1', 'W1', 'W1', 'W1'],
                         'run': ['1', '2', '3', '4'],
                         'id': [1, 2, 3, 4],
                         'current_field': ['A', 'A', 'A', 'A'],
                         'new_field': ['B', 'C', 'D', 'E']},
                        index=[0, 1, 2, 3])

    changes = pd.DataFrame({'field': ['current_field', 'current_field', 'current_field', 'current_field'],
                            'from': ['A', 'A', 'A', 'A'],
                            'to': ['B', 'B', 'B', 'B']},
                           index=[0, 1, 2, 3])

    actual = grouping_logic_okay(data, changes,
                                 group_by_type=['type'],
                                 group_by_field='id',
                                 current_group_fields=['current_field'],
                                 new_group_fields=['new_field'],
                                 allowed_field_changes=['current_field'])
    expected = pd.Series([False, False, False, False], index=[0, 1, 2, 3], name='field')
    assert_series_equal(actual, expected)

    # include "run" in group_by_type
    actual = grouping_logic_okay(data, changes,
                                 group_by_type=['type', 'run'],
                                 group_by_field='id',
                                 current_group_fields=['current_field'],
                                 new_group_fields=['new_field'],
                                 allowed_field_changes=['current_field'])
    expected = pd.Series([True, True, True, True], index=[0, 1, 2, 3], name='field')
    assert_series_equal(actual, expected)
