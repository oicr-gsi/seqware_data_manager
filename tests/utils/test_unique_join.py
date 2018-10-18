import pandas as pd
from pandas.testing import assert_frame_equal

from utils.transformations import unique_join


def test_single():
    from_df = pd.DataFrame([{'from_col_1': 'A', 'from_join_key_1': '1'},
                            {'from_col_1': 'B', 'from_join_key_1': '2'}])
    to_df = pd.DataFrame([{'to_col_1': 'C', 'to_join_key_1': '1'},
                          {'to_col_1': 'D', 'to_join_key_1': '3'}])

    actual = unique_join(from_df, to_df, ['from_col_1'], ['from_join_key_1'], ['to_col_1'], ['to_join_key_1'])
    expected = pd.DataFrame([{'from_col_1': 'A', 'to_col_1': 'C'}])

    assert_frame_equal(actual, expected)


def test_multiple():
    from_df = pd.DataFrame([{'from_col_1': 'A', 'from_join_key_1': '1'},
                            {'from_col_1': 'B', 'from_join_key_1': '2'}])
    to_df = pd.DataFrame([{'to_col_1': 'C', 'to_join_key_1': '1'},
                          {'to_col_1': 'D', 'to_join_key_1': '2'}])

    actual = unique_join(from_df, to_df, ['from_col_1'], ['from_join_key_1'], ['to_col_1'], ['to_join_key_1'])
    expected = pd.DataFrame([{'from_col_1': 'A', 'to_col_1': 'C'},
                             {'from_col_1': 'B', 'to_col_1': 'D'}])

    assert_frame_equal(actual, expected)


def test_duplicates_on_left():
    from_df = pd.DataFrame([{'from_col_1': 'A', 'from_join_key_1': '1'},
                            {'from_col_1': 'B', 'from_join_key_1': '1'}])
    to_df = pd.DataFrame([{'to_col_1': 'C', 'to_join_key_1': '1'}])

    actual = unique_join(from_df, to_df, ['from_col_1'], ['from_join_key_1'], ['to_col_1'], ['to_join_key_1'])
    expected = pd.DataFrame([{'from_col_1': 'A', 'to_col_1': 'C'},
                             {'from_col_1': 'B', 'to_col_1': 'C'}])

    assert_frame_equal(actual, expected)


def test_duplicates_on_right():
    from_df = pd.DataFrame([{'from_col_1': 'A', 'from_join_key_1': '1'}])
    to_df = pd.DataFrame([{'to_col_1': 'C', 'to_join_key_1': '1'},
                          {'to_col_1': 'D', 'to_join_key_1': '1'}])

    actual = unique_join(from_df, to_df, ['from_col_1'], ['from_join_key_1'], ['to_col_1'], ['to_join_key_1'])
    expected = pd.DataFrame(columns=['from_col_1', 'to_col_1'])

    assert_frame_equal(actual, expected, check_index_type=False, check_dtype=False)


def test_duplicates_both():
    from_df = pd.DataFrame([{'from_col_1': 'A', 'from_join_key_1': '1'},
                            {'from_col_1': 'B', 'from_join_key_1': '1'},
                            {'from_col_1': 'C', 'from_join_key_1': '2'}])
    to_df = pd.DataFrame([{'to_col_1': 'D', 'to_join_key_1': '1'},
                          {'to_col_1': 'E', 'to_join_key_1': '2'},
                          {'to_col_1': 'F', 'to_join_key_1': '2'}])

    actual = unique_join(from_df, to_df, ['from_col_1'], ['from_join_key_1'], ['to_col_1'], ['to_join_key_1'])
    expected = pd.DataFrame([{'from_col_1': 'A', 'to_col_1': 'D'},
                             {'from_col_1': 'B', 'to_col_1': 'D'}])

    assert_frame_equal(actual, expected)


def test_multiple_join_keys():
    from_df = pd.DataFrame([{'from_col_1': 'A', 'from_join_key_1': '1', 'from_join_key_2': 'x'},
                            {'from_col_1': 'B', 'from_join_key_1': '2', 'from_join_key_1': 'y'}])
    to_df = pd.DataFrame([{'to_col_1': 'C', 'to_join_key_1': '1', 'to_join_key_2': 'x'},
                          {'to_col_1': 'D', 'to_join_key_1': '2', 'to_join_key_2': 'x'}])

    actual = unique_join(from_df, to_df,
                         ['from_col_1'], ['from_join_key_1', 'from_join_key_2'],
                         ['to_col_1'], ['to_join_key_1', 'to_join_key_2'])
    expected = pd.DataFrame([{'from_col_1': 'A', 'to_col_1': 'C'}])

    assert_frame_equal(actual, expected)
