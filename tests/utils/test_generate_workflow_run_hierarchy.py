import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from utils.analysis import generate_workflow_run_hierarchy


def test_linear():
    fp = pd.DataFrame({'Workflow Run SWID': [1, 2, 3],
                       'Workflow Run Input File SWAs': [np.nan, np.array([101]), np.array([102])],
                       'File SWID': [101, 102, 103]})

    actual, input_ids_missing_in_fp = generate_workflow_run_hierarchy(fp)
    expected = pd.DataFrame([{'parent': 1, 'child': 2.0},
                             {'parent': 2, 'child': 3.0},
                             {'parent': 3, 'child': np.nan}], columns=['parent', 'child'])

    assert_frame_equal(actual, expected, check_dtype=False)
    assert len(input_ids_missing_in_fp) == 0


def test_split():
    fp = pd.DataFrame({'Workflow Run SWID': [1, 2, 3],
                       'Workflow Run Input File SWAs': [np.nan, np.array([101]), np.array([101])],
                       'File SWID': [101, 102, 103]})

    actual, input_ids_missing_in_fp = generate_workflow_run_hierarchy(fp)
    expected = pd.DataFrame([{'parent': 1, 'child': 2.0},
                             {'parent': 1, 'child': 3.0},
                             {'parent': 2, 'child': np.nan},
                             {'parent': 3, 'child': np.nan}], columns=['parent', 'child'])

    assert_frame_equal(actual, expected, check_dtype=False)
    assert len(input_ids_missing_in_fp) == 0


def test_merge():
    fp = pd.DataFrame({'Workflow Run SWID': [1, 2, 3, 4],
                       'Workflow Run Input File SWAs': [np.nan, np.array([101]), np.array([101]), np.array([102, 103])],
                       'File SWID': [101, 102, 103, 104]})

    actual, input_ids_missing_in_fp = generate_workflow_run_hierarchy(fp)
    print(actual)
    expected = pd.DataFrame([{'parent': 1, 'child': 2.0},
                             {'parent': 1, 'child': 3.0},
                             {'parent': 2, 'child': 4.0},
                             {'parent': 3, 'child': 4.0},
                             {'parent': 4, 'child': np.nan}], columns=['parent', 'child'])

    assert_frame_equal(actual, expected, check_dtype=False)
    assert len(input_ids_missing_in_fp) == 0


def test_missing():
    fp = pd.DataFrame({'Workflow Run SWID': [1, 2, 999],
                       'Workflow Run Input File SWAs': [np.nan, np.array([101]), np.array([1999])],
                       'File SWID': [101, 102, 10999]})

    actual, input_ids_missing_in_fp = generate_workflow_run_hierarchy(fp)
    expected = pd.DataFrame([{'parent': 1, 'child': 2.0},
                             {'parent': 2, 'child': np.nan},
                             {'parent': 999, 'child': np.nan}
                             ], columns=['parent', 'child'])

    assert_frame_equal(actual, expected, check_dtype=False)
    assert input_ids_missing_in_fp == {1999}


def test_one_parent_record():
    fp = pd.DataFrame({'Workflow Run SWID': [1],
                       'Workflow Run Input File SWAs': [np.nan],
                       'File SWID': [101]})
    fp['Workflow Run Input File SWAs'] = fp['Workflow Run Input File SWAs'].astype('object')

    actual, input_ids_missing_in_fp = generate_workflow_run_hierarchy(fp)
    expected = pd.DataFrame([{'parent': 1, 'child': np.nan}],
                            columns=['parent', 'child'])

    assert_frame_equal(actual, expected, check_dtype=False)
    assert len(input_ids_missing_in_fp) == 0


def test_one_child_record():
    fp = pd.DataFrame({'Workflow Run SWID': [999],
                       'Workflow Run Input File SWAs': [np.array([101])],
                       'File SWID': [102]})

    actual, input_ids_missing_in_fp = generate_workflow_run_hierarchy(fp)
    expected = pd.DataFrame([{'parent': 999, 'child': np.nan}],
                            columns=['parent', 'child'])

    assert_frame_equal(actual, expected, check_dtype=False)
    assert input_ids_missing_in_fp == {101}
