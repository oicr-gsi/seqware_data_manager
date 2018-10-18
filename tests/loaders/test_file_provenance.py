import pandas as pd
import pytest

from loaders import file_provenance


def test_load(shared_datadir):
    fpr = file_provenance.load(shared_datadir / 'file_provenance.tsv')  # type:pd.DataFrame
    assert len(fpr) == 1

    # attribute columns get flattened
    assert set(fpr.columns) - set(file_provenance.fpr_cols) == {'Lane Attributes.geo_lane',
                                                                'Parent Sample Attributes.geo_library_source_template_type',
                                                                'Parent Sample Attributes.geo_organism',
                                                                'Sample Attributes.geo_library_source_template_type',
                                                                'Sample Attributes.geo_organism',
                                                                'Sequencer Run Attributes.geo_instrument_run_id',
                                                                'Study Attributes.geo_lab_group_id'}
    assert set(file_provenance.fpr_cols) - set(fpr.columns) == {'Lane Attributes',
                                                                'Parent Sample Attributes',
                                                                'Sample Attributes',
                                                                'Sequencer Run Attributes',
                                                                'Study Attributes'}


def test_load_no_header(shared_datadir):
    fpr = file_provenance.load(shared_datadir / 'file_provenance_no_header.tsv')  # type:pd.DataFrame
    assert len(fpr) == 1


def test_load_bad_data(shared_datadir):
    pytest.raises(Exception, file_provenance.load, shared_datadir / 'file_provenance_malformed.tsv')
