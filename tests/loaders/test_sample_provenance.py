import pandas as pd
import pytest

from loaders import sample_provenance


def test_load(shared_datadir):
    fpr = sample_provenance.load((shared_datadir / 'sp.json').as_uri(), provider='lims-provider')  # type:pd.DataFrame
    assert len(fpr) == 1


def test_load_bad_data(shared_datadir):
    pytest.raises(Exception, sample_provenance.load, (shared_datadir / 'sp_malformed.json').as_uri(),
                  provider='lims-provider')
