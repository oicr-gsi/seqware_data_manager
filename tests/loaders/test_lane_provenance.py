import pandas as pd
import pytest

from loaders import lane_provenance


def test_load(shared_datadir):
    fpr = lane_provenance.load((shared_datadir / 'lp.json').as_uri(), provider='lims-provider')  # type:pd.DataFrame
    assert len(fpr) == 1


def test_load_bad_data(shared_datadir):
    pytest.raises(Exception, lane_provenance.load, (shared_datadir / 'lp_malformed.json').as_uri(),
                  provider='lims-provider')
