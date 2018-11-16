from numpy import nan
from operations.import_and_join_data import JoinedData


def test_load_from_files(shared_datadir):
    sp = shared_datadir / 'sp.json'
    lp = shared_datadir / 'lp.json'
    provider_id = 'lims-provider'
    fp = shared_datadir / 'fpr.tsv'
    ctx = JoinedData.load_from_files(
        sample_provenance_url=sp.as_uri(),
        lane_provenance_url=lp.as_uri(),
        provider_id=provider_id,
        file_provenance_url=fp)

    assert len(ctx.fpr) == 1
    expected = ['TEST_0001',
                '156_1_LDI8904',
                '36294f76c457bcdcc1e8f153f65925b47cecfeac59867b14995e79572b6ca7a3',
                'lims-provider']
    assert all(ctx.fpr.iloc[0][['rootSampleName', 'provenanceId', 'version', 'provider']].values == expected)


def test_load_from_files_using_id_map(shared_datadir):
    sp = shared_datadir / 'sp.json'
    lp = shared_datadir / 'lp.json'
    provider_id = 'lims-provider'
    id_map_url = shared_datadir / 'id_map.csv'
    fp = shared_datadir / 'fpr.tsv'
    ctx = JoinedData.load_from_files(
        sample_provenance_url=sp.as_uri(),
        lane_provenance_url=lp.as_uri(),
        provider_id=provider_id,
        file_provenance_url=fp,
        id_map_url=id_map_url.as_uri())

    assert len(ctx.fpr) == 1
    expected = ['180102_SEQ0001_0001_A0000000000',
                '1_1',
                '4c54d3ebea248af832c024e1d82fbb4167fddbde0319bf8c83d42bfdc6004f48',
                'lims-provider']
    assert all(ctx.fpr.iloc[0][['sequencerRunName', 'provenanceId', 'version', 'provider']].values == expected)
