from operations.import_and_join_data import JoinedData


def test_load_from_files(shared_datadir):
    sp = shared_datadir / 'sp.json'
    lp = shared_datadir / 'lp.json'
    provider = 'lims-provider'
    fp = shared_datadir / 'fpr.tsv'
    ctx = JoinedData.load_from_files(
        sample_provenance_path=sp.as_uri(),
        lane_provenance_path=lp.as_uri(),
        provider=provider,
        file_provenance_path=fp)

    assert len(ctx.fpr) == 1
    expected = ['TEST_0001',
                '156_1_LDI8904',
                '36294f76c457bcdcc1e8f153f65925b47cecfeac59867b14995e79572b6ca7a3',
                'lims-provider']
    assert all(ctx.fpr.iloc[0][['rootSampleName', 'provenanceId', 'version', 'provider']].values == expected)
