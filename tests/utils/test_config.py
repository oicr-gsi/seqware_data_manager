import utils.config


def test_load_json_config(shared_datadir):
    config = utils.config.load_json_config((shared_datadir / 'config1.json').as_uri())
    assert config.get('file') == (shared_datadir / 'test1.file').as_uri()
    assert config.get('file2') == (shared_datadir / 'test2.file').as_uri()
    assert config.get('object').get('file') == (shared_datadir / 'test3.file').as_uri()
