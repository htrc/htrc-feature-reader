import pytest
from htrc_features import FeatureReader
import htrc_features
import os


@pytest.fixture(scope="module")
def paths():
    return [os.path.join('tests', 'data',
                         'green-gables-15pages.json.bz2'),
            os.path.join('tests', 'data',
                         'frankenstein-15pages.json.bz2')]

@pytest.fixture(scope="module")
def ids():
    return ["uc2.ark:/13960/t1xd0sc6x", "hvd.hn6ltf"]
    
@pytest.fixture(scope="module")
def titles():
    return ['Anne of Green Gables / L.M. Montgomery.',
              'Frankenstein : or, The modern Prometheus.']


class TestFeatureReader():

    def test_single_path_load(self, paths):
        path = paths[0]
        feature_reader = FeatureReader(path)
        vol = next(feature_reader.volumes())
        assert type(vol) == htrc_features.feature_reader.Volume

    def test_list_load(self, paths, titles):
        feature_reader = FeatureReader(paths)
        vol = next(feature_reader.volumes())
        assert type(vol) == htrc_features.feature_reader.Volume

        for i, vol in enumerate(feature_reader):
            assert type(vol) == htrc_features.feature_reader.Volume
            assert vol.title == titles[i]
            
    def test_id_remote_load(self, ids):
        id = ids[0]
        feature_reader = FeatureReader(ids=id)
        vol = next(feature_reader.volumes())
        assert type(vol) == htrc_features.feature_reader.Volume

    def test_id_list_remote_load(self, ids, titles):
        feature_reader = FeatureReader(ids=ids)
        vol = next(feature_reader.volumes())
        assert type(vol) == htrc_features.feature_reader.Volume

        for i, vol in enumerate(feature_reader):
            assert type(vol) == htrc_features.feature_reader.Volume
            assert vol.title == titles[i]


    def test_json_only_load(self, paths):
        path = paths[0]
        feature_reader = FeatureReader(path)
        json = next(feature_reader.jsons())
        assert type(json) == dict
        assert json['features']['pages'][7]['header']['tokenCount'] == 5
        assert json['features']['pages'][7]['body']['capAlphaSeq'] == 2

    def test_iteration(self, paths):
        feature_reader = FeatureReader(paths)
        for vol in feature_reader:
            assert type(vol) == htrc_features.feature_reader.Volume
        for vol in feature_reader.volumes():
            assert type(vol) == htrc_features.feature_reader.Volume

    def test_first(self, paths, titles):
        feature_reader = FeatureReader(paths)
        vol = feature_reader.first()
        assert type(vol) == htrc_features.feature_reader.Volume
        assert vol.title == titles[0]

    def test_uncompressed(self, paths, titles):
        paths = [path.replace('.bz2', '') for path in paths]
        feature_reader = FeatureReader(paths, compressed=False)
        for i, vol in enumerate(feature_reader):
            assert type(vol) == htrc_features.feature_reader.Volume
            assert vol.title == titles[i]

    def test_compress_error(self, paths):
        feature_reader = FeatureReader(paths, compressed=False)
        with pytest.raises(ValueError):
            next(feature_reader.volumes())

        paths = [path.replace('.bz2', '') for path in paths]
        feature_reader = FeatureReader(paths, compressed=True)
        with pytest.raises(IOError):
            next(feature_reader.volumes())
