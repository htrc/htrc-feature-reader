import pytest
from htrc_features import FeatureReader
import htrc_features


class TestFeatureReader():
    PATHS = [('tests/data/green-gables-full-basic.json.bz2',
             'tests/data/green-gables-full-advanced.json.bz2')]
    TITLES = ['Anne of Green Gables / L.M. Montgomery.']

    def test_single_path_load(self):
        path = self.PATHS[0][0]
        feature_reader = FeatureReader(path)
        vol = next(feature_reader.volumes())
        assert type(vol) == htrc_features.feature_reader.Volume

    def test_tuple_path_load(self):
        paths = self.PATHS[0]
        feature_reader = FeatureReader(paths)
        vol = next(feature_reader.volumes())
        assert type(vol) == htrc_features.feature_reader.Volume

    def test_list_load(self):
        paths = [basic for basic, advanced in self.PATHS]
        feature_reader = FeatureReader(paths)
        vol = next(feature_reader.volumes())
        assert type(vol) == htrc_features.feature_reader.Volume

    def test_list_tuple_load(self):
        paths = self.PATHS
        feature_reader = FeatureReader(paths)
        for i, vol in enumerate(feature_reader):
            assert type(vol) == htrc_features.feature_reader.Volume
            assert vol.title == self.TITLES[i]

    def test_uncompressed(self):
        paths = [(basic.replace('.bz2', ''), advanced.replace('.bz2', ''))
                 for basic, advanced in self.PATHS]
        feature_reader = FeatureReader(paths, compressed=False)
        for i, vol in enumerate(feature_reader):
            assert type(vol) == htrc_features.feature_reader.Volume
            assert vol.title == self.TITLES[i]

    def test_compress_error(self):
        paths = self.PATHS
        feature_reader = FeatureReader(paths, compressed=False)
        with pytest.raises(ValueError):
            next(feature_reader.volumes())

        paths = [(basic.replace('.bz2', ''), advanced.replace('.bz2', ''))
                 for basic, advanced in self.PATHS]
        feature_reader = FeatureReader(paths, compressed=True)
        with pytest.raises(IOError):
            next(feature_reader.volumes())
