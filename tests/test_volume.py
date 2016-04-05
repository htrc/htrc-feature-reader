import pytest
from htrc_features import FeatureReader
import os


# TestFeatureReader already tested loading, so we'll load a common volume
# rather than loading a file with each test. For those unfamiliar with pytest,
# scope='module' ensures that the fixture is run only once

@pytest.fixture(scope="module")
def paths():
    return [(os.path.join('tests', 'data',
                          'green-gables-15pages-basic.json'),
             os.path.join('tests', 'data',
                          'green-gables-15pages-advanced.json'))]


@pytest.fixture(scope="module")
def volume(paths):
    paths = paths[0]
    feature_reader = FeatureReader(paths, compressed=False)
    return next(feature_reader.volumes())


@pytest.fixture(scope="module")
def volume_no_adv(paths):
    paths = paths[0][0]
    feature_reader = FeatureReader(paths, compressed=False)
    return next(feature_reader.volumes())


class TestVolume():

    def test_caching(self, paths):
        import time
        # Load new volume specifically for this test
        paths = paths[0]
        feature_reader = FeatureReader(paths, compressed=False)
        vol = next(feature_reader.volumes())
        # Systems are different, the rough test here simply checks whether
        # the first run is much slower than later runs.
        tokenlist_times = []
        for i in range(0, 6):
            start = time.time()
            vol.tokenlist()
            passed = time.time() - start
            tokenlist_times.append(passed)
        assert tokenlist_times[0] > sum(tokenlist_times[1:])

    def test_included_metadata(self, volume, volume_no_adv):
        metadata = {
            "id": "uc2.ark:/13960/t1xd0sc6x",
            "schemaVersion": "1.2",
            "dateCreated": "2015-02-12T13:30",
            "title": "Anne of Green Gables / L.M. Montgomery.",
            "pubDate": "1908",
            "language": "eng",
            "htBibUrl": "http://catalog.hathitrust.org/api/volumes/"
            "full/htid/uc2.ark:/13960/t1xd0sc6x.json",
            "handleUrl": "http://hdl.handle.net/2027/uc2.ark:/13960/"
            "t1xd0sc6x",
            "oclc": "320127250",
            "imprint": "Ryerson Press, c1908 by L.C. Page.",
            "pageCount": 414
            }
        assert volume.id == metadata['id']

        assert volume.schema_version == metadata['schemaVersion']
        assert volume.title == metadata['title']
        assert volume.date_created == metadata['dateCreated']
        assert volume.pub_date == metadata['pubDate']
        assert volume.language == metadata['language']
        assert volume.ht_bib_url == metadata['htBibUrl']
        assert volume.handle_url == metadata['handleUrl']
        assert volume.oclc == metadata['oclc']
        assert volume.imprint == metadata['imprint']

        # Syntactic sugar fields
        assert volume.year == metadata['pubDate']

        # Field from vol['features']
        assert volume.page_count == metadata['pageCount']

        # For the file without advanced, no need to retest all fields
        assert volume_no_adv.id == metadata['id']
        assert volume_no_adv.title == metadata['title']

    def test_metadata_api(self, volume):
        # For now, test for a valid response.
        metadata = volume.metadata
        assert type(metadata) == dict

    def test_token_stats(self, volume):
        assert volume.tokens[:600:100] == ['/', 'her', 'but', "'re",
                                           'announced', 'entirely']

    def test_token_per_page_counts(self, volume):
        import pandas
        # Test default settings
        tokencounts1 = volume.tokens_per_page()
        counts1 = {'count': {2: 0, 3: 2, 4: 4, 5: 4, 6: 63, 51: 229, 52: 298,
                   53: 344, 54: 316, 55: 295, 56: 327, 57: 383, 58: 341,
                   59: 187, 60: 277, 61: 341}}
        assert type(tokencounts1) == pandas.core.frame.DataFrame
        assert tokencounts1.to_dict() == counts1

        # Changing sections involved in count
        tokencounts2 = volume.tokens_per_page(section='group')
        counts2 = {'count': {2: 0, 3: 2, 4: 4, 5: 4, 6: 63, 51: 233, 52: 303,
                   53: 349, 54: 321, 55: 300, 56: 332, 57: 388, 58: 346,
                   59: 192, 60: 277, 61: 345}}
        assert type(tokencounts2) == pandas.core.frame.DataFrame
        assert tokencounts2.to_dict() == counts2

        # section='all' keeps section info
        tokencounts3 = volume.tokens_per_page(section='all')
        assert tokencounts3.loc[61].to_dict() == {'count': {'body': 341,
                                                            'footer': 0,
                                                            'header': 4}}

    def test_tokenlist_folding(self, volume):
        tl1 = volume.tokenlist()
        assert tl1.index.names == ['page', 'section', 'token', 'pos']
        assert tl1.loc[5, 'body', 'GREEN', 'NE']['count'] == 1
        assert tl1.loc[(54, slice(None), "the"), :].values[0][0] == 7
        assert (tl1.loc[(57, slice(None), "all", slice(None)), 'count'].size ==
                2)
        with pytest.raises(KeyError):
            tl1.loc[54, 'header']

        tl2 = volume.tokenlist(pos=False)
        assert tl2.index.names == ['page', 'section', 'token']
        assert tl2.loc[(57, slice(None), "all"), 'count'].size == 1
        with pytest.raises(KeyError):
            tl2.loc[54, 'header']

        tl3 = volume.tokenlist(case=False)
        assert tl3.index.names == ['page', 'section', 'lowercase', 'pos']
        assert tl3.loc[(54, slice(None), "the"), :].values[0][0] == 8

        tl4 = volume.tokenlist(pages=False, pos=False)
        assert (tl4.reset_index()['token'].size ==
                tl4.reset_index()['token'].unique().size)

        tl5 = volume.tokenlist(section='group')
        assert tl5.index.names == ['page', 'token', 'pos']
        assert (tl5.loc[(slice(None), 'ANNE'), :].sum()[0] >
                tl1.loc[(slice(None), slice(None), 'ANNE'), :].sum()[0])

        tl6 = volume.tokenlist(section='all')
        assert tl6.index.names == ['page', 'section', 'token', 'pos']
        assert tl6.size > tl1.size
        assert not tl6.loc[(slice(None), 'header'), :].empty

        tl7 = volume.tokenlist(section='group', pos=False, case=False,
                               pages=False)
        assert tl7.index.names == ['lowercase']

    def test_internal_tokencount_representation(self, paths):
        paths = paths[0]
        feature_reader = FeatureReader(paths, compressed=False)
        vol = next(feature_reader.volumes())

        assert vol._tokencounts.empty
        vol.tokenlist()
        assert vol._tokencounts.index.names == ['page', 'section', 'token',
                                                'pos']
        vol.tokenlist(case=False)
        assert vol._tokencounts.index.names == ['page', 'section', 'token',
                                                'pos']
