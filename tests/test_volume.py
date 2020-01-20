import pytest
from htrc_features import FeatureReader, Volume, utils
import os
import htrc_features


# TestFeatureReader already tested loading, so we'll load a common volume
# rather than loading a file with each test. For those unfamiliar with pytest,
# scope='module' ensures that the fixture is run only once

@pytest.fixture(scope="module")
def paths():
    return [os.path.join('tests', 'data', 'green-gables-15pages.json')]

@pytest.fixture(scope="module")
def volume(paths):
    paths = paths[0]
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
        assert 2*tokenlist_times[0] > sum(tokenlist_times[1:])
        
    def test_direct_loading(self, paths):
        import time
        # Load new volume specifically for this test
        vol = Volume(paths[0], compressed=False)
        assert type(vol) == htrc_features.feature_reader.Volume
        
    def test_parquet_saving(self, volume, tmp_path):
        volume.save_parquet(tmp_path, meta=True, tokens=True, chars=True, section_features=True)
        files = os.listdir(tmp_path)
        for ext in ['meta.json', 'tokens.parquet', 'section.parquet', 'chars.parquet']:
            assert '{}.{}'.format(utils.clean_htid(volume.id), ext) in files

    def test_included_metadata(self, volume):
        import re
        metadata = {
          "handleUrl": "http://hdl.handle.net/2027/uc2.ark:/13960/t1xd0sc6x",
          "htBibUrl": "http://catalog.hathitrust.org/api/volumes/full/htid/uc2.ark:/13960/t1xd0sc6x.json",
          "names": [
            "Montgomery, L. M. (Lucy Maud) 1874-1942 "
          ],
          "classification": {},
          "typeOfResource": "text",
          "issuance": "monographic",
          "genre": [],
          "bibliographicFormat": "BK",
          "language": "eng",
          "pubPlace": "onc",
          "pubDate": "1908",
          "governmentDocument": False,
          "sourceInstitution": "UC",
          "enumerationChronology": " ",
          "hathitrustRecordNumber": "7668057",
          "rightsAttributes": "pdus",
          "accessProfile": "open",
          "volumeIdentifier": "uc2.ark:/13960/t1xd0sc6x",
          "dateCreated": "2016-06-19T02:14:20.7051367Z",
          "schemaVersion": "1.3",
          "sourceInstitutionRecordNumber": "2480325",
          "oclc": [
            "320127250"
          ],
          "isbn": [],
          "issn": [],
          "lccn": [],
          "title": "Anne of Green Gables / L.M. Montgomery.",
          "imprint": "Ryerson Press, c1908 by L.C. Page.",
          "lastUpdateDate": "2010-04-29 20:31:43",
          "pageCount": 414
        }

        for CapitalCaseKey in metadata:
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', CapitalCaseKey)
            camel_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
            assert getattr(volume, camel_case) == metadata[CapitalCaseKey]

        # Syntactic sugar fields
        assert volume.year == metadata['pubDate']
        assert volume.author == metadata["names"]

        # Field from vol['features']
        assert volume.page_count == metadata['pageCount']

    def test_metadata_api(self, volume):
        import pymarc
        # For now, test for a valid response.
        metadata = volume.metadata
        assert type(metadata) == pymarc.record.Record

    def test_token_stats(self, volume):
        assert volume.tokens()[:600:100] == ['/', 'her', 'but', "'re",
                                             'announced', 'entirely']

    def test_line_counting(self, volume):
        assert sum(volume.line_counts()) == 441
        assert sum(volume.empty_line_counts()) == 92
        assert sum(volume.sentence_counts()) == 191
        
    def test_char_counts(self, volume):
        begin_characters = volume.begin_line_chars()
        assert(begin_characters.loc[(3, 'body', 'begin', '/'),].values[0] == 1)
        assert(begin_characters.groupby(level='char').sum().loc['a'].values[0] == 22)
        
        end_characters = volume.end_line_chars()
        assert(end_characters.loc[(3, 'body', 'end', '3'), ].values[0] == 1)
        assert(end_characters.groupby(level='char').sum().loc['.'].values[0] == 46)

    def test_cap_alpha_seq(self, volume):
        assert sum(volume.cap_alpha_seqs()) == 35

    def test_token_per_page_counts(self, volume):
        import pandas
        # Test default settings
        tokencounts1 = volume.tokens_per_page()
        counts1 = {2: 0, 3: 2, 4: 4, 5: 4, 6: 63, 51: 229, 52: 298,
                   53: 344, 54: 316, 55: 295, 56: 327, 57: 383, 58: 341,
                   59: 187, 60: 277, 61: 341}
        assert type(tokencounts1) == pandas.core.frame.Series
        assert tokencounts1.to_dict() == counts1

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
        
    def test_big_pages(self):
        ''' Test a document with *many* tokens per page. '''
        path = os.path.join('tests', 'data', 'aeu.ark+=13960=t1rf63t52.json.bz2')
        feature_reader = FeatureReader(path)
        volume = feature_reader.first()
        tokenlist = volume.tokenlist()
        assert tokenlist.shape[0] == 56397