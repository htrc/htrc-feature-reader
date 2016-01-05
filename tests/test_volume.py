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
