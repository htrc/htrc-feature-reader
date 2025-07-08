import unittest
import os
import json
from io import BytesIO
from unittest.mock import patch, mock_open, MagicMock
from htrc_features import workset, feature_reader
from urllib.request import Request, urlopen



class TestWorksetModule(unittest.TestCase):

    def test_get_volumes(self):
        sample_data = {
            "@graph": [
                {
                    "@type": "http://wcsa.htrc.illinois.edu/Workset",
                    "http://www.europeana.eu/schemas/edm/gathers": [
                        {"@id": "http://hdl.handle.net/2027/mdp.123"},
                        {"@id": "http://hdl.handle.net/2027/mdp.456"}
                    ]
                }
            ]
        }
        vols = workset.get_volumes(sample_data)
        self.assertEqual(sorted(vols), ["mdp.123", "mdp.456"])

    def test_create_jsonld(self):
        vols = ["mdp.123", "mdp.456"]
        result = workset.create_jsonld(vols, title="Test", curator="Tester")
        self.assertIn("gathers", result)  # updated from full RDF key

    def test_get_volumes_from_csv(self):
        tsv_data = b"htitem_id\ntest.1\ntest.2\n"
        vols = workset.get_volumes_from_csv(tsv_data)
        self.assertEqual(vols, ["test.1", "test.2"])

    @patch("os.path.isdir", return_value=True)
    @patch("os.listdir", return_value=["vol1", "vol2", "ignore.log"])
    def test_path_to_volumes_directory(self, mock_listdir, mock_isdir):
        vols = workset.path_to_volumes("some_dir")
        self.assertEqual(sorted(vols), ["vol1", "vol2"])

    @patch("os.path.isfile", return_value=True)
    def test_path_to_volumes_file(self, mock_isfile):
        with patch("builtins.open", mock_open(read_data="vol1\nvol2\n")):
            vols = workset.path_to_volumes("some_file.txt")
            self.assertEqual(vols, ["vol1", "vol2"])

    @patch("builtins.open", new_callable=mock_open, read_data=json.dumps({
        "@graph": [
            {
                "@type": "http://wcsa.htrc.illinois.edu/Workset",
                "http://www.europeana.eu/schemas/edm/gathers": [
                    {"@id": "http://hdl.handle.net/2027/mdp.123"}
                ]
            }
        ]
    }))
    def test_load_json_file(self, mock_file):
        vols = workset.load("workset.json")
        self.assertEqual(vols, ["mdp.123"])

    @patch("htrc_features.workset.urlopen")  # corrected patch path
    def test_load_hathitrust_collection(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = b"htitem_id\ntest.123\n"
        mock_urlopen.return_value = mock_response
        vols = workset.load_hathitrust_collection(
            "https://babel.hathitrust.org/cgi/mb?a=listis&c=548413090")
        self.assertEqual(vols, ["test.123"])


    def test_load_collection_with_download_link(self):
        url = "https://babel.hathitrust.org/cgi/mb?a=download&c=1174943610&format=text"

        # Spoof a browser User-Agent
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/114.0"
        }
        req = Request(url, headers=headers)
        with urlopen(req) as response:
            tsv_data = response.read()

        # Parse TSV
        volids = workset.get_volumes_from_csv(tsv_data)

        self.assertGreater(len(volids), 0, "No volume IDs found in TSV.")
        print(f"✅ Successfully loaded {len(volids)} volume IDs.")
        print(f"📚 Sample: {volids[:5]}")

    



if __name__ == '__main__':
    unittest.main()
