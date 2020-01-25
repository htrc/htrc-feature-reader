import pytest
from htrc_features.resolvers import IdResolver
import htrc_features.resolvers as resolvers
from htrc_features import Volume
import htrc_features
import os
import pandas as pd
import tempfile
from pathlib import Path
import tempfile

project_root = Path(htrc_features.__file__).parent.parent

def copy_between_resolvers(id, resolver1, resolver2):
    input = Volume(id, id_resolver=resolver1)
    output = Volume(id, id_resolver=resolver2, mode = 'wb')
    output.write(input)

class TestParsing():
    def test_names(self):
        resolver2 = IdResolver(dir = ".", format = "parquet", compression = "snappy")

        # Don't use compression in the name
        testname = resolver2.fname("mdp.12345", format = "parquet", compression = "snappy", suffix = "tokens")
        assert(testname == "mdp.12345.tokens.parquet")
        
        testname = resolver2.fname("mdp.12345", format = "json", suffix = None, compression = 'gz')
        assert(testname == "mdp.12345.json.gz")

    def test_local_to_pairtree_to_parquet(self):
        """
        An elaborate trip.
        """
        with tempfile.TemporaryDirectory() as first_new_dir:
            with tempfile.TemporaryDirectory() as second_new_dir:
                resolver1 = htrc_features.resolvers.LocalResolver(dir = Path(project_root, "tests", "data"), format = "json", compression = "bz2")
                resolver2 = htrc_features.resolvers.PairtreeResolver(dir = first_new_dir,  format = "json", compression = "gz")
                resolver3 = htrc_features.resolvers.LocalResolver(dir = second_new_dir, format = "parquet", compression = "snappy")

                copy_between_resolvers("aeu.ark:/13960/t1rf63t52", resolver1, resolver2)
                copy_between_resolvers("aeu.ark:/13960/t1rf63t52", resolver2, resolver3)

                all_files = []
                for loc, dir, files in os.walk(first_new_dir):
                    for file in files:
                        all_files.append(os.path.join(loc, file))

                assert(len(all_files) == 1)
                assert(all_files[0].endswith("aeu/pairtree_root/ar/k+/=1/39/60/=t/1r/f6/3t/52/ark+=13960=t1rf63t52/aeu.ark+=13960=t1rf63t52.json.gz"))

                # Our test assertion ensures that the data has made it all the way through.
                assert(Volume("aeu.ark:/13960/t1rf63t52", id_resolver = resolver3).tokenlist()['count'].sum() == 97691)        
