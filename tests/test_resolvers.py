import pytest
from htrc_features.resolvers import IdResolver
import htrc_features.resolvers as resolvers
from htrc_features.caching import copy_between_resolvers
from htrc_features import Volume
import htrc_features.resolvers
import htrc_features
import os
import pandas as pd
import tempfile
from pathlib import Path
import tempfile
import pyarrow

project_root = Path(htrc_features.__file__).parent.parent

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

    def test_parquet_snappy_resolvers_PairtreeResolver_resolution(self):
        self.combo_test("parquet", resolvers.PairtreeResolver, "snappy")

    def test_parquet_snappy_resolvers_ZiptreeResolver_resolution(self):
        self.combo_test("parquet", resolvers.ZiptreeResolver, "snappy")

    def test_parquet_snappy_resolvers_LocalResolver_resolution(self):
        self.combo_test("parquet", resolvers.LocalResolver, "snappy")

    def test_parquet_gz_resolvers_PairtreeResolver_resolution(self):
        self.combo_test("parquet", resolvers.PairtreeResolver, "gzip")

    # If you pass a bad compression format, pandas silently
    # writes with no compression even though pyarrow raises an exception.
    # Hmmm.
#    def test_parquet_gz_resolvers_PairtreeResolver_resolution(self):
#        with pytest.raises(pyarrow.lib.ArrowException):
#            self.combo_test("parquet", resolvers.PairtreeResolver, "gz")

    def test_parquet_gz_resolvers_ZiptreeResolver_resolution(self):
        self.combo_test("parquet", resolvers.ZiptreeResolver, "gzip")

    def test_parquet_gz_resolvers_LocalResolver_resolution(self):
        self.combo_test("parquet", resolvers.LocalResolver, "gzip")

    def test_json_gz_resolvers_PairtreeResolver_resolution(self):
        self.combo_test("json", resolvers.PairtreeResolver, "gz")

    def test_json_gz_resolvers_ZiptreeResolver_resolution(self):
        self.combo_test("json", resolvers.ZiptreeResolver, "gz")


    def test_json_gz_resolvers_LocalResolver_resolution(self):
        self.combo_test("json", resolvers.LocalResolver, "gz")


    def test_json_bz2_resolvers_PairtreeResolver_resolution(self):
        self.combo_test("json", resolvers.PairtreeResolver, "bz2")


    def test_json_bz2_resolvers_ZiptreeResolver_resolution(self):
        self.combo_test("json", resolvers.ZiptreeResolver, "bz2")


    def test_json_bz2_resolvers_LocalResolver_resolution(self):
        self.combo_test("json", resolvers.LocalResolver, "bz2")


    def test_json_None_resolvers_PairtreeResolver_resolution(self):
        self.combo_test("json", resolvers.PairtreeResolver, None)


    def test_json_None_resolvers_ZiptreeResolver_resolution(self):
        self.combo_test("json", resolvers.ZiptreeResolver, None)

    def test_json_None_resolvers_LocalResolver_resolution(self):
        self.combo_test("json", resolvers.LocalResolver, None)

    def combo_test(self, format, resolver, compression):
        id = "aeu.ark:/13960/t1rf63t52"
        print(format, resolver, compression)
        basic_resolver = resolvers.LocalResolver(dir = "tests/data", format="json", compression="bz2")
        with tempfile.TemporaryDirectory() as tempdir:
            testing_resolver_write = resolver(dir = tempdir, format = format, compression = compression)
            copy_between_resolvers(id, basic_resolver, testing_resolver_write)

            # Test read on a freshly made resolver just in case there's entanglement
            testing_resolver_read = resolver(dir = tempdir, format = format, compression = compression)
            assert(Volume(id, id_resolver = testing_resolver_read).tokenlist()['count'].sum() == 97691)
