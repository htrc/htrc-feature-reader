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
import pyarrow as pa

project_root = Path(htrc_features.__file__).parent.parent

class TestParsing():
    def test_arrow_metadata(self):
        with tempfile.TemporaryDirectory() as first_new_dir:
            resolver1 = htrc_features.resolvers.LocalResolver(dir = Path(project_root, "tests", "data"), format = "json", compression = "bz2")
            resolver2 = htrc_features.resolvers.LocalResolver(dir = first_new_dir, format = "parquet", compression = "snappy")
            id = "aeu.ark:/13960/t1rf63t52"

            copy_between_resolvers(id, resolver1, resolver2)
            f1 = Volume(id, id_resolver = resolver1)
            f2 = Volume(id, id_resolver = resolver2)
            assert(f2.title == f1.title)
            with pytest.raises(Exception):
                f2.parser._make_page_feature_df()

    def test_arrow_metadata_2(self):

        with tempfile.TemporaryDirectory() as first_new_dir:
            resolver1 = htrc_features.resolvers.LocalResolver(dir = Path(project_root, "tests", "data"), format = "json", compression = "bz2")
            resolver2 = htrc_features.resolvers.LocalResolver(dir = first_new_dir, format = "parquet", compression = "snappy")
            id = "hvd.hl112m"

            chain = resolvers.chain_resolver([
                {"method": "local", "dir" : first_new_dir,
                "format" : "parquet", "compression" : "brotli"},
                {"method": "stubbytree", "dir": project_root / "data/ef2-stubby",
                 "format" : "json", "compression" : "bz2"}
            ])

            f1 = Volume(id, id_resolver = chain)
            f2 = Volume(id, id_resolver = chain.fallback)
            assert(f2.title == f1.title)
            assert(f1.parser._make_page_feature_df().shape[0] == f2.parser._make_page_feature_df().shape[0])

    def test_arrow_export(self):
        """
        An elaborate trip.
        """
        with tempfile.TemporaryDirectory() as first_new_dir:
            resolver1 = htrc_features.resolvers.LocalResolver(dir = Path(project_root, "tests", "data"), format = "json", compression = "bz2")
            resolver2 = htrc_features.resolvers.LocalResolver(dir = first_new_dir, format = "parquet", compression = "snappy")
            id = "aeu.ark:/13960/t1rf63t52"

            copy_between_resolvers(id, resolver1, resolver2)
            f1 = Volume(id, id_resolver = resolver1)
            f2 = Volume(id, id_resolver = resolver2)

            arrow_1 = f1.arrow_counts(columns = ['count'])
            arrow_2 = f2.arrow_counts(columns = ['count'])
            assert(sum(arrow_1['count'].to_pylist()) == sum(arrow_1['count'].to_pylist()))
