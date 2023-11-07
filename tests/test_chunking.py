from htrc_features.transformations import chunk_ends, chunk_even, chunk_last
from htrc_features.resolvers import LocalResolver
from htrc_features import Volume
from pathlib import Path
import random
import numpy as np
from collections import Counter
import pandas as pd

class TestChunking():

    def test_write_to_chunked_parquet_sectionless(self, tmpdir):
        dir = "tests/data"
        vol_in = Volume(id='aeu.ark:/13960/t1rf63t52', dir = str(dir), id_resolver = 'local')
        output = Volume(id='foo.123', format = 'parquet', mode = 'wb', id_resolver='local', dir = tmpdir )
        output.write(vol_in, token_kwargs = {"chunk": True,"drop_section": True, "pos":False})

        read = pd.read_parquet(Path(tmpdir, "foo.123.tokens.parquet")).reset_index()
        assert("chunk" in read.columns)

    def test_write_to_chunked_parquet(self, tmpdir):
        dir = "tests/data"
        vol_in = Volume(id='aeu.ark:/13960/t1rf63t52', dir = str(dir), id_resolver = 'local')
        output = Volume(id='foo.123', dir = tmpdir, format = 'parquet', mode = 'wb')
        output.write(vol_in, token_kwargs = {"chunk": True})
        read = pd.read_parquet(Path(tmpdir, "foo.123.tokens.parquet")).reset_index()
        assert("chunk" in read.columns)

    def test_even_chunking(self):
        # All methods should solve it when pages are only one thing long.
        for method in chunk_ends, chunk_even, chunk_last:
            test_counts = np.ones(1000)
            target = 100
            c = Counter()
            for chunk, count in zip(method(test_counts, target), test_counts):
                c[chunk] += count
            assert(Counter(c.values()) == Counter({100:10}))

    def test_assymetric_chunking_end(self):
        # Previously this caused an infinite loop.
        for method in chunk_ends, chunk_even, chunk_last:
            test_counts = np.ones(1000)
            test_counts[-1] = 500
            target = 100
            c = Counter()
            for chunk, count in zip(method(test_counts, target), test_counts):
                c[chunk] += count

            assert(np.max([*c.values()]) == 500)
            assert(np.min([*c.values()]) == 99)

    def test_assymetric_chunking_middle(self):
        # in cases with page lengths like [1, 500, 2] the
        # outer chunks may try to eat the inner chunk at the same
        # time. The test assertion here is unimportant: the
        # goal is really not to raise an error.
        for method in chunk_ends, chunk_even, chunk_last:
            test_counts = np.ones(1000)
            test_counts[500] = 500
            target = 100
            c = Counter()
            for chunk, count in zip(method(test_counts, target), test_counts):
                c[chunk] += count

            assert(np.max([*c.values()]) <= 501)

    def test_tiny_chunk_size(self):
        # What if the chunk size is much smaller than any page?
        # The only reasonable response is
        for method in chunk_ends, chunk_even, chunk_last:
            test_counts = np.array([500] * 10)
            target = 100
            c = Counter()
            for chunk, count in zip(method(test_counts, target), test_counts):
                c[chunk] += count

            assert(np.max([*c.values()]) == 500)
