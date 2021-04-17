import pytest
from htrc_features import Volume, MissingDataError, MissingFieldError
import htrc_features
import htrc_features.resolvers
import tempfile
import os
import pandas as pd
from pathlib import Path
project_root = Path(htrc_features.__file__).parent.parent

class TestParsing():

    def test_ef_json(self):
        # Tested elsewhere
        pass

    def test_bad_parser(self):
        ''' Tests if format mismatch from data raises error'''
        dir = os.path.join('tests', 'data', 'fullparquet')

        with pytest.raises(ValueError):
            # This tries to load the ID from
            vol = Volume(id = 'uc2.ark:/13960/t1xd0sc6x', format='json', dir = dir, id_resolver = "http")

    def test_full_parquet(self):
        dir = os.path.join('tests', 'data', 'fullparquet')
        vol = Volume(id = 'uc2.ark:/13960/t1xd0sc6x', format='parquet', dir = dir)
        assert vol.id == 'uc2.ark:/13960/t1xd0sc6x'
        assert type(vol.tokenlist()) is pd.core.frame.DataFrame
        assert type(vol.begin_line_chars()) is pd.core.frame.DataFrame
        assert type(vol.section_features(section='all')) is pd.core.frame.DataFrame
        
    def test_new_parquet(self):
        resolver1 = htrc_features.resolvers.LocalResolver(dir = Path(project_root, "tests", "data"), format = "json", compression = "bz2")
        with tempfile.TemporaryDirectory() as tempdir:
            resolver2 = htrc_features.resolvers.LocalResolver(dir = tempdir, format = "parquet", compression = "snappy")
            id = "aeu.ark:/13960/t1rf63t52"
            bz_vol = Volume(id, id_resolver = resolver1)
            parquet_vol = Volume(id, id_resolver = resolver2, mode = "wb")
            parquet_vol.write(bz_vol)
            print("BAAAAA", [*Path(tempdir).glob("*")])
            parquet_vol = Volume(id, id_resolver = resolver2)        
            parquet_vol.parser.parse()
            print("\n\n\nFOOOO\n\n\n\n", parquet_vol.parser.meta)
        
    def test_token_only_parquet(self):
        htid = 'uc2.ark:/13960/t1xd0sc6x'
        filepath = os.path.join('tests', 'data', 'justtokens')
        vol = Volume(id = htid, format='parquet', dir = filepath)

        # Should be inferred from path
        assert vol.id == 'uc2.ark:/13960/t1xd0sc6x'

        # Only basic metadata is inferred from ID
        with pytest.raises(KeyError):
            vol.parser.meta['language']
        with pytest.raises(AttributeError):
            vol.language

        assert type(vol.tokenlist()) is pd.core.frame.DataFrame

        for method in ['section_features', 'begin_line_chars']:
            with pytest.raises(MissingDataError):
                getattr(vol, method)()

    def test_token_only_parquet(self):
        htid = 'uc2.ark:/13960/t1xd0sc6x'
        filepath = os.path.join('tests', 'data', 'justtokens')
        vol = Volume(id = htid, format='parquet', dir = filepath, id_resolver = "local")

        # Should be inferred from path
        assert vol.id == 'uc2.ark:/13960/t1xd0sc6x'

        # Only basic metadata is inferred from ID
        with pytest.raises(KeyError):
            vol.parser.meta['language']
        with pytest.raises(AttributeError):
            vol.language

        assert type(vol.tokenlist()) is pd.core.frame.DataFrame

        for method in ['section_features', 'begin_line_chars']:
            with pytest.raises(MissingDataError):
                getattr(vol, method)()

    def test_meta_only_parquet(self):
        htid = 'uc2.ark:/13960/t1xd0sc6x'
        filepath = os.path.join('tests', 'data', 'justmeta')
        vol = Volume(htid, dir=filepath, format='parquet', id_resolver = "local")

        assert vol.id == 'uc2.ark:/13960/t1xd0sc6x'
        assert vol.language == 'eng'

        for method in ['section_features', 'tokenlist', 'begin_line_chars']:
            with pytest.raises(MissingDataError):
                getattr(vol, method)()

    def test_partial_parq_tokenlist(self):
        '''
        Test loading of tokenlists saved with less information. In this case,
        vol.save_parquet('tests/data/partialparq/',
                          token_kwargs=dict(case=False, pos=False, drop_section=False)
                        )
        '''
        htid = 'uc2.ark:/13960/t1xd0sc6x'
        dirpath = os.path.join('tests', 'data', 'partialparq')
        vol = Volume(id=htid, format='parquet', dir=dirpath)

        tl = vol.tokenlist(case=False, pos=False)
        assert tl.reset_index().columns.tolist() == ['page', 'lowercase', 'count']

        with pytest.raises(MissingFieldError):
            tl = vol.tokenlist(case=True, pos=False)

        with pytest.raises(MissingFieldError):
            tl = vol.tokenlist(case=False, pos=True)

        with pytest.raises(MissingFieldError):
            tl = vol.tokenlist(case=False, pos=False, section='header')


    def test_chunked_parq_tokenlist(self):
        htid = 'uc2.ark+=13960=t1xd0sc6x'
        dirpath = os.path.join('tests', 'data', 'chunkedparq')
        vol = Volume(id=htid, format='parquet', dir=dirpath)

        assert vol.tokenlist(case=False, pos=True).reset_index().columns.tolist() == ['chunk', 'section', 'lowercase', 'pos', 'count']
        assert vol.tokenlist(case=True, pos=False).reset_index().columns.tolist() == ['chunk', 'section', 'token', 'count']
        assert vol.tokenlist().reset_index().columns.tolist() == ['chunk', 'section', 'token', 'pos', 'count']
        assert vol.tokenlist(drop_section=True).reset_index().columns.tolist() == ['chunk', 'token', 'pos', 'count']
