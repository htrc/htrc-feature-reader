import pytest
from htrc_features import Volume, MissingDataError, MissingFieldError
import os
import pandas as pd

class TestParsing():

    def test_ef_json(self):
        # Tested elsewhere
        pass

    def test_bad_parser(self):
        ''' Tests if format mismatch from data raises error'''
        dir = os.path.join('tests', 'data', 'fullparquet')
        
        with pytest.raises(Exception):
            vol = Volume(id = 'uc2.ark:/13960/t1xd0sc6x', format='json', dir = dir)
        
    def test_full_parquet(self):
        dir = os.path.join('tests', 'data', 'fullparquet')
        vol = Volume(id = 'uc2.ark:/13960/t1xd0sc6x', parser='parquet', dir = dir)
        assert vol.id == 'uc2.ark:/13960/t1xd0sc6x'
        assert type(vol.tokenlist()) is pd.core.frame.DataFrame
        assert type(vol.begin_line_chars()) is pd.core.frame.DataFrame
        assert type(vol.section_features(section='all')) is pd.core.frame.DataFrame
    
    def dont_test_token_only_parquet(self):
        htid = 'uc2.ark:/13960/t1xd0sc6x'
        filepath = os.path.join('tests', 'data', 'justtokens')
        vol = Volume(id = htid, parser='parquet', dir = filepath)
        
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

    def dont_test_meta_only_parquet(self):
        htid = 'uc2.ark+=13960=t1xd0sc6x'
        filepath = os.path.join('tests', 'data', 'justmeta', htid)
        vol = Volume(filepath, parser='parquet')
        
        assert vol.id == 'uc2.ark:/13960/t1xd0sc6x'
        assert vol.language == 'eng'
        
        for method in ['section_features', 'tokenlist', 'begin_line_chars']:
            with pytest.raises(MissingDataError):
                getattr(vol, method)()                
    
    def test_partial_parq_tokenlist(self):
        htid = 'uc2.ark+=13960=t1xd0sc6x'
        filepath = os.path.join('tests', 'data', 'partialparq', htid)
        vol = Volume(filepath, parser='parquet')
 
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
        filepath = os.path.join('tests', 'data', 'chunkedparq', htid)
        vol = Volume(filepath, parser='parquet')

        assert vol.tokenlist(case=False, pos=True).reset_index().columns.tolist() == ['chunk', 'section', 'lowercase', 'pos', 'count']
        assert vol.tokenlist(case=True, pos=False).reset_index().columns.tolist() == ['chunk', 'section', 'token', 'count']
        assert vol.tokenlist().reset_index().columns.tolist() == ['chunk', 'section', 'token', 'pos', 'count']
        assert vol.tokenlist(drop_section=True).reset_index().columns.tolist() == ['chunk', 'token', 'pos', 'count']

# Allow compression formats.

compressions = {
    'parquet': ['snappy', None, 'gz'],
    'json': ['bz2', None, 'gz']
}

for format in ['parquet', 'json']:
    for compression in compressions[format]:
        pass

def build_unit_test(format, compression):
    def my_test(self):
        p = Volume(id = "aeu.ark:/13960/t1rf63t52", format = "json", compression = "bz2")
        
