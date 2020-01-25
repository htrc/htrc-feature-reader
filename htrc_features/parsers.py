from __future__ import unicode_literals

import sys
PY3 = (sys.version_info[0] >= 3)

import warnings
import logging
import pandas as pd
import numpy as np
import pymarc
from six import iteritems, StringIO, BytesIO
import codecs
import os
import types

from htrc_features import utils, resolvers

try:
    import ujson as json
except ImportError:
    import json
import requests


import htrc_features.resolvers

import bz2

resolver_nicknames = {
    "path": resolvers.PathResolver,
    "pairtree": resolvers.PairtreeResolver,
    "ziptree": resolvers.ZiptreeResolver,
    "local": resolvers.LocalResolver,
    "http": resolvers.HttpResolver
}


SECREF = ['header', 'body', 'footer']

class MissingDataError(Exception):
    pass


class BaseFileHandler(object):
    
    def __init__(self, id = None, id_resolver = None, **kwargs):
        '''
        Base class for volume reading.
        '''

        self.meta = dict(id=id)        

        self.args = kwargs
        
        self.compression = kwargs.get('compression', 'default')
        
        if self.compression == 'default':
            if isinstance(id_resolver, resolvers.IdResolver):
                # First choice; inherit from the resolver.
                compression = id_resolver.compression
            elif self.format == "json":
                # bz2 default for local files, 'json' for remote.
                if id_resolver == 'http':
                    self.compression = None
                elif isinstance(id_resolver, resolvers.HttpResolver):
                    self.compression = None
                else:
                    self.compression = "bz2"
            elif self.format == 'parquet':
                self.compression = "snappy"
            else:
                raise
            
        kwargs['compression'] = self.compression

        
        self.resolver = self._init_resolver(id_resolver, **kwargs)
        
        if 'load' in self.args and self.args['load'] == False:
            return
        
        if 'mode' in self.args:
            """
            Not documented yet, but allow 'mode' = 'create'.
            """
            mode = self.args['mode']
            assert( mode in ['wb', 'rb'] )
            self.mode = mode
            if mode.startswith('w'):
                # No need to parse in write mode.
                return
        
        self.parse(**kwargs)
        
    def _init_resolver(self, id_resolver, format=None, **kwargs):
        if not format:
            format = self.format
            
        if isinstance(id_resolver, resolvers.IdResolver):
            return id_resolver
            
        elif isinstance(id_resolver, types.FunctionType):
            """
            Any arbitrary function can be made into a resolver
            by turning it into a class
            """
            logging.debug("Building class to handle retrieval")
            
            class Dummy():
                def __init__(self):
                    pass
                def open(self, **kwargs):
                    return id_resolver(**kwargs)
                
            return Dummy()
            
        else:
            try:
                return resolver_nicknames[id_resolver](format = format, **kwargs)
            except KeyError:
                raise TypeError("""Id resolver must be a function, htrc_features.IdResolver, or
                one of the strings {}""".format(", ".join(list(resolver_nicknames.keys()))))

    def parse(self, **kwargs):
        '''
        Save any info that needs to be held at init time for
        parsing. In some cases, little needs to be saved before methods
        like _make_tokencount_df need to be run.
        '''
        pass
    
    def _make_tokencount_df(self):
        pass
    
    def _make_line_char_df(self):
        pass
    
    def _make_section_feature_df(self):
        pass
    
    def _make_page_feature_df(self):
        pass
    
    def _parse_meta(self):
        pass
        
        
class JsonFileHandler(BaseFileHandler):
    SUPPORTED_SCHEMA = ['3.0']
    METADATA_FIELDS = [('schemaVersion', 'schema_version'), ('dateCreated', 'date_created'),
                       ('title', 'title'), ('pubDate', 'pub_date'), ('language', 'language'),
                       ('htBibUrl', 'ht_bib_url'), ('handleUrl', 'handle_url'),
                       ('oclc', 'oclc'), ('imprint', 'imprint'), ('names', 'names'),
                       ('classification', 'classification'),
                       ('typeOfResource', 'type_of_resource'), ('issuance', 'issuance'),
                       ('genre', 'genre'), ("bibliographicFormat", "bibliographic_format"),
                       ("pubPlace", "pub_place"), ("governmentDocument", "government_document"),
                       ("sourceInstitution", "source_institution"),
                       ("enumerationChronology", "enumeration_chronology"),
                       ("hathitrustRecordNumber", "hathitrust_record_number"),
                       ("rightsAttributes", "rights_attributes"),
                       ("accessProfile", "access_profile"),
                       ("volumeIdentifier", "volume_identifier"),
                       ("sourceInstitutionRecordNumber", "source_institution_record_number"),
                       ("isbn", "isbn"), ("issn", "issn"), ("lccn", "lccn"),
                       ("lastUpdateDate", "last_update_date")
                      ]
    ''' List of metadata fields, with their pythonic name mapping. '''
    
    BASIC_FIELDS = [('pageCount', 'page_count')]
    ''' List of fields which return primitive values in the schema, as tuples
    with (CamelCase, lower_with_under) mapping.
    '''
    
    PAGE_FIELDS =  ['seq', 'languages']
    SECTION_FIELDS =  ['tokenCount', 'lineCount', 'emptyLineCount',
                             'capAlphaSeq', 'sentenceCount']

    
    def __init__(self, id, id_resolver, compression = 'default', **kwargs):

        self.meta = dict(id=None, handle_url=None)
        self.format = "json"
        self.id = id
        self._schema = None
        self._pages = None


        # parsing and reading are called here.
        super().__init__(id, id_resolver = id_resolver, compression = compression, **kwargs)

    def write(self, outside_volume, **kwargs):

        
        # Look for a mode flag in three places: the passed args,
        # the parser, and the parser's resolver. This is to ensure
        # that a user actually typed 'wb' *somewhere* before doing
        # anything destructive.
        
        if not 'mode' in kwargs:
            try:
                kwargs['mode'] = self.mode
            except AttributeError:
                kwargs['mode'] = self.resolver.mode

        
        compression = kwargs.get("compression", self.compression)

        if compression == "default":
            compression = "bz2"
            
        if outside_volume.parser.format != "json":
            raise TypeError("Can only write to json from other json, because"
                            "data is lost in creating the parquet files.")

        if outside_volume.parser.compression == self.compression:
            if self.compression is not "default":
                skip_compression = True
            else:
                skip_compression = False
        else:
            skip_compression = False
            
        json_bytestring = outside_volume.parser._parse_json(object = False, skip_compression = skip_compression)
        
        with self.resolver.open(self.id, compression = self.compression, format = 'json',
                                skip_compression = skip_compression,
                                **kwargs) as fout:
            fout.write(json_bytestring)
    
    def read(self, **kwargs):
        '''
        Load JSON for a path. Allows remote files in addition to local ones. 
        Returns: string of json.
        '''
        pass
    
    def _parse_json(self, **kwargs):
        id = self.id
        resolver = self.resolver
        if not "compression" in kwargs:
            kwargs['compression'] = self.compression
        
        for k in self.args:
            if not k in kwargs:
                kwargs[k] = self.args[k]
        with resolver.open(id, **kwargs) as fin:
            rawjson = fin.read()

            if "object" in kwargs and kwargs['object'] == False:
                return rawjson

            if isinstance(rawjson, BytesIO):
                rawjson = rawjson.decode()
            return json.loads(rawjson)
        
    def parse(self, **kwargs):
        
        obj = self._parse_json()

        self._schema = obj['features']['schemaVersion']
        if self._schema not in self.SUPPORTED_SCHEMA:
            logging.warning('Schema version of imported (%s) file does not match '
                         'the supported versions (%s). Update your files or use an older '
                         'version of the library' %
                         (obj['features']['schemaVersion'],
                          self.SUPPORTED_SCHEMA))
            
        self._pages = obj['features']['pages']
        
        # Anything in self.meta becomes an attribute in the volume
        self.meta = dict(id=obj['id'])
        
        # Expand basic values to properties
        for key, pythonkey in self.METADATA_FIELDS:
            if key in obj['metadata']:
                self.meta[pythonkey] = obj['metadata'][key]
        for key, pythonkey in self.BASIC_FIELDS:
            if key in obj['features']:
                self.meta[pythonkey] = obj['features'][key]
        
        if 'language' in self.meta:
            if (self._schema in ['2.0', '3.0']) and (self.meta['language'] in ['jpn', 'chi']):
                logging.warning("This version of the EF dataset has a tokenization bug "
                            "for Chinese and Japanese. See " "https://wiki.htrc.illinois.edu/display/COM/Extracted+Features+Dataset#ExtractedFeaturesDataset-issues")
        
        # TODO collect while iterating earlier
        self.seqs = [int(page['seq']) for page in self._pages]
    
    def _parse_meta(self):
        pass
    
    def _make_page_feature_df(self):
        # Parse basic page features
        # saves a DF to self.page_features where the index is the seq
        # number and the columns are the values of PAGE_FIELDS
        page_features = pd.DataFrame([{k:v for k,v in page.items() 
                                   if k in self.PAGE_FIELDS} 
                                  for page in self._pages])
        page_features['seq'] = pd.to_numeric(page_features['seq'])
        page_features = page_features.rename(columns={'seq':'page'})
        return page_features.set_index('page')
        
    def _make_section_feature_df(self):
        # Parse non-token section-specific features
        # saves a DF to self.section_features where the index is
        # (seq, section) and the columns are the values of
        # section_feature_list
        collector = []
        for page in self._pages:
            for sec in SECREF:
                row = { feat: page[sec][feat] 
                       for feat in self.SECTION_FIELDS }
                row['page'] = int(page['seq'])
                row['section'] = sec
                collector.append(row)
        return pd.DataFrame(collector).set_index(['page', 'section'])
    
    @property
    def token_freqs(self):
        ''' Returns a dataframe of page / section /count '''
        if not hasattr(self, "_token_freqs"):
            d = [{'page': int(page['seq']), 'section': sec,
                 'count':page[sec]['tokenCount']} for page 
                 in self._pages for sec in SECREF]
            self._token_freqs = pd.DataFrame(d).set_index(['page', 'section']).sort_index()
        return self._token_freqs
        
    def _make_tokencount_df(self, pages=False):
        '''
        Returns a Pandas dataframe of:
            page / section / place(i.e. begin/end) / char / count
            
        If no page JSON is provided, internal representation will be used.
        '''
        if not pages:
            pages = self._pages

        tname = 'tokenPosCount'

        # Make structured numpy array
        # Because it is typed, this approach is ~40x faster than earlier
        # methods
        m = sum([page['tokenCount'] for page in pages])
        arr = np.zeros(m, dtype=[(str('page'), str('u8')),
                                 (str('section'), str('U6')),
                                 (str('token'), str('U64')),
                                 (str('pos'), str('U6')),
                                 (str('count'), str('u4'))])
        i = 0
        for page in pages:
            for sec in ['header', 'body', 'footer']:
                for token, posvalues in iteritems(page[sec][tname]):
                    for pos, value in iteritems(posvalues):
                        arr[i] = (page['seq'], sec, token, pos, value)
                        i += 1
                        if (i > m+1):
                            logging.error("This volume has more token info "
                                          "than the internal representation "
                                          "allows. Email organisciak@gmail.com"
                                          "to let the library author know!")

        # Create a DataFrame
        df = pd.DataFrame(arr[:i]).set_index(['page', 'section',
                                              'token', 'pos'])
        df.sort_index(inplace=True, level=0, sort_remaining=True)
        return df
            
    def _make_line_char_df(self, pages=False):
        '''
        Returns a Pandas dataframe of:
            page / section / place(i.e. begin/end) / char / count

        Provide an array of pages that hold beginLineChars and endLineChars.
        '''
        
        # Default to using the internal _pages json, but allow for
        # Parsing function to be used independently
        if not pages:
            pages = self._pages

        if self._schema == '3.0':
            logging.warning("Adapted to erroneous key names in schema 3.0.")
            place_key = [('begin', 'beginCharCounts'), ('end', 'endCharCount')]
        else:
            place_key = [('begin', 'beginLineChars'), ('end', 'endLineChars')]
           
        
        # Make structured numpy array
        # Because it is typed, this approach is ~40x faster than earlier
        # methods
        m = len(pages) * 3 * 2  # Pages * section types * places
        arr = np.zeros(int(m*100), dtype=[(str('page'), str('u8')),
                                          (str('section'), str('U6')),
                                          (str('place'), str('U5')),
                                          (str('char'), str('U1')),
                                          (str('count'), str('u8'))])
        i = 0
        for page in pages:
            for sec in ['header', 'body', 'footer']:
                for place, json_key in  place_key:
                    for char, value in iteritems(page[sec][json_key]):
                        arr[i] = (page['seq'], sec, place, char, value)
                        i += 1

        # Create a DataFrame
        df = pd.DataFrame(arr[:i]).set_index(['page', 'section',
                                              'place', 'char'])
        df.sort_index(0, inplace=True)
        return df
    
class ParquetFileHandler(BaseFileHandler):
    '''
        This Volume parser allows for Feature Reader data to be loaded from a
        parquet format, which allows JSON decompression, parsing, and processing
        to be avoided.
        
        The FeatureReader files that can be loaded are the same ones used internally:
        metadata (as JSON), token counts, line character counts, and
        page+section level features. The non-section specific features aren't supported.
        
        These are essentially what is held internally in a Volume (vol.parser.meta,
        vol._tokencounts, vol._line_chars, vol._section_features) so 
        this parser doesn't provide much fanciness beyond loading.
        
        TO LOAD DATA
        The loading enforces a filename convention, the path provided
        to the parser should avoid the file extension. The filename convention is
            - ../{htid}.meta.json
            - ../{htid}.tokens.parquet
            - ../{htid}.chars.parquet
            - ../{htid}.section.parquet
      
    '''
        
    
    def __init__(self, id, id_resolver, compression="default", mode = 'rb', **kwargs):

        self.format = "parquet"
        self.id = id
        self.compression = compression
        self.resolver = id_resolver
        self.mode = mode
        
        super().__init__(id = id, id_resolver = id_resolver, compression = compression, **kwargs)
    
    def parse(self, **kwargs):
        
        try:
            with self.resolver.open(self.id, suffix = "meta", format = "json", compression = None) as meta_buffer:
                self.meta = json.loads(meta_buffer.read().decode("utf-8"))
        except:

            self.meta = dict(id=self.id, title=self.id)

            
        if not self.meta['id']:
            self.meta['id'] = htrc_features.utils.extract_htid(self.id)
        
        if not 'handle_url' in self.meta or not self.meta['handle_url']:
            self.meta['handle_url'] = "http://hdl.handle.net/2027/%s" % self.meta['id']
            
        if not 'title' in self.meta or not self.meta['title']:
            self.meta['title'] = self.meta['id']

    def write(self, volume, meta=True, tokens=True, section_features=False, chars=False, **kwargs):
        '''

        Save the internal representations of feature data to parquet, and the metadata to json,
        using the naming convention used by ParquetFileHandler.
        
        The primary use is for converting the feature files to something more efficient. By default,
        only metadata and tokencounts are saved.
        
        'volume' is an object of the 'Volume' class which will be used for data.

        Saving page features is currently unsupported, as it's an ill-fit for parquet. This is currently
        just the language-inferences for each page - everything else is in section features 
        (page by body/header/footer).
        
        Since Volumes partially support incomplete dataframes, you can pass Volume.tokenlist arguments
        as a dict with token_kwargs. For example, if you want to save a representation with only body
        information, drop the 'section' level of the index, and fold part-of-speech counts, you can pass
        token_kwargs=dict(section='body', drop_section=True, pos=False).
        '''


        # Look for a mode flag in three places: the passed args,
        # the parser, and the parser's resolver. This is to ensure
        # that a user actually typed 'wb' *somewhere* before writing.
        
        if not 'mode' in kwargs:
            try:
                kwargs['mode'] = self.mode
            except AttributeError:
                kwargs['mode'] = self.resolver.mode

        indexed = kwargs.get("indexed", True)

        compression = kwargs.get('compression', self.compression)
        
        if compression == "default":
            compression = "snappy"
        
        if meta:
            metastring = BytesIO(json.dumps(volume.parser.meta).encode("utf-8"))
            with self.resolver.open(self.id, format = "json", compression = None, suffix = 'meta', **kwargs) as fout:
                fout.write(metastring.read())
        
        if tokens:
            feats = volume.tokenlist()
            if not indexed:
                feats = feats.reset_index()
            if not feats.empty:
                with self.resolver.open(id = self.id, suffix = 'tokens', **kwargs) as fout:
                    feats.to_parquet(fout, compression=compression)                
                
        if section_features:
            feats = volume.section_features(section='all')
            if not feats.empty:
                with self.resolver.open(id = self.id, suffix = 'section', **kwargs) as fout:
                    feats.to_parquet(fout, compression=compression)
            
        if chars:
            feats = volume.line_chars()
            if not feats.empty:
                with self.resolver.open(id = self.id, suffix = 'chars', **kwargs) as fout:
                    feats.to_parquet(fout, compression=compression)

    def _make_tokencount_df(self):
        try:
            with self.resolver.open(id = self.id, suffix = 'tokens', format = 'parquet') as fin:
                df = pd.read_parquet(fin)
        except IOError:
            raise MissingDataError("No token information available")
            
        indcols = [col for col in ['page', 'section', 'token', 'lowercase', 'pos'] if col in df.columns]
        if len(indcols):
            df = df.set_index(indcols)
        return df
    
    def _make_line_char_df(self):
        try:
            with self.resolver.open(id = self.id, suffix = 'chars', format = 'parquet') as fin:
                df = pd.read_parquet(fin)
                return df 
        except IOError:
            raise MissingDataError("No line char information available")
        
    def _make_section_feature_df(self):
        try:
            with self.resolver.open(id = self.id, suffix = 'section', format = 'parquet') as fin:
                df = pd.read_parquet(fin)
            return df 
        except IOError:
            raise MissingDataError("No section information available")
        
    def _make_page_feature_df(self):
        raise Exception("parquet parser doesn't support non-token, non-section page features")

    def _parse_meta(self):
        pass
    
