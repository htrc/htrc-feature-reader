from __future__ import unicode_literals

import warnings
import logging
import pandas as pd
import numpy as np
import pymarc
from six import iteritems, StringIO, BytesIO
import codecs
import os
import types
import bz2

try:
    import rapidjson as json
except ImportError:
    import json

import requests

from . import utils, resolvers
from .resolvers import resolver_nicknames

SECREF = ['header', 'body', 'footer']

class MissingDataError(Exception):
    pass

class BaseFileHandler(object):
    
    def __init__(self, id = None, id_resolver = None, mode = 'rb', **kwargs):
        '''
        Base class for volume reading.
        '''

        self.id = id
        self.meta = dict(id=id)        

        self.args = kwargs
        self.mode = mode

        # This needs to happen somehwere before here.
        assert(isinstance(id_resolver, resolvers.IdResolver))
        
        # Handle default compression
        """
        if self.compression == 'default':
            if isinstance(id_resolver, resolvers.IdResolver):
                # First choice; inherit from the resolver.
                self.compression = id_resolver.compression
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
        
        self.resolver = self._init_resolver(id_resolver, compression=self.compression, **kwargs)
        """

        self.id_resolver = id_resolver
#        self.compression = id_resolver.compression
        
        if 'load' in self.args and self.args['load'] == False:
            return
        
        
        assert( mode in ['wb', 'rb'] )
        if mode.startswith('w'):
            # No need to parse in write mode.
            return
        
        self.parse(**kwargs)
        
    def __init_resolver(self, id_resolver, format=None, **kwargs):
        """
        DELETE
        """
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
            
        elif isinstance(id_resolver, str):
            try:
                return resolver_nicknames[id_resolver](format = format, **kwargs)
            except KeyError:
                raise TypeError("""Id resolver strings must be
                one of the strings {}""".format(", ".join(list(resolver_nicknames.keys()))))
        raise TypeError("""Id resolver must be a function, htrc_features.IdResolver, or
        one of the strings {}. If you think you did it correctly, it's possible you're using
                reloading code with a relative import.""".format(", ".join(list(resolver_nicknames.keys()))))            

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
    SUPPORTED_SCHEMA = ['3.0', 'https://schemas.hathitrust.org/EF_Schema_FeaturesSubSchema_v_3.0']

    ''' List of metadata fields, with their pythonic name mapping. Intent is to be explicit here,
    for safety because metadata gets mapped to attributes in the Volume.
    '''
    METADATA_FIELDS = [('schemaVersion', 'metadata_schema_version'),
                       ("enumerationChronology", "enumeration_chronology"),
                       ('typeOfResource', 'type_of_resource'), ('title', 'title'),
                       ('dateCreated', 'date_created'), ('pubDate', 'pub_date'), 
                       ('language', 'language'), ("accessProfile", "access_profile"),
                       ("isbn", "isbn"), ("issn", "issn"), ("lccn", "lccn"), ('oclc', 'oclc'),
                       ('features.pageCount', 'page_count'), ("features.schemaVersion", "feature_schema_version")
                      ]
    
    METADATA_FIELDS_1_3 = [('htBibUrl', 'ht_bib_url'), ('genre', 'genre'), ('handleUrl', 'handle_url'),
                          ('imprint', 'imprint'), ('names', 'names'), ('.id', 'id'),
                          ("sourceInstitution", "source_institution"),
                          ('classification', 'classification'), ('issuance', 'issuance'),
                          ("bibliographicFormat", "bibliographic_format"),
                          ("governmentDocument", "government_document"),
                          ("hathitrustRecordNumber", "hathitrust_record_number"),
                          ("rightsAttributes", "rights_attributes"), ("pubPlace", "pub_place"),
                          ("volumeIdentifier", "volume_identifier"),
                          ("sourceInstitutionRecordNumber", "source_institution_record_number"),
                          ("lastUpdateDate", "last_update_date")
                         ]
    
    METADATA_FIELDS_3_0 = [('accessRights', 'access_rights'), ('alternateTitle','alternate_title'), 
                           ('category','category'), ('genre', 'genre_ld'), ('contributor','contributor_ld'), ('.htid', 'id'),
                           ('id','handle_url'), ("sourceInstitution", "source_institution_ld"),
                           ('lcc','lcc'), ('type', 'type'), ('isPartOf','is_part_of'), 
                           ('lastRightsUpdateDate','last_rights_update_date'),
                           ("pubPlace", "pub_place_ld"),
                           ('mainEntityOfPage','main_entity_of_page'), ('publisher','publisher_ld')
                     ]
    
    PAGE_FIELDS =  ['seq', 'languages', 'calculatedLanguage', 'version']
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
        
    def parse(self, **kwargs):
        
        obj = self._parse_json()

        self._schema = obj['features']['schemaVersion']
        if self._schema not in self.SUPPORTED_SCHEMA:
            logging.warning('Schema version of imported file (%s) does not match '
                         'the supported versions (%s). Update your files or use an older '
                         'version of the library' %
                         (obj['features']['schemaVersion'],
                          self.SUPPORTED_SCHEMA))
            
        self._pages = obj['features']['pages']
        
        fields = self.METADATA_FIELDS.copy()
        # Anything in self.meta becomes an attribute in the volume
        if self._schema in ['2.0', '3.0']:
            fields += self.METADATA_FIELDS_1_3
        else:
            fields += self.METADATA_FIELDS_3_0
        
        # Expand basic values to properties
        for key, pythonkey in fields:
            if '.' not in key:
                key = 'metadata.' + key
            
            fieldpath = key.strip('.').split('.')
            ptr = obj
            for field in fieldpath:
                if field in ptr:
                    ptr = ptr[field]
                else:
                    ptr = None
                    break
            self.meta[pythonkey] = ptr
            if pythonkey.endswith('_ld'):
                if pythonkey == 'genre_ld':
                    self.meta['genre'] = []
                    if type(ptr) is not list:
                        ptr = [ptr]
                    for genre in ptr:
                        if genre.startswith("http://id.loc.gov/vocabulary/marcgt/") and genre[36:] in utils.LOC_MARCGT_REFERENCE:
                            self.meta['genre'].append(utils.LOC_MARCGT_REFERENCE[genre[36:]])
                        else:
                            self.meta['genre'].append(genre)
                elif ptr is None:
                    self.meta[pythonkey[:-3]] = None
                elif (type(ptr) is dict) and ('name' in ptr):
                    self.meta[pythonkey[:-3]] = ptr['name']
                else:
                    self.meta[pythonkey[:-3]] = [v['name'] for v in ptr if 'name' in v]
        
        # TODO collect while iterating earlier
        self.seqs = [int(page['seq']) for page in self._pages]
        
        if 'language' in self.meta:
            if (self._schema in ['2.0', '3.0']) and (self.meta['language'] in ['jpn', 'chi']):
                logging.warning("This version of the EF dataset has a tokenization bug "
                            "for Chinese and Japanese. Use newer EF files.")

    def write(self, outside_volume, compression='default', mode='wb', **kwargs):

        if compression == "default":
            compression = self.id_resolver.compression
            
        if outside_volume.parser.format != "json":
            raise TypeError("Can only write to json from other json, because"
                            "data is lost in creating the parquet files.")

        if (outside_volume.id_resolver.compression == self.id_resolver.compression) and (self.id_resolver.compression != "default"):
            skip_compression = True
        else:
            skip_compression = False
            
        json_bytestring = outside_volume.parser._parse_json(object = False, skip_compression = skip_compression)

        with self.id_resolver as context:
            with context.open(self.id, compression = context.compression, format = 'json',
                                skip_compression = skip_compression, mode=mode,
                                **kwargs) as fout:
                fout.write(json_bytestring)


    def read(self, **kwargs):
        '''
        Load JSON for a path. Allows remote files in addition to local ones. 
        Returns: string of json.
        '''
        pass
    
    def _parse_json(self, compression='default', **kwargs):
        id = self.id
        resolver = self.id_resolver
        if compression is 'default':
            compression = self.id_resolver.compression
        
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
                if page[sec] is None:
                    continue
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
                if page[sec] is None:
                    continue
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
            place_key = [('begin', 'beginCharCounts'), ('end', 'endCharCount')]
        elif self._schema == 'https://schemas.hathitrust.org/EF_Schema_FeaturesSubSchema_v_3.0':
            place_key = [('begin', 'beginCharCount'), ('end', 'endCharCount')]
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
                if page[sec] is None:
                        continue
                for place, json_key in place_key:
                    if page[sec][json_key] is None:
                        continue
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

        It is recommended to use an id-based id_resolver with this class. Path-based
        resolvers cannot transparently resolve all four files with a single file path.
        
    '''
        
    
    def __init__(self, id, id_resolver, mode = 'rb', compression = 'snappy', **kwargs):

        self.format = "parquet"

        super().__init__(id = id, id_resolver = id_resolver, compression = compression,
                         mode = mode,  **kwargs)
    
    def parse(self, **kwargs):
        
        try:
            with self.id_resolver.open(self.id, suffix = "meta", format = "json", compression = None) as meta_buffer:
                self.meta = json.loads(meta_buffer.read().decode("utf-8"))
        except:
            self.meta = dict(id=self.id, title=self.id)
            
        if not self.meta['id']:
            self.meta['id'] = htrc_features.utils.extract_htid(self.id)
        
        if not 'handle_url' in self.meta or not self.meta['handle_url']:
            self.meta['handle_url'] = "http://hdl.handle.net/2027/%s" % self.meta['id']
            
        if not 'title' in self.meta or not self.meta['title']:
            self.meta['title'] = self.meta['id']

    def write(self, volume, files = ['meta', 'tokens'],
              mode='wb', compression="default", indexed=True,
              token_kwargs="default",
              **kwargs):
        '''

        Save the internal representations of feature data to parquet, and the metadata to json,
        using the naming convention used by ParquetFileHandler.
        
        The primary use is for converting the feature files to something more efficient. By default,
        only metadata and tokencounts are saved.
        
        files lists which files you want to get. Default is 'meta', and 'tokens'.
        Also allowed are 'chars' (character counts) and 'section_features'


        'volume' is an object of the 'Volume' class which will be used for data. It will
        almost certainly need to come from a true JSON file.

        Saving page features is currently unsupported, as it's an ill-fit for parquet. This is currently
        just the language-inferences for each page - everything else is in section features 
        (page by body/header/footer).
        
        Since Volumes partially support incomplete dataframes, you can pass Volume.tokenlist arguments
        as a dict with token_kwargs. For example, if you want to save a representation with only body
        information, drop the 'section' level of the index, and fold part-of-speech counts, you can pass
        token_kwargs=dict(section='body', drop_section=True, pos=False).
        '''

        if token_kwargs == "default":
            token_kwargs = dict(section='all', drop_section=False)
                
        if compression == "default":
            compression = self.id_resolver.compression

        if len(files) == 0:
            logging.warning("You're not saving anything with save_parquet")
            return

        for f in files:
            assert(f in ['meta', 'tokens', 'chars', 'section_features'])
        
        with self.id_resolver as resolver:
            """
            This context handling matters to ensure--eg--zipfiles are closed
            after writing.
            """
            if 'meta' in files:
                metastring = json.dumps(volume.parser.meta).encode("utf-8")
                with resolver.open( self.id, format = "json",
                                         compression = None, suffix = 'meta', mode=mode,
                                         **kwargs) as fout:
                    fout.write(metastring)

        with self.id_resolver as resolver:                    
            if 'tokens' in files:
                try:
                    feats = volume.tokenlist(**token_kwargs)
                except:
                    raise
                    # If the internal representation is incomplete, returning the above may fail,
                    # but the cache may have an acceptable dataset to return
                    feats = volume._tokencounts
                if not indexed:
                    feats = feats.reset_index()
                    
                if not feats.empty:
                    with resolver.open(id = self.id, suffix = 'tokens', mode=mode, **kwargs) as fout:
                        feats.to_parquet(fout, compression=compression, index = indexed)
                        
        with self.id_resolver as resolver:
            if 'section_features' in files:
                feats = volume.section_features(section='all')
                if not feats.empty:
                    with resolver.open(id = self.id, suffix = 'section', mode=mode, **kwargs) as fout:
                        feats.to_parquet(fout, compression=compression, index = indexed)
                        
        with self.id_resolver as resolver:
            if 'chars' in files:
                feats = volume.line_chars()
                if not feats.empty:
                    with resolver.open(id = self.id, suffix = 'chars', mode=mode, **kwargs) as fout:
                        feats.to_parquet(fout, compression=compression, index = indexed)

    def _make_tokencount_df(self):
        try:
            with self.id_resolver.open(id = self.id, suffix = 'tokens', format = 'parquet') as fin:
                df = pd.read_parquet(fin)
        except IOError:
            raise MissingDataError("No token information available")
            
        indcols = [col for col in ['page', 'section', 'token', 'lowercase', 'pos'] if col in df.columns]
        if len(indcols):
            df = df.set_index(indcols)
        return df
    
    def _make_line_char_df(self):
        try:
            with self.id_resolver.open(id = self.id, suffix = 'chars', format = 'parquet') as fin:
                df = pd.read_parquet(fin)
                return df 
        except IOError:
            raise MissingDataError("No line char information available")
        
    def _make_section_feature_df(self):
        try:
            with self.id_resolver.open(id = self.id, suffix = 'section', format = 'parquet') as fin:
                df = pd.read_parquet(fin)
            return df 
        except IOError:
            raise MissingDataError("No section information available")
        
    def _make_page_feature_df(self):
        raise Exception("parquet parser doesn't support non-token, non-section page features")

    def _parse_meta(self):
        pass
    
