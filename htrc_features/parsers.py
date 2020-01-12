from __future__ import unicode_literals

import sys
PY3 = (sys.version_info[0] >= 3)

import logging
import pandas as pd
import numpy as np
import pymarc
from six import iteritems, StringIO, BytesIO
import codecs
import os

from htrc_features import utils

try:
    import ujson as json
except ImportError:
    import json
import requests


if PY3:
    from urllib.request import urlopen as _urlopen
    from urllib.parse import urlparse as parse_url
    from urllib.error import HTTPError
else:
    from urlparse import urlparse as parse_url
    from urllib2 import urlopen as _urlopen
    from urllib2 import HTTPError

try:
    import bz2file as bz2
except ImportError:
    import bz2
    if not PY3:
        logging.warning("Loading volumes from a URL will not work in Python 2 unless you install bz2file")


def resolve_file(id, format, storage_strategy, compression = None, dir = None):

    if compression is not None:
        suffix = f".{format}.{compression}"
    else:
        suffix = f".{format}"
    if storage_strategy == 'pairtree':
        return os.path.join(dir, utils.id_to_pairtree(id))
    if storage_strategy == 'local':
        return os.path.join(dir, utils.clean_htid(id))
    if storage_strategy == 'http' or storage_scheme == 'https':
        return 


class BaseFileHandler(object):
    
    def __init__(self, path=False, id=False, id_resolver = None, **kwargs):
        ''' Base class for volume reading.'''
        
        assert (path or id) and not (path and id)
        
        self.meta = dict(id=None)
        self.args = kwargs
        self.args['id_resolver'] = id_resolver
        logging.info(self.args)
                    # When creating, we don't want to raise an error on existing files.
        
        self.path = None
        
        self.resolve_path(id, path, id_resolver)

        if 'load' in self.args and self.args['load'] == False:
            return        
        if 'mode' in self.args and self.args['mode'] == 'create':
            return
        
        # Note that any FileHanlder must define a path if one isn't passed
        # in explicitly.

        obj = self.read(self.path, **kwargs)
        self.parse(obj)

    def resolve_path(self, id = False, path = False, id_resolver = None):
        """
        Resolve a path to the object.
        """
        if path:
            self.path = path
            return
        
        if id_resolver == 'http' or id_resolver == 'https':
            self.path = self.DL_URL.format(id)
            
        if id_resolver == 'pairtree':
            dirname = utils.id_to_pairtree(id, self.args.get('format', 'json'), self.args.get('compression', None))
            self.path = os.path.join(self.args["dir"], dirname)
            
        return self.path
        
    def read(self):
        ''' Args can be dependent on individual parser.'''
        pass

    def write(self):
        '''
        
        '''

        if self.args['storage_scheme'] in ['pairtree', 'path', 'local']:
            directory = os.path.dirname(file_path)
            if not os.path.exists(directory):
                os.makedirs(directory)

        
        
        raise NotImplementedError("A write method has not been defined for this parser.")
                
    def parse(self):
        ''' Save any info that needs to be held at init time for parsing. In some cases,
        little needs to be saved before methods like _make_tokencount_df need to be run.
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
    DL_URL = "http://data.htrc.illinois.edu/htrc-ef-access/get?action=download-ids&id={0}&output=json"
    
    def __init__(self, path=False, id=False, id_resolver = None, **kwargs):
        self.meta = dict(id=None, handle_url=None)
        self._schema = None
        self._pages = None

        # parsing and reading are called here.
        super().__init__(path, id, id_resolver, **kwargs)

    def write(self, volume):
        """
        Logic: you can currently only write JSON from a JSON,
        because fully recreating that format is outside the scope here. (Would it ever be useful for it not to be?)

        Moreover, that volume needs to have the json actually attached, which doesn't happen by default.
        """

        pass
    
    def read(self, path_or_url, **kwargs):
        '''
            Load JSON for a path. Allows remote files in addition to local ones. 
            Returns: JSON object.
        '''
        try:
            compression = self.args['compression']
        except KeyError:
            compression = 'bz2'
        
        id_resolver = self.args['id_resolver']
            
        if parse_url(path_or_url).scheme in ['http', 'https']:
            try:
                req = _urlopen(path_or_url)
                filename_or_buffer = BytesIO(req.read())
            except HTTPError:
                logging.exception("HTTP Error accessing %s" % path_or_url)
                raise
            compressed = False
        else:
            filename_or_buffer = path_or_url

        compressed = False
        if 'compression' in self.args and self.args['compression'] != None:
            compressed = True
        
        try:
            if compressed:
                f = bz2.BZ2File(filename_or_buffer)
            else:
                if (type(filename_or_buffer) != BytesIO) and not isinstance(filename_or_buffer, StringIO):
                    f = codecs.open(filename_or_buffer, 'r+', encoding="utf-8")
                else:
                    f = filename_or_buffer
            rawjson = f.readline()
            f.close()
        except IOError:
            logging.exception("Can't read %s. Did you pass the incorrect "
                              "'compressed=' argument?", path_or_url)
            raise
        except:
            print(compressed, type(filename_or_buffer))
            logging.exception("Can't open %s", path_or_url)
            raise

        try:
            # For Python3 compatibility, decode to str object
            if PY3 and (type(rawjson) != str):
                rawjson = rawjson.decode()
            volumejson = json.loads(rawjson)
        except:
            logging.exception("Problem reading JSON for %s. One common reason"
                              " for this error is an incorrect compressed= "
                              "argument", path_or_url)
            raise
        return volumejson
    
    def parse(self, obj):
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
        if not self._token_freqs:
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
        By default, the loading enforces a filename convention, and the path provided
        to the parser should avoid the file extension. The filename convention is
            - ../{htid}.meta.json
            - ../{htid}.tokens.parquet
            - ../{htid}.chars.parquet
            - ../{htid}.section.parquet
        
        If assume_filenames is False, you won't need to follow the filename convention, and
        instead provide a tuple of the filenames. This is not currently implemented.
    '''
        
    
    def __init__(self, path=False, id=False, id_resolver = None, **kwargs):

        path = self.resolve_path(path, id, id_resolver, **kwargs)
        
        self.meta_path = path + '.meta.json'
        self.token_path = path + '.tokens.parquet'
        self.char_path = path + '.chars.parquet'
        self.section_path = path + '.section.parquet'
        self.compression = kwargs.get("compression", "snappy")
        
        super().__init__(path = path, id = id, id_resolver = id_resolver, **kwargs)
    
    def read(self, **kwargs):
        if os.path.exists(self.meta_path):
            with open(self.meta_path, mode='r') as f:
                self.meta = json.load(f)
        else:
            self.meta = dict(id=None, handle_url=None, title=None)
    
    def parse(self, meta, **kwargs):
        if not self.meta['id']:
            # Parse from filename
            filename = os.path.split(self.tokencount_path)[-1]
            self.meta['id'] = os.path.splitext(filename)[0].replace("+", ":").replace("=", "/").replace(",", ".")
        
        if not self.meta['handle_url']:
            self.meta['handle_url'] = "http://hdl.handle.net/2027/%s" % self.meta['id']
            
        if not self.meta['title']:
            self.meta['title'] = self.meta['id']

    def write(self, volume, **kwargs):
        
        '''

        Save the internal representations of feature data to parquet, and the metadata to json,
        using the naming convention used by parquetVolumeParser.
        
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

        if kwargs.get("meta", True):
            with open(self.meta_path, mode='w') as f:
                # Never compressed bc it's tiny anyway.
                json.dump(volume.parser.meta, f)
        
        if kwargs.get("tokens", True):
            feats = volume._tokencounts
            if not feats.empty:
                feats.to_parquet(fname_root + '.tokens.parquet', compression=self.compression)
            
        if kw.args.get("section_features", False):
            feats = volume.section_features(section='all')
            if not feats.empty:
                feats.to_parquet(fname_root + '.section.parquet', compression=self.compression)
            
        if kwargs.get("chars", False):
            feats = volume.line_chars()
            if not feats.empty:
                feats.to_parquet(fname_root + '.chars.parquet', compression=self.compression)

            
    def _make_tokencount_df(self):
        if not os.path.exists(self.token_path):
            raise MissingDataError("No token information available")
            
        df = pd.read_parquet(self.token_path)
        indcols = [col for col in ['page', 'section', 'token', 'lowercase', 'pos'] if col in df.columns]
        if len(indcols):
            df = df.set_index(indcols)
        return df
        
        return self._tokencount_df
        ''' Dummy: data already read at init and cached.'''
        return self._tokencount_df
    
    def _make_line_char_df(self):
        if not os.path.exists(self.char_path):
            raise MissingDataError("No line char information available")
            
        df = pd.read_parquet(self.char_path)    
        return df 
        
    def _make_section_feature_df(self):
        if not os.path.exists(self.section_path):
            raise MissingDataError("No page+section information available")
            
        df = pd.read_parquet(self.section_path)    
        return df 
    
    def _make_page_feature_df(self):
        raise Exception("parquet parser doesn't support non-token, non-section page features")

    def _parse_meta(self):
        pass
    
