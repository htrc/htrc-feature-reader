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

# UTILS
SECREF = ['header', 'body', 'footer']


def group_tokenlist(in_df, pages=True, section='all', case=True, pos=True,
                    page_freq=False):
    '''
        Return a token count dataframe with requested folding.

        pages[bool]: If true, keep pages. If false, combine all pages.
        section[string]: 'header', 'body', 'footer' will only return those
            sections. 'all' will return all info, unfolded. 'group' combines
            all sections info.
        case[bool]: If true, return case-sensitive token counts.
        pos[bool]: If true, return tokens facets by part-of-speech.
        page_freq[bool]: If true, will simply count whether or not a token is
        on a page. Defaults to false.
    '''
    groups = []
    if pages:
        assert 'page' in in_df.index.names
        groups.append('page')

    if section == 'all' and ('section' in in_df.index.names):
        groups.append('section')
    elif section in SECREF:
        assert 'section' in in_df.index.names
        groups.append('section')
        
    groups.append('token' if case else 'lowercase')

    if pos:
        assert 'pos' in in_df.index.names
        groups.append('pos')

    if in_df.empty:
        return pd.DataFrame([], columns=groups)

    if section in ['all', 'group']:
        df = in_df
    elif section in SECREF:
        idx = pd.IndexSlice
        try:
            df = in_df.loc[idx[:, section, :, :], ]
        except KeyError:
            logging.debug("Section {} not available".format(section))
            df = pd.DataFrame([], columns=groups+['count'])\
                   .set_index(groups)
            return df
    else:
        logging.error("Invalid section argument: {}".format(section))
        return

    if not case and 'lowercase' not in in_df.index.names:
        # Replace our df reference to a copy.
        df = df.copy()
        logging.debug('Adding lowercase column')
        df.insert(len(df.columns), 'lowercase',
                  df.index.get_level_values('token').str.lower())
    elif case:
        assert 'token' in in_df.index.names

    # Check if we need to group anything
    if groups == in_df.index.names:
        if page_freq:
            pd.options.mode.chained_assignment = None
            df['count'] = 1
            pd.options.mode.chained_assignment = 'warn'
        return df
    else:
        if not page_freq:
            return df.reset_index().groupby(groups).sum()[['count']]
        elif page_freq and 'page' in groups:
            df = df.reset_index().groupby(groups).sum()[['count']]
            pd.options.mode.chained_assignment = None
            df['count'] = 1
            pd.options.mode.chained_assignment = 'warn'
            return df
        elif page_freq and 'page' not in groups:
            # We'll have to group page-level, then group again
            def set_to_one(x):
                x['count'] = 1
                return x
            return df.reset_index().groupby(['page']+groups).apply(set_to_one)\
                     .groupby(groups).sum()[['count']]


def group_linechars(df, section='all', place='all'):

    # Set up grouping
    groups = ['page']
    if section in SECREF + ['all']:
        groups.append('section')
    if place in ['begin', 'end', 'all']:
        groups.append('place')
    groups.append('character')

    # Set up slicing
    slices = [slice(None)]
    if section in ['all', 'group']:
        slices.append(slice(None))
    elif section in SECREF:
        slices.append([section])
    if place in ['begin', 'end']:
        slices.append([place])
    elif place in ['group', 'all']:
        # It's hard to imagine a use for place='group', but adding for
        # completion
        slices.append(slice(None))

    if slices != [slice(None)] * 3:
            df = df.loc[tuple(slices), ]

    if groups == ['page', 'section', 'place', 'character']:
        return df
    else:
        return df.groupby(groups).sum()[['count']]

# CLASSES
class FeatureReader(object):

    def __init__(self, paths=None, ids=None, parser="json", **kwargs):
        '''
        A reader for Extracted Features Dataset files.
        
        parser: a VolumeParser class, or a string for a built in class (e.g.'json' or
        'parquet').
        
        paths: Filepaths to dataset files. The format will depend on 
        
        ids: HathiTrust IDs referred to files. Alternative to `paths`. By default will download
            json files behind the scenes.
        
        The other args depend on the parser. For the default jsonVolumeParser, allowable args
        are: `compressed`.
        '''
        
        # only one of paths or ids can be selected - otherwise it's not clear what to iterate
        # over. 
        assert (paths or ids) and not (paths and ids)
        
        if paths:
            self._online = False
            if type(paths) is list:
                self.paths = paths
            else:
                self.paths = [paths]
        else:
            self.paths = False

        if ids:
            if type(ids) is list:
                self.ids += [self.DL_URL.format(id) for id in ids]
            else:
                self.ids.append(self.DL_URL.format(ids))
        else:
            self.ids = False

        self.index = 0
        
        if parser == 'json':
            self.parser_class = jsonVolumeParser
        elif issubclass(parser, htrc_features.baseVolumeParser):
            self.parser_class = parser
        else:
            raise Exception("No valid parser defined.")
        
        self.parser_kwargs = kwargs

    def __iter__(self):
        return self.volumes()
    
    def __len__(self):
        return len(self.paths)

    def __str__(self):
        return "HTRC Feature Reader with %d paths loaded" % (len(self.paths))

    def volumes(self):
        ''' Generator for returning Volume objects '''
        if self.ids:
            for id in self.ids:
                yield Volume(path=False, id=id, parser=self.parser_class, **self.parser_kwargs)
        elif self.paths:
            for path in self.paths:
                yield Volume(path, id=False, parser=self.parser_class, **self.parser_kwargs)
        else:
            raise

    def jsons(self):
        ''' Generator for returning decompressed, parsed json dictionaries
        for volumes. Convenience function for when the FeatureReader objects
        are not needed. '''
        for path in self.paths:
            yield self._read_json(path, compressed=self.compressed)

    def first(self):
        ''' Return first volume from Feature Reader. This is a convenience
        feature for single volume imports or to quickly get a volume for
        testing.'''
        return next(self.volumes())
    
    def __repr__(self):
        if len(self.paths) > 1:
            return "<%d path FeatureReader (%s to %s)>" % (len(self.paths), self.paths[0], self.paths[-1])
        elif len(self.paths) == 1:
            return "<Empty FeatureReader>"
        else:
            return "<FeatureReader for %s>" % self.paths[0]
        
    def __str__(self):
        return "<%d path FeatureReader>" % (len(self.paths))

class baseVolumeParser(object):
    
    def __init__(self, path=False, id=False, **kwargs):
        ''' Base class for volume reading.'''
        self.meta = dict(id=None)
        
        # Example init process
        output = self.read(path, id, **kwargs)
        self.parse(output)
    
    def read(self):
        ''' Args can be dependent on individual parser.'''
        pass
    
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
        
        
class jsonVolumeParser(baseVolumeParser):
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
    
    DL_URL = "http://data.htrc.illinois.edu/htrc-ef-access/get?action=download-ids&id={0}&output=json"
    
    BASIC_FIELDS = [('pageCount', 'page_count')]
    ''' List of fields which return primitive values in the schema, as tuples
    with (CamelCase, lower_with_under) mapping.
    '''
    
    PAGE_FIELDS =  ['seq', 'languages']
    SECTION_FIELDS =  ['tokenCount', 'lineCount', 'emptyLineCount',
                             'capAlphaSeq', 'sentenceCount']
    
    def __init__(self, path=False, id=False, compressed=True, **kwargs):
        self.meta = dict(id=None, handle_url=None)
        self._schema = None
        self._pages = None
        
        assert (path or id) and not (path and id)
        
        if id:
            path = self.DL_URL.format(id)
        
        obj = self.read(path, compressed, **kwargs)
        self.parse(obj)
    
    def read(self, path_or_url, compressed=True, **kwargs):
        ''' Load JSON for a path. Allows remote files in addition to local ones. 
            Returns: JSON object.
        '''
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
            logging.warn("Adapted to erroneous key names in schema 3.0.")
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
    
class parquetVolumeParser(baseVolumeParser):
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
        
    
    def __init__(self, path=False, id=False, assume_filenames=True, **kwargs):

        if id:
            raise Exception("id not currently supported in parquetVolumeParser")
        if not assume_filenames:
            raise Exception("assume_filenames currently cannot be false. See docstring "
                            "for parquetVolumeParser")
        
        self.meta_path = path + '.meta.json'
        self.token_path = path + '.tokens.parquet'
        self.char_path = path + '.chars.parquet'
        self.section_path = path + '.section.parquet'
        
        if os.path.exists(self.meta_path):
            with open(self.meta_path, mode='r') as f:
                self.meta = json.load(f)
        else:
            self.meta = dict(id=None, handle_url=None, title=None)
        
        self.read()
        self.parse()
    
    def read(self):
        pass
    
    def parse(self):
        if not self.meta['id']:
            # Parse from filename
            filename = os.path.split(self.tokencount_path)[-1]
            self.meta['id'] = os.path.splitext(filename)[0].replace("+", ":").replace("=", "/").replace(",", ".")
        
        if not self.meta['handle_url']:
            self.meta['handle_url'] = "http://hdl.handle.net/2027/%s" % self.meta['id']
            
        if not self.meta['title']:
            self.meta['title'] = self.meta['id']
    
    def _make_tokencount_df(self):
        if not os.path.exists(self.token_path):
            raise Exception("No token information available")
            
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
            raise Exception("No line char information available")
            
        df = pd.read_parquet(self.char_path)    
        return df 
        
    def _make_section_feature_df(self):
        if not os.path.exists(self.section_path):
            raise Exception("No page+section information available")
            
        df = pd.read_parquet(self.section_path)    
        return df 
    
    def _make_page_feature_df(self):
        raise Exception("parquet parser doesn't support non-token, non-section page features")

    def _parse_meta(self):
        pass
    
class Volume(object):

    def __init__(self, path=False, id=False, parser='json', default_page_section='body', **kwargs):
        '''
        The Volume allows simplified, Pandas-based access to the HTRC
        Extracted Features files.
        
        This class recruits a VolumeParser class to read its data and parse
        it to up to four dataframes:
            - tokencounts : Case-sensitive, POS-tagged token counts,
            per section (body/header/footer) and page.
            - metadata : Data about the book.
            - page features : Features specific to a pages.
            - section features : Features specific to sections
            (body/header/footer) of each page.
        
        Most of the time, the parser with be the default json parser, which
        deals with the format that HTRC distributes, but for various reasons
        you may want to use or write alternative formats. e.g. perhaps you
        just need a nice view into the metadata, or hope for quicker access
        specifically to tokencounts.
        '''
        self._tokencounts = pd.DataFrame()
        self._line_chars = pd.DataFrame()
        self._page_features = pd.DataFrame()
        self._section_features = pd.DataFrame()
        
        self._extra_metadata = None
        self.default_page_section = default_page_section
        
        if parser == 'json':
            self.parser = jsonVolumeParser(path, id, **kwargs)
        elif parser == 'parquet':
            self.parser = parquetVolumeParser(path, id, **kwargs)
        elif issubclass(parser, baseVolumeParser):
            self.parser = parser(path, id, **kwargs)
        else:
            raise Exception("No valid parser defined.")
            
        self._update_meta_attrs()
    
    def _update_meta_attrs(self):
        ''' Takes metadata from the parser's metadata variable and 
        assigns it to attributes in the Volume '''
        for k, v in self.parser.meta.items():
            setattr(self, k, v)

    def __iter__(self):
        return self.pages()

    def __str__(self):
        try:
            return "<HTRC Volume: %s - %s (%s)>" % (self.id, self.title, self.year)
        except:
            return "<HTRC Volume: %s>" % self.id

    def _repr_html_(self):
        html_template = "<strong><a href='%s'>%s</a></strong> by <em>%s</em> (%s, %s pages) - <code>%s</code>"
        try:
            return html_template % (self.handle_url, self.title, ",".join(self.author), self.year, self.page_count, self.id)
        except:
            return "<strong><a href='%s'>%s</a></strong>" % (self.handle_url, self.title)

    def page_features(self, feature='all', page_select=False):
        if self._page_features.empty:
            self._page_features = self.parser._make_page_feature_df()
            
        return self._get_basic_feature(self._page_features, section='all', feature=feature, page_select=page_select)
    
    def section_features(self, feature='all', section='default', page_select=False):
        if self._section_features.empty:
            self._section_features = self.parser._make_section_feature_df()
        return self._get_basic_feature(self._section_features, section=section, feature=feature, page_select=page_select)

    def _get_basic_feature(self, df, feature='all', section='default', page_select=False):
        '''Selects a basic feature from a page_features or section_features dataframe'''
        
        if section == 'default':
            section = self.default_page_section
        
        if page_select:
            df = df.xs(page_select, level='page', drop_level=False)
        
        if feature is not 'all':
            df = df[feature]

        if section in ['header', 'body', 'footer']:
            return df.xs(section, level='section')
        elif section == 'all':
            return df
        elif section == 'group':
            return df.sum()
        else:
            raise Exception("Bad Section Arg")

    @property
    def year(self):
        ''' A friendlier name wrapping Volume.pubDate '''
        return self.pub_date
    
    @property
    def author(self):
        ''' A friendlier name wrapping Volume.names. Returns list. '''
        return self.names

    @property
    def metadata(self):
        """
        Fetch additional information about a volume from the HathITrust Bibliographic API.

        See: https://www.hathitrust.org/bib_api

        :return: A `pymarc` record. See pymarc's documentation for details on using it.
        """
        if not self._extra_metadata:
            logging.debug("Looking up full metadata for {0}".format(self.id))
            data = requests.get(self.ht_bib_url).json()

            record_id = data['items'][0]['fromRecord']
            marc = data['records'][record_id]['marc-xml']

            # Pymarc only reads a file, so stream the text as if it was one
            xml_stream = StringIO(marc)
            xml_record = pymarc.parse_xml_to_array(xml_stream)[0]
            xml_stream.close()

            self._extra_metadata = xml_record
        return self._extra_metadata

    def tokens(self, section='default', case=True, page_select=False):
        ''' Get unique tokens '''
        tokens = (self.tokenlist(section=section, page_select=page_select)
                  .index.get_level_values('token').to_series())
        if case:
            return tokens.unique().tolist()
        else:
            return tokens.str.lower().unique().tolist()

    def pages(self, **kwargs):
        ''' Iterate through Page objects with a reference to this class.
            This is mostly a convenience these days - logic exists in
            the Volume class.
        '''
        for seq in self.parser.seqs:
            yield Page(seq, self, **kwargs)

    def tokens_per_page(self, **kwargs):
        '''
        Return a Series page lengths
        '''
        df = self.tokenlist(**kwargs)
        return df.groupby(level='page').sum()['count']

    def line_counts(self, **kwargs):
        ''' Return a Series of line counts, per page '''
        return self.section_features(feature='lineCount', **kwargs)

    def empty_line_counts(self, section='default', **kwargs):
        ''' Return a list of empty line counts, per page '''
        return self.section_features(feature='emptyLineCount', **kwargs)

    def cap_alpha_seq(self, **kwargs):
        logging.warning("At the volume-level, use Volume.cap_alpha_seqs()")
        return self.cap_alpha_seqs(**kwargs)

    def cap_alpha_seqs(self, section='body', **kwargs):
        ''' Return the longest length of consecutive capital letters starting a
        line on the page. Returns a list for all pages. Only includes the body:
        header/footer information is not included.
        '''
        if ('section' in kwargs) and kwargs['section'] != 'body':
            logging.warning("cap_alpha_seq only includes counts for the body "
                         "section of pages.")
            kwargs['section'] = 'body'
        return self.section_features(feature='capAlphaSeq', **kwargs)

    def sentence_counts(self, **kwargs):
        ''' Return a list of sentence counts, per page '''
        return self.section_features(feature='sentenceCount', **kwargs)

    def tokenlist(self, pages=True, section='default', case=True, pos=True,
                  page_freq=False, page_select=False):
        ''' Get or set tokencounts DataFrame

        pages[bool]: Keep page-level info if true, else fold.

        section[string]: Which part of the page to return. In addition to
            'header', 'body', and 'footer', 'all' will return a DataFrame with
            all the sections, 'group' will sum all sections,
            section in ['header', 'footer', 'body'] will return those fields
            section == 'all' will group and sum all sections
            section == 'default' falls back on what the page object has saved

        case[bool] : Preserve case, or fold.

        pos[bool] : Specify whether to return frequencies per part of speech,
                    or simply by word

        page_freq[bool] : Whether to count page frequency (1 if it occurs on
        the page, else 0) or a term frequency (counts for the term, per page)
        
        page_select[int] : Page sequence number for optionally choosing just one
            page.
        
        '''
        if section == 'default':
            section = self.default_page_section
        
        assert not (page_select and not pages)

        # Create the internal representation if it does not already
        # exist. This will only need to exist once
        if self._tokencounts.empty:
            self._tokencounts = self.parser._make_tokencount_df()
        
        assert(('token' in self._tokencounts.index.names) or ('lower' in self._tokencounts.index.names))
        
        # Allow incomplete internal representations, as long as the args don't want the missing
        # data
        for arg, column in [(pages, 'page'), (page_select, 'page'), (case, 'token'),
                            (pos, 'pos')]:
            if arg and column not in self._tokencounts.index.names:
                raise Exception("Your internal tokenlist representation does not have enough "
                                "information for the current args. Missing column: %s" % column)
        
        if page_select:
            try:
                df = self._tokencounts.xs(page_select,
                                          level='page', drop_level=False)
            except KeyError:
                # Empty tokenlist
                return self._tokencounts.iloc[0:0]
        else:
            df = self._tokencounts

        return group_tokenlist(df, pages=pages, section=section,
                               case=case, pos=pos, page_freq=page_freq)
        

    def term_page_freqs(self, page_freq=True, case=True):
        ''' Return a term frequency x page matrix, or optionally a
        page frequency x page matrix '''
        all_page_dfs = self.tokenlist(page_freq=page_freq, case=case)
        return all_page_dfs.reset_index()\
                           .groupby(['token', 'page'], as_index=False).sum()\
                           .pivot(index='page', columns='token',
                                  values='count')\
                           .fillna(0)

    def term_volume_freqs(self, page_freq=True, pos=True, case=True):
        ''' Return a list of each term's frequency in the entire volume '''
        df = self.tokenlist(page_freq=page_freq, pos=pos, case=case)
        groups = ['token'] if not pos else ['token', 'pos']
        return df.reset_index().drop(['page'], axis=1)\
                 .groupby(groups, as_index=False).sum()\
                 .sort_values(by='count', ascending=False)

    def end_line_chars(self, **args):
        '''
        Get counts of characters at the end of lines, i.e. the characters on
        the far right of the page.
        '''
        return self.line_chars(place='end', **args)

    def begin_line_chars(self, **args):
        '''
        Get counts of characters at the begin of lines, i.e. the characters on
        the far left of the page.
        '''
        return self.line_chars(place='begin', **args)

    def line_chars(self, section='default', place='all', page_select=False):
        '''
        Interface for all begin/end of line character information.
        '''

        # Create the internal representation
        if self._line_chars.empty:
            self._line_chars = self.parser._make_line_char_df()
        
        df = self._line_chars
        if page_select:
            try:
                df = df.xs(page_select, level='page',
                                         drop_level=False)
            except KeyError:
                # Empty tokenlist
                return self._line_chars.iloc[0:0]
            
        if section == 'default':
            section = self.default_page_section

        return group_linechars(df, section=section, place=place)
    
    def save_parquet(self, path, meta=True, tokens=True, chars=False, 
                     section_features=False, compression='snappy'):
        '''
        Save the internal representations of feature data to parquet, and the metadata to json,
        using the naming convention used by parquetVolumeParser.
        
        The primary use is for converting the feature files to something more efficient. By default,
        only metadata and tokencounts are saved.
        
        Saving page features is currently unsupported, as it's an ill-fit for parquet. This is currently
        just the language-inferences for each page - everything else is in section features 
        (page by body/header/footer).
        '''
        fname_root = os.path.join(path, self.id)
        
        if meta:
            with open(fname_root + '.meta.json', mode='w') as f:
                json.dump(self.parser.meta, f)
        
        if tokens:
            try:
                feats = self.tokenlist()
            except:
                # In the internal representation is incomplete, returning the above may fail,
                # but the cache may have an acceptable dataset to return
                feats = self._tokencounts
            if not feats.empty:
                feats.to_parquet(fname_root + '.tokens.parquet', compression=compression)
            
        if section_features:
            feats = self.section_features(section='all')
            if not feats.empty:
                feats.to_parquet(fname_root + '.section.parquet', compression=compression)
            
        if chars:
            feats = self.line_chars()
            if not feats.empty:
                feats.to_parquet(fname_root + '.chars.parquet', compression=compression)
    
    def __str__(self):
        def truncate(s, maxlen):
            if len(s) > maxlen:
                return s[:maxlen].strip() + "..."
            else:
                return s.strip()
        return "<Volume: %s (%s) by %s>" % (truncate(self.title, 30), self.year, truncate(self.author[0], 40))

class Page:

    BASIC_FIELDS = [('seq', 'seq'), ('tokenCount', '_token_count'),
                    ('languages', 'languages')]
    ''' List of fields which return primitive values in the schema, as tuples
    with (CamelCase, lower_with_under) mapping '''
    SECTION_FIELDS = ['lineCount', 'emptyLineCount', 'sentenceCount',
                      'capAlphaSeq']
    ''' Fields that are counted by section.'''

    def __init__(self, seq, volume, default_section='body'):
        self.default_section = default_section
        self.volume = volume
        self.seq = int(seq)

        assert(self.default_section in SECREF + ['all', 'group'])

    def tokens(self, **kwargs):
        ''' Get unique tokens. Use args from Volume. '''
        return self.volume.tokens(page_select=self.seq, **kwargs)

    def line_count(self, section='default'):
        return self.volume.section_features(page_select=self.seq, feature='lineCount').values[0]

    def empty_line_count(self, section='default'):
        return self.volume.section_features(page_select=self.seq, feature='emptyLineCount').values[0]

    def cap_alpha_seq(self, section='body'):
        ''' Return the longest length of consecutive capital letters starting a
        line on the page. Returns an integer. Only includes the body:
        header/footer information is not included.
        '''
        if section != 'body':
            logging.warning("cap_alpha_seq only includes counts for the body "
                         "section of pages.")
        return self.volume.cap_alpha_seqs(page_select=self.seq).values[0]

    def sentence_count(self, section='default'):
        return self.volume.sentence_counts(page_select=self.seq).values[0]

    def tokenlist(self, **kwargs):
        '''
        Get tokencounts DataFrame.Use args from Volume.tokenlist().
        '''
        return self.volume.tokenlist(page_select=self.seq, **kwargs)

    def end_line_chars(self, **kwargs):
        return self.line_chars(place='end', **kwargs)

    def begin_line_chars(self, **kwargs):
        return self.line_chars(place='begin', **kwargs)

    def line_chars(self, **kwargs):
        '''
        Get a dataframe of character counts at the start and end of lines. Use
        args from Volume.line_chars().
        '''
        return self.volume.line_chars(page_select=self.seq, **kwargs)

    def token_count(self, **kwargs):
        ''' Count total tokens on the page '''
        return self.volume.section_features(page_select=self.seq, feature='tokenCount', **kwargs).values[0]

    def __str__(self):
        if self.volume:
            name = "<page %s of volume %s>" % (self.seq, self.volume.id)
        else:
            name = "<page %s with no volume parent>" % (self.seq)
        return name
