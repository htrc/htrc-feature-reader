from __future__ import unicode_literals

import sys
PY3 = (sys.version_info[0] >= 3)

import logging
import pandas as pd
import numpy as np
import pymarc
from six import iteritems, StringIO, BytesIO
import codecs

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
        groups.append('page')
    if section in ['all'] + SECREF:
        groups.append('section')
    groups.append('token' if case else 'lowercase')
    if pos:
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

    # Add lowercase column. Previously, this was saved internally. However,
    # DataFrame.str.lower() is reasonably fast and the need to call it
    # repeatedly is low, so it is no longer saved.
    # This makes the internal representation more predictable, hopefully
    # avoiding unexpected bugs.
    if not case:
        # Replace our df reference to a copy.
        df = df.copy()
        logging.debug('Adding lowercase column')
        df.insert(len(df.columns), 'lowercase',
                  df.index.get_level_values('token').str.lower())

    # Check if we need to group anything
    if groups == ['page', 'section', 'token', 'pos']:
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
    DL_URL = "http://data.htrc.illinois.edu/htrc-ef-access/get?action=download-ids&id={0}&output=json"

    def __init__(self, paths=None, compressed=True, ids=None):
        self.compressed = compressed
        
        if paths:
            self._online = False
            if type(paths) is list:
                self.paths = paths
            else:
                self.paths = [paths]
        else:
            self.paths = []

        if ids:
            if type(ids) is list:
                self.paths += [self.DL_URL.format(id) for id in ids]
            else:
                self.paths.append(self.DL_URL.format(ids))

        self.index = 0

    def __iter__(self):
        return self.volumes()
    
    def __len__(self):
        return len(self.paths)

    def __str__(self):
        return "HTRC Feature Reader with %d paths load" % (len(self.paths))

    def volumes(self):
        ''' Generator for returning Volume objects '''
        for path in self.paths:
            yield self._volume(path, compressed=self.compressed)

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

    def create_volume(self, path, **kwargs):
        return self._volume(path, **kwargs)

    def _read_json(self, path_or_url, compressed=True):
        ''' Load JSON for a path. Allows remote files in addition to local ones. '''
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

    def _volume(self, path, compressed=True):
        ''' Read a path into a volume.'''
        
        volumejson = self._read_json(path, compressed)
        return Volume(volumejson)

    def _wrap_func(self, func):
        '''
        Convert a volume path to a volume and run func(vol). For
        multiprocessing.
        TODO: Closures won't work, this is a useless function.
        Remove this after consideration...
        '''
        def new_func(path):
            vol = self._volume(path)
            func(vol)
        return new_func
    
    def __repr__(self):
        if len(self.paths) > 1:
            return "<%d path FeatureReader (%s to %s)>" % (len(self.paths), self.paths[0], self.paths[-1])
        elif len(self.paths) == 1:
            return "<Empty FeatureReader>"
        else:
            return "<FeatureReader for %s>" % self.paths[0]
        
    def __str__(self):
        return "<%d path FeatureReader>" % (len(self.paths))

class jsonVolumeReader(object):
    SUPPORTED_SCHEMA = ['3.0']
    METADATA_FIELDS = [('schemaVersion', 'schema_version'),
                       ('dateCreated', 'date_created'),
                       ('title', 'title'),
                       ('pubDate', 'pub_date'),
                       ('language', 'language'),
                       ('htBibUrl', 'ht_bib_url'),
                       ('handleUrl', 'handle_url'),
                       ('oclc', 'oclc'),
                       ('imprint', 'imprint'),
                       ('names', 'names'),
                       ('classification', 'classification'),
                       ('typeOfResource', 'type_of_resource'),
                       ('issuance', 'issuance'),
                       ('genre', 'genre'),
                       ("bibliographicFormat", "bibliographic_format"),
                       ("pubPlace", "pub_place"),
                       ("governmentDocument", "government_document"),
                       ("sourceInstitution", "source_institution"),
                       ("enumerationChronology", "enumeration_chronology"),
                       ("hathitrustRecordNumber", "hathitrust_record_number"),
                       ("rightsAttributes", "rights_attributes"),
                       ("accessProfile", "access_profile"),
                       ("volumeIdentifier", "volume_identifier"),
                       ("sourceInstitutionRecordNumber",
                        "source_institution_record_number"),
                       ("isbn", "isbn"),
                       ("issn", "issn"),
                       ("lccn", "lccn"),
                       ("lastUpdateDate", "last_update_date")
                      ]
    ''' List of metadata fields, with their pythonic name mapping. '''
    
    BASIC_FIELDS = [('pageCount', 'page_count')]
    ''' List of fields which return primitive values in the schema, as tuples
    with (CamelCase, lower_with_under) mapping.
    '''
    
    def __init__(self, obj):
        self._line_chars = pd.DataFrame()
        
        # Verify schema version
        self._schema = obj['features']['schemaVersion']

        if self._schema not in self.SUPPORTED_SCHEMA:
            logging.warning('Schema version of imported (%s) file does not match '
                         'the supported versions (%s). Update your files or use an older '
                         'version of the library' %
                         (obj['features']['schemaVersion'],
                          self.SUPPORTED_SCHEMA))
            
        self._pages = obj['features']['pages']
        
        # Info to keep internal
        #  self._pages
        #  self._schema
        # Info to send to Volume
        #  self.id
        
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
    
    @property
    def token_freqs(self):
        ''' Returns a dataframe of page / section /count '''
        if not self._token_freqs:
            d = [{'page': int(page['seq']), 'section': sec,
                 'count':page[sec]['tokenCount']} for page 
                 in self._pages for sec in SECREF]
            self._token_freqs = pd.DataFrame(d).set_index(['page', 'section']).sort_index()
        return self._token_freqs
    
    @property
    def line_chars(self):
        if self._line_chars.empty:
            self._line_chars = self._make_line_char_df()
        return self._line_chars
        
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
    
    def pages(self, **kwargs):
        for page in self._pages:
            yield Page(page, self, **kwargs)

    def _make_line_char_df(self, pages=False):
        '''
        Returns a Pandas dataframe of:
            page / section / place(i.e. begin/end) / char / count

        Provide an array of pages that hold beginLineChars and endLineChars.
        '''
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
    
class parquetVolumeReader(object):
    pass
    
class Volume(object):
    _metadata = None
    _tokencounts = pd.DataFrame()

    def __init__(self, obj, parser='json', default_page_section='body'):
        self.default_page_section = default_page_section
        
        if parser == 'json':
            self._reader = jsonVolumeReader(obj)
            
        self._update_meta_attrs()
    
    def _update_meta_attrs(self):
        ''' Takes metadata from the reader's metadata variable and assigns it
            to attributes in the Volume '''
        # TODO add allowable fields
        for k, v in self._reader.meta.items():
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
        if not self._metadata:
            logging.debug("Looking up full metadata for {0}".format(self.id))
            data = requests.get(self.ht_bib_url).json()

            record_id = data['items'][0]['fromRecord']
            marc = data['records'][record_id]['marc-xml']

            # Pymarc only reads a file, so stream the text as if it was one
            xml_stream = StringIO(marc)
            xml_record = pymarc.parse_xml_to_array(xml_stream)[0]
            xml_stream.close()

            self._metadata = xml_record
        return self._metadata

    def tokens(self, section='default', case=True):
        ''' Get unique tokens '''
        tokens = self.tokenlist(section=section).index\
                     .get_level_values('token').to_series()
        if case:
            return tokens.unique().tolist()
        else:
            return tokens.str.lower().unique().tolist()

    def pages(self, **kwargs):
        for Page in self._reader.pages(**kwargs):
            yield Page

    def tokens_per_page(self, **kwargs):
        '''
        Return a one dimension pd.DataFrame of page lengths
        '''
        if 'section' not in kwargs or kwargs['section'] == 'default':
            section = self.default_page_section
        else:
            section = kwargs['section']
        
        df = self._reader.token_freqs

        if section in SECREF:
            return df.loc[(slice(None), section), ].reset_index('section',
                                                                drop=True)
        elif section == 'all':
            return df
        elif section == 'group':
            return df.groupby(level='page').sum()

    def line_counts(self, section='default'):
        ''' Return a list of line counts, per page '''
        return [page.line_count(section=section) for page in self.pages()]

    def empty_line_counts(self, section='default'):
        ''' Return a list of empty line counts, per page '''
        return [page.empty_line_count(section=section)
                for page in self.pages()]

    def cap_alpha_seq(self, section='body'):
        logging.warning("At the volume-level, use Volume.cap_alpha_seqs()")
        return self.cap_alpha_seqs(section)

    def cap_alpha_seqs(self, section='body'):
        ''' Return the longest length of consecutive capital letters starting a
        line on the page. Returns a list for all pages. Only includes the body:
        header/footer information is not included.
        '''
        if section != 'body':
            logging.warning("cap_alpha_seq only includes counts for the body "
                         "section of pages.")
        return [page.cap_alpha_seq() for page in self.pages()]

    def sentence_counts(self, section='default'):
        ''' Return a list of sentence counts, per page '''
        return [page.sentence_count(section=section) for page in self.pages()]

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

        # Create the internal representation if it does not already
        # exist. This will only need to exist once
        if self._tokencounts.empty:
            self._tokencounts = self._reader._make_tokencount_df()
        
        if page_select:
            try:
                df = self._tokencounts.xs(self.seq, level='page', drop_level=False)
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

    def line_chars(self, section='default', place='all'):
        '''
        Interface for all begin/end of line character information.
        '''

        if section == 'default':
            section = self.default_page_section

        return group_linechars(self._reader.line_chars, section=section, place=place)
        
    def __str__(self):
        def truncate(s, maxlen):
            if len(s) > maxlen:
                return s[:maxlen].strip() + "..."
            else:
                return s.strip()
        return "<Volume: %s (%s) by %s>" % (truncate(self.title, 30), self.year, truncate(self.author[0], 40))

# TODO remove all JSON-specific code in Page. Likely, best to move as much info to volume
# and just call from above
class Page:

    _tokencounts = pd.DataFrame()
    _line_chars = pd.DataFrame()
    BASIC_FIELDS = [('seq', 'seq'), ('tokenCount', '_token_count'),
                    ('languages', 'languages')]
    ''' List of fields which return primitive values in the schema, as tuples
    with (CamelCase, lower_with_under) mapping '''
    SECTION_FIELDS = ['lineCount', 'emptyLineCount', 'sentenceCount',
                      'capAlphaSeq']
    ''' Fields that are counted by section.'''

    def __init__(self, pageobj, reader, default_section='body'):
        self.reader = reader
        self.volume = reader.volume
        self.default_section = default_section
        self._json = pageobj

        assert(self.default_section in SECREF + ['all', 'group'])

        for key, pythonkey in self.BASIC_FIELDS:
            if key in pageobj:
                setattr(self, pythonkey, pageobj[key])

        arr = np.zeros((len(SECREF), len(self.SECTION_FIELDS)), dtype='u4')
        for i, sec in enumerate(SECREF):
                for j, stat in enumerate(self.SECTION_FIELDS):
                            if stat in self._json[sec]:
                                arr[i, j] = self._json[sec][stat]
                            else:
                                arr[i, j] = 0
        self._basic_stats = pd.DataFrame(arr, columns=self.SECTION_FIELDS,
                                         index=SECREF)

    def tokens(self, section='default', case=True):
        ''' Get unique tokens '''
        tokens = self.tokenlist(section=section).index\
                     .get_level_values('token').to_series()
        if case:
            return tokens.unique().tolist()
        else:
            return tokens.str.lower().unique().tolist()

    def count(self):
        return self._df['count'].astype(int).sum()

    def line_count(self, section='default'):
        return self._get_basic_stat(section, 'lineCount')

    def empty_line_count(self, section='default'):
        return self._get_basic_stat(section, 'emptyLineCount')

    def cap_alpha_seq(self, section='body'):
        ''' Return the longest length of consecutive capital letters starting a
        line on the page. Returns an integer. Only includes the body:
        header/footer information is not included.
        '''
        if section != 'body':
            logging.warning("cap_alpha_seq only includes counts for the body "
                         "section of pages.")
        return self._get_basic_stat('body', 'capAlphaSeq')

    def sentence_count(self, section='default'):
        return self._get_basic_stat(section, 'sentenceCount')

    def _get_basic_stat(self, section, stat):
        if stat is 'all':
            # Return all columns. No publicized currently
            stat = slice(None)

        if section == 'default':
            section = self.default_section

        if section in ['header', 'body', 'footer']:
            return self._basic_stats.loc[section, stat]
        elif section == 'all':
            return self._basic_stats.loc[:, stat]
        elif section == 'group':
            return self._basic_stats.loc[:, stat].sum()

    def tokenlist(self, section='default', case=True, pos=True):
        ''' Get tokencounts DataFrame

        section[string]: Which part of the page to return. In addition to
            'header', 'body', and 'footer', 'all' will return a DataFrame with
            all the sections, 'group' will sum all sections,
            section in ['header', 'footer', 'body'] will return those fields
            section == 'all' will group and sum all sections
            section == 'default' falls back on what the page object has saved

        case[bool] : Preserve case, or fold. To save processing, it's likely
                    more efficient to calculate lowercase later in the process:
                    if you want information for all pages, first collect your
                    information case-sensitive, then fold at the end.

        pos[bool] : Specify whether to return frequencies per part of speech,
                    or simply by word
        '''
        section = self.default_section if section == 'default' else section
        return self.volume.tokenlist(section=section, case=case,
                                     pos=pos,
                                     page_select=int(self.seq))

    def end_line_chars(self, section='default'):
        return self.line_chars(section=section, place='end')

    def begin_line_chars(self, section='default'):
        return self.line_chars(section=section, place='begin')

    def line_chars(self, section='default', place='all'):
        '''
        Get a dataframe of character counts at the start and end of lines
        '''
        section = self.default_section if section == 'default' else section

        # If there are no tokens, return an empty dataframe
        if self._token_count == 0:
            emptycols = ['page']
            if section in SECREF + ['all']:
                emptycols.append('section')
            if place in ['begin', 'end', 'all']:
                emptycols.append('place')
            emptycols.append('character')
            emptycols.append('count')
            return pd.DataFrame([], columns=emptycols)

        # If there's a volume-level representation, simply pull from that
        elif not self.volume._line_chars.empty:
            try:
                self._line_chars = self.volume._line_chars\
                                       .loc[([int(self.seq)]), ]
            except:
                logging.error("Error subsetting volume DF for seq:{}".format(
                              self.seq))
                return

        # Create the internal representation if it does not already exist
        # Since the code is the same, we'll use the definition from Volume
        elif self._line_chars.empty:
            self._line_chars = self.volume._make_line_char_df(self,
                                                              [self._json])
        df = self._line_chars

        return group_linechars(df, section=section, place=place)

    def token_count(self, section='default'):
        ''' Count total tokens on the page '''
        return self.tokenlist(section=section)['count'].sum()

    def __str__(self):
        if self.volume:
            name = "<page %s of volume %s>" % (self.seq, self.volume.id)
        else:
            name = "<page %s with no volume parent>" % (self.seq)
        return name
