from __future__ import unicode_literals
import bz2
from multiprocessing import Pool
import logging
import pandas as pd
import numpy as np
# Because python2's dict.iteritem is python3's dict.item
from six import iteritems
try:
    import ujson as json
except ImportError:
    import json
try:
    import pysolr
except ImportError:
    logging.info("Pysolr not installed.")

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
    def __init__(self, paths, compressed=True):
        self.compressed = compressed
        if type(paths) is list:
            self.paths = paths
        else:
            self.paths = [paths]
        self.index = 0

    def __iter__(self):
        return self.volumes()

    def __str__(self):
        return "HTRC Feature Reader with %d paths load" % (len(self.paths))

    def volumes(self):
        ''' Generator for returning Volume objects '''
        for path in self.paths:
            # If path is a tuple, assume that the advanced path was also given
            if type(path) == tuple:
                basic, advanced = path
                yield self._volume(basic, advanced_path=advanced,
                                   compressed=self.compressed)
            else:
                yield self._volume(path, compressed=self.compressed)

    def jsons(self):
        ''' Generator for returning decompressed, parsed json dictionaries
        for volumes. Convenience function for when the FeatureReader objects
        are not needed. '''
        for path in self.paths:
            # If path is a tuple, assume that the advanced path was also given
            if type(path) == tuple:
                basic, advanced = path
                basicjson = self._read_json(basic, compressed=self.compressed)
                advjson = self._read_json(advanced, compressed=self.compressed)
                yield (basicjson, advjson)
            else:
                yield self._read_json(path, compressed=self.compressed)

    def first(self):
        ''' Return first volume from Feature Reader. This is a convenience
        feature for single volume imports or to quickly get a volume for
        testing.'''
        return next(self.volumes())

    def create_volume(self, path, **kwargs):
        return self._volume(path, **kwargs)

    def multiprocessing(self, map_func, callback=None):
        '''
        Pass a function to perform on each volume of the feature reader, using
        multiprocessing (map), then process the combined outputs (reduce).

        map_func

        Function to run on each individual volume. Takes as input a tuple
        containing a feature_reader and volume path, from which a volume can be
        created. Returns a (key, value) tuple.

        def do_something_on_vol(args):
            fr, path = args
            vol = fr.create_volume(path)
            # Do something with 'vol'
            return (key, value)

        '''
        # Match process count to cpu count
        p = Pool()
        # f = self._wrap_func(func)
        results = p.map(map_func, self._mp_paths(), chunksize=5)
        # , callback=callback)
        p.close()
        p.join()
        return results

    def _mp_paths(self):
        '''
        Package self with paths, so subprocesses can access it
        '''
        for path in self.paths:
            yield (self, path)

    def _read_json(self, path, compressed=True, advanced_path=False):
        ''' Load JSON for a path '''
        try:
            if compressed:
                f = bz2.BZ2File(path)
            else:
                f = open(path, 'r+')
            rawjson = f.readline()
            f.close()
        except IOError:
            logging.exception("Can't read %s. Did you pass the incorrect "
                              "'compressed=' argument?", path)
            raise
        except:
            logging.exception("Can't open %s", path)
            raise

        # This is a bandaid for schema version 2.0, not over-engineered
        # since upcoming releases of the extracted features
        # dataset won't keep the basic/advanced split

        try:
            # For Python3 compatibility, decode to str object
            if type(rawjson) != str:
                rawjson = rawjson.decode()
            volumejson = json.loads(rawjson)
        except:
            logging.exception("Problem reading JSON for %s. One common reason"
                              " for this error is an incorrect compressed= "
                              "argument", path)
            raise
        return volumejson

    def _volume(self, path, compressed=True, advanced_path=False):
        ''' Read a path into a volume.'''
        volumejson = self._read_json(path, compressed)
        if advanced_path:
            advanced = self._read_json(advanced_path, compressed)
            advanced = advanced['features']
        else:
            advanced = False
        return Volume(volumejson, advanced=advanced)

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


class Volume(object):
    SUPPORTED_SCHEMA = ['1.0', '2.0']
    METADATA_FIELDS = [('schemaVersion', 'schema_version'),
                       ('dateCreated', 'date_created'), ('title', 'title'),
                       ('pubDate', 'pub_date'),
                       ('language', 'language'), ('htBibUrl', 'ht_bib_url'),
                       ('handleUrl', 'handle_url'), ('oclc', 'oclc'),
                       ('imprint', 'imprint')]
    ''' List of metadata fields, with their pythonic name mapping. '''

    BASIC_FIELDS = [('pageCount', 'page_count')]
    ''' List of fields which return primitive values in the schema, as tuples
    with (CamelCase, lower_with_under) mapping.
    '''
    _metadata = None
    _tokencounts = pd.DataFrame()
    _line_chars = pd.DataFrame()

    def __init__(self, obj, advanced=False, default_page_section='body'):
        # Verify schema version
        self._schema = obj['features']['schemaVersion']
        if self._schema not in self.SUPPORTED_SCHEMA:
            logging.warn('Schema version of imported (%s) file does not match '
                         'the supported version (%s)' %
                         (obj['features']['schemaVersion'],
                          self.SUPPORTED_SCHEMA))
        self.id = obj['id']
        self._pages = obj['features']['pages']
        self.default_page_section = default_page_section

        # Expand basic values to properties
        for key, pythonkey in self.METADATA_FIELDS:
            if key in obj['metadata']:
                setattr(self, pythonkey, obj['metadata'][key])
        for key, pythonkey in self.BASIC_FIELDS:
            if key in obj['features']:
                setattr(self, pythonkey, obj['features'][key])

        if hasattr(self, 'genre'):
            self.genre = self.genre.split(", ")

        self._has_advanced = False

        if advanced:
            if self._schema != '2.0':
                logging.warn("Only schema 2.0 supports advanced files."
                             "Ignoring")
            else:
                self._has_advanced = True
                # Create an internal dataframe for lineChar counts
                self._line_chars = self._make_line_char_df(advanced['pages'])

                # Count up the capAlphaSeq (longest length of alphabetical
                # sequence of capital letters starting a line
                # TODO

    def __iter__(self):
        return self.pages()

    def __str__(self):
        return "<HTRC Volume: %s>" % self.id

    @property
    def year(self):
        ''' A friendlier name wrapping Volume.pubDate '''
        return self.pub_date

    @property
    def metadata(self):
        if not pysolr:
            logging.error("Cannot retrieve metadata. Pysolr not installed.")
        if not self._metadata:
            logging.debug("Looking up full metadata for {0}".format(self.id))
            solr = pysolr.Solr('http://chinkapin.pti.indiana.edu:9994'
                               '/solr/meta', timeout=10)
            results = solr.search('id:"{0}"'.format(self.id))
            if len(results) != 1:
                logging.error('Unexpected: there were {0} results for {1} '
                              'instead of 1.'.format(
                                  len(results), self.id)
                              )
            result = list(results)[0]
            self._metadata = result
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
        for page in self._pages:
            yield Page(page, self, **kwargs)

    def tokens_per_page(self, **kwargs):
        '''
        Return a one dimension pd.DataFrame of page lengths
        '''
        if 'section' not in kwargs or kwargs['section'] == 'default':
            section = self.default_page_section
        else:
            section = kwargs['section']

        d = [{'page': int(page['seq']), 'section': sec,
              'count':page[sec]['tokenCount']}
             for page in self._pages for sec in SECREF]
        df = pd.DataFrame(d).set_index(['page', 'section']).sort_index()

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
        return [page.empty_line_count(section=section) for page in self.pages()]

    def sentence_counts(self, section='default'):
        ''' Return a list of sentence counts, per page '''
        return [page.sentence_count(section=section) for page in self.pages()]

    def tokenlist(self, pages=True, section='default', case=True, pos=True,
                  page_freq=False):
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
        '''
        if section == 'default':
            section = self.default_page_section

        # Create the internal representation if it does not already
        # exist. This will only need to exist once
        if self._tokencounts.empty:
            self._tokencounts = self._make_tokencount_df(self._pages)

        return group_tokenlist(self._tokencounts, pages=pages, section=section,
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
        if self._schema == '2.0' and not self._has_advanced:
            logging.error("For schema version 2.0, you need load the "
                          "'advanced' file for begin/endLineChars")
            return

        if section == 'default':
            section = self.default_page_section

        if self._line_chars.empty and self._has_advanced:
            logging.error("Something went wrong. Expected Advanced features"
                          " to already be processed")
            return
        elif self._line_chars.empty and not self._has_advanced:
            self._line_chars = self._make_line_char_df(self._pages)

        df = self._line_chars
        return group_linechars(df, section=section, place=place)

    def _make_tokencount_df(self, pages):
        '''
        Returns a Pandas dataframe of:
            page / section / place(i.e. begin/end) / char / count

        Provide an array of pages that hold beginLineChars and endLineChars.
        '''
        if self._schema == '1.0':
            tname = 'tokens'
        else:
            tname = 'tokenPosCount'

        # Make structured numpy array
        # Because it is typed, this approach is ~40x faster than earlier
        # methods
        m = len(pages) * 2000  # Pages * oversized estimate for tokens/page
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
        df.sortlevel(inplace=True)
        return df

    def _make_line_char_df(self, pages):
        '''
        Returns a Pandas dataframe of:
            page / section / place(i.e. begin/end) / char / count

        Provide an array of pages that hold beginLineChars and endLineChars.
        '''

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
                for place in ['begin', 'end']:
                    for char, value in iteritems(page[sec][place+'LineChars']):
                        arr[i] = (page['seq'], sec, place, char, value)
                        i += 1

        # Create a DataFrame
        df = pd.DataFrame(arr[:i]).set_index(['page', 'section',
                                              'place', 'char'])
        df.sortlevel(inplace=True)
        return df


class Page:

    _tokencounts = pd.DataFrame()
    _line_chars = pd.DataFrame()
    BASIC_FIELDS = [('seq', 'seq'), ('tokenCount', '_token_count'),
                    ('languages', 'languages')]
    ''' List of fields which return primitive values in the schema, as tuples
    with (CamelCase, lower_with_under) mapping '''
    SECTION_FIELDS = ['lineCount', 'emptyLineCount', 'sentenceCount']
    ''' Fields that are counted by section.'''

    def __init__(self, pageobj, volume, default_section='body'):
        self.volume = volume
        self.default_section = default_section
        self._json = pageobj

        assert(self.default_section in SECREF + ['all', 'group'])

        for key, pythonkey in self.BASIC_FIELDS:
            if key in pageobj:
                setattr(self, pythonkey, pageobj[key])

        arr = np.zeros((len(SECREF), len(self.SECTION_FIELDS)), dtype='u4')
        for i, sec in enumerate(SECREF):
                for j, stat in enumerate(self.SECTION_FIELDS):
                            arr[i, j] = self._json[sec][stat]
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
        ''' Get or set tokencounts DataFrame

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

        # If there are no tokens, return an empty dataframe
        if self._token_count == 0:
            emptycols = ['page']
            if section in SECREF + ['all']:
                emptycols.append('section')
            emptycols.append('token' if case else 'lowercase')
            if pos:
                emptycols.append('pos')
            emptycols.append('count')
            return pd.DataFrame([], columns=emptycols)

        # If there's a volume-level representation, simply pull from that
        elif not self.volume._tokencounts.empty:
            try:
                df = self.volume._tokencounts.loc[([int(self.seq)]), ]
            except:
                logging.error("Error subsetting volume DF for seq:{}".format(
                              self.seq))
                return

        # Create the internal representation if it does not already
        # This will only need to be created once
        elif self._tokencounts.empty:
            # Using the DF building method from Volume
            self._tokencounts = self.volume._make_tokencount_df([self._json])
            df = self._tokencounts
        else:
            df = self._tokencounts

        return group_tokenlist(df, pages=True, section=section, case=case,
                               pos=pos)

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
