from __future__ import unicode_literals
from htrc_features.page import Page
from htrc_features.utils import group_tokenlist, group_linechars
from six import iteritems
import pandas as pd
import numpy as np
import logging

try:
    import pysolr
except ImportError:
    logging.info("Pysolr not installed.")


class Volume(object):
    SUPPORTED_SCHEMA = ['1.0', '2.0']
    _metadata = None
    _tokencounts = pd.DataFrame()
    _lineChars = pd.DataFrame()

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
        self.pageCount = obj['features']['pageCount']
        self.default_page_section = default_page_section

        # Expand metadata to attributes
        for (key, value) in obj['metadata'].items():
            setattr(self, key, value)

        if hasattr(self, 'genre'):
            self.genre = self.genre.split(", ")

        self.pageindex = 0
        self._has_advanced = False

        if advanced:
            if self._schema != '2.0':
                logging.warn("Only schema 2.0 supports advanced files."
                             "Ignoring")
            else:
                self._has_advanced = True
                # Create an internal dataframe for lineChar counts
                self._lineChars = self._make_line_char_df(advanced['pages'])

                # Count up the capAlphaSeq (longest length of alphabetical
                # sequence of capital letters starting a line
                # TODO

    def __iter__(self):
        return self.pages()

    @property
    def year(self):
        ''' A friendlier name wrapping Volume.pubDate '''
        return self.pubDate

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

    def pages(self, **kwargs):
        for page in self._pages:
            yield Page(page, self, **kwargs)

    def tokens_per_page(self, **kwargs):
        '''
        Return a one dimension pd.Series of page lengths
        '''
        return self.tokenlist().reset_index().groupby(['page']).sum()

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
        ''' The pythonic name for endLineChars '''
        return self.endLineChars(self, **args)

    def endLineChars(self, **args):
        return self.line_chars(place='end', **args)

    def begin_line_chars(self, **args):
        ''' The pythonic name for beginLineChars '''
        return self.beginLineChars(self, **args)

    def beginLineChars(self, **args):
        return self.line_chars(place='begin')

    def line_chars(self, section='default', place='all'):
        '''attr=[endLineChars|beginLineChars]'''
        if self._schema == '2.0' and not self._has_advanced:
            logging.error("For schema version 2.0, you need load the "
                          "'advanced' file for begin/endLineChars")
            return

        if section == 'default':
            section = self.default_page_section

        if self._lineChars.empty and self._has_advanced:
            logging.error("Something went wrong. Expected Advanced features"
                          " to already be processed")
            return
        elif self._lineChars.empty and not self._has_advanced:
            self._lineChars = self._make_line_char_df(self._pages)

        df = self._lineChars
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
                                          "the internal representation allows."
                                          " Email organisciak@gmail.com to let"
                                          " the library author know!")

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
        print(np.__version__)
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

    def __str__(self):
        return "<HTRC Volume: %s>" % self.id
