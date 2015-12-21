from __future__ import unicode_literals
from htrc_features.page import Page
from htrc_features.utils import group_tokenlist, SECREF
from htrc_features.term_index import TermIndex
from six import iteritems
import pandas as pd
import logging
import time

try:
    import pysolr
except ImportError:
    logging.info("Pysolr not installed.")


class Volume(object):
    SUPPORTED_SCHEMA = ['1.0', '2.0']
    _metadata = None
    _tokencounts = pd.DataFrame()

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
            start = time.time()
            apages = advanced['pages']
            if self._schema != '2.0':
                logging.warn("Only schema 2.0 supports advanced files."
                             "Ignoring")
            else:
                # Merge Advanced features into pages JSON
                for i in range(0, len(self._pages)):
                    if self._pages[i]['seq'] != apages[i]['seq']:
                        logging.warn('Sequence does not match between basic '
                                     'and advanced pages. Skipping for this '
                                     'volume.')
                        logging.debug("Due to limited advanced feature "
                                      "support, this code assumes that basic "
                                      "and advanced features line up. If the "
                                      "seq doesn't match up, it doesn't search"
                                      "for the correct page")
                    else:
                        for sec in ['header', 'body', 'footer']:
                            for key in apages[i][sec].keys():
                                self._pages[i][sec][key] = apages[i][sec][key]
                self._has_advanced = True
                logging.debug("Advanced merge took {}s".format(
                              (time.time()-start)))

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
            if self._schema == '1.0':
                tname = 'tokens'
            else:
                tname = 'tokenPosCount'
            tuples = {(int(page['seq']), sec, token, pos): {'count': value}
                      for page in self._pages
                      for sec in SECREF
                      for token, posvals in iteritems(page[sec][tname])
                      for pos, value in iteritems(posvals)
                      }
            self._tokencounts = pd.DataFrame(tuples).transpose()
            self._tokencounts.index.names = ['page', 'section', 'token', 'pos']

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
        return self._line_chars('endLineChars')

    def begin_line_chars(self, **args):
        return self._line_chars('beginLineChars')

    def _line_chars(self, attr, sec='body'):
        '''attr=[endLineChars|startLineChars]'''
        if self._schema == '2.0' and not self._has_advanced:
            logging.error("For schema version 2.0, you need load the "
                          "'advanced' file for start/endLineChars")
            return
        cp = TermIndex(self.pageCount)
        for (index, page) in enumerate(self.pages()):
            section = getattr(page, sec)
            for (char, count) in iteritems(getattr(section, attr)):
                cp[char][index] = count
        return cp

    def __str__(self):
        return "<HTRC Volume: %s>" % self.id
