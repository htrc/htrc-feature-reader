from __future__ import unicode_literals
from htrc_features.page import Page
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

    def __init__(self, obj, advanced=False):
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
                              (time.time()-start)/1000))

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

    # def _parseFeatures(self, featobj):
    #    rawpages = featobj['pages']

    def pages(self, **kwargs):
        for page in self._pages:
            yield Page(page, self, **kwargs)

    def tokens_per_page(self, **kwargs):
        l = [0] * self.pageCount
        for (index, page) in enumerate(self.pages(**kwargs)):
            try:
                l[index] = page.total_tokens()
            except:
                logging.error("Seq and pageCount don't match in %s" % self.id)
        return l

    def term_page_freqs(self, page_freq=True, case=True):
        ''' Return a term frequency x page matrix, or optionally a
        page frequency x page matrix '''
        all_page_dfs = self._frequencies(page_freq, case)
        return all_page_dfs.groupby(['token', 'page']).sum().reset_index()\
                           .pivot(index='page', columns='token',
                                  values='count')\
                           .fillna(0)

    def term_volume_freqs(self, page_freq=True, pos=True, case=True):
        ''' Return a list of each term's frequency in the entire volume '''
        df = self._frequencies(page_freq, pos, case)
        groups = ['token'] if not pos else ['token', 'POS']
        return df.drop(['page'], axis=1).groupby(groups).sum().reset_index()\
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

    def _frequencies(self, page_freq=True, pos=True, case=True):
        ''' Build a long dataframe with rows for each token/POS/count/page.

        page_freq[bool] : Whether to count page frequency (1 if it occurs on
        the page, else 0) or a term frequency (counts for the term, per page)
        '''
        if not hasattr(self, '_all_pages_count'):
            all_page_dfs = []
            for page in self.pages():
                tl = page.tokenlist.token_counts(pos=True, case=True)
                tl['page'] = page.seq
                all_page_dfs.append(tl)

            self._all_page_counts = pd.concat(all_page_dfs)

        # Only crunch lowercase when needed, but then keep it internally
        if case and 'lowercase' not in self._all_page_counts.columns:
            logging.debug('Adding lowercase column')
            self._all_page_counts['lowercase'] =\
                self._all_page_counts['token'].str.lower()

        all_pages = self._all_page_counts
        groups = ['page', ('token' if case else 'lowercase')]
        if pos:
            groups.append('POS')

        # TOFIX: Using sum() is pointless if page_freq is True
        all_pages = all_pages.groupby(groups).sum().reset_index()

        if not case:
            return all_pages.rename(columns={"lowercase": "token"})

        if page_freq:
            all_pages['count'] = 1

        return all_pages

    def __str__(self):
        return "<HTRC Volume: %s>" % self.id
