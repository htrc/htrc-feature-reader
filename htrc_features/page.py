from __future__ import unicode_literals
# Because python2's dict.iteritem is python3's dict.item
from six import iteritems
import pandas as pd
from htrc_features.utils import group_tokenlist, group_linechars, SECREF
import logging


class Page:

    _tokencounts = pd.DataFrame()
    _lineChars = pd.DataFrame()

    def __init__(self, pageobj, volume, default_section='body'):
        self.volume = volume
        self.default_section = default_section
        self._json = pageobj

        assert(self.default_section in SECREF + ['all', 'group'])

        for (key, item) in iteritems(pageobj):
            # Only add attributes to this object if it's not overwriting
            # a definition
            if not hasattr(self, key):
                setattr(self, key, pageobj[key])

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
        if self.tokenCount == 0:
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

        return group_tokenlist(df, pages=True, section=section, case=case,
                               pos=pos)

    def endLineChars(self, section='default'):
        return self.lineChars(section=section, place='end')

    def beginLineChars(self, section='default'):
        return self.lineChars(section=section, place='begin')

    def lineChars(self, section='default', place='all'):
        '''
        Get a dataframe of character counts at the start and end of lines
        '''
        section = self.default_section if section == 'default' else section

        # If there are no tokens, return an empty dataframe
        if self.tokenCount == 0:
            emptycols = ['page']
            if section in SECREF + ['all']:
                emptycols.append('section')
            if place in ['begin', 'end', 'all']:
                emptycols.append('place')
            emptycols.append('character')
            emptycols.append('count')
            return pd.DataFrame([], columns=emptycols)

        # If there's a volume-level representation, simply pull from that
        elif not self.volume._lineChars.empty:
            try:
                self._lineChars = self.volume._lineChars\
                                      .loc[([int(self.seq)]), ]
            except:
                logging.error("Error subsetting volume DF for seq:{}".format(
                              self.seq))
                return
        
        # Create the internal representation if it does not already exist
        # Since the code is the same, we'll use the definition from Volume
        elif self._lineChars.empty:
            self._lineChars = self.volume._make_line_char_df(self,
                                                             [self._json])
        df = self._lineChars

        return group_linechars(df, section=section, place=place)

    def token_count(self, section):
        ''' Count total tokens on the page '''
        return self.tokenlist(section=section)['count'].sum()

    def __str__(self):
        if self.volume:
            name = "<page %s of volume %s>" % (self.seq, self.volume.id)
        else:
            name = "<page %s with no volume parent>" % (self.seq)
        return name

    @property
    def tokens(self, section='default', case=True):
        ''' Get unique tokens '''
        tokens = self.tokenlist(section=section).index\
                     .get_level_values('token').to_series()
        if case:
            return tokens.unique().tolist()
        else:
            return tokens.str.lower().unique().tolist()

    @property
    def count(self):
        return self._df['count'].astype(int).sum()
