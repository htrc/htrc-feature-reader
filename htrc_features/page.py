from __future__ import unicode_literals
# Because python2's dict.iteritem is python3's dict.item
from six import iteritems
import pandas as pd
from htrc_features.utils import group_tokenlist, SECREF
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
            if self.volume._schema == '1.0':
                tname = 'tokens'
            else:
                tname = 'tokenPosCount'
            tuples = {(int(self.seq), sec, token, pos): {'count': value}
                      for sec in SECREF
                      for token, posvals in iteritems(self._json[sec][tname])
                      for pos, value in iteritems(posvals)
                      }
            self._tokencounts = pd.DataFrame(tuples).transpose()
            self._tokencounts.index.names = ['page', 'section', 'token', 'pos']
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

        if self._lineChars.empty:
            lineChars = {(int(self.seq), sec, place, char): {'count': value}
                         for sec in SECREF
                         for place in ['begin', 'end']
                         for char, value in iteritems(
                             self._json[sec][place+'LineChars'])
                         }
            self._lineChars = pd.DataFrame(lineChars).transpose()
            names = ['page', 'section', 'place', 'character']
            self._lineChars.index.names = names

        df = self._lineChars

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
        if place in ['begin', 'end', 'all']:
            slices.append([place])
        elif place == 'group':
            # It's hard to imagine a use for place='group', but adding for
            # completion
            slices.append(slice(None))

        if slices != [slice(None)] * 3:
                df = df.loc[tuple(slices), ]

        if groups == ['page', 'section', 'place', 'character']:
            return df
        else:
            return df.groupby(groups).sum()

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
