from __future__ import unicode_literals
from collections import defaultdict
import pandas as pd
import logging


class TokenList(object):

    def __init__(self, tokendict={}):
        self._rawtokens = defaultdict(lambda: defaultdict(int))

        # Make token list into a long Pandas DataFrame
        wide_df = pd.DataFrame(tokendict).transpose().reset_index()\
                    .rename(columns={"index": "token"})
        self._df = pd.melt(wide_df, id_vars=['token'], var_name="POS",
                           value_name="count").dropna()

    def token_counts(self, case=True, pos=True):
        '''
        Get counts of tokens in the tokenlist.

        case[bool] : Preserve case, or fold.
        pos[bool] : Specify whether to return frequencies per part of speech,
                    or simply by word
        '''

        if case is True and pos is True:
            # Specify columns explicitly to keep consistency
            return self._df[['token', 'POS', 'count']]
        elif case is True and pos is False:
            return self._df.groupby('token').sum().reset_index()
        elif case is False:
            if 'lowercase' not in self._df.columns:
                # We won't create lowercase versions of strings until needed
                # But might as well keep them around
                logging.debug('Adding lowercase column')
                self._df['lowercase'] = self._df['token'].str.lower()

            if pos is True:
                return self._df.groupby(['lowercase', 'POS']).sum()\
                           .reset_index()\
                           .rename(columns={"lowercase": "token"})
            elif pos is False:
                return self._df.groupby('lowercase').sum().reset_index()\
                           .rename(columns={"lowercase": "token"})
        else:
            logging.error("Invalid parameters for token counts")
            return

    def add_TokenList(self, tokenlist):
        ''' Concatenate a second tokenlist '''

        new_df = pd.concat([self._df, tokenlist.token_counts()])\
                   .drop('lowercase', 1).groupby(['token', 'POS']).sum()\
                   .reset_index()
        self._df = new_df

    @property
    def tokens(self):
        ''' Get unique tokens '''
        return self._df['token'].unique().tolist()

    @property
    def count(self):
        ''' Count total tokens on the page '''
        return self._df['count'].astype(int).sum()
