from __future__ import unicode_literals
from collections import defaultdict
from six import iteritems
import htrc_features.utils as utils

class TokenList(object):
    _rawtokens = defaultdict(lambda: defaultdict(int))

    def __init__(self, tokendict={}):
        #self._rawtokens = tokenarr

        for (t, poscounts) in iteritems(tokendict):
            self.add(t, poscounts)

    def token_counts(self, case=True, pos=True):
        tc = [tc for tc in self._token_counts(case=case, pos=pos)]
        # Since _token_counts is a toekn specific iterator, any casefold needs
        # to be done here
        if case is True:
            return dict(tc)
        else:
            if pos is True:
                folded = self._merge_duplicates(tc)
            else:
                folded = defaultdict(int)
                for (t,c) in tc:
                    assert(type(c)==int)
                    folded[t] += c
            return dict(folded)

    def _token_counts(self, case=True, pos=True):
        ''' 
        Generator for pulling (token, count) items.

        Since it's a generator, duplicates are possible.

        pos[bool] : Specify whether to return frequencies per part of speech, or overall
        '''
        for (token, counts) in iteritems(self._rawtokens):
            if case is False:
                token = token.lower()
            if pos is False:
                counts = sum(counts.values())
            yield (token, counts)
    
    def add(self, token, posdict):
        ''' Add a new ( token , { pos : count } ) token to the data, merging when necessary.'''
        for (pos, count) in iteritems(posdict):
            self._rawtokens[token][pos] += count

    def add_TokenList(self, tokenlist):
        for (token, posdict) in iteritems(tokenlist.token_counts()):
            self.add(token, posdict)
    
    @property
    def tokens(self):
        ''' Get unique tokens '''
        return list(self._rawtokens.keys())

#    def _merge_duplicates(tokens):
#        return utils.merge_token_duplicates(tokens)

    @property
    def count():
        counts = [count for (token, count) in self._token_counts(pos=False)]
        return sum(counts)
