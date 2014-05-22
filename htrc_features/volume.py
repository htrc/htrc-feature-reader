from __future__ import unicode_literals
from htrc_features.page import Page, Section
import logging

class Volume(object):
    SUPPORTED_SCHEMA = '1.0'

    def __init__(self, obj):
        # Verify schema version
        if obj['features']['schemaVersion'] != self.SUPPORTED_SCHEMA:
            logging.warn('Schema version of imported (%s) file does not match '
            'the supported version (%s)' % (obj['features']['schemaVersion'],
                                            self.SUPPORTED_SCHEMA) )
        self.id = obj['id']
        self._metadata = obj['metadata']
        self._pages = obj['features']['pages']
        self.pageCount = obj['features']['pageCount']
        self.pageindex = 0
    
    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        for page in self.pages():
            yield page

    def _parseFeatures(self, featobj):
        rawpages = featobj['pages']

    def _parsePages(self, pagesJSON):
        for page in pagesJSON:
            yield Page(page, self)
        
    def pages(self):
        for page in self._pages:
            yield Page(page, self)

    def __str__(self):
        return "<HTRC Volume: %s>" % self.id
