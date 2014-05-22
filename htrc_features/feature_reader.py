from __future__ import unicode_literals
import bz2
import json
from htrc_features.volume import Volume

class FeatureReader(object):
    def __init__(self, paths):
        if type(paths) is str:
            # Assume only one path was provided, wrap in list
            paths = [paths]

        self.paths = paths
        self.index = 0

    def __iter__(self):
        return self
    
    def next(self):
        return self.__next__()

    def __next__(self):
        ''' Get the next item.
        For iteration, and an easy way to get a volume object with only one
        path i'''
        if self.index == len(self.paths):
            raise StopIteration
        path = self.paths[self.index]
        vol = self._volume(path)
        self.index += 1
        return vol 

    def volumes(self):
        ''' Generator for return Volume objects '''
        for path in self.paths:
            yield self._volume(path)

    def _volume(self, path, compressed=True):
        ''' Read a path into a volume.'''
        if compressed:
            f = bz2.BZ2File(path)
        else:
            f = open(path, 'r+')
        rawjson = f.readline()
        
        f.close()
        # For Python3 compatibility, decode to str object
        if type(rawjson) != str:
            rawjson = rawjson.decode()
        volumejson = json.loads(rawjson)
        return Volume(volumejson)

    def __str__(self):
        return "HTRC Feature Reader with %d paths load" % (len(self.paths))
