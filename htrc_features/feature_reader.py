from __future__ import unicode_literals
import bz2
import json
from htrc_features.volume import Volume
from multiprocessing import Pool
import time
import logging

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
        path i
        
        Note that just calling the volume initializes it.
        '''
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
    
    def _wrap_func(self, func):
        ''' Convert a volume path to a volume and run func(vol). For multiprocessing'''
        def new_func(path):
            vol = self._volume(path)
            func(vol)
        return new_func

    def create_volume(self, path, **kwargs):
        return self._volume(path, **kwargs)

    def _mp_paths(self):
        '''
        Package self with paths, so subprocesses can access it
        '''
        for path in self.paths:
            yield (self, path)

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
        p = Pool() # Match process count to cpu count
        #f = self._wrap_func(func)
        results = p.map(map_func, self._mp_paths(), chunksize=5)#, callback=callback)
        p.close()
        p.join()
        return results
        

    def __str__(self):
        return "HTRC Feature Reader with %d paths load" % (len(self.paths))
