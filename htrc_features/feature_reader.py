from __future__ import unicode_literals
import bz2
try:
    import ujson as json
except ImportError:
    import json
from htrc_features.volume import Volume
from multiprocessing import Pool
import logging
import six


class FeatureReader(object):
    def __init__(self, paths):
        # Check for str type in 3.x, unicode type in 2.x
        if isinstance(paths, six.text_type):
            # Assume only one path was provided, wrap in list
            paths = [paths]

        if type(paths) is list:
            self.paths = paths
            self.index = 0
        else:
            logging.error("Bad input type for feature reader: {}".format(
                type(paths)))

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        ''' Get the next item.
        For iteration, and an easy way to get a volume object with only one
        path

        Note that just calling the volume initializes it.
        '''

        if not hasattr(self, '_volumes'):
            logging.warn("Iterating over the feature_reader is deprecated. Try"
                         " `for vol in feature_reader.volumes()` instead.")
            # Instantiate a generator
            self._volumes = self.volumes()
        return next(self._volumes)

    def volumes(self):
        ''' Generator for returning Volume objects '''
        for path in self.paths:
            # If path is a tuple, assume that the advanced path was also given
            if type(path) == tuple:
                basic, advanced = path
                yield self._volume(basic, advanced_path=advanced)
            else:
                yield self._volume(path)

    def _volume(self, path, compressed=True, advanced_path=False):
        ''' Read a path into a volume.'''
        try:
            if compressed:
                f = bz2.BZ2File(path)
            else:
                f = open(path, 'r+')
            rawjson = f.readline()
            f.close()
        except:
            logging.error("Can't open %s", path)
            return

        # This is a bandaid for schema version 2.0, not over-engineered
        # since upcoming releases of the extracted features
        # dataset won't keep the basic/advanced split

        try:
            # For Python3 compatibility, decode to str object
            if type(rawjson) != str:
                rawjson = rawjson.decode()
            volumejson = json.loads(rawjson)
        except:
            logging.error("Problem reading JSON for %s", path)
            return

        advanced = False
        if advanced_path:
            try:
                if compressed:
                    f = bz2.BZ2File(advanced_path)
                else:
                    f = open(path, 'r+')
                raw_advancedjson = f.readline()
                f.close()

                if type(raw_advancedjson) != str:
                    raw_advancedjson = raw_advancedjson.decode()

                advancedjson = json.loads(raw_advancedjson)
                advanced = advancedjson['features']
            except:
                logging.error("Can't open %s", advanced_path)

        return Volume(volumejson, advanced=advanced)

    def _wrap_func(self, func):
        '''
        Convert a volume path to a volume and run func(vol). For
        multiprocessing.
        TODO: Closures won't work, this is a useless function.
        Remove this after consideration...
        '''
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
        # Match process count to cpu count
        p = Pool()
        # f = self._wrap_func(func)
        results = p.map(map_func, self._mp_paths(), chunksize=5)
        # , callback=callback)
        p.close()
        p.join()
        return results

    def __str__(self):
        return "HTRC Feature Reader with %d paths load" % (len(self.paths))
