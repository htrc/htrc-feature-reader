from __future__ import unicode_literals
import glob
from htrc_features import FeatureReader
from six import iteritems, PY2, PY3 
import logging
import time
import bz2
import os
import math

def printIdByPath(args):
        fr, path = args
        vol = fr.create_volume(path)

        metadata = (vol.id, vol.year)
        results = vol.term_volume_freqs()
        return (metadata, results)


def main():
    batch_size = 1000;
    paths = glob.glob('data/*.json.bz2')
    paths = paths[0:10]
    logging.basicConfig(filename='features.log',
            format='%(asctime)s:%(levelname)s:%(message)s',
            level=logging.DEBUG)

    f = bz2.BZ2File("term-volume-counts.txt.bz2", "w")
    n = 0 
    m = math.ceil(float(len(paths))/batch_size)

    while (True):
        start = time.time()
        batch, paths = (paths[:batch_size], paths[batch_size:])
        n += 1
        logging.info("Starting batch {0}/{1}".format(n, m))
        feature_reader = FeatureReader(batch)

        results = feature_reader.multiprocessing(printIdByPath)
        
        for vol,result in results:
            for t,c in iteritems(result):
                s = "{0}\t{1}\t{2}\t{3}\n".format(vol[0], vol[1],t,c)
                if PY2:
                    f.write(s.encode('UTF-8'))
                if PY3:
                    f.write(str(s).encode('UTF-8'))

        logging.info("Batch of {0} volumes finished in in {1}s".format(
            batch_size, time.time() - start 
            ))
        logging.debug("Output filesize is currently: {0}Gb".format(
            os.stat('term-volume-counts.txt.bz2').st_size/(1024*1024*1024)
            ))

        if len(paths) == 0:
            break
    
    f.close()


if __name__ == '__main__':
    main()
