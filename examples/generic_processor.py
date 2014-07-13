'''
A re-usable function for running multiprocessing with logging.
'''

from htrc_features import FeatureReader
import time
import bz2
import math
import logging
import os
import csv
import sys

def generic_processor(map_func, result_func, paths, outpath=None, batch_size=1000):
    if outpath:
        f = bz2.BZ2File(outpath, "w")
    else:
        f = sys.stdout
    csvf = csv.writer(f)
    n = 0 
    m = math.ceil(float(len(paths))/batch_size)

    logging.info("Script started")

    while (True):
        start = time.time()
        batch, paths = (paths[:batch_size], paths[batch_size:])
        n += 1
        logging.info("Starting batch {0}/{1}".format(n, m))
        feature_reader = FeatureReader(batch)

        results = feature_reader.multiprocessing(map_func)
        result_func(results, csvf)

        logging.info("Batch of {0} volumes finished in in {1}s".format(
            len(batch), time.time() - start 
            ))

        if outpath:
            logging.debug("Output filesize is currently: {0}Gb".format(
                os.stat(outpath).st_size/(1024**3)
                ))

        if len(paths) == 0:
            break
   
    logging.info("Script done")
    f.close()
