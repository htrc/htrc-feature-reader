''' More complex version of examples/simple-termcount-example.py '''

from __future__ import unicode_literals
import glob
from htrc_features import FeatureReader
from generic_processor import generic_processor 
from six import iteritems, PY2, PY3 
import logging
import bz2

def get_term_volume_counts(args):
        fr, path = args
        vol = fr.create_volume(path)

        metadata = (vol.id, vol.year)
        results = vol.term_volume_freqs()
        return (metadata, results)

def process_results(results, file):
        for vol, result in results:
            for t,c in iteritems(result):
                s = "{0}\t{1}\t{2}\t{3}\n".format(vol[0], vol[1],t,c)
                if PY2:
                    file.write(s.encode('UTF-8'))
                if PY3:
                    file.write(str(s).encode('UTF-8'))


def main():
    paths = glob.glob('data/*.json.bz2')
    logging.basicConfig(#filename='features.log',
            format='%(asctime)s:%(levelname)s:%(message)s',
            level=logging.DEBUG)
    generic_processor(get_term_volume_counts, process_results, paths, 'term-volume-counts.txt.bz2')


if __name__ == '__main__':
    main()
