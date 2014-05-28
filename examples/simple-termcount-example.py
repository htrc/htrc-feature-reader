'''
This example shows how to use multiprocessing to write term-counts to an external file.

For brevity, it doesn't do any logging or timing, and is only written for Python2.7. 
'''

from __future__ import unicode_literals
import glob
from htrc_features import FeatureReader
import bz2


def get_term_volume_counts(args):
    ''' Mapping function for what should be done to each volume. '''
    fr, path = args
    vol = fr.create_volume(path)

    metadata = (vol.id, vol.year)
    results = vol.term_volume_freqs()
    return (metadata, results)


def main():
    # Get a list of json.bz2 files to read
    paths = glob.glob('data/*.json.bz2')
    paths = paths[0:4] # Truncate list for example

    # Open file for writing results
    f = bz2.BZ2File('term_volume_counts.bz2', "w")

    # Start a feature reader with the paths and pass the mapping function
    feature_reader = FeatureReader(paths)
    results = feature_reader.multiprocessing(get_term_volume_counts)

    # Save the results
    for vol, result in results:
        for t,c in result.iteritems(): # result.items() in python3
            s = "{0}\t{1}\t{2}\t{3}\n".format(vol[0], vol[1],t,c)
            f.write(s.encode('UTF-8')) # For python3, use str(s)

    f.close()


if __name__ == '__main__':
    main()
