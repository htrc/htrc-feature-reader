''' Process volumes to get begin/end line char counts for each 1/10th of volume. '''

from __future__ import unicode_literals
import glob
from htrc_features import FeatureReader
from generic_processor import generic_processor 
from six import iteritems, PY2, PY3 
import logging
import bz2

def char_counts_by_tenth(args):
        # Initialize volume
        fr, path = args
        vol = fr.create_volume(path)

        if vol.pageCount < 10:
            logging.debug("Returning fale due to short volume %s" % vol.id)
            return
        # Determine ranges of pages for spliting into tenths of a page
        size = float(vol.pageCount) // 10
        limits = [int(round(size*val)) for val in range(0,10)] + [vol.pageCount]
        # Create range tuples
        ranges = [(limits[i], limits[i+1]) for i in range(0, len(limits)-1)]
       
        # Get each chars occurrance at start and end of line by page and
        # sum pages within the ranges
        result_list = []
        for (char, counts) in iteritems(vol.begin_line_chars()):
            tenth_counts = map(lambda(x,y):sum(counts[x:y]), ranges)
            result_list += [('begin', char, tenth_counts)]

        for (char, counts) in iteritems(vol.end_line_chars()):
            tenth_counts = map(lambda(x,y):sum(counts[x:y]), ranges)
            result_list += [('end', char, tenth_counts)]

        return (vol.id, result_list)


def process_results(results, file):
        for result in results:
            if not result:
                continue
            vol, result_list = result
            for place, char, counts in result_list:
                assert(len(counts)==10)
                c = "\t".join([str(n) for n in counts]) 
                s = "{0}\t{1}\t{2}\t{3}\n".format(vol, place, char, c)
                if PY2:
                    file.write(s.encode('UTF-8'))
                if PY3:
                    file.write(str(s).encode('UTF-8'))


def main():
    paths = glob.glob('data/*.json.bz2')
    logging.basicConfig(#filename='features.log',
            format='%(asctime)s:%(levelname)s:%(message)s',
            level=logging.DEBUG)
    generic_processor(char_counts_by_tenth, process_results, paths)


if __name__ == '__main__':
    main()
