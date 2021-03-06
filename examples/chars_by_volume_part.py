''' Process volumes to get begin/end line char counts for each 1/10th of volume. '''

from __future__ import unicode_literals
import glob
from htrc_features import FeatureReader
from generic_processor import generic_processor 
from six import iteritems, PY2, PY3
from six.moves import map
import logging
import bz2

def char_counts_by_tenth(args):
        # Initialize volume
        fr, path = args
        vol = fr.create_volume(path)
        if not vol:
            return

        if vol.pageCount < 10:
            logging.debug("Returning false due to short volume %s" % vol.id)
            return
        # Determine ranges of pages for spliting into tenths of a page
        size = float(vol.pageCount) // 10
        limits = [int(round(size*val)) for val in range(0,10)] + [vol.pageCount]
        # Create range tuples
        ranges = [(limits[i], limits[i+1]) for i in range(0, len(limits)-1)]
       
        # Get each chars occurrance at start and end of line by page and
        # sum pages within the ranges
        result_list = []

        sum_ranges = lambda x, y : sum(counts[x:y])
        for (char, counts) in iteritems(vol.begin_line_chars()):
            tenth_counts = map(sum_ranges, ranges)
            result_list += [('begin', char, tenth_counts)]
        
        for (char, counts) in iteritems(vol.end_line_chars()):
            tenth_counts = map(sum_ranges, ranges)
            result_list += [('end', char, tenth_counts)]
        return (vol.id, result_list)


def process_results(results, csvwriter):
        for result in results:
            if not result:
                continue
            vol, result_list = result
            for place, char, counts in result_list:
                assert(len(counts)==10)
                l = [vol, place, char] + counts
                if PY2:
                #    file.write(s.encode('UTF-8'))
                    csvwriter.writerow([s.encode('UTF-8') for s in l])
                if PY3:
                    csvwriter.writerow([str(s).encode('UTF-8') for s in l])


def main():
    paths = glob.glob('data/*.json.bz2')
    logging.basicConfig(#filename='features.log',
            format='%(asctime)s:%(levelname)s:%(message)s',
            level=logging.DEBUG)
    generic_processor(char_counts_by_tenth, process_results, paths)


if __name__ == '__main__':
    main()
