HTRC-Features
=============

Tools for working with HTRC Feature Extraction files


## Installation

To install,

    git clone https://github.com/organisciak/htrc-feature-reader.git
    python setup.py install

That's it! This library is written for Python 2.7 and 3.0+.

## Usage

### Reading feature files

The easiest way to start using this library is to use the `FeatureReader`, which takes a list of paths on 

    import glob
    paths = glob.glob('data/*.json.bz2')
    feature_reader = FeatureReader(paths)
    for vol in feature_reader:
	print("%s - %s" % (vol.id, vol.title))

Iterating on the feature reader returns `Volume` objects.
Wherever possible, this library tries not to hold things in memory, so most of the time you want to iterate rather than casting to a list.
In addition to memory issues, since each volume needs to be read from a file and initialized, it will be slow. 
_Woe to whomever tries `list(feature_reader)`_.

The method for creating a path list with 'glob' is just one way to do so.
For large sets, it's better to just have a text file of your paths, and read it line by line.

The feature reader also has a useful method, `multiprocessing(map_func)`, for chunking a running functions across multiple processes.
This is an advanced feature, but extremely helpful for any large scale processing.

### Volume

A volume contains information about the current work and access to the pages of the work.

All the metadata fields from the HTRC JSON file are accessible as properties of the volume object, including _title_, _language_, _imprint_, _oclc_, _pubDate_, and _genre_. The main identifier _id_ and _pageCount_ are also accessible.

    >>> "Volume %s has %s pages in %s" % (vol.id, vol.pageCount, vol.language)
    'Volume loc.ark:/13960/t19k51c6v has 56 pages in eng'

As a convenience, Volume.year returns Volume.pubDate:

     >>> "%s == %s" % (vol.pubDate, vol.year)
    '1917 == 1917'

Like with the feature_reader, it doubles as a generator for pages, and again, it's preferable for speed and memory to iterate over the pages than to read them into a list.

    for page in vol:
         print(page)

This is just a pleasant way to access `vol.pages()`.
If you want to pass arguments to page initialization, such as changing the pages default section from body to 'fullpage', it can be done with `for page in vol.pages(default_section='fullpage')`. 

## Pages and Sections

A page contains the meat of the HTRC's extracted features.
SInce the HTRC provides information by header/body/footer, these are accessed as separate 'sections' with `Page.header`, `Page.body`, and `Page.footer`.

    print("The body has %s lines and %s sentences" % (page.body.lineCount, page.body.sentenceCount))

There is also `Page.fullpage`, which is a section combining the header, footer, and body.
Remember that these need to be added together, which isn't done until the first time `fullpage` is accessed, and in large-scale processing those milliseconds can add up.

    fullpage = page.fullpage
    combined_token_count = page.body.tokenCount + page.header.tokenCount + page.footer.tokenCount
    # check that full page is adding properly
    assert(fullpage.tokenCount == combined_token_count)

For the most part, the properties of the page and section are identical to the HTRC Feature Extraction schema, rather than following Python naming conventions.

A page has a default section, where some features -- such as accessing a token list -- can be accessed without specify the section each time. For example, with the default_section set to 'body', as it is by default, `Page.body.tokenlist` can be accessed with `Page.tokenlist`.

## The fun stuff: playing with token counts and character counts

Token lists are contained in Section.tokenlist.

    tl = page.body.tokenlist
    for token, data in tl.items():
	for part_of_speech, count in data.items():
		print("Token '%s' occurs '%d' as a '%s'" % (token, count, part_of_speech))

These can be manipulates in various ways. You can case-fold, for example:

    tl.token_counts(case=False)

Or, you can combine part of speech counts into a single integer.

    tl.token_counts(pos=False)

To get just the unique tokens, `TokenList.tokens` provides them, though it is just an easy way to get t1.token_counts().keys()

In addition to token lists, you can also access `Section.beginLineChars` and `Section.endLineChars`, which are dictionaries of character counts that occur at the start or end of the line.

### Volume stats collecting

The Volume object has a number of methods for collecting information from all its pages.

    >>vol.tokens_per_page()
    [0, 10, 0, 0, 5, 9, 34, 10, 8, 0, 28, 0, 117, 0, 90, 99, 117, 102, 112, 102, 118, 117, 119, 108, 114, 119, 117, 120, 103, 105, 116, 122, 117, 114, 116, 127, 126, 117, 125, 108, 114, 110, 120, 111, 117, 121, 117, 113, 101, 0, 0, 0, 0, 0, 0, 7]
    >>vol.term_end_line_chars()
    { ... 
     'd': [0, 0, 0, 4, 3, 0, 0, 3, 2, 3, ..,],
     'e': [0, 0, 0, 0, 0, 1, 0, 0, 0, 0, ...],
     'f': [0, 0, 0, 0, 0, 0, 0, 4, 0, 0, ...],
    }
    >>vol.term_page_freqs()
    { ... 
     'when': [0, 0, 0, 4, 3, 0, 0, 3, 2, 3, ..,],
     'thousand': [0, 0, 0, 0, 0, 1, 0, 0, 0, 0, ...],
     'brothers': [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, ...],
    }
    >>vol.term_volume_freq()
    { ... 'yacht': 2, 'Emerson': 2, 'together': 3, 'Fleming': 4, ... }

Volume.term_page_freqs provides a dictionary, where each key is a term and each value is a pageCount length list, showing that key's term count for each page. Volume.term_volume_freqs() simply sums these.

This library sticks to standard Python libraries to keep prerequisites minimal, but if you are crunching stats within Python, you can easily monkey-patch alternate versions of these methods that use numpy.
 
### Multiprocessing

For faster processing, you can write a mapping function for acting on volumes, then pass it to `FeatureReader.multiprocessing`.
This sends out the function to a different process per volume, spawning (CPU_CORES-1) processes at a time.
The map function receives the feature_reader and a volume path, and needs to initialize the volume.

Here's a simple example that returns the term counts for each volume (take note of the first two lines of the functions):

    def printTermCounts(args):
	fr, path = args
        vol = fr.create_volume(path)

	metadata = (vol.id, vol.year)
        return (metadata, results)

    results = feature_reader.multiprocessing(map_func)
    for vol, result in results:
	print("Results from %s (%d)" % vol)
        for term, count in result.items():
            print("%s: %d" % (term, count))


Some rules: results must be serializeable, and the map_func must be accessible from __main__ (basically: no dynamic functions: they should be written plainly in your script).

The results are collected and returned together, so you don't want a feature reader with all 250k files, because the results will be too much memory (depending on how big your result is).
Instead, it easier to initialize feature readers for smaller batches.
