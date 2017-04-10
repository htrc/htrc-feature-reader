
HTRC-Features [![Build Status](https://travis-ci.org/htrc/htrc-feature-reader.svg?branch=master)](https://travis-ci.org/htrc/htrc-feature-reader) [![PyPI version](https://badge.fury.io/py/htrc-feature-reader.svg)](https://badge.fury.io/py/htrc-feature-reader) [![Anaconda-Server Badge](https://anaconda.org/htrc/htrc-feature-reader/badges/installer/conda.svg)](https://anaconda.org/htrc/htrc-feature-reader)
=============

Tools for working with the [HTRC Extracted Features dataset](https://sharc.hathitrust.org/features), a dataset of page-level text analysis features extracted from from 4.8 million public domain volumes.

This library provides a `FeatureReader` for parsing files, which are handled as `Volume` objects with collections of `Page` objects. Volumes provide access to metadata (e.g. language), volume-wide feature information (e.g. token counts), and access to Pages. Pages allow you to easily parse page-level features, particularly token lists.

This library makes heavy use of [Pandas](pandas.pydata.org), returning many data representations as DataFrames. This is the leading way of dealing with structured data in Python, so this library doesn't try to reinvent the wheel. Since refactoring around Pandas, the primary benefit of using the HTRC Feature Reader is performance: reading the json structures and parsing them is generally faster than custom code. You also get convenient access to common information, such as case-folded token counts or part-of-page specific character counts. Details of the public methods provided by this library can be found in the [HTRC Feature Reader docs](http://htrc.github.io/htrc-feature-reader/htrc_features/feature_reader.m.html).

**Table of Contents**: [Installation](#Installation) | [Usage](#Usage) | 
[Additional Notes](#Additional-Notes)

**Links**: 
[HTRC Feature Reader Documentation](http://htrc.github.io/htrc-feature-reader/htrc_features/feature_reader.m.html) | [HTRC Extracted Features Dataset](https://sharc.hathitrust.org/features)

**Citation**:
Peter Organisciak and Boris Capitanu, "Text Mining in Python through the HTRC Feature Reader," *Programming Historian*, (22 November 2016), http://programminghistorian.org/lessons/text-mining-with-extracted-features.


## Installation

To install,

```bash
    pip install htrc-feature-reader
```

That's it! This library is written for Python 2.7 and 3.0+. For Python beginners, you'll need [pip](https://pip.pypa.io/en/stable/installing/).

Alternately, if you are using [Anaconda](https://www.continuum.io/downloads), you can install with

```bash
    conda install -c htrc htrc-feature-reader
```

This approach is recommended, because it makes sure that some of the hard-to-install dependencies are properly installed.

Given the nature of data analysis, using iPython with Jupyter notebooks for preparing your scripts interactively is a recommended convenience. Most basically, it can be installed with `pip install ipython[notebook]` and run with `ipython notebook` from the command line, which starts a session that you can access through your browser. If this doesn't work, consult the [iPython documentation](http://ipython.readthedocs.org/).

Optional: [installing the development version](#Installing-the-development-version). 

## Usage

*Note: for new Python users, a more in-depth lesson is published by Programming Historian: [Text Mining in Python through the HTRC Feature Reader](http://programminghistorian.org/lessons/text-mining-with-extracted-features). That lesson is also the official citation associated the HTRC Feature Reader library.*

### Reading feature files

The easiest way to start using this library is to use the [FeatureReader](http://htrc.github.io/htrc-feature-reader/htrc_features/feature_reader.m.html#htrc_features.feature_reader.FeatureReader) interface, which takes a list of paths.


```python
import glob
import pandas as pd
from htrc_features import FeatureReader
paths = glob.glob('data/PZ-volumes/*.json.bz2')
# Here we're loading five paths, for brevity
fr = FeatureReader(paths[:5])
for vol in fr.volumes():
    print("%s - %s" % (vol.id, vol.title))
```

    hvd.32044010273894 - The ballet dancer, and On guard,
    njp.32101068970662 - Seven years, and other tales / by Julia Kavanagh.
    nyp.33433074811310 - June / by Edith Barnard Delano ; with illustrations.
    nyp.33433075749246 - You never know your luck; being the story of a matrimonial deserter, by Gilbert Parker ... illustrated by W.L. Jacobs.
    mdp.39015028036104 - Russian short stories, ed. for school use,


Iterating on `FeatureReader` returns `Volume` objects. This is simply an easy way to access `feature_reader.volumes()`.
Wherever possible, this library tries not to hold things in memory, so most of the time you want to iterate rather than casting to a list.
In addition to memory issues, since each volume needs to be read from a file and initialized, it will be slow. 
_Woe to whomever tries `list(FeatureReader.volumes())`_.

The method for creating a path list with 'glob' is just one way to do so.
For large sets, it's better to just have a text file of your paths, and read it line by line.

The feature reader also has a useful method, `multiprocessing(map_func)`, for chunking a running functions across multiple processes.
This is an advanced feature, but extremely helpful for any large-scale processing.

In addition to iterating on `feature_reader.volumes()`, there is a convenient function to grab the first volume in a feature reader. This helps in testing code, and is what we'll do to continue this introduction:


```python
vol = fr.first()
vol
```




    <htrc_features.feature_reader.Volume at 0x1d2ffc52240>



### Volume

A [Volume](http://htrc.github.io/htrc-feature-reader/htrc_features/feature_reader.m.html#htrc_features.feature_reader.Volume) contains information about the current work and access to the pages of the work. All the metadata fields from the HTRC JSON file are accessible as properties of the volume object, including _title_, _language_, _imprint_, _oclc_, _pubDate_, and _genre_. The main identifier _id_ and _pageCount_ are also accessible, and you can find the URL for the Full View of the text in the HathiTrust Digital Library - if it exists - with `vol.handle_url`.


```python
"Volume %s is a %s page text written in %s. You can doublecheck at %s" % (vol.id, vol.page_count,
                                                                          vol.language, vol.handle_url)
```




    'Volume hvd.32044010273894 is a 284 page text written in eng. You can doublecheck at http://hdl.handle.net/2027/hvd.32044010273894'



As a convenience, `Volume.year` returns `Volume.pub_date`:


```python
"%s == %s" % (vol.pub_date, vol.year)
```




    '1901 == 1901'



`Volume` objects have an page genrator method for pages, through `Volume.pages()`. Iterating through pages using this generator only keeps one page at a time in memory, and again it is preferable to reading all the pages into the list at once. Unlike volumes, your computer can probably hold all the pages of a single volume in memory, so it is not dire if you try to read them into a list.

Like with the `FeatureReader`, you can also access the page generator by iterating directly on the object (i.e. `for page in vol`). Python beginners may find that using `vol.pages()` is more clear as to what is happening.


```python
# Let's skip ahead some pages
i = 0
for page in vol:
    # Same as `for page in vol.pages()`
    i += 1
    if i >= 16:
        break
print(page)
```

    <page 00000016 of volume hvd.32044010273894>


If you want to pass arguments to page initialization, such as changing the page's default section from 'body' to 'group' (which returns header+footer+body), it can be done with `for page in vol.pages(default_section='group')`.
     
Finally, if the minimal metadata included with the extracted feature files is insufficient, you can fetch the HTRC's metadata record from the Solr Proxy with `vol.metadata`.
Remember that this calls the HTRC servers for each volume, so can add considerable overhead.


```python
for vol in fr.volumes():
    print(vol.metadata['published'][0])
```

    New York, and London, Harper & brothers, 1901
    London : Hurst and Blackett, 1860
    Boston ; New York : Houghton Mifflin Company, 1916 (Cambridge : The Riverside Press)
    New York, George H. Doran Company [1914]
    Chicago, New York, Scott, Foresman and company [c1919]



```python
print("METADATA FIELDS: " + ", ".join(vol.metadata.keys()))
```

    METADATA FIELDS: _version_, htrc_charCount, title, htrc_volumePageCountBin, publishDate, title_a, mainauthor, author_only, oclc, authorSort, country_of_pub, author, htrc_gender, language, ht_id, publisher, author_top, publishDateRange, htrc_pageCount, title_top, callnosort, publication_place, topic, htsource, htrc_wordCount, title_ab, callnumber, fullrecord, htrc_volumeWordCountBin, format, lccn, genre, htrc_genderMale, topic_subject, topicStr, geographic, published, sdrnum, id


_At large-scales, using `vol.metadata` is an impolite and inefficient amount of server pinging; there are better ways to query the API than one volume at a time. Read about the [HTRC Solr Proxy](https://wiki.htrc.illinois.edu/display/COM/Solr+Proxy+API+User+Guide)._

Another source of bibliographic metadata is the HathiTrust Bib API. You can access this information through the URL returned with `vol.ht_bib_url`:


```python
vol.ht_bib_url
```




    'http://catalog.hathitrust.org/api/volumes/full/htid/mdp.39015028036104.json'



Volumes also have direct access to volume-wide info of features stored in pages. For example, you can get a list of words per page through [Volume.tokens_per_page()](http://htrc.github.io/htrc-feature-reader/htrc_features/feature_reader.m.html#htrc_features.feature_reader.Volume.tokens_per_page). We'll discuss these features [below](#Volume-stats-collecting), after looking first at Pages.

## Pages

A page contains the meat of the HTRC's extracted features, including information for:

- Part of speech tagged token counts, through `Page.tokenlist()`
- Counts of the characters occurred at the start and end of physical lines, though `Page.lineCounts()`
- Sentence counts, line counts (referring to the physical line on the page)
- And more, seen in the docs for [Page](http://htrc.github.io/htrc-feature-reader/htrc_features/feature_reader.m.html#htrc_features.feature_reader.Page)


```python
print("The body has %s lines, %s empty lines, and %s sentences" % (page.line_count(),
                                                                   page.empty_line_count(),
                                                                   page.sentence_count()))
```

    The body has 30 lines, 0 empty lines, and 9 sentences


Since the HTRC provides information by header/body/footer, most methods take a `section=` argument. If not specified, this defaults to `"body"`, or whatever argument is supplied to `Page.default_section`.


```python
print("%s tokens in the default section, %s" % (page.token_count(), page.default_section))
print("%s tokens in the header" % (page.token_count(section='header')))
print("%s tokens in the footer" % (page.token_count(section='footer')))
```

    294 tokens in the default section, body
    3 tokens in the header
    0 tokens in the footer


There are also two special arguments that can be given to `section`: `"all"` and "`group`". 'all' returns information for each section separately, when appropriate, while 'group' returns information for all header, body, and footer combined.


```python
print("%s tokens on the full page" % (page.token_count(section='group')))
assert(page.token_count(section='group') == (page.token_count(section='header') +
                                             page.token_count(section='body') + 
                                             page.token_count(section='footer')))
```

    297 tokens on the full page


Note that for the most part, the properties of the `Page` and `Volume` objects aligns with the names in the HTRC Extracted Features schema, except they are converted to follow [Python naming conventions](https://google.github.io/styleguide/pyguide.html?showone=Naming#Naming): converting the `CamelCase` of the schema to `lowercase_with_underscores`. E.g. `beginLineChars` from the HTRC data is accessible as `Page.begin_line_chars`.

## The fun stuff: playing with token counts and character counts

Token counts are returned by `Page.tokenlist()`. By default, part-of-speech tagged, case-sensitive counts are returned for the body.

The token count information is returned as a DataFrame with a MultiIndex (page, section, token, and part of speech) and one column (count).


```python
print(page.tokenlist()[:3])
```

                               count
    page section token    pos       
    16   body    !        .        1
                 '        ''       1
                 'Flowers NNS      1


`Page.tokenlist()` can be manipulated in various ways. You can case-fold, for example:


```python
df = page.tokenlist(case=False)
print(df[15:18])
```

                                count
    page section lowercase pos       
    16   body    ancient   JJ       1
                 and       CC      12
                 any       DT       1


Or, you can combine part of speech counts into a single integer.


```python
df = page.tokenlist(pos=False)
print(df[15:18])
```

                           count
    page section token          
    16   body    Naples        1
                 November      1
                 October       1


Section arguments are valid here: 'header', 'body', 'footer', 'all', and 'group'


```python
df = page.tokenlist(section="header", case=False, pos=False)
print(df)
```

                            count
    page section lowercase       
    16   header  ballet         1
                 dancer         1
                 the            1


The MultiIndex makes it easy to slice the results, and it is althogether more memory-efficient. If you are new to Pandas DataFrames, you might find it easier to learn by converting the index to columns.


```python
df = page.tokenlist()
# Slicing on Multiindex: get all Signular or Mass Nouns (NN)
idx = pd.IndexSlice
nouns = df.loc[idx[:,:,:,'NN'],]
print(nouns[:3])
print("With index reset: ")
print(nouns.reset_index()[:2])
```

                                   count
    page section token        pos       
    16   body    benefactress NN       1
                 bitterness   NN       1
                 case         NN       1
    With index reset: 
       page section         token pos  count
    0    16    body  benefactress  NN      1
    1    16    body    bitterness  NN      1


If you prefer not to use Pandas, you can always convert the object, with methods like `to_dict` and `to_csv`).


```python
df[:3].to_dict()
```




    {'count': {(16, 'body', '!', '.'): 1,
      (16, 'body', "'", "''"): 1,
      (16, 'body', "'Flowers", 'NNS'): 1}}



To get just the unique tokens, `Page.tokens` provides them as a list.


```python
page.tokens()[:7]
```




    ['!', "'", "'Flowers", "'s", ',', '.', '6']



In addition to token lists, you can also access `Page.begin_line_chars` and `Section.end_line_chars`, which are DataFrames of character counts that occur at the start or end of a line.

### Volume stats collecting

The Volume object has a number of methods for collecting information from all its pages.

`Volume.tokenlist()` works identically the page tokenlist method, except it returns information for the full volume:


```python
# Print case-insensitive occurrances of the word `she`
all_vol_token_counts = vol.tokenlist(pos=False, case=False)
print(all_vol_token_counts.loc[idx[:,'body', 'she'],][:3])
```

                            count
    page section lowercase       
    38   body    she            1
    39   body    she            1
    42   body    she            1


Note that a Volume-wide tokenlist is not crunched until you need it, then it will stay cached in case you need it. If you try to access `Page.tokenlist()` _after_ accessing `Volume.tokenlist()`, the Page object will return that page from the Volume's cached representation, rather than preparing it itself.

`Volume.tokens()`, and `Volume.tokens_per_page()` give easy access to the full vocabulary of the volume, and the token counts per page.


```python
vol.tokens()[:10]
```




    ['"', '.', ':', 'Fred', 'Newton', 'Scott', 'gift', 'i', 'ii', 'iiiiISI']



If you prefer a DataFrame structured like a term-document matrix (where pages are the 'documents'), `vol.term_page_freqs()` will return it.

By default, this returns a page-frequency rather than term-frequency, which is to say it counts `1` when a term occurs on a page, regardless of how much it occurs on that page. For a term frequency, pass `page_freq=False`.


```python
a = vol.term_page_freqs()
print(a.loc[10:11,['the','and','is','he', 'she']])
a = vol.term_page_freqs(page_freq=False)
print(a.loc[10:11,['the','and','is', 'he', 'she']])
```

    token  the  and   is   he  she
    page                          
    10     0.0  1.0  0.0  0.0  0.0
    11     1.0  1.0  1.0  0.0  0.0
    token   the  and   is   he  she
    page                           
    10      0.0  1.0  0.0  0.0  0.0
    11     22.0  7.0  4.0  0.0  0.0


Volume.term_page_freqs provides a wide DataFrame resembling a matrix, where terms are listed as columns, pages are listed as rows, and the values correspond to the term frequency (or page page frequency with `page_freq=true`).
Volume.term_volume_freqs() simply sums these.
 
### Multiprocessing

For faster processing, you can write a mapping function for acting on volumes, then pass it to `FeatureReader.multiprocessing`.
This sends out the function to a different process per volume, spawning (CPU_CORES-1) processes at a time.
The map function receives the feature_reader and a volume path as a tuple, and needs to initialize the volume.

Here's a simple example that returns the term counts for each volume (take note of the first two lines of the function):

```python
def printTokenList(args):
    fr, path = args
    vol = fr.create_volume(path)
    return ('tokens', vol.tokens)

fr  = FeatureReader(paths)
all_tokens = []
mapper = fr.multiprocessing(printTokenList)
for key, result in mapper:
    all_tokens = all_tokens + result
set(all_tokens)
```

Some rules: results must be serializeable, and the map_func must be accessible from __main__ (basically: no dynamic functions: they should be written plainly in your script).

The results are collected and returned together, so you don't want a feature reader with all 4.8 million files, because the results will be too much memory (depending on how big your result is).
Instead, it easier to initialize feature readers for smaller batches.

#### GNU Parallel
As an alternative to multiprocessing in Python, my preference is to have simpler Python scripts and to use GNU Parallel on the command line. To do this, you can set up your Python script to take variable length arguments of feature file paths, and to print to stdout.

This psuedo-code shows how that you'd use parallel, where the number of parallel processes is 90% the number of cores, and 50 paths are sent to the script at a time (if you send too little at a time, the initialization time of the script can add up).

```bash
find feature-files/ -name '*json.bz2' | parallel --eta --jobs 90% -n 50 python your_script.py >output.txt
```

## Additional Notes

### Installing the development version

    git clone https://github.com/htrc/htrc-feature-reader.git
    cd htrc-feature-reader
    python setup.py install

### Iterating through the JSON files

If you need to do fast, highly customized processing without instantiating Volumes, FeatureReader has a convenient generator for getting the raw JSON as a Python dict: `fr.jsons()`. This simply does the file reading, optional decompression, and JSON parsing.

### Downloading files within the library

`utils` includes an Rsyncing utility, `download_file`. This requires Rsync to be installed on your system.

**Usage:**

Download one file to the current directory:
    
```
utils.download_file(htids='nyp.33433042068894')
```

Download multiple files to the current directory:

```
ids = ['nyp.33433042068894', 'nyp.33433074943592', 'nyp.33433074943600']
utils.download_file(htids=ids)
```

Download file to `/tmp`:
```
utils.download_file(htids='nyp.33433042068894', outdir='/tmp')
```

Download file to current directory, keeping pairtree directory structure,
i.e. `./nyp/pairtree_root/33/43/30/42/06/88/94/33433042068894/nyp.33433042068894.json.bz2`:

```
utils.download_file(htids='nyp.33433042068894', keep_dirs=True)
    ```

### Getting the Rsync URL

If you have a HathiTrust Volume ID and want to be able to download the features for a specific book, `hrtc_features.utils` contains an [id_to_rsync](http://htrc.github.io/htrc-feature-reader/htrc_features/utils.m.html#htrc_features.utils.id_to_rsync) function. This uses the [pairtree](http://pythonhosted.org/Pairtree/) library but has a fallback written with that library is not installed, since it isn't compatible with Python 3.


```python
from htrc_features import utils
utils.id_to_rsync('miun.adx6300.0001.001')
```




    'miun/pairtree_root/ad/x6/30/0,/00/01/,0/01/adx6300,0001,001/miun.adx6300,0001,001.json.bz2'



See the [ID to Rsync notebook](examples/ID_to_Rsync_Link.ipynb) for more information on this format and on Rsyncing lists of urls.

There is also a command line utility installed with the HTRC Feature Reader:

```bash
$ htid2rsync miun.adx6300.0001.001
miun/pairtree_root/ad/x6/30/0,/00/01/,0/01/adx6300,0001,001/miun.adx6300,0001,001.json.bz2
```

### Advanced Features

In the beta Extracted Features release, schema 2.0, a few features were separated out to an advanced files. However, *this designation is no longer present starting with schema 3.0*, meaning information like `beginLineChars`, `endLineChars`, and `capAlphaSeq` are always available:


```python
# What is the longest sequence of capital letter on each page?
vol.cap_alpha_seqs()[:10]
```




    [0, 1, 0, 0, 0, 0, 0, 0, 4, 1]




```python
end_line_chars = vol.end_line_chars()
print(end_line_chars.head())
```

                             count
    page section place char       
    2    body    end   -         1
                       :         1
                       I         1
                       f         1
                       t         1



```python
# Find pages that have lines ending with "!"
idx = pd.IndexSlice
print(end_line_chars.loc[idx[:,:,:,'!'],].head())
```

                             count
    page section place char       
    45   body    end   !         1
    75   body    end   !         1
    77   body    end   !         1
    91   body    end   !         1
    92   body    end   !         1


### Testing

This library is meant to be compatible with Python 3.2+ and Python 2.7+. Tests are written for py.test and can be run with `setup.py test`, or directly with `python -m py.test -v`.

If you find a bug, leave an issue on the issue tracker, or contact Peter Organisciak at `organisciak+htrc@gmail.com`.
