
HTRC-Features
=============

Tools for working with HTRC Feature Extraction files


## Installation

To install,

    git clone https://github.com/organisciak/htrc-feature-reader.git
    cd htrc-feature-reader
    python setup.py install

That's it! This library is written for Python 2.7 and 3.0+.

Two optional modules improve the HTRC-Feature-Reader: `pysolr` allows fetching of metadata, and `ujson` speeds up loading by about 0.4s per file. To install:

    pip install pysolr ujson

## Usage

### Reading feature files

The easiest way to start using this library is to use the `FeatureReader`, which takes a list of paths.


```python
import glob
from htrc_features import FeatureReader
paths = glob.glob('data/*.json.bz2')
# Here we're loading five paths, for brevity
feature_reader = FeatureReader(paths[:5])
i = 0
for vol in feature_reader:
    print("%s - %s" % (vol.id, vol.title))
```

    loc.ark:/13960/t19k51c6v - Think peace, by Abe Cory.
    loc.ark:/13960/t24b3wp3b - Seeing it through. How Britain answered the call. By A. St. John Adcock.
    loc.ark:/13960/t33208m70 - Admission of Kansas.
    loc.ark:/13960/t3gx4xs5p - The streets of New York; a drama in five acts,
    loc.ark:/13960/t3qv43m8f - The courtship of Miles Standish, and other poems / by Henry Wadsworth Longfellow.


Iterating on the feature reader returns `Volume` objects.
Wherever possible, this library tries not to hold things in memory, so most of the time you want to iterate rather than casting to a list.
In addition to memory issues, since each volume needs to be read from a file and initialized, it will be slow. 
_Woe to whomever tries `list(feature_reader)`_.

The method for creating a path list with 'glob' is just one way to do so.
For large sets, it's better to just have a text file of your paths, and read it line by line.

The feature reader also has a useful method, `multiprocessing(map_func)`, for chunking a running functions across multiple processes.
This is an advanced feature, but extremely helpful for any large-scale processing.

### Volume

A volume contains information about the current work and access to the pages of the work.

All the metadata fields from the HTRC JSON file are accessible as properties of the volume object, including _title_, _language_, _imprint_, _oclc_, _pubDate_, and _genre_. The main identifier _id_ and _pageCount_ are also accessible.


```python
"Volume %s has %s pages in %s" % (vol.id, vol.pageCount, vol.language)
```




    u'Volume loc.ark:/13960/t3qv43m8f has 242 pages in eng'



As a convenience, Volume.year returns Volume.pubDate:


```python
"%s == %s" % (vol.pubDate, vol.year)
```




    '1859 == 1859'



Like with the feature_reader, it doubles as a generator for pages, and again, it's preferable for speed and memory to iterate over the pages than to read them into a list.


```python
i = 0
for page in vol:
    i += 1
    print(page)
    # You get the idea, let's stop on the 16th page
    if i == 16:
        break
```

    <page 1 of volume loc.ark:/13960/t3qv43m8f>
    <page 2 of volume loc.ark:/13960/t3qv43m8f>
    <page 3 of volume loc.ark:/13960/t3qv43m8f>
    <page 4 of volume loc.ark:/13960/t3qv43m8f>
    <page 5 of volume loc.ark:/13960/t3qv43m8f>
    <page 6 of volume loc.ark:/13960/t3qv43m8f>
    <page 7 of volume loc.ark:/13960/t3qv43m8f>
    <page 8 of volume loc.ark:/13960/t3qv43m8f>
    <page 9 of volume loc.ark:/13960/t3qv43m8f>
    <page 10 of volume loc.ark:/13960/t3qv43m8f>
    <page 11 of volume loc.ark:/13960/t3qv43m8f>
    <page 12 of volume loc.ark:/13960/t3qv43m8f>
    <page 13 of volume loc.ark:/13960/t3qv43m8f>
    <page 14 of volume loc.ark:/13960/t3qv43m8f>
    <page 15 of volume loc.ark:/13960/t3qv43m8f>
    <page 16 of volume loc.ark:/13960/t3qv43m8f>


This is just a pleasant way to access `vol.pages()`.
If you want to pass arguments to page initialization, such as changing the pages default section from body to 'fullpage', it can be done with `for page in vol.pages(default_section='fullpage')`. 

Finally, if the minimal metadata included with the extracted feature files is insufficient, you can fetch the HTRC's metadata record with `vol.metadata`.
Remember that this calls the HTRC servers for each volume, so can add considerable overhead.


```python
fr = FeatureReader(paths[0:5])
for vol in fr:
    print(vol.metadata['published'][0])
```

    Cincinnati, The Standard publishing company, c1917
    London, New York [etc.,] Hodder and Stoughton, 1915
    Washington, D.C., 1856
    Chicago, The Dramatic publishing company [188-?]
    Boston : Ticknor and Fields, 1859



```python
print("METADATA FIELDS: " + ", ".join(vol.metadata.keys()))
```

    METADATA FIELDS: mainauthor, htrc_genderMale, htrc_gender, htrc_charCount, htrc_pageCount, title_a, title_c, topic, htrc_volumeWordCountBin, authorSort, author2, id, author, sdrnum, topicStr, format, country_of_pub, title_top, topic_subject, _version_, publishDateRange, fullrecord, htrc_wordCount, author_only, topic_name, callnosort, author_top, publishDate, genre, htrc_volumePageCountBin, publisher, ht_id, htsource, language, lccn, title, callnumber, title_ab, era, published, publication_place


_At large-scales, using `vol.metadata` is an impolite and inefficient amount of server pinging; there are better ways to query the API than one-by-one._

## Pages and Sections

A page contains the meat of the HTRC's extracted features.
Since the HTRC provides information by header/body/footer, these are accessed as separate 'sections' with `Page.header`, `Page.body`, and `Page.footer`.



```python
print("The body has %s lines and %s sentences" % (page.body.lineCount, page.body.sentenceCount))
```

    The body has 32 lines and 6 sentences


There is also `Page.fullpage`, which is a section combining the header, footer, and body.
Remember that these need to be added together, which isn't done until the first time `fullpage` is accessed, and in large-scale processing those milliseconds can add up.


```python
fullpage = page.fullpage
combined_token_count = page.body.tokenCount + page.header.tokenCount + page.footer.tokenCount
# check that full page is adding properly
assert(fullpage.tokenCount == combined_token_count)
```

For the most part, the properties of the page and section are identical to the HTRC Extracted Features schema, rather than following Python naming conventions (e.g. CamelCase when convention would expect underscore_separation).

A page has a default section, where some features -- such as accessing a token list -- can be accessed without specify the section each time. For example, with the default_section set to 'body', as it is by default, `Page.body.tokenlist` can be accessed with `Page.tokenlist`.

## The fun stuff: playing with token counts and character counts

Token lists are contained in Section.tokenlist.


```python
tl = page.body.tokenlist
```

A `tokenlist` returns a [Pandas](http://pandas.pydata.org/) DataFrame through `tokenlist.token_counts()`, and provides syntactic access to the vocabulary (`tokenlist.tokens`) and a total token count (`tokenlist.count`).


```python
df = tl.token_counts()
df.sort('count', ascending=False)[:5]
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>token</th>
      <th>POS</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>,</td>
      <td>,</td>
      <td>23</td>
    </tr>
    <tr>
      <th>527</th>
      <td>the</td>
      <td>DT</td>
      <td>14</td>
    </tr>
    <tr>
      <th>298</th>
      <td>and</td>
      <td>CC</td>
      <td>6</td>
    </tr>
    <tr>
      <th>599</th>
      <td>of</td>
      <td>IN</td>
      <td>5</td>
    </tr>
    <tr>
      <th>93</th>
      <td>.</td>
      <td>.</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



These can be manipulated in various ways. You can case-fold, for example:


```python
df = tl.token_counts(case=False)
df.sort('count', ascending=False)[:5]
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>token</th>
      <th>POS</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>,</td>
      <td>,</td>
      <td>23</td>
    </tr>
    <tr>
      <th>73</th>
      <td>the</td>
      <td>DT</td>
      <td>15</td>
    </tr>
    <tr>
      <th>7</th>
      <td>and</td>
      <td>CC</td>
      <td>6</td>
    </tr>
    <tr>
      <th>49</th>
      <td>of</td>
      <td>IN</td>
      <td>6</td>
    </tr>
    <tr>
      <th>3</th>
      <td>.</td>
      <td>.</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



Or, you can combine part of speech counts into a single integer.


```python
df = tl.token_counts(pos=False)
df.sort('count', ascending=False)[:3]
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>token</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>,</td>
      <td>23</td>
    </tr>
    <tr>
      <th>77</th>
      <td>the</td>
      <td>14</td>
    </tr>
    <tr>
      <th>28</th>
      <td>and</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>



To get just the unique tokens, `TokenList.tokens` provides them, though it is just an easy way to get `TokenList.token_counts().keys()`


```python
tl.tokens[:10]
```




    [u',', u'!', u'.', u'STANDISH.', u';', u'and', u'or', u'12', u'THE', u'a']



In addition to token lists, you can also access `Section.beginLineChars` and `Section.endLineChars`, which are dictionaries of character counts that occur at the start or end of the line.

### Volume stats collecting

The Volume object has a number of methods for collecting information from all its pages.


```python
tokens = vol.tokens_per_page()
# Show first 15 pages
tokens[:15]
```




    [11, 15, 0, 0, 21, 50, 96, 99, 5, 0, 77, 137, 145, 146, 149]




```python
end_line_chars = vol.end_line_chars()
print(end_line_chars['!'])[:15]
```

    [0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0]



```python
a = vol.term_page_freqs()
```


```python
a.iloc[1:3, 4:14]
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th>token</th>
      <th>%</th>
      <th>&amp;</th>
      <th>&amp;c</th>
      <th>&amp;e</th>
      <th>'</th>
      <th>''</th>
      <th>'133</th>
      <th>'S</th>
      <th>'er</th>
      <th>'erhead</th>
    </tr>
    <tr>
      <th>page</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>




```python
vol.term_volume_freqs()[:4]
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>token</th>
      <th>POS</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>42</th>
      <td>,</td>
      <td>,</td>
      <td>223</td>
    </tr>
    <tr>
      <th>5637</th>
      <td>the</td>
      <td>DT</td>
      <td>218</td>
    </tr>
    <tr>
      <th>45</th>
      <td>.</td>
      <td>.</td>
      <td>218</td>
    </tr>
    <tr>
      <th>4552</th>
      <td>of</td>
      <td>IN</td>
      <td>213</td>
    </tr>
  </tbody>
</table>
</div>



Volume.term_page_freqs provides a wide DataFrame resembling a matrix, where terms are listed as columns, pages are listed as rows, and the values correspond to the term frequency (or page page frequency with `page_freq=true`).
Volume.term_volume_freqs() simply sums these.
 
### Multiprocessing

For faster processing, you can write a mapping function for acting on volumes, then pass it to `FeatureReader.multiprocessing`.
This sends out the function to a different process per volume, spawning (CPU_CORES-1) processes at a time.
The map function receives the feature_reader and a volume path, and needs to initialize the volume.

Here's a simple example that returns the term counts for each volume (take note of the first two lines of the functions):

```python
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
```

Some rules: results must be serializeable, and the map_func must be accessible from __main__ (basically: no dynamic functions: they should be written plainly in your script).

The results are collected and returned together, so you don't want a feature reader with all 250k files, because the results will be too much memory (depending on how big your result is).
Instead, it easier to initialize feature readers for smaller batches.

