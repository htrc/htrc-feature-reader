from __future__ import unicode_literals

import logging
import pandas as pd
import numpy as np
import pymarc
from six import iteritems, StringIO, BytesIO
import codecs
import os
import warnings
import tempfile

from htrc_features import utils
from htrc_features import parsers, resolvers, transformations
from htrc_features.parsers import JsonFileHandler, BaseFileHandler, ParquetFileHandler, MissingDataError, SECREF

try:
    import rapidjson as json
except ImportError:
    import json
    
import requests

from urllib.request import urlopen as _urlopen
from urllib.parse import urlparse as parse_url
from urllib.error import HTTPError

import bz2

class MissingFieldError(Exception):
    pass

def group_tokenlist(in_df, pages=True, section='all', case=True, pos=True,
                    page_freq=False, pagecolname='page', indexed = True):
    
    '''
        Return a token count dataframe with requested folding.

        pages[bool]: If true, keep pages. If false, combine all pages.
        section[string]: 'header', 'body', 'footer' will only return those
            sections. 'all' will return all info, unfolded. 'group' combines
            all sections info. If in_df has no section column,
            this arg is ignored,
        case[bool]: If true, return case-sensitive token counts.
        pos[bool]: If true, return tokens facets by part-of-speech.
        page_freq[bool]: If true, will simply count whether or not a token is
        on a page. Defaults to false.
        pagecolname[string]: Name of the page column. Only used if treating
            a different column like pages (e.g. chunks)
    '''
    groups = []
    if pages:
        assert pagecolname in in_df.index.names
        groups.append(pagecolname)

    if 'section' not in in_df.index.names:
        section = 'ignore'

    if section == 'all':
        groups.append('section')
    elif section in SECREF:
        assert 'section' in in_df.index.names
        groups.append('section')
        
    groups.append('token' if case else 'lowercase')

    if pos:
        assert 'pos' in in_df.index.names
        groups.append('pos')

    if in_df.empty:
        return pd.DataFrame([], columns=groups)

    if section in ['all', 'group', 'ignore']:
        df = in_df
    elif section in SECREF:
        idx = pd.IndexSlice
        try:
            df = in_df.loc[idx[:, section, :, :], ]
        except KeyError:
            logging.debug("Section {} not available".format(section))
            df = pd.DataFrame([], columns=groups+['count'])\
                   .set_index(groups)
            return df
    else:
        logging.error("Invalid section argument: {}".format(section))
        return

    if not case and 'lowercase' not in in_df.index.names:
        # Replace our df reference to a copy.
        df = df.copy()
        logging.debug('Adding lowercase column')
        df.insert(len(df.columns), 'lowercase',
                  df.index.get_level_values('token').str.lower())
    elif case:
        assert 'token' in in_df.index.names

    # Check if we need to group anything
    if groups == in_df.index.names:
        if page_freq:
            pd.options.mode.chained_assignment = None
            df['count'] = 1
            pd.options.mode.chained_assignment = 'warn'
        return df
    else:
        if not page_freq:
            return df.reset_index().groupby(groups).sum()[['count']]
        elif page_freq and 'page' in groups:
            df = df.reset_index().groupby(groups).sum()[['count']]
            pd.options.mode.chained_assignment = None
            df['count'] = 1
            pd.options.mode.chained_assignment = 'warn'
            return df
        elif page_freq and 'page' not in groups:
            # We'll have to group page-level, then group again
            def set_to_one(x):
                x['count'] = 1
                return x
            return df.reset_index().groupby([pagecolname]+groups).apply(set_to_one)\
                     .groupby(groups).sum()[['count']]

def fold_pages(page_list, chunkname):
    '''
    Fold the tokenlist from a provided list of page tokenlists,
    replacing the page with a named 'chunk'
    '''
    chunk = pd.concat(page_list, sort=False)
    indexnames = chunk.index.names
    newindex = [v if v != 'page' else 'chunk'  for v in indexnames]
    
    chunk['chunk'] = chunkname
    grouped = chunk.reset_index().groupby(newindex)[['count']].sum()
    return grouped

def default_resolver(id, path, format, dir):
    if (id is None) or (path is not None):
        return "path"
    else:
        guess = filename_or_id(id)
        
    ### First arg.
    if guess == "filename":
        return "path"
    elif guess == "id" and format == "json":
        return "locally_cached_http"
    elif guess == "id" and format == "parquet" and dir is not None:      
        return "local"
    
    raise AttributeError("No sensible default for format of {} with ids like {}".format(format, id))


def group_linechars(df, section='all', place='all'):

    # Set up grouping
    groups = ['page']
    if section in SECREF + ['all']:
        groups.append('section')
    if place in ['begin', 'end', 'all']:
        groups.append('place')
    groups.append('character')

    # Set up slicing
    slices = [slice(None)]
    if section in ['all', 'group']:
        slices.append(slice(None))
    elif section in SECREF:
        slices.append([section])
    if place in ['begin', 'end']:
        slices.append([place])
    elif place in ['group', 'all']:
        # It's hard to imagine a use for place='group', but adding for
        # completion
        slices.append(slice(None))

    if slices != [slice(None)] * 3:
            df = df.loc[tuple(slices), ]

    if groups == ['page', 'section', 'place', 'character']:
        return df
    else:
        return df.groupby(groups).sum()[['count']]

# CLASSES
class FeatureReader(object):

    def __init__(self, ids=None, paths=None, dir = None, format = "default",
                 id_resolver = None,
                 compression = "default",
                 **kwargs):
        '''A reader for Extracted Features Dataset files.
        
        parser: a VolumeParser class, or a string for a built in class
        (e.g.'json' or 'parquet').
        
        ids: HathiTrust IDs. Preferred to `paths`. By default will
        download json files behind the scenes.

        paths: Filepaths to dataset files.
 
        format: "json", "parquet", or "default". ("default" is JSON,
        but will inherit the format from a passed id_resolver.) For
        custom use, this can also be a class factory that inherits
        from BaseFileHandler.

        id_resolver: The method used to resolve filenames. The most common options
        are 'local', 'http', 'stubbytree', or 'pairtree.'

        The other args are passed to the parser and its id resolver.

        `dir`: The location for local files, stubbytree root, pairtree root, etc.

        '''
        
        # only one of paths or ids can be selected - otherwise it's not clear what to iterate over. 
        assert (paths or ids) and not (paths and ids)

        if (paths):
            self.id_resolver = "path"

        self.dir = dir
        self.format = format
        self.id_resolver = id_resolver

        # Define paths as ids with "path" resolution.        
        if paths is not None:
            ids = paths
            paths = None
        
        self.ids = ids

        if self.ids and type(self.ids) is not list:
            logging.warning("You have passed a single items to 'ids'"
                            "or 'paths' in a FeatureReader initialization."
                            "Consider calling 'volume' directly.")
            self.ids = [self.ids]
                            
        self.index = 0
        self.parser_kwargs = kwargs
        self.compression = compression
            
    def __iter__(self):
        return self.volumes()
    
    def __len__(self):
        return len(self.ids)

    def __str__(self):
        return "HTRC Feature Reader with %d paths loaded" % (len(self.paths))

    def volumes(self):
        ''' Generator for returning Volume objects '''
        for id in self.ids:
            vol = Volume(id=id, format = self.format,
                         id_resolver=self.id_resolver, dir = self.dir,
                         compression = self.compression,
                         **self.parser_kwargs)
            # Learn the resolver and formats from the Volume instance,
            # and keep what we've learned for all later volumes we make.
            yield vol
            if self.format == 'default':
                self.format = vol.id_resolver.format
            if self.id_resolver == 'default':
                self.id_resolver = vol.id_resolver
            if self.compression == 'default':
                self.compression = vol.id_resolver.compression
            if self.dir != vol.id_resolver.dir:
                self.dir = vol.id_resolver.dir
                
    def jsons(self, object = True, decompress = True):
        ''' 

        Generator for returning decompressed, parsed json dictionaries
        for volumes. Convenience function for when the FeatureReader objects
        are not needed. 

        object defines whether to return parsed json, or simply json strings.

        decompress only applies when object is false; whether to bother 
        decompressing the binary text.

        '''
        
        # Can't avoid decompressing if there's an object involved.
        assert ((object and decompress) or (not object))
        
        for id in self.ids:
            vol = Volume(id, format = self.format, id_resolver = self.id_resolver,
                         load = False, dir=self.dir, **self.parser_kwargs)
            if decompress == True:
                yield vol.parser._parse_json(object = object)
            else:
                yield vol.parser._parse_json(object = object, compression = None)               

    def first(self):
        ''' Return first volume from Feature Reader. This is a convenience
        feature for single volume imports or to quickly get a volume for
        testing.'''
        return next(self.volumes())
    
    def __repr__(self):
        if len(self.ids) > 1:
            return "<%d path FeatureReader (%s to %s)>" % (len(self.ids), self.ids[0], self.ids[-1])
        elif len(self.ids) == 0:
            return "<Empty FeatureReader>"
        else:
            return "<FeatureReader for %s>" % self.ids[0]
        
    def __str__(self):
        return "<%d path FeatureReader>" % (len(self.ids))

def filename_or_id(string):
    """
    Determine based on suffix is something is a file or an ide.
    """
    for ending in [".gz", ".bz2", ".json", ".parquet"]:
        if string.endswith(ending):
            return "filename"
    if "." in string[:6]:
        # All Hathi ids have dots in them.
        return "id"
    raise NameError("Can't determine if {} is supposed to be a filename or an id.".format(string) + 
                    "Please explicitly name your first argument to Volume 'id' or 'path'.")

def retrieve_parser(id, format, id_resolver, compression, dir=None,
                    file_handler=None, **kwargs):
    
    """
    Retrieve a parser based on kwargs.
    """

    if file_handler:
        Handler = file_handler
    elif format == "json":
        Handler = parsers.JsonFileHandler
    elif format == "parquet":
        Handler = parsers.ParquetFileHandler
    else:
        raise NotImplementedError("Must pass a format. Currently 'json' and 'parquet' are supported.")
    
    return Handler(id, id_resolver = id_resolver, dir = dir,
                   compression = compression, **kwargs)

def create_resolver(id_resolver, dir, format,
                    compression, mode = 'rb'):

    if isinstance(id_resolver, resolvers.IdResolver):
        # We have a fully-formed resolver already
        return id_resolver
    if isinstance(id_resolver, str):
        id_resolver = resolvers.resolver_nicknames[id_resolver]
    
    try:
        assert(issubclass(id_resolver, resolvers.IdResolver))
    except:
        # There are class issues with the FallbackResolver, so ignore this check for now
        # and assume that it was an instance of something
        return id_resolver
        
    return id_resolver(dir = dir, mode = mode, format = format, compression = compression)
        
class Volume(object):
    def __init__(self, id = None,
                    format = "default",
                    id_resolver = None,
                    default_page_section='body',
                    path = None,
                    compression = 'default',
                    dir = None,
                    file_handler = None,
                     **kwargs):
        '''
        The Volume allows simplified, Pandas-based access to the HTRC
        Extracted Features files.
        
        This class recruits a VolumeParser class to read its data and parse
        it to up to four dataframes:
            - tokencounts : Case-sensitive, POS-tagged token counts,
            per section (body/header/footer) and page.
            - metadata : Data about the book.
            - page features : Features specific to a pages.
            - section features : Features specific to sections
            (body/header/footer) of each page.
        
        Most of the time, the format will be the default json parser, which
        deals with the format that HTRC distributes, but for various reasons
        you may want to use or write alternative formats. e.g. perhaps you
        just need a nice view into the metadata, or hope for quicker access
        specifically to tokencounts.
        '''
        self._tokencounts = pd.DataFrame()
        self._line_chars = pd.DataFrame()
        self._page_features = pd.DataFrame()
        self._section_features = pd.DataFrame()
        self._extra_metadata = None

        if "resolver" in kwargs:
            raise NameError("Caught 'resolver' arg: did you mean to pass 'id_resolver'?")
        
        if id == False:
            warnings.warn("Please use None to indicate lack of an id", DeprecationWarning)
            id = None
        if path == False:
            warnings.warn("Please use None to indicate lack of a path", DeprecationWarning)
            path = None
            
        if 'compressed' in kwargs:
            warning.warn("Use 'compression' argument. `compressed` has been deprecated.")
            if kwargs['compressed'] == False:
                compression = None
            elif kwargs['compressed'] == True:
                compression = 'bz2'
                
        if format == "default":
            # Allow learning the format from the resolver.
            if isinstance(id_resolver, resolvers.IdResolver):
                format = id_resolver.format
                if (dir is not None) and (dir != id_resolver.dir):
                    warn.warning("You provided a dir argument ({}) and id_resolver instance with a "
                                 "different dir ({}).".format(dir, id_resolver.dir))
                # Why accept the resolver's dir? Anyone know?
                dir = id_resolver.dir
            else:
                # The actual default
                format = "json"
                
        assert format in ["json", "parquet"]

        if compression == 'default':
            if isinstance(id_resolver, resolvers.IdResolver):
                compression = id_resolver.compression
            else:
                if id_resolver == 'http':
                    compression = "bz2"
                elif format == 'parquet':
                    compression = 'snappy'
                elif format == 'json':
                    compression = "bz2"

        if id_resolver is None:
            id_resolver = default_resolver(id, path, format, dir)
            
        if id_resolver == "locally_cached_http":
            if dir is None:
                dir = tempfile.gettempdir()

        id_resolver = create_resolver(id_resolver, dir = dir,
                                   format = format,
                                   compression = compression,
                                   mode = 'rb')
        
        self.id_resolver = id_resolver
        if path:
            id = path
        
        self.default_page_section = default_page_section
        
        # Sanity checks.

        if isinstance(id_resolver, resolvers.IdResolver):
            if id_resolver.format != format:
                raise TypeError("You have passed an id resolver for {} files,"
                                "but requested {} files".format(id_resolver.format, format))
        
        self.parser = retrieve_parser(id, format, id_resolver, compression, dir, 
                                      file_handler=file_handler, **kwargs)
        
        self.args = kwargs
        self._update_meta_attrs()
    
    def _update_meta_attrs(self):
        ''' Takes metadata from the parser's id metadata variable and 
        assigns it to attributes in the Volume '''
        for k, v in self.parser.meta.items():
            setattr(self, k, v)

    def __iter__(self):
        return self.pages()

    def __str__(self):
        try:
            return "<HTRC Volume: %s - %s (%s)>" % (self.id, self.title, self.year)
        except:
            return "<HTRC Volume: %s>" % self.id

    def _repr_html_(self):
        html_template = "<strong><a href='%s'>%s</a></strong> by <em>%s</em> (%s, %s pages) - <code>%s</code>"
        try:
            return html_template % (self.handle_url, self.title, ", ".join(self.author), self.year, self.page_count, self.id)
        except:
            try:
                return "<strong><a href='%s'>%s</a></strong>" % (self.handle_url, self.title)
            except:
                return "Unloaded volume <strong>%s</strong>" % (self.id)                

    def page_features(self, feature='all', page_select=False):
        if self._page_features.empty:
            self._page_features = self.parser._make_page_feature_df()
            
        return self._get_basic_feature(self._page_features, section='all', feature=feature, page_select=page_select)
    
    def section_features(self, feature='all', section='default', page_select=False):
        if self._section_features.empty:
            self._section_features = self.parser._make_section_feature_df()
        return self._get_basic_feature(self._section_features, section=section, feature=feature, page_select=page_select)

    def _get_basic_feature(self, df, feature='all', section='default', page_select=False):
        '''Selects a basic feature from a page_features or section_features dataframe'''
        
        if section == 'default':
            section = self.default_page_section
        
        if page_select:
            df = df.xs(page_select, level='page', drop_level=False)
        
        if feature is not 'all':
            df = df[feature]

        if section in ['header', 'body', 'footer']:
            return df.xs(section, level='section')
        elif section == 'all':
            return df
        elif section == 'group':
            return df.groupby(level='page').sum()
        else:
            raise Exception("Bad Section Arg")
        
    def write(self, volume, **kwargs):
        self.parser.write(volume, **kwargs)
        self._update_meta_attrs()
        
    @property
    def year(self):
        ''' A friendlier name wrapping Volume.pubDate '''
        return self.pub_date
    
    @property
    def author(self):
        ''' A friendlier name wrapping Volume.names or Volume.contributor.
        Returns a list.
        '''
        if hasattr(self, "contributor"):
            author = self.contributor
        elif hasattr(self, "names"):
            author = self.names
        else:
            raise KeyError("This volume does not have metadata for 'names' or 'contributor'")
        
        if author is None:
            return []
        elif type(author) is not list:
                author = [author]
        return author

    @property
    def metadata(self):
        """
        Fetch additional information about a volume from the HathITrust Bibliographic API.

        See: https://www.hathitrust.org/bib_api

        return: A `pymarc` record. See pymarc's documentation for details on using it.
        """
        if not self._extra_metadata:
            logging.debug("Looking up full metadata for {0}".format(self.id))
            data = requests.get(self.ht_bib_url).json()

            record_id = data['items'][0]['fromRecord']
            marc = data['records'][record_id]['marc-xml']

            # Pymarc only reads a file, so stream the text as if it was one
            xml_stream = StringIO(marc)
            xml_record = pymarc.parse_xml_to_array(xml_stream)[0]
            xml_stream.close()

            self._extra_metadata = xml_record
        return self._extra_metadata

    def tokens(self, section='default', case=True, page_select=False, min_count=1):
        '''
        Get unique tokens as a set. Setting min_count increases processing.
        '''
        tokencolname = 'token' if case else 'lowercase'
        tl = self.tokenlist(case=case, page_select=page_select).reset_index()
        if min_count > 1:
            matches = tl.groupby(tokencolname)['count'].transform('sum').ge(min_count)
            tl = tl[matches]
        return set(tl[tokencolname])

    def pages(self, **kwargs):
        ''' Iterate through Page objects with a reference to this class.
            This is mostly a convenience these days - logic exists in
            the Volume class.
        '''
        for seq in self.parser.seqs:
            yield Page(seq, self, **kwargs)

    def tokens_per_page(self, **kwargs):
        '''
        Return a Series of page lengths
        '''
        try:
            tokencounts = self.section_features(feature='tokenCount')
        except MissingDataError:
            tokencounts = self.tokenlist(pos=False, case=False).groupby(level=self._pagecolname).sum()['count']
        return tokencounts

    def line_counts(self, **kwargs):
        ''' Return a Series of line counts, per page '''
        return self.section_features(feature='lineCount', **kwargs)

    def empty_line_counts(self, section='default', **kwargs):
        ''' Return a list of empty line counts, per page '''
        return self.section_features(feature='emptyLineCount', **kwargs)

    def cap_alpha_seq(self, **kwargs):
        logging.warning("At the volume-level, use Volume.cap_alpha_seqs()")
        return self.cap_alpha_seqs(**kwargs)

    def cap_alpha_seqs(self, section='body', **kwargs):
        ''' Return the longest length of consecutive capital letters starting a
        line on the page. Returns a list for all pages. Only includes the body:
        header/footer information is not included.
        '''
        if section and (section != 'body'):
            logging.warning("cap_alpha_seq only includes counts for the body "
                         "section of pages.")
            kwargs['section'] = 'body'
        return self.section_features(feature='capAlphaSeq', **kwargs)

    def sentence_counts(self, **kwargs):
        ''' Return a list of sentence counts, per page '''
        return self.section_features(feature='sentenceCount', **kwargs)

    def tokenlist(self, pages=True, section='default', case=True, pos=True,
                  page_freq=False, page_select=False, drop_section=False,
                  htid=False, chunk = False, overflow_strategy="ends", chunk_target = 10000,
                  page_ref=False, **kwargs):

        ''' Get or set tokencounts DataFrame

        pages[bool]: Keep page-level info if true, else fold.

        section[string]: Which part of the page to return. In addition to
            'header', 'body', and 'footer', 'all' will return a DataFrame with
            all the sections, 'group' will sum all sections,
            section in ['header', 'footer', 'body'] will return those fields
            section == 'all' will group and sum all sections
            section == 'default' falls back on what the page object has saved

        case[bool] : Preserve case, or fold.

        pos[bool] : Specify whether to return frequencies per part of speech,
                    or simply by word

        page_freq[bool] : Whether to count page frequency (1 if it occurs on
        the page, else 0) or a term frequency (counts for the term, per page)
        
        page_select[int] : Page sequence number for optionally choosing just one
            page.
            
        drop_section[bool]: Whether to drop the index level refering to the section.

        htid[bool]: whether to add an index level with the htid included.
        
        # Chunking specific arguments

        chunk[bool]: whether to divide the text into equal-sized chunks. All remaining options 

        page_ref[bool]: Include first and last page of chunk in the output
        
        chunk_target: the target size--in number of words--of each chunk.

        - pages are collected together until their word count is > chunk_target
        - chunk_target is adjusted slightly to minimize the size of straggler chunks

        - overflow_strategy: How to handle cases in which the total number of words does not divide directly into the chosen chunk_target.
           - "ends" allows the first and last chunks -- which, in books, are often the messiest -- to vary greatly in length
             while keeping the middle sections as close to `chunk_target` in length as possible given the page lengths.
           - "even" sets preset targets based on the document length, and creates chunks that are of approximately even size within each book.
             There may be great variability in chunk length *between* books using this method.
           - "last" keeps all but the last chunk at the desired length, but allows huge variability in the final chunk.

        '''
        assert not (page_select and not pages)

        if chunk:
            return self._chunked_tokenlist(section=section, case=case, pos=pos,
                  page_freq=page_freq, page_select=page_select, drop_section=drop_section,
                                           htid=htid, overflow_strategy = overflow_strategy,
                                           chunk_target = chunk_target, page_ref=page_ref)
        # Create the internal representation if it does not already
        # exist. This will only need to exist once
        if self._tokencounts.empty:
            self._tokencounts = self.parser._make_tokencount_df()
            if 'chunk' in self._tokencounts.index.names:
                logging.info("Internal representation has chunks rather than pages. Treating them"
                                    " identically.")
                self._pagecolname = 'chunk'
            else:
                self._pagecolname = 'page'
        
        assert(('token' in self._tokencounts.index.names) or ('lowercase' in self._tokencounts.index.names))
        
        if section == 'default':
            section = self.default_page_section
        elif 'section' not in self._tokencounts.index.names:
            raise MissingFieldError("Section not saved in internal representation, so you can't "
                                    "select a specific section. Use section='default' or load a "
                                    "complete dataset.")
        
        # Allow incomplete internal representations, as long as the args don't want the missing
        # data
        for arg, column in [(pages, self._pagecolname), (page_select, self._pagecolname),
                            (case, 'token'), (pos, 'pos')]:
            if arg and column not in self._tokencounts.index.names:
                raise MissingFieldError("Your internal tokenlist representation does not have "
                                        "enough information for the current args. Missing "
                                        "column: %s" % column)
        
        if page_select:
            try:
                df = self._tokencounts.xs(page_select,
                                          level=self._pagecolname, drop_level=False)
            except KeyError:
                # Empty tokenlist
                return self._tokencounts.iloc[0:0]
        else:
            df = self._tokencounts

        df = group_tokenlist(df, pages=pages, section=section,
                               case=case, pos=pos, page_freq=page_freq, pagecolname=self._pagecolname)
        
        if drop_section:
            df = df.droplevel('section')
        
        if htid:
            # Prepent level with htid
            df = pd.concat([df], keys=[self.id], names=['htid'])
        
        return df

    def term_page_freqs(self, page_freq=True, case=True):
        ''' Return a term frequency x page matrix, or optionally a
        page frequency x page matrix '''
        all_page_dfs = self.tokenlist(page_freq=page_freq, case=case, pos=False)
        tokencolname = 'token' if case else 'lowercase'
        
        return all_page_dfs.reset_index()\
                           .groupby([tokencolname, self._pagecolname], as_index=False).sum()\
                           .pivot(index=self._pagecolname, columns=tokencolname,
                                  values='count')\
                           .fillna(0)
                           
    def _chunked_tokenlist(self, chunk_target = 10000, overflow_strategy = "ends", page_ref=False, **kwargs):
        '''
        Return a tokenlist dataframe grouped by numbered 'chunks', each of which has roughly `chunk_target` words.


        - also takes tokenlist() arguments, such as case, drop_section, pos

        '''
        # Chunking won't work with pages=False
        kwargs['pages'] = True
        tl = self.tokenlist(**kwargs)
        newindex = ['chunk'] + [v for v in tl.index.names if v != 'page']
        
        with_chunks = tl.reset_index().set_index("page")
        pagecounts = with_chunks.groupby('page')['count'].sum()

        assert overflow_strategy in ["ends", "even", "last"]
        chunking_method = getattr(transformations, "chunk_{}".format(overflow_strategy))
        chunk_labs = chunking_method(pagecounts.values, chunk_target)

        indexed = pd.Series(chunk_labs, index = pagecounts.index, name='chunk')
        with_chunks = with_chunks.join(indexed) 
        

        groups = [g for g in tl.index.names if g != 'page']
        return_val = with_chunks.groupby(groups + ['chunk'])['count'].sum().reset_index()

        if page_ref:
            chunk_bounds = with_chunks.reset_index().groupby("chunk")['page']\
               .agg(['min', 'max'])\
               .rename(columns = {'min':'pstart', 'max':'pend'})
               
            return_val = return_val.set_index('chunk').join(chunk_bounds).reset_index()
            newindex = ['chunk', 'pstart', 'pend'] + newindex[1:]

        return return_val.set_index(newindex).sort_index()

    def term_volume_freqs(self, page_freq=True, pos=True, case=True):
        ''' Return a list of each term's frequency in the entire volume '''
        df = self.tokenlist(page_freq=page_freq, pos=pos, case=case)
        tokencolname = 'token' if case else 'lowercase'
        groups = [tokencolname] if not pos else [tokencolname, 'pos']
        return df.reset_index().drop([self._pagecolname], axis=1)\
                 .groupby(groups, as_index=False).sum()\
                 .sort_values(by='count', ascending=False)

    def end_line_chars(self, **args):
        '''
        Get counts of characters at the end of lines, i.e. the characters on
        the far right of the page.
        '''
        return self.line_chars(place='end', **args)

    def begin_line_chars(self, **args):
        '''
        Get counts of characters at the begin of lines, i.e. the characters on
        the far left of the page.
        '''
        return self.line_chars(place='begin', **args)

    def line_chars(self, section='default', place='all', page_select=False):
        '''
        Interface for all begin/end of line character information.
        '''

        # Create the internal representation
        if self._line_chars.empty:
            self._line_chars = self.parser._make_line_char_df()
        
        df = self._line_chars
        if page_select:
            try:
                df = df.xs(page_select, level='page',
                                         drop_level=False)
            except KeyError:
                # Empty tokenlist
                return self._line_chars.iloc[0:0]
            
        if section == 'default':
            section = self.default_page_section

        return group_linechars(df, section=section, place=place)
    
    def save(self, dir, format = 'parquet', token_kwargs="default", **kwargs):
        '''
        
        A wrapper around the 'write' method of all IdResolvers, 
        that allows you to quickly declare a div, a format, and other
        kwargs.

        The primary use is for converting the feature files to
        a more efficient parquet format. By default, only metadata and
        tokencounts are saved, using the naming convention used by
        parquetVolumeParser.
        
        Saving page features is currently unsupported, as it's an
        ill-fit for parquet. This is currently just the
        language-inferences for each page - everything else is in
        section features (page by body/header/footer).
        
        Since Volumes partially support incomplete dataframes, you can
        pass Volume.tokenlist arguments as a dict with
        token_kwargs. For example, if you want to save a
        representation with only body information, drop the 'section'
        level of the index, and fold part-of-speech counts, you can
        pass token_kwargs=dict(section='body', drop_section=True,
        pos=False).

        '''

        new_vol = Volume(self.id, dir = dir, format = format, id_resolver = "local",
                         mode = 'wb', **kwargs)
        new_vol.write(self, token_kwargs=token_kwargs, **kwargs)
    
    def __str__(self):
        def truncate(s, maxlen):
            if len(s) > maxlen:
                return s[:maxlen].strip() + "..."
            else:
                return s.strip()
        if len(self.author) > 0:
            return "<Volume: %s (%s) by %s>" % (truncate(self.title, 30), self.year, truncate(self.author[0], 40))
        else:
            return "<Volume: %s (%s) without a listed author>" % (truncate(self.title, 30), self.year)

class Page:

    BASIC_FIELDS = [('seq', 'seq'), ('tokenCount', '_token_count'),
                    ('languages', 'languages')]
    ''' List of fields which return primitive values in the schema, as tuples
    with (CamelCase, lower_with_under) mapping '''
    SECTION_FIELDS = ['lineCount', 'emptyLineCount', 'sentenceCount',
                      'capAlphaSeq']
    ''' Fields that are counted by section.'''

    def __init__(self, seq, volume, default_section='body'):
        self.default_section = default_section
        self.volume = volume
        self.seq = int(seq)

        assert(self.default_section in SECREF + ['all', 'group'])

    def tokens(self, **kwargs):
        ''' Get unique tokens. Use args from Volume. '''
        return self.volume.tokens(page_select=self.seq, **kwargs)

    def line_count(self, section='default'):
        return self.volume.section_features(page_select=self.seq, feature='lineCount').values[0]

    def empty_line_count(self, section='default'):
        return self.volume.section_features(page_select=self.seq, feature='emptyLineCount').values[0]

    def cap_alpha_seq(self, section='body'):
        ''' Return the longest length of consecutive capital letters starting a
        line on the page. Returns an integer. Only includes the body:
        header/footer information is not included.
        '''
        if section != 'body':
            logging.warning("cap_alpha_seq only includes counts for the body "
                         "section of pages.")
        return self.volume.cap_alpha_seqs(page_select=self.seq).values[0]

    def sentence_count(self, section='default'):
        return self.volume.sentence_counts(page_select=self.seq).values[0]

    def tokenlist(self, **kwargs):
        '''
        Get tokencounts DataFrame.Use args from Volume.tokenlist().
        '''
        return self.volume.tokenlist(page_select=self.seq, **kwargs)

    def end_line_chars(self, **kwargs):
        return self.line_chars(place='end', **kwargs)

    def begin_line_chars(self, **kwargs):
        return self.line_chars(place='begin', **kwargs)

    def line_chars(self, **kwargs):
        '''
        Get a dataframe of character counts at the start and end of lines. Use
        args from Volume.line_chars().
        '''
        return self.volume.line_chars(page_select=self.seq, **kwargs)

    def token_count(self, **kwargs):
        ''' Count total tokens on the page '''
        return self.volume.section_features(page_select=self.seq, feature='tokenCount', **kwargs).values[0]

    def __str__(self):
        if self.volume:
            name = "<page %s of volume %s>" % (self.seq, self.volume.id)
        else:
            name = "<page %s with no volume parent>" % (self.seq)
        return name
