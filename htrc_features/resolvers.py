import os
import zipfile
import hashlib
from io import BytesIO
import bz2
import gzip
import logging
import sys
from pathlib import Path
from string import Formatter
from .utils import _id_encode, id_to_pairtree, id_to_stubbytree, clean_htid


MINOR_VERSION = (sys.version_info[1])

from urllib.request import urlopen as _urlopen
from urllib.parse import urlparse as parse_url
from urllib.error import HTTPError

class IdResolver():
    """
    The base class method handles decompression for gzip and bz2.

    Note: subclasses must consume **kwargs on init to support full compatibility.

    A subclass must, at a minimum, define an '_open' method that handles the non-compression
    parts of reading and writing.

    This base class enforces some pretty strict rules.

    """
    def __init__(self, _sentinel = None, format = None, mode = 'rb', dir=None, compression=None, **kwargs):
        if _sentinel is not None:
            raise NameError("You must name arguments to the IdHandler constructor.")

        if format is None:
            raise NameError("You must define the file format this resolver uses: json or parquet")
        else:
            pass

        self.format = format
        self.dir = dir
        
        self.mode = mode
        self.compression = compression
        
        # Sometimes (only zipfiles) we have to remember open buffers to close--
        # e.g., the zipfile holding an open file object.
        self.active_buffers = []
            
    def fname(self, id, format, compression, suffix):
        """
        Returns a filename given an id and format.

        No defaults because this should always be fully described.

        """
        clean_id = clean_htid(id)
        if format == "parquet":
            # Because it's not in the parquet filename.
            compression = None
        fname = [clean_id, suffix, format, compression]
        return ".".join([part for part in fname if part is not None])
    
    def _decompress(self, buffer, format, compression, mode = 'rb'):
        
        """
        Return a version of a buffer wrapped in a compression
        layer. Generally used in decompress mode,
        but using 'wb' make this return a compressed buffer.
        """
        if buffer is None:
            raise FileNotFoundError("Empty buffer found very late")            
        elif compression is None or format=="parquet":
            return buffer
        elif compression == "bz2":
            return bz2.open(buffer, mode)
        elif compression == "gz":
            return gzip.open(buffer, mode)

        
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        for buffer in self.active_buffers:
            buffer.close()
        self.active_buffers = []

    def open(self, id, suffix = None, format=None, mode = 'rb',
             skip_compression = False, compression = 'default',
             **kwargs):
        """
        Open a file for reading.
        
        format: the data format. 'parquet' and 'json' are supported.
        compression: the compression used in the data. Generally 'bz2' or 'gz' for json,
          and 'gz' or 'snappy' for parquet.
        Suffix: an addition key at the end, mostly used with parquet.
        Mode: 'rb' (read only) or 'wb' (write and read).
        skip_compression: whether to ignore the decompress stage. ("compression" arguments 
        may still matter forthe resolution of file names)
        
        """
        if not mode in ['rb', 'wb']:
            raise TypeError("Storage backends only support binary writing formats ('wb' or 'rb')")
        
        # Update with some defaults.
        if compression =='default':
            try:
                compression = self.compression
            except AttributeError:
                raise AttributeError("You must specify compression somewhere")

        
        if not format:
            format = self.format
        if format == 'parquet':
            skip_compression = True
            
        uncompressed = self._open(id = id, suffix = suffix, mode = mode, format = format,
                                  compression=compression,  **kwargs)
        
        if uncompressed is None:
            raise FileNotFoundError("Empty buffer found very late")
        
        # The name here is misleading; if mode is 'w', 'decompress' may actually be
        # acting as a compression filter on write actions.
        
        if skip_compression:
            fout = uncompressed
        else:
            fout = self._decompress(uncompressed, format, compression, mode)
            
        assert fout

        return fout

    def _open(self, id, compression=None, format=None, mode='rb'):
        """
        Each method should define '_open' for itself. The class's 
        'open' method will then handle the method.
        """
        raise NotImplementedError("An IdResolver superclass must overwrite the "
                                  "'_open' method to return an io.Buffer object")

class HttpResolver(IdResolver):
    
    def __init__(self, url='http://data.analytics.hathitrust.org/features-2020.03/{stubbypath}', dir=None, format='json', compression='bz2', **kwargs):
        """
        Initialize with a url; it must contain the string "{id}", "{stubbypath}", or "{pairtreepath}" in it somewhere, and that will be replaced with the id/path in the get call.
        """
        self.url = url
        self.compression = compression
        self.format = format
        if dir is not None:
            raise ValueError("HTTP Resolver doesn't work with `dir`. Are you sure you meant to try to load "
                             "from HTTP? If not, make sure you're explicit about id_resolver or your format "
                             "argument is correct. If so, remove the `dir` argument.")
            
        super().__init__(dir=dir, format=format, compression=self.compression, **kwargs)
    
    def _open(self, id = None, mode = 'rb', compression='default', **kwargs):
        formatter = Formatter()
        fields = [f[1] for f in formatter.parse(self.url)]
        assert len(set(fields).intersection(['id', 'stubbypath', 'pairtreepath'])) > 0
        
        if compression == 'default':
            compression = self.compression
        if mode == 'wb':
            raise NotImplementedError("Mode is not defined")
        
        stubbypath, pairtreepath = None, None
        if 'stubbypath' in fields:
            stubbypath = id_to_stubbytree(id, format=self.format, compression=compression)
        if 'pairtreepath' in fields:
            pairtreepath = id_to_pairtree(id, format=self.format, compression=compression)
            
        path_or_url = self.url.format(id = id, stubbypath=stubbypath, pairtreepath=pairtreepath)

        try:
            byt = _urlopen(path_or_url).read()
            req = BytesIO(byt)
        except HTTPError:
            logging.exception("HTTP Error accessing %s" % path_or_url)
            raise
        return req
    

class LocalResolver(IdResolver):

    def __init__(self, dir, **kwargs):
        super().__init__(dir = dir, **kwargs)
    
    def _open(self, id, format = None, mode = 'rb', compression='default', dir=None, suffix=None, **kwargs):

        if compression is 'default':
            compression = self.compression
        if not dir:
            dir = self.dir
        
        filename = self.fname(id, format = format, compression = compression, suffix = suffix)
        return Path(dir, filename).open(mode = mode)

        
class PathResolver(IdResolver):
    # A path is the simplest form of id storage. These are not HTIDs, and so aren't stored.
    # We could check to make sure the pathname makes sense. But we don't.
    def _open(self, id, mode = 'rb', compression=None, **kwargs):
        self.compression = compression
        return open(id, mode)

class PairtreeResolver(IdResolver):
    def __init__(self, dir=None, **kwargs):
        if not dir:
            raise NameError("You must specify a directory with 'dir'")
        super().__init__(dir=dir, **kwargs)
        
    def _open(self, id, mode = 'rb', format=None, compression='default', suffix=None, **kwargs):
        assert(mode.endswith('b'))
        
        if not format:
            format = self.format
        if compression == 'default':
            compression = self.compression

        path = Path(id_to_pairtree(id, format, suffix, compression)).parent
        fname = self.fname(id = id, format = format, suffix = suffix, compression = compression)
        full_path = Path(self.dir, path, fname)
        try:
            return full_path.open(mode=mode)
        except FileNotFoundError:
            if mode.startswith('w'):
                full_path.parent.mkdir(parents=True, exist_ok=True)
                return full_path.open(mode=mode)
            else:
                raise

class StubbytreeResolver(IdResolver):
    '''
    An alternative to pairtree that uses loc/code, where the code is every third digit of the ID.
    '''
    def __init__(self, dir=None, **kwargs):
        if not dir:
            raise NameError("You must specify a directory with 'dir'")
        super().__init__(dir=dir, **kwargs)
        
    def _open(self, id, mode = 'rb', format=None, compression='default', suffix=None, **kwargs):
        assert(mode.endswith('b'))
        
        if not format:
            format = self.format
        if compression == 'default':
            compression = self.compression

        path = Path(id_to_stubbytree(id, format, suffix, compression)).parent
        fname = self.fname(id, format= format, suffix = suffix, compression = compression)
        full_path = Path(self.dir, path, fname)
        try:
            return full_path.open(mode=mode)
        except FileNotFoundError:
            if mode.startswith('w'):
                full_path.parent.mkdir(parents=True, exist_ok=True)
                return full_path.open(mode=mode)
            else:
                raise
    
class ZiptreeResolver(IdResolver):
    """
    This class includes methods for populating a ziptree from a pairtree.
    
    A 'ziptree' is a set of zipfiles. 
    """
    def __init__(self, dir, format, pairtree_root = None, mode = 'rb', hash_chars = 3, **kwargs):
        self.pairtree_root = pairtree_root
        self.hash_chars = hash_chars
        if not os.path.exists(dir) and not mode.startswith('r'):
            os.makedirs(dir)
        super().__init__(dir = dir, format = format, **kwargs)
        
    def which_zipfile(self, id):
        digits = self.hash_chars
        # Use the sha1 hash of the id; and take only the first three digits.
        code = hashlib.sha1(bytes(id, 'utf-8')).hexdigest()[:digits]
        if digits == 0:
            return os.path.join(self.dir, "features.zip")
            return None
        return os.path.join(self.dir, code + ".zip")

    def _open(self, id, mode = 'rb', format=None, compression='default', suffix=None, force=False, **kwargs):
        """
        Force: overwrite existing files where found. (not implemented).
        """
        
        if not format:
            format = self.format
        if compression == 'default':
            compression = self.compression
        
        filename = self.fname(id, format = format, suffix = suffix, compression = compression)
        
        fin = self.which_zipfile(id)
        
        zip_mode = 'r'
        if mode.startswith('w'):
            # To write, we append to the zipfile.
            zip_mode = 'a'

        if mode.startswith('w') and MINOR_VERSION <= 5:
            raise NotImplementedError("Writing to zipfiles with this module requires python 3.6")
            
        zipcontainer = zipfile.ZipFile(fin, mode = zip_mode)

        # Prepare it to be closed.
        self.active_buffers.append(zipcontainer)
        
        if mode.startswith('w') and filename in zipcontainer.namelist():
            logging.warning("Id '{}' already in zipfile. Refusing to overwrite".format(id))
            # zipcontainer.close()
            # Switching away from error.
            raise KeyError("Id '{}' already in zipfile. Refusing to overwrite".format(id))
        try:
            fout = zipcontainer.open(filename, mode.rstrip('b'))
        except KeyError:
            raise FileNotFoundError("{} not found in {}".format(filename, zipcontainer))
        if zip_mode == 'r':
            import io
            fout = io.BytesIO(fout.read())
        return fout
    

class resolver_dict(dict):
    """
    # A method to allow the creation of new methods that cache between the basic 
    # nickname formats.

    """
    
    def __missing__(self, key):
        try:
            method, cached, fallback = key.split("_")
            if not cached == "cached":
                raise KeyError("No known resolver for " + key)
            if method == "locally":
                # i.e., the default is "locally_cached_http"
                method = "local"
            if key != "locally_cached_http":
                logging.warning("You're creating an exotic resolver; this is undocumented"
                                "and unspported, and may be removed in a future version",
                                DeprecationWarning)
            logging.debug("Creating resolver for {} -> {}".format(method, fallback))

            # This import has to wait until here to avoid circular dependencies.
            from .caching import make_fallback_resolver
            resolver = make_fallback_resolver(self[method], fallback)

            self.__setitem__(key, resolver)
            return resolver
        
        except IndexError:
            raise KeyError("No known resolver for " + key)
       
resolver_nicknames = resolver_dict({
    "path": PathResolver,
    "stubbytree": StubbytreeResolver,
    "pairtree": PairtreeResolver,
    "ziptree": ZiptreeResolver,
    "local": LocalResolver,
    "http": HttpResolver
})
