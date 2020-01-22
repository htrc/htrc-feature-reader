import os
import zipfile
import htrc_features.utils
import hashlib
from io import BytesIO
import bz2
import gzip
import logging
import sys

MINOR_VERSION = (sys.version_info[1])

from urllib.request import urlopen as _urlopen
from urllib.parse import urlparse as parse_url
from urllib.error import HTTPError


class FauxFile():
    def __init__(self, *buffers):
        self.main = buffers[0]
        # Zipfile requires closing other buffers.
        self.buffers = buffers

    def read(self, **kwargs):
        return self.main.read(**kwargs)

    def write(self, x, **kwargs):
        return self.main.write(x, **kwargs)
    
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def close(self):
        for buffer in self.buffers:
            buffer.close()
            
class IdResolver():
    """
    This class exposes two methods for each storage format: get and put.

    The base class method handles decompression for zip and csv files.

    Note: subclasses must consume **kwargs on init and get to support full compatibility.

    """
    def __init__(self, _sentinel = None, format = None, compression = None, **kwargs):
        if _sentinel is not None:
            raise KeyError("You must name arguments to the IdHandler constructor.")
        if "dir" in kwargs:
            self.dir = kwargs['dir']
        else:
            pass
        
        self.format = format
        self.compression = compression
        
        # Sometimes we have to remember open buffers to close.
        self.active_buffers = []
            
    def fname(self, id, format = None, compression = None, suffix = None):
        """
        Returns a filename given an id and format.
        """
        if format is None:
            format = self.format
            
        if compression is None:
            compression = self.compression
            
        clean_id = htrc_features.utils.clean_htid(id)
        
        fname = [clean_id, format]
        
        if suffix is not None:
            fname.append(suffix)
        if compression is not None and format != "parquet":
            fname.append(compression)
        return ".".join(fname)
    
    def _decompress(self, buffer, format, compression, mode = 'rb'):
        
        """
        Return a version of a buffer wrapped in a compression
        layer. Generally used in decompress mode,
        but using 'wb' make this return a compressed buffer.
        """
        
        if compression is None:
            compression = self.compression
        if compression is None or format=="parquet":
            return buffer
        elif compression == "bz2":
            return bz2.open(buffer, mode)
        elif compression == "gz":
            return gzip.open(buffer, mode)

    def open(self, id, suffix = None, mode = 'rb', **kwargs):
        """
        Open a file for reading.
        
        format: the data format. 'parquet' and 'json' are supported.
        compression: the compression used in the data. Generally 'bz2' or 'gz' for json,
          and 'gz' or 'snappy' for parquet.
        Suffix: an addition key at the end, mostly used with parquet.
        Mode: 'rb' (read only) or 'wb' (write and read).
        """

        # Update with some defaults.
        compression = kwargs.get("compression", self.compression)
        format = kwargs.get("format", self.format)
        uncompressed = self._open(id = id, suffix = suffix, mode = mode, **kwargs)
        fout = self._decompress(uncompressed, format, compression, mode)
        if len(self.active_buffers) > 0:
            # When working with zipfiles, we need 'with' and 'close' to
            # close multiple files even during an exception. This wrapper does that.
            return FauxFile(fout, *self.active_buffers)
        
        return fout

    def _open(self, id, **kwargs):
        """
        Each method should define '_open' for itself. The class's 
        'open' method will then handle the method.
        """
        raise NotImplementedError("An IdResolver superclass must overwrite the "
                                  "'_open' method to return an io.Buffer object")
        compression = kwargs.get('compression', self.compression)
        format = kwargs.get
        return open("{}.{}.{}".format(id, format, compression, mode = mode))

class HttpResolver(IdResolver):
    
    def __init__(self, url = "http://data.htrc.illinois.edu/htrc-ef-access/get?action=download-ids&id={id}&output=json", **kwargs):
        """
        Initialize with a url; it must contain the string "{id}" in it somewhere, and that will be replaced with the id in the get call.
        """
        self.url = url
        kwargs['format'] = kwargs.get("format", "json")
        
        super().__init__(**kwargs)
    
        # Currently this only returns uncompressed data.
        self.compression = None


    def _open(self, id = None, mode = 'rb', **kwargs):
        if 'compression' in kwargs and kwargs['compression'] == 'bz2':
            raise Warning("You have requested to read from HTTP with bz2 compression, but at time of writing this was not supported.")
        if mode == 'wb':
            raise NotImplementedError("Mode is not defined")
        path_or_url = self.url.format(id = id)
        try:
            req = BytesIO(_urlopen(path_or_url).read())
        except HTTPError:
            logging.exception("HTTP Error accessing %s" % path_or_url)
            raise
        return req
        
class LocalResolver(IdResolver):

    def __init__(self, dir, **kwargs):
        super().__init__(dir = dir, **kwargs)
    
    def _open(self, id, format = None, compression = None, mode = 'rb', **kwargs):
        filename = self.fname(id, format = format, compression = compression, suffix = None)
        dirname = kwargs.get("dir", self.dir)
        return open(os.path.join(dirname, filename), mode)

        
class PathResolver(IdResolver):
    # A path is the simplest form of id storage. These are not HTIDs, and so aren't stored.
    # We could check to make sure the pathname makes sense. But we don't.
    def _open(self, id, mode = 'rb', **kwargs):
        self.compression = kwargs.get("compression", None)
        if self.compression is None:
            if id.endswith(".gz"):
                self.compression = "gz"
            if id.endswith(".bz2"):
                self.compression = "bz2"
        """ Use kwargs to absorb unused arguments."""
        return open(id, "rb")

class PairtreeResolver(IdResolver):
    def _open(self, id, mode = 'rb', **kwargs):
        assert(mode.endswith('b'))
        if mode.startswith('w'):
            # Ensure DIRS EXIST
            pass
        format = kwargs.get("format", self.format)
        compression = kwargs.get("compression", self.compression)
        suffix = kwargs.get("suffix", None)
        path = htrc_features.utils.id_to_pairtree(id, format, suffix, compression)
        full_path = os.path.join(self.dir, path)
        return open(full_path, mode)
    
    
class ZiptreeResolver(IdResolver):
    """
    This class includes methods for populating a ziptree from a pairtree.
    
    A 'ziptree' is a set of zipfiles. 
    """
    def __init__(self, dir, format, compression, pairtree_root = None, **kwargs):
        self.pairtree_root = pairtree_root
        if not os.path.exists(dir):
            os.makedirs(dir)
        super().__init__(dir = dir, format = format, compression = compression, **kwargs)
        
    def which_zipfile(self, id, digits = 3):
        # Use the sha1 hash of the id; and take only the first three digits.
        code = hashlib.sha1(bytes(id, 'utf-8')).hexdigest()[:digits]
        if digits == 0:
            return None
        return os.path.join(self.dir, code + ".zip")

    def _open(self, id, mode = 'rb', **kwargs):
        """
        Force: overwrite existing files where found. (not implemented).
        """
        format = kwargs.get("format", self.format)
        compression = kwargs.get("compression", self.compression)
        suffix = kwargs.get("suffix", None)
        force = kwargs.get("force", False)
        
        filename = self.fname(id, format = format, suffix = suffix, compression = compression)
        
        fin = self.which_zipfile(id)
        
        zip_mode = 'r'
        if mode.startswith('w'):
            # To write, we append to the zipfile.
            zip_mode = 'a'

        if mode.startswith('w') and MINOR_VERSION <= 5:
            
            raise NotImplementedError("Writing to zipfiles with this module requires python 3.6")
        
            """
            I would love to make this work, but for now it's broken.
            zipcontainer = zipfile.ZipFile(fin, mode = zip_mode)
            if mode.startswith('w') and filename in zipcontainer.namelist():
                raise KeyError("Id '{}' already in zipfile. Refusing to overwrite".format(id))

            return self._open_fallback(filename, zipcontainer, format, compression, suffix)
            """
            
        self.zipcontainer = zipfile.ZipFile(fin, mode = zip_mode)

        # Prepare it to be closed.
        self.active_buffers = [self.zipcontainer]
        if mode.startswith('w') and filename in self.zipcontainer.namelist():
            self.zipcontainer.close()
            raise KeyError("Id '{}' already in zipfile. Refusing to overwrite".format(id))
        fout = self.zipcontainer.open(filename, mode.rstrip('b'))
        return fout
    
    def _open_fallback(self, filename, zipfile, format, compression, suffix):
        # Temporary kludge, not complete.
        
        # A python 3.5 compatible fallback for mimicking an 'open'
        # interface to zipfiles that allows writing only.

        # (Because a 'read' interface already exists).
        
        class DummyWriter():
            def __init__(self, filename, zipfile):
                self.filename = filename
                self.zipfile = zipfile
            def close(self):
                pass
            def write(self, what):
                if compression is not None and format != "parquet":
                    import io
                    compress = gzip.compress
                    if compression == "bz2":
                        compress = bz2.compress
                    what = compress(what)
                self.zipfile.writestr(self.filename, what)
                
        return DummyWriter(filename, zipfile)
    
    def insert(self, id, depth = 3, dangerously = False):
        """
        Dangerously: ignore errors on the insertion step. This is used only for building out pairtrees.
        """
        # Use the json reader to get the pairtree name.
        f = htrc_features.parsers.JsonFileHandler(id = id, dir = self.pairtree_root, id_resolver = "pairtree", compression = "bz2", load = False)
        path = f.path
        # Insert and close in a single operation to avoid contamination.
        with zipfile.ZipFile(self.which_zipfile(id, depth), mode="a") as zipdest:
            filename = path.split("/")[-1]
            if filename in zipdest.namelist():
                return "already present"
            try:

                return "successful insertion"
            except:
                zipdest.close()
                if not dangerously:
                    raise
                return "assorted error"

if __name__ == "__main__":
    import sys
    zipdir, id = sys.argv[1:]
    resolver = ZiptreeResolver(zipdir)
    import json
    import bz2
    v = resolver.get(id).read()
