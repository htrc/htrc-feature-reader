import os
import zipfile
import htrc_features.utils
import hashlib
from io import BytesIO
import bz2
import gzip
import logging
import sys
from pathlib import Path

MINOR_VERSION = (sys.version_info[1])

from urllib.request import urlopen as _urlopen
from urllib.parse import urlparse as parse_url
from urllib.error import HTTPError


class FauxFile():
    """
    This class is a shim to allow sensible filehandling and errors
    with zipfiles, where on an error both the file *inside* the zip
    and the zipfile itself must be closed. It is only used in that
    specific case at the moment.
    """
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
    The base class method handles decompression for gzip and bz2.

    Note: subclasses must consume **kwargs on init to support full compatibility.

    A subclass must, at a minimum, define an '_open' method that handles the non-compression
    parts of reading and writing.

    This base class enforces some pretty strict rules.

    """
    def __init__(self, _sentinel = None, format = None, mode = 'rb', dir=None, **kwargs):
        if _sentinel is not None:
            raise NameError("You must name arguments to the IdHandler constructor.")
        if "dir" in kwargs:
            self.dir = kwargs['dir']
        if format is None:
            raise NameError("You must define the file format this resolver uses: json or parquet")
        else:
            pass

        self.format = format
        self.dir = dir
        
        self.mode = mode
        
        if "compression" in kwargs:
            self.compression = kwargs['compression']
        else:
            # Do **not** set compression to 'None', because None also means
            # 'no compression.'
            raise AttributeError("You must specify compression for a resolver")
        
        # Sometimes we have to remember open buffers to close.
        self.active_buffers = []
            
    def fname(self, id, format, compression, suffix):
        """
        Returns a filename given an id and format.

        No defaults because this should always be fully described.

        """
        clean_id = htrc_features.utils.clean_htid(id)
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
        self.close()

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
        
        if len(self.active_buffers) > 0:
            # When working with zipfiles, we need 'with' and 'close' to
            # close multiple files even during an exception. This wrapper does that.
            return FauxFile(fout, *self.active_buffers)
        
        return fout

    def _open(self, id, compression=None, format=None, mode='rb'):
        """
        Each method should define '_open' for itself. The class's 
        'open' method will then handle the method.
        """
        raise NotImplementedError("An IdResolver superclass must overwrite the "
                                  "'_open' method to return an io.Buffer object")
        if not compression:
            compression = self.compression
        if not format:
            format = self.format
        return open("{}.{}.{}".format(id, format, compression, mode = mode))

class HttpResolver(IdResolver):
    
    def __init__(self, url = "http://data.htrc.illinois.edu/htrc-ef-access/get?action=download-ids&id={id}&output=json", dir=None, format='json', **kwargs):
        """
        Initialize with a url; it must contain the string "{id}" in it somewhere, and that will be replaced with the id in the get call.
        """
        self.url = url
        if dir is not None:
            raise ValueError("HTTP Resolver doesn't work with `dir`. Are you sure you meant to try to load "
                             "from HTTP? If not, make sure you're explicit about id_resolver or your format "
                             "argument is correct. If so, remove the `dir` argument.")
        
        # Currently this only returns uncompressed data.
        if not 'compression' in kwargs:
            kwargs['compression'] = None
            
        super().__init__(dir=dir, format=format, **kwargs)
    
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
    
    def _open(self, id, format = None, mode = 'rb', compression='default', suffix=None, **kwargs):

        if compression is 'default':
            compression = self.compression
        
        filename = self.fname(id, format = format, compression = compression, suffix = suffix)
        dirname = kwargs.get("dir", self.dir)
        return Path(dirname, filename).open(mode = mode)

        
class PathResolver(IdResolver):
    # A path is the simplest form of id storage. These are not HTIDs, and so aren't stored.
    # We could check to make sure the pathname makes sense. But we don't.
    def _open(self, id, mode = 'rb', **kwargs):
        if "compression" in kwargs:
            self.compression = kwargs["compression"]
        
        return open(id, mode)

class PairtreeResolver(IdResolver):
    def __init__(self, **kwargs):
        if not "dir" in kwargs:
            raise NameError("You must specify a directory with 'dir'")
        super().__init__(**kwargs)
        
    def _open(self, id, mode = 'rb', **kwargs):
        assert(mode.endswith('b'))
        
        format = kwargs.get("format", self.format)
        compression = kwargs.get("compression", self.compression)
        suffix = kwargs.get("suffix", None)

        path = htrc_features.utils.id_to_pairtree(id, format, suffix, compression)
        full_path = Path(self.dir, path)
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
    def __init__(self, dir, format, pairtree_root = None, **kwargs):
        self.pairtree_root = pairtree_root
        if not os.path.exists(dir) and not kwargs['mode'].startswith('b'):
            os.makedirs(dir)
        super().__init__(dir = dir, format = format, **kwargs)
        
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
    
if __name__ == "__main__":
    import sys
    zipdir, id = sys.argv[1:]
    resolver = ZiptreeResolver(zipdir)
    import json
    import bz2
    v = resolver.get(id).read()
