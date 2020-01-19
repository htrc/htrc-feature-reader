import os
import zipfile
import htrc_features.utils
import hashlib
from io import BytesIO
import bz2
import gzip
import logging

from urllib.request import urlopen as _urlopen
from urllib.parse import urlparse as parse_url
from urllib.error import HTTPError


class IdResolver():
    """
    This class exposes two methods for each storage format: get and put.

    The base class method handles decompression for zip and csv files.

    Note: subclasses must consume **kwargs on init and get to support full compatibility
    """
    def __init__(self, _sentinel = None, format = None, compression = None, **kwargs):
        if _sentinel is not None:
            raise KeyError("You must name arguments to this super call")
        
        if "dir" in kwargs:
            self.dir = kwargs['dir']
        else:
            pass
        self.format = format
        self.compression = compression
        pass

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
    
    def decompress(self, buffer, compression = None):
        if compression is None:
            compression = self.compression
        if compression is None or self.format=="parquet":
            return buffer
        elif compression == "bz2":
            return bz2.open(buffer)
        elif compression == "gz":
            return gzip.open(buffer)
        
    def get(self, id, format = "json", suffix = None, compression = "bz2"):
        return
            
    
    def put(self, object, id, format = "json", compression = "bz2"):
        """
        Write a readable buffer from 'object' to a file in this system.
        """
        pass

class HttpResolver(IdResolver):
    
    def __init__(self, url = "http://data.htrc.illinois.edu/htrc-ef-access/get?action=download-ids&id={id}&output=json", **kwargs):
        """
        Initialize with a url; it must contain the string "{id}" in it somewhere, and that will be replaced with the id in the get call.
        """
        self.url = url
    
    def get(self, id = None, **kwargs):
        path_or_url = self.url.format(id = id)
        try:
            req = _urlopen(path_or_url)
            buffer = BytesIO(req.read())
        except HTTPError:
            logging.exception("HTTP Error accessing %s" % path_or_url)
            raise
        return buffer
    
    def put(self, id):
        raise NotImplementedError("Writing files over http is not possible.")
        
class LocalResolver(IdResolver):
    def __init__(self, dir, format, compression):
        self.dir = dir
        self.format = format
        self.compression = compression

    def get(self, id, format = None, compression = None):
        filename = self.fname(id, format = format, compression = compression, suffix = None)
        dirname = self.dir
        fin = open(os.path.join(dirname, filename), 'rb')
        return self.decompress(fin, compression = compression)

    def put(self, buffer, id, format = None, compression = None):
        raise NotImplementedError("Not available")
        
class PathResolver(IdResolver):
    # A path is the simplest form of id storage. These are not HTIDs, and so aren't stored.
    # We could check to make sure the pathname makes sense. But we don't.
    def get(self, id, **kwargs):
        if id.endswith(".gz"):
            self.compression = "gz"
        if id.endswith(".bz2"):
            self.compression = "bz2"
        """ Use kwargs to absorb unused arguments."""
        return self.decompress(open(id, "rb"))
    
    def put(self, buffer, id, **kwargs):
        with open(id, "wb") as fout:
            fout.write(buffer)

class PairtreeResolver(IdResolver):
    def get(self, id, **kwargs):
        pass
    
    def put(self, id, **kwargs):
        raise NotImplementedError("It's enough trouble to get out of these things.")
    
class ZiptreeResolver(IdResolver):
    """
    This class includes methods for populating a ziptree from a pairtree.
    
    A 'ziptree' is a set of zipfiles. 
    """
    def __init__(self, dir, format, compression, pairtree_root = None, **kwargs):
        self.pairtree_root = pairtree_root
        if not os.path.exists(dir):
            os.makedirs(dir)
        super().__init__(dir = dir, format = format, compression = compression)
        
    def which_zipfile(self, id, digits = 3):
        # Use the sha1 hash of the id; and take only the first three digits.
        code = hashlib.sha1(bytes(id, 'utf-8')).hexdigest()[:digits]
        if digits == 0:
            return None
        return os.path.join(self.dir, code + ".zip")

    def put(self, object, id, format = "json", compression = "bz2", suffix = None, depth = 3):
        filename = self.fname(id, format, compression, suffix)
        with zipfile.ZipFile(self.which_zipfile(id, depth), mode="a") as zipdest:
            if filename in zipdest.namelist():
                return "already present"
            try:
                zipdest.open(filename, "w").write(object.read())
                return "successful insertion"                
            except:
                raise
            
    def get(self, id, format = None, suffix = None, compression = None, **kwargs):
        if format is None:
            format = self.format
        if compression is None:
            compression = self.compression
            
        filename = self.fname(id, format, compression, suffix)
        logging.debug(id)
        fin = self.which_zipfile(id)
        with zipfile.ZipFile(fin, mode = "r") as zipcontainer:
            return self.decompress(zipcontainer.open(filename, 'r'))
        
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
                with open(path, 'rb') as orig:
                    zipdest.open(filename, "w").write(orig.read())
                return "successful insertion"
            except:
                zipdest.close()
                if not dangerously:
                    raise
                return "assorted error"

    def retrieve(self, id):
        fin = self.which_zipfile(id)
        with zipfile.ZipFile(fin, mode = "r") as zipcontainer:
            fname = htrc_features.utils.clean_htid(id) + ".json.bz2"
            return zipcontainer.open(fname, 'r')

if __name__ == "__main__":
    import sys
    zipdir, id = sys.argv[1:]
    resolver = ZiptreeResolver(zipdir)
    import json
    import bz2
    v = bz2.decompress(resolver.retrieve(id).read())
