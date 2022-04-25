import os
import tarfile
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

    def path(self, **kwargs):
        """
        an alias to open that returns a pathlib.Path instead of an open
        file. For certain non-local resolvers (e.g., zipfile, http) this may fail.
        """

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
        'open' method will then handle the method. Classes that use
        an on-disk structure or are otherwise capable of returning a
        pathlib.Path object should subclass FilesystemResolver and
        instead define a _path method that returns a pathlib.path object.
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

class FilesystemResolver(IdResolver):
    """
    Stubbytree, local, and pairtree all share some methods on the local
    filesystem.
    """
    def _path(self, id, format, compression, suffix, dir = None):
        raise NotImplementedError("An IdResolver superclass must overwrite the "
                                  "'_path' method to return a pathlib.Path object")

    def _open(self, id, mode, format=None, compression='default', suffix=None, **kwargs):
        assert(mode.endswith('b'))
        if compression == 'default':
            compression = self.compression
        full_path = self._path(id, format, compression, suffix)
        try:
            return full_path.open(mode = mode)
        except FileNotFoundError:
            if mode.startswith('w'):
                full_path.parent.mkdir(parents=True, exist_ok=True)
                return full_path.open(mode=mode)
            else:
                raise


class LocalResolver(FilesystemResolver):

    def __init__(self, dir, **kwargs):
        super().__init__(dir = dir, **kwargs)
#        compression = kwargs.get("compression", None)
#        suffix = kwargs.get("suffix", None)
#        full_path =  self._path(id, format, compression, suffix)

    def _path(self, id, format, compression, suffix, dir = None):
        if not dir:
            dir = self.dir
        filename = self.fname(id, format = format, compression = compression, suffix = suffix)
        return Path(dir, filename)

class PathResolver(FilesystemResolver):
    # A path is the simplest form of id storage. These are not HTIDs, and so aren't stored.
    # We could check to make sure the pathname makes sense. But we don't.
    def _path(self, id, format, compression = None, suffix = None, dir = None):
        return Path(id)

class PairtreeResolver(FilesystemResolver):
    def __init__(self, dir=None, **kwargs):
        if not dir:
            raise NameError("You must specify a directory with 'dir'")
        super().__init__(dir=dir, **kwargs)

    def _path(self, id, format, compression = None, suffix = None, dir = None):
        if dir is None:
            dir = self.dir
        path = Path(id_to_pairtree(id, format, suffix, compression)).parent
        fname = self.fname(id = id, format = format, suffix = suffix, compression = compression)
        full_path = Path(self.dir, path, fname)
        return full_path



class StubbytreeResolver(FilesystemResolver):
    '''
    An alternative to pairtree that uses loc/code, where the code is every third digit of the ID.
    '''
    def __init__(self, dir=None, **kwargs):
        if not dir:
            raise NameError("You must specify a directory with 'dir'")
        super().__init__(dir=dir, **kwargs)

    def _path(self, id, format = None, compression = 'default', suffix = None):
        """
        Returns a pathlib.Path object.
        """

        if not format:
            format = self.format

        path = Path(id_to_stubbytree(id, format, suffix, compression)).parent
        fname = self.fname(id, format = format, suffix = suffix, compression = compression)
        full_path = Path(self.dir, path, fname)
        return full_path

class StubbyTarResolver(IdResolver):
    """
    Stubbytree format, but with the final directory bundled into a tar file.

    Read-only (no support for creation.)

    Useful in situations where inode capacity, not disk storage, limits
    access.
    """
    def __init__(self, dir, format, compression, mode = 'r', **kwargs):
        self.stubbytree_root = dir
        if not os.path.exists(dir) and not mode.startswith('r'):
            os.makedirs(dir)
        super().__init__(dir = dir, format = format, mode = mode, compression = compression, **kwargs)

    def which_tarfile(self, id: str, compression = None) -> Path:
        if compression is None:
            compression = self.compression
        stubbypath = id_to_stubbytree(id, format=self.format, compression=compression)
        return self.dir / Path(stubbypath).parents[0].with_suffix(".tar")

    def _open(self, id, mode = 'r', format=None, compression='default', suffix=None, force=False, **kwargs):
        """
        Force: overwrite existing files where found. (not implemented).
        """

        if not format:
            format = self.format
        if compression == 'default':
            compression = self.compression
        if mode != 'rb':
            raise NotImplementedError("Tarfile formats are read-only.")

        filename = self.fname(id, format = format, suffix = suffix, compression = compression)
        fin = self.which_tarfile(id)
        foldername = Path(fin.parts[-1]).with_suffix("")

        container = tarfile.open(fin, mode = 'r')

        # Prepare it to be closed.
        self.active_buffers.append(container)

        try:
            fout = container.extractfile(str(foldername / filename))
        except KeyError:
            raise FileNotFoundError("{} not found in {}".format(str(foldername / filename), container))
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
    "stubbytar": StubbyTarResolver,
    "pairtree": PairtreeResolver,
    "local": LocalResolver,
    "http": HttpResolver
})



def chain_resolver(dicts = None):
    """
    dicts: a list of arguments to make into resolvers. E.g.
    [{"method": "stubbytree", "dir": "~/hathi-features", "format": "parquet"},
     {"method": "http", "format": "json", "compression": "bz2"}
    ]

    Requested ids are sought at each of these origins in turn. If not found,
    they are sought down the chain and cached to every higher location if possible.

    If no input is passed, looks for a file called ".htrc-config.yaml" and
    reads that into the above format.

    Returns a resolver that calls
    each one in order, starting with the first. Usually you will want to
    include caching on the first one.
    """
    if dicts is None:
        # Default is user global
        default = Path("~").expanduser()
        # But prefer parent directories of cwd.
        parents = [*Path(".").parents]
        parents.reverse() # So the last place looked is the current directory.
        for path in [default, *parents, Path(".")]:
            if (path / ".htrc-config.yaml").exists():
                import yaml
                dicts = yaml.safe_load((path / ".htrc-config.yaml").open())

            if (path / ".htrc-config.json").exists():
                import json
                dicts = json.load((path / ".htrc-config.json").open())
                break
        if dicts is None:
            raise ValueError("You must initiate a chain resolver with"
            "either a list of resolver specifications, or have a file"
            "called .htrc-config.yaml somewhere in your path.")
        if 'resolver' in dicts:
            dicts = dicts['resolver']
    import copy

    dicts = copy.deepcopy(dicts)

    for dict in dicts:
        assert("method" in dict)

    # This import has to wait until here to avoid circular dependencies.
    from .caching import make_fallback_resolver

    starting_dict = dicts.pop()
    starting_format = starting_dict["method"]
    del starting_dict["method"]
    current_resolver = resolver_nicknames[starting_format](**starting_dict)

    while len(dicts):
        next_layer = dicts.pop()
        cache_resolver = make_fallback_resolver(resolver_nicknames[next_layer['method']], current_resolver)
        del next_layer["method"]
        current_resolver = cache_resolver(**next_layer)

    return current_resolver
