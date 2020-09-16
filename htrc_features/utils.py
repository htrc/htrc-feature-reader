import logging
import os

EF_CHECK_URL= "http://data.htrc.illinois.edu/htrc-ef-access/get?action=check-exists&ids={}"

# Genre reference for the genres using the MARC Genre Terms schema, hardcoded to avoid excessive dependencies. From http://id.loc.gov/vocabulary/marcgt/
LOC_MARCGT_REFERENCE = {"rev": "review", "atl": "atlas", "lan": "language instruction", "mot": "motion picture", "dra": "drama", "com": "computer program", "ins": "instruction", "his": "history", "fon": "font", "sur": "survey of literature", "art": "article", "num": "numeric data", "lec": "legal case and case notes", "han": "handbook", "map": "map", "sta": "statistics", "pro": "programmed text", "loo": "loose-leaf", "doc": "document (computer)", "reh": "rehearsal", "pos": "postcard", "fin": "finding aid", "mem": "memoir", "law": "law report or digest", "arr": "art reproduction", "rea": "realia", "ess": "essay", "aro": "art original", "lea": "legal article", "enc": "encyclopedia", "ser": "series", "stp": "standard or specification", "hum": "humor, satire", "vid": "videorecording", "wal": "wall map", "sli": "slide", "mic": "microscope slide", "off": "offprint", "dir": "directory", "rem": "remote sensing image", "man": "manuscript", "kit": "kit", "boo": "book", "gov": "government publication", "poe": "poetry", "rep": "representational", "web": "web site", "tra": "transparency", "inm": "interactive multimedia", "dio": "diorama", "iss": "issue", "puz": "puzzle", "pat": "patent", "leg": "legislation", "per": "periodical", "ons": "online system or service", "nos": "nonmusical sound", "fla": "flash card", "cal": "calendar", "yea": "yearbook", "scr": "script", "gra": "graphic", "new": "newspaper", "rpt": "reporting", "glo": "globe", "sho": "short story", "fol": "folktale", "dic": "dictionary", "fes": "festschrift", "gam": "game", "ind": "index", "toy": "toy", "cpb": "conference publication", "jou": "journal", "spe": "speech", "bib": "bibliography", "the": "thesis", "ter": "technical report", "dis": "discography", "dtb": "database", "fil": "filmography", "int": "interview", "sou": "sound", "bio": "biography", "abs": "abstract or summary", "pic": "picture", "cha": "chart", "fls": "filmstrip", "ted": "technical drawing", "mod": "model", "cat": "catalog", "cgn": "comic or graphic novel", "pla": "playing cards", "let": "letter", "cod": "comedy", "fic": "fiction", "bda": "bibliographic data", "aut": "autobiography", "nov": "novel", "tre": "treaty"}

def _id_encode(id):
    '''
    :param id: A Pairtree ID. If it's a Hathitrust ID, this is the part after the library
        code; e.g. the part after the first period for vol.123/456.
    :return: A sanitized id. e.g., 123/456 will return as 123=456 to avoid filesystem issues.
    '''
    return id.replace(":", "+").replace("/", "=").replace(".", ",")

def _id_decode(id):
    '''
    :param id: A sanitized Pairtree ID.
    :return: An original Pairtree ID.
    '''
    return id.replace("+", ":").replace("=", "/").replace(",", ".")

def files_available(ids):
    """
    Check for EF files matching a list of volume IDs.

    :param ids: List of HathiTrust IDs
    :return: Dictionary of boolean matches for whether the corresponding file exists in
        the Extract Features Dataset.
    """
    import requests

    url = EF_CHECK_URL.format(",".join(ids))
    result = requests.get(url).json()
    return result

def extract_htid(filename):
    """
    Inverse of clean_htid, that also strips file suffixes
    """
    def trim(string, suffix):
        if string.endswith(suffix):
            return string[:-len(suffix)]
        return string
    
    for suffix in [".gz", ".bz2"]:
        filename = trim(filename, suffix)
    for suffix in [".json", ".parquet"]:
        filename = trim(filename, suffix)
    for suffix in [".meta", ".tokens", ".chars", ".section"]:
        filename = trim(filename, suffix)
                       
    return _id_decode(filename)

def clean_htid(htid):
    '''
    :param htid: A HathiTrust ID of form lib.vol; e.g. mdp.1234
    :return: A sanitized version of the HathiTrust ID, appropriate for filename use.
    '''
    libid, volid = htid.split('.', 1)
    volid_clean = _id_encode(volid)
    return '.'.join([libid, volid_clean])


def _id2path(id):
    '''
    :param id: Pairtree ID. For HathiTrust, only the volume id of the lib.vol id format.
    :type id: str
    :return: A corresponding file path for the id.
    '''
    clean_id = _id_encode(id)
    path = []
    while len(clean_id) > 0:
        val, clean_id = clean_id[:2], clean_id[2:]
        path.append(val)
    return path


def download_file(htids, outdir='./', keep_dirs=False, silent=True, rsync_endpoint='ef-latest', format='stubbytree'):
    '''
    A function for downloading one or more Extracted Features files by ID.
    
    This uses a subprocess call to 'rsync', so will only work if rsync is available
    on your system and accessible in the same environment as Python.
    
    Returns (return code, stdout) tuple.
    
    htids:
        A string or list of strings, comprising HathiTrust identifiers.
        
    outdir:
        Location to save the file(s). Defaults to current directory.
        
    keep_dirs:
        Whether to keep the remote pairtree file structure or save just the files to outdir.
        Defaults to False (flattening).
        
    rsync_endpoint:
        Location of rsync endpoint directory, *or* one of ['ef-latest', 'ef-2.0', 'ef-1.5'] to point to HTRC servers.
        
    silent:
        If False, return the rsync stdout.
        
     
    Usage
    -------
    
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

    Download to current directory (EF 2.0 format), keeping stubbytree directory structure;
    i.e. './nyp/33469/nyp.33433042068894.json.bz2':
    
    ```
    utils.download_file(htids='nyp.33433042068894', keep_dirs=True)
    ```
    
    Download EF 1.5 file to current directory, keeping pairtree directory structure;
    i.e. './nyp/pairtree_root/33/43/30/42/06/88/94/33433042068894/nyp.33433042068894.json.bz2':
    
    ```
    utils.download_file(htids='nyp.33433042068894', keep_dirs=True, rsyncroot='ef1.5', format='pairtree')
    ```
    
    '''
    import subprocess
    import tempfile
    import os
    import sys
    from six import string_types

    tmppath = None
    sub_kwargs = dict()
    
    if not outdir.endswith("/"):
        outdir += "/"
    
    if keep_dirs:
        relative = '--relative'
    else:
        relative = '--no-relative'
        
    if rsync_endpoint == 'ef-latest':
        rsync_endpoint = "data.analytics.hathitrust.org::features-2020.03"
    elif rsync_endpoint == 'ef-2.0':
        rsync_endpoint = "data.analytics.hathitrust.org::features-2020.03"
    elif rsync_endpoint == 'ef-1.5':
        if format == 'stubbytree':
            logging.warn("ef-1.5 does not use stubbytree format; forcing pairtree")
            format = "pairtree"
        rsync_endpoint = "data.analytics.hathitrust.org::features-2018.01"

    if isinstance(htids, string_types):
        # Download a single file
        dest_file = id_to_rsync(htids, format=format)
        args = [rsync_endpoint.strip('/') + '/' + dest_file]
    else:
        # Download a list of files
        paths = [id_to_rsync(htid, format=format) for htid in htids]
        
        fdescrip, tmppath =  tempfile.mkstemp()
        with open(tmppath, mode='w') as f:
            f.write("\n".join(paths))
        args = ["--files-from=%s" % tmppath, rsync_endpoint.strip('/') + '/']

    cmd = ["rsync", relative, "-a","-v"] + args + [outdir]
    
    major, minor = sys.version_info[:2]
    if (major >= 3 and minor >=5):
        # Recommended use for 3.5+ is subprocess.run
        if not silent:
            sub_kwargs = dict(stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        else:
            devnull = open(os.devnull, 'w')
            sub_kwargs = dict(stdout=devnull, stderr=devnull, universal_newlines=True)

        response = subprocess.run(cmd, check=True, **sub_kwargs)
        out = (response.returncode, response.stdout)
    else:
        # Support older Python, currently without error catching
        out = (subprocess.call(cmd), None)
    
    if tmppath:
        f.close()
        os.close(fdescrip)
        os.remove(tmppath)
        
    return out

def id_to_pairtree(htid, format = None, suffix = None, compression = None):
    '''
    Take an HTRC id and convert it to a pairtree location.

    suffix: a filename suffix to add to the end.

    '''
    libid, volid = htid.split('.', 1)
    volid_clean = _id_encode(volid)
    
    suffixes = [s for s in [format, compression] if s is not None]
    filename = ".".join([clean_htid(htid), *suffixes]) 
    path = os.path.join(*[libid, 'pairtree_root', * _id2path(volid),
                     volid_clean, filename])
    return path

def id_to_stubbytree(htid, format = None, suffix = None, compression = None):
    '''
    Take an HTRC id and convert it to a 'stubbytree' location.

    '''
    libid, volid = htid.split('.', 1)
    volid_clean = _id_encode(volid)

    suffixes = [s for s in [format, compression] if s is not None]
    filename = ".".join([clean_htid(htid), *suffixes])
    path = os.path.join(libid, volid_clean[::3], filename)
    return path
    
def id_to_rsync(htid, format="stubbytree"):
    '''
    Take an HTRC id and convert it to an Rsync location for syncing Extracted
    Features.
    '''
    if format == 'stubbytree':
        id_to_path_func = id_to_stubbytree
    elif format == "pairtree":
        id_to_path_func = id_to_pairtree
    else:
        raise ValueError("Unknown format for id_to_rsync")
    path = id_to_path_func(htid, format = "json", compression = "bz2")
    return path


def htid2rsync_cmd():
    ''' A module to install for command line access, through 'htid2rsync' '''
    import sys
    parser = _htid2rsync_argparser()
    _htid2rsync_parse_args(parser, sys.argv[1:])

def _htid2rsync_argparser():
    '''
    Return arg parser. Separated from htid2rsync_cmd For easier testing.
    '''
    import argparse
    import sys
    parser = argparse.ArgumentParser(description='Convert a HathiTrust ID to '
                                     'a stubbytree path for Rsyncing that id\'s '
                                     'Extracted Features dataset file. This '
                                     'does not check if the file exists.')
    
    #group = parser.add_mutually_exclusive_group()
    parser.add_argument('id', type=str, nargs='*',
                        help="A HathiTrust id or multiple ids to convert.")
    
    parser.add_argument('--from-file', '-f', nargs='?', type=argparse.FileType('r'),
                        const='-',
                       help="Read volume ids from an external file. Use as flag or supply - to read from stdin.")
    
    parser.add_argument('--oldstyle', '-s', action="store_true",
                       help="Whether to use the pre-EF2.0 file structure (pairtree) rather than the current stubbytree.")
    
    parser.add_argument('--outfile', '-o', nargs='?', type=argparse.FileType('w'),
                        default=sys.stdout,
                        help="File to save to. By default it writes to standard out."
                       )
    return parser

def _htid2rsync_parse_args(parser, in_args):
    import sys
    args = parser.parse_args(in_args)
    style = ("pairtree" if args.oldstyle else "stubbytree")
    if (args.id and len(args.id) > 0) and args.from_file:
        sys.stderr.write("ERROR: Can't combine id arguments with --from-file. Only use one. \n-----\n")
        parser.print_help()
        sys.exit(2)
        return
    elif args.id and len(args.id) > 0:
        urls = [id_to_rsync(htid, format=style) for htid in args.id]
        for url in urls:
            args.outfile.write(url+"\n")
    elif args.from_file:
        try:
            for line in args.from_file.readlines():
                url = id_to_rsync(line.strip(), format=style)
                args.outfile.write(url+"\n")
        except KeyboardInterrupt:
            pass
    else:
        sys.stderr.write("ERROR: Need to supply volume ids, either through positional arguments or a file with --from-file. Run with --help for details. \n-----\n")
        parser.print_help()
        sys.exit(2)

if __name__ == '__main__':
    htid2rsync_cmd()
