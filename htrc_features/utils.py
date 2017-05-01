import logging

def id_encode(id):
    return id.replace(":", "+").replace("/", "=").replace(".", ",")


def id2path(id):
    clean_id = id_encode(id)
    path = []
    while len(clean_id) > 0:
        val, clean_id = clean_id[:2], clean_id[2:]
        path.append(val)
    return '/'.join(path)


def download_file(htids, outdir='./', keep_dirs=False, silent=True):
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
    
    Download file to current directory, keeping pairtree directory structure;
    i.e. './nyp/pairtree_root/33/43/30/42/06/88/94/33433042068894/nyp.33433042068894.json.bz2':
    
    ```
    utils.download_file(htids='nyp.33433042068894', keep_dirs=True)
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

    if isinstance(htids, string_types):
        # Download a single file
        dest_file = id_to_rsync(htids)
        args = ["data.analytics.hathitrust.org::features/" + dest_file]
    else:
        # Download a list of files
        paths = [id_to_rsync(htid) for htid in htids]
        
        fdescrip, tmppath =  tempfile.mkstemp()
        with open(tmppath, mode='w') as f:
            f.write("\n".join(paths))
        args = ["--files-from=%s" % tmppath, "data.analytics.hathitrust.org::features/"]

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

    
def id_to_rsync(htid, **kwargs):
    '''
    Take an HTRC id and convert it to an Rsync location for syncing Extracted
    Features.
    '''
    if 'kind' in kwargs:
        logging.warn("The basic/advanced split with extracted features files "
                     "was removed in schema version 3.0. This function only "
                     "supports the current format for Rsync URLs, if you "
                     "would like to see the legacy 2.0 format, see Github: "
                     "https://github.com/htrc/htrc-feature-reader/blob/3e100ae"
                     "9ea45317443ae05f43a188b12afe2e69a/htrc_features/utils.py"
                     )
    libid, volid = htid.split('.', 1)
    volid_clean = id_encode(volid)
    filename = '.'.join([libid, volid_clean, 'json.bz2'])
    path = '/'.join([libid, 'pairtree_root', id2path(volid).replace('\\', '/'),
                     volid_clean, filename])
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
                                     'a pairtree path for Rsyncing that id\'s '
                                     'Extracted Features dataset file. This '
                                     'does not check if the file exists.')
    
    #group = parser.add_mutually_exclusive_group()
    parser.add_argument('id', type=str, nargs='*',
                        help="A HathiTrust id or multiple ids to convert.")
    
    parser.add_argument('--from-file', '-f', nargs='?', type=argparse.FileType('r'),
                        const='-',
                       help="Read volume ids from an external file. Use as flag or supply - to read from stdin.")
    
    parser.add_argument('--outfile', '-o', nargs='?', type=argparse.FileType('w'),
                        default=sys.stdout,
                        help="File to save to. By default it writes to standard out."
                       )
    return parser

def _htid2rsync_parse_args(parser, in_args):
    import sys
    args = parser.parse_args(in_args)
    if (args.id and len(args.id) > 0) and args.from_file:
        sys.stderr.write("ERROR: Can't combine id arguments with --from-file. Only use one. \n-----\n")
        parser.print_help()
        sys.exit(2)
        return
    elif args.id and len(args.id) > 0:
        urls = [id_to_rsync(htid) for htid in args.id]
        for url in urls:
            args.outfile.write(url+"\n")
    elif args.from_file:
        try:
            for line in args.from_file.readlines():
                url = id_to_rsync(line.strip())
                args.outfile.write(url+"\n")
        except KeyboardInterrupt:
            pass
    else:
        sys.stderr.write("ERROR: Need to supply volume ids, either through positional arguments or a file with --from-file. Run with --help for details. \n-----\n")
        parser.print_help()
        sys.exit(2)


if __name__ == '__main__':
    htid2rsync_cmd()
