import logging

try:
    from pairtree import id2path, id_encode
except:
    logging.debug("Falling back on custom functions to replace pairtree.")

    def id_encode(id):
        return id.replace(":", "+").replace("/", "=").replace(".", ",")

    def id2path(id):
        clean_id = id_encode(id)
        path = []
        while len(clean_id) > 0:
            val, clean_id = clean_id[:2], clean_id[2:]
            path.append(val)
        return '/'.join(path)


def id_to_rsync(htid, **kwargs):
    '''
    Take an HTRC id and convert it to an Rsync location for syncing Extracted
    Features
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
    import argparse
    parser = argparse.ArgumentParser(description='Convert a HathiTrust ID to '
                                     'a pairtree path for Rsyncing that id\'s '
                                     'Extracted Features dataset file. This '
                                     'does not check if the file exists.')
    parser.add_argument('id', type=str, nargs='+',
                        help="A HathiTrust id or multiple ids to convert.")
    args = parser.parse_args()
    urls = [id_to_rsync(htid) for htid in args.id]
    for url in urls:
        print(url)


if __name__ == '__main__':
    htid2rsync_cmd()
