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


def id_to_rsync(htid, kind='basic'):
    '''
    Take an HTRC id and convert it to an Rsync location for syncing Extracted
    Features

    kind: [basic|advanced]
    '''
    libid, volid = htid.split('.', 1)
    volid_clean = id_encode(volid)
    filename = '.'.join([libid, volid_clean, kind, 'json.bz2'])

    path = '/'.join([kind, libid, 'pairtree_root',
                     id2path(volid).replace('\\', '/'), volid_clean, filename])
    return path
