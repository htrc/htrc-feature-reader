from collections import defaultdict


def id_to_rsync(htid, kind='basic'):
    '''
    Take an HTRC id and convert it to an Rsync location for syncing Extracted
    Features
    
    kind: [basic|advanced]
    '''
    clean_htid = id_to_filename(htid)
    institution = clean_htid.split('.')[0]
    loc = clean_htid[len(institution)+1:]
    
    url = [institution, "pairtree_root"]
    while len(loc) > 0:
        val, loc = loc[:2], loc[2:]
        url.append(val)
    url += ["".join(url[2:]), clean_htid]
    return "%s/%s.%s.json.bz2" % (kind, "/".join(url), kind)


def id_to_filename(id):
    filename = id.replace(":", "+").replace("/", "=")
    # Replace any periods after the first
    b = filename.split('.')
    filename = ".".join(b[:2]) + ",".join(['']+b[2:])
    return filename


def merge_token_duplicates(tokens):
    ''' fold a list of tokens when there are duplicates, such as when case-folding '''
    folded = defaultdict(lambda: defaultdict(int))
    for (token, c) in tokens:
        assert(type(c)==dict)
        for (pos, poscount) in c.iteritems():
            folded[token][pos] += poscount
    return folded
