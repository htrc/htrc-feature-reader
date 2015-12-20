from collections import defaultdict
import logging
import pandas as pd

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

_secref = ['header', 'body', 'footer']


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


def merge_token_duplicates(tokens):
    ''' fold a list of tokens when there are duplicates, such as when
    case-folding '''
    folded = defaultdict(lambda: defaultdict(int))
    for (token, c) in tokens:
        assert(type(c) == dict)
        for (pos, poscount) in c.iteritems():
            folded[token][pos] += poscount
    return folded


def group_tokenlist(in_df, pages=True, section='all', case=True, pos=True):
    '''
        Return a token count dataframe with requested folding.

        pages[bool]: If true, keep pages. If false, combine all pages.
        section[string]: 'header', 'body', 'footer' will only return those
            sections. 'all' will return all info, unfolded. 'group' combines
            all sections info.
        case[bool]: If true, return case-sensitive token counts.
        pos[bool]: If true, return tokens facets by part-of-speech.
    '''
    groups = []
    if pages:
        groups.append('page')
    if section in ['all'] + _secref:
        groups.append('section')
    groups.append('token' if case else 'lowercase')
    if pos:
        groups.append('pos')

    if section in ['all', 'group']:
        df = in_df
    elif section in _secref:
        idx = pd.IndexSlice
        try:
            df = in_df.loc[idx[:, section, :, :], ]
        except KeyError:
            logging.debug("Section {} not available".format(section))
            df = pd.DataFrame([], columns=groups+['count'])\
                   .set_index(groups)
            return df
    else:
        logging.error("Invalid section argument: {}".format(section))
        return

    # Add lowercase column. Previously, this was saved internally. However,
    # DataFrame.str.lower() is reasonably fast and the need to call it
    # repeatedly is low, so it is no longer saved.
    if not case:
        logging.debug('Adding lowercase column')
        df = df.reset_index()
        df['lowercase'] = df['token'].str.lower()
        df.set_index(['page', 'section', 'lowercase', 'token', 'pos'],
                     inplace=True)

    # Check if we need to group anything
    if groups == ['page', 'section', 'token', 'pos']:
        return df
    else:
        return df.reset_index().groupby(groups).sum()
