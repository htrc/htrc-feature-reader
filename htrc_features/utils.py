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

SECREF = ['header', 'body', 'footer']


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


def group_tokenlist(in_df, pages=True, section='all', case=True, pos=True,
                    page_freq=False):
    '''
        Return a token count dataframe with requested folding.

        pages[bool]: If true, keep pages. If false, combine all pages.
        section[string]: 'header', 'body', 'footer' will only return those
            sections. 'all' will return all info, unfolded. 'group' combines
            all sections info.
        case[bool]: If true, return case-sensitive token counts.
        pos[bool]: If true, return tokens facets by part-of-speech.
        page_freq[bool]: If true, will simply count whether or not a token is
        on a page. Defaults to false.
    '''
    groups = []
    if pages:
        groups.append('page')
    if section in ['all'] + SECREF:
        groups.append('section')
    groups.append('token' if case else 'lowercase')
    if pos:
        groups.append('pos')

    if in_df.empty:
        return pd.DataFrame([], columns=groups)

    if section in ['all', 'group']:
        df = in_df
    elif section in SECREF:
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
        df.insert(len(df.columns), 'lowercase',
                  df.index.get_level_values('token').str.lower())

    # Check if we need to group anything
    if groups == ['page', 'section', 'token', 'pos']:
        if page_freq:
            df['count'] = 1
        return df
    else:
        if not page_freq:
            return df.reset_index().groupby(groups).sum()[['count']]
        elif page_freq and 'page' in groups:
            df = df.reset_index().groupby(groups).sum()[['count']]
            df['count'] = 1
            return df
        elif page_freq and 'page' not in groups:
            # We'll have to group page-level, then group again
            def set_to_one(x):
                x['count'] = 1
                return x
            return df.reset_index().groupby(['page']+groups).apply(set_to_one)\
                     .groupby(groups).sum()[['count']]


def group_linechars(df, section='all', place='all'):

    # Set up grouping
    groups = ['page']
    if section in SECREF + ['all']:
        groups.append('section')
    if place in ['begin', 'end', 'all']:
        groups.append('place')
    groups.append('character')

    # Set up slicing
    slices = [slice(None)]
    if section in ['all', 'group']:
        slices.append(slice(None))
    elif section in SECREF:
        slices.append([section])
    if place in ['begin', 'end']:
        slices.append([place])
    elif place in ['group', 'all']:
        # It's hard to imagine a use for place='group', but adding for
        # completion
        slices.append(slice(None))

    if slices != [slice(None)] * 3:
            df = df.loc[tuple(slices), ]

    if groups == ['page', 'section', 'place', 'character']:
        return df
    else:
        return df.groupby(groups).sum()[['count']]
