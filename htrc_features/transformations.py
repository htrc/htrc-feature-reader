import numpy as np

def chunk_to_wem(chunk_tl, model, vocab=None, stop=True, log=True, min_ncount=10):
    ''' Take a file that has ['token', 'count'] data and convert to a WEM vector'''
    
    if 'token' in chunk_tl.columns and not 'token' in chunk_tl.index.names:
        chunk_tl = chunk_tl.set_index('token')[['count']]
    elif 'lowercase' in chunk_tl.columns and not 'lowercase' in chunk_tl.index.names:
        chunk_tl = chunk_tl.set_index('lowercase')[['count']]

    n_dim = 300
    placeholder = np.array(n_dim * [None])

    tl = chunk_tl.copy() #Avoidable?
    tcolname = 'token' if 'token' in tl.index.names else 'lowercase'
    tl.index = tl.index.get_level_values(tcolname)

    if not vocab:
            vocab = set(model.vocab.keys())

    if stop:
        from spacy.lang.en.stop_words import STOP_WORDS
        vocab = vocab.difference(STOP_WORDS)

    # Cross-reference the page or volume vocab with the words in the model
    doc_vocab = set(tl.index.get_level_values(tcolname))
    joint_vocab = list(vocab.intersection(doc_vocab))

    if len(joint_vocab) <= min_ncount:
        return placeholder

    all_vecs = model[joint_vocab]

    # The counts will be used as weights for an average
    counts = tl.loc[joint_vocab]['count'].values
    if log:
        counts = np.log(1+counts)

    if counts.shape[0] != all_vecs.shape[0]:
        raise BaseException("Counts and all_vecs don't align. Like, this means there are duplicated tokens in the data"
                           " e.g. Passing in a dataframe with counts for multiple pages/chunks")

    doc_wem = np.dot(all_vecs.T, counts)
    
    return doc_wem