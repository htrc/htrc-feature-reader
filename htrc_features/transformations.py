import numpy as np

def chunk_to_wem(chunk_tl, model, vocab=None, stop=True, log=True, min_ncount=10):
    ''' Take a file that has ['token', 'count'] data and convert to a WEM vector'''
    
    if 'token' in chunk_tl.columns and not 'token' in chunk_tl.index.names:
        chunk_tl = chunk_tl.set_index('token')[['count']]
    elif 'lowercase' in chunk_tl.columns and not 'lowercase' in chunk_tl.index.names:
        chunk_tl = chunk_tl.set_index('lowercase')[['count']]

    n_dim = model.vector_size
    placeholder = np.array(n_dim * [None])

    tl = chunk_tl #.copy() #Avoidable?
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
        raise BaseException("Counts and all_vecs don't align. Likely, this means there are duplicated tokens in the data"
                           " e.g. Passing in a dataframe with counts for multiple pages/chunks")

    #doc_wem = np.dot(all_vecs.T, counts)
    doc_wem = np.average(all_vecs, weights=counts, axis=0)
    
    return doc_wem

def chunk_last(page_counts, target):
    return _chunking_algorithm(page_counts, target, even = False, two_sided = False, procrastinate = True)

def chunk_even(page_counts, target):
    return _chunking_algorithm(page_counts, target, even = True, two_sided = True, procrastinate = False)

def chunk_ends(page_counts, target):
    return _chunking_algorithm(page_counts, target, even = "mids", two_sided = True, procrastinate = False)


def _chunking_algorithm(page_counts, target, even = False, two_sided = True, procrastinate = False):
    """
    page_counts: an np.array with individual level page counts.

    target: the number of words to target per chunk

    even: Whether to distribute overflow evenly, or to attempt to eliminate all overflow in the current chunk/pair of chunks.

    two_sided = whether to work from both the front and back simultaneously, or *only* from the front. (to operate from the back, just flip the page counts before sending it to this function.

    procrastinate: whether to leave all chunks intact without rebalancing.
    """

    # Register front and back offsets. At the beginning, this is the size of the full page counts;
    # as the process continues, the page_counts object will be slowly trimmed.
    assert(target > 0)

    # maintain pointers to navigate the array.
    position = [0, len(page_counts)]

    breaks = np.zeros(page_counts.shape[0], np.int)
    breaks[0] = 1
    
    loop = -1
       
    while True:
        loop += 1
        if loop > 10000:
            raise OverflowError("Unable to escape loop")
        if position[0] == position[1]:
            # Either we had a perfect final chunk, or
            # were passed an empty list to start.
            break
        
        forward = np.cumsum(page_counts)
        if two_sided:
            backward = np.cumsum(np.flip(page_counts))

        words_left = forward[-1]
        # Exit conditions
        if words_left < (target * 1.5):
            break
            
        overflow = words_left % target
    
        if (target - overflow) < overflow:
            overflow = -(target - overflow)
            
        if even == True or (even=="mids" and loop > 0):
            chunks_remaining = np.round(words_left/target)
            if chunks_remaining > 2 and two_sided:
                # The share belonging here
                overflow = overflow * 2 / chunks_remaining
            if (chunks_remaining > 1) and (two_sided == False):
                overflow = overflow/chunks_remaining
        # Split the overflow across the ends
        if two_sided:
            loc_target = target + overflow/2
        else:
            loc_target = target + overflow
        if procrastinate:
            # No overflow handling
            loc_target = target
            
        #What is this number supposed to be?    
        if two_sided and words_left < (target * 2.5):
            midpoint = np.argmin(np.abs(forward - words_left/2))
            breaks[midpoint + position[0] + 1]  = 1
            break

        best_front = np.argmin(np.abs(forward - loc_target))
        position[0] = position[0] + best_front + 1
        try:
            breaks[position[0]] = 1
        except IndexError:
            if position[0] == len(breaks):
                # Can happen if the last page is > 2.5x the chunk length
                break
            else:
                raise
        if two_sided:
            best_back = np.argmin(np.abs(backward - loc_target))
            position[1] = position[1] - best_back - 1
            breaks[position[1]] = 1
        
            if position[0] > position[1]:
                # happens very rarely, (only in tests)
                # when both sides want to eat the same chunk at once,
                # as with a single giant chunk in the exact middle.
                # Not bothering to be smart about it.
                position[1] = position[0]
                
            new_end = page_counts.shape[0] - best_back - 1
        else:
            # The end stays where it is.
            new_end = len(page_counts)
            
        page_counts = page_counts[(best_front + 1):(new_end)]
        
    return np.cumsum(breaks)
