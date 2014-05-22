from collections import defaultdict

def merge_token_duplicates(tokens):
    ''' fold a list of tokens when there are duplicates, such as when case-folding '''
    folded = defaultdict(lambda: defaultdict(int))
    print("@@@")
    print(tokens)
    for (token, c) in tokens:
        assert(type(c)==dict)
        for (pos, poscount) in c.iteritems():
            folded[token][pos] += poscount
    return folded
