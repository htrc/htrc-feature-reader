class TermIndex(dict):
    ''' 
    Class for holding lists of ints for a key

    Acts like a dict, but initialized with a page number.
    When new keys are added, they start with an volume-length list of zeroes.
    '''

    def __init__(self, page_count):
        self.page_count = page_count
        pass

    def __missing__(self, key):
        self[key] = [0] * self.page_count
        return self[key]


