import htrc_features.resolvers as resolvers
import logging

from htrc_features.feature_reader import Volume
from htrc_features.parsers import MissingDataError
from .resolvers import resolver_nicknames

def copy_between_resolvers(id, resolver1, resolver2):
#    print (resolver1, "--->", resolver2)
    input = Volume(id, id_resolver=resolver1)
    output = Volume(id, id_resolver=resolver2, mode = 'wb')
    output.write(input)
    
def make_fallback_resolver(preferred, fallback = None, cache = True):
    """

    A function to return a constructor that uses 
    a cache.

    If 'fallback' is None, the actual fallback creation can be handled by the user (usually by
    attaching an IdResolver to self.fallback).

    """
    if preferred in resolver_nicknames:
        preferred = resolver_nicknames[preferred]
    
    class FallbackResolver(preferred):

        """
        A cache resolver is a resolver that uses either of two methods.

        It's called with the constructors of both arguments. If you have options
        for the fallback, they must be passed as dict in the first argument; 
        then the rest of the args are passed as normal.
        """

        def __init__(self, fallback_kwargs = {}, **preferred_args):
            if fallback in resolver_nicknames:
                self.fallback = resolver_nicknames[fallback](**fallback_kwargs)       
            elif isinstance(fallback, resolvers.IdResolver):
                self.fallback = fallback
            elif issubclass(fallback, resolvers.IdResolver):
                self.fallback = fallback(**fallback_kwargs)
            elif self.fallback is None:
                self.fallback = None
            else:
                raise TypeError("You must pass an id_resolver to do fallback searches.")

            # Keep a super instance around for copying.
            self.super = preferred(**preferred_args)
            
            super().__init__(**preferred_args)            

        def open(self, id, fallback_kwargs = {}, **kwargs):
            
            """
            Open a file with a fallback method.


            kwargs: a set of arguments that will be passed to open.
          
            """
            if len(fallback_kwargs) != 0:
                logging.warn("""You seem to be passing kwargs to a fallback
                parser while opening. This feature may be deprecated because
                I can't think of any case in which it would be useful.
                Well, I guess if you're using two different parquet caches
                at once??
                Please e-mail bmschmidt@gmail.com if you want it to stick around!
                And send me your use case.
                """)
            try:
                fout = super().open(id, **kwargs)
                logging.debug("Successfully returning from cache")
                return fout
            except (MissingDataError, FileNotFoundError) as e:
                if self.fallback is None:
                    logging.warning("No fallback defined")
                    raise e
                if not cache:
                    input = Volume(id, id_resolver=self.fallback, **fallback_kwargs)
                    return input.parser.open(id, **fallback_kwargs)
                else:
                    copy_between_resolvers(id, self.fallback, self.super)
                    fout = super().open(id, **kwargs)
                    logging.debug("Successfully returning from cache")
                    return fout           
            
    return FallbackResolver

