import pytest
from htrc_features import Volume, MissingDataError, MissingFieldError, resolvers
from htrc_features.caching import make_fallback_resolver
import os
import pandas as pd


def copy_between_resolvers(id, resolver1, resolver2):
    input = Volume(id, id_resolver=resolver1)
    output = Volume(id, id_resolver=resolver2, mode = 'wb')
    output.write(input)



