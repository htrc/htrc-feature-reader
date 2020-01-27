import yaml
import htrc_features
import os

_config = None

def config():
    """
    Quickly return a configuration object.
    """
    global _config
    if _config is not None:
        return _config
    else:
        _config = load_configuration()
        return _config

def load_configuration(*args):
    """
    Return a configuration object including reloading all file locations.

    Any arguments are treated as yaml files to be parsed: file not found errors
    are silently ignored.
    """
    global _config
    _config = {}

    base_path = os.path.split(htrc_features.__file__)[0]
    base_file = base_path + "/default_htrc_config.yml"

    for location in [
            base_file,
            os.path.expanduser("~/.htrc_features_config.yml"),
            "htrc_features_config.yml",
            *args
        ]:                  
            try:
                print(base_file)
                fin = open(location)
                _config.update(yaml.load(fin, Loader=yaml.FullLoader))
            except FileNotFoundError:
                continue
    
    return _config
