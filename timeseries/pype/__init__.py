from .pipeline import Pipeline
from .lib_import import *
import pype
import pkg_resources

try:
    __version__ = pkg_resources.get_distribution(__name__).version
except:
    __version__ = 'unknown'
