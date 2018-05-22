import os

from izaber import config, app_config, autoloader
from izaber.startup import request_initialize, initializer
from izaber.log import log

import izaber.plpython

from izaber.plpython.zerp.base import IPLPY

__version__ = '1.0'

CONFIG_BASE = """
"""

def reload_base():
    import importlib
    importlib.reload(izaber.plpython.zerp.base)
    from izaber.plpython.zerp.base import IPLPY
    izaber.plpython.reload_base()

def init_plpy(plpy_globals,reload=False):
    if reload or plpy_globals['GD'].get('always_reload'):
        reload_base()
    return IPLPY(plpy_globals)


