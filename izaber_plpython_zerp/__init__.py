import os

from izaber import config, app_config, autoloader
from izaber.startup import request_initialize, initializer
from izaber.log import log

import importlib
import izaber.plpython

from izaber.plpython.zerp.base import IPLPY

__version__ = '1.0'

CONFIG_BASE = """
"""

def reload_base():
    izaber.plpython.reload_base()
    importlib.reload(izaber.plpython.zerp.base)
    from izaber.plpython.zerp.base import IPLPY
    return IPLPY

def init_plpy(plpy_globals,reload=False):
    global IPLPY

    plpy = plpy_globals['plpy']

    if reload or plpy_globals['GD'].get('always_reload'):
        plpy_globals['plpy'].debug(
            "Reloading izaber.plpython.zerp.base"
        )
        IPLPY = reload_base()
    return IPLPY(plpy_globals)


