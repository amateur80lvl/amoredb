'''
Simple append-only database.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

__version__ = '0.0.1'

from .core import BaseAmoreDB, AmoreIO

class AmoreDB(BaseAmoreDB, AmoreIO):
    pass
