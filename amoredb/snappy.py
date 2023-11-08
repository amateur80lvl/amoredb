'''
Snappy compression support for AmoreDB.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

import snappy
from . import AmoreDB


class SnappyMixin:
    '''
    Snappy compression mix-in for AmoreDB.
    '''

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            snappy.compress(record_data)
        )

    def record_from_raw_data(self, record_data):
        return snappy.uncompress(
            super().record_from_raw_data(record_data)
        )
