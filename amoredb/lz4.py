'''
LZ4 compression support for AmoreDB.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

import lz4.frame
from . import AmoreDB


class Lz4Mixin:
    '''
    LZ4 compression mix-in for AmoreDB.
    '''

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            lz4.frame.compress(record_data)
        )

    def record_from_raw_data(self, record_data):
        return lz4.frame.decompress(
            super().record_from_raw_data(record_data)
        )
