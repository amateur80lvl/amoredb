'''
Brotli compression support for AmoreDB.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

import brotli
from . import AmoreDB


class BrotliMixin:
    '''
    Brotli compression mix-in for AmoreDB.
    '''

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            brotli.compress(record_data)
        )

    def record_from_raw_data(self, record_data):
        return brotli.decompress(
            super().record_from_raw_data(record_data)
        )
