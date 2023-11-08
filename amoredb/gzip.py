'''
GZIP compression support for AmoreDB.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

import gzip
from . import AmoreDB


class GzipMixin:
    '''
    GZIP compression mix-in for AmoreDB.
    '''

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            gzip.compress(record_data)
        )

    def record_from_raw_data(self, record_data):
        return gzip.decompress(
            super().record_from_raw_data(record_data)
        )
