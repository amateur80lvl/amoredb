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

    The following compression parameters can be provided:

    * compresslevel
        Range 0-9 where 0 means no compression and 9 is highest compression.
        Default: 9
    '''

    def __init__(self, *args, compresslevel=9, **kwargs):
        self._compresslevel = compresslevel
        super().__init__(*args, **kwargs)

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            gzip.compress(record_data, compresslevel=self._compresslevel)
        )

    def record_from_raw_data(self, record_data):
        return gzip.decompress(
            super().record_from_raw_data(record_data)
        )
