'''
Brotli compression support for AmoreDB.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

import brotli
from . import AmoreDB

MODE_GENERIC = brotli.MODE_GENERIC
MODE_TEXT = brotli.MODE_TEXT
MODE_FONT = brotli.MODE_FONT

class BrotliMixin:
    '''
    Brotli compression mix-in for AmoreDB.

    The following compression parameters can be provided:

    * brotli_mode (int, optional):
        The compression mode can be MODE_GENERIC (default),
        MODE_TEXT (for UTF-8 format text input) or MODE_FONT (for WOFF 2.0).

    * compresslevel | brotli_quality (int, optional):
        Controls the compression-speed vs compression-
        density tradeoff. The higher the quality, the slower the compression.
        Range is 0 to 11. Defaults to 11.

    * lgwin (int, optional):
        Base 2 logarithm of the sliding window size.
        Range is 10 to 24. Defaults to 22.

    * lgblock (int, optional):
        Base 2 logarithm of the maximum input block size.
        Range is 16 to 24. If set to 0, the value will be set based on the
        quality. Defaults to 0.
    '''

    def __init__(self, *args, **kwargs):
        params_map = {
            'brotli_mode':    'mode',
            'compresslevel':  'quality',
            'brotli_quality': 'quality',
            'lgwin':          'lgwin',
            'lgblock':        'lgblock'
        }
        self._brotli_params = dict((params_map[k], v) for k, v in kwargs.items() if k in params_map)
        super().__init__(*args, **kwargs)

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            brotli.compress(record_data, **self._brotli_params)
        )

    def record_from_raw_data(self, record_data):
        return brotli.decompress(
            super().record_from_raw_data(record_data)
        )
