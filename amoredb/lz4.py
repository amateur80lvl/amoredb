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

    The following compression parameters can be provided:

    * lz4_block_size (int):
        Specifies the maximum blocksize to use. Options:
            lz4.frame.BLOCKSIZE_DEFAULT: the lz4 library default
            lz4.frame.BLOCKSIZE_MAX64KB: 64 kB
            lz4.frame.BLOCKSIZE_MAX256KB: 256 kB
            lz4.frame.BLOCKSIZE_MAX1MB: 1 MB
            lz4.frame.BLOCKSIZE_MAX4MB: 4 MB
        If unspecified, will default to lz4.frame.BLOCKSIZE_DEFAULT
        which is currently equal to lz4.frame.BLOCKSIZE_MAX64KB.

    * lz4_block_linked (bool):
        Specifies whether to use block-linked compression.
        If True, the compression ratio is improved, particularly for small block sizes.
        Default is True.

    * compresslevel (int):
        Specifies the level of compression used.
        Values between 0-16 are valid, with 0 (default) being the lowest
        compression (0-2 are the same value), and 16 the highest.
        Values below 0 will enable “fast acceleration”, proportional to the value.
        Values above 16 will be treated as 16.
        The following module constants are provided as a convenience:
            lz4.frame.COMPRESSIONLEVEL_MIN: Minimum compression (0, the default)
            lz4.frame.COMPRESSIONLEVEL_MINHC: Minimum high-compression mode (3)
            lz4.frame.COMPRESSIONLEVEL_MAX: Maximum compression (16)

    * lz4_content_checksum (bool):
        Specifies whether to enable checksumming of the uncompressed content.
        If True, a checksum is stored at the end of the frame, and checked during decompression.
        Default is False.

    * lz4_block_checksum (bool):
        Specifies whether to enable checksumming of the uncompressed content of each block.
        If True a checksum of the uncompressed data in each block in the frame is stored at
        the end of each block. If present, these checksums will be used
        to validate the data during decompression.
        The default is False meaning block checksums are not calculated and stored.
        This functionality is only supported if the underlying LZ4 library has version >= 1.8.0.
        Attempting to set this value to True with a version of LZ4 < 1.8.0
        will cause a RuntimeError to be raised.

    * lz4_store_size (bool):
        If True then the frame will include an 8-byte header field that is the uncompressed
        size of data included within the frame.
        Default is True.
    '''

    def __init__(self, *args, **kwargs):
        params_map = {
            'lz4_block_size':       'block_size',
            'lz4_block_linked':     'block_linked',
            'compresslevel':        'compression_level',
            'lz4_content_checksum': 'content_checksum',
            'lz4_block_checksum':   'block_checksum',
            'lz4_store_size':       'store_size',
        }
        self._lz4_params = dict(
            (params_map[k], kwargs.pop(k)) for k in list(kwargs.keys()) if k in params_map
        )
        super().__init__(*args, **kwargs)

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            lz4.frame.compress(record_data, **self._lz4_params)
        )

    def record_from_raw_data(self, record_data):
        return lz4.frame.decompress(
            super().record_from_raw_data(record_data)
        )
