'''
Struct record format support for AmoreDB.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

import struct
from . import AmoreDB


class StructMixin:
    '''
    Struct coverter mix-in for AmoreDB.
    '''

    def __init__(self, *args, struct_format=None, **kwargs):
        self._struct_format = struct_format
        super().__init__(*args, **kwargs)

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            struct.pack(self._struct_format, *record_data)
        )

    def record_from_raw_data(self, record_data):
        return struct.unpack(
            self._struct_format,
            super().record_from_raw_data(record_data)
        )


class StructAmoreDB(StructMixin, AmoreDB):
    pass
