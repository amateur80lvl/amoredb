'''
String record format support for AmoreDB.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

from . import AmoreDB


class StrMixin:
    '''
    String coverter mix-in for AmoreDB.
    '''

    def __init__(self, *args, encoding='utf-8', **kwargs):
        self._str_encoding = encoding
        super().__init__(*args, **kwargs)

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            record_data.encode(self._str_encoding)
        )

    def record_from_raw_data(self, record_data):
        return super().record_from_raw_data(record_data).decode(self._str_encoding)


class StrAmoreDB(StrMixin, AmoreDB):
    pass
