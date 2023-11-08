'''
JSON support for AmoreDB.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

import json
from . import AmoreDB


class JsonMixin:
    '''
    JSON coverter mix-in for AmoreDB.
    '''

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            json.dumps(record_data, ensure_ascii=False).encode('utf-8')
        )

    def record_from_raw_data(self, record_data):
        return json.loads(
            super().record_from_raw_data(record_data).decode('utf-8')
        )


class JsonAmoreDB(JsonMixin, AmoreDB):
    pass
