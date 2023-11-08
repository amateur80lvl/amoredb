'''
BSON support for AmoreDB.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

import simple_bson as bson
from . import AmoreDB


class BsonMixin:
    '''
    BSON coverter mix-in for AmoreDB.
    '''

    def record_to_raw_data(self, record_data):
        return super().record_to_raw_data(
            bson.dumps(record_data)
        )

    def record_from_raw_data(self, record_data):
        return bson.loads(
            super().record_from_raw_data(record_data)
        )


class BsonAmoreDB(BsonMixin, AmoreDB):
    pass
