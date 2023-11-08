import pytest

from amoredb import AmoreDB

from amoredb.bson import BsonAmoreDB, BsonMixin

from amoredb.brotli import BrotliMixin
from amoredb.lz4 import Lz4Mixin
from amoredb.snappy import SnappyMixin

from utils import _run_db_test

@pytest.mark.asyncio
async def test_amoredb_bson():
    await _run_db_test(BsonAmoreDB, [{'foo': 'bar'}, {'bar': 'foo'}])

@pytest.mark.asyncio
async def test_amoredb_brotli_bson():
    class BsonBrotliAmoreDB(BsonMixin, BrotliMixin, AmoreDB):
        pass
    await _run_db_test(BsonBrotliAmoreDB, [{'foo': 'bar'}, {'bar': 'foo'}])

@pytest.mark.asyncio
async def test_amoredb_lz4_json():
    class BsonLz4AmoreDB(BsonMixin, Lz4Mixin, AmoreDB):
        pass
    await _run_db_test(BsonLz4AmoreDB, [{'foo': 'bar'}, {'bar': 'foo'}])

@pytest.mark.asyncio
async def test_amoredb_snappy_json():
    class BsonSnappyAmoreDB(BsonMixin, SnappyMixin, AmoreDB):
        pass
    await _run_db_test(BsonSnappyAmoreDB, [{'foo': 'bar'}, {'bar': 'foo'}])
