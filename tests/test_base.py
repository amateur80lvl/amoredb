import pytest

from amoredb import AmoreDB

from amoredb.json import JsonAmoreDB, JsonMixin
from amoredb.str import StrAmoreDB
from amoredb.struct import StructAmoreDB

from amoredb.gzip import GzipMixin
from amoredb.lzma import LzmaMixin

from utils import _run_db_test

@pytest.mark.asyncio
async def test_amoredb():
    await _run_db_test(AmoreDB, [b'foo', b'bar', b'baz'])

@pytest.mark.asyncio
async def test_amoredb_json():
    await _run_db_test(JsonAmoreDB, [{'foo': 'bar'}, {'bar': 'foo'}])

@pytest.mark.asyncio
async def test_amoredb_str():
    await _run_db_test(StrAmoreDB, ['foo', 'bar', 'baz'])  # UTF-8 encoding by default
    await _run_db_test(StrAmoreDB, ['foo', 'bar', 'baz'], encoding='ascii')

@pytest.mark.asyncio
async def test_amoredb_struct():
    await _run_db_test(StructAmoreDB, [(10, b'spoons'), (20, b'cups'), (30, b'plates')], struct_format='B10p')

@pytest.mark.asyncio
async def test_amoredb_gzip_json():
    class JsonGzipAmoreDB(JsonMixin, GzipMixin, AmoreDB):
        pass
    await _run_db_test(JsonGzipAmoreDB, [{'foo': 'bar'}, {'bar': 'foo'}])

@pytest.mark.asyncio
async def test_amoredb_lzma_json():
    class JsonLzmaAmoreDB(JsonMixin, LzmaMixin, AmoreDB):
        pass
    await _run_db_test(JsonLzmaAmoreDB, [{'foo': 'bar'}, {'bar': 'foo'}])
