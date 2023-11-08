# AmoreDB

Simple append-only database for Python with rich record formats and compression.

## For impatients

### Example 1

```python
from amoredb import AmoreDB

with AmoreDB('test', 'w') as db:
    db.append(b'foo')
    db.append(b'bar')
    db.append(b'baz')
    for i, record in enumerate(db):
        print(i, record)
```

Result:

```
0 b'foo'
1 b'bar'
2 b'baz'
```

### Example 2

```python
from amoredb.json import JsonAmoreDB

with JsonAmoreDB('test.json', 'w') as db:
    db.append({'foo': 'bar'})
    db.append({'bar': 'foo'})
    for i, record in enumerate(db):
        print(i, record)
```

Result:

```
0 {'foo': 'bar'}
1 {'bar': 'foo'}
```

### Example 3

```python
async with AsyncAmoreDB('test', 'w') as db:
    await db.append({'foo': 'bar'})
    await db.append({'bar': 'foo'})
    async for i, record in enumerate(db):
        print(i, record)
    async for i, record in enumerate(db):
        print(i, record)
```

## Record formats

The basic format for database records is bytes object. Subclasses may support other formats,
as demonstrated above in the Example 2. AmoreDB provides support for the following formats:

* JSON: `JsonMixin`, `JsonAmoreDB` from `amoredb.json`
* strings: `StrMixin`, `StrAmoreDB` from `amoredb.str`
* structures: `StructMixin`, `StructAmoreDB` from `amoredb.struct`
* BSON: `BsonMixin`, `BsonAmoreDB` from `amoredb.bson`, requires [simple_bson](https://pypi.org/project/simple-bson/) package

Records are converted to the binary data by mixins and AmoreDB provides predefined classes
which are defined, for example, as

```python
class JsonAmoreDB(JsonMixin, AmoreDB):
    pass
```

## Record compression

Similar to record format conversion, compression is implemented by mix-ins.
AmoreDB provides a few for the following formats:

* gzip: `amoredb.gzip.GzipMixin`
* lzma: `amoredb.lzma.LzmaMixin`
* lz4: `amoredb.lzma.Lz4Mixin`, requires [lz4](https://pypi.org/project/lz4/) package
* brotli: `amoredb.brotli.BrotliMixin`, requires [brotli](https://pypi.org/project/Brotli/) package
* snappy: `amoredb.snappy.SnappyMixin`, requires [python-snappy](https://pypi.org/project/python-snappy/) package

There are no predefined classes for compression, it's up to end users to define one for their needs.
For example,

```python
from amoredb import AmoreDB
from amoredb.json import JsonMixin
from amoredb.gzip import GzipMixin

class MyDB(JsonMixin, GzipMixin, AmoreDB):
    pass

with MyDB('test.json.gz', 'w') as db:
    db.append({'foo': 'bar'})
    db.append({'bar': 'foo'})
    for i, record in enumerate(db):
        print(i, record)
```

## Record transformation pipeline

Records in AmoreDB are processed by the following methods:

```python
    def record_to_raw_data(self, record_data):
        # do custom conversion here
        # ...
        # call base method
        return super().record_to_raw_data(record_data)

    def record_from_raw_data(self, record_data):
        # do custom conversion here
        # ...
        # call base method
        return super().record_from_raw_data(record_data)
```

Mix-ins override these methods and to make pipeline working, mix-ins should be defined in the right order.
As we have seen above,

```python
class MyDB(JsonMixin, GzipMixin, AmoreDB):
    pass
```

`GzipMixin` is placed in between, because compression takes place after converting record from JSON to binary data
and before writing this data to file. Same for opposite direction.

## asyncio support

from amoredb import AsyncAmoreDB


## Database structure

The database consists of data file and index file. Optional metadata file in JSON format may contain
the definition of the database class.

Index file contains positions of records except the first one which is always zero.
The first element in index file is the offset of the next record.
Thus, the number of items in the index file equals to the number of records.

Record id is implicit, it is the index of the record.
Thus, to get a record by id, read its offset from the index file and then read the record from data file.

## Utilities

* amore-get
* amore-add

## Algorithm

Write:
* lock index file exclusively
* get position for the new record from the index file
* append new record to the data file
* write new size of data file to the index file
* release file lock

Read item by id:
* Given that id is the index of record, seek to id * 16 in the index file.
* Read the position of the record and the position of the next record from the index file, 16 bytes total.
* Read the record from the data file, the size of record is calculated as a difference between positions.

No lock is necessary for read operation.
That's because append is atomic and the data written to the index file (16 bytes)
will never be split across pages to make this bug take into effect: https://bugzilla.kernel.org/show_bug.cgi?id=55651
