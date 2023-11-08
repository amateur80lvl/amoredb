# AmoreDB

Simple append-only database for Python with rich record formats and compression.

## For impatients

### Example 1

```python
from amoredb import AmoreDB

async with AmoreDB('test', 'w') as db:
    await db.append(b'foo')
    await db.append(b'bar')
    await db.append(b'baz')
    async for record in db:
        print(record)
```

Result:

```
b'foo'
b'bar'
b'baz'
```

### Example 2

```python
from amoredb.json import JsonAmoreDB

async with JsonAmoreDB('test.json', 'w') as db:
    await db.append({'foo': 'bar'})
    await db.append({'bar': 'foo'})
    async for record in db:
        print(record)
```

Result:

```
{'foo': 'bar'}
{'bar': 'foo'}
```

## Record formats

The basic format for database records is bytes object. Subclasses may support other formats,
as demonstrated above in the Example 2. AmoreDB provides support for the following formats:

* JSON: `JsonMixin`, `JsonAmoreDB` from `amoredb.json`
* strings: `StrMixin`, `StrAmoreDB` from `amoredb.str`
* structures: `StructMixin`, `StructAmoreDB` from `amoredb.struct`
* BSON: `BsonMixin`, `BsonAmoreDB` from `amoredb.bson`, requires [simple_bson](https://pypi.org/project/simple-bson/) package

Records are converted to the binary data by mixins and AmoreDB provides
predefined classes, such, for example, as

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

There are no predefined classes for compression, it's up to end users to define ones for their needs.
For example,

```python
from amoredb import AmoreDB
from amoredb.json import JsonMixin
from amoredb.gzip import GzipMixin

class MyDB(JsonMixin, GzipMixin, AmoreDB):
    pass

async with MyDB('test.json.gz', 'w') as db:
    await db.append({'foo': 'bar'})
    await db.append({'bar': 'foo'})
    async for record in db:
        print(record)
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


## Database structure

The database consists of data file and index file. Optional metadata file in JSON format may contain
the structure of database class.

Index file contains positions of records except the first one which is always zero.
The first element in index file is the offset of the next record.
Thus, the number of items in the index file equals to the number of records.

Record id is implicit, it is the index of the record.
Thus, to get a record by id, read its offset from the index file and then read the record from data file.
