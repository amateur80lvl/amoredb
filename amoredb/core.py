'''
The basic implementation with records as binary data.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

import abc
import asyncio
import concurrent.futures
import fcntl
from functools import partial
import os
import struct


def _in_loop():
    '''
    Return True if asyncio loop is running.
    '''
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _run_coroutine(coro):
    '''
    Execute coroutine synchronously.
    '''
    try:
        while True:
            coro.send(None)
    except StopIteration as e:
        return e.value


_fileio_executor = None

def _get_default_fileio_executor():
    # XXX mind thread safety
    # XXX how many workers?
    global _fileio_executor
    if _fileio_executor is None:
        _fileio_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    return _fileio_executor


class BaseAmoreDB(abc.ABC):
    '''
    This class is a context manager that provides both synchronous and asynchronous
    interfaces to the database.
    '''

    def __init__(self, base_path, *args, mode='r', **kwargs):
        self._base_path = base_path
        self._mode = mode
        self._db_context = None
        super().__init__(*args, **kwargs)

    async def _do_enter(self):
        '''
        Helper method for __enter__ and __aenter__
        '''
        index_file, data_file = await self._open_db_files(self._base_path, self._modes)
        return AmoreDBContext(self, index_file, data_file)

    # Synchronous interface

    def __enter__(self):
        _run_coroutine(self._do_enter())
        return self._db_context

    def __exit__(self, exc_type, exc_value, exc_tb):
        _run_coroutine(self._db_context.close())
        self._db_context = None

    # Asynchronous interface

    async def __aenter__(self):
        await self._do_enter()
        return self._db_context

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self._db_context.close()
        self._db_context = None

    # Database interface

    index_entry_format = '<Q'  # 64-bit, can be redefined in a subclass or mix-in

    @abc.abstractmethod
    async def _open_db_files(self, base_path, mode):
        '''
        Return instances of IndexFile and DataFile
        '''

    # Record transformation interface

    # Record transformation functions are called from DataFile methods.
    # Given that in asynchronous mode file I/O is executed in the context
    # of a separate thread, these functions can perform heavy tasks and should
    # be executed in the context of that thread as well.
    # This should be revised later, using worker thread for compression, maybe.

    def record_to_raw_data(self, record_data):
        return record_data

    def record_from_raw_data(self, record_data):
        return record_data


class BaseAmoreDBContext:
    '''
    The basic interface to the database is asynchronous.
    AmoreDBSyncContext wraps methods with _run_coroutine.
    '''

    def __init__(self, db, index_file, data_file):
        self._db = db
        self._index_file = index_file
        self._data_file = data_file

    async def close(self):
        await self._index_file.close()
        await self._data_file.close()
        self.db = None  # break circular reference

    async def read(self, record_id):
        '''
        Get record by index.
        '''
        if record_id == 0:
            data_pos = 0
            next_pos = await self._index_file.read_nth_entry(0)
        else:
            data_pos = await self._index_file.read_nth_entry(record_id - 1)
            next_pos = await self._index_file.read_current_entry()
        if next_pos is None:
            return None
        else:
            size = next_pos - data_pos
            return await self.data_file.read(data_pos, size, self._db.record_from_raw_data)

    async def append(self, record):
        '''
        Append new record and return its index (i.e. record id)
        '''
        async with self._index_file.lock():
            # If previous append was unsuccessfull, the data file may contain garbage at the end.
            # Get the position for writing from the index file instead of simply appending to the data file.
            pos = await self._index_file.read_last_entry()
            if pos is None:
                pos = 0
            next_pos = await self._data_file.write(pos, record, self._db.record_to_raw_data)
            return await self._index_file.append(next_pos)

    async def count(self):
        '''
        Return the number of records.
        '''
        with self._db._lock:
            return self._index_file.count()


class AmoreDBSyncContext(BaseAmoreDBContext):

    def __iter__(self):
        return AmoreForwardIterator(self)

    def __reversed__(self):
        return AmoreReverseIterator(self)

    def __getitem__(self, key):
        if isinstance(key, int):
            if key < 0:
                # for negative key need to get the number of records to calculate record index
                count = _run_coroutine(self.count())
                record_id = count + key
                if record_id < 0 or record_id >= count:
                    raise IndexError(f'Record index {key} out of range 0..{count}')
                return self.read(record_id)
            else:
                # faster implementation for positive key
                record = self.read(key)
                if record is None:
                    raise IndexError(f'Record index {key} out of range')
                else:
                    return record
        elif isinstance(key, slice):
            if (key.step or 1) > 0:
                return AmoreForwardIterator(self, key.start, key.stop, key.step)
            else:
                return AmoreReverseIterator(self, key.start, key.stop, key.step)
        else:
            raise TypeError(f'Record indices must be integers or slices, not {type(key).__name__}')

    def read(self, record_id):
        '''
        Get record by index.
        '''
        return _run_coroutine(super().read(record_id))

    def append(self, record):
        '''
        Append new record and return its index (i.e. record id)
        '''
        return _run_coroutine(super().append(record))

    def count(self):
        '''
        Return the number of records.
        '''
        return _run_coroutine(super().count())


class AmoreDBAsyncContext(BaseAmoreDBContext):

    def __aiter__(self):
        return AmoreForwardIterator(self)

    def __getitem__(self, key):
        if isinstance(key, int):
            raise TypeError('Record indices must be slices. Use awaitable read() instead')
        elif isinstance(key, slice):
            if (key.step or 1) > 0:
                return AmoreForwardIterator(self, key.start, key.stop, key.step)
            else:
                return AmoreReverseIterator(self, key.start, key.stop, key.step)
        else:
            raise TypeError(f'Record indices must be slices, not {type(key).__name__}')


class AmoreForwardIterator:

    def __init__(self, context, start=None, stop=None, step=None):
        self.db = context._db
        self.start = start or 0
        self.stop = stop
        self.step = step or 1

        self.index_file = None
        self.data_file = None

        self.initialized = False
        self.no_iteration = self.stop is not None and self.start >= self.stop  # XXX need to get count to set stop

        self.n = self.start

    def __next__(self):
        if self.no_iteration:
            raise StopIteration()

        record = _run_coroutine(self._get_next_record())
        if record is None:
            raise StopIteration()
        else:
            return record

    async def __anext__(self):
        if self.no_iteration:
            raise StopAsyncIteration()

        record = await self._get_next_record()
        if record is None:
            raise StopAsyncIteration()
        else:
            return record

    async def _get_next_record():
        try:
            if not self.initialized:
                if not self._initialize():
                    return None

            next_pos = await self.index_file.read_current_entry()
            if next_pos is None:
                # index file must include position for the next record,
                # otherwise it was the last record
                self._finalize()
                return None

            # load record
            size = next_pos - data_pos
            record = await self.db._load_record(self.data_file, self.data_pos, size)

            # bump n and data_pos
            self.n += self.step
            if self.stop is not None and self.n >= self.stop:
                # we've done, iteration will be stopped on the next call
                self._finalize()
            else:
                if self.step == 1:
                    self.data_pos = self.next_pos
                else:
                    self.data_pos = await self.index_file.read_nth_entry(self.n - 1)

            return record

        except:
            self._finalize()
            raise

    async def _initialize(self):
        try:
            # open files again for separate seek operations
            self.index_file, self.data_file = await self.db._open_db_files(self.db._base_path, 'r')

            # get record count for negative indices
            if self.start < 0 or self.stop < 0:
                count = await self.index_file.count()

            # make indices absolute
            if self.start < 0:
                self.start = count + self.start
            if self.stop < 0:
                self.stop = count + self.stop

            # set initial data_pos
            if self.start == 0:
                self.data_pos = 0
            else:
                # skip to the start record
                self.data_pos = await self.index_file.read_nth_entry(self.start - 1)
                if self.data_pos is None:
                    self._finalize()
                    return False

            self.initialized = True
            return True

        except:
            self._finalize()
            raise

    async def _finalize(self)
        # close files
        if self.index_file is not None:
            await self.index_file.close()
            self.index_file = None

        if self.data_file is not None:
            await self.data_file.close()
            self.data_file = None

        self.db = None  # break circular reference
        self.no_iteration = True


class AmoreReverseIterator:

    def __init__(self, context, start=None, stop=None, step=None):
        self.db = context._db
        self.start = start
        self.stop = stop
        self.step = step
        raise NotImplementedError('Reverse iteration is not implemented yet')


class AmoreIO:
    '''
    File I/O mix-in/base class for BaseAmoreDB
    '''

    def __init__(self, *args, fileio_executor=None, **kwargs):
        # set fileio_executor
        if _in_loop():
            self._loop = asyncio.get_running_loop()
            # if an executor is not supplied, use default one
            if fileio_executor is None:
                self._fileio_executor = _get_default_fileio_executor()
            else:
                # use supplied executor
                self._fileio_executor = fileio_executor
        else:
            # not running any loop - no executor needed
            self._fileio_executor = None

        super().__init__(*args, **kwargs)

    async def _fileio_run(func, *args, **kwargs):
        '''
        Run func in an executor if set or in the context of current thread.
        '''
        if self._fileio_executor is None:
            return func(*args, **kwargs)
        else:
            return self._loop.run_in_executor(self._fileio_executor, partial(func, *args, **kwargs))

    def _make_open_flags(self, mode):
        '''
        `mode`: 'r' or 'w'.
        Return flags for `os.open`.
        '''
        if mode == 'r':
            flags = os.O_RDONLY
        else:
            flags = os.O_CREAT | os.O_RDWR
        return flags

    async def _open_file(filename, mode):
        return await self.fileio_run(
            os.open, filename, self._make_open_flags(mode), mode=0o666
        )

    async def _open_index_file(self, base_path, mode):
        return await self._open_file(base_path + '.index')

    async def _open_data_file(self, base_path, mode):
        return await self._open_file(base_path + '.data')

    async def _open_db_files(base_path, mode):
        if mode not in ['r', 'w']:
            raise Exception(f'Wrong mode: {mode}')

        if mode == 'w':
            dest_dir = os.path.dirname(base_path)
            if dest_dir:
                await self.fileio_run(
                    os.makedirs, dest_dir, exist_ok=True
                )

        index_file = AmoreIndexFile(
            await self.open_index_file(base_path),
            self.index_entry_format
        )
        try:
            data_file = AmoreDataFile(
                await self.open_data_file(base_path)
            )
        except:
            await index_file.close()
            raise
        return index_file, data_file


class AmoreLockContext:
    '''
    Helper class for `AmoreIndexFile.lock` method.
    '''
    def __init__(self, unlock_coro):
        self.unlock_coro = unlock_coro

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self.unlock_coro


class AmoreIndexFile:

    def __init__(self, fd, index_entry_format):
        self.fd = fd
        self.entry_size = struct.calcsize(self.index_entry_format)

    async def close(self):
        if self.fd is not None:
            await os.close(self.fd)
            self.fd = None

    async def lock(self):
        fcntl.lockf(self.fd, fcntl.LOCK_EX)
        return AmoreLockContext(self.unlock)

    def unlock(self):
        fcntl.lockf(self.fd, fcntl.LOCK_UN)

    def seek(self, entry_index):
        if entry_index < 0:
            # seek from the end, this may raise OSError if resulting position is negative
            os.lseek(self.fd, entry_index * self.entry_size, os.SEEK_END)
        else:
            # seek from the beginning
            os.lseek(self.fd, entry_index * self.entry_size, os.SEEK_SET)

    def read_entry(self):
        '''
        Read entry at current position.
        '''
        entry = os.read(self.fd, self.entry_size)
        if len(entry) == self.entry_size:
            return struct.unpack(self.entry_format, entry)[0]
        else:
            return None

    def read_nth_entry(self, n):
        '''
        Read Nth entry.
        '''
        self.seek(n)
        return self.read_entry()

    def read_last_entry(self):
        '''
        Read last entry.
        '''
        try:
            self.seek(-1)
        except OSError:
            # index file is empty
            return None
        return self.read_entry()

    def append(self, entry):
        '''
        Append new entry and return its index in the file.
        '''
        pos = os.lseek(self.fd, 0, os.SEEK_END)
        os.write(self.fd, struct.pack(self.entry_format, entry))
        return pos // self.entry_size

    def rollback(self, n):
        '''
        Rollback last n entries.
        Return position for writing to the data file.
        '''
        try:
            self.seek(-n-1)
        except OSError:
            os.ftruncate(self.fd, 0)
            return 0
        entry = self.read_entry()
        os.ftruncate(self.fd, os.lseek(self.fd, 0, os.SEEK_CUR))
        return entry

    def count(self):
        return os.lseek(self.fd, 0, os.SEEK_END) // self.entry_size


class AmoreDataFile:


    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def __del__(self):
        self.close()

    def read(self, pos, size):
        '''
        Read `size` bytes from data file starting from `pos`.
        '''
        os.lseek(self.fd, pos, os.SEEK_SET)
        return os.read(self.fd, size)

    def write(self, pos, data):
        os.lseek(self.fd, pos, os.SEEK_SET)
        bytes_written = os.write(self.fd, data)
        return pos + bytes_written

    def truncate(self, pos):
        os.ftruncate(self.fd, pos)
