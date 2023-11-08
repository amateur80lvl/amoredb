'''
The basic file I/O implementation.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

import fcntl
import struct
import os

import aiofiles
import aiofiles.os
aiofiles.os.open = aiofiles.os.wrap(os.open)
aiofiles.os.close = aiofiles.os.wrap(os.close)
aiofiles.os.lseek = aiofiles.os.wrap(os.lseek)
aiofiles.os.read = aiofiles.os.wrap(os.read)
aiofiles.os.write = aiofiles.os.wrap(os.write)
aiofiles.os.lockf = aiofiles.os.wrap(fcntl.lockf)

from aiofiles.os import open, close, lockf, lseek, makedirs, read, write


def _make_open_flags(mode):
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
    return await open(filename, _make_open_flags(mode), mode=0o666)

def _read_and_convert_func(fd, size, conversion_func):
    raw_data = os.read(fd, size)
    return conversion_func(raw_data)

def _convert_and_write_func(fd, data, conversion_func):
    raw_data = conversion_func(data)
    return os.write(fd, raw_data)

_read_and_convert = aiofiles.os.wrap(_read_and_convert_func)
_convert_and_write = aiofiles.os.wrap(_convert_and_write_func)


class AmoreAiofiles:
    '''
    File I/O mix-in for AmoreDB
    '''

    async def _open_db_files(self, base_path, mode):
        if mode not in ['r', 'w']:
            raise Exception(f'Wrong mode: {mode}')

        if mode == 'w':
            dest_dir = os.path.dirname(base_path)
            if dest_dir:
                await makedirs(dest_dir, exist_ok=True)

        index_file = AmoreIndexFile(
            await _open_file(base_path + '.index', mode),
            self.index_entry_format
        )
        try:
            data_file = AmoreDataFile(
                await _open_file(base_path + '.data', mode)
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

    def __init__(self, fd, entry_format):
        self.fd = fd
        self.entry_format = entry_format
        self.entry_size = struct.calcsize(entry_format)

    async def close(self):
        if self.fd is not None:
            await close(self.fd)
            self.fd = None

    async def lock(self):
        await lockf(self.fd, fcntl.LOCK_EX)
        return AmoreLockContext(self.unlock())

    async def unlock(self):
        await lockf(self.fd, fcntl.LOCK_UN)

    async def seek(self, entry_index):
        if entry_index < 0:
            # seek from the end, this may raise OSError if resulting position is negative
            await lseek(self.fd, entry_index * self.entry_size, os.SEEK_END)
        else:
            # seek from the beginning
            await lseek(self.fd, entry_index * self.entry_size, os.SEEK_SET)

    async def read_current_entry(self):
        '''
        Read entry at current position.
        '''
        entry = await read(self.fd, self.entry_size)
        if len(entry) == self.entry_size:
            return struct.unpack(self.entry_format, entry)[0]
        else:
            return None

    async def read_nth_entry(self, n):
        '''
        Read Nth entry.
        '''
        await self.seek(n)
        return await self.read_current_entry()

    async def read_last_entry(self):
        '''
        Read last entry.
        '''
        try:
            await self.seek(-1)
        except OSError:
            # index file is empty
            return None
        return await self.read_current_entry()

    async def append(self, entry):
        '''
        Append new entry and return its index in the file.
        '''
        pos = await lseek(self.fd, 0, os.SEEK_END)
        await write(self.fd, struct.pack(self.entry_format, entry))
        return pos // self.entry_size

    async def count(self):
        return (await lseek(self.fd, 0, os.SEEK_END)) // self.entry_size


class AmoreDataFile:

    def __init__(self, fd):
        self.fd = fd

    async def close(self):
        if self.fd is not None:
            await close(self.fd)
            self.fd = None

    async def read(self, pos, size, record_from_raw_data):
        '''
        Read `size` bytes from data file starting from `pos`.
        '''
        await lseek(self.fd, pos, os.SEEK_SET)
        return await _read_and_convert(self.fd, size, record_from_raw_data)

    async def write(self, pos, data, record_to_raw_data):
        await lseek(self.fd, pos, os.SEEK_SET)
        return pos + await _convert_and_write(self.fd, data, record_to_raw_data)
