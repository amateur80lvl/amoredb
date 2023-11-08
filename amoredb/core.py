'''
The basic implementation with records as binary data.

:copyright: Copyright 2023 amateur80lvl
:license: LGPLv3, see LICENSE for details
'''

class BaseAmoreDB:

    def __init__(self, base_path, *args, mode=None, **kwargs):
        self._base_path = base_path
        if mode is None:
            if len(args):
                if args[0] not in 'rw':
                    raise Exception(f'Mode should be "r" or "w", not {args[0]}')
                mode = args[0]
                args = args[1:]
            else:
                mode = 'r'
        self._mode = mode
        self._db_context = None
        super().__init__(*args, **kwargs)

    async def __aenter__(self):
        index_file, data_file = await self._open_db_files(self._base_path, self._mode)
        self._db_context = AmoreDBContext(self, index_file, data_file)
        return self._db_context

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self._db_context.close()
        self._db_context = None

    # Database interface

    index_entry_format = '<Q'  # 64-bit, can be redefined in a subclass or mix-in

    # Abstract methods suck when used with mix-ins. The following should be implemented (see fileio):
    #
    #@abc.abstractmethod
    #async def _open_db_files(self, base_path, mode):
    #    '''
    #    Return instances of IndexFile and DataFile
    #    '''

    # Record transformation interface

    # Record transformation functions are called from DataFile methods.
    # Given that in asynchronous mode file I/O is executed in the context
    # of a separate thread, these functions can perform heavy tasks and should
    # be executed in the context of that thread as well.
    # This should be revised later for I/O without worker threads, such as io_uring.
    # In that case worker threads may still be necessary for compression.
    # Thus, these functions are most likely called in the context of a separate thread.

    def record_to_raw_data(self, record_data):
        return record_data

    def record_from_raw_data(self, record_data):
        return record_data


class AmoreDBContext:

    def __init__(self, db, index_file, data_file):
        self._db = db
        self._index_file = index_file
        self._data_file = data_file

    def __aiter__(self):
        return self._forward_iterator()

    def __getitem__(self, key):
        if isinstance(key, int):
            raise TypeError('Record indices must be slices. Use awaitable read() instead')
        elif isinstance(key, slice):
            if (key.step or 1) > 0:
                return self._forward_iterator(key.start, key.stop, key.step)
            else:
                return self._reverse_iterator(key.start, key.stop, key.step)
        else:
            raise TypeError(f'Record indices must be slices, not {type(key).__name__}')

    async def close(self):
        await self._index_file.close()
        await self._data_file.close()
        self.db = None  # break circular reference

    async def read(self, record_id):
        '''
        Get record by index.

        * seek to record_id * index_entry_size in the index file
        * read the position of the record and the position of the next record from the index file
        * read the record from the data file, the size of record is calculated as a difference between positions

        No lock is necessary for read operation.
        That's because append is atomic and the data written to the index file
        will never be split across pages to make this bug take into effect:
        https://bugzilla.kernel.org/show_bug.cgi?id=55651
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
        Append new record and return its index (i.e. record id):

        * lock index file exclusively
        * get position for the new record from the index file
        * append new record to the data file
        * write new size of data file to the index file
        * release file lock

        Caveat: locking may not work across threads at OS level. Always use single wriring instance per process.
        '''
        async with await self._index_file.lock():
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


    async def _forward_iterator(self, start=None, stop=None, step=None):
        start = start or 0
        step = step or 1

        n = start

        # open files again for separate seek operations
        index_file, data_file = await self._db._open_db_files(self._db._base_path, 'r')
        try:
            # get record count for negative indices
            if start < 0 or (stop or 0) < 0:
                count = await index_file.count()

            # make indices absolute
            if start < 0:
                start = count + start
            if (stop or 0) < 0:
                stop = count + stop

            # set initial data_pos
            if start == 0:
                data_pos = 0
            else:
                # skip to the start record
                data_pos = await index_file.read_nth_entry(start - 1)
                if data_pos is None:
                    return

            while True:
                next_pos = await index_file.read_current_entry()
                if next_pos is None:
                    # index file must include position for the next record,
                    # otherwise it was the last record
                    return

                # load record
                size = next_pos - data_pos
                record = await data_file.read(data_pos, size, self._db.record_from_raw_data)
                if record is None:
                    # XXX something went wrong, raise an exception?
                    return

                yield record

                # bump n and data_pos
                n += step
                if stop is not None and n >= stop:
                    return

                if step == 1:
                    data_pos = next_pos
                else:
                    data_pos = await index_file.read_nth_entry(n - 1)

        finally:
            await index_file.close()
            await data_file.close()

    def _reverse_iterator(self, start=None, stop=None, step=None):
        raise NotImplementedError('Reverse iteration is not implemented yet')
