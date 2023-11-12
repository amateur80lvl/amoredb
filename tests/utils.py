import os
import shutil
import tempfile

async def _run_db_test(db_class, records_in, **db_params):
    directory = tempfile.mkdtemp()
    try:
        db_basename = os.path.join(directory, 'amoredb-test')
        records_out = []
        async with db_class(db_basename, 'w', **db_params) as db:
            for record in records_in:
                await db.append(record)
            async for record in db:
                records_out.append(record)
            print(db_class.__name__, 'data size:', await db.data_size())
        assert records_in == records_out
    finally:
        shutil.rmtree(directory)
