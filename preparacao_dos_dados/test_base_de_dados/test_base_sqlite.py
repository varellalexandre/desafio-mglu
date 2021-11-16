def do_sqlite_file_exists():
    from sqlalchemy import create_engine
    import os

    files = os.listdir('.')
    sqlite_filepath = os.environ.get('SQL_FILENAME')
    if sqlite_filepath is None:
        return False

    for file in files:
        if file == sqlite_filepath:
            return True
    else:
        return False

def test_sqlite_file_exists():
    assert do_sqlite_file_exists()
