from preparacao_dos_dados import file_is_csv

def test_is_file_csv():
    files = ['test123.csv', 'test23123.csv', 'test13322.csv']
    for file in files:
        assert file_is_csv(file) is True

def test_is_file_not_csv():
    files = [31222,'is_not_csv', 'hire_me.parquet', False]
    for file in files:
        assert file_is_csv(file) is False
