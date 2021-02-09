# testing
import pytest
# db2fs 
from db2fs import DatabaseExtractor
# path
from pathlib import Path
import shutil


@pytest.fixture(scope="module")
def test_download_dir_mysql():
    """
        data where db tables will be downloaded
    """
    # get current file dir
    base_test_dir = Path(__file__).parent.absolute()
    # join base/current file dir with name of test data dir
    test_download_dir_mysql = base_test_dir.joinpath("test_data_dir_mysql")
    # create dir if it doesn't exist
    test_download_dir_mysql.mkdir(parents=True, exist_ok=True)
    # yield data dir
    yield test_download_dir_mysql
    # delete data dir after tests finish
    shutil.rmtree(str(test_download_dir_mysql))


def test_db2fs_table2csv_mysql(mock_mysql_dsn, test_download_dir_mysql, mock_mysql_table_names):
    """
        test table 2 csv extractions
    """
    # init class
    db_ext = DatabaseExtractor(
        connection_info=mock_mysql_dsn,
        download_dir_path=test_download_dir_mysql
    )
    # table name
    table_name = mock_mysql_table_names[0]
    # run extraction to files
    db_ext.table2csv(table_name=table_name)
    # build local file path
    if "csv" in table_name:
        table_name = table_name
    else:
        table_name = table_name + ".csv"
    # add file name to data dir
    local_file_path = test_download_dir_mysql.joinpath(table_name)
    # assert file exists in download dir
    assert local_file_path.exists()


def test_db2fs_db2csv_mysql(mock_mysql_dsn, test_download_dir_mysql, mock_mysql_table_names):
    """
        test db 2 csv extractions
    """
    # init class
    db_ext = DatabaseExtractor(
        connection_info=mock_mysql_dsn,
        download_dir_path=test_download_dir_mysql
    )
    # run extraction to files
    db_ext.db2csv()
    # assert all files exist
    for table_name in mock_mysql_table_names:
        if "csv" in table_name:
            table_name = table_name
        else:
            table_name = table_name + ".csv"
        # build local file path
        local_file_path = test_download_dir_mysql.joinpath(table_name)
        # assert file exists in download dir
        assert local_file_path.exists()
