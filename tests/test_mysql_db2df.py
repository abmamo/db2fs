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
    # shutil.rmtree(str(test_download_dir_mysql))


def test_db2fs_table2csv_mysql(mock_mysql_dsn, test_download_dir_mysql, mock_mysql_table_names):
    # init class
    db_ext = DatabaseExtractor(
        connection_info=mock_mysql_dsn,
        download_dir_path=test_download_dir_mysql
    )
    # table name
    table_name = mock_mysql_table_names[0]
    # run extraction to files
    db_ext.table2csv(table_name=table_name)
    # add file name to data dir
    local_file_path = test_download_dir_mysql.joinpath(table_name + ".csv")
    # assert file exists in download dir
    assert local_file_path.exists()

def test_db2fs_table2json_mysql(mock_mysql_dsn, test_download_dir_mysql, mock_mysql_table_names):
    # init class
    db_ext = DatabaseExtractor(
        connection_info=mock_mysql_dsn,
        download_dir_path=test_download_dir_mysql
    )
    # table name
    table_name = mock_mysql_table_names[0]
    # run extraction to files
    db_ext.table2other(table_name=table_name)
    # build local file path
    table_name = table_name + ".json"
    # add file name to data dir
    local_file_path = test_download_dir_mysql.joinpath(table_name)
    # assert file exists in download dir
    assert local_file_path.exists()


def test_db2fs_table2parquet_mysql(mock_mysql_dsn, test_download_dir_mysql, mock_mysql_table_names):
    # init class
    db_ext = DatabaseExtractor(
        connection_info=mock_mysql_dsn,
        download_dir_path=test_download_dir_mysql
    )
    # table name
    table_name = mock_mysql_table_names[0]
    # run extraction to files
    db_ext.table2other(
        table_name=table_name,
        file_type=".parquet"
    )
    # build local file path
    table_name = table_name + ".parquet"
    # add file name to data dir
    local_file_path = test_download_dir_mysql.joinpath(table_name)
    # assert file exists in download dir
    assert local_file_path.exists()


def test_db2fs_db2csv_mysql(mock_mysql_dsn, test_download_dir_mysql, mock_mysql_table_names):
    # init class
    db_ext = DatabaseExtractor(
        connection_info=mock_mysql_dsn,
        download_dir_path=test_download_dir_mysql
    )
    # run extraction to files
    db_ext.db2csv()
    # assert all files exist
    for table_name in mock_mysql_table_names:
        # table name
        table_name = table_name + ".csv"
        # build local file path
        local_file_path = test_download_dir_mysql.joinpath(table_name)
        # assert file exists in download dir
        assert local_file_path.exists()

def test_db2fs_db2json_mysql(mock_mysql_dsn, test_download_dir_mysql, mock_mysql_table_names):
    # init class
    db_ext = DatabaseExtractor(
        connection_info=mock_mysql_dsn,
        download_dir_path=test_download_dir_mysql
    )
    # run extraction to files
    db_ext.db2other()
    # assert all files exist
    for table_name in mock_mysql_table_names:
        # build local file name
        table_name = table_name + ".json"
        # build local file path
        local_file_path = test_download_dir_mysql.joinpath(table_name)
        # assert file exists in download dir
        assert local_file_path.exists()

def test_db2fs_db2parquet_mysql(mock_mysql_dsn, test_download_dir_mysql, mock_mysql_table_names):
    # init class
    db_ext = DatabaseExtractor(
        connection_info=mock_mysql_dsn,
        download_dir_path=test_download_dir_mysql
    )
    # run extraction to files
    db_ext.db2other(file_type=".parquet")
    # assert all files exist
    for table_name in mock_mysql_table_names:
        # build local file name
        table_name = table_name + ".parquet"
        # build local file path
        local_file_path = test_download_dir_mysql.joinpath(table_name)
        # assert file exists in download dir
        assert local_file_path.exists()
"""
"""