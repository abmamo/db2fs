# testing
import pytest
# db2fs 
from db2fs import DatabaseExtractor
# path
from pathlib import Path
import shutil


@pytest.fixture(scope="module")
def test_download_dir_psql():
    """
        data where db tables will be downloaded
    """
    # get current file dir
    base_test_dir = Path(__file__).parent.absolute()
    # join base/current file dir with name of test data dir
    test_download_dir_psql = base_test_dir.joinpath("test_data_dir_psql")
    # create dir if it doesn't exist
    test_download_dir_psql.mkdir(parents=True, exist_ok=True)
    # yield data dir
    yield test_download_dir_psql
    # delete data dir after tests finish
    shutil.rmtree(str(test_download_dir_psql))


def test_db2fs_table2csv_psql(mock_psql_dsn, test_download_dir_psql, mock_psql_table_names):
    """
        test table 2 csv extractions
    """
    # init class
    db_ext = DatabaseExtractor(
        connection_info=mock_psql_dsn,
        download_dir_path=test_download_dir_psql
    )
    # table name
    table_name = mock_psql_table_names[0]
    # run extraction to files
    db_ext.table2csv(table_name=table_name)
    # build local file path
    # add file name to data dir
    local_file_path = test_download_dir_psql.joinpath(table_name + ".csv")
    # assert file exists in download dir
    assert local_file_path.exists()


def test_db2fs_db2csv_psql(mock_psql_dsn, test_download_dir_psql, mock_psql_table_names):
    """
        test db 2 csv extractions
    """
    # init class
    db_ext = DatabaseExtractor(
        connection_info=mock_psql_dsn,
        download_dir_path=test_download_dir_psql
    )
    # run extraction to files
    db_ext.db2csv()
    # assert all files exist
    for table_name in mock_psql_table_names:
        # build local file path
        local_file_path = test_download_dir_psql.joinpath(table_name + ".csv")
        # assert file exists
        assert local_file_path.exists()
