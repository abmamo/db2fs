# testing
import pytest
# mock db
from fastdb import MockPostgres, MockMySQL
# db middleware (Postgres + MySQL)
from fs2db.connectors.rdb import RDBMiddleware
# db population
from populate import Populate
# file del
import shutil
# path
from pathlib import Path
import os
# mock docker dbs
mock_mysql = MockMySQL()
mock_postgres = MockPostgres()
# start docker dbs
mock_mysql.start()
mock_postgres.start()


### Postgres ###
@pytest.fixture(scope="package")
def test_populate_obj_psql():
    """
        init populate class to populate
        a db
    """
    # init populate obj
    psql_populate_obj = Populate()
    # return populate object
    return psql_populate_obj


@pytest.fixture(scope="package")
def test_dir_psql():
    """
        directory to generate data in for psql
    """
    # get current file dir
    base_test_dir = Path(__file__).parent.absolute()
    # join base/current file dir with name of test data dir
    test_dir_psql = base_test_dir.joinpath("test_data_dir_psql")
    # create dir if it doesn't exist
    test_dir_psql.mkdir(parents=True, exist_ok=True)
    # yield data dir
    yield test_dir_psql


@pytest.fixture(scope="package", autouse=True)
def mock_psql_dsn():
    """
        postgres connection into
    """
    # add engine
    mock_psql_dsn = mock_postgres.info()
    mock_psql_dsn["engine"] = "pg"
    # yield store
    yield mock_psql_dsn
    # stop after tests finish (basically deleting docker containers)
    mock_postgres.stop()


@pytest.fixture(scope="package", autouse=True)
def mock_psql_table_names(mock_psql_dsn, test_dir_psql, test_populate_obj_psql):
    # num files to generate & their sizes
    num_tables, max_size = 10, 10
    # populate db from files
    psql_table_names = test_populate_obj_psql.populate(
        connection_info=mock_psql_dsn,
        dir_path=str(test_dir_psql),
        num_tables=num_tables,
        max_size=max_size
    )
    # get number of files generated
    num_files = len(psql_table_names)
    # assert number of files generated = num_tables
    assert num_files == num_tables 
    # assert dir_path specified by us is used
    assert test_populate_obj_psql.dir_path == str(test_dir_psql)
    # assert tables exist
    rdb_middleware = RDBMiddleware(connection_info=mock_psql_dsn)
    assert len(rdb_middleware.get_tables()) == num_files
    assert all(rdb_middleware.table_exists(table_name) for table_name in psql_table_names)
    # yield table names
    yield psql_table_names
    # remove dir if it exists
    if test_dir_psql.exists():
        # delete
        # shutil.rmtree(test_dir_psql)
        pass
 
### MySQL ###
@pytest.fixture(scope="package")
def test_populate_obj_mysql():
    # init populate
    populate_obj = Populate()
    # return populate object
    return populate_obj

@pytest.fixture(scope="package")
def test_dir_mysql():
    # get current file dir
    base_test_dir = Path(__file__).parent.absolute()
    # join base/current file dir with name of test data dir
    test_dir_mysql = base_test_dir.joinpath("test_data_dir_mysql")
    # create dir if it doesn't exist
    test_dir_mysql.mkdir(parents=True, exist_ok=True)
    # yield data dir
    yield test_dir_mysql

@pytest.fixture(scope="package", autouse=True)
def mock_mysql_dsn():
    # add engine
    mock_mysql_dsn = mock_mysql.info()
    mock_mysql_dsn["engine"] = "mysql"
    # yield store
    yield mock_mysql_dsn
    # stop after tests finish (basically deleting docker containers)
    mock_mysql.stop()

@pytest.fixture(scope="package", autouse=True)
def mock_mysql_table_names(mock_mysql_dsn, test_dir_mysql, test_populate_obj_mysql):
    # num files to generate & their sizes
    num_tables, max_size = 10, 10
    # populate db from files
    mysql_table_names = test_populate_obj_mysql.populate(
        connection_info=mock_mysql_dsn,
        dir_path=str(test_dir_mysql),
        num_tables=num_tables,
        max_size=max_size
    )
    print(mysql_table_names)
    # get number of files generated
    num_files = len(mysql_table_names)
    # assert number of files generated = num_tables
    assert num_files == num_tables 
    # assert dir_path specified by us is used
    assert test_populate_obj_mysql.dir_path == str(test_dir_mysql)
    # assert tables exist
    rdb_middleware = RDBMiddleware(connection_info=mock_mysql_dsn)
    assert len(rdb_middleware.get_tables()) == num_files
    assert all(rdb_middleware.table_exists(table_name) for table_name in mysql_table_names)
    # yield table names
    yield mysql_table_names
    # remove dir if it exists
    if test_dir_mysql.exists():
        # delete
        shutil.rmtree(test_dir_mysql)