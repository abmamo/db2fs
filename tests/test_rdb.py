# os
import os
# testing
import pytest
# dbapis
import psycopg2
import pymysql
# classes being tested
from fs2db.connectors.rdb import without, RDBConnector, RDBMiddleware


def test_without_valid_key():
    # test dict
    test_dict = {"a": 1, "b": 2}
    # key to remove
    rm_key = "a"
    # remove key
    new_dict = without(test_dict, rm_key)
    # assert key is not in dict
    assert rm_key not in list(new_dict.keys())


def test_without_invalid_key():
    # test dict
    test_dict = {"a": 1, "b": 2}
    # key to remove
    rm_key = "c"
    # check key error raised
    with pytest.raises(KeyError):
        # remove key
        without(test_dict, rm_key)


def test_rdbconnector_init():
    # init connector
    rdb_connector = RDBConnector()
    # assert init
    assert rdb_connector.rdb_initialized
    assert not rdb_connector.rdb_connected
    assert rdb_connector.connection_info is None
    assert rdb_connector.mappings is None


def test_rdbconnector_psql_connect_valid(mock_psql_dsn):
    # init connector
    rdb_connector = RDBConnector()
    # assert init
    assert rdb_connector.rdb_initialized
    # connect connector
    rdb_connector.connect(mock_psql_dsn)
    # assert successfully connected
    assert rdb_connector.rdb_initialized
    assert rdb_connector.rdb_connected
    assert rdb_connector.mappings is not None


def test_rdbconnector_mysql_connect_valid(mock_mysql_dsn):
    # init connector
    rdb_connector = RDBConnector()
    # assert init
    assert rdb_connector.rdb_initialized
    assert not rdb_connector.rdb_connected
    # connect connector
    rdb_connector.connect(mock_mysql_dsn)
    # assert successfully connected
    assert rdb_connector.rdb_initialized
    assert rdb_connector.mappings is not None


def test_rdbconnector_missing():
    # create invalid conn info
    invalid_conn_info = {
        'host': '127.0.0.1',
        'user': 'postgres',
        'database': 'test',
        'engine': 'pg'
    }
    # init connector
    rdb_connector = RDBConnector()
    # assert init
    assert rdb_connector.rdb_initialized
    # connect connector
    connected = rdb_connector.connect(invalid_conn_info)
    # check successfully connected
    assert connected is None
    assert rdb_connector.rdb_initialized
    assert not rdb_connector.rdb_connected


def test_rdbconnector_psql_connect_invalid():
    # create invalid conn info
    invalid_conn_info = {
        'port': 00000,
        'host': '127.0.0.1',
        'user': 'postgres',
        'database': 'test',
        'engine': 'pg'
    }
    # init connector
    rdb_connector = RDBConnector()
    # assert init
    assert rdb_connector.rdb_initialized
    # connect connector
    connected = rdb_connector.connect(invalid_conn_info)
    # check successfully connected
    assert connected is None
    assert rdb_connector.rdb_initialized
    assert not rdb_connector.rdb_connected


def test_rdbconnector_mysql_connect_invalid():
    # create invalid conn info (invalid info)
    invalid_conn_info = {
        'port': 00000,
        'host': '127.0.0.1',
        'user': 'postgres',
        'database': 'test',
        'engine': 'mysql'
    }
    # init connector
    rdb_connector = RDBConnector()
    # assert init
    assert rdb_connector.rdb_initialized
    # connect connector
    connected = rdb_connector.connect(invalid_conn_info)
    # check successfully connected
    assert connected is None
    assert rdb_connector.rdb_initialized
    assert not rdb_connector.rdb_connected
    # create invalid conn info (missing keys)
    invalid_conn_info = {
        'host': '127.0.0.1',
        'user': 'postgres',
        'database': 'test',
        'engine': 'mysql'
    }
    # connect connector
    connected = rdb_connector.connect(invalid_conn_info)
    # check successfully connected
    assert connected is None
    assert rdb_connector.rdb_initialized
    assert not rdb_connector.rdb_connected


def test_rdbconnector_psql_disconnect(mock_psql_dsn):
    # init connector
    rdb_connector = RDBConnector()
    # assert init
    assert rdb_connector.rdb_initialized
    # connect connector
    rdb_connector.connect(mock_psql_dsn)
    # check successfully connected
    assert rdb_connector.rdb_connected
    assert rdb_connector.mappings is not None
    # disconnect connector
    rdb_connector.disconnect()
    # check if successfully disconnected
    assert rdb_connector.connection_info is None
    assert rdb_connector.rdb_initialized
    assert not rdb_connector.rdb_connected

def test_rdbconnector_mysql_disconnect(mock_mysql_dsn):
    # init connector
    rdb_connector = RDBConnector()
    # assert init
    assert rdb_connector.rdb_initialized
    # connect connector
    rdb_connector.connect(mock_mysql_dsn)
    # check successfully connected
    assert rdb_connector.rdb_connected
    assert rdb_connector.mappings is not None
    # disconnect connector
    rdb_connector.disconnect()
    # check if successfully disconnected
    assert rdb_connector.connection_info is None
    assert rdb_connector.rdb_initialized
    assert not rdb_connector.rdb_connected


def test_rdbmiddleware_init_psql(mock_psql_dsn):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_psql_dsn)
    # assert init
    assert rdb_middleware.rdb_connected
    assert rdb_middleware.rdb_initialized
    assert rdb_middleware.connection_info is not None
    assert rdb_middleware.mappings is not None
    

def test_rdbmiddleware_init_mysql(mock_mysql_dsn):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_mysql_dsn)
    # assert init
    assert rdb_middleware.rdb_connected
    assert rdb_middleware.rdb_initialized
    assert rdb_middleware.connection_info is not None
    assert rdb_middleware.mappings is not None
    

def test_rdbmiddleware_init_psql_invalid():
    # create invalid conn info
    invalid_conn_info = {
        'port': 00000,
        'host': '127.0.0.1',
        'user': 'postgres',
        'database': 'test',
        'engine': 'pg'
    }
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=invalid_conn_info)
    # assert init
    assert not rdb_middleware.rdb_connected
    assert rdb_middleware.rdb_initialized
    assert rdb_middleware.connection_info is not None
    assert rdb_middleware.mappings is None
    

def test_rdbmiddleware_init_mysql_invalid():
    # create invalid conn info
    invalid_conn_info = {
        'port': 00000,
        'host': '127.0.0.1',
        'user': 'postgres',
        'database': 'test',
        'engine': 'mysql'
    }
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=invalid_conn_info)
    # assert init
    assert not rdb_middleware.rdb_connected
    assert rdb_middleware.rdb_initialized
    assert rdb_middleware.connection_info is not None
    assert rdb_middleware.mappings is None
    

def test_rdbmiddleware_execute_valid_query_psql(mock_psql_dsn):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_psql_dsn)
    # sql statement
    info_sql_stmt = "SELECT table_name FROM information_schema.tables;"
    # execute query w/ middleware
    conn, cursor = rdb_middleware.execute_query(info_sql_stmt)
    # assert statement execution
    assert conn, cursor


def test_rdbmiddleware_execute_valid_query_mysql(mock_mysql_dsn):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_mysql_dsn)
    # sql statement
    info_sql_stmt = "SELECT table_name FROM information_schema.tables;"
    # execute query w/ middleware
    conn, cursor = rdb_middleware.execute_query(info_sql_stmt)
    # assert statement executed
    assert conn, cursor


def test_rdbmiddleware_execute_invalid_query_psql(mock_psql_dsn):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_psql_dsn)
    # sql statement
    info_sql_stmt = "SELECT table_name FRO information_schema.tables;"
    # assert syntax error raised
    with pytest.raises(psycopg2.errors.SyntaxError):
        # execute query w/ middleware
        conn, cursor = rdb_middleware.execute_query(info_sql_stmt)


def test_rdbmiddleware_execute_invalid_query_mysql(mock_mysql_dsn):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_mysql_dsn)
    # sql statement
    info_sql_stmt = "SELECT table_name FRO information_schema.tables;"
    # assert syntax error raised
    with pytest.raises(pymysql.err.ProgrammingError):
        # execute query w/ middleware
        conn, cursor = rdb_middleware.execute_query(info_sql_stmt)


def test_rdbmiddleware_create_table_psql(mock_psql_dsn, test_table_name, test_database_metadata):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_psql_dsn)
    # create table
    rdb_middleware.create_table(test_table_name, test_database_metadata)
    # existence
    exists = rdb_middleware.table_exists(table_name=test_table_name)
    # assert
    assert exists


def test_rdbmiddleware_create_table_mysql(mock_mysql_dsn, test_table_name, test_database_metadata):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_mysql_dsn)
    # create table
    rdb_middleware.create_table(test_table_name, test_database_metadata)
    # existence
    exists = rdb_middleware.table_exists(table_name=test_table_name)
    # assert
    assert exists


def test_rdbmiddleware_table_exists_valid_psql(mock_psql_dsn, test_table_name, test_database_metadata):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_psql_dsn)
    # create table
    rdb_middleware.create_table(test_table_name, test_database_metadata)
    # existence
    exists = rdb_middleware.table_exists(table_name=test_table_name)
    # assert
    assert exists


def test_rdbmiddleware_table_exists_valid_mysql(mock_mysql_dsn, test_table_name, test_database_metadata):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_mysql_dsn)
    # create table
    rdb_middleware.create_table(test_table_name, test_database_metadata)
    # existence
    exists = rdb_middleware.table_exists(table_name=test_table_name)
    # assert
    assert exists


def test_rdbmiddleware_table_exists_invalid_psql(mock_psql_dsn, test_database_metadata):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_psql_dsn)
    # existence
    exists = rdb_middleware.table_exists(table_name="nonexistence_tablename")
    # assert
    assert not exists


def test_rdbmiddleware_table_exists_invalid_mysql(mock_mysql_dsn, test_database_metadata):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_mysql_dsn)
    # existence
    exists = rdb_middleware.table_exists(table_name="nonexistence_tablename")
    # assert
    assert not exists

def test_rdbmiddleware_populate_table_invalid_file(mock_psql_dsn, test_dir, test_database_metadata):
    # init middleware
    rdb_middleware = RDBMiddleware(connection_info=mock_psql_dsn)
    # generate non existent file path
    fake_filepath = test_dir.joinpath("unknown.csv")
    # assert populate table raises OSError
    # assert syntax error raised
    with pytest.raises(OSError):
        # execute query w/ middleware
        conn, cursor = rdb_middleware.populate_table(fake_filepath, "unknown.csv", test_database_metadata)

