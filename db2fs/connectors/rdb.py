# logging
import logging
# postgres dbapi
import psycopg2
# mysql dbapi
import pymysql
# csv processor
import csv
# progress bars
from tqdm import tqdm
# pandas
import pandas as pd
# configure logger
logger = logging.getLogger(__name__)


def without(d, key):
    """
        return dict without a given key

        params:
            d: dict
            key: key to be removed
    """
    # deep copy dictionary
    new_d = d.copy()
    # remove key/val from new copy
    new_d.pop(key)
    # return dict without key
    return new_d


class RDBConnector:
    """
        connect to a relational db
        (currently supports MySQL and PostgreSQL)
    """
    def __init__(self):
        # connection status
        self.rdb_initialized = True
        self.rdb_connected = False
        # db conn info
        self.connection_info = None
        # python types to db types mapping dict
        # conditionally configured based on engine
        self.mappings = None
        # expected connection info (differs by engine)
        self.expected = {
            "pg": [
                "engine",
                "host",
                "user",
                "port",
                "database"
            ],
            "mysql": [
                "engine",
                "host",
                "user",
                "port",
                "database"
            ]
        }
        # optional connection info (differs by engine)
        self.optional = {
            "pg": [
                "password"
            ],
            "mysql": [
                "password"
            ]
        }

    def connect(self, connection_info):
        """
            connect to database using the appropriate
            low level client

            params:
                - connection_info: relational db connection info
                    - engine: "pg" or "mysql" specifying the relational
                              db type
                    - host: db host localhost if running on the same
                            machine or URI if hosted on Azure or AWS
                    - dbname: database name
                    - user: username with read/write permissions on dbname
                    - password: password for user
        """
        # assert expected connection paramters are present in connection info
        if not all([
                    param in self.expected[connection_info["engine"]] or
                    self.optional[connection_info["engine"]]
                    for param in connection_info.keys()
                ]):
            # log
            logger.error(
                "invalid connection info: %s" % connection_info
            )
            # set to inactive
            self.rdb_initialized = True
            self.rdb_connected = False
        else:
            # get connection info
            self.connection_info = connection_info
            # postgres connect
            if self.connection_info["engine"] == "pg":
                # connect to postgres db
                try:
                    # connect using psycopg2
                    conn = psycopg2.connect(
                        # remove engine & pass db conn info
                        **without(self.connection_info, "engine")
                    )
                    # specify mappings to convert python types
                    # into PostgreSQL types
                    self.mappings = {
                        "int": "REAL",
                        "int64": "REAL",
                        "str": "TEXT",
                        "datetime": "TIMESTAMP",
                        "date": "TIMESTAMP",
                        "float64": "REAL",
                        "Timestamp": "TIMESTAMP",
                        "dict": "JSON"
                    }
                    # set connection status to active
                    self.rdb_initialized = True
                    self.rdb_connected = True
                    # return conn object
                    return conn
                except psycopg2.OperationalError as err:
                    # log error
                    logger.error(
                        "connecting to pg rdb failed with: %s" % str(err)
                    )
                    # set to inactive
                    self.rdb_initialized = True
                    self.rdb_connected = False
            # mysql connect
            elif self.connection_info["engine"] == "mysql":
                try:
                    # connect using pymysql
                    conn = pymysql.connect(
                        **without(self.connection_info, "engine")
                    )
                    # specify mappings to convert python types
                    # into MySQL types
                    self.mappings = {
                        "int": "BIGINT",
                        "int64": "BIGINT",
                        "str": "TEXT",
                        "datetime": "TEXT",
                        # temporary fix for datetime formatting
                        # in MySQL
                        "date": "TEXT",
                        "float64": "FLOAT",
                        "Timestamp": "TEXT",
                        "dict": "JSON"
                    }
                    # set connection status to active
                    self.rdb_initialized = True
                    self.rdb_connected = True
                    # return connection object
                    return conn
                except pymysql.OperationalError as err:
                    # log
                    logger.error(
                        "connecting to mysql rdb failed with: %s" % str(err)
                    )
                    # set to inactive
                    self.rdb_initialized = True
                    self.rdb_connected = False


    def disconnect(self):
        """
            disconnect from a relational db
        """
        self.connection_info = None
        # set to inactive
        self.rdb_initialized = True
        self.rdb_connected = False


class RDBMiddleware(RDBConnector):
    """
        provide basic SQL functionalities using RAW SQL queries.
        inherits RDBConnector and will use it to connect
        to a db

        init params:
            - connection_info: relational db connection info
                - engine: "pg" or "mysql" type of relational db
                - host: db host localhost if running on the same machine or URI
                        if hosted on Azure or AWS
                - dbname: database name
                - user: username with read/write permissions on dbname
                - password: password for user
    """
    def __init__(self, connection_info):
        # init rdb connector class
        super().__init__()
        # connect to rdb
        self.connect(connection_info=connection_info)

    def execute_query(self, sql_query_string, values=None):
        """
            execute SQL queries

            params:
                - sql_query_string: sql statement to execute
                - values: values to format and add to sql_query_string
        """
        # connect to db
        conn = self.connect(connection_info=self.connection_info)
        # get cursor from conn
        cursor = conn.cursor()
        if values is not None:
            # execute statement
            cursor.execute(
                # sql statement
                sql_query_string,
                # values
                values
            )
        else:
            # execute sql statement
            cursor.execute(sql_query_string)
        # return true for success
        return conn, cursor

    def get_tables(self):
        """
            get names of all tables in db & return them as list
        """
        if self.connection_info["engine"] == "pg":
            # table info query
            sql_get_tbls = "SELECT table_name FROM information_schema.tables where table_schema not in ('pg_catalog', 'information_schema');"
            #and table_schema not like 'pg_toast%'
        elif self.connection_info["engine"] == "mysql":
            # table info query
            sql_get_tbls = "SELECT table_name FROM information_schema.tables WHERE TABLE_SCHEMA='%s';" % self.connection_info["database"]
        # execute table info query
        conn, cursor = self.execute_query(sql_get_tbls)
        # get result of query from returned cursor
        tables = cursor.fetchall()
        # var to store existence state
        exists = False
        # table names
        table_names = []
        # for each table in returned tables
        for table in tables:
            # add name to table names
            table_names.append(table[0])
        return table_names

    def table_exists(self, table_name):
        """
            check if a given table exists in db

            params:
                - table_name: name of table
            returns:
                - table_exists: True or False
        """
        # check if table name exists in list of tables
        return table_name in self.get_tables()

    def create_table(self, table_name, metadata):
        """
            create table from metadata (a dict containing col name & type)
            e.g. metadata = [('col_1', 'REAL'), ('col_2', 'TEXT')] will
            create a table with columns name and age of types real and int

            params:
                - table_name: the name of the table being created
                - metadata: the information about the table
            returns:
                - nothing
        """
        if self.table_exists(table_name):
            return
        else:
            # log
            logger.info(
                "creating table: %s with metadata: %s" % (
                    table_name,
                    metadata
                )
            )
            # my sql
            if self.connection_info["engine"] == "mysql":
                # build create string (for mysql notice the usage of ``)
                sql_create_table = "CREATE TABLE `" + table_name + "` ("
                # add column info from metadata
                sql_create_table += ", ".join('`{0}` {1}'.format(key, val) for key, val in metadata) + ");"
            elif self.connection_info["engine"] == "pg":
                # build create string (for postgres notice the usage of "")
                sql_create_table = "CREATE TABLE \"" + table_name + "\" ("
                # add column info from metadata
                sql_create_table += ", ".join('\"{0}\" {1}'.format(key.replace('"', ''), val) for key, val in metadata) + ");"
            # execute create statement
            conn, cursor = self.execute_query(sql_create_table)
            # save changes in db
            conn.commit()

    def populate_table(self, file_path, table_name, metadata):
        """
            populates a SQL table by iteratively reading a CSV file.
            iterative because large CSV will cause memory overflow

            params:
                - file_path: location on disk to a CSV file
                - table_name: table we are inserting SQL into
                - metadata: the information about the table used to generate
                            SQL statements
        """
        # log
        logger.info(
            "populating table: %s with file: %s" % (
                table_name,
                file_path
            )
        )
        # postgres
        if self.connection_info["engine"] == "pg":
            # insert statement for postgres (notice the \")
            sql_populate_table = "INSERT INTO \"" + table_name + "\" VALUES (" + ", ".join("%s" for key, val in metadata) + ");"
        elif self.connection_info["engine"] == "mysql":
            # insert statement for mysq (notice the ``)
            sql_populate_table = "INSERT INTO `" + table_name + "` VALUES (" + ", ".join("%s" for key, val in metadata) + ");"
        try:
            # open csv file
            with open(file_path, 'r') as f:
                # read csv
                reader = csv.reader(
                    f,
                    delimiter=',',
                    quotechar='"',
                    skipinitialspace=True
                )
                # skip header
                next(reader)
                # initialize with tqdm to show progress
                pbar = tqdm(reader)
                # insert each row (use tqdm to show progress)
                for row in pbar:
                    pbar.set_description("generating table: %s" % table_name)
                    # convert numeric types
                    converted_row = pd.DataFrame(
                        pd.to_numeric(
                            row,
                            errors='coerce'
                        )
                    )
                    original_row = pd.DataFrame(
                        pd.to_numeric(
                            row,
                            errors='ignore'
                        )
                    )
                    # merge converted rows
                    merged_row = converted_row.fillna(original_row).T.convert_dtypes()
                    # remove quotes
                    merged_row.replace('"', '', regex=True, inplace=True)
                    # convert to list
                    converted_row = merged_row.values.tolist()[0]
                    # check metadata
                    if len(metadata) == len(converted_row):
                        # execute insert
                        conn, cursor = self.execute_query(
                            # insert statement
                            sql_populate_table,
                            # values
                            converted_row
                            )
            # save changes
            conn.commit()
        except OSError as err:
            # log
            logger.error(
                "opening file: %s failed w/ err: %s" % (
                    file_path,
                    str(err)
                )
            )
            raise err