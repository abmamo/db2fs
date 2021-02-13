# db middleware
from db2fs.connectors.rdb import RDBMiddleware
# gcp middleware
from db2fs.connectors.gcp import GCPStorageMiddleware
# common io functions
from db2fs.shared import IOFunctions
# multi processing / batch processing
import multiprocessing as mp
# path
from pathlib import Path
# pandas
import pandas as pd
# io
import os

class DatabaseExtractor(IOFunctions, RDBMiddleware, GCPStorageMiddleware):
    """
        database extractor class to download tables
        in a database into csv files. currently supports
        mysql & postgres
    """
    def __init__(self, connection_info, auth_file_path=None, download_dir_name="downloaded", download_dir_path=None):
        # init db middleware
        RDBMiddleware.__init__(
            self,
            connection_info=connection_info
        )
        # init gcp middleware if auth file specified
        if auth_file_path is not None:
            # init middleware
            GCPStorageMiddleware.__init__(self, auth_file_path=auth_file_path)
        # specify download dir info
        self.download_dir_name = download_dir_name
        self.download_dir_path = download_dir_path
    
    def table2csv(self, table_name, file_name=None):
        """
            convert single table to csv

            params:
                - table_name: name of table to download
                - file_name: local file name to save table to
        """
        print("downloading: %s" % table_name)
        # get db connection
        conn = self.connect(self.connection_info)
        # get cursor
        cursor = conn.cursor()
        # create download dir
        if self.download_dir_path is None:
            # use current file's dir as download dir
            download_dir = os.path.dirname(os.path.realpath(__file__))
            # create dir using download dir name
            self.download_dir_path = self.create_dir(os.path.join(download_dir, self.download_dir_name))
        # create local file name
        if ".csv" in table_name:
            # if file name specified
            if file_name is not None:
                # create local file path just using file name
                # / no extension
                local_file_path = os.path.join(self.download_dir_path, file_name + ".csv")
            # if file name not specified use name of table
            else:
                # create local file path just using file name
                # / no extension
                local_file_path = os.path.join(self.download_dir_path, table_name)
        else:
            # if file name specified
            if file_name is not None:
                # create local file path just using file name
                # / no extension
                local_file_path = os.path.join(self.download_dir_path, file_name + ".csv")
            # if file name not specified use name of table
            else:
                # create local file path just using file name
                # / no extension
                local_file_path = os.path.join(self.download_dir_path, table_name + ".csv")
        # raw sql select statement for different engines
        # postgres
        if self.connection_info["engine"] == "pg":
            # get all rows in table
            select_query = "SELECT * FROM \"%s\"" % table_name
        # mysql
        elif self.connection_info["engine"] == "mysql":
            # get all rows in table
            select_query = "SELECT * FROM `%s`" % table_name
        # open csv
        with open(local_file_path, "w") as out_file:
            # raw sql copy differs by dialect
            # postgres
            if self.connection_info["engine"] == "pg":
                # define copy query (use postgres native csv functions)
                copy_query = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(select_query)
                # execute copy query
                cursor.copy_expert(copy_query, out_file)
            elif self.connection_info["engine"] == "mysql":
                # batch write using select statements to
                # avoid issues with using INTO OUTFILE
                # execute select query
                cursor.execute(select_query)
                # read data in batches
                while True:
                    # read the data using select statement
                    df = pd.DataFrame(cursor.fetchmany(10000))
                    # We are done if there are no data
                    if len(df) == 0:
                        break
                    # Let's write to the file
                    else:
                        df.to_csv(out_file, header=False)
                    # free memory
                    del df
            return local_file_path
    
    def table2other(self, table_name, file_type=".json", file_name=None):
        """
            convert a single table to a given file type

            params:
                - table_name: name of table to download
                - file_type: file to convert to table
                - file_name: name of file
        """
        # download table 2 csv
        local_csv_path = self.table2csv(table_name = table_name, file_name = file_name)
        # convert csv to specified file type
        if file_type == ".json":
            # convert csv to json
            df = pd.read_csv(local_csv_path)
            # create json path
            local_json_path = Path(local_csv_path)
            # update extension
            local_json_path.rename(local_json_path.with_suffix(file_type))
            # write to json
            df.to_json(local_json_path)
            # delete csv
            os.remove(local_csv_path)
            # return json file path
            return local_json_path
        
    def db2csv(self):
        # get all tables
        all_tables = self.get_tables()
        # get number of cpus available
        pool_size = mp.cpu_count()
        # create worker pool
        pool = mp.Pool(processes=pool_size)
        # start download function
        download_status = [pool.apply(self.table2csv, args=(table_name,)) for table_name in all_tables]
        # return
        return download_status

    def db2other(self, file_type=".json"):
        # get all tables
        all_tables = self.get_tables()
        # get number of cpus available
        pool_size = mp.cpu_count()
        # create worker pool
        pool = mp.Pool(processes=pool_size)
        # start download function
        download_status = [pool.apply(self.table2other, args=(table_name, file_type,)) for table_name in all_tables]
        # return
        return download_status
