# testing framework
import pytest
# os
import os
import json
import glob
import uuid
import shutil
import random
import string
from pathlib import Path
# db population
from populate import Populate
# db middleware (Postgres + MySQL)
from fs2db.connectors.rdb import RDBMiddleware
# mock db
from fastdb import MockPostgres, MockMySQL
# gcp
from google.cloud import storage
from google.api_core import exceptions
from google.oauth2 import service_account
# azure
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
# aws
import boto3
# env
from dotenv import load_dotenv
# load env from .env
load_dotenv()
# init mock db classes
mock_mysql = MockMySQL()
mock_postgres = MockPostgres()
# start mock dbs in docker
mock_mysql.start()
mock_postgres.start()

@pytest.fixture(scope="module")
def test_dir():
    # get current file dir
    base_test_dir = Path(__file__).parent.absolute()
    # join base/current file dir with name of test data dir
    test_dir = base_test_dir.joinpath("test_data_dir")
    # create dir if it doesn't exist
    test_dir.mkdir(parents=True, exist_ok=True)
    # generate mock files in dir
    from mock import FileGenerator
    # file + data types to generate
    file_types=["csv", "json", "parquet", "xls"]
    data_types = ["job", "address", "profile", "currency"]
    # generate files
    for file_type, data_type in zip(file_types, data_types):
        # init file generator
        file_generator = FileGenerator(
            data_size=10,
            file_type=file_type,
            data_type=data_type
        )
        # store data to dir
        file_generator.store(data_dir=str(test_dir))
    # yield data dir
    yield test_dir
    # delete data dir after tests finish
    shutil.rmtree(str(test_dir))

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
    

@pytest.fixture(scope="module")
def test_table_name():
    return "test_table"

### GCP ###
@pytest.fixture(scope="module")
def test_gcp_auth_path():
    # get auth path from env file if present
    auth_path = os.environ.get("GCP_AUTH_PATH", "")
    # if auth path is found
    if auth_path:
        # if path exists
        if os.path.exists(auth_path):
            # return path
            return auth_path
        # path doesn't exist
        else:
            # return empty string
            return ""
    # no auth path specified
    else:
        # return empty string
        return ""


@pytest.fixture(scope="module")
def test_gcp_storage_client(test_gcp_auth_path):
    # check if auth path correctly configured
    if test_gcp_auth_path:
        # try initializing storage client
        try:
            # init gcp storage client
            with open(test_gcp_auth_path) as auth_file:
                # load json
                auth_dict = json.load(auth_file)
                # get credentials from auth dict
                credentials = service_account.Credentials.from_service_account_info(auth_dict)
                # init gcp storage client with auth
                test_gcp_storage_client = storage.Client(
                                        project=auth_dict["project_id"],
                                        credentials=credentials
                                    )
            # return authorized storage client
            return test_gcp_storage_client
        # if storage client initialization failed
        except:
            # return no client
            return None
    # if no auth path specified
    else:
        # return no client
        return None


@pytest.fixture(scope="module")
def test_gcp_bucket_name():
    # generate random string bucket name
    return ''.join(random.choice(string.ascii_lowercase) for i in range(10))

    
@pytest.fixture(scope="module")
def test_gcp_bucket(test_dir, test_gcp_storage_client, test_gcp_bucket_name):
    # if gcp storage client configured
    if test_gcp_storage_client is not None:
        # init bucket
        bucket = test_gcp_storage_client.bucket(test_gcp_bucket_name)
        # create bucket
        test_gcp_bucket = test_gcp_storage_client.create_bucket(bucket, location="us")
        # get all files in dir
        all_files = glob.glob(os.path.join(test_dir, "*.*"))
        # iterate through files and upload
        for file_path in all_files:
            # get file name
            file_name = os.path.basename(file_path)
            # create blob
            blob = test_gcp_bucket.blob(file_name)
            # upload local file to blob
            blob.upload_from_filename(file_path)
        # yield bucket
        yield test_gcp_bucket
        # empty bucket
        blobs = bucket.list_blobs()
        # iterate through files
        for blob in blobs:
            # delete file
            blob.delete()
        # delete bucket
        bucket.delete()
    # if client not correctly configured
    else:
        # yield no bucket
        yield None


### AWS ###
@pytest.fixture(scope="module")
def test_s3_auth_info():
    # get access key & secret access key
    access_key = os.environ.get("AWS_ACCESS_KEY", "")
    secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    # if either is missing
    if not access_key or not secret_access_key:
        # return empty dict
        return {}
    # return aws credentials
    return {
        "aws_access_key": access_key,
        "aws_secret_access_key": secret_access_key
    }


@pytest.fixture(scope="module")
def test_s3_bucket_name():
    # generate random string bucket name
    return ''.join(random.choice(string.ascii_lowercase) for i in range(10))

@pytest.fixture(scope="module")
def test_s3_resource(test_s3_auth_info):
    # if valid aws auth info found
    if test_s3_auth_info:
        # create s3 resource
        test_s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=test_s3_auth_info["aws_access_key"],
            aws_secret_access_key=test_s3_auth_info["aws_secret_access_key"]
        )
        # return authorized resource
        return test_s3_resource
    # invalid aws auth info found
    else:
        # return nothing
        return None

@pytest.fixture(scope="module")
def test_s3_bucket(test_dir, test_s3_resource, test_s3_bucket_name):
    # if s3 storage resource has been configured
    if test_s3_resource is not None:
        # create bucket
        test_s3_resource.create_bucket(
            Bucket=test_s3_bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'
            }
        )
        # get all files in test dir
        all_files = glob.glob(os.path.join(test_dir, "*.*"))
        # iterate through files and upload
        for file_path in all_files:
            # get file name from path
            file_name = os.path.basename(file_path)
            # upload file
            test_s3_resource.Object(test_s3_bucket_name, file_name).put(Body=open(file_path, 'rb'))
        # get bucket
        test_s3_bucket = test_s3_resource.Bucket(test_s3_bucket_name)
        # yield bucket
        yield test_s3_bucket
        # empty bucket
        test_s3_bucket.objects.all().delete()
        # delete bucket
        test_s3_bucket.delete()
    # if client not correctly configured
    else:
        # yield no bucket
        yield None

### azure ###
@pytest.fixture(scope="module")
def test_azure_connection_string():
    return os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")

@pytest.fixture(scope="module")
def test_azure_container_name():
    return str(uuid.uuid4())

@pytest.fixture(scope="module")
def test_azure_blob_service_client(test_azure_connection_string):
    # if connection string found
    if test_azure_connection_string:
        # authorize blob service client using connection string
        test_azure_blob_service_client = BlobServiceClient.from_connection_string(test_azure_connection_string)
        # return authorized blob service client
        return test_azure_blob_service_client
    # connection string not found
    else:
        # return no client
        return None

@pytest.fixture(scope="module")
def test_azure_container(test_dir, test_azure_blob_service_client, test_azure_container_name):
    # if blob service client configured
    if test_azure_blob_service_client:
        # create & return container client
        test_azure_container = test_azure_blob_service_client.create_container(test_azure_container_name)
        # upload files
        # get all files in test dir
        all_files = glob.glob(os.path.join(test_dir, "*.*"))
        # iterate through files and upload
        for file_path in all_files:
            # get file name from path
            file_name = os.path.basename(file_path)
            # create blob client using local file name
            blob_client = test_azure_blob_service_client.get_blob_client(
                container=test_azure_container_name,
                blob=file_name
            )
            # open file
            with open(file_path, "rb") as data:
                # upload file data
                blob_client.upload_blob(data)
        # yield container client
        yield test_azure_container
        # delete container after tests finish
        test_azure_container.delete_container()
    # blob service client not configured
    else:
        # yield no container
        yield None
