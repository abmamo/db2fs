import pytest
# os
import os
import glob
import shutil
from pathlib import Path
# import aws middleware
from db2fs.connectors.aws import AWSS3Middleware

@pytest.fixture(scope="module")
def download_dir():
    # get current file dir
    base_test_dir = Path(__file__).parent.absolute()
    # join base/current file dir with name of test data dir
    download_dir = base_test_dir.joinpath("test_aws_download_dir")
    # create dir if it doesn't exist
    download_dir.mkdir(parents=True, exist_ok=True)
    # yield data dir
    yield download_dir
    # delete data dir after tests finish
    shutil.rmtree(str(download_dir))

@pytest.fixture(scope="module")
def test_aws_middleware(test_s3_auth_info):
    # if auth info found
    if test_s3_auth_info:
        # init middleware
        test_aws_middleware = AWSS3Middleware(
            aws_access_key=test_s3_auth_info["aws_access_key"],
            aws_secret_access_key=test_s3_auth_info["aws_secret_access_key"]
        )
        return test_aws_middleware
    else:
        return None

def test_get_buckets(test_aws_middleware, test_s3_bucket_name, test_s3_bucket):
    if test_aws_middleware is not None:
        assert test_s3_bucket.name == test_s3_bucket_name
        assert test_s3_bucket_name in test_aws_middleware.get_buckets()  
    else:
        pass

def test_get_bucket_files(test_dir, test_aws_middleware, test_s3_bucket_name, test_s3_bucket):
    if test_aws_middleware is not None:
        # get all file names in dir
        file_names = [os.path.basename(file_path) for file_path in glob.glob(os.path.join(test_dir, "*.*"))]
        # assert all file names in bucket
        assert all(file_name in test_aws_middleware.get_bucket_files(bucket_name=test_s3_bucket_name) for file_name in file_names)
    else:
        pass

def test_download_bucket_file(test_dir, download_dir, test_aws_middleware, test_s3_bucket_name, test_s3_bucket):
    if test_aws_middleware is not None:
        # get all file names in dir
        file_names = [os.path.basename(file_path) for file_path in glob.glob(os.path.join(test_dir, "*.*"))]
        # download file
        test_aws_middleware.download_bucket_file(
            bucket_name=test_s3_bucket_name,
            file_name=file_names[0],
            download_dir=download_dir
        )
        # assert file exists
        assert os.path.exists(os.path.join(download_dir, file_names[0]))
    else:
        pass 