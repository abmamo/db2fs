import pytest
# os
import os
import glob
import shutil
from pathlib import Path
# import aws middleware
from fs2db.connectors.gcp import GCPStorageMiddleware

@pytest.fixture(scope="module")
def download_dir():
    # get current file dir
    base_test_dir = Path(__file__).parent.absolute()
    # join base/current file dir with name of test data dir
    download_dir = base_test_dir.joinpath("test_gcp_download_dir")
    # create dir if it doesn't exist
    download_dir.mkdir(parents=True, exist_ok=True)
    # yield data dir
    yield download_dir
    # delete data dir after tests finish
    shutil.rmtree(str(download_dir))

@pytest.fixture(scope="module")
def test_gcp_middleware(test_gcp_auth_path):
    # if auth info found
    if test_gcp_auth_path:
        # init middleware
        test_gcp_middleware = GCPStorageMiddleware(
            auth_file_path=test_gcp_auth_path
        )
        return test_gcp_middleware
    else:
        return None

def test_get_buckets(test_gcp_middleware, test_gcp_bucket_name, test_gcp_bucket):
    if test_gcp_middleware is not None:
        assert test_gcp_bucket.name == test_gcp_bucket_name
        assert test_gcp_bucket_name in test_gcp_middleware.get_buckets()  
    else:
        pass

def test_get_bucket_files(test_dir, test_gcp_middleware, test_gcp_bucket_name, test_gcp_bucket):
    if test_gcp_middleware is not None:
        # get all file names in dir
        file_names = [os.path.basename(file_path) for file_path in glob.glob(os.path.join(test_dir, "*.*"))]
        # assert all file names in bucket
        assert all(file_name in test_gcp_middleware.get_bucket_files(bucket_name=test_gcp_bucket_name) for file_name in file_names)
    else:
        pass

def test_download_bucket_file(test_dir, download_dir, test_gcp_middleware, test_gcp_bucket_name, test_gcp_bucket):
    if test_gcp_middleware is not None:
        # get all file names in dir
        file_names = [os.path.basename(file_path) for file_path in glob.glob(os.path.join(test_dir, "*.*"))]
        # download file
        test_gcp_middleware.download_bucket_file(
            bucket_name=test_gcp_bucket_name,
            file_name=file_names[0],
            download_dir=download_dir
        )
        # assert file exists
        assert os.path.exists(os.path.join(download_dir, file_names[0]))
    else:
        pass 