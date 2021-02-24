import pytest
# os
import os
import glob
import shutil
from pathlib import Path
# import aws middleware
from fs2db.connectors.azure import AzureStorageMiddleware

@pytest.fixture(scope="module")
def download_dir():
    # get current file dir
    base_test_dir = Path(__file__).parent.absolute()
    # join base/current file dir with name of test data dir
    download_dir = base_test_dir.joinpath("test_azure_download_dir")
    # create dir if it doesn't exist
    download_dir.mkdir(parents=True, exist_ok=True)
    # yield data dir
    yield download_dir
    # delete data dir after tests finish
    shutil.rmtree(str(download_dir))

@pytest.fixture(scope="module")
def test_azure_middleware(test_azure_connection_string):
    # if auth info found
    if test_azure_connection_string:
        # init middleware
        test_azure_middleware = AzureStorageMiddleware(
            connection_string=test_azure_connection_string
        )
        return test_azure_middleware
    else:
        return None

def test_get_containers(test_azure_middleware, test_azure_container_name, test_azure_container):
    if test_azure_middleware is not None:
        assert test_azure_container.container_name == test_azure_container_name
        assert test_azure_container_name in test_azure_middleware.get_containers()  
    else:
        pass

def test_get_container_files(test_dir, test_azure_middleware, test_azure_container_name, test_azure_container):
    if test_azure_middleware is not None:
        # get all file names in dir
        file_names = [os.path.basename(file_path) for file_path in glob.glob(os.path.join(test_dir, "*.*"))]
        # assert all file names in bucket
        assert all(file_name in test_azure_middleware.get_container_files(container_name=test_azure_container_name) for file_name in file_names)
    else:
        pass

def test_download_container_file(test_dir, download_dir, test_azure_middleware, test_azure_container_name, test_azure_container):
    if test_azure_middleware is not None:
        # get all file names in dir
        file_names = [os.path.basename(file_path) for file_path in glob.glob(os.path.join(test_dir, "*.*"))]
        # download file
        test_azure_middleware.download_container_file(
            container_name=test_azure_container_name,
            file_name=file_names[0],
            download_dir=download_dir
        )
        # assert file exists
        assert os.path.exists(os.path.join(download_dir, file_names[0]))
    else:
        pass

def test_upload_container_file(test_dir, test_azure_middleware, test_azure_container_name, test_azure_container):
    if test_azure_middleware is not None:
        # get all file names in dir
        file_paths = glob.glob(os.path.join(test_dir, "*.*"))
        # upload file
        test_azure_middleware.upload_container_file(
            container_name=test_azure_container_name,
            file_path=file_paths[0],
            key="test_azure_upload_file"
        )
        # assert file exists in container
        assert "test_azure_upload_file" in test_azure_middleware.get_container_files(container_name=test_azure_container_name)
    else:
        pass 