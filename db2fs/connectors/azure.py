# logging
import logging
# azure
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
# system
import os
# path
from pathlib import Path
# config logger
logger = logging.getLogger(__name__)


class AzureStorageMiddleware:
    """
        connect to an azure storage account using a connection string + 
        provide general Azure Storage functionalities such as:-
            - getting a container
            - getting all files in a container
            - downloading a file from a container

        init params:
            - connection_string: Azure Storage account connection string
    """
    def __init__(self, connection_string):
        # connection string to storage account
        self.connection_string = connection_string
        # try to connect to storage account
        try:
            # init azure storage client with connection string
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        # if client init failed
        except Exception as err:
            # log
            logger.error(
                "conn. to azure blob service failed w/ err: %s" % str(err)
            )
            # set connection status to inactive
            self.azure_storage_connected = False
            # exit
            return
        else:
            # set connection status to active
            self.azure_storage_connected = True

    def get_containers(self):
        """
            get all containers in an Azure Storage account

            params:
                - none (connection string saved during init)
            returns:
                - container_names: list of container names

        """
        # if not connected to Azure don't run
        if not self.azure_storage_connected:
            # log
            logger.error(
                "not connected. blob client: %s" % self.blob_service_client
            )
            # exit
            return
        try:
            # get all containers
            all_containers = self.blob_service_client.list_containers(
                                include_metadata=True
                             )
            # empty list to store names in
            container_names = []
            # for each container
            for container in all_containers:
                # add container name
                container_names.append(container['name'])
            # return container names
            return container_names
        except Exception as err:
            # log
            logger.error(
                "get containers failed with err: %s" % str(err)
            )
            # exit
            return

    def upload_container_file(self, container_name, file_path):
        """
            upload file to an Azure Storage container

            params:
                - container_name: uniqueu name of Azure Storage container
                                  we want to upload to
                - file_path: local location of file
        """
        # if not connected don't run
        if not self.azure_storage_connected:
            # log
            logger.error(
                "not connected. blob client: %s" % self.blob_service_client
            )
            # exit
            return
        try:
            # get file name from file path
            local_file_name = Path(file_path).name
            # init blob client
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
            # read file
            with open(file_path, "rb") as upload_file:
                # upload file
                blob_client.upload_blob(upload_file)
        except Exception as err:
            # log
            logger.error(
                "upload file: %s failed with err: %s" % (
                    file_path,
                    str(err)
                )
            )
            # exit
            return

    def upload_container_dir(self, bucket_name, local_dir_path):
        pass

    def get_container_files(self, container_name, prefix=None):
        """
            get all files in an Azure Storage container

            params:
                - container_name: valid & unique name of Azure container
                - prefix: filter by starting prefix
        """
        # if not connected don't run
        if not self.azure_storage_connected:
            # log
            logger.error(
                "not connected. blob client: %s" % self.blob_service_client
            )
            # exit
            return
        # get container client using container name
        container_client = self.blob_service_client.get_container_client(container_name)
        # get files using container client
        try:
            # if prefix specified get blobs
            if prefix is not None:
                # get blobs with prefix
                blobs = [blob.name for blob in container_client.list_blobs(prefix=prefix)]
            else:
                # get all blobs
                blobs = [blob.name for blob in container_client.list_blobs()]
            # return list of blobs
            return blobs
        # container not found
        except ResourceNotFoundError:
            # log
            logger.error(
                "container %s not found" % container_name
            )
            # exit
            return
        # unhandled error encountered
        except Exception as err:
            # log
            logger.error(
                "get container files: %s failed with err: %s" % (
                    container_name,
                    str(err)
                )
            )
            # exit
            return

    def download_container_file(self, container_name, file_name, download_dir):
        """
            download file from an Azure Storage container

            params:
                - container_name: name of Azure Storage container
                - file_name: name of file / blob
                - download_dir: location to download to
        """
        # if not connected don't run
        if not self.azure_storage_connected:
            # log
            logger.error(
                "not connected. blob client: %s" % self.blob_service_client
            )
            # exit
            return
        # get local file path
        local_file_path = os.path.join(download_dir, file_name)
        # get blob client
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=file_name)
        # check if file is in folder
        if "/" in file_name:
            # get folder name from file name
            dir_name = os.path.dirname(file_name)
            # create local dir path
            dir_path = os.path.join(download_dir, dir_name)
            # check if dir exists
            if not os.path.exists(dir_path):
                # create if it doesn't exist
                os.mkdir(dir_path)
            # update local file path
            local_file_path = os.path.join(download_dir, "_".join(dir_name.split("/")) + "_" + os.path.basename(file_name))
        try:
            # open file
            with open(local_file_path, "wb") as download_file:
                # log
                logger.info(
                    "downloading: %s" % file_name
                )
                logger.info(
                    "location: %s" % local_file_path
                )
                # log
                try:
                    # write file to local location
                    download_file.write(blob_client.download_blob().readall())
                except Exception as err:
                    # log
                    logger.error(
                        "downloading %s failed w/ err: %s" % (
                            file_name,
                            str(err)
                        )
                    )
                    # exit
                    return
        # opening file failed
        except OSError as err:
            # log
            logger.error(
                "opening file: %s failed w/ err: %s" % (
                    local_file_path,
                    str(err)
                )
            )
            # exit
            return
