# logging
import logging
# gcp 
from google.cloud import storage
from google.api_core import exceptions
from google.oauth2 import service_account
# json
import json
# os
import os
# config logger
logger = logging.getLogger(__name__)

class GCPStorageMiddleware:
    """
        connect to a GCP storage account using
        service account auth + provide general
        GCP Storage functions such as:-
            - getting a bucket
            - getting all files in a bucket
            - downloading a file from a bucket
        
        init params:
            - auth_path: file path to goole auth file
    """
    def __init__(self, auth_file_path):
        # location of GCP auth file
        self.auth_file_path = auth_file_path
        # try to connect to storage account
        try:
            # init gcp storage client
            with open(self.auth_file_path) as auth_file:
                # load json
                auth_dict = json.load(auth_file)
                # get credentials from auth dict
                credentials = service_account.Credentials.from_service_account_info(auth_dict)
                # init gcp storage client with auth
                self.storage_client = storage.Client(
                                        project=auth_dict["project_id"],
                                        credentials=credentials
                                    )
        # if client init failed
        except Exception as err:
            # log
            logger.error(
                "conn. to gcp storage service failed w/ err: %s" % str(err)
            )
            # set connection status to inactive
            self.gcp_storage_connected = False
            # exit
            return
        # if client init succeeded
        else:
            # set connection status to active
            self.gcp_storage_connected = True
    
    def get_buckets(self):
        """
            get all buckets in a GCP Storage account

            params:
                - none (account info stored in auth file)
            returns:
                - bucket_names: list of bucket names
        """
        # if not connected to GCP don't run
        if not self.gcp_storage_connected:
            # log
            logger.error(
                "not connected. storage client: %s" % self.storage_client
            )
            # exit
            return
        # get all buckets
        all_buckets = self.storage_client.list_buckets()
        # get all bucket names
        return [bucket.name for bucket in all_buckets]
    
    def get_bucket_files(self, bucket_name, prefix=None):
        """
            get all files in a bucket

            params:
                - bucket_name: valid & unique GCP bucket name
        """
        # if not connected don't run
        if not self.gcp_storage_connected:
            # log
            logger.error(
                "not connected. storage client: %s" % self.storage_client
            )
            # exit
            return
        try:
            # if prefix specified get blobs
            if prefix is not None:
                # get blobs with prefix
                blobs = self.storage_client.list_blobs(bucket_name, prefix=prefix)
            else:
                # get blobs in bucket
                blobs = self.storage_client.list_blobs(bucket_name)
        except exceptions.NotFound:
            # log
            logger.error(
                "bucket: %s not found" % bucket_name
            )
            # exit
            return
        # return name of blobs
        return [blob.name for blob in blobs]

    def upload_file_bucket(self, bucket_name, local_file_path, destination_blob_name=None):
        """
            upload file to a GCP Storage bucket
        """
        try:
            # get bucket
            bucket = self.storage_client.bucket(bucket_name)
        except exceptions.NotFound:
            # log
            logger.error(
                "bucket: %s not found" % bucket_name
            )
            # exit
            return
        # get destination blob name from local dir path
        if destination_blob_name is None:
            # get name of file locally
            destination_blob_name = os.path.basename(local_file_path)
        # create blob with name
        blob = bucket.blob(destination_blob_name)
        # upload local file to blob
        blob.upload_from_filename(local_file_path)
        print(
            "file {} uploaded to {}.".format(
                local_file_path, destination_blob_name
            )
        )
    
    def upload_bucket_dir(self, bucket_name, local_dir_path):
        pass

    def download_bucket_file(self, bucket_name, file_name, download_dir):
        """
            download file from a GCP Storage bucket

            params:
                - bucket_name: name of GCP bucket
                - file_name: name of file in GCP bucket
                - download_dir: location to download to
        """
        # if not connected don't run
        if not self.gcp_storage_connected:
            # log
            logger.error(
                "not connected. blob client: %s" % self.storage_client
            )
            # exit
            return
        # generate local file path
        local_file_path = os.path.join(download_dir, file_name)
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
            # get bucket
            bucket = self.storage_client.bucket(bucket_name)
        except exceptions.NotFound:
            # log
            logger.error(
                "bucket: %s not found" % bucket_name
            )
            # exit
            return
        # get blob client
        blob = bucket.blob(file_name)
        # download
        blob.download_to_filename(local_file_path)
