# logging
import logging
# aws
import boto3
from botocore.exceptions import ClientError
# json
import json
# os
import os
# config logger
logger = logging.getLogger(__name__)

class AWSS3Middleware:
    """
        connect to an AWS S3 + provide general
        S3 functions such as :-
            - getting a bucket
            - getting all files in a bucket
            - downloading a file from a bucket
        
        init params:
            - aws_access_key
            - aws_secret_access_key
    """
    def __init__(self, aws_access_key, aws_secret_access_key):
        # try to connect to s3 account
        try:
            self.s3_resource = boto3.resource(
                "s3",
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_access_key
            )
        # if resource init failed
        except Exception as err:
            # log
            logger.error(
                "conn. to aws s3 service failed w/ err: %s"
            )
            # set connection status to inactive
            self.s3_connected = False
            # exit
            return
        # if resource init succeeded
        else:
            # set connection status to active
            self.s3_connected = True
    
    def get_buckets(self):
        """
            get all buckets in a AWS account

            params:
                - none (account info stored during init)
            returns:
                - bucket_names: list of bucket names
        """
        # if not connected to AWS don't run
        if not self.s3_connected:
            # log
            logger.error(
                "not connected. s3 resource: %s" % self.s3_resource
            )
            # exit
            return
        # get all buckets
        all_buckets = self.s3_resource.buckets.all()
        # get all bucket names & return
        return [bucket.name for bucket in all_buckets]

    def get_bucket_files(self, bucket_name, prefix=None):
        """
            get all files in a bucket

            params:
                - bucket_name: valid & unique AWS bucket name
        """    
        # if not connected don't run
        if not self.s3_connected:
            # log
            logger.error(
                "not connected. s3 resource: %s" % self.s3_resource
            )
            # exit
            return
        try:
            # get bucket
            bucket = self.s3_resource.Bucket(bucket_name)
            # if prefix specified get blobs by prefix
            if prefix is not None:
                # get object in bucket by prefix
                objs = bucket.objects.filter(Prefix=prefix)
            else:
                # get object in bucket
                objs = bucket.objects.all()
        except self.s3_resource.meta.client.exceptions.NoSuchBucket:
            # log
            logger.error(
                "bucket: %s not found" % bucket_name
            )
            # exit
            return
        # return name of objects
        return [obj.key for obj in objs]
    
    def upload_bucket_file(self, bucket_name, local_file_path, key=None, prefix=None):
        # if key is not specified use local file name
        if key is None:
            # use filename as key
            key = os.path.basename(local_file_path)
        # if prefix is specified
        if prefix is not None:
            # update key
            key = os.path.join(prefix, key)
        # check if bucket exists
        try:
            # get bucket
            bucket = self.s3_resource.Bucket(bucket_name)
        except self.s3_resource.meta.client.exceptions.NoSuchBucket:
            # log
            logger.error(
                "bucket: %s not found" % bucket_name
            )
            # exit
            return
        # upload file
        bucket.upload_file(local_file_path, bucket_name, key)
    
    def upload_bucket_dir(self, bucket_name, local_dir_path):
        pass
    
    def download_bucket_file(self, bucket_name, file_name, download_dir):
        """
            download file / folder from an AWS S3 bucket

            params:
                - bucket_name: name of AWS S3 bucket
                - file_name: name of file in AWS S3 bucket
                - download_dir: location to download to
        """
        # if not connected don't run
        if not self.s3_connected:
            # log
            logger.error(
                "not connected. s3 resource: %s" % self.s3_resource
            )
            # exit
            return
        # generate local file path
        local_file_path = os.path.join(download_dir, file_name)
        # get bucket
        try:
            # get bucket
            bucket = self.s3_resource.Bucket(bucket_name)
        except self.s3_resource.meta.client.exceptions.NoSuchBucket:
            # log
            logger.error(
                "bucket: %s not found" % bucket_name
            )
            # exit
            return
        try:
            # get object
            obj = bucket.Object(file_name)
        except:
            # log
            logger.error(
                "object: %s not found" % bucket_name
            )
            # exit
            return
        # check if file is in a folder
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
        # download file
        bucket.download_file(file_name, local_file_path)