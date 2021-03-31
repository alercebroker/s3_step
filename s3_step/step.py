from apf.core.step import GenericStep
import logging
import io
import math
import datetime
from db_plugins.db.sql import SQLConnection
from db_plugins.db.sql.models import Step
import boto3
from botocore.config import Config


class S3Step(GenericStep):
    """S3Step Description

    Parameters
    ----------
    consumer : GenericConsumer
        Description of parameter `consumer`.
    **step_args : type
        Other args passed to step (DB connections, API requests, etc.)

    """

    def __init__(
        self,
        consumer=None,
        config=None,
        level=logging.INFO,
        **step_args
    ):
        super().__init__(consumer, config=config, level=level)
        self.key = self.config["KEY"]

    def get_object_url(self, bucket_name, candid):
        """
        Formats a valid s3 url for an avro file given a bucket and candid.
        The format for saving avros on s3 is <candid>.avro and they are
        all stored in the root directory of the bucket.

        Parameters
        ----------
        bucket_name : str
            name of the bucket
        candid : int
            candid of the avro to be stored
        """
        reverse_candid = self.reverse_candid(candid)
        return "https://{}.s3.amazonaws.com/{}.avro".format(bucket_name, reverse_candid)

    def upload_file(self, f, candid, bucket_name):
        """
        Uploads a avro file to s3 storage

        You have to configure STORAGE settings in the step. A dictionary like this is required:

        .. code-block:: python

            STEP_CONFIG = {
                "STORAGE": {
                    "AWS_ACCESS_KEY": "",
                    "AWS_SECRET_ACCESS_KEY": "",
                    "REGION_NAME": "",
                }
            }

        Parameters
        ----------
        f : file-like object
            Readable file like object that will be uploaded
        candid : int
            candid of the avro file. Avro files are stored using avro as object name
        bucket_name : str
            name of the s3 bucket
        """
        s3 = boto3.client("s3")
        reverse_candid = self.reverse_candid(candid)
        object_name = "{}.avro".format(reverse_candid)
        s3.upload_fileobj(f, bucket_name, object_name)
        return self.get_object_url(bucket_name, candid)

    def reverse_candid(self, candid):
        """
        Returns reverse digits of the candid

        Parameters
        ----------
        candid : int or str
            original candid to be reversed
        """
        reversed = str(candid)[::-1]
        return reversed

    def get_candid(self,message,key):
        if callable(key):
            return key(message)
        else:
            return message[key]

    def execute(self, message):
        self.logger.debug(message["objectId"])
        f = io.BytesIO(self.consumer.messages[0].value())
        self.upload_file(
            f, self.get_candid(message,self.key), self.config["STORAGE"]["BUCKET_NAME"]
        )
