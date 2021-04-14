from apf.core.step import GenericStep
import logging
import io
import math
import datetime
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
        s3_client=None,
        **step_args
    ):
        super().__init__(consumer, config=config, level=level)
        self.key = self.config["KEY"]
        self.s3_client = s3_client or boto3.client("s3")

    def get_object_url(self, bucket_name, identifier):
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
        return "https://{}.s3.amazonaws.com/{}.avro".format(bucket_name, identifier)

    def upload_file(self, f, identifier, bucket_name):
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
        object_name = "{}.avro".format(identifier)
        self.s3_client.upload_fileobj(f, bucket_name, object_name)
        return self.get_object_url(bucket_name, identifier)

    def get_candid(self, message, key):
        if callable(key):
            return key(message)
        else:
            return message[key]

    def execute(self, message):
        self.logger.debug(message["objectId"])
        f = io.BytesIO(self.consumer.messages[0].value())
        self.upload_file(
            f, self.get_candid(message, self.key), self.config["STORAGE"]["BUCKET_NAME"]
        )
