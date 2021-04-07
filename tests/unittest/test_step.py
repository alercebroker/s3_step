import unittest
from unittest import mock
from s3_step.step import S3Step, boto3, io
from apf.consumers import GenericConsumer


class StepTestCase(unittest.TestCase):
    def setUp(self):
        STORAGE_CONFIG = {
            "BUCKET_NAME": "fake_bucket",
            "AWS_ACCESS_KEY": "fake",
            "AWS_SECRET_ACCESS_KEY": "fake",
            "REGION_NAME": "fake",
        }
        STEP_METADATA = {
            "STEP_VERSION": "dev",
            "STEP_ID": "s3",
            "STEP_NAME": "s3",
            "STEP_COMMENTS": "s3 upload",
        }
        METRICS_CONFIG = {}
        self.step_config = {
            "STORAGE": STORAGE_CONFIG,
            "STEP_METADATA": STEP_METADATA,
            "METRICS_CONFIG": METRICS_CONFIG,
            "KEY": "candid",
        }
        mock_consumer = mock.create_autospec(GenericConsumer)
        mock_message = mock.MagicMock()
        mock_message.value.return_value = b"fake"

        mock_consumer.messages = [mock_message]
        self.step = S3Step(
            config=self.step_config, consumer=mock_consumer, s3_client=mock.MagicMock()
        )

    def test_get_object_url(self):
        bucket_name = self.step_config["STORAGE"]["BUCKET_NAME"]
        candid = 123
        url = self.step.get_object_url(bucket_name, candid)
        self.assertEqual(url, "https://fake_bucket.s3.amazonaws.com/123.avro")

    def test_upload_file(self):
        f = io.BytesIO(b"fake")
        candid = 123
        bucket_name = self.step_config["STORAGE"]["BUCKET_NAME"]
        self.step.upload_file(f, candid, bucket_name)
        self.step.s3_client.upload_fileobj.assert_called_with(
            f, bucket_name, f"{candid}.avro"
        )

    @mock.patch("s3_step.S3Step.upload_file")
    def test_execute(self, mock_upload):
        message = {"objectId": "obj", "candid": 123}
        self.step.execute(message)
        mock_upload.assert_called_once()
