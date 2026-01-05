# s3_boto3.py
# Contains code to perform data operations on s3 via boto3

import boto3

from dao.data_object import S3FileObject, S3DirectoryObject


def s3_path_generator(*paths, s3_prefix="s3"):
    """Generates a s3 path for reading data based on the required
    parameters."""

    return s3_prefix + "://" + "/".join([prefix for prefix in paths if prefix and prefix.strip()])


class S3Boto3:
    def __init__(self, bucket: str):
        self.bucket = bucket

        self.s3_client = boto3.client("s3")

    def write_data(self, data_object: S3FileObject, data, **kwargs):
        """Upload a file to an S3 bucket

        :param data_object: File Object to upload to.
        :param data: Data to upload to the FileObject
        :return: True if file was uploaded, else False
        """

        extra_args = kwargs.get("extra_args", {})
        callback = kwargs.get("callback", None)
        config = kwargs.get("config", None)

        try:
            response = self.s3_client.upload_fileobj(
                Fileobj=data,
                Bucket=self.bucket,
                Key=data_object.key,
                ExtraArgs=extra_args,
                Callback=callback,
                Config=config,
            )
        except Exception as e:
            print(f"Error uploading file: {e}")
            return False
        return response

    def read_data(self, data_object: S3FileObject, **kwargs):
        """Read a file from an S3 bucket

        :param data_object: File Object to read from.
        """
        try:
            # Extract parameters from data_object
            s3_key = data_object.key

            # Optional parameters
            version_id = kwargs.get("version_id", None)
            range_bytes = kwargs.get("range_bytes", None)

            # Additional arguments for get_object
            extra_args = {}
            if version_id:
                extra_args["VersionId"] = version_id
            if range_bytes:
                extra_args["Range"] = f"bytes={range_bytes}"

            # Read from S3
            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key, **extra_args)

            return response["Body"]
        except Exception as e:
            print(f"Error reading file file: {e}")
            return False

    def list_files(self, data_object: S3DirectoryObject, **kwargs):
        """
        Lists files in the S3 directory

        :param data_object:
        :param kwargs:
        """
        s3_path = data_object.key

        files = []

        continuation_token = None

        request_params = {
            "Bucket": self.bucket,
            "Prefix": s3_path + "/",
        }

        while True:
            if continuation_token:
                request_params["ContinuationToken"] = continuation_token

            response = self.s3_client.list_objects_v2(**request_params)

            if "Contents" in response:
                for obj in response["Contents"]:
                    if obj["Key"].endswith("/"):
                        continue

                    files.append(s3_path_generator(self.bucket, obj["Key"], s3_prefix="s3"))

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

        return files
