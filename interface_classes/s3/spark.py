# spark.py
# Contains code to perform data operations on s3 via spark

import boto3
from pyspark.sql import DataFrame


def s3_path_generator(*paths, s3_prefix="s3a"):
    """Generates a s3 path for reading data based on the required
    parameters."""

    return s3_prefix + "://" + "/".join([prefix for prefix in paths if prefix and prefix.strip()])


class SparkS3:
    def __init__(self, bucket, prefix, **kwargs):
        self.bucket = bucket
        self.prefix = prefix

        for k, v in kwargs.items():
            setattr(self, k, v)

    def write_data_in_csv(self, data: DataFrame, data_object, write_format: str = "csv", **kwargs):
        s3_path = s3_path_generator(
            self.bucket,
            self.prefix,
            data_object.identifier.upper(),
        )

        writer = data.write.option("header", "true").mode("overwrite")
        getattr(writer, data_object.data_format, write_format)(s3_path)

        return True

    def list_files(self, data_object, list_files: bool, **kwargs):
        s3_path = "/".join(
            [
                prefix
                for prefix in [
                    self.prefix,
                    data_object.db.upper(),
                    data_object.name.upper(),
                ]
                if prefix != "" and prefix is not None
            ]
        )

        files = []

        s3 = boto3.client("s3")
        continuation_token = None

        request_params = {
            "Bucket": self.bucket,
            "Prefix": s3_path + "/",
        }

        while True:
            if continuation_token:
                request_params["ContinuationToken"] = continuation_token

            response = s3.list_objects_v2(**request_params)

            if "Contents" in response:
                for obj in response["Contents"]:
                    if obj["Key"].endswith("/"):
                        continue

                    files.append(s3_path_generator(self.bucket, obj["Key"], s3_prefix="s3"))

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

        return files
