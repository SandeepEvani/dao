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

    def write_data(self, data: DataFrame, data_object, write_format: str = "csv", **kwargs):
        s3_path = s3_path_generator(
            self.bucket,
            self.prefix,
            data_object.identifier.upper(),
        )

        writer = data.write.option("header", "true").mode("overwrite")
        getattr(writer, data_object.data_format, write_format)(s3_path)

        return True
