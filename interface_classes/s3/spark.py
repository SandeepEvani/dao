# spark.py
# Contains code to perform data operations on s3 via spark

from pyspark.sql import DataFrame, SparkSession

from dao.data_object import S3TableObject


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

    def write_data(self, data: DataFrame, data_object: S3TableObject, format: str = "csv", spark_options=None) -> bool:
        """
        Writes data to S3 via Spark

        :param data:
        :param data_object:
        :param format:
        :param spark_options:
        :return:
        """

        s3_path = s3_path_generator(
            self.bucket,
            self.prefix,
            data_object.key,
        )

        writer = data.write

        if spark_options and "mode" in spark_options:
            # Preventing the mutation of the original spark_options dict
            spark_options = spark_options.copy()
            mode = spark_options.pop("mode")
            writer = writer.mode(mode)

            writer.options(**spark_options)

        getattr(writer, format)(s3_path)

        return True

    def read_data(self, data_object: S3TableObject, format: str = "csv", spark_options=None) -> DataFrame:
        """
        Read data from S3 via Spark

        :param data_object:
        :param format:
        :param spark_options:
        :return:
        """

        spark_session = SparkSession.getActiveSession()

        s3_path = s3_path_generator(
            self.bucket,
            self.prefix,
            data_object.key,
        )

        reader = spark_session.read

        if spark_options:
            reader = reader.options(**spark_options)

        return getattr(reader, format)(s3_path)
