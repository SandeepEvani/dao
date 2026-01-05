"""
Hudi operations on AWS S3 using Apache Spark.

Provides S3SparkHudi class for reading, writing, deleting, and streaming
data to/from Hudi tables on S3 with CDC (Change Data Capture) support.
"""

from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from dao.data_object import S3HudiObject


def s3_path_generator(*paths, s3_prefix="s3a"):
    """
    Generates S3 URI from path components.

    Args:
        *paths: Path components (bucket, prefix, key, etc.). Filters out None/empty strings.
        s3_prefix (str): Protocol prefix ("s3a" for Spark, "s3" for AWS SDK). Default: "s3a"

    Returns:
        str: Formatted S3 URI (e.g., "s3a://bucket/path/to/key")
    """

    return s3_prefix + "://" + "/".join([prefix for prefix in paths if prefix and prefix.strip()])


class S3SparkHudi:
    """
    Manages Hudi table operations on AWS S3 using Apache Spark.

    Provides methods for CRUD operations (Create, Read, Update, Delete) on Hudi tables.
    Supports snapshot reads, incremental CDC reads, writes, deletes, and streaming operations.

    Attributes:
        bucket (str): S3 bucket name
        prefix (str, optional): Base S3 prefix/path within the bucket
    """

    def __init__(self, bucket: str, prefix: Optional[str] = None, **kwargs):
        """
        Initialize S3SparkHudi instance.

        Args:
            bucket (str): S3 bucket name
            prefix (str, optional): Base S3 prefix path. Default: None
            **kwargs: Additional attributes to set on the instance
        """
        self.bucket = bucket
        self.prefix = prefix

        for k, v in kwargs.items():
            setattr(self, k, v)

    def read_hudi_table_from_s3(self, data_object: S3HudiObject) -> DataFrame:
        """
        Reads a Hudi table from S3 and returns a Spark DataFrame.

        Performs a snapshot read of the latest committed records. For incremental reads
        (CDC), use read_incremental_hudi_data_from_s3() instead.

        Args:
            data_object (S3HudiObject): Table metadata with 'key' attribute (S3 path)

        Returns:
            pyspark.sql.DataFrame: Spark DataFrame with Hudi table data
        """
        spark_session = SparkSession.getActiveSession()

        s3_path = s3_path_generator(
            self.bucket,
            self.prefix,
            data_object.key,
        )

        df = spark_session.read.format("hudi").load(s3_path)
        return df

    def write_hudi_table_to_s3(
        self,
        data: DataFrame,
        data_object: S3HudiObject,
        data_write_mode="bulk_insert",
        table_write_mode="append",
        hudi_options=None,
    ):
        """
        Writes a Spark DataFrame to S3 in Hudi format.

        Args:
            data (DataFrame): DataFrame to write to S3
            data_object (S3HudiObject): Table metadata (key, identifier, precombine_key, record_key, partition_key)
            data_write_mode (str): "bulk_insert", "upsert", or "insert". Default: "bulk_insert"
            table_write_mode (str): "append", "overwrite", "ignore", or "error". Default: "append"
            hudi_options (dict, optional): Additional Hudi options

        Returns:
            bool: True on success
        """
        s3_path = s3_path_generator(
            self.bucket,
            self.prefix,
            data_object.key,
        )

        hudi_options = hudi_options or {}

        (
            data.write.format("hudi")
            .option("hoodie.datasource.write.operation", data_write_mode)
            .option("hoodie.table.name", data_object.identifier)
            .option("hoodie.datasource.hive_sync.table", data_object.identifier)
            .option("hoodie.datasource.write.precombine.field", data_object.precombine_key)
            .option("hoodie.datasource.write.recordkey.field", data_object.record_key)
            .option("hoodie.datasource.write.partitionpath.field", data_object.partition_key)
            .options(**hudi_options)
            .mode(table_write_mode)
            .save(s3_path)
        )

        return True

    def delete_hudi_from_s3(self, data: DataFrame, data_object: S3HudiObject, hudi_options: Optional[Dict] = None):
        """
        Deletes records from a Hudi table on S3 (soft delete - records marked as deleted).

        Args:
            data (DataFrame): DataFrame with record keys to delete
            data_object (S3HudiObject): Table metadata (key, identifier, precombine_key, record_key, partition_key)
            hudi_options (dict, optional): Additional Hudi options

        Returns:
            bool: True on success
        """

        s3_path = s3_path_generator(
            self.bucket,
            self.prefix,
            data_object.key,
        )

        hudi_options = hudi_options or {}
        (
            data.write.format("hudi")
            .option("hoodie.datasource.write.operation", "delete")
            .option("hoodie.table.name", data_object.identifier)
            .option("hoodie.datasource.write.precombine.field", data_object.precombine_key)
            .option("hoodie.datasource.write.recordkey.field", data_object.record_key)
            .option("hoodie.datasource.write.partitionpath.field", data_object.partition_key)
            .options(**hudi_options)
            .mode("append")
            .save(s3_path)
        )

        return True

    def read_incremental_hudi_data_from_s3(self, data_object: S3HudiObject, instant_time):
        """
        Reads incremental changes from a Hudi table (Change Data Capture).

        Returns only records updated/inserted since the specified instant_time.
        More efficient than reading the entire snapshot.

        Args:
            data_object (S3HudiObject): Table metadata with 'key' attribute (S3 path)
            instant_time (str): Start timestamp in format "YYYYMMDDHHmmssSSS" (e.g., "20221228073128455")

        Returns:
            pyspark.sql.DataFrame: Incremental changes since instant_time
        """
        # Read Options, to read in incremental mode
        read_options = {
            "hoodie.datasource.query.type": "incremental",
            "hoodie.datasource.read.begin.instanttime": instant_time,
        }

        spark_session = SparkSession.getActiveSession()

        s3_path = s3_path_generator(
            self.bucket,
            self.prefix,
            data_object.key,
        )

        # Reading the upserted records
        incremental_df = spark_session.read.format("hudi").options(**read_options).load(s3_path)

        # returns the Spark Dataframe
        return incremental_df

    def write_stream_to_s3(
        self,
        data_stream,
        data_object: S3HudiObject,
        checkpoint_location,
        data_write_mode="upsert",
        table_write_mode="append",
        hudi_options=None,
    ):
        """
        Writes a Spark streaming DataFrame to S3 in Hudi format with checkpoint management.

        Enables streaming data ingestion with exactly-once semantics and supports upserts for CDC.

        Args:
            data_stream: Spark structured streaming DataFrame
            data_object (S3HudiObject): Table metadata (key, identifier, name, precombine_key, record_key, partition_key)
            checkpoint_location (str): S3 or local path for checkpoint data (e.g., "s3a://bucket/checkpoints/")
            data_write_mode (str): "upsert", "bulk_insert", or "insert". Default: "upsert"
            table_write_mode (str): "append", "complete", or "update". Default: "append"
            hudi_options (dict, optional): Additional Hudi options

        Returns:
            pyspark.sql.streaming.StreamingQuery: Active streaming query object
        """

        s3_path = s3_path_generator(
            self.bucket,
            self.prefix,
            data_object.key,
        )

        stream_query = (
            data_stream.writeStream.format("hudi")
            .option("checkpointLocation", checkpoint_location)
            .option("path", s3_path)
            .trigger(processingTime="10 seconds")
            .option("outputMode", table_write_mode)
            .option("queryName", f"{data_object.identifier}")
            .option("hoodie.datasource.write.operation", data_write_mode)
            .option("hoodie.table.name", data_object.name.lower())
            .option("hoodie.datasource.write.precombine.field", data_object.precombine_key)
            .option("hoodie.datasource.write.recordkey.field", data_object.record_key)
            .option("hoodie.datasource.write.partitionpath.field", data_object.partition_key)
            .options(**hudi_options)
            .start()
        )
        return stream_query
