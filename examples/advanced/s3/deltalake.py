# delta.py
# includes writer and reader methods for delta tables

import re
from functools import lru_cache

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from dao import register
from dao.data_object import S3DeltaObject


class S3DeltaInterface:
    global_delta_options = {}

    def __init__(self, bucket: str, prefix=None, **kwargs):
        """
        Initializes the delta class for DAO
        Args:
              bucket: The name of the bucket for the data store
              prefix: optional: The subsegment in which the actual delta tables are present
            **kwargs: Additional properties for the delta class
        """
        self.bucket = bucket
        self.prefix = prefix

        for key, value in kwargs.items():
            setattr(self, key, value)

    def write_dataframe_to_delta(self, data: DataFrame, data_object: S3DeltaObject, **delta_options) -> bool:
        """
        Creates a new Delta table in S3 with the required table properties
        Args:
            data:
            data_object:
            delta_options:

        Returns:
        """

        s3_path = self._s3_path_generator(self.bucket, self.prefix, data_object.key)

        net_delta_options = {**self.global_delta_options, **delta_options}

        # Pop required properties from the net options
        partition_keys = data_object.partition_keys
        clustering_keys = data_object.cluster_keys

        mode = net_delta_options.pop("mode", "overwrite")

        if partition_keys and clustering_keys:
            raise Exception("partition_on & cluster_on can't be passed together!")

        # Removes any special characters from the column names
        dataframe = self._rename_columns(data)

        # Creates the writer object
        writer_obj = dataframe.write.format("delta").options(**net_delta_options).option("path", s3_path).mode(mode)

        # adds partition or clustering based on the properties given
        if partition_keys:
            writer_obj = writer_obj.partitionBy(*self._format_column_name(partition_keys).split(","))
        elif clustering_keys:
            writer_obj = writer_obj.clusterBy(*self._format_column_name(clustering_keys).split(","))

        # Saves the table to the S3 path
        writer_obj.save()

        return True

    def read_delta_table(self, data_object, columns="*", **delta_options) -> DataFrame:
        """
        Creates a new Delta table in S3 with the required table properties
        Args:
            columns:
            data_object:
            delta_options:

        Returns:
        """

        s3_path = self._s3_path_generator(self.bucket, self.prefix, data_object.prefix, data_object.source)

        spark_session = SparkSession.getActiveSession()

        if not spark_session:
            raise Exception("No active Spark Session found")

        if columns != "*":
            # Reading the Data
            dataframe = spark_session.read.format("delta").load(s3_path).select(*columns)
        else:
            dataframe = spark_session.read.format("delta").load(s3_path)

        # Returning the DataFrame
        return dataframe

    def _generate_merge_condition(self, table):
        """Generates the merge condition from the primary keys of the table
        object."""

        primary_keys = table.primary_keys.split(",")

        return " and ".join(
            [
                f"target.{self._format_column_name(column)} = source.{self._format_column_name(column)}"
                for column in primary_keys
            ]
        )

    @register(mode="read")
    def generate_delta_table_object(self, data_object, return_delta_object):
        """
        Creates a new Delta table in S3 with the required table properties
        Args:
            data_object:
            return_delta_object:

        Returns:
        """

        spark_session = SparkSession.getActiveSession()

        if not spark_session:
            raise Exception("No active Spark Session found")

        s3_path = self._s3_path_generator(self.bucket, self.prefix, data_object.prefix, data_object.source)

        delta_table = DeltaTable.forPath(spark_session, s3_path)
        return delta_table

    def merge_delta_to_s3(self, data, data_object, **delta_options) -> bool:
        """
        Merges the given dataframe into the existing delta table
        based on the primary keys, a default operation of `whenMatchedUpdateAll`
        and `whenNotMatchedInsertAll` is performed
        Args:
            data:
            data_object:

        Returns: bool
        """

        spark_session = data.sparkSession or SparkSession.getActiveSession()

        if not spark_session:
            raise Exception("No active Spark Session found")

        spark_session.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

        s3_path = self._s3_path_generator(self.bucket, self.prefix, data_object.prefix, data_object.source)

        # Create the delta table pointer into which we should merge the table
        delta_table = DeltaTable.forPath(spark_session, s3_path)

        # Removes any special characters from the column names
        dataframe = self._rename_columns(data)

        # The default merger
        delta_table.alias("target").merge(
            dataframe.alias("source"), self._generate_merge_condition(data_object)
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        return True

    @lru_cache
    def _format_column_name(self, columns):
        """Formats the column names by removing special characters."""
        columns = columns.split(",")
        return ",".join([re.sub("\W+", "_", column).strip("_").lower() for column in columns])

    def _rename_columns(self, dataframe):
        """Formats the column names by removing special characters in a
        dataframe."""

        return dataframe.select(*[f.col(f"`{c}`").alias(self._format_column_name(c)) for c in dataframe.columns])

    @staticmethod
    def _s3_path_generator(*paths, s3_prefix="s3a"):
        """Generates a s3 path for reading data based on the required
        parameters."""

        return s3_prefix + "://" + "/".join([prefix for prefix in paths if prefix != "" and prefix is not None])
