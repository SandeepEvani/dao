# spark.py
# Contains code to read and write data to Redshift using Spark

from urllib.parse import quote

from pyspark.sql import DataFrame, SparkSession

from dao.data_object import TableObject

from .odbc import RedshiftConnector

REDSHIFT_DRIVER = "io.github.spark_redshift_community.spark.redshift"


class RedshiftSparkInterface:
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        s3_temp_dir: str,
        iam_role_arn: str,
        port: int = 5439,
    ):
        """

        :param host:
        :param user:
        :param password:
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.s3_temp_dir = s3_temp_dir
        self.iam_role_arn = iam_role_arn

    @property
    def jdbc_url(self) -> str:
        """Construct the JDBC URL for Redshift connection.

        :return: JDBC URL string.
        """
        return (
            f"jdbc:redshift://{self.host}:{self.port}/{self.database}"
            f"?user={quote(self.user)}&password={quote(self.password)}"
        )

    def read_data(self, data_object: TableObject, **options) -> DataFrame:
        """Read data from a Redshift table into a Spark DataFrame.

        Args:
            data_object (TableObject): Table metadata object containing at least an `identifier` (e.g., "schema.table").
            **options: Optional backend-specific options forwarded to the reader.

        Returns:
            pyspark.sql.DataFrame: Spark DataFrame loaded from the Redshift table.

        Raises:
            RuntimeError: If no active Spark session is available.

        Notes:
            - Uses JDBC driver configured via `jdbc_url` and Redshift-specific options
              such as `s3_temp_dir` and `iam_role_arn`.
            - The returned DataFrame is the raw table contents; further transformations
              should be applied by caller code.
        """

        spark_session = SparkSession.getActiveSession()

        df = (
            spark_session.read.format(REDSHIFT_DRIVER)
            .option("url", self.jdbc_url)
            .option("dbtable", data_object.identifier)
            .option("tempdir", self.s3_temp_dir)
            .option("aws_iam_role", self.iam_role_arn)
            .load()
        )

        return df

    def write_data(self, data: DataFrame, data_object: TableObject, **options) -> bool:
        """Write data from a Spark DataFrame to a Redshift table

        Args:
            data: Spark dataframe to save in redshift
            data_object: Euclidean table object containing required metadata

        Returns: boolean
        """

        (
            data.write.format(REDSHIFT_DRIVER)
            .option("url", self.jdbc_url)
            .option("dbtable", data_object.identifier)
            .option("tempdir", self.s3_temp_dir)
            .option("aws_iam_role", self.iam_role_arn)
            .mode("append")
            .save()
        )

        return True

    def upsert_dataframe_to_redshift(self, data: DataFrame, data_object: TableObject) -> bool:
        """Upsert `dataframe` into `table` using a staging table in Redshift.

        - Writes the dataframe to a staging table in Redshift (overwrite).
        - Runs a delete/insert transaction to upsert into the target table.
        """
        stage_table = f"{data_object.identifier}_stage_table"

        # write staging table to Redshift
        (
            data.write.format(REDSHIFT_DRIVER)
            .option("url", self.jdbc_url)
            .option("dbtable", stage_table)
            .option("tempdir", self.s3_temp_dir)
            .option("aws_iam_role", self.iam_role_arn)
            .mode("overwrite")
            .save()
        )

        # build WHERE condition from record_key columns
        keys = [c.strip() for c in data_object.primary_keys.split(",")]
        where_condition = " AND ".join(f"{stage_table}.{k} = {data_object.identifier}.{k}" for k in keys)

        query = (
            f"begin; "
            f"delete from {data_object.name} using {stage_table} where {where_condition}; "
            f"insert into {data_object.name} select * from {stage_table}; "
            f"drop table {stage_table}; "
            f"end;"
        )

        redshift_connection = RedshiftConnector(
            host=self.host,
            username=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            iam_role=self.iam_role_arn,
        )
        redshift_connection.execute_query(query)

        return True
