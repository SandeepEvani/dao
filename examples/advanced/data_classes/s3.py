class S3:
    """S3 interface class — primary interface for the 'raw' data store.

    Methods prefixed with 'write_' and 'read_' are automatically discovered
    by the DAO and registered in the route table.
    """

    def __init__(self, bucket=None, **kwargs):
        self.bucket = bucket

    def write_object(self, data, data_object, **kwargs):
        """Write an object to S3.

        Args:
            data: The data payload to write
            data_object: The target data object (table/file)
            **kwargs: Additional options (e.g., format, compression)
        """
        print(f"[S3] Writing to s3://{self.bucket}/{data_object.name}")
        print(f"  Data: {data}")
        return True

    def read_object(self, data_object, **kwargs):
        """Read an object from S3.

        Args:
            data_object: The source data object
            **kwargs: Additional options (e.g., filters)
        """
        print(f"[S3] Reading from s3://{self.bucket}/{data_object.name}")
        return {"source": f"s3://{self.bucket}/{data_object.name}", "rows": []}

