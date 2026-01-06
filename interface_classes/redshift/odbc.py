# odbc.py
# Contains Redshift connector class using redshift-connector to connect and execute statement in redshift

import logging
from typing import Optional

import redshift_connector


def get_secret(secret_name: str) -> dict[str, str]:
    """
    Fetches the secrets from secret manager
    Args:
        secret_name->str: name of secrets
    Returns: dict
    """
    import json

    import boto3

    secrets_manager_client = boto3.client("secretsmanager")
    resp = secrets_manager_client.get_secret_value(SecretId=secret_name)
    secret_str = resp["SecretString"]
    secret_dict = json.loads(secret_str)
    return secret_dict


class RedshiftConnector:
    """
    Simple Redshift connector wrapper using psycopg2 (works well with Glue).
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        iam_role: str,
        credential_secret: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        Initialize Redshift connection parameters.

        Credentials can be provided either via AWS Secrets Manager or directly.
        For local development, direct credentials (username/password) can be used.
        For production, credential_secret should be used for security.

        Args:
            host: Redshift cluster endpoint
            port: Redshift port (usually 5439)
            database: Database name
            credential_secret: AWS Secret name containing username and password (optional)
            username: Direct username (optional, for local dev)
            password: Direct password (optional, for local dev)

        Raises:
            ValueError: If neither credential_secret nor username/password are provided
        """

        # Set up logging first
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.host = host
        self.port = port
        self.database = database
        self.connection = None
        self.iam_role = iam_role

        # Get credentials from either Secrets Manager or direct parameters
        if username and password:
            # Direct credentials provided
            self.user = username
            self.password = password
            self.logger.info("Using direct credentials for Redshift connection")

        elif credential_secret:
            # Fetch credentials from AWS Secrets Manager (production)
            self.logger.info(f"Fetching credentials from AWS Secrets Manager: {credential_secret}")
            secret_value = get_secret(credential_secret)
            self.user = secret_value["username"]
            self.password = secret_value["password"]
        else:
            raise ValueError("Must provide either 'credential_secret' or both 'username' and 'password'. ")

    def _get_connection(self):
        """Get or create a database connection."""
        if self.connection is None:
            try:
                self.connection = redshift_connector.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.__password,
                )
                self.logger.info("Successfully connected to Redshift")
            except Exception as e:
                self.logger.error(f"Error connecting to Redshift: {str(e)}")
                raise e
        return self.connection

    @property
    def password(self):
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute 'password'")

    @password.setter
    def password(self, value):
        self.__password = value

    def close(self):
        """Close database connection."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
            self.logger.info("Redshift connection closed.")
        except Exception as e:
            self.logger.error(f"Error closing connection: {str(e)}")

    def execute_query(self, query):
        """
        Execute a query on the prescribed database after creating a connection to the cluster.

        Args:
            query: Query that has to be run on the Cluster.

        Returns: bool.
        """

        connection = self._get_connection()
        cursor = connection.cursor()
        self.logger.info(f"Executing Query: {query}")
        try:
            # Executing the insert query for writing data
            cursor.execute(query)
            connection.commit()
            self.logger.info("Success")
        except (Exception, redshift_connector.DatabaseError) as error:
            self.logger.error("Error: %s" % error)
            connection.rollback()
            cursor.close()

        cursor.close()

        return True
