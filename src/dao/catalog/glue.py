# glue.py
# AWS Glue Data Catalog implementation

import json
from typing import Any, Dict, List, Optional, Union

import boto3

from dao.catalog.base import BaseCatalog


class GlueCatalog(BaseCatalog):
    """Catalog backed by the AWS Glue Data Catalog.

    Authentication precedence:
    1. ``aws_profile`` — boto3 named-profile session.
    2. ``aws_access_key_id`` / ``aws_secret_access_key`` — explicit keys.
    3. Neither — boto3 default credential chain.
    """

    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        aws_region: Optional[str] = None,
        aws_profile: Optional[str] = None,
    ):
        if aws_profile and aws_access_key_id:
            raise ValueError("Ambiguous authentication: supply either 'aws_profile' or 'aws_access_key_id', not both.")

        if aws_profile:
            session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
            self._glue = session.client("glue")
        else:
            self._glue = boto3.client(
                "glue",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=aws_region,
            )

        super().__init__()

    def _load_data_store_configs(self) -> Dict[str, Any]:
        """Fetch data-store configs from Glue databases with 'data_store_config' parameter."""

        response = self._glue.get_databases()
        return {
            db["Name"]: deserialize_config(db["Parameters"]["data_store_config"])
            for db in response["DatabaseList"]
            if "data_store_config" in db.get("Parameters", {})
        }

    def _load_data_object_configs(self) -> Dict[str, Any]:
        """Glue fetches data-object configs on demand — return empty dict."""
        return {}

    def _resolve_data_object_properties(self, data_store: str, data_object: str) -> Dict[str, Any]:
        response = self._glue.get_table(DatabaseName=data_store, Name=data_object)
        table = response["Table"]
        return deserialize_config(table["Parameters"].get("data_object_config", "{}"))


def serialize_config(config: Union[Dict, List[Dict]], **kwargs) -> str:
    """Serialize a config dict/list to a JSON string for storage in Glue parameters."""
    try:
        kwargs.setdefault("sort_keys", True)
        return json.dumps(config, **kwargs)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Failed to serialize config: {e}") from e


def deserialize_config(config_str: str) -> Union[Dict, List[Dict]]:
    """Deserialize a JSON string from Glue parameters back to a config dict/list."""
    try:
        return json.loads(config_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to deserialize config string: {e}") from e
