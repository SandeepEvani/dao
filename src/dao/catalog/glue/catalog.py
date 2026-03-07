# catalog.py
# AWS Glue Data Catalog implementation with translator-based metadata extraction.

import json
from typing import Any, Dict, Optional

import boto3

from dao.catalog.base import BaseCatalog

from .translator import GlueTableTranslator


class GlueCatalog(BaseCatalog):
    """Catalog backed by the AWS Glue Data Catalog.

    **Metadata translation**

    By default a :class:`GlueTableTranslator` extracts ``location``,
    ``columns``, ``partition_keys``, and ``classification`` from every
    ``GetTable`` response.  You can swap in a different translator to
    control exactly which Glue metadata lands on each :class:`DataObject`::

        from dao.catalog.glue import GlueCatalog
        from dao.catalog.glue.translator import GlueTableTranslator

        catalog = GlueCatalog(
            aws_profile="dev",
            translator=GlueTableTranslator.full(),
        )

    Any ``data_object_config`` JSON blob stored in the table's Parameters
    is **overlaid** on top of the translated values (user config wins).

    **Object-type resolution**

    If the resulting properties contain a ``"type"`` key (either from the
    translator or from ``data_object_config``), :meth:`BaseCatalog.get`
    resolves it via the :class:`~dao.data_object.registry.DataObjectRegistry`.
    Include :class:`~dao.catalog.glue.extractors.ObjectTypeExtractor` in the
    translator (or use ``GlueTableTranslator.full()``) to have the Glue
    ``TableType`` automatically mapped to the right subclass.

    **Caching**

    Resolved properties are cached indefinitely once fetched.  Call
    :meth:`refresh` to evict a specific entry or clear the entire cache.

    **Authentication precedence**

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
        translator: Optional[GlueTableTranslator] = None,
    ) -> None:
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

        self._translator = translator or GlueTableTranslator()
        self._cache: Dict[str, Dict[str, Any]] = {}

        super().__init__()

    def _load_data_store_configs(self) -> Dict[str, Any]:
        """Fetch data-store configs from Glue databases with ``data_store_config`` parameter."""
        response = self._glue.get_databases()
        return {
            db["Name"]: _deserialize_config(db["Parameters"]["data_store_config"])
            for db in response["DatabaseList"]
            if "data_store_config" in db.get("Parameters", {})
        }

    def _load_data_object_configs(self) -> Dict[str, Any]:
        """Glue fetches data-object configs on demand — return empty dict."""
        return {}

    def _resolve_data_object_properties(self, data_store: str, data_object: str) -> Dict[str, Any]:
        """Resolve properties via translator + optional user overlay.

        Results are cached after the first call.  Use :meth:`refresh` to
        force a fresh ``GetTable`` call.
        """
        cache_key = f"{data_store}.{data_object}"

        if cache_key in self._cache:
            return dict(self._cache[cache_key])

        response = self._glue.get_table(DatabaseName=data_store, Name=data_object)
        table = response["Table"]

        # Phase 1 — translator extracts crawler-discovered metadata
        props = self._translator.translate(table)

        # Phase 2 — overlay explicit user config (always wins)
        user_config_str = table.get("Parameters", {}).get("data_object_config")
        if user_config_str:
            props.update(_deserialize_config(user_config_str))

        self._cache[cache_key] = dict(props)
        return props

    def refresh(self, fully_qualified_name: Optional[str] = None) -> None:
        """Invalidate cached ``GetTable`` results.

        Args:
            fully_qualified_name: If given (``"store.object"``), only that
                entry is evicted.  Otherwise the entire cache is cleared.
        """
        if fully_qualified_name is not None:
            self._cache.pop(fully_qualified_name, None)
        else:
            self._cache.clear()


def _serialize_config(config: dict, **kwargs: Any) -> str:
    """Serialize a config dict/list to a JSON string for storage in Glue parameters."""
    try:
        kwargs.setdefault("sort_keys", True)
        return json.dumps(config, **kwargs)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Failed to serialize config: {e}") from e


def _deserialize_config(config_str: str) -> Any:
    """Deserialize a JSON string from Glue parameters back to a config dict/list."""
    try:
        return json.loads(config_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to deserialize config string: {e}") from e
