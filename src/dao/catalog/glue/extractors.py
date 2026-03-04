# extractors.py
# Built-in FieldExtractor implementations for AWS Glue GetTable responses.

from typing import Any, Callable, Dict


class FieldExtractor:
    """Base class for field extractors.

    Subclass this or satisfy the same duck-typed protocol (``name`` attribute
    + ``extract(table)`` method).

    Return ``None`` from :meth:`extract` to signal that the field is absent
    and should be omitted from the resulting properties dict.
    """

    name: str = ""

    def extract(self, table: Dict[str, Any]) -> Any:
        """Return extracted value, or ``None`` to skip."""
        return None  # pragma: no cover


class LocationExtractor(FieldExtractor):
    """``StorageDescriptor.Location`` → ``location``."""

    name = "location"

    def extract(self, table: Dict[str, Any]) -> Any:
        sd = table.get("StorageDescriptor", {})
        return sd.get("Location")


class ColumnsExtractor(FieldExtractor):
    """``StorageDescriptor.Columns`` → ``columns`` (list of dicts)."""

    name = "columns"

    def extract(self, table: Dict[str, Any]) -> Any:
        sd = table.get("StorageDescriptor", {})
        return sd.get("Columns")


class PartitionKeysExtractor(FieldExtractor):
    """``PartitionKeys`` → ``partition_keys``."""

    name = "partition_keys"

    def extract(self, table: Dict[str, Any]) -> Any:
        keys = table.get("PartitionKeys")
        return keys if keys else None


class ClassificationExtractor(FieldExtractor):
    """``Parameters.classification`` → ``classification``."""

    name = "classification"

    def extract(self, table: Dict[str, Any]) -> Any:
        return table.get("Parameters", {}).get("classification")


class InputFormatExtractor(FieldExtractor):
    """``StorageDescriptor.InputFormat`` → ``input_format``."""

    name = "input_format"

    def extract(self, table: Dict[str, Any]) -> Any:
        sd = table.get("StorageDescriptor", {})
        return sd.get("InputFormat")


class OutputFormatExtractor(FieldExtractor):
    """``StorageDescriptor.OutputFormat`` → ``output_format``."""

    name = "output_format"

    def extract(self, table: Dict[str, Any]) -> Any:
        sd = table.get("StorageDescriptor", {})
        return sd.get("OutputFormat")


class SerDeExtractor(FieldExtractor):
    """``StorageDescriptor.SerdeInfo`` → ``serde_info``."""

    name = "serde_info"

    def extract(self, table: Dict[str, Any]) -> Any:
        sd = table.get("StorageDescriptor", {})
        return sd.get("SerdeInfo")


class TableTypeExtractor(FieldExtractor):
    """``TableType`` → ``table_type``."""

    name = "table_type"

    def extract(self, table: Dict[str, Any]) -> Any:
        return table.get("TableType")


class CompressedExtractor(FieldExtractor):
    """``StorageDescriptor.Compressed`` → ``compressed``."""

    name = "compressed"

    def extract(self, table: Dict[str, Any]) -> Any:
        sd = table.get("StorageDescriptor", {})
        return sd.get("Compressed")


class RawParametersExtractor(FieldExtractor):
    """``Parameters`` (full dict) → ``parameters``."""

    name = "parameters"

    def extract(self, table: Dict[str, Any]) -> Any:
        params = table.get("Parameters")
        if params is None:
            return None
        return dict(params)


class ObjectTypeExtractor(FieldExtractor):
    """Map Glue ``TableType`` to a dao :class:`DataObject` subclass name.

    The extracted value is stored under the ``"type"`` key which
    :meth:`BaseCatalog.get` uses to look up the correct class from the
    :class:`~dao.data_object.registry.DataObjectRegistry`.
    """

    name = "type"

    MAPPING: Dict[str, str] = {
        "EXTERNAL_TABLE": "TableObject",
        "MANAGED_TABLE": "TableObject",
        "VIRTUAL_VIEW": "DataObject",
        "GOVERNED": "TableObject",
    }

    def extract(self, table: Dict[str, Any]) -> Any:
        table_type = table.get("TableType", "")
        return self.MAPPING.get(table_type, "DataObject")


class _LambdaExtractor(FieldExtractor):
    """Adapter that wraps a plain callable as a :class:`FieldExtractor`."""

    def __init__(self, name: str, func: Callable[[Dict[str, Any]], Any]):
        self.name = name
        self._func = func

    def extract(self, table: Dict[str, Any]) -> Any:
        return self._func(table)

    def __repr__(self) -> str:
        return f"LambdaExtractor({self.name!r})"


def lambda_extractor(name: str, func: Callable[[Dict[str, Any]], Any]) -> FieldExtractor:
    """Create a :class:`FieldExtractor` from a name and a callable.

    Example::

        owner = lambda_extractor(
            "owner",
            lambda t: t.get("Parameters", {}).get("owner"),
        )
    """
    return _LambdaExtractor(name, func)
