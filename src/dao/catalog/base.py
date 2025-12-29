# base.py
# The base class for catalogs

from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any, Dict, List, Optional

logger = getLogger(__name__)


class CatalogException(Exception):
    """Base exception for catalog-related errors."""

    pass


class DataStoreNotFoundError(CatalogException):
    """Raised when a data store is not found in the catalog."""

    pass


class DataObjectNotFoundError(CatalogException):
    """Raised when a data object is not found in the catalog."""

    pass


class InvalidCatalogFormatError(CatalogException):
    """Raised when catalog format is invalid."""

    pass


class BaseCatalog(ABC):
    """Base class for all catalog implementations.

    Provides common interface and shared functionality for catalogs.
    Specific catalog types should extend this class.

    The catalog follows a two-layer approach:
    1. Pre-defined data stores (provided during DAO initialization)
    2. Runtime data objects (provided on-demand from various sources)
    """

    def __init__(self, validate_on_init: bool = True):
        """Initialize the catalog.

        Args:
            validate_on_init: Whether to validate catalog on initialization.
        """
        self.validate_on_init = validate_on_init
        if validate_on_init:
            self.validate()

    @abstractmethod
    def get_data_stores(self) -> List[str]:
        """Retrieve all available data store names.

        Returns:
            A list of data store identifiers/names.

        Raises:
            CatalogException: If unable to retrieve data stores.
        """
        raise NotImplementedError("Subclasses must implement get_data_stores method.")

    @abstractmethod
    def get_data_objects(self, data_store: Optional[str] = None) -> List[str]:
        """Retrieve all data object names, optionally filtered by data store.

        Args:
            data_store: Optional filter for specific data store. If None, returns all objects.

        Returns:
            A list of data object identifiers/names.

        Raises:
            DataStoreNotFoundError: If specified data_store does not exist.
            CatalogException: If unable to retrieve data objects.
        """
        raise NotImplementedError("Subclasses must implement get_data_objects method.")

    @abstractmethod
    def get_data_object(self, fully_qualified_name: str) -> Dict[str, Any]:
        """Retrieve a specific data object configuration.

        Args:
            fully_qualified_name: Format should be 'data_store.object_name'

        Returns:
            A dictionary containing the data object configuration/metadata.

        Raises:
            ValueError: If fully_qualified_name format is invalid.
            DataStoreNotFoundError: If data store does not exist.
            DataObjectNotFoundError: If data object does not exist.
        """
        raise NotImplementedError("Subclasses must implement get_data_object method.")

    @abstractmethod
    def validate(self) -> bool:
        """Validate the catalog structure and contents.

        Returns:
            True if catalog is valid.

        Raises:
            InvalidCatalogFormatError: If catalog structure is invalid.
        """
        raise NotImplementedError("Subclasses must implement validate method.")

    def check_data_store_exists(self, data_store: str) -> bool:
        """Check if a data store exists in the catalog.

        Args:
            data_store: Name of the data store to check.

        Returns:
            True if data store exists, False otherwise.
        """
        return data_store in self.get_data_stores()

    def check_data_object_exists(self, fully_qualified_name: str) -> bool:
        """Check if a data object exists in the catalog.

        Args:
            fully_qualified_name: Fully qualified name in format 'data_store.object_name'

        Returns:
            True if data object exists, False otherwise.
        """
        try:
            self._parse_fully_qualified_name(fully_qualified_name)
            self.get_data_object(fully_qualified_name)
            return True
        except (ValueError, DataStoreNotFoundError, DataObjectNotFoundError):
            return False

    def _parse_fully_qualified_name(self, fully_qualified_name: str) -> tuple:
        """Parse and validate fully qualified name format.

        Args:
            fully_qualified_name: Format should be 'data_store.object_name'

        Returns:
            Tuple of (data_store, object_name)

        Raises:
            ValueError: If format is invalid.
        """
        if not isinstance(fully_qualified_name, str):
            raise ValueError(f"Expected string, got {type(fully_qualified_name)}")

        if "." not in fully_qualified_name:
            raise ValueError(
                f"Invalid fully qualified name '{fully_qualified_name}'. Expected format: 'data_store.object_name'"
            )

        parts = fully_qualified_name.split(".", 1)
        if len(parts) != 2 or not all(parts):
            raise ValueError(
                f"Invalid fully qualified name '{fully_qualified_name}'. "
                f"Both data_store and object_name must be non-empty"
            )

        return tuple(parts)

    def get_object_metadata(self, fully_qualified_name: str) -> Dict[str, Any]:
        """Retrieve complete metadata for a data object.

        Args:
            fully_qualified_name: Fully qualified name in format 'data_store.object_name'

        Returns:
            Dictionary containing object metadata.

        Raises:
            ValueError: If format is invalid.
            DataStoreNotFoundError: If data store does not exist.
            DataObjectNotFoundError: If data object does not exist.
        """
        data_store, object_name = self._parse_fully_qualified_name(fully_qualified_name)

        if not self.check_data_store_exists(data_store):
            raise DataStoreNotFoundError(
                f"Data store '{data_store}' not found. Available data stores: {self.get_data_stores()}"
            )

        return self.get_data_object(fully_qualified_name)
