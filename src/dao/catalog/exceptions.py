class CatalogException(Exception):
    """Base exception for catalog-related errors."""


class DataStoreNotFoundError(CatalogException):
    """Raised when a data store is not found in the catalog."""


class DataObjectNotFoundError(CatalogException):
    """Raised when a data object is not found in the catalog."""


class InvalidCatalogFormatError(CatalogException):
    """Raised when catalog format is invalid."""
