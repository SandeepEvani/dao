# translator.py
# Translates a raw Glue GetTable response into a flat properties dict
# suitable for constructing a DataObject (or subclass).

from typing import Any, Dict, List, Optional

from .extractors import (
    ClassificationExtractor,
    ColumnsExtractor,
    CompressedExtractor,
    FieldExtractor,
    InputFormatExtractor,
    LocationExtractor,
    ObjectTypeExtractor,
    OutputFormatExtractor,
    PartitionKeysExtractor,
    RawParametersExtractor,
    SerDeExtractor,
    TableTypeExtractor,
    lambda_extractor,
)

__all__ = [
    "GlueTableTranslator",
    "lambda_extractor",
]


class GlueTableTranslator:
    """Translates a Glue ``GetTable`` response into :class:`DataObject` properties.

    Ships with sensible defaults (location, columns, partition_keys,
    classification).  Users can customize via :meth:`include`, :meth:`exclude`,
    :meth:`only`, or by supplying their own list of extractors at init time.

    All mutator methods return ``self`` so they can be chained::

        translator = (
            GlueTableTranslator()
            .include(InputFormatExtractor(), SerDeExtractor())
            .exclude("partition_keys")
        )
    """

    def __init__(self, extractors: Optional[List[FieldExtractor]] = None):
        if extractors is not None:
            self._extractors: Dict[str, FieldExtractor] = {e.name: e for e in extractors}
        else:
            self._extractors = self._default_extractors()

    def include(self, *extractors: FieldExtractor) -> "GlueTableTranslator":
        """Add or replace extractors."""
        for e in extractors:
            self._extractors[e.name] = e
        return self

    def exclude(self, *names: str) -> "GlueTableTranslator":
        """Remove extractors by name."""
        for n in names:
            self._extractors.pop(n, None)
        return self

    def only(self, *names: str) -> "GlueTableTranslator":
        """Keep *only* the named extractors, dropping everything else."""
        self._extractors = {n: e for n, e in self._extractors.items() if n in names}
        return self

    def translate(self, table: Dict[str, Any]) -> Dict[str, Any]:
        """Run every registered extractor and return a flat properties dict.

        Extractors that return ``None`` are silently skipped.
        """
        props: Dict[str, Any] = {}
        for extractor in self._extractors.values():
            value = extractor.extract(table)
            if value is None:
                continue
            props[extractor.name] = value
        return props

    @property
    def extractor_names(self) -> list:
        """Return a sorted list of currently-active extractor names."""
        return sorted(self._extractors.keys())

    @classmethod
    def minimal(cls) -> "GlueTableTranslator":
        """Only ``location`` + ``classification``."""
        return cls().only("location", "classification")

    @classmethod
    def schema_aware(cls) -> "GlueTableTranslator":
        """Defaults + input/output format + serde — for schema-validation pipelines."""
        return cls().include(InputFormatExtractor(), OutputFormatExtractor(), SerDeExtractor())

    @classmethod
    def full(cls) -> "GlueTableTranslator":
        """Every built-in extractor enabled."""
        return cls(
            extractors=[
                LocationExtractor(),
                ColumnsExtractor(),
                PartitionKeysExtractor(),
                ClassificationExtractor(),
                InputFormatExtractor(),
                OutputFormatExtractor(),
                SerDeExtractor(),
                TableTypeExtractor(),
                CompressedExtractor(),
                RawParametersExtractor(),
                ObjectTypeExtractor(),
            ]
        )

    @staticmethod
    def _default_extractors() -> Dict[str, FieldExtractor]:
        return {
            e.name: e
            for e in [
                LocationExtractor(),
                ColumnsExtractor(),
                PartitionKeysExtractor(),
                ClassificationExtractor(),
            ]
        }
