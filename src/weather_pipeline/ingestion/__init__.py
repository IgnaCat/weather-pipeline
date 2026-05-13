"""Módulo de ingesta — clases para descargar datos de distintas fuentes."""

from weather_pipeline.ingestion.base import (
    DataNotFoundError,
    IngestorBase,
    IngestorError,
    IngestStats,
    ParseError,
    RateLimitError,
)
from weather_pipeline.ingestion.noaa_nexrad import (
    NEXRADFile,
    NEXRADMetadata,
    NOAANEXRADIngestor,
)

__all__ = [
    "IngestorBase",
    "IngestorError",
    "DataNotFoundError",
    "ParseError",
    "RateLimitError",
    "IngestStats",
    "NOAANEXRADIngestor",
    "NEXRADFile",
    "NEXRADMetadata",
]
