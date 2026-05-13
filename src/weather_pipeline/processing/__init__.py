"""Módulo de procesamiento — transformaciones Bronze → Silver → Gold con Polars."""

from weather_pipeline.processing.schemas import GOLD_SCHEMA, SILVER_SCHEMA
from weather_pipeline.processing.transformers import (
    raw_files_to_silver,
    silver_to_gold,
    summarize_day,
    validate_silver,
)

__all__ = [
    "SILVER_SCHEMA",
    "GOLD_SCHEMA",
    "raw_files_to_silver",
    "silver_to_gold",
    "validate_silver",
    "summarize_day",
]
