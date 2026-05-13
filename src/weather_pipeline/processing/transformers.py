"""
Módulo de transformación de datos — Pipeline Bronze → Silver → Gold.

Bronze: archivos binarios crudos de NEXRAD tal cual los descargamos.
Silver: DataFrame Polars limpio, tipado, con timestamps UTC y sin duplicados.
Gold:   métricas agregadas listas para análisis y visualización.

Cada transformación es una función pura: recibe datos, devuelve datos.
No tiene efectos secundarios ni escribe archivos — eso lo hace storage/.
"""

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
from loguru import logger

from weather_pipeline.ingestion.noaa_nexrad import NEXRADMetadata, NOAANEXRADIngestor
from weather_pipeline.processing.schemas import GOLD_SCHEMA, SILVER_SCHEMA

# ---------------------------------------------------------------------------
# Bronze → Silver
# ---------------------------------------------------------------------------


def raw_files_to_silver(raw_paths: list[Path]) -> pl.DataFrame:
    """
    Transforma archivos NEXRAD crudos (Bronze) a un DataFrame limpio (Silver).

    Para cada archivo:
    1. Parsea el header binario para extraer metadata
    2. Normaliza el timestamp a UTC
    3. Agrega columnas de particionado (year, month, day)

    Args:
        raw_paths: Lista de paths a archivos NEXRAD Level II descargados.

    Returns:
        DataFrame Polars con schema SILVER_SCHEMA.
        Vacío si no hay archivos válidos.
    """
    if not raw_paths:
        logger.warning("raw_files_to_silver: lista de archivos vacía")
        return pl.DataFrame(schema=SILVER_SCHEMA)

    ingestor = NOAANEXRADIngestor()
    records: list[dict[str, object]] = []

    for path in raw_paths:
        try:
            meta: NEXRADMetadata = ingestor.parse_metadata(path)
            records.append(_metadata_to_record(meta, path))
            logger.debug(f"Parseado: {path.name} → {meta.scan_datetime}")
        except Exception as exc:
            # No fallar por un archivo corrupto — loguear y seguir
            logger.warning(f"Error parseando {path.name}: {exc}")
            continue

    if not records:
        logger.error("No se pudo parsear ningún archivo")
        return pl.DataFrame(schema=SILVER_SCHEMA)

    # Construir DataFrame
    df = pl.DataFrame(records, schema=SILVER_SCHEMA)

    # Aplicar transformaciones de limpieza
    df = _clean_silver(df)

    logger.info(
        f"Silver: {len(df)} registros válidos de {len(raw_paths)} archivos "
        f"({len(raw_paths) - len(df)} descartados)"
    )
    return df


def _metadata_to_record(meta: NEXRADMetadata, path: Path) -> dict[str, object]:
    """Convierte un NEXRADMetadata a un dict compatible con SILVER_SCHEMA."""
    dt = meta.scan_datetime

    # Asegurar que el datetime siempre sea UTC
    dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)

    return {
        "station_id": meta.station_id,
        "scan_datetime": dt,
        "year": dt.year,
        "month": dt.month,
        "day": dt.day,
        "hour": dt.hour,
        "volume_number": meta.volume_number,
        "format_version": meta.version,
        "file_path": str(path),
        "file_size_bytes": path.stat().st_size,
        "ingested_at": datetime.now(tz=UTC),
    }


def _clean_silver(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica limpieza estándar al DataFrame Silver:
    - Elimina duplicados por (station_id, scan_datetime)
    - Descarta registros con station_id vacío o nulo
    - Ordena por scan_datetime
    """
    n_before = len(df)

    df = (
        df
        # Eliminar station_id inválidos
        .filter(pl.col("station_id").is_not_null() & (pl.col("station_id").str.len_chars() == 4))
        # Eliminar duplicados exactos (mismo archivo procesado dos veces)
        .unique(subset=["station_id", "scan_datetime"], keep="first")
        # Ordenar cronológicamente
        .sort("scan_datetime")
    )

    n_removed = n_before - len(df)
    if n_removed > 0:
        logger.info(f"Limpieza Silver: {n_removed} registros eliminados (duplicados/inválidos)")

    return df


# ---------------------------------------------------------------------------
# Silver → Gold
# ---------------------------------------------------------------------------


def silver_to_gold(silver_df: pl.DataFrame) -> pl.DataFrame:
    """
    Genera métricas agregadas (Gold) a partir del DataFrame Silver.

    Produce agregaciones por estación y hora:
    - Cantidad de escaneos por hora (indica operatividad del radar)
    - Gap máximo entre escaneos consecutivos (detecta interrupciones)
    - Hora del primer y último escaneo del día

    Args:
        silver_df: DataFrame Silver con schema SILVER_SCHEMA.

    Returns:
        DataFrame Gold con schema GOLD_SCHEMA.
    """
    if silver_df.is_empty():
        logger.warning("silver_to_gold: DataFrame Silver vacío")
        return pl.DataFrame(schema=GOLD_SCHEMA)

    # Calcular tiempo entre escaneos consecutivos (por estación)
    # Esto detecta "gaps" — períodos donde el radar no escaneó
    df_with_gaps = _calculate_scan_gaps(silver_df)

    # Agregar por estación + hora
    gold_df = (
        df_with_gaps.group_by(["station_id", "year", "month", "day", "hour"])
        .agg(
            [
                pl.len().alias("scan_count"),
                pl.col("scan_datetime").min().alias("first_scan"),
                pl.col("scan_datetime").max().alias("last_scan"),
                pl.col("gap_minutes").max().alias("max_gap_minutes"),
                pl.col("gap_minutes").mean().alias("avg_gap_minutes"),
                pl.col("file_size_bytes").sum().alias("total_bytes"),
            ]
        )
        .sort(["station_id", "year", "month", "day", "hour"])
    )

    logger.info(
        f"Gold: {len(gold_df)} filas agregadas "
        f"({silver_df['station_id'].n_unique()} estaciones)"
    )
    return gold_df


def _calculate_scan_gaps(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calcula el tiempo en minutos entre cada escaneo y el anterior,
    separado por estación (para no mezclar gaps entre estaciones distintas).

    Ejemplo:
        scan_datetime    gap_minutes
        10:00            null          ← primer escaneo, no tiene anterior
        10:06            6.0
        10:12            6.0
        10:30            18.0          ← gap largo, posible interrupción
    """
    return df.sort(["station_id", "scan_datetime"]).with_columns(
        [
            # diff() calcula la diferencia con el valor anterior dentro del grupo
            pl.col("scan_datetime")
            .diff()
            .over("station_id")  # aplicar separado por cada estación
            .dt.total_minutes()  # convertir duración a minutos
            .cast(pl.Float64)
            .alias("gap_minutes")
        ]
    )


# ---------------------------------------------------------------------------
# Helpers de validación
# ---------------------------------------------------------------------------


def validate_silver(df: pl.DataFrame) -> dict[str, object]:
    """
    Validaciones básicas de calidad de datos sobre el DataFrame Silver.

    Returns:
        Dict con el resultado de cada chequeo.
        En Fase 4+ esto se integraría con Great Expectations.
    """
    checks: dict[str, object] = {}

    # Hay datos?
    checks["has_rows"] = len(df) > 0

    # Todos los station_id tienen exactamente 4 caracteres?
    checks["valid_station_ids"] = df.filter(pl.col("station_id").str.len_chars() != 4).is_empty()

    # Todos los timestamps son UTC (tienen timezone)?
    # Polars representa timezone-aware datetimes como Datetime("us", "UTC")
    checks["timestamps_are_utc"] = str(df.schema.get("scan_datetime")) in (
        "Datetime(time_unit='us', time_zone='UTC')",
        "Datetime(time_unit='ms', time_zone='UTC')",
    )

    # Hay duplicados?
    n_unique = df.unique(subset=["station_id", "scan_datetime"]).height
    checks["no_duplicates"] = n_unique == len(df)

    # El gap máximo es razonable? (NEXRAD escanea cada ~5-6 minutos)
    # Un gap > 60 min podría indicar datos corruptos o radar offline
    if "gap_minutes" in df.columns:
        max_gap = df["gap_minutes"].drop_nulls().max()
        checks["max_gap_ok"] = max_gap is None or float(max_gap) <= 60.0

    # Resumen
    passed = sum(1 for v in checks.values() if v is True)
    total = len(checks)
    checks["_summary"] = f"{passed}/{total} checks pasaron"

    if passed < total:
        failed = [k for k, v in checks.items() if v is False]
        logger.warning(f"Validación Silver: fallaron {failed}")
    else:
        logger.info("Validación Silver: todos los checks pasaron")

    return checks


def summarize_day(silver_df: pl.DataFrame, target_date: date) -> str:
    """
    Genera un resumen en texto del día procesado. Útil para logging y reportes.

    Args:
        silver_df: DataFrame Silver del día.
        target_date: Fecha procesada.

    Returns:
        String con el resumen.
    """
    if silver_df.is_empty():
        return f"{target_date}: sin datos"

    stations = silver_df["station_id"].unique().to_list()
    total_scans = len(silver_df)
    time_range = f"{silver_df['scan_datetime'].min()} → {silver_df['scan_datetime'].max()}"

    return (
        f"Fecha: {target_date} | "
        f"Estaciones: {', '.join(sorted(stations))} | "
        f"Escaneos: {total_scans} | "
        f"Rango: {time_range}"
    )
