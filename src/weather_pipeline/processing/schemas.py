"""
Schemas de datos para cada capa del pipeline.

Definir los schemas explícitamente tiene tres ventajas:
1. Documentación viva: cualquiera puede ver qué columnas tiene cada DataFrame.
2. Validación automática: Polars lanza error si los datos no coinciden.
3. Consistencia: Bronze, Silver y Gold siempre tienen la misma estructura.
"""

import polars as pl

# ---------------------------------------------------------------------------
# Silver — datos limpios y tipados
# ---------------------------------------------------------------------------
# Una fila = un escaneo volumétrico de radar

SILVER_SCHEMA = {
    "station_id": pl.Utf8,  # "KBUF", "KOKX", etc.
    "scan_datetime": pl.Datetime("us", "UTC"),  # timestamp UTC del escaneo
    "year": pl.Int32,  # para particionado
    "month": pl.Int32,
    "day": pl.Int32,
    "hour": pl.Int32,
    "volume_number": pl.Int32,  # número de volumen del día
    "format_version": pl.Utf8,  # "AR2V0006.", etc.
    "file_path": pl.Utf8,  # path local del archivo
    "file_size_bytes": pl.Int64,  # tamaño en bytes
    "ingested_at": pl.Datetime("us", "UTC"),  # cuándo lo procesamos
}

# ---------------------------------------------------------------------------
# Gold — métricas agregadas por estación y hora
# ---------------------------------------------------------------------------
# Una fila = una hora de operación de una estación

GOLD_SCHEMA = {
    "station_id": pl.Utf8,
    "year": pl.Int32,
    "month": pl.Int32,
    "day": pl.Int32,
    "hour": pl.Int32,
    "scan_count": pl.UInt32,  # escaneos en esa hora
    "first_scan": pl.Datetime("us", "UTC"),  # primer escaneo
    "last_scan": pl.Datetime("us", "UTC"),  # último escaneo
    "max_gap_minutes": pl.Float64,  # gap más largo (minutos)
    "avg_gap_minutes": pl.Float64,  # gap promedio (minutos)
    "total_bytes": pl.Int64,  # bytes procesados
}
