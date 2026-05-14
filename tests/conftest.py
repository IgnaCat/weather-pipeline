"""
Fixtures compartidas entre todos los tests.

conftest.py es un archivo especial de pytest: se carga automáticamente
antes de los tests, sin necesidad de importarlo. Cualquier fixture
definida acá está disponible en todos los archivos de tests.
"""

import struct
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from weather_pipeline.processing.schemas import GOLD_SCHEMA, SILVER_SCHEMA

# ---------------------------------------------------------------------------
# Fixtures de DataFrames
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_silver_df() -> pl.DataFrame:
    """
    DataFrame Silver de ejemplo con 6 escaneos de la estación KBUF.
    Cubre dos horas del día para poder probar agregaciones.
    """
    rows = [
        {
            "station_id": "KBUF",
            "scan_datetime": datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC),
            "year": 2024,
            "month": 1,
            "day": 15,
            "hour": 10,
            "volume_number": 1,
            "format_version": "AR2V0006.",
            "file_path": "data/raw/KBUF20240115_100000_V06",
            "file_size_bytes": 18_000_000,
            "ingested_at": datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        },
        {
            "station_id": "KBUF",
            "scan_datetime": datetime(2024, 1, 15, 10, 6, 0, tzinfo=UTC),
            "year": 2024,
            "month": 1,
            "day": 15,
            "hour": 10,
            "volume_number": 2,
            "format_version": "AR2V0006.",
            "file_path": "data/raw/KBUF20240115_100600_V06",
            "file_size_bytes": 17_500_000,
            "ingested_at": datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        },
        {
            "station_id": "KBUF",
            "scan_datetime": datetime(2024, 1, 15, 10, 12, 0, tzinfo=UTC),
            "year": 2024,
            "month": 1,
            "day": 15,
            "hour": 10,
            "volume_number": 3,
            "format_version": "AR2V0006.",
            "file_path": "data/raw/KBUF20240115_101200_V06",
            "file_size_bytes": 18_200_000,
            "ingested_at": datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        },
        {
            "station_id": "KBUF",
            "scan_datetime": datetime(2024, 1, 15, 11, 0, 0, tzinfo=UTC),
            "year": 2024,
            "month": 1,
            "day": 15,
            "hour": 11,
            "volume_number": 4,
            "format_version": "AR2V0006.",
            "file_path": "data/raw/KBUF20240115_110000_V06",
            "file_size_bytes": 17_800_000,
            "ingested_at": datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        },
        {
            "station_id": "KBUF",
            "scan_datetime": datetime(2024, 1, 15, 11, 6, 0, tzinfo=UTC),
            "year": 2024,
            "month": 1,
            "day": 15,
            "hour": 11,
            "volume_number": 5,
            "format_version": "AR2V0006.",
            "file_path": "data/raw/KBUF20240115_110600_V06",
            "file_size_bytes": 18_100_000,
            "ingested_at": datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        },
        {
            "station_id": "KBUF",
            "scan_datetime": datetime(2024, 1, 15, 11, 12, 0, tzinfo=UTC),
            "year": 2024,
            "month": 1,
            "day": 15,
            "hour": 11,
            "volume_number": 6,
            "format_version": "AR2V0006.",
            "file_path": "data/raw/KBUF20240115_111200_V06",
            "file_size_bytes": 17_900_000,
            "ingested_at": datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        },
    ]
    return pl.DataFrame(rows, schema=SILVER_SCHEMA)


@pytest.fixture
def empty_silver_df() -> pl.DataFrame:
    """DataFrame Silver vacío — útil para probar manejo de casos edge."""
    return pl.DataFrame(schema=SILVER_SCHEMA)


@pytest.fixture
def empty_gold_df() -> pl.DataFrame:
    """DataFrame Gold vacío."""
    return pl.DataFrame(schema=GOLD_SCHEMA)


# ---------------------------------------------------------------------------
# Fixture de archivos NEXRAD falsos (para tests sin red)
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_nexrad_file(tmp_path: Path) -> Path:
    """
    Crea un archivo NEXRAD Level II falso con un header válido.

    Esto nos permite testear parse_metadata() sin descargar datos reales.
    El header tiene exactamente 24 bytes con el formato AR2V esperado.

    Formato del header:
        bytes  0-8:  "AR2V0006." (versión)
        bytes  9-11: "001"       (número de volumen)
        bytes 12-15: días desde epoch en big-endian uint32
        bytes 16-19: ms desde medianoche en big-endian uint32
        bytes 20-23: "KBUF"     (ICAO de la estación)
    """
    # 2024-01-15 10:00:00 UTC
    # días desde 1970-01-01 = 19737
    # milisegundos desde medianoche = 10 * 3600 * 1000 = 36_000_000
    days = 19737
    millis = 36_000_000

    header = (
        b"AR2V0006."  # bytes 0-8: versión (9 bytes)
        b"001"  # bytes 9-11: volumen
        + struct.pack(">I", days)  # bytes 12-15: fecha
        + struct.pack(">I", millis)  # bytes 16-19: hora
        + b"KBUF"  # bytes 20-23: estación
    )

    # El archivo real tiene mucho más contenido, pero para parse_metadata
    # solo necesitamos los primeros 24 bytes
    file_path = tmp_path / "KBUF20240115_100000_V06"
    file_path.write_bytes(header + b"\x00" * 1000)  # padding para simular tamaño real
    return file_path


@pytest.fixture
def fake_nexrad_file_invalid(tmp_path: Path) -> Path:
    """Archivo con header inválido (no empieza con AR2V)."""
    file_path = tmp_path / "INVALID20240115_V06"
    file_path.write_bytes(b"NOTVALID" + b"\x00" * 100)
    return file_path
