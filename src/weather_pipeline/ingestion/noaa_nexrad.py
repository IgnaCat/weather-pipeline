"""
Ingestor para datos de radar NOAA NEXRAD Level II.

NEXRAD (Next-Generation Radar) es una red de ~160 radares Doppler en EE.UU.
Los datos Level II son volumétricos: 360° de azimut × múltiples elevaciones.

Fuente: s3://noaa-nexrad-level2/ (bucket público de AWS, sin autenticación)
Estructura del bucket: YYYY/MM/DD/STATION/STATIONYYYYMMDDHHmmss_V06[.gz]

Documentación oficial:
https://registry.opendata.aws/noaa-nexrad/
https://www.ncei.noaa.gov/products/radar/next-generation-weather-radar
"""

import struct
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from loguru import logger

from weather_pipeline.config import settings
from weather_pipeline.ingestion.base import (
    DataNotFoundError,
    IngestorBase,
    IngestorError,
    ParseError,
)

# ---------------------------------------------------------------------------
# Modelos de datos
# ---------------------------------------------------------------------------


@dataclass
class NEXRADFile:
    """Metadatos de un archivo NEXRAD Level II."""

    s3_key: str
    station: str
    scan_datetime: datetime
    size_bytes: int
    volume_number: int | None = None
    compressed: bool = False

    @property
    def filename(self) -> str:
        return Path(self.s3_key).name

    @property
    def local_filename(self) -> str:
        """Nombre del archivo sin la extensión .gz si está comprimido."""
        name = self.filename
        if name.endswith(".gz"):
            return name[:-3]
        return name


@dataclass
class NEXRADMetadata:
    """
    Metadata extraída del header de un archivo NEXRAD Level II (formato AR2V).

    El header tiene 24 bytes:
    - Bytes 0-8:  "AR2V0006." o similar (versión del formato)
    - Bytes 9-11: Extensión de volumen ("001", "002", etc.)
    - Bytes 12-15: Date (días desde 1 enero 1970, big-endian uint32)
    - Bytes 16-19: Time (milisegundos desde medianoche, big-endian uint32)
    - Bytes 20-24: ICAO del radar (4 caracteres ASCII)
    """

    station_id: str
    scan_datetime: datetime
    version: str
    volume_number: int
    source_file: str


# ---------------------------------------------------------------------------
# Ingestor NEXRAD
# ---------------------------------------------------------------------------


class NOAANEXRADIngestor(IngestorBase):
    """
    Descarga datos de radar NOAA NEXRAD Level II desde el bucket público de S3.

    El bucket s3://noaa-nexrad-level2/ es completamente público y no requiere
    credenciales de AWS. Se accede con UNSIGNED config de boto3.

    Uso:
        ingestor = NOAANEXRADIngestor()
        paths, stats = ingestor.ingest(date(2024, 1, 15), station="KBUF")
    """

    SOURCE_NAME = "nexrad"

    def __init__(self) -> None:
        super().__init__()
        # Cliente S3 sin autenticación — bucket público
        self._s3 = boto3.client(
            "s3",
            region_name=settings.aws_default_region,
            config=Config(signature_version=UNSIGNED),
        )
        self._bucket = settings.nexrad_bucket
        logger.info(
            f"NOAANEXRADIngestor conectado | "
            f"bucket=s3://{self._bucket} región={settings.aws_default_region}"
        )

    def list_available(self, target_date: date, **kwargs: Any) -> list[str]:
        """
        Lista las claves S3 de todos los archivos Level II para una estación y fecha.

        Args:
            target_date: Fecha de los datos.
            station: Código ICAO de 4 letras (ej: "KBUF"). Default: settings.nexrad_station.

        Returns:
            Lista de claves S3 (strings) ordenadas cronológicamente.

        Raises:
            DataNotFoundError: Si no hay datos para esa estación/fecha.
            IngestorError: Para errores de conexión con S3.
        """
        station = kwargs.get("station", settings.nexrad_station).upper()
        prefix = f"{target_date.year}/{target_date.month:02d}/{target_date.day:02d}/{station}/"

        logger.debug(f"Listando s3://{self._bucket}/{prefix}")

        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self._bucket, Prefix=prefix)

            keys: list[str] = []
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    # Filtrar archivos que no son datos (ej: .tar, índices)
                    if self._is_level2_file(key):
                        keys.append(key)

        except self._s3.exceptions.NoSuchBucket as exc:
            raise IngestorError(f"Bucket no encontrado: {self._bucket}") from exc
        except Exception as exc:
            raise IngestorError(f"Error listando S3: {exc}") from exc

        if not keys:
            raise DataNotFoundError(f"Sin datos NEXRAD para estación={station} fecha={target_date}")

        # Ordenar cronológicamente por nombre de archivo
        keys.sort()
        logger.info(f"Encontrados {len(keys)} archivos Level II para {station}/{target_date}")
        return keys

    def download_file(self, file_id: str, dest_dir: Path) -> Path:
        """
        Descarga un archivo NEXRAD desde S3 a disco local.

        Args:
            file_id: Clave S3 del archivo (de list_available).
            dest_dir: Directorio de destino.

        Returns:
            Path del archivo descargado.

        Raises:
            DataNotFoundError: Si la clave no existe en S3.
            IngestorError: Para otros errores de descarga.
        """
        filename = Path(file_id).name
        dest_path = dest_dir / filename

        logger.debug(f"Descargando s3://{self._bucket}/{file_id}")

        try:
            self._s3.download_file(
                Bucket=self._bucket,
                Key=file_id,
                Filename=str(dest_path),
            )
        except self._s3.exceptions.ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code in ("404", "NoSuchKey"):
                raise DataNotFoundError(f"Archivo no encontrado en S3: {file_id}") from exc
            raise IngestorError(f"Error S3 ({error_code}): {file_id}") from exc
        except Exception as exc:
            raise IngestorError(f"Error descargando {file_id}: {exc}") from exc

        return dest_path

    def parse_metadata(self, file_path: Path) -> NEXRADMetadata:
        """
        Extrae metadata del header de un archivo NEXRAD Level II.

        El header tiene 24 bytes en formato AR2V:
        - [0:9]   Versión del archivo (ej: "AR2V0006.")
        - [9:12]  Número de volumen (3 dígitos ASCII)
        - [12:16] Fecha (días desde 1/1/1970, big-endian uint32)
        - [16:20] Hora (milisegundos desde medianoche, big-endian uint32)
        - [20:24] ICAO de la estación (4 chars ASCII)

        Args:
            file_path: Path al archivo Level II (puede estar comprimido con gzip).

        Returns:
            NEXRADMetadata con los campos extraídos.

        Raises:
            ParseError: Si el archivo no tiene el formato esperado.
        """
        try:
            raw = self._read_header(file_path)
            if len(raw) < 24:
                raise ParseError(f"Header muy corto ({len(raw)} bytes): {file_path}")

            version = raw[0:9].decode("ascii", errors="replace").strip("\x00")
            if not version.startswith("AR2V"):
                raise ParseError(
                    f"Formato inesperado (esperado AR2V, got '{version}'): {file_path}"
                )

            volume_number = int(raw[9:12].decode("ascii", errors="replace").strip())
            # Fecha: días julianos desde el 1 ene 1970 (epoch Unix en días)
            days_since_epoch: int = struct.unpack(">I", raw[12:16])[0]
            millis_since_midnight: int = struct.unpack(">I", raw[16:20])[0]
            station_id = raw[20:24].decode("ascii", errors="replace").strip()

            total_seconds = days_since_epoch * 86400 + millis_since_midnight / 1000.0
            scan_datetime = datetime.fromtimestamp(total_seconds, tz=UTC)

            return NEXRADMetadata(
                station_id=station_id,
                scan_datetime=scan_datetime,
                version=version,
                volume_number=volume_number,
                source_file=str(file_path),
            )

        except (struct.error, ValueError) as exc:
            raise ParseError(f"Error parseando header de {file_path}: {exc}") from exc

    def list_stations(self) -> list[str]:
        """
        Lista las estaciones disponibles para hoy en el bucket.
        Útil para exploración inicial.

        Returns:
            Lista de códigos ICAO de estaciones con datos hoy.
        """
        today = date.today()
        prefix = f"{today.year}/{today.month:02d}/{today.day:02d}/"

        try:
            response = self._s3.list_objects_v2(
                Bucket=self._bucket,
                Prefix=prefix,
                Delimiter="/",
            )
            stations = [cp["Prefix"].split("/")[-2] for cp in response.get("CommonPrefixes", [])]
            return sorted(stations)
        except Exception as exc:
            raise IngestorError(f"Error listando estaciones: {exc}") from exc

    # ------------------------------------------------------------------
    # Helpers privados
    # ------------------------------------------------------------------

    @staticmethod
    def _is_level2_file(key: str) -> bool:
        """
        Determina si una clave S3 corresponde a un archivo Level II válido.

        Los archivos Level II tienen formato:
        KBUF20240115_000000_V06 o KBUF20240115_000000_V06.gz
        """
        name = Path(key).name
        # Descartar archivos que no son datos de radar
        if name.endswith((".tar", ".tar.gz", ".tar.bz2", ".idx")):
            return False
        # Los archivos válidos contienen "_V06" o "_V08" (versión del formato)
        return "_V0" in name

    @staticmethod
    def _read_header(file_path: Path) -> bytes:
        """Lee los primeros bytes del archivo (maneja gzip transparentemente)."""
        import gzip

        if file_path.suffix == ".gz":
            with gzip.open(file_path, "rb") as f:
                return f.read(24)
        else:
            with open(file_path, "rb") as f:
                return f.read(24)
