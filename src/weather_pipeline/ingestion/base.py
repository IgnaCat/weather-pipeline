"""
Clase base para todos los ingestores del pipeline.

Define la interfaz común e implementa retry con backoff exponencial,
rate limiting y manejo robusto de errores.
"""

import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from loguru import logger
from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from weather_pipeline.config import settings

# ---------------------------------------------------------------------------
# Excepciones custom — no fallar silenciosamente
# ---------------------------------------------------------------------------


class IngestorError(Exception):
    """Error base del ingestor."""


class RateLimitError(IngestorError):
    """Se excedió el rate limit de la fuente de datos."""


class DataNotFoundError(IngestorError):
    """No hay datos disponibles para el período solicitado."""


class ParseError(IngestorError):
    """Error al parsear los datos descargados."""


# ---------------------------------------------------------------------------
# Estadísticas de ingesta
# ---------------------------------------------------------------------------


@dataclass
class IngestStats:
    """Métricas de una sesión de ingesta."""

    files_attempted: int = 0
    files_downloaded: int = 0
    files_skipped: int = 0  # Ya existían en disco
    files_failed: int = 0
    bytes_downloaded: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Porcentaje de archivos descargados exitosamente."""
        if self.files_attempted == 0:
            return 0.0
        return self.files_downloaded / self.files_attempted * 100

    def log_summary(self) -> None:
        """Loguea un resumen de la sesión."""
        logger.info(
            "Ingesta finalizada | "
            f"intentados={self.files_attempted} "
            f"descargados={self.files_downloaded} "
            f"skipped={self.files_skipped} "
            f"fallidos={self.files_failed} "
            f"success_rate={self.success_rate:.1f}% "
            f"bytes={self.bytes_downloaded:,}"
        )
        if self.errors:
            for err in self.errors[:5]:  # Mostrar máximo 5 errores
                logger.warning(f"Error registrado: {err}")


# ---------------------------------------------------------------------------
# Rate limiter simple (token bucket)
# ---------------------------------------------------------------------------


class RateLimiter:
    """
    Rate limiter basado en ventana deslizante.

    Garantiza no superar `max_calls` llamadas en `period` segundos.
    Thread-safe para uso single-threaded (suficiente para este pipeline).
    """

    def __init__(self, max_calls: int, period: float) -> None:
        self.max_calls = max_calls
        self.period = period
        self._calls: deque[float] = deque()

    def wait_if_needed(self) -> None:
        """Bloquea si se superó el rate limit hasta que haya slot disponible."""
        now = time.monotonic()

        # Limpiar llamadas vencidas
        while self._calls and now - self._calls[0] > self.period:
            self._calls.popleft()

        if len(self._calls) >= self.max_calls:
            # Esperar hasta que la llamada más antigua salga de la ventana
            wait_time = self.period - (now - self._calls[0]) + 0.01
            if wait_time > 0:
                logger.debug(f"Rate limit alcanzado — esperando {wait_time:.2f}s")
                time.sleep(wait_time)
            # Re-limpiar después de esperar
            now = time.monotonic()
            while self._calls and now - self._calls[0] > self.period:
                self._calls.popleft()

        self._calls.append(time.monotonic())


# ---------------------------------------------------------------------------
# Clase base abstracta
# ---------------------------------------------------------------------------


class IngestorBase(ABC):
    """
    Clase base para todos los ingestores.

    Responsabilidades:
    - Definir la interfaz (list_available / download_file)
    - Manejar retry con backoff exponencial vía tenacity
    - Aplicar rate limiting automático
    - Logging estructurado de todas las operaciones
    - Persistir datos crudos en data/raw/ con particionado por fecha
    """

    SOURCE_NAME: str = "base"  # Sobreescribir en cada subclase

    def __init__(self) -> None:
        self._rate_limiter = RateLimiter(
            max_calls=settings.pipeline_rate_limit_calls,
            period=settings.pipeline_rate_limit_period,
        )
        self._stats = IngestStats()
        logger.info(f"Ingestor inicializado: {self.SOURCE_NAME}")

    # ------------------------------------------------------------------
    # Métodos abstractos — cada fuente los implementa
    # ------------------------------------------------------------------

    @abstractmethod
    def list_available(self, target_date: date, **kwargs: Any) -> list[str]:
        """
        Lista los identificadores de archivos disponibles para una fecha.

        Args:
            target_date: Fecha para la que buscar datos.
            **kwargs: Parámetros específicos de cada fuente (ej: station).

        Returns:
            Lista de identificadores (paths, URLs, claves S3, etc.)
        """
        ...

    @abstractmethod
    def download_file(self, file_id: str, dest_dir: Path) -> Path:
        """
        Descarga un archivo y lo guarda en dest_dir.

        Args:
            file_id: Identificador del archivo (de list_available).
            dest_dir: Directorio de destino.

        Returns:
            Path del archivo guardado localmente.

        Raises:
            DataNotFoundError: Si el archivo no existe.
            IngestorError: Para otros errores de descarga.
        """
        ...

    # ------------------------------------------------------------------
    # Lógica de ingesta con retry — no sobreescribir
    # ------------------------------------------------------------------

    def ingest(
        self,
        target_date: date,
        dest_dir: Path | None = None,
        **kwargs: Any,
    ) -> tuple[list[Path], IngestStats]:
        """
        Punto de entrada principal. Lista y descarga todos los archivos
        disponibles para target_date, con retry y rate limiting.

        Args:
            target_date: Fecha a ingestar.
            dest_dir: Directorio destino. Si es None, usa data/raw/<fecha>.
            **kwargs: Parámetros adicionales para list_available/download_file.

        Returns:
            Tupla (lista de paths descargados, estadísticas).
        """
        self._stats = IngestStats()

        # Construir ruta de destino con particionado por fecha
        if dest_dir is None:
            dest_dir = (
                settings.data_raw_path
                / f"year={target_date.year}"
                / f"month={target_date.month:02d}"
                / f"day={target_date.day:02d}"
                / self.SOURCE_NAME
            )
        dest_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"[{self.SOURCE_NAME}] Iniciando ingesta | " f"fecha={target_date} destino={dest_dir}"
        )

        # Listar archivos disponibles (con retry)
        try:
            available = self._list_with_retry(target_date, **kwargs)
        except RetryError as exc:
            raise IngestorError(
                f"[{self.SOURCE_NAME}] No se pudo listar archivos después de "
                f"{settings.pipeline_max_retries} intentos: {exc}"
            ) from exc

        if not available:
            logger.warning(f"[{self.SOURCE_NAME}] No hay archivos disponibles para {target_date}")
            return [], self._stats

        logger.info(f"[{self.SOURCE_NAME}] {len(available)} archivos disponibles")

        # Descargar cada archivo
        downloaded_paths: list[Path] = []
        for file_id in available:
            self._stats.files_attempted += 1
            self._rate_limiter.wait_if_needed()

            # Verificar si ya existe (idempotencia)
            existing = self._find_existing(file_id, dest_dir)
            if existing:
                logger.debug(f"Ya existe, skipping: {file_id}")
                self._stats.files_skipped += 1
                downloaded_paths.append(existing)
                continue

            try:
                path = self._download_with_retry(file_id, dest_dir)
                downloaded_paths.append(path)
                self._stats.files_downloaded += 1
                file_size = path.stat().st_size
                self._stats.bytes_downloaded += file_size
                logger.info(f"Descargado: {path.name} ({file_size:,} bytes)")
            except RetryError as exc:
                self._stats.files_failed += 1
                error_msg = f"Falló después de reintentos: {file_id} — {exc}"
                self._stats.errors.append(error_msg)
                logger.error(error_msg)
            except DataNotFoundError as exc:
                self._stats.files_failed += 1
                error_msg = f"Archivo no encontrado: {file_id} — {exc}"
                self._stats.errors.append(error_msg)
                logger.warning(error_msg)

        self._stats.log_summary()
        return downloaded_paths, self._stats

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _retry_policy(self) -> Retrying:
        """Configuración de retry compartida por todos los métodos."""
        return Retrying(
            retry=retry_if_exception_type((OSError, TimeoutError, IngestorError)),
            stop=stop_after_attempt(settings.pipeline_max_retries),
            wait=wait_exponential(
                min=settings.pipeline_retry_wait_min,
                max=settings.pipeline_retry_wait_max,
            ),
            reraise=True,
        )

    def _list_with_retry(self, target_date: date, **kwargs: Any) -> list[str]:
        """list_available ejecutado con retry."""
        for attempt in self._retry_policy():
            with attempt:
                result: list[str] = self.list_available(target_date, **kwargs)
        return result  # noqa: F821  # siempre asignado: si todos los intentos fallan, reraise=True lanza

    def _download_with_retry(self, file_id: str, dest_dir: Path) -> Path:
        """download_file ejecutado con retry."""
        for attempt in self._retry_policy():
            with attempt:
                result: Path = self.download_file(file_id, dest_dir)
        return result  # noqa: F821

    def _find_existing(self, file_id: str, dest_dir: Path) -> Path | None:
        """Busca si el archivo ya fue descargado (por nombre base)."""
        filename = Path(file_id).name
        candidate = dest_dir / filename
        return candidate if candidate.exists() else None

    @property
    def stats(self) -> IngestStats:
        """Estadísticas de la última sesión de ingesta."""
        return self._stats
