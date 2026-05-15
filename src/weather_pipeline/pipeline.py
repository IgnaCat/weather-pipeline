"""
Pipeline principal — orquesta todo el flujo de datos.

Este módulo conecta los tres módulos que ya construimos:
    ingestion/  → descarga archivos NEXRAD de S3
    processing/ → convierte archivos crudos a DataFrames Silver y Gold
    storage/    → persiste en Parquet y registra en el catálogo DuckDB

Flujo completo:
    1. INGEST   → S3 → data/raw/<station>/<date>/
    2. SILVER   → raw files → DataFrame limpio → data/processed/silver/
    3. GOLD     → Silver DataFrame → métricas por hora → data/processed/gold/

Uso desde CLI:
    # Pipeline completo para hoy
    python -m weather_pipeline.pipeline --station KBUF

    # Solo una fecha específica
    python -m weather_pipeline.pipeline --station KBUF --date 2024-01-15

    # Solo una etapa (útil para reprocessar sin re-descargar)
    python -m weather_pipeline.pipeline --station KBUF --date 2024-01-15 --step silver
    python -m weather_pipeline.pipeline --station KBUF --date 2024-01-15 --step gold

    # Backfill: procesar un rango de fechas
    python -m weather_pipeline.pipeline --station KBUF --backfill 2024-01-15 2024-01-20

    # Ver qué haría sin ejecutar nada
    python -m weather_pipeline.pipeline --station KBUF --date 2024-01-15 --dry-run
"""

import argparse
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal

from loguru import logger

from weather_pipeline.config import settings
from weather_pipeline.ingestion.noaa_nexrad import NOAANEXRADIngestor
from weather_pipeline.processing.transformers import (
    raw_files_to_silver,
    silver_to_gold,
    summarize_day,
    validate_silver,
)
from weather_pipeline.storage.local import LocalStorage

# ---------------------------------------------------------------------------
# Tipos
# ---------------------------------------------------------------------------

Step = Literal["ingest", "silver", "gold", "all"]


# ---------------------------------------------------------------------------
# Resultado de una ejecución
# ---------------------------------------------------------------------------


@dataclass
class PipelineResult:
    """
    Captura el resultado de correr el pipeline para un día y estación.

    Separar el resultado en un dataclass (en vez de solo loggear) nos permite
    testear el pipeline sin depender de stdout, y en Fase 3 podemos mandar
    estos resultados a un sistema de monitoreo.
    """

    station: str
    target_date: date
    step: Step

    # Etapa ingest
    files_downloaded: int = 0
    files_skipped: int = 0
    ingest_errors: int = 0

    # Etapa silver
    silver_rows: int = 0
    silver_path: Path | None = None
    silver_valid: bool = False

    # Etapa gold
    gold_rows: int = 0
    gold_path: Path | None = None

    # Errores generales
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """True si el pipeline terminó sin errores críticos."""
        return len(self.errors) == 0

    def summary(self) -> str:
        """Resumen en una línea para el log final."""
        parts = [f"[{self.station}] {self.target_date}"]
        if self.files_downloaded > 0:
            parts.append(f"descargados={self.files_downloaded}")
        if self.silver_rows > 0:
            parts.append(f"silver={self.silver_rows}filas")
        if self.gold_rows > 0:
            parts.append(f"gold={self.gold_rows}filas")
        if self.errors:
            parts.append(f"ERRORES={len(self.errors)}")
        return " | ".join(parts)


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------


class WeatherPipeline:
    """
    Orquestador del pipeline de datos meteorológicos.

    Responsabilidades:
    - Recibir parámetros (fecha, estación, etapa)
    - Coordinar ingestor, transformers y storage
    - Manejar errores sin detener el pipeline completo
    - Producir un PipelineResult con el resumen de la ejecución

    Decisión de diseño: WeatherPipeline no hace ninguna lógica de negocio
    (eso lo hacen los módulos que llama). Su única responsabilidad es la
    coordinación y el manejo de errores entre etapas.
    """

    def __init__(self, dry_run: bool = False) -> None:
        """
        Args:
            dry_run: Si True, muestra qué haría pero no ejecuta ni descarga
                     ni escribe nada en disco.
        """
        self._dry_run = dry_run
        self._storage = LocalStorage()
        self._ingestor = NOAANEXRADIngestor()

        if dry_run:
            logger.info("Modo DRY-RUN activo — no se escribirá nada en disco")

    def run(
        self,
        station: str,
        target_date: date,
        step: Step = "all",
    ) -> PipelineResult:
        """
        Ejecuta el pipeline para una estación y fecha.

        Args:
            station: Código ICAO de 4 letras (ej: "KBUF").
            target_date: Fecha a procesar.
            step: Qué etapas correr.
                  "all"    → ingest + silver + gold
                  "ingest" → solo descargar de S3
                  "silver" → solo transformar a Silver (asume que ya hay raw files)
                  "gold"   → solo transformar a Gold (asume que ya hay Silver)

        Returns:
            PipelineResult con el resumen de la ejecución.
        """
        result = PipelineResult(station=station, target_date=target_date, step=step)

        logger.info(
            f"▶ Iniciando pipeline | estación={station} | fecha={target_date} | step={step}"
        )

        # ------------------------------------------------------------------
        # Etapa 1: INGEST — descarga archivos crudos de S3
        # ------------------------------------------------------------------
        raw_files: list[Path] = []

        if step in ("all", "ingest"):
            raw_files = self._run_ingest(station, target_date, result)
        else:
            # Si saltamos la ingesta, buscamos los archivos que ya existen en disco
            raw_dir = settings.data_raw_path / station / str(target_date)
            if raw_dir.exists():
                raw_files = list(raw_dir.glob("*"))
                logger.info(f"  Usando {len(raw_files)} archivos locales existentes en {raw_dir}")
            else:
                logger.warning(
                    f"  No hay archivos locales en {raw_dir} — Silver puede quedar vacío"
                )

        # Si hubo errores en ingest y no hay archivos, no tiene sentido seguir
        if step == "ingest":
            logger.info(f"✔ Etapa 'ingest' completada | {result.summary()}")
            return result

        # ------------------------------------------------------------------
        # Etapa 2: SILVER — convierte raw files a DataFrame limpio
        # ------------------------------------------------------------------
        if step in ("all", "silver", "gold"):
            self._run_silver(station, target_date, raw_files, result)

        if step == "silver":
            logger.info(f"✔ Etapa 'silver' completada | {result.summary()}")
            return result

        # ------------------------------------------------------------------
        # Etapa 3: GOLD — agrega Silver por hora
        # ------------------------------------------------------------------
        if step in ("all", "gold"):
            self._run_gold(station, target_date, result)

        status = "✔" if result.success else "✖"
        logger.info(f"{status} Pipeline completado | {result.summary()}")
        return result

    # ------------------------------------------------------------------
    # Etapas internas
    # ------------------------------------------------------------------

    def _run_ingest(
        self,
        station: str,
        target_date: date,
        result: PipelineResult,
    ) -> list[Path]:
        """
        Descarga archivos NEXRAD de S3 al disco local.

        Retorna la lista de archivos descargados (para pasarla a la etapa Silver).
        Si falla, registra el error en result y retorna lista vacía.
        """
        logger.info(f"  [ingest] Listando archivos S3 para {station} / {target_date}")

        if self._dry_run:
            # En dry-run mostramos cuántos archivos habría sin descargar
            try:
                available = self._ingestor.list_available(target_date, station=station)
                logger.info(
                    f"  [dry-run] Encontrados {len(available)} archivos en S3 (no se descargan)"
                )
            except Exception as exc:
                logger.warning(f"  [dry-run] No se pudo listar S3: {exc}")
            return []

        try:
            dest_dir = settings.data_raw_path / station / str(target_date)
            dest_dir.mkdir(parents=True, exist_ok=True)

            # ingest() retorna (lista de paths descargados, stats de la ejecución)
            downloaded_files, stats = self._ingestor.ingest(
                target_date=target_date,
                dest_dir=dest_dir,
                station=station,
            )

            result.files_downloaded = stats.files_downloaded
            result.files_skipped = stats.files_skipped
            result.ingest_errors = stats.files_failed

            logger.info(
                f"  [ingest] descargados={stats.files_downloaded} "
                f"saltados={stats.files_skipped} fallidos={stats.files_failed}"
            )
            return downloaded_files

        except Exception as exc:
            msg = f"Error en etapa ingest: {exc}"
            logger.error(msg)
            result.errors.append(msg)
            return []

    def _run_silver(
        self,
        station: str,
        target_date: date,
        raw_files: list[Path],
        result: PipelineResult,
    ) -> None:
        """
        Convierte archivos crudos a DataFrame Silver y lo persiste.

        Silver = metadata extraída del header de cada archivo NEXRAD:
        station_id, scan_datetime, volume_number, file_size, etc.
        """
        logger.info(f"  [silver] Procesando {len(raw_files)} archivos crudos")

        if self._dry_run:
            logger.info("  [dry-run] Se omitiría la transformación a Silver")
            return

        try:
            # raw_files_to_silver parsea los headers y arma el DataFrame
            silver_df = raw_files_to_silver(raw_files)

            if silver_df.is_empty():
                logger.warning("  [silver] DataFrame resultante está vacío — no se guarda")
                return

            # Validar antes de guardar
            checks = validate_silver(silver_df)
            result.silver_valid = all(v for k, v in checks.items() if k != "_summary")
            logger.info(f"  [silver] Validación: {checks.get('_summary', '?')}")

            if not result.silver_valid:
                failed = [k for k, v in checks.items() if k != "_summary" and not v]
                logger.warning(f"  [silver] Checks fallidos: {failed}")

            # Guardar en Parquet con particionado Hive-style
            path = self._storage.save_silver(silver_df, target_date, station)
            result.silver_rows = len(silver_df)
            result.silver_path = path

            # Imprimir resumen legible del día
            logger.info(summarize_day(silver_df, target_date))

        except Exception as exc:
            msg = f"Error en etapa silver: {exc}"
            logger.error(msg)
            result.errors.append(msg)

    def _run_gold(
        self,
        station: str,
        target_date: date,
        result: PipelineResult,
    ) -> None:
        """
        Carga Silver del disco, agrega por hora y persiste Gold.

        Gold = una fila por (station, hour) con:
        scan_count, max_gap_minutes, avg_gap_minutes, total_bytes.
        """
        logger.info(f"  [gold] Cargando Silver de {target_date}")

        if self._dry_run:
            logger.info("  [dry-run] Se omitiría la transformación a Gold")
            return

        try:
            # Cargar Silver que guardamos en la etapa anterior
            silver_df = self._storage.load_silver(target_date, station)

            if silver_df.is_empty():
                logger.warning("  [gold] Silver vacío — no hay nada que agregar")
                return

            # Transformar a Gold (agrega por hora)
            gold_df = silver_to_gold(silver_df)

            if gold_df.is_empty():
                logger.warning("  [gold] Gold resultante está vacío")
                return

            # Guardar Gold
            path = self._storage.save_gold(gold_df, target_date, station)
            result.gold_rows = len(gold_df)
            result.gold_path = path

            logger.info(f"  [gold] {len(gold_df)} horas agregadas → {path.name}")

        except Exception as exc:
            msg = f"Error en etapa gold: {exc}"
            logger.error(msg)
            result.errors.append(msg)


# ---------------------------------------------------------------------------
# Backfill — procesar un rango de fechas
# ---------------------------------------------------------------------------


def run_backfill(
    station: str,
    start_date: date,
    end_date: date,
    step: Step = "all",
    dry_run: bool = False,
) -> list[PipelineResult]:
    """
    Ejecuta el pipeline para cada día en el rango [start_date, end_date].

    Útil para cargar datos históricos. Cada día es independiente:
    si un día falla, el backfill continúa con el siguiente.

    Args:
        station: Código ICAO de la estación.
        start_date: Primer día del rango (inclusive).
        end_date: Último día del rango (inclusive).
        step: Etapas a ejecutar (igual que en run()).
        dry_run: Si True, no escribe nada en disco.

    Returns:
        Lista de PipelineResult, uno por día.
    """
    pipeline = WeatherPipeline(dry_run=dry_run)
    results: list[PipelineResult] = []

    current = start_date
    total_days = (end_date - start_date).days + 1
    logger.info(f"▶ Backfill {station} | {start_date} → {end_date} ({total_days} días)")

    day_num = 0
    while current <= end_date:
        day_num += 1
        logger.info(f"\n── Día {day_num}/{total_days}: {current} ──")
        result = pipeline.run(station=station, target_date=current, step=step)
        results.append(result)
        current += timedelta(days=1)

    # Resumen final del backfill
    successful = sum(1 for r in results if r.success)
    total_silver = sum(r.silver_rows for r in results)
    total_gold = sum(r.gold_rows for r in results)

    logger.info(
        f"\n▶ Backfill completado | "
        f"exitosos={successful}/{total_days} | "
        f"silver_total={total_silver} filas | "
        f"gold_total={total_gold} filas"
    )
    return results


# ---------------------------------------------------------------------------
# CLI — interfaz de línea de comandos
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    """
    Construye el parser de argumentos de CLI.

    argparse es la librería estándar de Python para argumentos de CLI.
    Genera automáticamente el texto de --help.
    """
    parser = argparse.ArgumentParser(
        prog="weather-pipeline",
        description="Pipeline de ingestión y procesamiento de datos NEXRAD",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Pipeline completo para hoy
  python -m weather_pipeline.pipeline --station KBUF

  # Fecha específica
  python -m weather_pipeline.pipeline --station KBUF --date 2024-01-15

  # Solo descargar de S3
  python -m weather_pipeline.pipeline --station KBUF --date 2024-01-15 --step ingest

  # Solo reprocessar (ya tenés los raw files)
  python -m weather_pipeline.pipeline --station KBUF --date 2024-01-15 --step silver

  # Backfill de una semana
  python -m weather_pipeline.pipeline --station KBUF --backfill 2024-01-10 2024-01-17

  # Ver qué haría sin ejecutar
  python -m weather_pipeline.pipeline --station KBUF --date 2024-01-15 --dry-run
        """,
    )

    parser.add_argument(
        "--station",
        type=str,
        default=settings.nexrad_station,
        metavar="ICAO",
        help=f"Código ICAO de la estación (ej: KBUF). Default: {settings.nexrad_station}",
    )

    # --date y --backfill son mutuamente excluyentes
    date_group = parser.add_mutually_exclusive_group()

    date_group.add_argument(
        "--date",
        type=date.fromisoformat,  # "2024-01-15" → date(2024, 1, 15) automáticamente
        default=None,
        metavar="YYYY-MM-DD",
        help="Fecha a procesar. Default: hoy.",
    )

    date_group.add_argument(
        "--backfill",
        nargs=2,
        type=date.fromisoformat,
        metavar=("START", "END"),
        help="Procesar rango de fechas: --backfill 2024-01-10 2024-01-20",
    )

    parser.add_argument(
        "--step",
        type=str,
        choices=["all", "ingest", "silver", "gold"],
        default="all",
        help="Etapa a ejecutar. Default: all (ingest + silver + gold)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra qué haría sin ejecutar ni escribir nada",
    )

    return parser


def main() -> None:
    """
    Entry point del pipeline.

    Esta función es la que se llama cuando ejecutás:
        python -m weather_pipeline.pipeline
    o el comando instalado:
        weather-pipeline

    El entry point está definido en pyproject.toml:
        [project.scripts]
        weather-pipeline = "weather_pipeline.pipeline:main"
    """
    parser = _build_parser()
    args = parser.parse_args()

    # Configurar logging
    logger.remove()  # quitar el handler default
    logger.add(
        sys.stderr,
        level=settings.log_level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        colorize=True,
    )

    # Asegurar que las carpetas de datos existen
    settings.ensure_directories()

    # ------------------------------------------------------------------
    # Modo backfill
    # ------------------------------------------------------------------
    if args.backfill:
        start_date, end_date = args.backfill
        if start_date > end_date:
            logger.error("La fecha de inicio debe ser anterior o igual a la fecha de fin")
            sys.exit(1)

        results = run_backfill(
            station=args.station.upper(),
            start_date=start_date,
            end_date=end_date,
            step=args.step,
            dry_run=args.dry_run,
        )

        # Salir con error si algún día falló
        failed = [r for r in results if not r.success]
        if failed:
            logger.error(f"{len(failed)} días fallaron: {[str(r.target_date) for r in failed]}")
            sys.exit(1)

        return

    # ------------------------------------------------------------------
    # Modo single-date (default)
    # ------------------------------------------------------------------
    target_date = args.date or datetime.now(tz=UTC).date()

    pipeline = WeatherPipeline(dry_run=args.dry_run)
    result = pipeline.run(
        station=args.station.upper(),
        target_date=target_date,
        step=args.step,
    )

    if not result.success:
        logger.error(f"Pipeline falló: {result.errors}")
        sys.exit(1)

    # Mostrar stats del catálogo al final
    if not args.dry_run:
        stats = pipeline._storage.catalog_stats()
        if stats:
            logger.info(
                f"\n📊 Catálogo total: "
                f"{stats.get('total_datasets', 0)} datasets | "
                f"{stats.get('total_rows', 0)} filas | "
                f"{stats.get('total_size_mb', 0)} MB"
            )


if __name__ == "__main__":
    main()
