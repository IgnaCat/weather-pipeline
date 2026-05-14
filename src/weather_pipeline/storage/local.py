"""
Módulo de almacenamiento local — Parquet + DuckDB.

Responsabilidades:
1. Guardar DataFrames Silver y Gold como archivos Parquet en disco,
   con particionado por fecha (year=YYYY/month=MM/day=DD/).
2. Mantener un catálogo DuckDB que registra cada archivo guardado,
   permitiendo queries SQL sobre los metadatos del data lake.

Separación de responsabilidades:
- Los transformers (processing/) producen DataFrames — no saben nada de disco.
- LocalStorage consume esos DataFrames y los persiste — no sabe nada de transformaciones.
"""

from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import polars as pl
from loguru import logger

from weather_pipeline.config import settings

# ---------------------------------------------------------------------------
# Clase principal
# ---------------------------------------------------------------------------


class LocalStorage:
    """
    Gestiona el almacenamiento local del data lake.

    Estructura en disco:
        data/processed/
          silver/year=YYYY/month=MM/day=DD/<station>.parquet
          gold/year=YYYY/month=MM/day=DD/<station>.parquet

    Catálogo DuckDB en data/catalog.duckdb:
        tabla 'datasets' — un registro por cada archivo Parquet guardado

    Uso:
        storage = LocalStorage()
        storage.save_silver(df, date(2024, 1, 15), station="KBUF")
        storage.save_gold(df, date(2024, 1, 15), station="KBUF")
        storage.show_catalog()
    """

    def __init__(self) -> None:
        self._processed_path = settings.data_processed_path
        self._db_path = settings.duckdb_path

        # Crear directorios si no existen
        self._processed_path.mkdir(parents=True, exist_ok=True)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        # Inicializar catálogo DuckDB
        self._init_catalog()
        logger.info(f"LocalStorage inicializado | db={self._db_path}")

    # ------------------------------------------------------------------
    # Guardar datos
    # ------------------------------------------------------------------

    def save_silver(
        self,
        df: pl.DataFrame,
        target_date: date,
        station: str,
    ) -> Path:
        """
        Guarda un DataFrame Silver como Parquet y registra en el catálogo.

        Args:
            df: DataFrame con schema SILVER_SCHEMA.
            target_date: Fecha de los datos (usada para el particionado).
            station: Código ICAO de la estación (ej: "KBUF").

        Returns:
            Path del archivo Parquet guardado.
        """
        return self._save_layer(df, target_date, station, layer="silver")

    def save_gold(
        self,
        df: pl.DataFrame,
        target_date: date,
        station: str,
    ) -> Path:
        """
        Guarda un DataFrame Gold como Parquet y registra en el catálogo.

        Args:
            df: DataFrame con schema GOLD_SCHEMA.
            target_date: Fecha de los datos.
            station: Código ICAO de la estación.

        Returns:
            Path del archivo Parquet guardado.
        """
        return self._save_layer(df, target_date, station, layer="gold")

    def _save_layer(
        self,
        df: pl.DataFrame,
        target_date: date,
        station: str,
        layer: str,
    ) -> Path:
        """Lógica común para guardar Silver o Gold."""
        if df.is_empty():
            logger.warning(f"DataFrame vacío para {layer}/{station}/{target_date}, no se guarda")
            # Devolvemos el path donde hubiera ido, aunque no se escribió
            return self._build_path(layer, target_date, station)

        dest_path = self._build_path(layer, target_date, station)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Guardar Parquet con compresión Snappy
        # Snappy: buen balance entre velocidad de compresión y tamaño
        df.write_parquet(dest_path, compression="snappy")

        file_size = dest_path.stat().st_size
        logger.info(
            f"Guardado {layer}/{station}/{target_date} → {dest_path.name} "
            f"({len(df)} filas, {file_size:,} bytes)"
        )

        # Registrar en el catálogo
        self._register_in_catalog(
            layer=layer,
            target_date=target_date,
            station=station,
            file_path=dest_path,
            row_count=len(df),
            file_size_bytes=file_size,
            schema_columns=list(df.schema.keys()),
        )

        return dest_path

    # ------------------------------------------------------------------
    # Leer datos
    # ------------------------------------------------------------------

    def load_silver(self, target_date: date, station: str) -> pl.DataFrame:
        """
        Carga el Parquet Silver de una estación y fecha.

        Returns:
            DataFrame Silver. Vacío si el archivo no existe.
        """
        return self._load_layer("silver", target_date, station)

    def load_gold(self, target_date: date, station: str) -> pl.DataFrame:
        """
        Carga el Parquet Gold de una estación y fecha.

        Returns:
            DataFrame Gold. Vacío si el archivo no existe.
        """
        return self._load_layer("gold", target_date, station)

    def _load_layer(self, layer: str, target_date: date, station: str) -> pl.DataFrame:
        """Lógica común para cargar Silver o Gold."""
        path = self._build_path(layer, target_date, station)
        if not path.exists():
            logger.warning(f"No encontrado: {path}")
            return pl.DataFrame()
        df = pl.read_parquet(path)
        logger.debug(f"Cargado {path} → {len(df)} filas")
        return df

    def query(self, sql: str) -> pl.DataFrame:
        """
        Ejecuta una query SQL sobre los archivos Parquet usando DuckDB.

        DuckDB puede leer Parquet directamente con SQL estándar.
        Muy útil para exploración y validación ad-hoc.

        Ejemplo:
            df = storage.query('''
                SELECT station_id, COUNT(*) as scans
                FROM read_parquet('data/processed/silver/**/*.parquet')
                WHERE year = 2024 AND month = 1
                GROUP BY station_id
            ''')

        Args:
            sql: Query SQL estándar.

        Returns:
            DataFrame Polars con el resultado.
        """
        with duckdb.connect(str(self._db_path)) as conn:
            result = conn.execute(sql).pl()
        return result

    # ------------------------------------------------------------------
    # Catálogo DuckDB
    # ------------------------------------------------------------------

    def _init_catalog(self) -> None:
        """
        Crea la tabla 'datasets' en DuckDB si no existe.

        El catálogo es una tabla que registra cada archivo Parquet
        guardado en el data lake, con sus metadatos.
        """
        with duckdb.connect(str(self._db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS datasets (
                    id            INTEGER PRIMARY KEY,
                    layer         VARCHAR NOT NULL,    -- 'silver' o 'gold'
                    station_id    VARCHAR NOT NULL,
                    date          DATE NOT NULL,
                    file_path     VARCHAR NOT NULL UNIQUE,
                    row_count     INTEGER NOT NULL,
                    file_size_bytes BIGINT NOT NULL,
                    columns       VARCHAR NOT NULL,    -- JSON array con nombres de columnas
                    created_at    TIMESTAMP NOT NULL,
                    updated_at    TIMESTAMP NOT NULL
                )
            """)
            # Secuencia para el ID autoincremental
            conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS datasets_id_seq START 1
            """)

    def _register_in_catalog(
        self,
        layer: str,
        target_date: date,
        station: str,
        file_path: Path,
        row_count: int,
        file_size_bytes: int,
        schema_columns: list[str],
    ) -> None:
        """
        Registra o actualiza un archivo en el catálogo DuckDB.

        Usa INSERT OR REPLACE para que sea idempotente:
        si el pipeline corre dos veces para la misma fecha,
        actualiza el registro en vez de duplicarlo.
        """
        import json

        now = datetime.now(tz=UTC)
        columns_json = json.dumps(schema_columns)

        with duckdb.connect(str(self._db_path)) as conn:
            # Verificar si ya existe un registro para este archivo
            existing = conn.execute(
                "SELECT id FROM datasets WHERE file_path = ?",
                [str(file_path)],
            ).fetchone()

            if existing:
                # Actualizar registro existente
                conn.execute(
                    """
                    UPDATE datasets SET
                        row_count = ?,
                        file_size_bytes = ?,
                        columns = ?,
                        updated_at = ?
                    WHERE file_path = ?
                    """,
                    [row_count, file_size_bytes, columns_json, now, str(file_path)],
                )
                logger.debug(f"Catálogo: actualizado {layer}/{station}/{target_date}")
            else:
                # Insertar nuevo registro
                conn.execute(
                    """
                    INSERT INTO datasets
                        (id, layer, station_id, date, file_path,
                         row_count, file_size_bytes, columns, created_at, updated_at)
                    VALUES
                        (nextval('datasets_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        layer,
                        station,
                        target_date,
                        str(file_path),
                        row_count,
                        file_size_bytes,
                        columns_json,
                        now,
                        now,
                    ],
                )
                logger.debug(f"Catálogo: registrado {layer}/{station}/{target_date}")

    def show_catalog(self) -> None:
        """
        Imprime el catálogo completo en consola.
        Útil para explorar qué datos hay disponibles.
        """
        with duckdb.connect(str(self._db_path)) as conn:
            try:
                df = conn.execute("""
                    SELECT
                        layer,
                        station_id,
                        date,
                        row_count,
                        ROUND(file_size_bytes / 1024.0 / 1024.0, 2) AS size_mb,
                        updated_at
                    FROM datasets
                    ORDER BY date DESC, layer, station_id
                """).pl()

                if df.is_empty():
                    print("El catálogo está vacío — todavía no se procesaron datos.")
                else:
                    print(f"\n{'=' * 60}")
                    print(f"  CATÁLOGO — {len(df)} datasets")
                    print(f"{'=' * 60}")
                    print(df)
                    print()

            except duckdb.CatalogException:
                print("El catálogo todavía no fue inicializado.")

    def catalog_stats(self) -> dict[str, object]:
        """
        Devuelve estadísticas resumidas del catálogo.

        Returns:
            Dict con total de datasets, estaciones únicas, rango de fechas, etc.
        """
        with duckdb.connect(str(self._db_path)) as conn:
            try:
                row = conn.execute("""
                    SELECT
                        COUNT(*)                            AS total_datasets,
                        COUNT(DISTINCT station_id)          AS unique_stations,
                        MIN(date)                           AS earliest_date,
                        MAX(date)                           AS latest_date,
                        SUM(row_count)                      AS total_rows,
                        ROUND(SUM(file_size_bytes) / 1e6, 1) AS total_size_mb
                    FROM datasets
                """).fetchone()

                if row is None:
                    return {}

                return {
                    "total_datasets": row[0],
                    "unique_stations": row[1],
                    "earliest_date": row[2],
                    "latest_date": row[3],
                    "total_rows": row[4],
                    "total_size_mb": row[5],
                }
            except duckdb.CatalogException:
                return {}

    # ------------------------------------------------------------------
    # Helpers privados
    # ------------------------------------------------------------------

    def _build_path(self, layer: str, target_date: date, station: str) -> Path:
        """
        Construye el path de destino con particionado Hive-style.

        Hive-style significa: year=YYYY/month=MM/day=DD/
        Es el formato estándar que entienden Spark, BigQuery, Athena, etc.
        Cuando migremos a GCS en Fase 3, mantenemos exactamente esta estructura.

        Ejemplo:
            data/processed/silver/year=2024/month=01/day=15/KBUF.parquet
        """
        return (
            self._processed_path
            / layer
            / f"year={target_date.year}"
            / f"month={target_date.month:02d}"
            / f"day={target_date.day:02d}"
            / f"{station.upper()}.parquet"
        )
