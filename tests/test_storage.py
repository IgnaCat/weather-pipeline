"""
Tests para el módulo de almacenamiento local.

Estos tests usan tmp_path — un fixture de pytest que crea una carpeta
temporal y la borra automáticamente al terminar. Así no ensuciamos
el proyecto con archivos de test.
"""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from weather_pipeline.storage.local import LocalStorage

# ---------------------------------------------------------------------------
# Fixture: storage apuntando a carpetas temporales
# ---------------------------------------------------------------------------


@pytest.fixture
def storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> LocalStorage:
    """
    LocalStorage configurado para usar carpetas temporales en vez de data/.

    monkeypatch es otro fixture especial de pytest: permite modificar
    temporalmente variables, atributos o settings durante un test.
    Al terminar el test, todo vuelve al estado original.
    """
    # Redirigir los paths de settings a la carpeta temporal
    monkeypatch.setattr(
        "weather_pipeline.storage.local.settings.data_processed_path", tmp_path / "processed"
    )
    monkeypatch.setattr(
        "weather_pipeline.storage.local.settings.duckdb_path", tmp_path / "catalog.duckdb"
    )
    return LocalStorage()


# ---------------------------------------------------------------------------
# Tests de save/load
# ---------------------------------------------------------------------------


class TestSaveLoad:
    def test_save_silver_crea_archivo(
        self,
        storage: LocalStorage,
        sample_silver_df: pl.DataFrame,
    ) -> None:
        """save_silver debe crear un archivo .parquet en disco."""
        path = storage.save_silver(sample_silver_df, date(2024, 1, 15), "KBUF")

        assert path.exists()
        assert path.suffix == ".parquet"

    def test_save_crea_estructura_hive(
        self,
        storage: LocalStorage,
        sample_silver_df: pl.DataFrame,
    ) -> None:
        """El archivo debe estar en la carpeta year=YYYY/month=MM/day=DD/."""
        path = storage.save_silver(sample_silver_df, date(2024, 1, 15), "KBUF")

        assert "year=2024" in str(path)
        assert "month=01" in str(path)
        assert "day=15" in str(path)

    def test_load_silver_devuelve_mismo_df(
        self,
        storage: LocalStorage,
        sample_silver_df: pl.DataFrame,
    ) -> None:
        """Lo que se guarda debe ser idéntico a lo que se carga."""
        storage.save_silver(sample_silver_df, date(2024, 1, 15), "KBUF")
        cargado = storage.load_silver(date(2024, 1, 15), "KBUF")

        assert len(cargado) == len(sample_silver_df)
        assert set(cargado.columns) == set(sample_silver_df.columns)

    def test_load_sin_archivo_devuelve_vacio(self, storage: LocalStorage) -> None:
        """Si no hay archivo para esa fecha/estación, load devuelve DataFrame vacío."""
        resultado = storage.load_silver(date(2099, 1, 1), "KXYZ")
        assert resultado.is_empty()

    def test_save_df_vacio_no_crea_archivo(
        self,
        storage: LocalStorage,
        empty_silver_df: pl.DataFrame,
    ) -> None:
        """Un DataFrame vacío no debe generar un archivo Parquet."""
        path = storage.save_silver(empty_silver_df, date(2024, 1, 15), "KBUF")
        assert not path.exists()


# ---------------------------------------------------------------------------
# Tests del catálogo DuckDB
# ---------------------------------------------------------------------------


class TestCatalog:
    def test_registro_en_catalogo(
        self,
        storage: LocalStorage,
        sample_silver_df: pl.DataFrame,
    ) -> None:
        """Después de save_silver, el catálogo debe tener un registro."""
        storage.save_silver(sample_silver_df, date(2024, 1, 15), "KBUF")

        stats = storage.catalog_stats()
        assert stats["total_datasets"] == 1
        assert stats["unique_stations"] == 1

    def test_registro_idempotente(
        self,
        storage: LocalStorage,
        sample_silver_df: pl.DataFrame,
    ) -> None:
        """
        Guardar el mismo dataset dos veces no debe duplicar el registro en el catálogo.
        Esta es la propiedad de idempotencia.
        """
        storage.save_silver(sample_silver_df, date(2024, 1, 15), "KBUF")
        storage.save_silver(sample_silver_df, date(2024, 1, 15), "KBUF")  # segunda vez

        stats = storage.catalog_stats()
        assert stats["total_datasets"] == 1  # sigue siendo 1, no 2

    def test_catalogo_registra_row_count(
        self,
        storage: LocalStorage,
        sample_silver_df: pl.DataFrame,
    ) -> None:
        """El catálogo debe registrar la cantidad de filas correcta."""
        storage.save_silver(sample_silver_df, date(2024, 1, 15), "KBUF")

        stats = storage.catalog_stats()
        assert stats["total_rows"] == len(sample_silver_df)

    def test_multiples_fechas(
        self,
        storage: LocalStorage,
        sample_silver_df: pl.DataFrame,
    ) -> None:
        """Guardar datos de dos fechas distintas → catálogo con 2 datasets."""
        storage.save_silver(sample_silver_df, date(2024, 1, 15), "KBUF")
        storage.save_silver(sample_silver_df, date(2024, 1, 16), "KBUF")

        stats = storage.catalog_stats()
        assert stats["total_datasets"] == 2

    def test_catalogo_vacio_devuelve_dict_vacio(self, storage: LocalStorage) -> None:
        """Sin datos guardados, catalog_stats devuelve 0 datasets."""
        stats = storage.catalog_stats()
        # El catálogo existe pero está vacío
        assert stats.get("total_datasets", 0) == 0


# ---------------------------------------------------------------------------
# Tests de _build_path
# ---------------------------------------------------------------------------


class TestBuildPath:
    def test_nombre_estacion_en_mayusculas(
        self,
        storage: LocalStorage,
        sample_silver_df: pl.DataFrame,
    ) -> None:
        """La estación debe normalizarse a mayúsculas en el path."""
        path_lower = storage.save_silver(sample_silver_df, date(2024, 1, 15), "kbuf")
        assert "KBUF.parquet" in path_lower.name

    def test_mes_con_cero_adelante(
        self,
        storage: LocalStorage,
        sample_silver_df: pl.DataFrame,
    ) -> None:
        """Los meses del 1 al 9 deben tener un cero adelante (01, 02, ...)."""
        path = storage.save_silver(sample_silver_df, date(2024, 3, 5), "KBUF")
        assert "month=03" in str(path)
        assert "day=05" in str(path)
