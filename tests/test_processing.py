"""
Tests unitarios para el módulo de procesamiento.

Tests unitarios = prueban una función en aislamiento, sin red ni disco.
Son rápidos (milisegundos) y deben cubrir:
- El camino feliz (input válido → output correcto)
- Los casos edge (input vacío, datos inválidos, duplicados)
"""

import polars as pl
import pytest

from weather_pipeline.processing.schemas import GOLD_SCHEMA, SILVER_SCHEMA
from weather_pipeline.processing.transformers import (
    _calculate_scan_gaps,
    _clean_silver,
    raw_files_to_silver,
    silver_to_gold,
    summarize_day,
    validate_silver,
)

# ---------------------------------------------------------------------------
# Tests de _clean_silver
# ---------------------------------------------------------------------------


class TestCleanSilver:
    """Prueba la función de limpieza del DataFrame Silver."""

    def test_elimina_duplicados(self, sample_silver_df: pl.DataFrame) -> None:
        """Si hay dos filas con mismo station_id + scan_datetime, queda una sola."""
        # Duplicar las primeras dos filas
        df_con_dupes = pl.concat([sample_silver_df, sample_silver_df.head(2)])
        assert len(df_con_dupes) == 8  # 6 originales + 2 duplicados

        resultado = _clean_silver(df_con_dupes)

        assert len(resultado) == 6  # volvemos a 6

    def test_elimina_station_id_invalido(self, sample_silver_df: pl.DataFrame) -> None:
        """Filas con station_id de longitud ≠ 4 deben eliminarse."""
        # Agregar una fila con station_id inválido (muy corto)
        fila_invalida = sample_silver_df.head(1).with_columns(
            pl.lit("KB").alias("station_id")  # solo 2 caracteres
        )
        df_con_invalido = pl.concat([sample_silver_df, fila_invalida])

        resultado = _clean_silver(df_con_invalido)

        # La fila inválida no debe estar
        assert "KB" not in resultado["station_id"].to_list()
        assert len(resultado) == 6

    def test_ordena_por_datetime(self, sample_silver_df: pl.DataFrame) -> None:
        """El resultado debe estar ordenado cronológicamente."""
        # Desordenar el DataFrame
        df_desordenado = sample_silver_df.sample(fraction=1.0, shuffle=True, seed=42)

        resultado = _clean_silver(df_desordenado)

        # Verificar que está ordenado: cada timestamp >= el anterior
        timestamps = resultado["scan_datetime"].to_list()
        assert timestamps == sorted(timestamps)

    def test_dataframe_vacio_no_falla(self, empty_silver_df: pl.DataFrame) -> None:
        """_clean_silver con DataFrame vacío debe devolver DataFrame vacío sin error."""
        resultado = _clean_silver(empty_silver_df)
        assert resultado.is_empty()
        assert resultado.schema == pl.Schema(SILVER_SCHEMA)


# ---------------------------------------------------------------------------
# Tests de _calculate_scan_gaps
# ---------------------------------------------------------------------------


class TestCalculateScanGaps:
    def test_primer_gap_es_null(self, sample_silver_df: pl.DataFrame) -> None:
        """El primer escaneo de cada estación no tiene anterior → gap debe ser null."""
        resultado = _calculate_scan_gaps(sample_silver_df)

        # Ordenado por datetime, el primer registro de KBUF tiene gap null
        primer_gap = resultado.sort("scan_datetime")["gap_minutes"][0]
        assert primer_gap is None

    def test_gap_correcto_entre_escaneos(self, sample_silver_df: pl.DataFrame) -> None:
        """El gap entre escaneos de 6 minutos debe calcularse correctamente."""
        resultado = _calculate_scan_gaps(sample_silver_df)

        # Los escaneos están a 6 minutos de distancia
        # Tomamos los gaps no-null y verificamos que son 6.0
        gaps_validos = resultado["gap_minutes"].drop_nulls().to_list()
        for gap in gaps_validos:
            assert gap == pytest.approx(6.0, abs=0.1)

    def test_gap_entre_horas(self, sample_silver_df: pl.DataFrame) -> None:
        """
        El gap entre el último escaneo de las 10hs (10:12) y
        el primero de las 11hs (11:00) debe ser 48 minutos.
        """
        resultado = _calculate_scan_gaps(sample_silver_df).sort("scan_datetime")

        # El 4to registro (índice 3) es 11:00, cuyo anterior es 10:12
        # 11:00 - 10:12 = 48 minutos
        gap_entre_horas = resultado["gap_minutes"][3]
        assert gap_entre_horas == pytest.approx(48.0, abs=0.1)


# ---------------------------------------------------------------------------
# Tests de silver_to_gold
# ---------------------------------------------------------------------------


class TestSilverToGold:
    def test_produce_una_fila_por_hora(self, sample_silver_df: pl.DataFrame) -> None:
        """Con 3 escaneos a las 10hs y 3 a las 11hs → Gold debe tener 2 filas."""
        gold = silver_to_gold(sample_silver_df)
        assert len(gold) == 2

    def test_scan_count_correcto(self, sample_silver_df: pl.DataFrame) -> None:
        """Cada hora debe tener 3 escaneos."""
        gold = silver_to_gold(sample_silver_df).sort("hour")

        scan_counts = gold["scan_count"].to_list()
        assert all(c == 3 for c in scan_counts)

    def test_columnas_gold(self, sample_silver_df: pl.DataFrame) -> None:
        """Gold debe tener exactamente las columnas del GOLD_SCHEMA."""
        gold = silver_to_gold(sample_silver_df)
        assert set(gold.columns) == set(GOLD_SCHEMA.keys())

    def test_dataframe_vacio_devuelve_vacio(self, empty_silver_df: pl.DataFrame) -> None:
        """silver_to_gold con DataFrame vacío debe devolver DataFrame vacío."""
        gold = silver_to_gold(empty_silver_df)
        assert gold.is_empty()

    def test_max_gap_correcto(self, sample_silver_df: pl.DataFrame) -> None:
        """
        En la hora 11, el primer escaneo tiene gap de 48 min (viene de 10:12).
        El max_gap de esa hora debe ser 48.
        """
        gold = silver_to_gold(sample_silver_df).sort("hour")

        # hora 11 (índice 1)
        max_gap_hora11 = gold["max_gap_minutes"][1]
        assert max_gap_hora11 == pytest.approx(48.0, abs=0.1)


# ---------------------------------------------------------------------------
# Tests de validate_silver
# ---------------------------------------------------------------------------


class TestValidateSilver:
    def test_df_valido_pasa_todos_los_checks(self, sample_silver_df: pl.DataFrame) -> None:
        """Un DataFrame Silver bien formado debe pasar todos los checks."""
        checks = validate_silver(sample_silver_df)

        assert checks["has_rows"] is True
        assert checks["valid_station_ids"] is True
        assert checks["no_duplicates"] is True

    def test_df_vacio_falla_has_rows(self, empty_silver_df: pl.DataFrame) -> None:
        """Un DataFrame vacío debe fallar el check has_rows."""
        checks = validate_silver(empty_silver_df)
        assert checks["has_rows"] is False

    def test_duplicados_fallan_check(self, sample_silver_df: pl.DataFrame) -> None:
        """DataFrame con duplicados debe fallar no_duplicates."""
        df_con_dupes = pl.concat([sample_silver_df, sample_silver_df.head(1)])
        checks = validate_silver(df_con_dupes)
        assert checks["no_duplicates"] is False

    def test_summary_en_checks(self, sample_silver_df: pl.DataFrame) -> None:
        """Los checks deben incluir una clave '_summary' con el resumen."""
        checks = validate_silver(sample_silver_df)
        assert "_summary" in checks
        assert "/" in str(checks["_summary"])  # formato "X/Y checks"


# ---------------------------------------------------------------------------
# Tests de raw_files_to_silver
# ---------------------------------------------------------------------------


class TestRawFilesToSilver:
    def test_lista_vacia_devuelve_df_vacio(self) -> None:
        """Sin archivos de entrada → DataFrame Silver vacío con schema correcto."""
        resultado = raw_files_to_silver([])

        assert resultado.is_empty()
        assert resultado.schema == pl.Schema(SILVER_SCHEMA)

    def test_archivo_valido_produce_una_fila(self, fake_nexrad_file: pl.Path) -> None:
        """Un archivo NEXRAD válido debe producir exactamente 1 fila en Silver."""
        resultado = raw_files_to_silver([fake_nexrad_file])

        assert len(resultado) == 1
        assert resultado["station_id"][0] == "KBUF"

    def test_archivo_invalido_no_falla_pipeline(
        self,
        fake_nexrad_file: pl.Path,
        fake_nexrad_file_invalid: pl.Path,
    ) -> None:
        """
        Si un archivo es corrupto, el pipeline no debe caerse.
        Debe procesar los archivos válidos e ignorar los corruptos.
        """
        resultado = raw_files_to_silver([fake_nexrad_file, fake_nexrad_file_invalid])

        # Solo el archivo válido genera una fila
        assert len(resultado) == 1

    def test_timestamps_son_utc(self, fake_nexrad_file: pl.Path) -> None:
        """Los timestamps en Silver siempre deben ser UTC."""
        resultado = raw_files_to_silver([fake_nexrad_file])

        dtype = resultado.schema["scan_datetime"]
        assert str(dtype) == "Datetime(time_unit='us', time_zone='UTC')"


# ---------------------------------------------------------------------------
# Tests de summarize_day
# ---------------------------------------------------------------------------


class TestSummarizeDay:
    from datetime import date

    def test_resumen_contiene_fecha(self, sample_silver_df: pl.DataFrame) -> None:
        from datetime import date

        resumen = summarize_day(sample_silver_df, date(2024, 1, 15))
        assert "2024-01-15" in resumen

    def test_resumen_df_vacio(self, empty_silver_df: pl.DataFrame) -> None:
        from datetime import date

        resumen = summarize_day(empty_silver_df, date(2024, 1, 15))
        assert "sin datos" in resumen
