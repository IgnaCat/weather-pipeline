"""
Configuración centralizada del pipeline.

Usa pydantic-settings para leer variables de entorno o archivo .env.
Todas las configuraciones se definen acá — nunca hardcodear valores en el código.
"""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuración global del pipeline cargada desde .env o variables de entorno."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # --- General ---
    pipeline_env: str = Field(default="development", description="Entorno de ejecución")
    log_level: str = Field(default="INFO", description="Nivel de logging")

    # --- Rutas de datos ---
    data_raw_path: Path = Field(default=Path("data/raw"), description="Datos crudos (Bronze)")
    data_processed_path: Path = Field(
        default=Path("data/processed"), description="Datos procesados (Silver)"
    )
    data_analytics_path: Path = Field(
        default=Path("data/analytics"), description="Datos analíticos (Gold)"
    )

    # --- NOAA NEXRAD ---
    nexrad_bucket: str = Field(
        default="noaa-nexrad-level2",
        description="Bucket S3 público de NOAA NEXRAD",
    )
    nexrad_station: str = Field(
        default="KBUF",
        description="Código ICAO de 4 letras de la estación radar",
    )
    aws_default_region: str = Field(
        default="us-east-1",
        description="Región AWS del bucket NEXRAD",
    )

    # --- Open-Meteo ---
    open_meteo_base_url: str = Field(
        default="https://api.open-meteo.com/v1",
        description="URL base de la API Open-Meteo",
    )

    # --- DuckDB ---
    duckdb_path: Path = Field(
        default=Path("data/catalog.duckdb"),
        description="Ruta del archivo DuckDB local",
    )

    # --- Retry / Rate limiting ---
    pipeline_max_retries: int = Field(default=3, description="Máximo de reintentos")
    pipeline_retry_wait_min: float = Field(
        default=1.0, description="Espera mínima entre reintentos (segundos)"
    )
    pipeline_retry_wait_max: float = Field(
        default=60.0, description="Espera máxima entre reintentos (segundos)"
    )
    pipeline_rate_limit_calls: int = Field(
        default=10, description="Máximo de llamadas por período"
    )
    pipeline_rate_limit_period: float = Field(
        default=1.0, description="Período del rate limit (segundos)"
    )

    def ensure_directories(self) -> None:
        """Crea las carpetas de datos si no existen."""
        for path in [self.data_raw_path, self.data_processed_path, self.data_analytics_path]:
            path.mkdir(parents=True, exist_ok=True)


# Instancia global — importar esto en el resto del proyecto
settings = Settings()
