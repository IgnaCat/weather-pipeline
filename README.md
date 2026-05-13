# 🌩️ Weather Data Engineering Pipeline

**Sistema distribuido para ingestión, transformación y almacenamiento de datos meteorológicos volumétricos.**
Pipeline orquestado con Airflow sobre GCP, procesamiento con Python/Polars, almacenamiento en GCS + BigQuery,
API REST con FastAPI desplegada en Cloud Run, containerizado con Docker y CI/CD con GitHub Actions.

---

## Stack Tecnológico

| Categoría | Tecnología | Para qué |
|---|---|---|
| Lenguaje | Python 3.11+ | Base de todo |
| Procesamiento | Polars | DataFrame rápido y moderno |
| Formato | Parquet + NetCDF/HDF5 | Columnar para data lake |
| Data Lake (local) | DuckDB | Warehouse local en desarrollo |
| Data Lake (cloud) | Google Cloud Storage | Almacenamiento de objetos |
| Data Warehouse | BigQuery | Warehouse en GCP |
| Orquestación | Apache Airflow | DAGs del pipeline |
| API | FastAPI | REST API para servir datos |
| Containers | Docker + Compose | Containerización |
| CI/CD | GitHub Actions | Integración y deploy |

## Arquitectura

```
NOAA NEXRAD (S3) ──┐
                   ├──► Ingesta ──► Raw (Parquet) ──► Procesamiento ──► Processed (Parquet)
Open-Meteo API ────┘                                                         │
                                                                             ▼
                                                                    DuckDB (catálogo local)
                                                                             │
                                                                     [Fase 3] GCS + BigQuery
```

## Modelo de Datos (Medallion)

| Capa | Descripción |
|---|---|
| 🟤 Bronze (raw) | Datos tal cual llegan de NOAA. Append-only. |
| ⚪ Silver (clean) | Limpios, tipados, timestamps UTC, deduplicados. |
| 🟡 Gold (analytics) | Métricas agregadas, promedios horarios, anomalías. |

---

## Setup rápido

### Prerrequisitos
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) instalado

### Instalación

```bash
# Clonar el repo
git clone https://github.com/tu-usuario/weather-pipeline.git
cd weather-pipeline

# Crear entorno virtual e instalar dependencias
uv sync --extra dev

# Configurar variables de entorno
cp .env.example .env
# Editá .env con tus valores (la mayoría tienen defaults razonables)

# Instalar pre-commit hooks
uv run pre-commit install
```

### Correr el pipeline

```bash
# Descargar y procesar datos de hoy
make run

# Correr para una fecha específica
make run DATE=2024-01-15

# Solo ingesta
make ingest

# Solo procesamiento
make process
```

### Tests

```bash
make test          # Todos los tests con cobertura
make test-unit     # Solo tests unitarios (rápidos)
make lint          # Linting con ruff + mypy
```

---

## Estructura del Proyecto

```
weather-pipeline/
├── src/
│   └── weather_pipeline/
│       ├── ingestion/        # Módulos de descarga de datos
│       ├── processing/       # Transformaciones con Polars
│       ├── storage/          # Almacenamiento Parquet + DuckDB
│       ├── config.py         # Configuración centralizada
│       └── pipeline.py       # Orquestador principal
├── tests/                    # pytest: unit + integration
├── dags/                     # [Fase 2] DAGs de Airflow
├── data/
│   ├── raw/                  # Bronze: datos crudos
│   └── processed/            # Silver/Gold: datos procesados
├── docs/                     # Decisiones técnicas, diagramas
├── infra/                    # [Fase 4] Terraform
├── pyproject.toml
└── Makefile
```

---

## Decisiones Técnicas

### ¿Por qué Polars en vez de Pandas?
Polars está escrito en Rust y es entre 5x y 100x más rápido que Pandas para operaciones sobre DataFrames grandes.
Usa Apache Arrow internamente (igual que Spark), lo que facilita la migración a PySpark en Fase 3.
Además es una señal de modernidad en el CV.

### ¿Por qué NOAA NEXRAD?
Los datos de radar Level II son volumétricos (3D), tienen formatos complejos (binario propietario),
y son públicos en un bucket S3 de AWS. Es el tipo de dato que trabajan empresas de meteorología, seguros,
agricultura de precisión y logística. Muy diferente a los típicos datos de redes sociales en portfolios.

### ¿Por qué uv?
Es el gestor de paquetes más rápido del ecosistema Python (escrito en Rust). Reemplaza pip, pip-tools
y virtualenv con una única herramienta. Está ganando adopción muy rápida en la industria.

---

## Recursos

- [NOAA NEXRAD en AWS](https://registry.opendata.aws/noaa-nexrad/)
- [Documentación de Polars](https://pola.rs)
- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [Fundamentals of Data Engineering](https://www.oreilly.com/library/view/fundamentals-of-data/9781098108298/)
