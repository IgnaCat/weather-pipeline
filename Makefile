.PHONY: help install test test-unit lint format run ingest process clean

# Fecha por defecto: hoy
DATE ?= $(shell date +%Y-%m-%d)
STATION ?= KBUF

help:  ## Mostrar ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Instalar dependencias con uv
	uv sync --extra dev
	uv run pre-commit install

test:  ## Correr todos los tests con cobertura
	uv run pytest tests/ -v

test-unit:  ## Correr solo tests unitarios (sin integración)
	uv run pytest tests/ -v -m "not integration"

test-integration:  ## Correr tests de integración (requiere red)
	uv run pytest tests/ -v -m "integration"

lint:  ## Linting con ruff y mypy
	uv run ruff check src/ tests/
	uv run mypy src/

format:  ## Formatear código con ruff
	uv run ruff format src/ tests/
	uv run ruff check --fix src/ tests/

run:  ## Correr el pipeline completo para una fecha (make run DATE=2024-01-15)
	uv run python -m weather_pipeline.pipeline --date $(DATE) --station $(STATION)

ingest:  ## Solo ingesta de datos
	uv run python -m weather_pipeline.pipeline --date $(DATE) --station $(STATION) --step ingest

process:  ## Solo procesamiento de datos
	uv run python -m weather_pipeline.pipeline --date $(DATE) --station $(STATION) --step process

backfill:  ## Cargar datos históricos (make backfill DATE_FROM=2024-01-01 DATE_TO=2024-01-31)
	uv run python -m weather_pipeline.pipeline --date-from $(DATE_FROM) --date-to $(DATE_TO) --station $(STATION) --backfill

catalog:  ## Mostrar el catálogo de datasets en DuckDB
	uv run python -c "from weather_pipeline.storage.local import LocalStorage; LocalStorage().show_catalog()"

clean:  ## Limpiar caches y archivos temporales
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
