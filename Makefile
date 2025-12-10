.PHONY: install install-dev test test-e2e test-cov test-cov-e2e lint format

# Install dependencies (normal packages only)
install:
	uv sync

# Install dependencies (including dev packages)
install-dev:
	uv sync --dev

test:
	uv run pytest tests/ -m "not e2e"

test-e2e:
	uv run pytest tests/ -m e2e

# Run tests with coverage (mocked only)
test-cov:
	uv run pytest tests/ -m "not e2e" --cov=pydantic_bq --cov-branch --cov-report=xml --cov-report=term-missing

# Run tests with coverage (e2e tests)
test-cov-e2e:
	uv run pytest tests/ -m e2e --cov=pydantic_bq --cov-branch --cov-report=xml --cov-report=term-missing

# Lint code
lint:
	uv run ruff check .
	uv run ruff format --check .

# Format code
format:
	uv run ruff check --fix .
	uv run ruff format .

