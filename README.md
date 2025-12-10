# pydantic-bq

A Pydantic-based BigQuery client for type-safe schema definition and data operations.

[![CI](https://github.com/tutorcruncher/pydantic-bq/actions/workflows/ci.yml/badge.svg)](https://github.com/tutorcruncher/pydantic-bq/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/pydantic-bq.svg)](https://badge.fury.io/py/pydantic-bq)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

## Installation

```bash
pip install pydantic-bq
```

Or with uv:
```bash
uv add pydantic-bq
```

## Quick Start

### 1. Configure Credentials

Create a `.env` file with your BigQuery credentials:

```bash
# Dataset name
BQ_DATASET=my_dataset

# Option 1: Base64-encoded service account JSON (recommended)
BIGQUERY_CREDENTIALS=eyJ0eXBlIjoic2VydmljZV9hY2NvdW50Ii...

# Option 2: Individual fields
G_PROJECT_ID=your-project-id
G_CLIENT_EMAIL=service-account@your-project.iam.gserviceaccount.com
G_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----
...your key here...
-----END PRIVATE KEY-----"
```

### 2. Define a Model

```python
from datetime import datetime
from typing import Optional

from pydantic_bq import BQBaseModel


class UserEvent(BQBaseModel):
    user_id: int
    event_type: str
    timestamp: datetime
    metadata: Optional[str] = None

    class Meta:
        table_id = 'user_events'
        table_description = 'User activity events'
```

### 3. Connect and Query

```python
from pydantic_bq import DatasetClient

# Uses credentials from environment/.env automatically
client = DatasetClient('my_dataset')

# Or use BQ_DATASET from env
client = DatasetClient()

# Query rows as Pydantic objects
events = client.table(UserEvent).get_rows(
    where="event_type = 'login'",
    limit=100
)

for event in events:
    print(f"User {event.user_id} logged in at {event.timestamp}")
```

### 4. Insert Data

```python
from datetime import datetime

# Create new events
new_events = [
    UserEvent(user_id=1, event_type='signup', timestamp=datetime.now()),
    UserEvent(user_id=2, event_type='login', timestamp=datetime.now()),
]

# Insert into BigQuery
client.table(UserEvent).add_rows(*new_events)
```

### 5. Table Management

```python
# Create a table from model schema
client.create_table(UserEvent)

# Delete and recreate (useful for schema changes)
client.recreate_table(UserEvent)

# Delete a table
client.delete_table(UserEvent)
```

### 6. Raw SQL Queries

```python
results = client.query("""
    SELECT user_id, COUNT(*) as event_count
    FROM my_dataset.user_events
    GROUP BY user_id
    ORDER BY event_count DESC
    LIMIT 10
""")

for row in results:
    print(f"User {row['user_id']}: {row['event_count']} events")
```

## Authentication

Credentials are loaded from environment variables (or `.env` file) in this priority:

1. **Base64-encoded JSON** (simplest for deployment):
   ```bash
   BIGQUERY_CREDENTIALS=eyJ0eXBlIjoic2VydmljZV9hY2NvdW50Ii...
   ```
   
   Generate with:
   ```bash
   cat service-account.json | base64
   ```

2. **Individual fields** (useful for local development):
   ```bash
   G_PROJECT_ID=your-project-id
   G_CLIENT_EMAIL=sa@your-project.iam.gserviceaccount.com
   G_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----
   ...
   -----END PRIVATE KEY-----"
   ```

## API Reference

### `BQBaseModel`

Base class for defining BigQuery table schemas using Pydantic.

```python
class MyTable(BQBaseModel):
    # Define fields with type hints
    name: str
    count: int
    price: float
    is_active: bool
    created_at: datetime
    birth_date: date
    tags: list[str] = []
    description: Optional[str] = None

    class Meta:
        table_id = 'my_table'
        table_description = 'Optional description'
```

**Type mappings:**

| Python Type | BigQuery Type | Mode |
|-------------|---------------|------|
| `str` | STRING | REQUIRED |
| `int` | INTEGER | REQUIRED |
| `float` | FLOAT | REQUIRED |
| `bool` | BOOL | REQUIRED |
| `datetime` | TIMESTAMP | REQUIRED |
| `date` | DATE | REQUIRED |
| `Enum` | STRING | REQUIRED |
| `Optional[T]` | T | NULLABLE |
| `list[T]` | T | REPEATED |

### `DatasetClient`

Main client for BigQuery operations.

```python
client = DatasetClient('my_dataset')  # or DatasetClient() to use BQ_DATASET env var
```

**Methods:**

| Method | Description |
|--------|-------------|
| `table(Model)` | Get `BQTable` wrapper for CRUD operations |
| `view(Model)` | Get `BQView` wrapper for read-only operations |
| `query(sql)` | Execute raw SQL, returns `list[dict]` |
| `add_rows(*objs)` | Insert model instances (infers table from type) |
| `create_table(Model)` | Create table from model schema |
| `delete_table(Model)` | Delete a table |
| `recreate_table(Model)` | Drop and recreate table |

### `BQTable`

Table wrapper with full CRUD support.

```python
table = client.table(UserEvent)
```

**Methods:**

| Method | Description |
|--------|-------------|
| `get_rows(fields=None, where=None, limit=None, as_objects=True)` | Fetch rows |
| `count_rows(where=None)` | Count rows |
| `add_rows(*objs, send_as_file=True)` | Insert rows |
| `delete_rows(where)` | Delete matching rows |
| `create()` | Create the table |
| `delete()` | Delete the table |
| `recreate()` | Drop and recreate |

### `BQView`

View wrapper with read-only operations. Same query methods as `BQTable`.

## Development

```bash
# Clone and install
git clone https://github.com/tutorcruncher/pydantic-bq.git
cd pydantic-bq
uv sync --all-extras

# Run tests (mocked)
uv run pytest tests/ -m "not e2e"

# Run E2E tests (requires credentials in .env)
uv run pytest tests/ -m e2e

# Lint and format
uv run ruff check .
uv run ruff format .
```

## License

MIT
