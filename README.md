# pydantic-bq

A Pydantic-based BigQuery client for type-safe schema definition and data operations.

## Installation

```bash
pip install pydantic-bq
```

Or with uv:
```bash
uv add pydantic-bq
```

## Quick Start

### Define a Model

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

### Connect and Query

```python
from pydantic_bq import DatasetClient

# Using environment variables (BIGQUERY_CREDENTIALS or GOOGLE_APPLICATION_CREDENTIALS)
client = DatasetClient('my_dataset')

# Or with explicit credentials
client = DatasetClient(
    'my_dataset',
    credentials_file='/path/to/service-account.json'
)

# Query rows as Pydantic objects
events = client.table(UserEvent).get_rows(
    where="event_type = 'login'",
    limit=100
)

for event in events:
    print(f"User {event.user_id} logged in at {event.timestamp}")
```

### Insert Data

```python
# Create new events
new_events = [
    UserEvent(user_id=1, event_type='signup', timestamp=datetime.now()),
    UserEvent(user_id=2, event_type='login', timestamp=datetime.now()),
]

# Insert into BigQuery
client.table(UserEvent).add_rows(*new_events)
```

### Raw SQL Queries

```python
# Execute arbitrary SQL
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

The library supports multiple authentication methods (in priority order):

1. **Direct parameters:**
   ```python
   # Base64-encoded service account JSON
   client = DatasetClient('dataset', credentials_base64='...')
   
   # Raw JSON string
   client = DatasetClient('dataset', credentials_json='{"type": "service_account", ...}')
   
   # File path
   client = DatasetClient('dataset', credentials_file='/path/to/creds.json')
   
   # Dict with credentials
   client = DatasetClient('dataset', credentials_dict={...})
   ```

2. **Environment variables (via `.env` file or shell):**
   ```bash
   # BigQuery dataset
   BQ_DATASET=my_dataset
   
   # Google service account credentials (individual fields)
   G_PROJECT_ID=your-project-id
   G_CLIENT_EMAIL=your-service-account@your-project.iam.gserviceaccount.com
   G_PRIVATE_KEY_ID=your-private-key-id
   G_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
   G_CLIENT_ID=your-client-id
   G_CLIENT_X509_CERT_URL=https://www.googleapis.com/robot/v1/metadata/x509/...
   
   # Or use a credentials file instead:
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   ```

3. **With settings configured, no arguments needed:**
   ```python
   from pydantic_bq import DatasetClient
   
   # Uses BQ_DATASET and G_* env vars automatically
   client = DatasetClient()
   ```

## API Reference

### `BQBaseModel`

Base class for defining BigQuery table schemas using Pydantic.

**Supported field types:**
- `str` → STRING
- `int` → INTEGER
- `float` → FLOAT
- `bool` → BOOL
- `datetime` → TIMESTAMP
- `date` → DATE
- `Enum` → STRING
- `Optional[T]` → NULLABLE
- `list[T]` → REPEATED

### `DatasetClient`

Main client for BigQuery operations.

**Methods:**
- `table(model)` - Get table wrapper for CRUD operations
- `view(model)` - Get view wrapper for read operations
- `query(sql)` - Execute raw SQL query
- `create_table(model)` - Create a new table
- `delete_table(model)` - Delete a table
- `recreate_table(model)` - Drop and recreate a table

### `BQTable`

Table wrapper with full CRUD support.

**Methods:**
- `get_rows(fields, where, limit, as_objects)` - Fetch rows
- `count_rows(where)` - Count rows
- `add_rows(*objs)` - Insert rows
- `delete_rows(where)` - Delete rows
- `create()` - Create the table
- `delete()` - Delete the table
- `recreate()` - Drop and recreate

### `BQView`

View wrapper with read-only operations.

**Methods:**
- `get_rows(fields, where, limit, as_objects)` - Fetch rows
- `count_rows(where)` - Count rows

## License

MIT

