# Longtrace

A Rust-based Python library for logging records to a PostgreSQL database.

## Features

- Exposes a `Database` object to Python.
- Automatically creates a database named after the current date (YYYYMMDD).
- Manages a connection pool.
- Creates a `records` table with the specified schema.

## Prerequisites

- Rust (cargo)
- Python 3.7+
- PostgreSQL server

## Installation

1. Install `maturin`:
   ```bash
   pip install maturin
   ```

2. Build and install the package:
   ```bash
   maturin develop
   # or for release
   maturin develop --release
   ```

## Development

To run unit tests (requires a running PostgreSQL instance):

```bash
# The tests expect a local Postgres instance. 
# You can override the connection string with DATABASE_URL environment variable.
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres"
cargo test --no-default-features
```

## Usage

```python
import longtrace
import uuid

# Connection string to your PostgreSQL server
# Note: Do not include the database name in the connection string if you want it to use the default 'postgres' db for initialization.
# The library will connect to 'postgres' to check/create the YYYYMMDD database.
connection_string = "host=localhost user=postgres password=yourpassword"

try:
    # Create database with default batch size (1024)
    db = longtrace.Database(connection_string)
    
    # Or specify a custom batch size
    # db = longtrace.Database(connection_string, batch_size=500)
    
    print(f"Successfully connected to database: {db.db_name}")
    
    # Report records (asynchronous, batched)
    # Note: It's recommended to use UUID v7 for better time-based sorting
    span_id = str(uuid.uuid7())  # or uuid.uuid4()
    parent_id = str(uuid.uuid7())
    
    db.report(
        message="User logged in",
        span_id=span_id,
        parent_id=parent_id,
        attr='{"user_id": 123, "ip": "192.168.1.1"}'
    )
    
    # Report multiple records
    for i in range(100):
        db.report(
            message=f"Processing item {i}",
            span_id=str(uuid.uuid7()),
            parent_id=span_id,
            attr=f'{{"item_id": {i}}}'
        )
    
    # Manually flush pending records to database
    db.flush()
    
    # The database object holds the connection pool and batch writer thread.
    # It will remain active as long as the object exists.
    # On destruction, it will automatically flush any pending records.

except Exception as e:
    print(f"An error occurred: {e}")
```

## Features

### Asynchronous Batch Writing
- Records are collected in batches and written asynchronously in a background thread
- Default batch size is 1024 records (configurable)
- Automatic flushing when batch size is reached
- Manual flush available via `flush()` method
- Automatic flush on database object destruction

### Date-based Database Creation

## Schema

The `records` table is created with the following schema:

```sql
CREATE TABLE records (
    id BIGSERIAL PRIMARY KEY,
    span_id UUID,
    parent_id UUID,
    type INTEGER,
    timestamp TIMESTAMP,
    message TEXT,
    attr JSONB
);
CREATE INDEX idx_records_parent_id ON records(parent_id);
```
