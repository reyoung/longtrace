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

# Connection string to your PostgreSQL server
# Note: Do not include the database name in the connection string if you want it to use the default 'postgres' db for initialization.
# The library will connect to 'postgres' to check/create the YYYYMMDD database.
connection_string = "host=localhost user=postgres password=yourpassword"

try:
    # This will:
    # 1. Connect to 'postgres'
    # 2. Create database '20251128' (if today is 2025-11-28) if it doesn't exist
    # 3. Connect to '20251128'
    # 4. Create 'records' table if it doesn't exist
    db = longtrace.Database(connection_string)
    
    print(f"Successfully connected to database: {db.db_name}")
    
    # The database object holds the connection pool.
    # It will remain active as long as the object exists.

except Exception as e:
    print(f"An error occurred: {e}")
```

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
