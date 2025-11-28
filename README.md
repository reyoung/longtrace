# Longtrace

A Rust-based Python library for logging records to a PostgreSQL database with distributed tracing support.

## Features

- **High Performance**: Implemented in Rust for minimal overhead.
- **Global Singleton**: Easy initialization and access across your application.
- **Tracer Support**: Built-in `Tracer` class for managing spans and parent IDs automatically, with thread-local context support.
- **Asynchronous Batching**: Records are buffered and written to the database in background threads to avoid blocking the main application.
- **Automatic Schema Management**: Automatically creates the necessary tables (`records`) if they don't exist.

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

### Initialization

Initialize the library once at the start of your application.

```python
import longtrace

# Connection string to your PostgreSQL server
connection_string = "host=localhost user=postgres password=yourpassword dbname=longtrace"

# Initialize the global database connection
longtrace.initialize(connection_string)
```

### Using Tracer (Recommended)

The `Tracer` class helps manage `span_id` and `parent_id` automatically, supporting nested spans and thread-local context.

```python
import longtrace
import json

# Create a tracer (can be global or local)
# Optionally provide a root parent_id
tracer = longtrace.Tracer()

# Log a simple event
tracer.log("Application started")

# Start a span
with tracer.span("Processing Request", attr=json.dumps({"request_id": "123"})):
    tracer.log("Step 1 completed")
    
    # Nested span
    with tracer.span("Database Query"):
        tracer.log("Querying user data")
        # ... perform work ...
    
    tracer.log("Step 2 completed")

# Attributes are optional
tracer.log("Simple log without attributes")
```

### Flushing

The library automatically flushes records in the background and when the application exits. However, you can force a flush manually if needed.

```python
longtrace.flush()
```

## Schema

The `records` table is created with the following schema:

```sql
CREATE TABLE records (
    id BIGSERIAL PRIMARY KEY,
    span_id UUID,
    parent_id UUID,
    type INTEGER, -- 0: Log, 1: Span Start, 2: Span End
    timestamp TIMESTAMP,
    message TEXT,
    attr JSONB
);
CREATE INDEX idx_records_parent_id ON records(parent_id);
```
