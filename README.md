# longtrace

A long-time trace reporting tool built with Rust + pyo3, using PostgreSQL as the backend database.

## Development Setup

### Prerequisites

- [Docker](https://www.docker.com/get-started)
- [VS Code](https://code.visualstudio.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

### Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/reyoung/longtrace.git
   cd longtrace
   ```

2. Open in VS Code:
   ```bash
   code .
   ```

3. When prompted, click "Reopen in Container" or use the command palette (`F1`) and select "Dev Containers: Reopen in Container".

4. The development environment will be set up automatically with:
   - Rust toolchain with clippy and rustfmt
   - Python 3 with maturin for pyo3 development
   - PostgreSQL 16 database

### Environment Variables

The following environment variables are pre-configured in the development container:

- `DATABASE_URL`: `postgresql://postgres:postgres@db:5432/longtrace`

### Database Connection

**Note: These credentials are for local development only.**

- Host: `db` (within container) or `localhost` (from host machine)
- Port: `5432`
- Database: `longtrace`
- Username: `postgres`
- Password: `postgres`