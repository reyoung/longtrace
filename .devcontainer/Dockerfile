# Development container for longtrace
# Rust + Python + pyo3 development environment

FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

# Install Python and development dependencies
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    libpq-dev \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install maturin for building pyo3 projects
# Note: --break-system-packages is safe in a container environment
# where Python package isolation is not needed
RUN pip3 install --break-system-packages maturin

# Set up Rust components
RUN rustup component add clippy rustfmt

# Set environment variables for pyo3
ENV PYTHONUNBUFFERED=1
