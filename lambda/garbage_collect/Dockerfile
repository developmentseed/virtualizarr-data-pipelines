# Build stage
FROM python:3.12-slim AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set uv environment variables
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Copy workspace packages maintaining the directory structure
# The build context is 'lambda/', so we copy relative to that
WORKDIR /build/lambda
COPY virtualizarr-processor ./virtualizarr-processor
COPY garbage_collect ./garbage_collect

# Install using uv with workspace support
WORKDIR /build/lambda/garbage_collect
RUN uv pip install --system --no-cache .

# Runtime stage
FROM python:3.12-slim

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

# Copy application code
WORKDIR /app
COPY garbage_collect ./garbage_collect

# Set Python path to find the module
ENV PYTHONPATH=/app

# Run the handler directly
CMD ["python", "-c", "from garbage_collect.handler import handler; handler()"]
