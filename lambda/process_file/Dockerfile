# Build stage
FROM public.ecr.aws/lambda/python:3.12 AS builder

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
COPY process_file ./process_file

# Install using uv with workspace support
WORKDIR /build/lambda/process_file
RUN uv pip install --python /var/lang/bin/python3.12 --target /var/task --no-cache .

# Runtime stage
FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /var/task /var/task

### Set the CMD to your handler
CMD ["handler.handler"]
