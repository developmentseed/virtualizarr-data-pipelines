# Build stage
FROM public.ecr.aws/lambda/python:3.12 AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /build/lambda
COPY virtualizarr-processor ./virtualizarr-processor
COPY backfill ./backfill

# Install both local paths explicitly and non-editably so the processor's real
# files (not an editable .pth link to the build stage) land in /var/task.
RUN uv pip install --python /var/lang/bin/python3.12 --target /var/task --no-cache \
    ./virtualizarr-processor ./backfill

# Runtime stage
FROM public.ecr.aws/lambda/python:3.12

COPY --from=builder /var/task /var/task

CMD ["backfill_handlers.partition.handler"]
