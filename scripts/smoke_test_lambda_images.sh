#!/usr/bin/env bash
# Build each Lambda image and import its handler(s) inside the real runtime image.
# Catches packaging regressions (e.g. an editable path source leaving
# virtualizarr_processor out of the image) BEFORE a deploy, at build time rather
# than as a Runtime.ImportModuleError at execution time.
#
# Usage: ./scripts/smoke_test_lambda_images.sh
# Requires: docker.
#
# NOTE: a real processor with lazily-imported optional deps (imported inside a
# function, not at module top) can still fail only at execution time. When you add
# such deps, force them here too, e.g. append `; import <dep>` to the import check.
set -euo pipefail

cd "$(dirname "$0")/../lambda"

# name | dockerfile | import statement (as the runtime CMD imports it) | workdir
check() {
  local name="$1" dockerfile="$2" stmt="$3" workdir="${4:-/var/task}"
  printf '==> %-16s build... ' "$name"
  docker build --platform linux/amd64 -f "$dockerfile" -t "lambda-smoke-$name" . >/dev/null
  printf 'import... '
  docker run --rm --platform linux/amd64 --entrypoint python --workdir "$workdir" \
    "lambda-smoke-$name" -c "$stmt; print('OK')"
}

check backfill "backfill/Dockerfile" \
  "import backfill_handlers.init, backfill_handlers.partition, backfill_handlers.fork, backfill_handlers.worker, backfill_handlers.reduce, backfill_handlers.promote"
check process_messages "process_messages/Dockerfile" "import handler"
check initialize "initialize/Dockerfile" "import handler"
check garbage_collect "garbage_collect/Dockerfile" \
  "from garbage_collect.handler import handler" "/app"

echo "All lambda image smoke tests passed."
