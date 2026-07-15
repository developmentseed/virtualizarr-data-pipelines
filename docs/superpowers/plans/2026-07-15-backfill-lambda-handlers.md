# Backfill Lambda Handlers (sub-project B) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the backfill Lambda layer — an `open_backfill_repo()` Protocol method plus six handler functions (`partition`, `init`, `fork`, `worker`, `reduce`, `promote`) and shared S3 helpers — tested end-to-end locally with `moto` (mocked S3) and `local_filesystem_storage` (repo), no AWS or CDK.

**Architecture:** Each handler is a thin, single-responsibility function that parses a JSON event, calls the sub-project-A backfill helpers/Processor methods, and reads/writes S3 artifacts (partition manifests, pickled fork blobs) via boto3. A per-run S3 prefix threads through the flow. The durable repo is opened via a new env-configurable Protocol method (S3 in Lambda, local filesystem in tests). No state-machine wiring (that is sub-project C).

**Tech Stack:** Python 3.11+, `uv`, `pytest`, `boto3`, `moto` (dev), `icechunk` 1.1.14, `zarr` 3.1.5, `aws-lambda-powertools`. The env-configurable `open_backfill_repo` was prototyped and verified (yields a `main` branch; re-opening the durable path returns the same repo). The fork/merge chain is proven in sub-project A.

**Spec:** `docs/superpowers/specs/2026-07-15-backfill-lambda-handlers-design.md`
**Overview:** `docs/superpowers/backfill-pipeline-overview.md`

---

## Critical implementation notes

1. **Isolated pre-commit mypy has no icechunk/boto3.** The `mirrors-mypy` hook runs in an env
   without these packages, with `warn_return_any = true`. A function annotated to return a
   **concrete** type (e.g. `-> str`, `-> list[str]`) whose return expression comes from an
   untyped source (`session.commit()`, `json.loads(...)`, a boto3 call) trips `[no-any-return]`
   and must wrap the return in `typing.cast(...)`. A function annotated `-> Repository` (or any
   icechunk type, which resolves to `Any` in that env) does NOT need a cast — this is why the
   existing `initialize_repo` needs none but the A helpers returning `-> str` do. Each task below
   already places the required casts; keep their explanatory comments.
2. **mypy `disallow_untyped_defs = true`, and the pre-commit mypy hook checks test files too**
   (it has no `files:` restriction, so pre-commit passes every staged `.py` — including tests).
   Every function needs fully typed params AND return. For the **test snippets below**, the code
   is shown with `-> None` but you MUST also annotate the fixture params (as sub-project A's tests
   do). Use exactly these types and add the imports:
   - `s3_bucket: str`
   - `tmp_path: Path` (add `from pathlib import Path`)
   - `monkeypatch: pytest.MonkeyPatch` (add `import pytest`)
   Handlers use `context: LambdaContext` (matching the existing `process_messages`/`initialize`
   handlers). Note: because the isolated pre-commit mypy env has no icechunk/boto3/powertools/
   backfill_handlers installed, those imports resolve to `Any`, so calling
   `handler(event, None)` in tests and calling processor/boto3 methods in handlers raise no
   arg-type errors there — only `disallow_untyped_defs` and `warn_return_any` on the file's own
   defs matter.
3. **Repo durability:** backfill uses durable storage. In tests, `open_backfill_repo` reads
   `ICECHUNK_LOCAL_PATH` and uses `local_filesystem_storage`; the same path must persist across
   the handler calls within a test so forks resolve their base snapshot.
4. All new S3 artifact I/O goes through boto3 so `moto` can mock it. Inventory and partition
   manifests are JSON arrays of string keys.

## File Structure

- **Modify `lambda/virtualizarr-processor/virtualizarr_processor/typing.py`** — add
  `open_backfill_repo` to the Protocol.
- **Modify `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`** — env-configurable
  reference `open_backfill_repo`.
- **Create `lambda/backfill/pyproject.toml`** — the `backfill_handlers` package.
- **Create `lambda/backfill/backfill_handlers/__init__.py`** — empty package marker.
- **Create `lambda/backfill/backfill_handlers/config.py`** — `parse_s3_uri`, `s3_client`.
- **Create `lambda/backfill/backfill_handlers/inventory.py`** — `read_inventory`, `write_manifest`, `read_manifest`.
- **Create `lambda/backfill/backfill_handlers/fork_store.py`** — `save_fork`, `load_fork`, `list_forks`.
- **Create `lambda/backfill/backfill_handlers/partition.py` / `init.py` / `fork.py` / `worker.py` / `reduce.py` / `promote.py`** — the six handlers.
- **Modify root `pyproject.toml`** — add `boto3`, `moto`, and the editable `backfill-handlers` to the `dev` group + `[tool.uv.sources]`.
- **Create `tests/backfill_handlers/conftest.py`** — moto S3 bucket + repo-env fixtures.
- **Create `tests/backfill_handlers/test_*.py`** — per-module tests + the end-to-end chain test.

No `cdk/` or existing `lambda/*/handler.py` code is touched. The Dockerfile is deferred to C.

---

### Task 1: Package scaffold + dependency wiring

**Files:**
- Create: `lambda/backfill/pyproject.toml`, `lambda/backfill/backfill_handlers/__init__.py`
- Modify: root `pyproject.toml`
- Test: `tests/backfill_handlers/test_package_imports.py`

- [ ] **Step 1: Write the failing test**

Create `tests/backfill_handlers/test_package_imports.py`:

```python
def test_backfill_handlers_package_importable() -> None:
    import backfill_handlers

    assert backfill_handlers is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/backfill_handlers/test_package_imports.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_handlers'`.

- [ ] **Step 3: Create the package**

Create `lambda/backfill/backfill_handlers/__init__.py` (empty file).

Create `lambda/backfill/pyproject.toml`:

```toml
[project]
name = "backfill-handlers"
version = "0.1.0"
description = "Backfill Lambda handlers"
requires-python = ">=3.11"
dependencies = [
    "aws-lambda-powertools>=2.30.0",
    "aws-xray-sdk",
    "boto3>=1.34.0",
    "icechunk",
    "virtualizarr-processor",
]

[tool.uv.sources]
virtualizarr-processor = { path = "../virtualizarr-processor" }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["backfill_handlers"]
```

- [ ] **Step 4: Wire it into the root project**

In the root `pyproject.toml`, add to `[tool.uv.sources]`:

```toml
backfill-handlers = { path = "lambda/backfill", editable = true }
```

And add these three entries to the `[dependency-groups] dev` list:

```toml
  "backfill-handlers",
  "boto3>=1.34.0",
  "moto>=5.0.0",
```

Then sync:

Run: `uv sync`
Expected: resolves and installs `boto3`, `moto`, and the editable `backfill-handlers`.

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_package_imports.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add lambda/backfill/pyproject.toml lambda/backfill/backfill_handlers/__init__.py pyproject.toml uv.lock tests/backfill_handlers/test_package_imports.py
git commit -m "build: scaffold backfill_handlers package + boto3/moto deps"
```

---

### Task 2: `open_backfill_repo` Protocol method + reference impl

**Files:**
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/typing.py`
- Modify: `lambda/virtualizarr-processor/virtualizarr_processor/processor.py`
- Test: `tests/test_backfill.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_backfill.py`:

```python
def test_open_backfill_repo_local_filesystem(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    processor = Processor()

    repo = processor.open_backfill_repo()

    assert isinstance(repo, icechunk.Repository)
    assert "main" in repo.list_branches()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_backfill.py::test_open_backfill_repo_local_filesystem -v`
Expected: FAIL with `AttributeError: 'Processor' object has no attribute 'open_backfill_repo'`.

- [ ] **Step 3a: Add the Protocol method to `typing.py`**

Inside the `VirtualizarrProcessor` Protocol class (after `initialize_backfill_store`), add:

```python
    def open_backfill_repo(self) -> Repository:
        """
        Open (or create) the durable backfill repository.

        Storage is chosen by the implementation (e.g. S3 in a deployed Lambda,
        local filesystem in tests). Must use durable, shared storage — a pickled
        ForkSession cannot resolve its base snapshot from in-memory storage.
        Uses open_or_create semantics so the `main` branch exists for
        initialize_backfill_store to branch off.

        Returns
        -------
        Repository
            An Icechunk Repository backed by durable storage.
        """
        ...
```

- [ ] **Step 3b: Add the reference implementation to `processor.py`**

Add this method to the `Processor` class (near `initialize_backfill_store`). It reuses the
module-level `CHUNK_DIR` / `CHUNK_DIRECTORY_URL_PREFIX`:

```python
    def open_backfill_repo(self) -> Repository:
        chunk_store = icechunk.local_filesystem_store(CHUNK_DIR)
        bucket = os.environ.get("ICECHUNK_BUCKET")
        if bucket:
            storage = icechunk.s3_storage(
                bucket=bucket,
                prefix=os.environ.get("ICECHUNK_PREFIX"),
                region=os.environ.get("ICECHUNK_REGION"),
                from_env=True,
            )
        else:
            storage = icechunk.local_filesystem_storage(
                os.environ["ICECHUNK_LOCAL_PATH"]
            )
        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(CHUNK_DIRECTORY_URL_PREFIX, chunk_store)
        )
        return icechunk.Repository.open_or_create(
            storage=storage,
            config=config,
            authorize_virtual_chunk_access={CHUNK_DIRECTORY_URL_PREFIX: None},
        )
```

(No `cast` needed: the return type `Repository` resolves to `Any` in the isolated mypy env, so
`warn_return_any` does not fire — same as the existing `initialize_repo`.)

- [ ] **Step 4: Run tests to verify they pass (and conformance stays green)**

Run: `uv run pytest tests/test_backfill.py -k "open_backfill_repo" tests/test_example.py::test_follows_protocol -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add lambda/virtualizarr-processor/virtualizarr_processor/typing.py lambda/virtualizarr-processor/virtualizarr_processor/processor.py tests/test_backfill.py
git commit -m "feat: open_backfill_repo Protocol method + env-configurable reference impl"
```

---

### Task 3: `config.py` (S3 plumbing) + `inventory.py`

**Files:**
- Create: `lambda/backfill/backfill_handlers/config.py`, `lambda/backfill/backfill_handlers/inventory.py`
- Create: `tests/backfill_handlers/conftest.py`
- Test: `tests/backfill_handlers/test_inventory.py`

- [ ] **Step 1: Write the failing test + fixtures**

Create `tests/backfill_handlers/conftest.py`:

```python
from collections.abc import Iterator

import boto3
import pytest
from moto import mock_aws

BUCKET = "test-backfill-bucket"


@pytest.fixture()
def s3_bucket() -> Iterator[str]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield BUCKET
```

Create `tests/backfill_handlers/test_inventory.py`:

```python
import boto3
from backfill_handlers import inventory


def test_read_inventory_returns_keys(s3_bucket: str) -> None:
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=s3_bucket, Key="inv.json", Body=b'["a", "b", "c"]'
    )
    assert inventory.read_inventory(f"s3://{s3_bucket}/inv.json") == ["a", "b", "c"]


def test_write_then_read_manifest_round_trips(s3_bucket: str) -> None:
    uri = f"s3://{s3_bucket}/partitions/0.json"
    inventory.write_manifest(uri, ["k1", "k2"])
    assert inventory.read_manifest(uri) == ["k1", "k2"]
```

The `s3_bucket` fixture yields the bucket name, so tests use that value directly rather than
importing a constant (there is no `tests/__init__.py`, so `tests.backfill_handlers.conftest` is
not importable as a module).

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/backfill_handlers/test_inventory.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_handlers.inventory'`.

- [ ] **Step 3: Implement `config.py` then `inventory.py`**

Create `lambda/backfill/backfill_handlers/config.py`:

```python
"""Shared S3 plumbing for the backfill handlers."""

from typing import Any


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into ``(bucket, key)``."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 uri: {uri}")
    bucket, _, key = uri[len("s3://") :].partition("/")
    return bucket, key


def s3_client() -> Any:
    """Return a boto3 S3 client (region from the AWS environment)."""
    import boto3

    return boto3.client("s3")
```

Create `lambda/backfill/backfill_handlers/inventory.py`:

```python
"""Read the S3 inventory file and read/write partition manifests (JSON key lists)."""

import json
from typing import cast

from backfill_handlers.config import parse_s3_uri, s3_client


def read_inventory(uri: str) -> list[str]:
    """Read a JSON array of file keys from the inventory object."""
    bucket, key = parse_s3_uri(uri)
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    # cast: json.loads returns Any; the isolated mypy env would flag the return.
    return cast(list[str], json.loads(body))


def write_manifest(uri: str, keys: list[str]) -> None:
    """Write a partition manifest (JSON array of keys) to S3."""
    bucket, key = parse_s3_uri(uri)
    s3_client().put_object(Bucket=bucket, Key=key, Body=json.dumps(keys).encode())


def read_manifest(uri: str) -> list[str]:
    """Read a partition manifest (JSON array of keys) from S3."""
    bucket, key = parse_s3_uri(uri)
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    return cast(list[str], json.loads(body))
```

Note: the tests must run with `mock_aws` active before any boto3 client is created; the
`s3_bucket` fixture guarantees this because the handler code calls `s3_client()` lazily inside
each function (not at import time).

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_inventory.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill/backfill_handlers/config.py lambda/backfill/backfill_handlers/inventory.py tests/backfill_handlers/conftest.py tests/backfill_handlers/test_inventory.py
git commit -m "feat: backfill config + inventory S3 helpers"
```

---

### Task 4: `fork_store.py`

**Files:**
- Create: `lambda/backfill/backfill_handlers/fork_store.py`
- Test: `tests/backfill_handlers/test_fork_store.py`

- [ ] **Step 1: Write the failing test**

Create `tests/backfill_handlers/test_fork_store.py`:

```python
from backfill_handlers import fork_store


def test_save_then_load_round_trips(s3_bucket: str) -> None:
    uri = f"s3://{s3_bucket}/forks/0/in/fork.pkl"
    fork_store.save_fork(uri, b"\x01\x02\x03")
    assert fork_store.load_fork(uri) == b"\x01\x02\x03"


def test_list_forks_returns_all_uris_under_prefix(s3_bucket: str) -> None:
    prefix = f"s3://{s3_bucket}/forks/0/out/"
    fork_store.save_fork(prefix + "a.pkl", b"a")
    fork_store.save_fork(prefix + "b.pkl", b"b")
    listed = fork_store.list_forks(prefix)
    assert sorted(listed) == [prefix + "a.pkl", prefix + "b.pkl"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/backfill_handlers/test_fork_store.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_handlers.fork_store'`.

- [ ] **Step 3: Implement `fork_store.py`**

Create `lambda/backfill/backfill_handlers/fork_store.py`:

```python
"""Save, load, and list pickled fork blobs in S3."""

from typing import cast

from backfill_handlers.config import parse_s3_uri, s3_client


def save_fork(uri: str, data: bytes) -> None:
    """Write a pickled fork blob to S3."""
    bucket, key = parse_s3_uri(uri)
    s3_client().put_object(Bucket=bucket, Key=key, Body=data)


def load_fork(uri: str) -> bytes:
    """Read a pickled fork blob from S3."""
    bucket, key = parse_s3_uri(uri)
    return cast(bytes, s3_client().get_object(Bucket=bucket, Key=key)["Body"].read())


def list_forks(prefix: str) -> list[str]:
    """List all object URIs under an ``s3://`` prefix."""
    bucket, key_prefix = parse_s3_uri(prefix)
    paginator = s3_client().get_paginator("list_objects_v2")
    uris: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            uris.append(f"s3://{bucket}/{obj['Key']}")
    return uris
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_fork_store.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill/backfill_handlers/fork_store.py tests/backfill_handlers/test_fork_store.py
git commit -m "feat: backfill fork_store S3 helpers"
```

---

### Task 5: `partition` handler

**Files:**
- Create: `lambda/backfill/backfill_handlers/partition.py`
- Test: `tests/backfill_handlers/test_partition.py`

- [ ] **Step 1: Write the failing test**

Create `tests/backfill_handlers/test_partition.py`:

```python
import boto3
from backfill_handlers import inventory, partition


def test_partition_splits_inventory_into_manifests(s3_bucket: str) -> None:
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=s3_bucket, Key="inv.json", Body=b'["0", "1", "2", "3", "4"]'
    )
    event = {
        "inventory_uri": f"s3://{s3_bucket}/inv.json",
        "run_prefix": f"s3://{s3_bucket}/run/",
        "partition_size": 2,
    }

    result = partition.handler(event, None)

    parts = result["partitions"]
    assert [p["partition_id"] for p in parts] == ["0", "1", "2"]
    assert inventory.read_manifest(parts[0]["manifest_uri"]) == ["0", "1"]
    assert inventory.read_manifest(parts[2]["manifest_uri"]) == ["4"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/backfill_handlers/test_partition.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_handlers.partition'`.

- [ ] **Step 3: Implement `partition.py`**

Create `lambda/backfill/backfill_handlers/partition.py`:

```python
"""Handler: split the S3 inventory into partition manifests."""

from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from backfill_handlers import inventory

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    keys = inventory.read_inventory(event["inventory_uri"])
    size = int(event["partition_size"])
    run_prefix = event["run_prefix"]

    partitions: list[dict[str, str]] = []
    for i in range(0, len(keys), size):
        partition_id = str(i // size)
        manifest_uri = f"{run_prefix}partitions/{partition_id}.json"
        inventory.write_manifest(manifest_uri, keys[i : i + size])
        partitions.append(
            {"partition_id": partition_id, "manifest_uri": manifest_uri}
        )

    logger.info("Partitioned inventory", extra={"count": len(partitions)})
    return {"partitions": partitions}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_partition.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill/backfill_handlers/partition.py tests/backfill_handlers/test_partition.py
git commit -m "feat: partition handler"
```

---

### Task 6: `init` handler

**Files:**
- Create: `lambda/backfill/backfill_handlers/init.py`
- Test: `tests/backfill_handlers/test_init.py`

- [ ] **Step 1: Write the failing test**

Create `tests/backfill_handlers/test_init.py`:

```python
from backfill_handlers import init


def test_init_creates_backfill_branch(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))

    result = init.handler({}, None)

    assert isinstance(result["base_snapshot"], str) and result["base_snapshot"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/backfill_handlers/test_init.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_handlers.init'`.

- [ ] **Step 3: Implement `init.py`**

Create `lambda/backfill/backfill_handlers/init.py`:

```python
"""Handler: create the full-shape backfill store and commit (clean base)."""

from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor.processor import Processor

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    processor = Processor()
    repo = processor.open_backfill_repo()
    base_snapshot = processor.initialize_backfill_store(repo)
    logger.info("Initialized backfill store", extra={"base_snapshot": base_snapshot})
    return {"base_snapshot": base_snapshot}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_init.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill/backfill_handlers/init.py tests/backfill_handlers/test_init.py
git commit -m "feat: init handler"
```

---

### Task 7: `fork` handler

**Files:**
- Create: `lambda/backfill/backfill_handlers/fork.py`
- Test: `tests/backfill_handlers/test_fork.py`

- [ ] **Step 1: Write the failing test**

Create `tests/backfill_handlers/test_fork.py`:

```python
from backfill_handlers import fork, fork_store, init


def test_fork_writes_shared_fork_blob(s3_bucket, tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    init.handler({}, None)

    event = {
        "partition_id": "0",
        "manifest_uri": f"s3://{s3_bucket}/run/partitions/0.json",
        "run_prefix": f"s3://{s3_bucket}/run/",
    }
    result = fork.handler(event, None)

    assert result["fork_in_uri"] == f"s3://{s3_bucket}/run/forks/0/in/fork.pkl"
    assert result["forks_out_prefix"] == f"s3://{s3_bucket}/run/forks/0/out/"
    assert result["partition_id"] == "0"
    assert result["manifest_uri"] == event["manifest_uri"]
    # the shared fork blob exists and is non-empty
    assert len(fork_store.load_fork(result["fork_in_uri"])) > 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/backfill_handlers/test_fork.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_handlers.fork'`.

- [ ] **Step 3: Implement `fork.py`**

Create `lambda/backfill/backfill_handlers/fork.py`:

```python
"""Handler: create one shared fork for a partition and write it to S3."""

from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from backfill_handlers import fork_store
from virtualizarr_processor import backfill
from virtualizarr_processor.processor import Processor

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    partition_id = event["partition_id"]
    run_prefix = event["run_prefix"]
    fork_in_uri = f"{run_prefix}forks/{partition_id}/in/fork.pkl"
    forks_out_prefix = f"{run_prefix}forks/{partition_id}/out/"

    processor = Processor()
    repo = processor.open_backfill_repo()
    fork_store.save_fork(fork_in_uri, backfill.create_fork(repo))

    logger.info("Created shared fork", extra={"partition_id": partition_id})
    return {
        "partition_id": partition_id,
        "manifest_uri": event["manifest_uri"],
        "fork_in_uri": fork_in_uri,
        "forks_out_prefix": forks_out_prefix,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_fork.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill/backfill_handlers/fork.py tests/backfill_handlers/test_fork.py
git commit -m "feat: fork handler"
```

---

### Task 8: `worker` handler

**Files:**
- Create: `lambda/backfill/backfill_handlers/worker.py`
- Test: `tests/backfill_handlers/test_worker.py`

- [ ] **Step 1: Write the failing test**

Create `tests/backfill_handlers/test_worker.py`:

```python
from backfill_handlers import fork, fork_store, init, worker


def test_worker_writes_child_fork(s3_bucket, tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    init.handler({}, None)
    fork_result = fork.handler(
        {
            "partition_id": "0",
            "manifest_uri": f"s3://{s3_bucket}/run/partitions/0.json",
            "run_prefix": f"s3://{s3_bucket}/run/",
        },
        None,
    )

    event = {
        "fork_in_uri": fork_result["fork_in_uri"],
        "forks_out_prefix": fork_result["forks_out_prefix"],
        "file_keys": ["0", "1", "2"],
    }
    result = worker.handler(event, None)

    assert result["child_fork_uri"].startswith(fork_result["forks_out_prefix"])
    assert len(fork_store.load_fork(result["child_fork_uri"])) > 0
    # one child fork object was written under the out prefix
    assert len(fork_store.list_forks(fork_result["forks_out_prefix"])) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/backfill_handlers/test_worker.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_handlers.worker'`.

- [ ] **Step 3: Implement `worker.py`**

Create `lambda/backfill/backfill_handlers/worker.py`:

```python
"""Handler: write one file-batch's virtual refs into a child fork on S3."""

import pickle
import uuid
from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from backfill_handlers import fork_store
from virtualizarr_processor.processor import Processor

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    processor = Processor()
    shared = pickle.loads(fork_store.load_fork(event["fork_in_uri"]))
    child = shared.fork()
    for file_key in event["file_keys"]:
        if not processor.process_backfill_file(file_key, child):
            logger.error("Failed to process file", extra={"file_key": file_key})
            raise RuntimeError(f"process_backfill_file failed for {file_key}")

    child_fork_uri = f"{event['forks_out_prefix']}{uuid.uuid4().hex}.pkl"
    fork_store.save_fork(child_fork_uri, pickle.dumps(child))
    logger.info("Wrote child fork", extra={"child_fork_uri": child_fork_uri})
    return {"child_fork_uri": child_fork_uri}
```

Note: `uuid.uuid4()` is fine for the test — the assertion checks the child count and the returned
uri's prefix, not a fixed name.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_worker.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill/backfill_handlers/worker.py tests/backfill_handlers/test_worker.py
git commit -m "feat: worker handler"
```

---

### Task 9: `reduce` handler

**Files:**
- Create: `lambda/backfill/backfill_handlers/reduce.py`
- Test: `tests/backfill_handlers/test_reduce.py`

- [ ] **Step 1: Write the failing test**

Create `tests/backfill_handlers/test_reduce.py`:

```python
from backfill_handlers import fork, init, reduce, worker


def test_reduce_commits_all_worker_forks(s3_bucket, tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    init.handler({}, None)
    fork_result = fork.handler(
        {
            "partition_id": "0",
            "manifest_uri": f"s3://{s3_bucket}/run/partitions/0.json",
            "run_prefix": f"s3://{s3_bucket}/run/",
        },
        None,
    )
    base = {
        "fork_in_uri": fork_result["fork_in_uri"],
        "forks_out_prefix": fork_result["forks_out_prefix"],
    }
    worker.handler({**base, "file_keys": ["0", "1", "2"]}, None)
    worker.handler({**base, "file_keys": ["3", "4", "5"]}, None)

    result = reduce.handler(
        {"partition_id": "0", "forks_out_prefix": fork_result["forks_out_prefix"]},
        None,
    )

    assert isinstance(result["tip"], str) and result["tip"]
    assert result["partition_id"] == "0"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/backfill_handlers/test_reduce.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_handlers.reduce'`.

- [ ] **Step 3: Implement `reduce.py`**

Create `lambda/backfill/backfill_handlers/reduce.py`:

```python
"""Handler: merge all child forks for a partition into one commit."""

from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from backfill_handlers import fork_store
from virtualizarr_processor import backfill
from virtualizarr_processor.processor import Processor

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    partition_id = event["partition_id"]
    processor = Processor()
    repo = processor.open_backfill_repo()

    child_uris = fork_store.list_forks(event["forks_out_prefix"])
    children = [fork_store.load_fork(uri) for uri in child_uris]
    tip = backfill.merge_and_commit(
        repo, children, message=f"Backfill partition {partition_id}"
    )

    logger.info("Committed partition", extra={"partition_id": partition_id, "tip": tip})
    return {"partition_id": partition_id, "tip": tip}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_reduce.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill/backfill_handlers/reduce.py tests/backfill_handlers/test_reduce.py
git commit -m "feat: reduce handler"
```

---

### Task 10: `promote` handler

**Files:**
- Create: `lambda/backfill/backfill_handlers/promote.py`
- Test: `tests/backfill_handlers/test_promote.py`

- [ ] **Step 1: Write the failing test**

Create `tests/backfill_handlers/test_promote.py`:

```python
from backfill_handlers import init, promote
from virtualizarr_processor.processor import Processor


def test_promote_moves_main_to_backfill_tip(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    init.handler({}, None)

    result = promote.handler({}, None)

    assert result["promoted"] is True
    repo = Processor().open_backfill_repo()
    assert repo.lookup_branch("main") == repo.lookup_branch("backfill")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/backfill_handlers/test_promote.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backfill_handlers.promote'`.

- [ ] **Step 3: Implement `promote.py`**

Create `lambda/backfill/backfill_handlers/promote.py`:

```python
"""Handler: fast-forward main to the backfill tip."""

from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor import backfill
from virtualizarr_processor.processor import Processor

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    processor = Processor()
    repo = processor.open_backfill_repo()
    backfill.promote(repo)
    logger.info("Promoted main to backfill tip")
    return {"promoted": True}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_promote.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lambda/backfill/backfill_handlers/promote.py tests/backfill_handlers/test_promote.py
git commit -m "feat: promote handler"
```

---

### Task 11: End-to-end handler chain test

The capstone: prove the whole handler layer works together over moto S3 + a local-FS repo.

**Files:**
- Test: `tests/backfill_handlers/test_end_to_end.py`

- [ ] **Step 1: Write the test**

Create `tests/backfill_handlers/test_end_to_end.py`:

```python
import boto3
import numpy as np
import zarr
from backfill_handlers import fork, init, inventory, partition, promote, reduce, worker
from virtualizarr_processor.processor import Processor


def test_full_backfill_chain(s3_bucket, tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("ICECHUNK_BUCKET", raising=False)
    monkeypatch.setenv("ICECHUNK_LOCAL_PATH", str(tmp_path / "repo"))
    run_prefix = f"s3://{s3_bucket}/run/"

    # Inventory: 6 synthetic keys "0".."5" (one per time slice).
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=s3_bucket, Key="inv.json", Body=b'["0", "1", "2", "3", "4", "5"]'
    )

    # partition -> init
    parts = partition.handler(
        {
            "inventory_uri": f"s3://{s3_bucket}/inv.json",
            "run_prefix": run_prefix,
            "partition_size": 3,
        },
        None,
    )["partitions"]
    init.handler({}, None)

    # serial over partitions: fork -> workers (one per file) -> reduce
    for part in parts:
        fork_result = fork.handler(
            {
                "partition_id": part["partition_id"],
                "manifest_uri": part["manifest_uri"],
                "run_prefix": run_prefix,
            },
            None,
        )
        for file_key in inventory.read_manifest(part["manifest_uri"]):
            worker.handler(
                {
                    "fork_in_uri": fork_result["fork_in_uri"],
                    "forks_out_prefix": fork_result["forks_out_prefix"],
                    "file_keys": [file_key],
                },
                None,
            )
        reduce.handler(
            {
                "partition_id": part["partition_id"],
                "forks_out_prefix": fork_result["forks_out_prefix"],
            },
            None,
        )

    # promote and verify all 6 slices on main
    promote.handler({}, None)
    repo = Processor().open_backfill_repo()
    arr = zarr.open_group(repo.readonly_session("main").store, mode="r")["foo"]
    assert (np.asarray(arr[:]) == np.arange(6)[:, None, None]).all()
```

- [ ] **Step 2: Run test to verify it passes**

Run: `uv run pytest tests/backfill_handlers/test_end_to_end.py -v`
Expected: PASS — all 6 slices correct on `main` after the full chain.

- [ ] **Step 3: Run the whole suite to confirm no regressions**

Run: `uv run pytest -q`
Expected: all tests pass (existing append + backfill A tests + all backfill handler tests).

- [ ] **Step 4: Commit**

```bash
git add tests/backfill_handlers/test_end_to_end.py
git commit -m "test: end-to-end backfill handler chain over moto S3 + local repo"
```

---

## Notes for the implementer

- The env-configurable `open_backfill_repo` (local-FS branch) and the full fork/merge chain were
  prototyped and verified against icechunk 1.1.14. The boto3/moto S3 glue is standard.
- Do NOT change the append path (`process_messages`, existing `initialize`) or the existing
  Processor append methods.
- Watch the isolated-mypy `warn_return_any` rule (see Critical Notes): returns sourced from
  `json.loads`, boto3 calls, or `session.commit()` and typed to a concrete type need
  `typing.cast`; returns typed to an icechunk type (`Repository`) do not.
- `moto>=5` uses `from moto import mock_aws` (already in the conftest). Create boto3 clients
  lazily inside functions (as the handlers do) so `mock_aws` is active first.
- This is sub-project B. Dockerfiles, CDK, and the Step Functions state machine are sub-project C.
```
