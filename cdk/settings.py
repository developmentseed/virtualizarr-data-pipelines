import os
from typing import Any, Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings

print("STAGE from env:", os.getenv("STAGE"))


def include_trailing_slash(value: Any) -> Any:
    """Make sure the value includes a trailing slash if str"""
    if isinstance(value, str):
        return value.rstrip("/") + "/"
    return value


class StackSettings(BaseSettings):
    PROJECT_NAME: str = "virtualizarr-data-pipelines"
    STACK_NAME: str = "virtualizarr-data-pipelines"
    STAGE: Literal["dev", "prod"]
    # Optional: when blank, app.py falls back to CDK_DEFAULT_ACCOUNT (the account
    # of the active AWS credentials) so synth/deploy still resolve an environment.
    ACCOUNT_ID: str | None = None
    ACCOUNT_REGION: str = "us-east-1"
    ICECHUNK_BUCKET_NAME: str = "icechunk-outuput"
    ICECHUNK_BUCKET: str | None = None
    # Key prefix for this dataset's repo. Icechunk >=2.1.0 refuses to CREATE a
    # repo at an empty prefix (bucket root), so this must be non-empty to bootstrap
    # a new store. Passed into the Lambda env as ICECHUNK_PREFIX.
    ICECHUNK_PREFIX: str | None = None
    DATA_BUCKET_NAME: str | None = None
    PROJECT: str = "virtualizarr-data-pipelines"
    SNS_TOPIC: str | None = None
    MAX_CONCURRENCY: int = 50
    SQS_BATCH_SIZE: int = 10

    # ARN of the Secrets Manager secret holding Earthdata {username, password}.
    # Optional: required only for reading protected GES DISC granules. When unset,
    # no secret resource or IAM grant is created.
    EARTHDATA_SECRET_ARN: str | None = None

    # Freguency in days to run garbage collection.
    GARBAGE_COLLECTION_FREQUENCY: int | None = None

    VPC_ID: str | None = None
    # AWS Batch cluster reference to SSM parameter describing the AMI _or_ the AMI ID
    # If using SSM to resolve the AMI ID, prefix with `resolve:ssm`.
    # MCP_AMI_ID: str = "resolve:ssm:/mcp/amis/aml2023-ecs"
    AMI_ID: str = (
        "resolve:ssm:/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id"
    )

    # Cluster scaling max
    BATCH_MAX_VCPU: int = 10

    # Backfill (partitioned fork/merge) pipeline
    BACKFILL_ENABLED: bool = False
    BACKFILL_PARTITION_SIZE: int = 500
    BACKFILL_MAX_ITEMS_PER_BATCH: int = 10
    BACKFILL_MAX_CONCURRENCY: int = 50

    # Forward SQS consumer. `None` resolves in the validator below:
    #   backfill enabled  -> default disabled (bootstrap via backfill, enable later)
    #   backfill disabled -> default enabled  (normal forward-only deployment)
    FORWARD_QUEUE_ENABLED: bool | None = None

    @model_validator(mode="after")
    def _resolve_forward_queue_enabled(self) -> "StackSettings":
        if self.FORWARD_QUEUE_ENABLED is None:
            self.FORWARD_QUEUE_ENABLED = not self.BACKFILL_ENABLED
        return self
