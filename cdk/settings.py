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
    ACCOUNT_ID: str
    ACCOUNT_REGION: str = "us-west-2"
    ICECHUNK_BUCKET_NAME: str = "icechunk-outuput"
    ICECHUNK_BUCKET: str | None = None
    ICECHUNK_PREFIX: str | None = None
    DATA_BUCKET_NAME: str | None = None
    PROJECT: str = "virtualizarr-data-pipelines"
    SNS_TOPIC: str | None = None
    MAX_CONCURRENCY: int = 50
    SQS_BATCH_SIZE: int = 10
    # SQS -> Lambda batching window (seconds). AWS requires this to be >= 1
    # when SQS_BATCH_SIZE > 10; with a window the poller waits up to this long
    # to fill a larger batch before invoking. Harmless for batch sizes <= 10.
    SQS_MAX_BATCHING_WINDOW: int = 5

    # process_messages Lambda tuning. Raise LAMBDA_TIMEOUT toward 900 (the
    # Lambda max) to fit larger SQS_BATCH_SIZE values.
    # If commits are run serially on main, bigger batches (fewer commits)
    # scale better than more concurrency.
    LAMBDA_TIMEOUT: int = 300
    LAMBDA_MEMORY: int = 2048
    # SQS visibility timeout. Must be >= LAMBDA_TIMEOUT so a record isn't
    # redelivered while its batch is still being processed.
    VISIBILITY_TIMEOUT: int = 360
    # How long (in days) SQS retains messages in the main queue and the DLQ
    # before they are dropped. AWS allows 1-14 days.
    SQS_RETENTION_PERIOD_DAYS: int = 14

    # Freguency in days to run garbage collection.
    GARBAGE_COLLECTION_FREQUENCY: int | None = None
    # Age (in hours) of snapshots to expire when running garbage collection.
    # Snapshots older than `now - GARBAGE_COLLECTION_EXPIRY_HOURS` are garbage
    # collected.
    GARBAGE_COLLECTION_EXPIRY_HOURS: int = 3

    VPC_ID: str | None = None
    # AWS Batch cluster reference to SSM parameter describing the AMI _or_ the AMI ID
    # If using SSM to resolve the AMI ID, prefix with `resolve:ssm`.
    # MCP_AMI_ID: str = "resolve:ssm:/mcp/amis/aml2023-ecs"
    AMI_ID: str = (
        "resolve:ssm:/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id"
    )

    # Cluster scaling max
    BATCH_MAX_VCPU: int = 10

    @model_validator(mode="after")
    def _check_visibility_timeout(self) -> "StackSettings":
        if self.VISIBILITY_TIMEOUT < self.LAMBDA_TIMEOUT:
            raise ValueError(
                f"VISIBILITY_TIMEOUT ({self.VISIBILITY_TIMEOUT}s) must be >= "
                f"LAMBDA_TIMEOUT ({self.LAMBDA_TIMEOUT}s)"
            )
        return self

    @model_validator(mode="after")
    def _check_retention_period(self) -> "StackSettings":
        if not 1 <= self.SQS_RETENTION_PERIOD_DAYS <= 14:
            raise ValueError(
                f"SQS_RETENTION_PERIOD_DAYS ({self.SQS_RETENTION_PERIOD_DAYS}) must be "
                f"between 1 and 14 days (AWS SQS limit)"
            )
        return self

    @model_validator(mode="after")
    def _check_batching_window(self) -> "StackSettings":
        if self.SQS_BATCH_SIZE > 10 and self.SQS_MAX_BATCHING_WINDOW < 1:
            raise ValueError(
                f"SQS_MAX_BATCHING_WINDOW ({self.SQS_MAX_BATCHING_WINDOW}s) must be "
                f">= 1 when SQS_BATCH_SIZE ({self.SQS_BATCH_SIZE}) > 10"
            )
        return self
