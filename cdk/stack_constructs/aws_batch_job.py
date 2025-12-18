from typing import Any

from aws_cdk import (
    Aws,
    Duration,
    Size,
)
from aws_cdk import (
    aws_batch as batch,
)
from aws_cdk import (
    aws_ecr_assets as ecr_assets,
)
from aws_cdk import (
    aws_ecs as ecs,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_logs as logs,
)
from constructs import Construct


class BatchJob(Construct):
    """An AWS Batch job running a Docker container"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vcpu: int,
        image_asset: ecr_assets.DockerImageAsset,
        memory_mb: int,
        retry_attempts: int,
        environment: None | dict[str, str] = None,
        secrets: None | dict[str, batch.Secret] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.log_group = logs.LogGroup(
            self,
            "JobLogGroup",
        )

        # Execution role needs ECR permissions to pull from private repo
        # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html#ecr-required-iam-permissions
        execution_role = iam.Role(
            self,
            "ExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
        )
        execution_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=["*"],
                actions=[
                    "ecr:GetAuthorizationToken",
                ],
            )
        )

        self.role = iam.Role(
            self,
            "TaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        self.job_def = batch.EcsJobDefinition(
            self,
            "JobDef",
            container=batch.EcsEc2ContainerDefinition(
                self,
                "BatchContainerDef",
                image=ecs.ContainerImage.from_docker_image_asset(image_asset),
                execution_role=execution_role,
                job_role=self.role,
                cpu=vcpu,
                memory=Size.mebibytes(memory_mb),
                logging=ecs.LogDriver.aws_logs(
                    stream_prefix="job",
                    log_group=self.log_group,
                ),
                secrets=secrets,
                environment=environment or {},
            ),
            timeout=Duration.hours(1),
            retry_attempts=retry_attempts,
            retry_strategies=[
                batch.RetryStrategy.of(
                    batch.Action.RETRY, batch.Reason.CANNOT_PULL_CONTAINER
                ),
                batch.RetryStrategy.of(
                    batch.Action.RETRY, batch.Reason.SPOT_INSTANCE_RECLAIMED
                ),
                batch.RetryStrategy.of(
                    batch.Action.EXIT,
                    batch.Reason.custom(on_reason="*"),
                ),
            ],
            propagate_tags=True,
        )

        # It's useful to have the ARN of the job definition _without_ the revision
        # so submitted jobs use the "latest" active job
        self.job_def_arn_without_revision = ":".join(
            [
                "arn",
                "aws",
                "batch",
                Aws.REGION,
                Aws.ACCOUNT_ID,
                f"job-definition/{self.job_def.job_definition_name}",
            ]
        )
