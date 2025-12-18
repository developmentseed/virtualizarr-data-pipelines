from typing import Any

from aws_cdk import (
    CustomResource,
    Duration,
    Stack,
    Tags,
)
from aws_cdk import (
    aws_ec2 as ec2,
)
from aws_cdk import (
    aws_ecr_assets as ecr_assets,
)
from aws_cdk import (
    aws_events as events,
)
from aws_cdk import (
    aws_events_targets as targets,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_lambda as _lambda,
)
from aws_cdk import (
    aws_lambda_event_sources as lambda_event_sources,
)
from aws_cdk import (
    aws_s3 as s3,
)
from aws_cdk import (
    aws_sns as sns,
)
from aws_cdk import (
    aws_sns_subscriptions as subscriptions,
)
from aws_cdk import (
    aws_sqs as sqs,
)
from aws_cdk import custom_resources as cr
from constructs import Construct
from settings import StackSettings  # type: ignore[import-not-found]
from stack_constructs import BatchInfra, BatchJob


class VirtualizarrSqsStack(Stack):
    def __init__(
        self: Any,
        scope: Construct,
        construct_id: str,
        settings: StackSettings,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        Tags.of(self).add("Project", settings.PROJECT)

        self.dlq = sqs.Queue(
            self,
            f"{settings.STACK_NAME}-Dlq",
            queue_name=f"{settings.STACK_NAME}-Dlq",
            retention_period=Duration.days(14),
        )

        self.queue = sqs.Queue(
            self,
            f"{settings.STACK_NAME}-queue",
            queue_name=f"{settings.STACK_NAME}-queue",
            visibility_timeout=Duration.seconds(1800),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=20,
                queue=self.dlq,
            ),
        )
        if settings.ICECHUNK_BUCKET:
            self.icechunk_bucket = s3.Bucket.from_bucket_name(
                self,
                f"{settings.STACK_NAME}-bucket",
                bucket_name=settings.ICECHUNK_BUCKET,
            )
        else:
            self.icechunk_bucket = s3.Bucket(
                self,
                f"{settings.STACK_NAME}-bucket",
                bucket_name=settings.ICECHUNK_BUCKET_NAME,
            )

        if settings.SNS_TOPIC:
            self.sns_topic = sns.Topic.from_topic_arn(
                self,
                f"{settings.STACK_NAME}-sns-topic",
                topic_arn=settings.SNS_TOPIC,
            )

            self.sns_topic.add_subscription(
                subscriptions.SqsSubscription(
                    self.queue,
                    raw_message_delivery=True,
                )
            )

        self.process_file_lambda = _lambda.DockerImageFunction(
            self,
            f"{settings.STACK_NAME}-process_file_lambda",
            code=_lambda.DockerImageCode.from_image_asset(
                directory="lambda",
                file="process_file/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,  # or LINUX_AMD64
            ),
            architecture=_lambda.Architecture.X86_64,
            timeout=Duration.minutes(5),
            memory_size=2048,
        )

        self.queue.grant_consume_messages(self.process_file_lambda)

        # Grant Lambda permissions to read from S3 (for processing HRRR files)
        self.process_file_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:ListBucket",
                ],
                resources=[
                    f"arn:aws:s3:::{settings.DATA_BUCKET_NAME}/*",
                    f"arn:aws:s3:::{settings.DATA_BUCKET_NAME}",
                ],
            )
        )

        # Grant Lambda permissions to write to the icechunk S3 bucket
        self.icechunk_bucket.grant_read_write(self.process_file_lambda)

        self.process_file_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(
                self.queue,
                batch_size=10,
                report_batch_item_failures=True,
                max_concurrency=2,
            )
        )

        self.initialize_icechunk_lambda = _lambda.DockerImageFunction(
            self,
            f"{settings.STACK_NAME}-initialize-icechunk-lambda",
            code=_lambda.DockerImageCode.from_image_asset(
                directory="lambda",
                file="initialize/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,  # or LINUX_AMD64
            ),
            architecture=_lambda.Architecture.X86_64,
            timeout=Duration.minutes(5),
            memory_size=2048,
        )

        self.icechunk_bucket.grant_read_write(self.initialize_icechunk_lambda)

        if settings.ICECHUNK_BUCKET:
            # Trigger it once on first deploy
            self.trigger = cr.AwsCustomResource(
                self,
                "TriggerOnce",
                on_create=cr.AwsSdkCall(
                    service="Lambda",
                    action="invoke",
                    parameters={
                        "FunctionName": self.initialize_icechunk_lambda.function_name,
                        "InvocationType": "Event",
                    },
                    physical_resource_id=cr.PhysicalResourceId.of("trigger-once-id"),
                ),
                policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                    resources=[self.initialize_icechunk_lambda.function_arn]
                ),
            )

            self.trigger.node.add_dependency(self.initialize_icechunk_lambda)
        else:
            self.custom_resource_provider = cr.Provider(
                self,
                "S3BucketCustomResourceProvider",
                on_event_handler=self.initialize_icechunk_lambda,
            )

            self.bucket_custom_resource = CustomResource(
                self,
                "S3BucketCustomResource",
                service_token=self.custom_resource_provider.service_token,
                properties={
                    "BucketName": self.icechunk_bucket.bucket_name,
                },
            )

            self.bucket_custom_resource.node.add_dependency(self.icechunk_bucket)

        if settings.GARBAGE_COLLECTION_FREQUENCY:
            self.vpc = ec2.Vpc.from_lookup(self, "VPC", vpc_id=settings.VPC_ID)

            self.gc_image_asset = ecr_assets.DockerImageAsset(
                self,
                "GCImage",
                directory="lambda",
                file="garbage_collect/Dockerfile",
                platform=ecr_assets.Platform.LINUX_AMD64,
            )

            self.batch_infra = BatchInfra(
                self,
                "Batch-Infra",
                max_vcpu=settings.BATCH_MAX_VCPU,
                ami_id=settings.AMI_ID,
                vpc=self.vpc,
                stage=settings.STAGE,
                stack_name=settings.STACK_NAME,
            )

            self.gc_job = BatchJob(
                self,
                "GC-Job",
                vcpu=2,
                image_asset=self.gc_image_asset,
                memory_mb=2000,
                retry_attempts=1,
            )
            self.icechunk_bucket.grant_read_write(self.gc_job.role)

            self.cron_rule = events.Rule(
                self,
                "GarbageCollectionSchedule",
                schedule=events.Schedule.rate(
                    Duration.days(settings.GARBAGE_COLLECTION_FREQUENCY)
                ),
            )

            self.cron_rule.add_target(
                targets.BatchJob(
                    job_queue_arn=self.batch_infra.queue.job_queue_arn,
                    job_definition_arn=self.gc_job.job_def.job_definition_arn,
                    job_name="garbage-collection",
                )
            )
