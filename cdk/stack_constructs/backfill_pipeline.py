from typing import Any

from aws_cdk import Aws, Duration
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lmb
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct

_ACTIONS = ["partition", "init", "fork", "worker", "reduce", "promote"]


class BackfillPipeline(Construct):
    """Backfill Step Functions pipeline: six Lambda handlers built from one image,
    wired into an outer serial Map over partitions with an inner Distributed Map of
    workers."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        icechunk_bucket: s3.IBucket,
        data_bucket_name: str,
        partition_size: int,
        max_items_per_batch: int,
        max_concurrency: int,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.functions: dict[str, lmb.DockerImageFunction] = {}
        for action in _ACTIONS:
            fn = lmb.DockerImageFunction(
                self,
                f"{action}-fn",
                code=lmb.DockerImageCode.from_image_asset(
                    "lambda",
                    file="backfill/Dockerfile",
                    platform=ecr_assets.Platform.LINUX_AMD64,
                    cmd=[f"backfill_handlers.{action}.handler"],
                ),
                architecture=lmb.Architecture.X86_64,
                timeout=Duration.minutes(15),
                memory_size=2048,
                environment={
                    "ICECHUNK_BUCKET": icechunk_bucket.bucket_name,
                    "ICECHUNK_REGION": Aws.REGION,
                },
            )
            icechunk_bucket.grant_read_write(fn)
            self.functions[action] = fn

        # worker parses source files; partition reads the inventory object.
        data_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:GetObject", "s3:ListBucket"],
            resources=[
                f"arn:aws:s3:::{data_bucket_name}/*",
                f"arn:aws:s3:::{data_bucket_name}",
            ],
        )
        self.functions["worker"].add_to_role_policy(data_policy)
        self.functions["partition"].add_to_role_policy(data_policy)

        self.state_machine = self._build_state_machine(
            icechunk_bucket, partition_size, max_items_per_batch, max_concurrency
        )

    def _build_state_machine(
        self,
        icechunk_bucket: s3.IBucket,
        partition_size: int,
        max_items_per_batch: int,
        max_concurrency: int,
    ) -> sfn.StateMachine:
        partition = tasks.LambdaInvoke(
            self,
            "PartitionTask",
            lambda_function=self.functions["partition"],
            payload=sfn.TaskInput.from_object(
                {
                    "inventory_uri": sfn.JsonPath.string_at("$.inventory_uri"),
                    "run_prefix": sfn.JsonPath.format(
                        "s3://{}/backfill/{}/",
                        icechunk_bucket.bucket_name,
                        sfn.JsonPath.string_at("$$.Execution.Name"),
                    ),
                    "partition_size": partition_size,
                }
            ),
            payload_response_only=True,
            result_path="$.partitionResult",
        )

        init = tasks.LambdaInvoke(
            self,
            "InitTask",
            lambda_function=self.functions["init"],
            payload=sfn.TaskInput.from_object({}),
            payload_response_only=True,
            result_path="$.initResult",
        )

        fork = tasks.LambdaInvoke(
            self,
            "ForkTask",
            lambda_function=self.functions["fork"],
            payload_response_only=True,
            result_path="$.forkResult",
        )

        worker = tasks.LambdaInvoke(
            self,
            "WorkerTask",
            lambda_function=self.functions["worker"],
            payload=sfn.TaskInput.from_object(
                {
                    "file_keys": sfn.JsonPath.string_at("$.Items"),
                    "fork_in_uri": sfn.JsonPath.string_at("$.BatchInput.fork_in_uri"),
                    "forks_out_prefix": sfn.JsonPath.string_at(
                        "$.BatchInput.forks_out_prefix"
                    ),
                }
            ),
            payload_response_only=True,
        )

        inner_map = sfn.DistributedMap(
            self,
            "InnerMap",
            item_reader=sfn.S3JsonItemReader(
                bucket=icechunk_bucket,
                # manifest_key comes from the partition item ($ here is the outer
                # Map iteration state); the fork result does not carry it.
                key=sfn.JsonPath.string_at("$.manifest_key"),
            ),
            item_batcher=sfn.ItemBatcher(
                max_items_per_batch=max_items_per_batch,
                # ItemBatcher.BatchInput does NOT convert JsonPath values into
                # ".$"-suffixed keys (unlike TaskInput.from_object) — a JsonPath
                # value here renders as a literal constant, so the worker would
                # receive the string "$.forkResult.fork_in_uri". Write the ".$"
                # path keys explicitly so Step Functions resolves them.
                batch_input={
                    "fork_in_uri.$": "$.forkResult.fork_in_uri",
                    "forks_out_prefix.$": "$.forkResult.forks_out_prefix",
                },
            ),
            max_concurrency=max_concurrency,
            # No tolerated_failure_*: the Distributed Map default fails on any worker
            # failure, so a partition never reduces on an incomplete fork set.
            result_path=sfn.JsonPath.DISCARD,
        )
        inner_map.item_processor(worker)

        reduce = tasks.LambdaInvoke(
            self,
            "ReduceTask",
            lambda_function=self.functions["reduce"],
            # forks_out_prefix lives under the fork result; reshape to the flat
            # event the reduce handler expects.
            payload=sfn.TaskInput.from_object(
                {
                    "partition_id": sfn.JsonPath.string_at("$.partition_id"),
                    "forks_out_prefix": sfn.JsonPath.string_at(
                        "$.forkResult.forks_out_prefix"
                    ),
                }
            ),
            payload_response_only=True,
            result_path="$.reduceResult",
        )

        outer_map = sfn.Map(
            self,
            "OuterMap",
            items_path="$.partitionResult.partitions",
            max_concurrency=1,
            result_path=sfn.JsonPath.DISCARD,
        )
        outer_map.item_processor(fork.next(inner_map).next(reduce))

        promote = tasks.LambdaInvoke(
            self,
            "PromoteTask",
            lambda_function=self.functions["promote"],
            payload=sfn.TaskInput.from_object({}),
            payload_response_only=True,
        )

        definition = partition.next(init).next(outer_map).next(promote)
        return sfn.StateMachine(
            self,
            "StateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
        )
