import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
from aws_cdk.assertions import Match, Template
from stack_constructs.backfill_pipeline import BackfillPipeline


def _template() -> Template:
    app = cdk.App()
    stack = cdk.Stack(
        app,
        "TestStack",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )
    bucket = s3.Bucket(stack, "IceBucket")
    BackfillPipeline(
        stack,
        "Backfill",
        icechunk_bucket=bucket,
        data_bucket_name="my-data-bucket",
        partition_size=500,
        max_items_per_batch=10,
        max_concurrency=50,
    )
    return Template.from_stack(stack)


def test_six_functions_with_cmd_overrides() -> None:
    template = _template()
    template.resource_count_is("AWS::Lambda::Function", 6)
    for action in ["partition", "init", "fork", "worker", "reduce", "promote"]:
        template.has_resource_properties(
            "AWS::Lambda::Function",
            Match.object_like(
                {"ImageConfig": {"Command": [f"backfill_handlers.{action}.handler"]}}
            ),
        )


def test_worker_has_data_bucket_read_and_list() -> None:
    template = _template()
    template.has_resource_properties(
        "AWS::IAM::Policy",
        Match.object_like(
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Action": ["s3:GetObject", "s3:ListBucket"],
                                    "Resource": [
                                        "arn:aws:s3:::my-data-bucket/*",
                                        "arn:aws:s3:::my-data-bucket",
                                    ],
                                }
                            )
                        ]
                    )
                }
            }
        ),
    )


def _state_machine_asl() -> str:
    app = cdk.App()
    stack = cdk.Stack(
        app,
        "TestStack",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )
    bucket = s3.Bucket(stack, "IceBucket")
    BackfillPipeline(
        stack,
        "Backfill",
        icechunk_bucket=bucket,
        data_bucket_name="my-data-bucket",
        partition_size=500,
        max_items_per_batch=10,
        max_concurrency=50,
    )
    tmpl = app.synth().get_stack_by_name("TestStack").template
    for res in tmpl["Resources"].values():
        if res["Type"] == "AWS::StepFunctions::StateMachine":
            parts = res["Properties"]["DefinitionString"]["Fn::Join"][1]
            return "".join(p if isinstance(p, str) else "<REF>" for p in parts)
    raise AssertionError("no state machine synthesized")


def test_state_machine_shape() -> None:
    template = _template()
    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)

    asl = _state_machine_asl()
    # inner Distributed Map with a dynamic per-partition ItemReader key
    # (manifest_key comes from the partition item, not the fork result)
    assert '"Mode":"DISTRIBUTED"' in asl
    assert '"Key.$":"$.manifest_key"' in asl
    assert '"MaxItemsPerBatch":10' in asl
    # outer Map is serial
    assert '"MaxConcurrency":1' in asl
    # worker event reshape (Items -> file_keys, BatchInput carries the constants)
    assert '"file_keys.$":"$.Items"' in asl
    assert "$.BatchInput.fork_in_uri" in asl
    # reduce is reshaped to the flat event its handler expects
    assert '"forks_out_prefix.$":"$.forkResult.forks_out_prefix"' in asl
    # run_prefix derives from the execution name
    assert "Execution.Name" in asl
