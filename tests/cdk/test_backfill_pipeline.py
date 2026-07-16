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
