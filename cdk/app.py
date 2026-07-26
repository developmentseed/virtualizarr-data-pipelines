import os

from aws_cdk import App, Tags
from settings import StackSettings
from stack import VirtualizarrSqsStack

settings = StackSettings()

# Resolve the target environment. Prefer explicit settings; fall back to the CDK
# CLI's ambient account/region (from the active AWS credentials) so a blank
# ACCOUNT_ID does not synth an invalid "aws:///<region>" environment.
account = settings.ACCOUNT_ID or os.environ.get("CDK_DEFAULT_ACCOUNT")
region = settings.ACCOUNT_REGION or os.environ.get("CDK_DEFAULT_REGION")
if not account:
    raise SystemExit(
        "No AWS account resolved. Set ACCOUNT_ID in your environment/.env "
        "(e.g. `uv run --env-file .env cdk synth`), or invoke through the CDK CLI "
        "so CDK_DEFAULT_ACCOUNT is populated from your active AWS credentials."
    )

app = App()
stack = VirtualizarrSqsStack(
    app,
    settings.STACK_NAME,
    settings=settings,
    env={"account": account, "region": region},
)

for k, v in dict(
    Project=settings.PROJECT_NAME,
    Stack=settings.STACK_NAME,
).items():
    Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
