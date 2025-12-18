from aws_cdk import App, Tags
from settings import StackSettings
from stack import VirtualizarrSqsStack

settings = StackSettings()

app = App()
stack = VirtualizarrSqsStack(
    app,
    settings.STACK_NAME,
    settings=settings,
    env={"account": settings.ACCOUNT_ID, "region": settings.ACCOUNT_REGION},
)

for k, v in dict(
    Project=settings.PROJECT_NAME,
    Stack=settings.STACK_NAME,
).items():
    Tags.of(app).add(k, v, apply_to_launched_instances=True)

app.synth()
