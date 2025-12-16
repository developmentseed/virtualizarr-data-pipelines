### virtualizarr-data-pipelines

```
uv sync --all-groups
source .venv/bin/activate
uv run nodeenv --node=22.1.0 -p
npm install -g aws-cdk@2.1032.0
uv run --env-file .env cdk synth

```
