#!/usr/bin/env bash
#
# Start a backfill Step Functions execution.
#
# Resolves the state machine ARN from the stack's `BackfillStateMachineArn`
# CloudFormation output, then starts an execution with the given name and
# inventory URI.
#
# Usage:
#   scripts/start_backfill.sh <execution-name> <inventory-uri>
#
# Example:
#   scripts/start_backfill.sh gpm-backfill-test-2yr \
#     s3://gpm-imerghh-partitioned/inventory/gpm_2yr.json
#
# Overrides (env vars):
#   STACK   CloudFormation stack name   (default: STACK_NAME from .env, else
#           "gpm-imerghh-partitioned")
#   REGION  AWS region                  (default: ACCOUNT_REGION from .env, else
#           the AWS CLI default)

set -euo pipefail

usage() {
  echo "Usage: $0 <execution-name> <inventory-uri>" >&2
  echo "  e.g. $0 gpm-backfill-test-2yr s3://bucket/inventory/gpm_2yr.json" >&2
  exit 2
}

[ $# -eq 2 ] || usage
EXECUTION_NAME="$1"
INVENTORY_URI="$2"

# Load STACK_NAME / ACCOUNT_REGION defaults from .env if present (without
# clobbering values already exported in the environment).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"
env_get() {
  # Read KEY from .env, stripping quotes; empty string if absent.
  [ -f "$ENV_FILE" ] || return 0
  grep -E "^$1=" "$ENV_FILE" | tail -n1 | cut -d= -f2- | sed 's/^["'\'']//;s/["'\'']$//'
}

STACK="${STACK:-$(env_get STACK_NAME)}"
STACK="${STACK:-gpm-imerghh-partitioned}"
REGION="${REGION:-$(env_get ACCOUNT_REGION)}"

# Region flag is only added when we actually have one; otherwise defer to the
# AWS CLI's configured default.
REGION_ARGS=()
[ -n "$REGION" ] && REGION_ARGS=(--region "$REGION")

echo "Resolving BackfillStateMachineArn from stack '$STACK'..." >&2
STATE_MACHINE_ARN="$(aws cloudformation describe-stacks \
  "${REGION_ARGS[@]}" \
  --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='BackfillStateMachineArn'].OutputValue" \
  --output text)"

if [ -z "$STATE_MACHINE_ARN" ] || [ "$STATE_MACHINE_ARN" = "None" ]; then
  echo "Error: BackfillStateMachineArn output not found on stack '$STACK'." >&2
  echo "Is BACKFILL_ENABLED=true and the stack deployed in region '${REGION:-<default>}'?" >&2
  exit 1
fi

echo "State machine: $STATE_MACHINE_ARN" >&2
echo "Starting execution '$EXECUTION_NAME'..." >&2

aws stepfunctions start-execution \
  "${REGION_ARGS[@]}" \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "$EXECUTION_NAME" \
  --input "{\"inventory_uri\": \"$INVENTORY_URI\"}"
