#!/bin/bash
set -e  # Exit on error

echo "📦 Syncing uv dependencies..."
uv sync --all-groups

echo "📦 Setting up Node.js environment..."
uv run nodeenv --node=22.1.0 -p

echo "📦 Installing AWS CDK..."
uv run npm install -g aws-cdk@2.103.0

echo "✅ Setup complete!"
