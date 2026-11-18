#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bin
go build -trimpath -o bin/streamforge ./cmd/streamforge
go build -trimpath -o bin/streamforge-cli ./cmd/streamforge-cli
echo "Built StreamForge binaries in ./bin/"
