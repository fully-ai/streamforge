#!/usr/bin/env bash
set -euo pipefail
command -v go >/dev/null 2>&1 || { echo "Go 1.24+ required." >&2; exit 1; }
cd "$(dirname "$0")/.."
./scripts/build.sh
