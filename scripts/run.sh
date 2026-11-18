#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[ -x bin/streamforge ] || ./scripts/build.sh
exec ./bin/streamforge -data-dir "${STREAMFORGE_DATA:-data/streamforge}" -wire-addr "${STREAMFORGE_WIRE:-127.0.0.1:9092}" -http-addr "${STREAMFORGE_HTTP:-127.0.0.1:9093}"
