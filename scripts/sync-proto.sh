#!/usr/bin/env bash
# Regenerates cairn/ (packages cairn.v1, cairn.worker.v1) from the Cairn proto
# contract: the same `protoc --python_out --pyi_out` recipe the workers used.
# Connect clients are not generated: a worker speaks NATS, not Connect, and
# the SDK does not depend on connectrpc.
#
#   scripts/sync-proto.sh                 # protoc from PATH
#   CORE=../cairn-core scripts/sync-proto.sh
#   IN_CONTAINER=1 scripts/sync-proto.sh  # protoc of ghcr.io/opencairn-org/cairn-build-base:1
set -euo pipefail
cd "$(dirname "$0")/.."
CORE=${CORE:-../cairn-core}
IMAGE=${IMAGE:-ghcr.io/opencairn-org/cairn-build-base:1}
test -d "$CORE/api/proto" || { echo "no proto contract at $CORE/api/proto (set CORE)"; exit 1; }

generate() {
  rm -rf cairn
  # shellcheck disable=SC2046  # one argument per .proto file
  protoc -I "$CORE/api/proto" --python_out=. --pyi_out=. $(find "$CORE/api/proto" -name '*.proto' | sort)
}

if [ "${IN_CONTAINER:-}" = "1" ]; then
  runner=$(command -v podman || command -v docker)
  "$runner" run --rm --security-opt label=disable --userns=keep-id \
    -v "$(cd "$CORE" && pwd)":/core -v "$PWD":/src -w /src -e HOME=/tmp "$IMAGE" \
    sh -c 'rm -rf cairn && protoc -I /core/api/proto --python_out=. --pyi_out=. $(find /core/api/proto -name "*.proto" | sort)'
else
  generate
fi
echo "generated $(find cairn -name '*_pb2.py' | wc -l) modules under cairn/"
