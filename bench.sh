#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:?Usage: $0 <scenario-file>}"
[[ -f "$SCENARIO" ]] || { echo "scenario file not found: $SCENARIO" >&2; exit 1; }

REPO="$(cd "$(dirname "$0")" && pwd)"
set -a; source "$SCENARIO"; set +a

docker compose -f "$REPO/docker-compose.yaml" --env-file "$SCENARIO" up --build -d
echo "Stack up. Stop with: docker compose -f $REPO/docker-compose.yaml down"
