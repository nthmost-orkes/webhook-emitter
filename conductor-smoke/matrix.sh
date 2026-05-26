#!/usr/bin/env bash
# matrix.sh — drive smoke.py against conductor with each persistence backing.
#
# For a given backing, brings up the corresponding docker-compose stack from
# the conductor repo, waits for /health, fires the 6-verifier smoke from
# smoke.py at the exposed conductor port (8000), then tears the stack down.
#
# Requires:
#   - docker / docker compose
#   - webhook-emitter running locally (default http://localhost:8765)
#   - a conductor:server image built from the branch under test
#     (build with: `docker compose -f docker-compose-postgres.yaml build`
#      from the conductor repo, or pass --build to this script)
#
# Usage:
#   matrix.sh --backing postgres [--build] [--keep-up]
#   matrix.sh --backing all       # iterate every supported backing
#
# Supported backings: postgres, mysql, redis, cassandra
#   (sqlite isn't covered here — it's already proven via in-process tests)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONDUCTOR_REPO="${CONDUCTOR_REPO:-$HOME/projects/git/conductor-oss/conductor}"
EMITTER_URL="${EMITTER_URL:-http://localhost:8765}"
CONDUCTOR_PORT="${CONDUCTOR_PORT:-8000}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-300}"

declare -A COMPOSE_FOR=(
  [postgres]=docker-compose-postgres.yaml
  [mysql]=docker-compose-mysql.yaml
  [redis]=docker-compose.yaml
  [cassandra]=docker-compose-cassandra-es7.yaml
)

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") --backing {postgres|mysql|redis|cassandra|all} [--build] [--keep-up]

Options:
  --backing X    Which persistence backing to test. 'all' iterates supported set.
  --build        Force docker compose build before bring-up (slow; needed after code changes).
  --keep-up      Don't tear down after; useful for poking at the running stack.

Environment:
  CONDUCTOR_REPO   path to conductor checkout (default: \$HOME/projects/git/conductor-oss/conductor)
  EMITTER_URL      webhook-emitter URL (default: http://localhost:8765)
  CONDUCTOR_PORT   exposed conductor port from the compose file (default: 8000)
  HEALTH_TIMEOUT   seconds to wait for /health (default: 300)
EOF
  exit 2
}

BACKING=""
BUILD=0
KEEP_UP=0
while [ $# -gt 0 ]; do
  case "$1" in
    --backing) BACKING="${2:-}"; shift 2 ;;
    --build) BUILD=1; shift ;;
    --keep-up) KEEP_UP=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

[ -z "$BACKING" ] && usage

if ! curl -sf "$EMITTER_URL/healthz" >/dev/null 2>&1; then
  echo "ERROR: webhook-emitter not reachable at $EMITTER_URL" >&2
  echo "       start it with: cd $(dirname "$SCRIPT_DIR") && uvicorn webhook_emitter.main:app --port 8765" >&2
  exit 3
fi

wait_health() {
  local timeout="$1" elapsed=0
  echo "  waiting for http://localhost:$CONDUCTOR_PORT/health (timeout ${timeout}s)..."
  while [ $elapsed -lt "$timeout" ]; do
    if curl -sf "http://localhost:$CONDUCTOR_PORT/health" 2>/dev/null | grep -q '"healthy":true'; then
      echo "  healthy after ${elapsed}s"
      return 0
    fi
    sleep 5; elapsed=$((elapsed + 5))
  done
  echo "  TIMEOUT after ${timeout}s" >&2
  return 1
}

run_one() {
  local backing="$1" file="${COMPOSE_FOR[$1]:-}"
  if [ -z "$file" ]; then
    echo "unsupported backing: $backing (supported: ${!COMPOSE_FOR[*]})" >&2
    return 2
  fi
  local compose_path="$CONDUCTOR_REPO/docker/$file"
  if [ ! -f "$compose_path" ]; then
    echo "compose file missing: $compose_path" >&2
    return 2
  fi

  echo
  echo "================================================================"
  echo "  backing: $backing"
  echo "  compose: $compose_path"
  echo "================================================================"

  ( cd "$CONDUCTOR_REPO/docker" && docker compose -f "$file" down -v --remove-orphans >/dev/null 2>&1 || true )

  if [ "$BUILD" -eq 1 ]; then
    echo "  building conductor:server image..."
    ( cd "$CONDUCTOR_REPO/docker" && docker compose -f "$file" build 2>&1 | tail -5 )
  fi

  ( cd "$CONDUCTOR_REPO/docker" && docker compose -f "$file" up -d 2>&1 | tail -5 )

  local smoke_rc=0
  if wait_health "$HEALTH_TIMEOUT"; then
    echo
    echo "  running smoke.py against conductor on :$CONDUCTOR_PORT..."
    if python3 "$SCRIPT_DIR/smoke.py" \
        --conductor-url "http://localhost:$CONDUCTOR_PORT" \
        --emitter-url "$EMITTER_URL"; then
      smoke_rc=0
    else
      smoke_rc=$?
    fi
  else
    echo "  conductor never came up healthy; dumping last log lines:" >&2
    ( cd "$CONDUCTOR_REPO/docker" && docker compose -f "$file" logs --tail=200 conductor-server 2>&1 | tail -200 ) >&2
    smoke_rc=4
  fi

  if [ "$KEEP_UP" -eq 0 ]; then
    ( cd "$CONDUCTOR_REPO/docker" && docker compose -f "$file" down -v --remove-orphans 2>&1 | tail -3 )
  else
    echo "  --keep-up set: leaving stack running"
  fi

  return $smoke_rc
}

declare -a TARGETS
if [ "$BACKING" = "all" ]; then
  TARGETS=(postgres mysql redis cassandra)
else
  TARGETS=("$BACKING")
fi

PASS=()
FAIL=()
for t in "${TARGETS[@]}"; do
  if run_one "$t"; then
    PASS+=("$t")
  else
    FAIL+=("$t")
  fi
done

echo
echo "================================================================"
echo "  MATRIX RESULTS"
echo "================================================================"
echo "PASS (${#PASS[@]}):"
for s in ${PASS[@]+"${PASS[@]}"}; do echo "  ✓  $s"; done
echo "FAIL (${#FAIL[@]}):"
for s in ${FAIL[@]+"${FAIL[@]}"}; do echo "  ✗  $s"; done

[ ${#FAIL[@]} -eq 0 ] && exit 0 || exit 1
