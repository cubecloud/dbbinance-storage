#!/bin/bash
# deploy.sh — full dbupdater deploy on the build host.
#
# Steps: git pull (ff-only) -> docker build -> stop+rm old container ->
# docker run new container (detached) -> verify status + tail recent logs.
#
# Run from the repo root on the build host (e.g. ~/dbbinance-storage).
#
# Requirements:
#   ~/.dbbinance_pat        — file with TOKEN=ghp_... (chmod 600)
#   *.env files in repo dir — BINANCEKEYS.env, BINANCE_KEY.env,
#                              PSGSQLKEYS.env, PSGSQL_KEY.env
#   sudo                    — for docker build / run / stop / rm
#
# Tail logs after deploy:
#   sudo docker logs dbupdater -f

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

PAT_FILE="${HOME}/.dbbinance_pat"
ENV_FILES=(BINANCEKEYS.env BINANCE_KEY.env PSGSQLKEYS.env PSGSQL_KEY.env)
CONTAINER=dbupdater
IMAGE=dbbinance-image:latest

log() { printf '\n==> %s\n' "$*"; }
die() { printf 'Error: %s\n' "$*" >&2; exit 1; }

# --- 1. Preflight: PAT + env files ---
[[ -f "$PAT_FILE" ]] || die "$PAT_FILE missing. Create: echo TOKEN=ghp_xxx > $PAT_FILE && chmod 600 $PAT_FILE"
set -a
# shellcheck source=/dev/null
source "$PAT_FILE"
set +a
[[ -n "${TOKEN:-}" ]] || die "TOKEN not set after sourcing $PAT_FILE"

for f in "${ENV_FILES[@]}"; do
    [[ -f "$REPO_DIR/$f" ]] || die "missing $REPO_DIR/$f"
done

# --- 2. Update repo ---
log "git pull (ff-only)"
git pull --ff-only
log "HEAD: $(git log --oneline -1)"

# --- 3. Build image (--no-cache, clones HEAD main from GitHub) ---
log "docker build (this will take a few minutes)"
./docker_build.sh

# --- 4. Stop + remove existing container (idempotent) ---
log "stop + rm existing container (if any)"
if sudo docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    sudo docker stop "$CONTAINER" >/dev/null
    sudo docker rm "$CONTAINER" >/dev/null
    echo "    removed previous $CONTAINER"
else
    echo "    no existing $CONTAINER"
fi

# --- 5. Start new container (detached) ---
log "docker run (detached)"
./docker_run.sh

# --- 6. Verify ---
sleep 5
log "container status"
sudo docker ps --filter "name=${CONTAINER}" \
    --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.CreatedAt}}'

log "last log lines"
sudo docker logs "$CONTAINER" --tail 30 2>&1 || true

cat <<EOF

Deploy complete.
Follow logs:   sudo docker logs $CONTAINER -f
Inspect DB:    ssh fridai-jarvis 'sudo -n docker exec dbupdater python /tmp/ohlcv_garbage_scan.py'
EOF
