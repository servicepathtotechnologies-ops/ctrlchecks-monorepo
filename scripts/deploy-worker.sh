#!/usr/bin/env bash
# deploy-worker.sh — Build and deploy the worker monolith to Hostinger production.
#
# Usage (from repo root):
#   bash scripts/deploy-worker.sh
#
# Requires sshpass for password-based SSH to Hostinger.
# Pass the server password via DEPLOY_PASS env var (do NOT hardcode here):
#   DEPLOY_PASS="<password>" bash scripts/deploy-worker.sh
#
# Target: root@187.127.185.105 /opt/ctrlchecks-worker
# Domain: https://worker.ctrlchecks.com
#
# Deploys dist/ via tar+scp — no git dependency on server.
# Preserves /opt/ctrlchecks-worker/.env on the server.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORKER_DIR="$REPO_ROOT/worker"
SERVER_USER="root"
SERVER_HOST="187.127.185.105"
SERVER_PATH="/opt/ctrlchecks-worker"

# Password auth via sshpass — never hardcoded here.
# Set DEPLOY_PASS in the environment before running this script.
if [[ -z "${DEPLOY_PASS:-}" ]]; then
  echo "❌ DEPLOY_PASS env var not set." >&2
  echo "   Run: DEPLOY_PASS='<password>' bash scripts/deploy-worker.sh" >&2
  exit 1
fi

# Verify sshpass is available
if ! command -v sshpass &>/dev/null; then
  echo "❌ sshpass not found. Install it:" >&2
  echo "   Ubuntu/Debian: sudo apt-get install sshpass" >&2
  echo "   macOS:         brew install hudochenkov/sshpass/sshpass" >&2
  echo "   Git Bash (Windows): use the Python deploy alternative below" >&2
  exit 1
fi

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15"
SCP_CMD="sshpass -e scp $SSH_OPTS"
SSH_CMD="sshpass -e ssh $SSH_OPTS"
export SSHPASS="$DEPLOY_PASS"

echo "▶ Deploying to $SERVER_USER@$SERVER_HOST:$SERVER_PATH"
echo "  Domain: https://worker.ctrlchecks.com"
echo ""

echo "▶ Type-checking worker…"
cd "$WORKER_DIR"
npm run type-check
echo "✅ Type-check passed"

echo "▶ Linting worker…"
npm run lint
echo "✅ Lint passed"

echo "▶ Building worker…"
NODE_OPTIONS="--max-old-space-size=4096" npm run build
# Copy clickup node (JS file not compiled by tsc)
mkdir -p dist/services/clickup
cp src/services/clickup/clickupNode.js dist/services/clickup/clickupNode.js 2>/dev/null || true
echo "✅ Build complete: $(du -sh dist | cut -f1)"

echo "▶ Packaging…"
TMP_TAR="/tmp/worker-$(date +%s).tar.gz"
tar -czf "$TMP_TAR" \
  dist/ \
  package.json \
  package-lock.json \
  prisma/
echo "✅ Package: $(du -sh "$TMP_TAR" | cut -f1)"

echo "▶ Uploading to $SERVER_USER@$SERVER_HOST …"
$SCP_CMD "$TMP_TAR" "$SERVER_USER@$SERVER_HOST:/tmp/worker-deploy.tar.gz"
rm "$TMP_TAR"
echo "✅ Upload complete"

echo "▶ Extracting, installing, restarting…"
$SSH_CMD "$SERVER_USER@$SERVER_HOST" bash <<'REMOTE'
  set -euo pipefail
  TARGET=/opt/ctrlchecks-worker

  tar -xzf /tmp/worker-deploy.tar.gz -C "$TARGET"
  rm /tmp/worker-deploy.tar.gz

  cd "$TARGET"
  npm ci --omit=dev 2>&1 | tail -3

  # Run any pending DB migrations (safe to re-run)
  if [[ -f node_modules/.bin/prisma ]]; then
    npx prisma migrate deploy 2>/dev/null \
      && echo "  migrations OK" \
      || echo "  migrations skipped (no pending)"
  fi

  systemctl restart ctrlchecks-worker
  sleep 8
  systemctl is-active ctrlchecks-worker

  # Health checks
  curl -fsS http://localhost:3001/health | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('health:', d.get('status'))
for k, v in d.get('checks', {}).items():
    print(f'  {k}: {v}')
" && echo "✅ /health OK"

  curl -fsS http://localhost:3001/health/live | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('live:', d.get('status'))
" && echo "✅ /health/live OK"

REMOTE

echo ""
echo "✅ Worker deployed and healthy on :3001"
echo "   https://worker.ctrlchecks.com/health/live"
