#!/usr/bin/env bash
# rollback-migration.sh — Mark a Prisma migration as rolled back in the shadow database.
#
# Usage (from repo root or worker/):
#   bash worker/scripts/rollback-migration.sh <migration_name>
#
# Example:
#   bash worker/scripts/rollback-migration.sh 20240601120000_add_user_preferences
#
# What it does:
#   Runs `prisma migrate resolve --rolled-back <name>` which marks the named
#   migration as "rolled back" in the _prisma_migrations table without touching
#   schema or data. Use this AFTER you have manually reverted the SQL changes.
#
# Pre-requisites:
#   1. Set DIRECT_DATABASE_URL to a direct (non-pooling) connection string.
#      On Hostinger this is the same as DATABASE_URL.
#   2. The migration name must match exactly the folder name under prisma/migrations/.
#
# Full rollback procedure:
#   Step 1 — Deploy the previous dist/ (re-run deploy with the old tar or git checkout).
#   Step 2 — Manually revert the SQL changes in the DB (connect via psql or Prisma Studio).
#   Step 3 — Run this script to mark the migration rolled back:
#             bash worker/scripts/rollback-migration.sh <name>
#   Step 4 — Verify: npx prisma migrate status
#
# See: https://www.prisma.io/docs/reference/api-reference/command-reference#migrate-resolve

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: bash $(basename "$0") <migration_name>" >&2
  echo "Example: bash $(basename "$0") 20240601120000_add_user_preferences" >&2
  exit 1
fi

MIGRATION_NAME="$1"

# Use DIRECT_DATABASE_URL when available (bypasses pgBouncer for Prisma migrations)
DB_URL="${DIRECT_DATABASE_URL:-${DATABASE_URL:-}}"
if [[ -z "$DB_URL" ]]; then
  echo "ERROR: Neither DIRECT_DATABASE_URL nor DATABASE_URL is set." >&2
  exit 1
fi

# Resolve script location so it works from any directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKER_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "▶ Rolling back migration: $MIGRATION_NAME"
echo "  DB: ${DB_URL%%@*}@***"

DATABASE_URL="$DB_URL" npx --prefix "$WORKER_DIR" prisma migrate resolve \
  --rolled-back "$MIGRATION_NAME" \
  --schema "$WORKER_DIR/prisma/schema.prisma"

echo "✅ Migration marked as rolled back: $MIGRATION_NAME"
echo "   Run 'npx prisma migrate status' to verify."
