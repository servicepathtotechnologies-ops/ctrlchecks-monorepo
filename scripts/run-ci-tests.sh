#!/usr/bin/env bash
# run-ci-tests.sh — local mirror of the GitHub Actions CI smoke-test subset.
#
# Runs the same commands as the CI jobs without hitting production
# (no live RDS, Cognito, or EC2). Safe to run on a laptop or on EC2
# against the source checkout (not the deployed dist).
#
# Usage:
#   bash scripts/run-ci-tests.sh
#
# Requirements: Node 20, npm ci already run in each workspace.
# Exits non-zero on first failure.

set -euo pipefail
export NODE_OPTIONS=--max-old-space-size=4096

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== CI subset: Worker — Registry Contracts ==="
(
  cd "$REPO_ROOT/worker"
  npx jest \
    "src/core/registry/__tests__/unified-node-registry-contract.test.ts" \
    --runInBand --no-coverage --passWithNoTests
)

echo ""
echo "=== CI subset: Worker — Microservice Delegation ==="
(
  cd "$REPO_ROOT/worker"
  npm run test:microservices-delegation
)

echo ""
echo "=== CI subset: Worker — FIX-1/2/3 Regressions ==="
(
  cd "$REPO_ROOT/worker"
  npx jest \
    --runInBand --no-coverage --passWithNoTests \
    --testPathPattern "form-trigger|dispatch-execution-notifications|email-service|if_else" \
    --testPathIgnorePatterns "bug-condition|fillmode|log-output-merge|branch-generation"
)

echo ""
echo "=== CI subset: workflow-crud-service ==="
(
  cd "$REPO_ROOT/services/workflow-crud-service"
  npm test -- --runInBand --no-coverage
)

echo ""
echo "=== CI subset: COMPLETE — all jobs passed ==="
