# Live Server Test Runbook

## Test environment matrix

| Where | What | When to use |
|---|---|---|
| **GitHub Actions (CI)** | Mocked unit tests — no live DB/Cognito | Every PR automatically |
| **Laptop (source checkout)** | Same as CI via `scripts/run-ci-tests.sh` | Before pushing a fix locally |
| **EC2 (T1+T3)** | Infrastructure smoke + contract/regression tests | After every deploy |
| **EC2 (T4)** | Live HTTP with real JWT — CREATE/execute/DELETE | Manual, post-deploy validation |

## CI (GitHub Actions) — automatic on every PR

CI runs the mocked subset automatically. No action needed — just push.

To mirror CI locally:
```bash
bash scripts/run-ci-tests.sh
```

Jobs on CI:
- `worker-test-contracts` — registry contracts (unified-node-registry, ~3378 tests)
- `worker-test-delegation` — microservice client delegation (111 tests)
- `worker-test-fix-regressions` — FIX-1/2/3 guards (39 tests)
- `workflow-crud-service-test` — full crud suite incl. LT-012 quota tests (70 tests)
- `microservices-smoke` — per-service matrix (ai-gen, exec-engine, cred, notif, trigger)

Excluded from CI (documented P3/P4 gaps — see `.claude/logs/LIVE_TEST_ISSUES.md`):
- `registry-frontend-parity.test.ts` — LT-001: frontend not co-located on CI runner
- `workflow-auto-repair.test.ts`, `integration.test.ts` — LT-002: old node shape in mocks
- `node-schema-registry.test.ts` — LT-003: slack test uses wrong field
- `bug-condition-*`, `log-output-merge-*` — LT-004/005/007/008: intentional bug-doc tests

---

**Server:** ubuntu@3.7.115.58 | PEM: Guide/Worker/ctrlchecks-backend.pem

## Quick run (T1+T2+T3 — safe, no token needed)

```bash
ssh -i Guide/Worker/ctrlchecks-backend.pem ubuntu@3.7.115.58
cd /opt/ctrlchecks-worker
bash scripts/run-live-tests.sh
```

## With T4 live E2E (requires Cognito JWT for test user)

```bash
RUN_T4=1 LIVE_TEST_BEARER_TOKEN="<cognito jwt>" bash scripts/run-live-tests.sh
```

## Individual tiers

```bash
# T1 — infrastructure only (~30s)
bash scripts/verify-production.sh

# T3a — registry contracts only (~5min)
cd worker && npm run test:contracts -- --no-coverage

# T3b — delegation client tests only (~3min)
cd worker && npm run test:microservices-delegation

# T3c — FIX-1/2/3 regression tests (~3min)
cd worker && npm run test:live-regression
```

## After deploy on EC2 (read-only smoke, no JWT required)

Run after `sudo systemctl restart ctrlchecks-*` to verify the deployment landed:
```bash
# T1 — infra smoke (~30s, safe, read-only)
bash /opt/ctrlchecks-worker/scripts/verify-production.sh

# T1+T3 — infra + mocked contract/regression tests
bash /opt/ctrlchecks-worker/scripts/run-live-tests.sh
```

Note: `run-live-tests.sh` runs T1–T3 only by default. T4 requires an explicit token (see below).

> **DEPLOY-HOOK:** Do NOT wire these into deploy scripts automatically without
> `approve task DEPLOY-HOOK` — post-deploy hooks can block rollbacks if they hang.

## Rules

- **Never** run `npm test --coverage` or `npm test` (full suite) on EC2 — 453 test files, OOM risk
- **Never** run T4 without a dedicated test user token (not main admin)
- All T4 artifacts prefixed `live-test-*` and auto-deleted after run
- Logs: `.claude/logs/live-runs/YYYYMMDD-HHMMSS/`

## Interpreting results

| Tier | Pass means |
|---|---|
| T1 | All 7 services active + healthy, both retirement gates = true |
| T2 | Jest runtime works in each deployed service (0 test files — expected) |
| T3a | Node registry contracts intact, no schema regressions |
| T3b | Microservice delegation client functions work correctly |
| T3c | FIX-1 (if_else), FIX-2 (form trigger), FIX-3 (notifications) still passing |
| T4 | Real HTTP: save + execute + status poll + delegation metric + cleanup |
