# Runbook: DLQ Replay

**Service**: ctrlchecks-worker  
**Applies to**: Failed workflow executions that landed in the Dead Letter Queue

---

## What is the DLQ?

When a workflow execution fails after all retry attempts, the job is moved to the Dead Letter Queue (DLQ) — a Redis key `queue:executions:dlq`. Jobs remain there until manually inspected and replayed or discarded.

DLQ entries are created by `execution-queue.ts` when `maxAttempts` is exhausted.

---

## When to Use This Runbook

- Users report workflows stuck in "failed" with no clear error
- Monitoring shows DLQ depth > 0 after a transient infrastructure issue (Redis blip, DB overload, Gemini timeout)
- After resolving the root cause of a batch failure (e.g. RDS maintenance window)

---

## Step 1 — Inspect DLQ Contents

```bash
ssh -i Guide/Worker/ctrlchecks-backend.pem ubuntu@3.7.115.58

# Check DLQ depth
redis-cli -h 127.0.0.1 LLEN queue:executions:dlq

# Peek at the first 5 jobs (without removing)
redis-cli -h 127.0.0.1 LRANGE queue:executions:dlq 0 4
```

Each entry is a JSON string with `executionId`, `workflowId`, `userId`, `error`, `attempts`.

---

## Step 2 — Identify Root Cause

```bash
# Check worker logs around the failure time
sudo journalctl -u ctrlchecks-worker --since "2026-06-10 14:00" --until "2026-06-10 15:00" \
  | grep -E "ERROR|FAILED|DLQ|circuit" | head -50

# Check DB connection health
curl -s http://localhost:3001/health/ready | python3 -m json.tool

# Check Redis health
redis-cli -h 127.0.0.1 PING
```

Do NOT replay until root cause is resolved — replaying into a broken environment will re-DLQ the same jobs.

---

## Step 3 — Replay Jobs

### Option A: API replay (preferred)

```bash
# Replay a single job by executionId
curl -s -X POST http://localhost:3001/api/admin/dlq/replay \
  -H "Content-Type: application/json" \
  -d '{"executionId": "<id>"}' \
  -H "x-admin-key: $ADMIN_API_KEY"

# Replay all jobs in DLQ (use with caution — confirm count first)
curl -s -X POST http://localhost:3001/api/admin/dlq/replay-all \
  -H "x-admin-key: $ADMIN_API_KEY"
```

### Option B: Manual Redis replay

```bash
# Move one job back to the main queue (RPOPLPUSH is atomic)
redis-cli -h 127.0.0.1 RPOPLPUSH queue:executions:dlq queue:executions:pending
```

Repeat for each job, or use a loop:

```bash
COUNT=$(redis-cli -h 127.0.0.1 LLEN queue:executions:dlq)
for i in $(seq 1 $COUNT); do
  redis-cli -h 127.0.0.1 RPOPLPUSH queue:executions:dlq queue:executions:pending
done
echo "Replayed $COUNT jobs"
```

---

## Step 4 — Discard Jobs (if replay not appropriate)

```bash
# Remove a specific job by value
redis-cli -h 127.0.0.1 LREM queue:executions:dlq 1 '<json-string>'

# Flush entire DLQ (destructive — confirm first)
redis-cli -h 127.0.0.1 DEL queue:executions:dlq
```

---

## Step 5 — Verify

```bash
# Confirm DLQ is empty
redis-cli -h 127.0.0.1 LLEN queue:executions:dlq

# Watch worker logs for job processing
sudo journalctl -u ctrlchecks-worker -f | grep -E "JobRunner|execution"
```

---

## Prevention

- Set up an alert when `LLEN queue:executions:dlq > 0` (Redis keyspace notification or cron check)
- `maxAttempts` default is 3 with exponential backoff — tune in `execution-queue.ts` if transient failures are common
- Ensure SES email notifications are enabled (`EXECUTION_EMAIL_NOTIFICATIONS=true`) so users are notified immediately on failure
