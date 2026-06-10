#!/usr/bin/env bash
# verify-rds-backup.sh — Check RDS automated backup retention and last snapshot.
#
# Usage:
#   ./scripts/verify-rds-backup.sh [--instance ctrlchecks-db] [--region ap-south-1]
#
# Prerequisites:
#   - AWS CLI installed and configured (aws configure or IAM instance role)
#   - rds:DescribeDBInstances + rds:DescribeDBSnapshots permissions
#
# Exit codes:
#   0 — backup retention ≥ 7 days and a recent snapshot exists
#   1 — backup retention < 7 days OR no snapshot in last 24 hours (alert!)
#   2 — AWS CLI not available or instance not found

set -euo pipefail

DB_INSTANCE="${1:-ctrlchecks-db}"
AWS_REGION="${AWS_REGION:-ap-south-1}"
MIN_RETENTION_DAYS=7
MAX_SNAPSHOT_AGE_HOURS=24

# ─── Check AWS CLI ─────────────────────────────────────────────────────────────
if ! command -v aws &>/dev/null; then
  echo "❌ AWS CLI not found. Install: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
  exit 2
fi

echo "▶ Checking RDS instance: $DB_INSTANCE (region: $AWS_REGION)"
echo ""

# ─── Fetch instance info ───────────────────────────────────────────────────────
INSTANCE_JSON=$(aws rds describe-db-instances \
  --db-instance-identifier "$DB_INSTANCE" \
  --region "$AWS_REGION" \
  --output json 2>&1) || {
  echo "❌ Could not describe RDS instance '$DB_INSTANCE'. Check instance name and AWS credentials."
  exit 2
}

RETENTION=$(echo "$INSTANCE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['DBInstances'][0]['BackupRetentionPeriod'])
")

STATUS=$(echo "$INSTANCE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['DBInstances'][0]['DBInstanceStatus'])
")

ENGINE=$(echo "$INSTANCE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
i = d['DBInstances'][0]
print(i['Engine'] + ' ' + i['EngineVersion'])
")

CLASS=$(echo "$INSTANCE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['DBInstances'][0]['DBInstanceClass'])
")

MULTI_AZ=$(echo "$INSTANCE_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d['DBInstances'][0]['MultiAZ'])
")

echo "  Instance class : $CLASS"
echo "  Engine         : $ENGINE"
echo "  Status         : $STATUS"
echo "  Multi-AZ       : $MULTI_AZ"
echo "  Backup retention: ${RETENTION} days"
echo ""

FAILED=0

# ─── Check retention ───────────────────────────────────────────────────────────
if [[ "$RETENTION" -lt "$MIN_RETENTION_DAYS" ]]; then
  echo "❌ ALERT: Backup retention is ${RETENTION} days (minimum: ${MIN_RETENTION_DAYS})"
  echo "   Fix in AWS Console: RDS → $DB_INSTANCE → Modify → Backup retention period"
  FAILED=1
else
  echo "✅ Backup retention: ${RETENTION} days (≥ ${MIN_RETENTION_DAYS} required)"
fi

# ─── Check automated snapshots ────────────────────────────────────────────────
echo ""
echo "▶ Checking automated snapshots…"
SNAPSHOTS=$(aws rds describe-db-snapshots \
  --db-instance-identifier "$DB_INSTANCE" \
  --snapshot-type automated \
  --region "$AWS_REGION" \
  --output json 2>/dev/null)

SNAPSHOT_COUNT=$(echo "$SNAPSHOTS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(len(d['DBSnapshots']))
")

if [[ "$SNAPSHOT_COUNT" -eq 0 ]]; then
  echo "❌ ALERT: No automated snapshots found for $DB_INSTANCE"
  FAILED=1
else
  LATEST_SNAPSHOT=$(echo "$SNAPSHOTS" | python3 -c "
import sys, json, datetime
d = json.load(sys.stdin)
snaps = sorted(d['DBSnapshots'], key=lambda x: x.get('SnapshotCreateTime',''), reverse=True)
s = snaps[0]
print(s['DBSnapshotIdentifier'] + '|' + s.get('SnapshotCreateTime','unknown') + '|' + s.get('Status','unknown') + '|' + str(s.get('AllocatedStorage',0)) + 'GB')
")
  IFS='|' read -r SNAP_ID SNAP_TIME SNAP_STATUS SNAP_SIZE <<< "$LATEST_SNAPSHOT"

  echo "  Latest snapshot : $SNAP_ID"
  echo "  Created         : $SNAP_TIME"
  echo "  Status          : $SNAP_STATUS"
  echo "  Size            : $SNAP_SIZE"
  echo "  Total snapshots : $SNAPSHOT_COUNT"
  echo ""

  # Check age of latest snapshot
  SNAP_AGE_HOURS=$(python3 -c "
import datetime, sys
ts = '$SNAP_TIME'
if ts == 'unknown':
    print(9999)
    sys.exit()
try:
    snap_dt = datetime.datetime.fromisoformat(ts.replace('Z','+00:00'))
    now = datetime.datetime.now(datetime.timezone.utc)
    age = (now - snap_dt).total_seconds() / 3600
    print(f'{age:.1f}')
except:
    print(9999)
")

  if python3 -c "exit(0 if float('$SNAP_AGE_HOURS') <= $MAX_SNAPSHOT_AGE_HOURS else 1)"; then
    echo "✅ Latest snapshot is ${SNAP_AGE_HOURS}h old (≤ ${MAX_SNAPSHOT_AGE_HOURS}h threshold)"
  else
    echo "❌ ALERT: Latest snapshot is ${SNAP_AGE_HOURS}h old (threshold: ${MAX_SNAPSHOT_AGE_HOURS}h)"
    FAILED=1
  fi
fi

# ─── Multi-AZ warning ─────────────────────────────────────────────────────────
echo ""
if [[ "$MULTI_AZ" == "False" ]]; then
  echo "⚠  Multi-AZ is DISABLED — no automatic failover. Consider enabling for production."
else
  echo "✅ Multi-AZ is enabled"
fi

# ─── Summary ──────────────────────────────────────────────────────────────────
echo ""
if [[ "$FAILED" -eq 0 ]]; then
  echo "✅ RDS backup check PASSED"
  exit 0
else
  echo "❌ RDS backup check FAILED — see alerts above"
  exit 1
fi
