# Runbook: Database Restore

**Service**: ctrlchecks-worker  
**Database**: AWS RDS PostgreSQL (`ctrlchecks-db`, `ap-south-1`)

---

## When to Use This Runbook

- Accidental data deletion (user data, workflows, credentials)
- Database corruption after a failed migration
- Disaster recovery — RDS instance unavailable

---

## RTO / RPO Targets

| Metric | Target | Notes |
|---|---|---|
| RPO (max data loss) | ≤ 24 hours | Automated daily snapshots |
| RTO (restore time) | ≤ 2 hours | Includes DNS/app reconfiguration |

For better RPO: enable RDS Point-in-Time Recovery (PITR) and restore to within 5 minutes of any incident.

---

## Option 1: Point-in-Time Restore (PITR) — Preferred

Restores to any second within the backup retention window (default 7 days).

### AWS Console steps

1. Go to **RDS → Databases → ctrlchecks-db → Actions → Restore to point in time**
2. Choose **Latest restorable time** or a specific timestamp
3. New DB identifier: `ctrlchecks-db-restore-YYYYMMDD`
4. Keep same instance class (`db.t3.micro`)
5. Keep same VPC and security groups
6. Click **Restore DB Instance** (takes ~10-15 minutes)

### AWS CLI equivalent

```bash
aws rds restore-db-instance-to-point-in-time \
  --source-db-instance-identifier ctrlchecks-db \
  --target-db-instance-identifier ctrlchecks-db-restore-$(date +%Y%m%d) \
  --restore-time 2026-06-10T14:00:00Z \
  --region ap-south-1
```

### After restore completes

```bash
# 1. Get the new endpoint
aws rds describe-db-instances \
  --db-instance-identifier ctrlchecks-db-restore-$(date +%Y%m%d) \
  --region ap-south-1 \
  --query 'DBInstances[0].Endpoint.Address' \
  --output text

# 2. Update DATABASE_URL on the server
ssh -i Guide/Worker/ctrlchecks-backend.pem ubuntu@3.7.115.58
sed -i 's|ctrlchecks-db.cxm8gymyysvy|<new-endpoint>|g' /opt/ctrlchecks-worker/.env

# 3. Restart worker
sudo systemctl restart ctrlchecks-worker
sleep 10 && curl -fsS http://localhost:3001/health/ready
```

---

## Option 2: Restore from Automated Snapshot

Use when a specific daily snapshot is preferable over PITR.

```bash
# List available automated snapshots
aws rds describe-db-snapshots \
  --db-instance-identifier ctrlchecks-db \
  --snapshot-type automated \
  --region ap-south-1 \
  --query 'DBSnapshots[*].[DBSnapshotIdentifier,SnapshotCreateTime,Status]' \
  --output table

# Restore from a specific snapshot
aws rds restore-db-instance-from-db-snapshot \
  --db-instance-identifier ctrlchecks-db-restored \
  --db-snapshot-identifier <snapshot-id> \
  --db-instance-class db.t3.micro \
  --region ap-south-1
```

---

## Option 3: Dump/Restore (for selective table recovery)

When only specific tables need restoring (e.g. accidentally deleted workflows):

```bash
# 1. Create a pg_dump of the restored RDS instance
pg_dump \
  "postgresql://ctrlchecks_admin:<pass>@<restored-endpoint>:5432/ctrlchecks" \
  --table workflows \
  --table executions \
  -Fc -f /tmp/selective-restore.dump

# 2. Restore only those tables into production
pg_restore \
  --data-only \
  --table workflows \
  --table executions \
  -d "postgresql://ctrlchecks_admin:<pass>@<prod-endpoint>:5432/ctrlchecks" \
  /tmp/selective-restore.dump
```

---

## Verify Restore Succeeded

```bash
# Connect to restored DB and verify row counts
psql "postgresql://ctrlchecks_admin:<pass>@<endpoint>:5432/ctrlchecks" << 'SQL'
SELECT
  (SELECT COUNT(*) FROM users)       AS user_count,
  (SELECT COUNT(*) FROM workflows)   AS workflow_count,
  (SELECT COUNT(*) FROM executions)  AS execution_count,
  (SELECT COUNT(*) FROM credentials) AS credential_count;
SQL

# Confirm worker is healthy after switching endpoint
curl -fsS http://localhost:3001/health/ready
```

---

## Cleanup (after restore is verified)

```bash
# Delete the temporary restored instance (saves ~$20/month)
aws rds delete-db-instance \
  --db-instance-identifier ctrlchecks-db-restore-YYYYMMDD \
  --skip-final-snapshot \
  --region ap-south-1
```

---

## Prevention

1. Run `./scripts/verify-rds-backup.sh` weekly (or add to CI cron)
2. Enable Multi-AZ for automatic failover (eliminates RTO for instance failure)
3. Set backup retention to ≥ 7 days (currently configured — verify with script)
4. Never run `DROP TABLE` or mass deletes directly on production — use a migration file
5. Before any risky migration: create a manual snapshot first:
   ```bash
   aws rds create-db-snapshot \
     --db-instance-identifier ctrlchecks-db \
     --db-snapshot-identifier pre-migration-$(date +%Y%m%d-%H%M) \
     --region ap-south-1
   ```
