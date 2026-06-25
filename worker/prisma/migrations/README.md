# Prisma Migrations

This directory contains database migrations for the memory system.

## Setup

1. Install dependencies:
```bash
npm install
```

2. Set up your database connection in `.env`:
```
DATABASE_URL=postgresql://user:password@host:port/database
```

3. Generate Prisma client:
```bash
npx prisma generate
```

4. Run migrations:
```bash
npx prisma migrate dev --name init
```

## PostgreSQL Extensions Required

The memory system requires the `pgvector` extension for vector similarity search:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

Run this in your PostgreSQL database before running migrations.

## Manual Migration

If you prefer to run migrations manually, you can use the SQL files in this directory or generate them with:

```bash
npx prisma migrate dev --create-only
```

Then apply them manually to your database.

## Down-migration convention

Prisma does not generate rollback SQL automatically. For every migration that
makes a **destructive or hard-to-reverse change** (column drop, table drop,
data backfill), add a `-- down:` comment block at the top of the `.sql` file
so any engineer can revert it manually:

```sql
-- up: add notification_channel column
-- down: ALTER TABLE notifications DROP COLUMN IF EXISTS notification_channel;

ALTER TABLE notifications ADD COLUMN notification_channel TEXT NOT NULL DEFAULT 'email';
```

After writing the rollback SQL manually, mark the migration resolved via:

```bash
bash worker/scripts/rollback-migration.sh <migration_name>
```

See `worker/scripts/rollback-migration.sh` for the full rollback procedure.
