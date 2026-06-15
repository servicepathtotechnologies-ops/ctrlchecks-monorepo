import { Pool } from 'pg';

let pool: Pool | null = null;

export async function getDb(): Promise<Pool | null> {
  if (pool) return pool;

  const url = process.env.DATABASE_URL;
  if (!url) return null;

  try {
    pool = new Pool({ connectionString: url, max: 2, idleTimeoutMillis: 30000, connectionTimeoutMillis: 3000, ssl: { rejectUnauthorized: false } });
    return pool;
  } catch {
    return null;
  }
}

export async function closeDb(): Promise<void> {
  if (pool) {
    const p = pool;
    pool = null;
    await p.end();
  }
}
