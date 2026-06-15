import { Pool } from 'pg';

let pool: Pool | null = null;

export async function getDb(): Promise<Pool | null> {
  if (pool) return pool;

  const url = process.env.DATABASE_URL;
  if (!url) return null;

  try {
    pool = new Pool({
      connectionString: url,
      max: 5,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 3000,
      ssl: { rejectUnauthorized: false },
    });
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

/** Runs a parameterized query and returns the rows array. Throws if DB unavailable. */
export async function queryDb(sql: string, params: unknown[] = []): Promise<any[]> {
  const db = await getDb();
  if (!db) throw new Error('Database unavailable — DATABASE_URL not configured');
  const result = await db.query(sql, params);
  return result.rows;
}

/** Check DB connectivity — used by /health/ready. */
export async function checkDb(): Promise<'ok' | 'skip' | 'error'> {
  const url = process.env.DATABASE_URL;
  if (!url) return 'skip';
  try {
    const db = await getDb();
    if (!db) return 'skip';
    await db.query('SELECT 1');
    return 'ok';
  } catch {
    return 'error';
  }
}

/** Reset pool — for test isolation only. */
export function _resetPool(): void {
  pool = null;
}
