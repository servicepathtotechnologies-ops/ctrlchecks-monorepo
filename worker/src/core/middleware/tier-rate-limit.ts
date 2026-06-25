/**
 * Tier-aware rate limiting middleware.
 *
 * Enforces per-user per-minute limits based on the user's subscription tier,
 * adding RFC-standard X-RateLimit-* headers to every response so clients can
 * implement back-off without hitting 429s.
 *
 * Tier resolution (from req.user.subscriptionPlan set by authenticateUser):
 *   - 'Enterprise' / 'enterprise'               → enterprise limits
 *   - anything else that is not 'Free'           → paid limits
 *   - 'Free' / missing plan / unauthenticated    → free limits
 *
 * Falls back to in-memory counters when Redis is unavailable so the service
 * never hard-fails due to rate-limiter issues.
 */

import { Request, Response, NextFunction } from 'express';
import type { AuthenticatedRequest } from './subscription-auth';

// ─── Tier limits ─────────────────────────────────────────────────────────────

export interface TierLimits {
  executePerMin: number;
  generatePerMin: number;
}

const TIER_LIMITS: Record<string, TierLimits> = {
  free:       { executePerMin: 60,  generatePerMin: 20  },
  paid:       { executePerMin: 100, generatePerMin: 50  },
  enterprise: { executePerMin: 200, generatePerMin: 100 },
};

function resolveTier(plan: string | undefined): 'free' | 'paid' | 'enterprise' {
  if (!plan) return 'free';
  const lower = plan.toLowerCase();
  if (lower === 'enterprise') return 'enterprise';
  if (lower === 'free') return 'free';
  return 'paid';
}

// ─── In-memory fallback store ────────────────────────────────────────────────

interface Counter {
  count: number;
  resetAt: number;
}

const inMemoryStore = new Map<string, Counter>();

function inMemoryCheck(
  key: string,
  limit: number,
  windowMs: number
): { allowed: boolean; remaining: number; resetAt: number } {
  const now = Date.now();

  let entry = inMemoryStore.get(key);
  if (!entry || now >= entry.resetAt) {
    entry = { count: 0, resetAt: now + windowMs };
    inMemoryStore.set(key, entry);
  }

  entry.count += 1;
  const allowed = entry.count <= limit;
  const remaining = Math.max(0, limit - entry.count);

  return { allowed, remaining, resetAt: entry.resetAt };
}

// Prune expired in-memory entries to avoid unbounded growth.
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of inMemoryStore) {
    if (now >= v.resetAt) inMemoryStore.delete(k);
  }
}, 60_000).unref();

// ─── Redis-backed check ──────────────────────────────────────────────────────

async function redisCheck(
  key: string,
  limit: number,
  windowMs: number
): Promise<{ allowed: boolean; remaining: number; resetAt: number } | null> {
  try {
    const { createClient } = await import('redis');
    const redisUrl = process.env.REDIS_URL;
    if (!redisUrl) return null;

    // Lazy singleton so we don't create a new connection per request
    const client = (globalThis as any).__tierRateLimitRedis as ReturnType<typeof createClient> | undefined;
    if (!client || !client.isOpen) {
      const c = createClient({ url: redisUrl });
      c.on('error', () => { /* handled by fallback */ });
      await c.connect();
      (globalThis as any).__tierRateLimitRedis = c;
    }

    const redis = (globalThis as any).__tierRateLimitRedis as ReturnType<typeof createClient>;
    const windowSec = Math.ceil(windowMs / 1000);
    const pipeline = redis.multi();
    pipeline.incr(key);
    pipeline.expire(key, windowSec, 'NX' as any);
    pipeline.ttl(key);
    const result = await pipeline.exec();

    const count = Number(result?.[0] ?? 1);
    const ttlSec = Number(result?.[2] ?? windowSec);
    const allowed = count <= limit;
    const remaining = Math.max(0, limit - count);
    const resetAt = Date.now() + ttlSec * 1000;

    return { allowed, remaining, resetAt };
  } catch {
    return null; // Redis unavailable → fall through to in-memory
  }
}

// ─── Middleware factory ──────────────────────────────────────────────────────

export type EndpointType = 'execute' | 'generate';

export function tierRateLimit(endpoint: EndpointType) {
  const windowMs = 60_000;

  return async (req: Request, res: Response, next: NextFunction) => {
    const authReq = req as AuthenticatedRequest;
    const tier = resolveTier(authReq.user?.subscriptionPlan);
    const limits = TIER_LIMITS[tier];
    const limit = endpoint === 'execute' ? limits.executePerMin : limits.generatePerMin;

    const userId = authReq.user?.id || req.ip || 'anon';
    const key = `tier-rl:${endpoint}:${userId}`;

    let result = await redisCheck(key, limit, windowMs);
    if (!result) {
      result = inMemoryCheck(key, limit, windowMs);
    }

    res.setHeader('X-RateLimit-Limit', String(limit));
    res.setHeader('X-RateLimit-Remaining', String(result.remaining));
    res.setHeader('X-RateLimit-Reset', String(Math.ceil(result.resetAt / 1000)));
    res.setHeader('X-RateLimit-Tier', tier);

    if (!result.allowed) {
      const retryAfter = Math.ceil((result.resetAt - Date.now()) / 1000);
      res.setHeader('Retry-After', String(retryAfter));
      return res.status(429).json({
        error: 'Too many requests',
        code: 'TIER_RATE_LIMIT_EXCEEDED',
        tier,
        limit,
        retryAfter,
        message: `${tier} tier allows ${limit} ${endpoint} calls per minute`,
      });
    }

    next();
  };
}
