/**
 * Centralized logging — backed by pino for structured JSON output in production.
 *
 * Exports:
 *   logger          — application logger (debug/info/warn/error/workflow/validation)
 *   httpLogger      — pino-http middleware (attach before routes for per-request logs)
 *   createChildLogger — bind static fields to a child logger (e.g. requestId, workflowId)
 *
 * Production:  JSON lines to stdout (aggregated by CloudWatch / Datadog).
 * Development: pretty-printed if pino-pretty is installed, else JSON.
 */

import pino from 'pino';
import pinoHttp from 'pino-http';
import { randomUUID } from 'crypto';

const isDev = process.env.NODE_ENV !== 'production';
const logLevel = (process.env.LOG_LEVEL || (isDev ? 'debug' : 'info')).toLowerCase();

// ── Transport (dev pretty-print, prod JSON) ─────────────────────────────────
let transport: pino.TransportSingleOptions | undefined;
if (isDev) {
  try {
    require.resolve('pino-pretty');
    transport = {
      target: 'pino-pretty',
      options: {
        colorize: true,
        translateTime: 'SYS:standard',
        ignore: 'pid,hostname,service',
      },
    };
  } catch {
    // pino-pretty absent — JSON output used instead
  }
}

// ── Core pino instance ───────────────────────────────────────────────────────
const pinoInstance = pino({
  level: logLevel,
  base: { service: 'ctrlchecks-worker' },
  timestamp: pino.stdTimeFunctions.isoTime,
  ...(transport ? { transport } : {}),
});

// ── Public logger — preserves the existing API used across 31 files ─────────
export const logger = {
  debug: (...args: any[]) => pinoInstance.debug(args.length === 1 ? args[0] : args.join(' ')),
  info:  (...args: any[]) => pinoInstance.info(args.length === 1 ? args[0] : args.join(' ')),
  warn:  (...args: any[]) => pinoInstance.warn(args.length === 1 ? args[0] : args.join(' ')),
  error: (...args: any[]) => pinoInstance.error(args.length === 1 ? args[0] : args.join(' ')),

  // Structured variants: pass an object as first arg to merge into log record
  debugObj: (obj: Record<string, unknown>, msg: string) => pinoInstance.debug(obj, msg),
  infoObj:  (obj: Record<string, unknown>, msg: string) => pinoInstance.info(obj, msg),
  warnObj:  (obj: Record<string, unknown>, msg: string) => pinoInstance.warn(obj, msg),
  errorObj: (obj: Record<string, unknown>, msg: string) => pinoInstance.error(obj, msg),

  // Specialised channels — kept for backward-compat callers
  workflow:   (...args: any[]) => pinoInstance.debug({ channel: 'workflow' },   args.join(' ')),
  validation: (...args: any[]) => pinoInstance.debug({ channel: 'validation' }, args.join(' ')),
};

// ── Child logger factory — binds static fields (e.g. requestId, workflowId) ─
export function createChildLogger(bindings: Record<string, unknown>) {
  const child = pinoInstance.child(bindings);
  return {
    debug: (...args: any[]) => child.debug(args.length === 1 ? args[0] : args.join(' ')),
    info:  (...args: any[]) => child.info(args.length === 1 ? args[0] : args.join(' ')),
    warn:  (...args: any[]) => child.warn(args.length === 1 ? args[0] : args.join(' ')),
    error: (...args: any[]) => child.error(args.length === 1 ? args[0] : args.join(' ')),
    infoObj:  (obj: Record<string, unknown>, msg: string) => child.info(obj, msg),
    errorObj: (obj: Record<string, unknown>, msg: string) => child.error(obj, msg),
  };
}

// ── pino-http middleware — logs every HTTP request/response with requestId ───
export const httpLogger = pinoHttp({
  logger: pinoInstance,
  // Reuse the requestId already stamped by requestIdMiddleware
  genReqId: (req) => ((req as any).requestId as string) || randomUUID(),
  customProps: (req) => ({
    requestId: (req as any).requestId,
  }),
  // Skip health and metrics endpoints to avoid log noise
  autoLogging: {
    ignore: (req) =>
      req.url === '/health' ||
      req.url === '/health/live' ||
      req.url === '/health/ready' ||
      req.url === '/metrics',
  },
  serializers: {
    req: pino.stdSerializers.req,
    res: pino.stdSerializers.res,
  },
});
