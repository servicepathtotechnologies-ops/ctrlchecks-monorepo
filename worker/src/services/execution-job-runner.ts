/**
 * Execution Job Runner
 *
 * Handles the full lifecycle of a single execution job:
 *   1. Mark execution as 'running' in the DB
 *   2. Publish 'running' WS event via Redis bridge
 *   3. Call execute-workflow handler
 *   4. Mark execution as 'success' or 'failed' in the DB
 *   5. Publish terminal WS event
 *
 * Called by ExecutionQueue.executeJob() so the queue logic stays
 * focused on scheduling/retry, not DB or WS concerns.
 */

import { getDbClient } from '../core/database/aws-db-client';
import { publishExecutionEvent } from './ws-redis-bridge';
import type { ExecutionJob } from './execution-queue';

export interface JobRunResult {
  status: 'success' | 'failed';
  durationMs: number;
  error?: string;
  result?: any;
}

/**
 * Run a single execution job end-to-end.
 * Throws on unrecoverable errors; returns a result object otherwise.
 */
export async function runExecutionJob(job: ExecutionJob): Promise<JobRunResult> {
  const startedAt = Date.now();
  const db = getDbClient();

  // ── 1. Mark execution as running ─────────────────────────────────────────
  try {
    await db
      .from('executions')
      .update({ status: 'running', current_node: null })
      .eq('id', job.executionId);
  } catch (dbErr) {
    console.warn(`[JobRunner] Could not update execution ${job.executionId} to running:`, dbErr);
  }

  await publishExecutionEvent(job.executionId, {
    type: 'EXECUTION_UPDATE',
    data: { executionId: job.executionId, status: 'running', progress: 0, currentStep: null },
  }).catch(() => { /* non-fatal */ });

  // ── 2. Execute workflow ───────────────────────────────────────────────────
  let executionResult: any = null;
  let executionError: string | undefined;
  let responseStatus = 200;

  const executeWorkflowHandler = (await import('../api/execute-workflow')).default;

  const req = {
    body: {
      workflowId: job.workflowId,
      executionId: job.executionId,
      input: job.input,
      useQueue: false,
    },
    headers: {
      authorization: job.metadata?.authToken ? `Bearer ${job.metadata.authToken}` : undefined,
      ...(job.metadata?.headers || {}),
    },
  } as any;

  const res = {
    statusCode: 200,
    status(code: number) { responseStatus = code; return this; },
    json(data: any) {
      executionResult = data;
      if (responseStatus >= 400) executionError = data?.error || data?.message || 'Execution failed';
      return this;
    },
    send(data: any) {
      executionResult = data;
      if (responseStatus >= 400) executionError = typeof data === 'string' ? data : (data?.error || 'Execution failed');
      return this;
    },
  } as any;

  try {
    await executeWorkflowHandler(req, res);
  } catch (err: any) {
    executionError = err?.message || String(err);
  }

  // ── 3. Determine outcome ──────────────────────────────────────────────────
  const durationMs = Date.now() - startedAt;
  const succeeded = !executionError && responseStatus < 400;
  const finalStatus: 'success' | 'failed' = succeeded ? 'success' : 'failed';

  // ── 4. Persist terminal state to DB ──────────────────────────────────────
  try {
    await db
      .from('executions')
      .update({
        status: finalStatus,
        duration_ms: durationMs,
        error: executionError ?? null,
        current_node: null,
        finished_at: new Date().toISOString(),
      })
      .eq('id', job.executionId);
  } catch (dbErr) {
    console.warn(`[JobRunner] Could not update execution ${job.executionId} to ${finalStatus}:`, dbErr);
  }

  // ── 5. Publish terminal WS event ─────────────────────────────────────────
  await publishExecutionEvent(job.executionId, {
    type: 'EXECUTION_UPDATE',
    data: {
      executionId: job.executionId,
      status: finalStatus,
      progress: succeeded ? 100 : 0,
      durationMs,
      error: executionError ?? null,
    },
  }).catch(() => { /* non-fatal */ });

  console.log(`[JobRunner] ${finalStatus.toUpperCase()}: execution ${job.executionId} (${durationMs}ms)`);

  // Notifications are dispatched inside execute-workflow.ts terminal path,
  // which is called by this runner — no dispatch needed here.

  return {
    status: finalStatus,
    durationMs,
    error: executionError,
    result: executionResult,
  };
}
