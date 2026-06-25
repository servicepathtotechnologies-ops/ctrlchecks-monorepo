import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { releaseExecutionLock } from '../services/execution/execution-lock';
import { logger } from '../core/logger';

export async function cancelExecutionRoute(req: Request, res: Response) {
  const { executionId } = req.params;
  const userId = (req as any).user?.id;

  if (!userId) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const db = getDbClient();

  const { data: execution, error: fetchError } = await db
    .from('executions')
    .select('id, status, workflow_id')
    .eq('id', executionId)
    .single();

  if (fetchError || !execution) {
    return res.status(404).json({ error: 'Execution not found' });
  }

  const { data: workflow } = await db
    .from('workflows')
    .select('user_id')
    .eq('id', execution.workflow_id)
    .single();

  const ownerUserId = workflow?.user_id;
  if (ownerUserId !== userId) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  if (execution.status !== 'running' && execution.status !== 'waiting') {
    return res.status(409).json({
      error: 'Execution is not active',
      status: execution.status,
    });
  }

  const workflowId = execution.workflow_id;

  await db
    .from('executions')
    .update({
      status: 'failed',
      error_message: 'Cancelled by user',
      finished_at: new Date().toISOString(),
    })
    .eq('id', executionId);

  if (workflowId) {
    await releaseExecutionLock(db, workflowId, executionId);
  }

  logger.info(`[CancelExecution] Execution ${executionId} cancelled by user ${userId}`);

  return res.json({ success: true, executionId, status: 'failed' });
}
