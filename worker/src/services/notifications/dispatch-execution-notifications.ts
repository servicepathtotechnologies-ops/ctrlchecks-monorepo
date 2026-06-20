import { getDbClient } from '../../core/database/aws-db-client';
import { sendExecutionCompleted, sendExecutionFailed } from './email-service';
import { sendInAppExecutionCompleted, sendInAppExecutionFailed } from '../in-app-service';

interface DispatchParams {
  userId: string;
  workflowId: string;
  executionId: string;
  succeeded: boolean;
  error?: string;
}

/**
 * Fire-and-forget execution completion notifications via email + in-app.
 * Resolves workflow name from DB; never throws (notifications are best-effort).
 * Shared by execution-job-runner.ts and execute-workflow.ts.
 */
export function dispatchExecutionNotifications(params: DispatchParams): void {
  const { userId, workflowId, executionId, succeeded, error } = params;
  setImmediate(async () => {
    try {
      const db = await getDbClient();
      const { data: wfRow } = await db
        .from('workflows')
        .select('name')
        .eq('id', workflowId)
        .single();
      const workflowName = (wfRow as any)?.name ?? workflowId;
      if (succeeded) {
        await sendExecutionCompleted(userId, workflowName, executionId);
        await sendInAppExecutionCompleted(userId, workflowName, executionId);
      } else {
        await sendExecutionFailed(userId, workflowName, error ?? 'Unknown error');
        await sendInAppExecutionFailed(userId, workflowName, error ?? 'Unknown error');
      }
    } catch {
      // notifications are best-effort — never let errors surface
    }
  });
}
