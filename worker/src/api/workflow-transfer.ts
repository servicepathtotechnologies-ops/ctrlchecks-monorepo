/**
 * Workflow Ownership Transfer
 *
 * Allows a user to claim ownership of workflows that belong to a different
 * Cognito sub but share the same email address (e.g. localhost vs production
 * login, or email/password vs Google OAuth login).
 *
 * POST /api/workflows/:workflowId/transfer-ownership
 *   - Authenticated endpoint
 *   - Transfers workflow.user_id to the calling user
 *   - Guard: the current owner's email must match the caller's email
 *
 * POST /api/workflows/transfer-all
 *   - Transfers ALL workflows whose owner shares the caller's email
 *   - Useful for the first-time production login scenario
 */

import { Request, Response } from 'express';
import { queryAsService } from '../core/database/db-pool';
import { logger } from '../core/logger';

function getUserId(req: Request): string | null {
  return (req as any).user?.id || (req as any).user?.sub || null;
}

function getUserEmail(req: Request): string | null {
  return (req as any).user?.email || null;
}

/**
 * Resolve all user IDs that share the same email (across different Cognito subs).
 */
async function getPeerUserIds(email: string, excludeId: string): Promise<string[]> {
  const rows = await queryAsService<{ id: string }>(
    `SELECT id FROM users WHERE LOWER(email) = LOWER($1) AND id != $2`,
    [email, excludeId]
  );
  return rows.map((r) => r.id);
}

/**
 * POST /api/workflows/:workflowId/transfer-ownership
 *
 * Transfer a single workflow to the authenticated user.
 */
export async function transferWorkflowOwnership(req: Request, res: Response) {
  const currentUserId = getUserId(req);
  const currentEmail = getUserEmail(req);

  if (!currentUserId) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }

  const { workflowId } = req.params;
  if (!workflowId) {
    return res.status(400).json({ success: false, error: 'workflowId is required' });
  }

  try {
    // Fetch workflow
    const workflows = await queryAsService<{ id: string; user_id: string; title: string }>(
      `SELECT id, user_id, title FROM workflows WHERE id = $1 LIMIT 1`,
      [workflowId]
    );

    if (!workflows.length) {
      return res.status(404).json({ success: false, error: 'Workflow not found' });
    }

    const workflow = workflows[0];

    // Already owned by caller
    if (workflow.user_id === currentUserId) {
      return res.json({ success: true, message: 'You already own this workflow', workflowId });
    }

    // Verify caller's email matches the owner's email (security guard)
    if (currentEmail) {
      const ownerRows = await queryAsService<{ email: string }>(
        `SELECT email FROM users WHERE id = $1 LIMIT 1`,
        [workflow.user_id]
      );
      const ownerEmail = ownerRows[0]?.email;

      if (ownerEmail && ownerEmail.toLowerCase() !== currentEmail.toLowerCase()) {
        return res.status(403).json({
          success: false,
          error: 'You can only claim workflows that belong to an account with the same email address',
        });
      }
    }

    // Transfer ownership
    await queryAsService(
      `UPDATE workflows SET user_id = $1, updated_at = NOW() WHERE id = $2`,
      [currentUserId, workflowId]
    );

    logger.info(`[WorkflowTransfer] Workflow ${workflowId} ("${workflow.title}") transferred from ${workflow.user_id} → ${currentUserId}`);

    return res.json({
      success: true,
      message: `Workflow "${workflow.title}" transferred to your account`,
      workflowId,
    });
  } catch (error: any) {
    logger.error('[WorkflowTransfer] Error:', error.message);
    return res.status(500).json({ success: false, error: 'Transfer failed' });
  }
}

/**
 * POST /api/workflows/transfer-all
 *
 * Transfer ALL workflows from peer user IDs (same email, different sub)
 * to the authenticated user. Useful on first production login.
 */
export async function transferAllWorkflows(req: Request, res: Response) {
  const currentUserId = getUserId(req);
  const currentEmail = getUserEmail(req);

  if (!currentUserId || !currentEmail) {
    return res.status(401).json({ success: false, error: 'Unauthorized' });
  }

  try {
    const peerIds = await getPeerUserIds(currentEmail, currentUserId);

    if (!peerIds.length) {
      return res.json({ success: true, transferred: 0, message: 'No peer accounts found' });
    }

    // Count workflows to transfer
    const countPlaceholders = peerIds.map((_, i) => `$${i + 1}`).join(', ');
    const countRows = await queryAsService<{ count: string }>(
      `SELECT COUNT(*) as count FROM workflows WHERE user_id IN (${countPlaceholders})`,
      peerIds
    );
    const total = parseInt(countRows[0]?.count || '0', 10);

    if (total === 0) {
      return res.json({ success: true, transferred: 0, message: 'No workflows to transfer' });
    }

    // Transfer all — $1 = currentUserId, $2..$n = peerIds
    const updatePlaceholders = peerIds.map((_, i) => `$${i + 2}`).join(', ');
    await queryAsService(
      `UPDATE workflows SET user_id = $1, updated_at = NOW()
        WHERE user_id IN (${updatePlaceholders})`,
      [currentUserId, ...peerIds]
    );

    logger.info(`[WorkflowTransfer] Bulk transfer: ${total} workflows from [${peerIds.join(', ')}] → ${currentUserId} (email: ${currentEmail})`);

    return res.json({
      success: true,
      transferred: total,
      message: `${total} workflow${total !== 1 ? 's' : ''} transferred to your account`,
    });
  } catch (error: any) {
    logger.error('[WorkflowTransfer] Bulk transfer error:', error.message);
    return res.status(500).json({ success: false, error: 'Bulk transfer failed' });
  }
}
