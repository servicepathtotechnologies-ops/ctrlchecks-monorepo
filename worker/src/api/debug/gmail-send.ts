/**
 * Debug Endpoint for Gmail Send Testing
 * 
 * POST /api/debug/gmail-send
 * Body: { workflowId, nodeId }
 * 
 * Tests Gmail credential resolution and send operation without full workflow execution
 */

import { Request, Response } from 'express';
import { getDbClient } from '../../core/database/aws-db-client';
import { resolveGmailCredentials, sendGmailEmail, REQUIRED_GMAIL_SCOPES } from '../../shared/gmail-executor';
import { logger } from '../../core/logger';

export default async function debugGmailSendHandler(req: Request, res: Response) {
  try {
    const { workflowId, nodeId, to, subject, body } = req.body;
    
    if (!workflowId) {
      return res.status(400).json({
        success: false,
        error: 'workflowId is required',
      });
    }
    
    if (!nodeId) {
      return res.status(400).json({
        success: false,
        error: 'nodeId is required',
      });
    }
    
    // Get user from auth header
    const db = getDbClient();
    const authHeader = req.headers.authorization;
    let userId: string | undefined;
    let currentUserId: string | undefined;
    
    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.replace('Bearer ', '').trim();
      if (token) {
        try {
          const { data: { user }, error: authError } = await db.auth.getUser(token);
          if (!authError && user) {
            currentUserId = user.id;
          }
        } catch (authErr) {
          logger.warn('[DebugGmailSend] Auth error (non-fatal):', authErr);
        }
      }
    }
    
    // Get workflow owner
    const { data: workflow, error: workflowError } = await db
      .from('workflows')
      .select('user_id')
      .eq('id', workflowId)
      .single();
    
    if (workflowError || !workflow) {
      return res.status(404).json({
        success: false,
        error: 'Workflow not found',
      });
    }
    
    userId = workflow.user_id;
    
    logger.info(`[DebugGmailSend] Testing Gmail send for workflow ${workflowId}, node ${nodeId}`);
    
    // Resolve credentials
    const credential = await resolveGmailCredentials(
      db,
      workflowId,
      nodeId,
      userId,
      currentUserId
    );
    
    if (!credential) {
      return res.status(401).json({
        success: false,
        error: 'Gmail credentials not found. Please connect your Google account.',
        details: {
          workflowOwner: userId,
          currentUser: currentUserId,
          hint: 'Connect Google account in settings and ensure Gmail scopes are granted.',
        },
      });
    }
    
    // Check scopes
    const scopes = credential.scopes || [];
    const hasRequiredScopes = REQUIRED_GMAIL_SCOPES.some(requiredScope =>
      scopes.some((scope: string) => scope === requiredScope || scope.includes('gmail'))
    );
    
    // Test send if to/subject/body provided
    if (to && subject && body) {
      const sendResult = await sendGmailEmail(credential, {
        to,
        subject,
        body,
      });
      
      return res.json({
        success: sendResult.success,
        credential: {
          userId: credential.userId,
          hasAccessToken: !!credential.accessToken,
          hasRefreshToken: !!credential.refreshToken,
          expiresAt: credential.expiresAt?.toISOString(),
          scopes: credential.scopes,
          hasRequiredScopes,
        },
        sendResult: sendResult.success ? {
          messageId: sendResult.messageId,
        } : {
          error: sendResult.error,
        },
      });
    }
    
    // Just return credential info
    return res.json({
      success: true,
      credential: {
        userId: credential.userId,
        hasAccessToken: !!credential.accessToken,
        hasRefreshToken: !!credential.refreshToken,
        expiresAt: credential.expiresAt?.toISOString(),
        scopes: credential.scopes,
        hasRequiredScopes,
        requiredScopes: REQUIRED_GMAIL_SCOPES,
      },
      message: 'Credentials resolved successfully. Provide to/subject/body to test send.',
    });
  } catch (error) {
    logger.error('[DebugGmailSend] Error:', error);
    return res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error',
    });
  }
}
