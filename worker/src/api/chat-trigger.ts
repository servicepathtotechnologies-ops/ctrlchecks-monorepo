// Chat Trigger Route
// Handles chat messages and workflow resumption for chat trigger nodes

import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { config } from '../core/config';
import { getChatServer } from '../services/chat/chat-server';
import { v4 as uuidv4 } from 'uuid';
import {
  shouldUseTriggerService,
  dispatchChatRemote,
} from '../services/trigger-service-client';
import { logger } from '../core/logger';

/**
 * GET /api/chat-trigger/:workflowId/:nodeId
 * Get chat configuration and serve chat interface
 */
export async function getChatConfig(req: Request, res: Response) {
  const db = getDbClient();
  const { workflowId, nodeId } = req.params;

  try {
    // Verify workflow exists and is active
    const { data: workflow, error: workflowError } = await db
      .from("workflows")
      .select("*")
      .eq("id", workflowId)
      .single();

    if (workflowError || !workflow) {
      return res.status(404).json({ 
        error: "Workflow not found", 
        message: "The requested workflow could not be found." 
      });
    }

    const { isSetupPending } = await import('./workflow-setup-lifecycle');
    if (isSetupPending(workflow)) {
      return res.status(404).json({
        error: "Workflow not found",
        message: "The requested workflow could not be found."
      });
    }

    if (workflow.status !== "active") {
      return res.status(400).json({ 
        error: "Chat expired", 
        message: "This chat is no longer active. The workflow has been deactivated." 
      });
    }

    // Find the chat trigger node - try multiple matching strategies
    const nodes = workflow.nodes as any[];
    
    // First, try to find by ID and type (most specific)
    let chatNode = nodes?.find((node: any) => {
      const nodeIdMatch = node.id === nodeId || node.data?.id === nodeId;
      const typeMatch = node.data?.type === "chat_trigger" || node.type === "chat_trigger";
      return nodeIdMatch && typeMatch;
    });
    
    // If not found, try to find by ID only (in case type check is too strict)
    if (!chatNode) {
      chatNode = nodes?.find((node: any) => 
        node.id === nodeId || node.data?.id === nodeId
      );
      
      // If found by ID but wrong type, log warning but allow it (might be a custom node)
      if (chatNode) {
        const nodeType = chatNode.data?.type || chatNode.type;
        if (nodeType !== "chat_trigger") {
          logger.warn('[Chat Trigger] Found node by ID but type is:', nodeType, 'Expected: chat_trigger');
          // Still allow it - might be a custom node that acts as chat trigger
        }
      }
    }
    
    // If still not found, try to find any chat_trigger node (fallback)
    if (!chatNode) {
      chatNode = nodes?.find((node: any) => 
        node.data?.type === "chat_trigger" || node.type === "chat_trigger"
      );
      
      if (chatNode) {
        logger.warn('[Chat Trigger] Found chat_trigger node but ID mismatch. Requested:', nodeId, 'Found:', chatNode.id || chatNode.data?.id);
        // Use the found node anyway - might be a workflow with only one chat trigger
      }
    }
    
    if (!chatNode) {
      logger.warn('[Chat Trigger] Chat node not found in getChatConfig. NodeId:', nodeId);
      logger.warn('[Chat Trigger] Available nodes:', nodes?.map((n: any) => ({
        id: n.id || n.data?.id,
        type: n.type || n.data?.type,
        dataType: n.data?.type
      })));
      return res.status(404).json({ 
        error: "Chat trigger not found", 
        message: "The chat trigger node was not found in this workflow. Please ensure the workflow contains a chat trigger node and is saved." 
      });
    }

    // Return chat configuration
    res.json({
      success: true,
      workflowId,
      nodeId,
      chatNode: {
        id: chatNode.id || chatNode.data?.id,
        label: chatNode.data?.label || chatNode.label || 'Chat Trigger',
        type: chatNode.data?.type || chatNode.type,
      },
    });
  } catch (error: any) {
    logger.error('[Chat Trigger] Error getting chat config:', error);
    res.status(500).json({ 
      error: "Server error", 
      message: "Failed to load chat configuration." 
    });
  }
}

/**
 * POST /api/chat-trigger/:workflowId/:nodeId/message
 * Submit chat message and trigger NEW workflow execution (like webhook)
 */
export async function submitChatMessage(req: Request, res: Response) {
  const db = getDbClient();
  const { workflowId, nodeId } = req.params;
  const { message, sessionId } = req.body;

  try {
    if (!message || typeof message !== 'string' || message.trim().length === 0) {
      return res.status(400).json({
        error: "Invalid message",
        message: "Message is required and must be a non-empty string."
      });
    }

    // Canary: delegate to trigger-service when enabled and workflowId hashes in
    if (shouldUseTriggerService(workflowId)) {
      const delegated = await dispatchChatRemote(workflowId, nodeId, {
        message,
        sessionId,
      });
      if (delegated) {
        return res.json({
          success: true,
          message: 'Chat message received. Workflow execution started.',
          executionId: delegated.executionId,
          status: delegated.status,
        });
      }
      logger.warn(`[chat-trigger] trigger-service returned null for ${workflowId}/${nodeId} — local fallback`);
    }

    // Verify workflow exists and is active
    const { data: workflow, error: workflowError } = await db
      .from("workflows")
      .select("*")
      .eq("id", workflowId)
      .single();

    if (workflowError || !workflow) {
      return res.status(404).json({ 
        error: "Workflow not found", 
        message: "The requested workflow could not be found." 
      });
    }

    const { isSetupPending } = await import('./workflow-setup-lifecycle');
    if (isSetupPending(workflow)) {
      return res.status(404).json({
        error: "Workflow not found",
        message: "The requested workflow could not be found."
      });
    }

    if (workflow.status !== "active") {
      return res.status(400).json({ 
        error: "Workflow not active", 
        message: "This workflow is not active. Please activate it to receive chat messages." 
      });
    }

    // Find the chat trigger node - try multiple matching strategies
    const nodes = workflow.nodes as any[];
    
    // First, try to find by ID and type (most specific)
    let chatNode = nodes?.find((node: any) => {
      const nodeIdMatch = node.id === nodeId || node.data?.id === nodeId;
      const typeMatch = node.data?.type === "chat_trigger" || node.type === "chat_trigger";
      return nodeIdMatch && typeMatch;
    });
    
    // If not found, try to find by ID only (in case type check is too strict)
    if (!chatNode) {
      chatNode = nodes?.find((node: any) => 
        node.id === nodeId || node.data?.id === nodeId
      );
      
      // If found by ID but wrong type, log warning but allow it (might be a custom node)
      if (chatNode) {
        const nodeType = chatNode.data?.type || chatNode.type;
        if (nodeType !== "chat_trigger") {
          logger.warn('[Chat Trigger] Found node by ID but type is:', nodeType, 'Expected: chat_trigger');
          // Still allow it - might be a custom node that acts as chat trigger
        }
      }
    }
    
    // If still not found, try to find any chat_trigger node (fallback)
    if (!chatNode) {
      chatNode = nodes?.find((node: any) => 
        node.data?.type === "chat_trigger" || node.type === "chat_trigger"
      );
      
      if (chatNode) {
        logger.warn('[Chat Trigger] Found chat_trigger node but ID mismatch. Requested:', nodeId, 'Found:', chatNode.id || chatNode.data?.id);
        // Use the found node anyway - might be a workflow with only one chat trigger
      }
    }
    
    if (!chatNode) {
      logger.warn('[Chat Trigger] Chat node not found. NodeId:', nodeId);
      logger.warn('[Chat Trigger] Available nodes:', nodes?.map((n: any) => ({
        id: n.id || n.data?.id,
        type: n.type || n.data?.type,
        dataType: n.data?.type,
        fullNode: JSON.stringify(n).substring(0, 200) // First 200 chars for debugging
      })));
      return res.status(404).json({ 
        error: "Chat trigger not found", 
        message: "The chat trigger node was not found in this workflow. Please ensure the workflow contains a chat trigger node and is saved." 
      });
    }

    // Generate session ID - ALWAYS use static format: workflowId_nodeId for consistent chat sessions
    // This ensures the chat UI and backend use the same sessionId for WebSocket connections
    // Ignore any provided sessionId and always use the static format for consistency
    const chatSessionId = `${workflowId}_${nodeId}`;
    logger.info(`[Chat Trigger] Using static sessionId format: ${chatSessionId} (workflowId: ${workflowId}, nodeId: ${nodeId})`);

    // Create or ensure chat session exists for WebSocket connections
    try {
      const chatServer = getChatServer();
      if (!chatServer.hasSession(chatSessionId)) {
        // Create session with a temporary execution ID (will be updated when execution is created)
        chatServer.createSession(chatSessionId, workflowId, `temp-${Date.now()}`, nodeId);
        logger.info(`[Chat Trigger] Created chat session: ${chatSessionId}`);
      }
    } catch (sessionError: any) {
      logger.warn(`[Chat Trigger] Failed to create chat session (non-fatal):`, sessionError?.message || sessionError);
      // Don't fail the request if session creation fails - it will be auto-created when needed
    }

    // Prepare execution input with chat message (like webhook does)
    const executionInput = {
      message: message.trim(),
      trigger: 'chat',
      workflow_id: workflowId,
      node_id: nodeId,
      sessionId: chatSessionId,
      timestamp: new Date().toISOString(),
      _chat: true,
    };

    // Create NEW execution record (like webhook trigger does)
    const startedAt = new Date().toISOString();
    const { data: execution, error: execError } = await db
      .from('executions')
      .insert({
        workflow_id: workflowId,
        user_id: workflow.user_id,
        status: 'running',
        trigger: 'chat',
        input: executionInput,
        logs: [],
        started_at: startedAt,
      })
      .select()
      .single();

    if (execError || !execution) {
      logger.error('[Chat Trigger] Execution creation error:', execError);
      return res.status(500).json({ 
        error: "Server error", 
        message: "Failed to create workflow execution. Please try again." 
      });
    }

    logger.info(`[Chat Trigger] Created new execution ${execution.id} for message: ${message.trim().substring(0, 50)}...`);

    // Update chat session with actual execution ID
    try {
      const chatServer = getChatServer();
      const session = chatServer.getSession(chatSessionId);
      if (session) {
        // Update execution ID in session (session object is mutable)
        (session as any).executionId = execution.id;
        logger.info(`[Chat Trigger] Updated chat session ${chatSessionId} with execution ID: ${execution.id}`);
      }
    } catch (sessionError: any) {
      logger.warn(`[Chat Trigger] Failed to update chat session (non-fatal):`, sessionError?.message || sessionError);
    }

    // Trigger workflow execution asynchronously (like webhook does)
    const executeUrl = config.publicBaseUrl 
      ? `${config.publicBaseUrl}/api/execute-workflow`
      : (process.env.NODE_ENV === 'production' 
          ? (() => {
              logger.error('[Chat Trigger] PUBLIC_BASE_URL is required in production');
              return null;
            })()
          : `http://localhost:${config.port}/api/execute-workflow`);

    if (!executeUrl) {
      return res.status(500).json({
        error: 'Configuration error',
        message: 'PUBLIC_BASE_URL environment variable is required in production.',
      });
    }

    // Trigger workflow execution (don't wait for it)
    fetch(executeUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Internal-Chat-Execution": "true", // Bypass Google OAuth for internal chat-trigger calls
      },
      body: JSON.stringify({
        workflowId,
        executionId: execution.id,
        input: executionInput,
      }),
    }).catch((error) => {
      logger.error('[Chat Trigger] Error triggering workflow execution:', error);
    });

    // Return immediate response (like webhook does)
    return res.json({
      success: true,
      message: "Chat message received. Workflow execution started.",
      executionId: execution.id,
      status: 'running',
    });
  } catch (error: any) {
    logger.error('[Chat Trigger] Error submitting chat message:', error);
    res.status(500).json({ 
      error: "Server error", 
      message: "Failed to process chat message. Please try again." 
    });
  }
}

/**
 * Main handler for chat trigger routes
 */
export default async function chatTriggerHandler(req: Request, res: Response) {
  const { workflowId, nodeId } = req.params;
  
  logger.info(`[Chat Trigger] ${req.method} ${req.originalUrl} - workflowId: ${workflowId}, nodeId: ${nodeId}`);

  if (req.method === 'GET') {
    return getChatConfig(req, res);
  } else if (req.method === 'POST') {
    // Check if it's a message submission
    const originalUrl = req.originalUrl || '';
    const path = req.path || '';
    const url = req.url || '';
    const isMessage = originalUrl.endsWith('/message') || 
                     path.endsWith('/message') || 
                     url.endsWith('/message') ||
                     originalUrl.includes('/message') ||
                     path.includes('/message');

    if (isMessage) {
      return submitChatMessage(req, res);
    } else {
      // POST without /message - treat as message for backwards compatibility
      return submitChatMessage(req, res);
    }
  } else {
    return res.status(405).json({ 
      error: "Method not allowed", 
      message: "This endpoint only supports GET and POST requests." 
    });
  }
}
