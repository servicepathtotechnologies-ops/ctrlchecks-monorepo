// Chat API Route
// Worker API handler

import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { config } from '../core/config';
import { HybridMemoryService } from '../shared/memory';
import { logger } from '../core/logger';

interface ChatRequest {
  workflowId: string;
  message: string;
  sessionId?: string;
  apiKey?: string;
  metadata?: Record<string, any>;
}

interface ChatResponse {
  response: string;
  sessionId: string;
  metadata?: Record<string, any>;
  suggestions?: string[];
}

export default async function chatApiHandler(req: Request, res: Response) {
  const db = getDbClient();

  try {
    const { workflowId, message, sessionId, apiKey, metadata }: ChatRequest = req.body;

    if (!workflowId) {
      return res.status(400).json({ error: 'workflowId is required' });
    }

    if (!message || typeof message !== 'string' || !message.trim()) {
      return res.status(400).json({ error: 'message is required' });
    }

    // Fetch workflow
    const { data: workflow, error: workflowError } = await db
      .from('workflows')
      .select('*')
      .eq('id', workflowId)
      .single();

    if (workflowError || !workflow) {
      return res.status(404).json({ error: 'Workflow not found' });
    }

    const { isSetupPending, setupPendingResponse } = await import('./workflow-setup-lifecycle');
    if (isSetupPending(workflow)) {
      return res.status(409).json(setupPendingResponse(workflowId));
    }

    // Check if workflow is chatbot or agent type
    if (workflow.workflow_type !== 'chatbot' && workflow.workflow_type !== 'agent') {
      return res.status(400).json({ error: 'Workflow is not a chatbot or agent type' });
    }

    // Generate or use provided session ID
    const chatSessionId = sessionId || `chat-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

    // Initialize memory service
    const memoryService = new HybridMemoryService(config.supabaseUrl, config.supabaseKey);
    await memoryService.initialize();
    await memoryService.getOrCreateSession(workflowId, chatSessionId);

    // Store user message in memory
    await memoryService.store(chatSessionId, 'user', message, metadata);

    // Prepare input for workflow execution
    const workflowInput = {
      message,
      session_id: chatSessionId,
      _session_id: chatSessionId,
      _workflow_id: workflowId,
      metadata: metadata || {},
    };

    // Execute workflow by calling execute-workflow endpoint
    const executeUrl = `${config.publicBaseUrl}/api/execute-workflow`;
    
    let workflowResponse: any;
    try {
      const response = await fetch(executeUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          workflowId,
          input: workflowInput,
        }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Workflow execution failed: ${errorText}`);
      }

      workflowResponse = await response.json();
    } catch (error) {
      logger.error('Error executing workflow:', error);
      return res.status(500).json({ 
        error: 'Failed to execute workflow',
        message: error instanceof Error ? error.message : String(error)
      });
    }

    // Extract response from workflow output
    let chatResponse = '';
    const output = workflowResponse.output || {};

    if (typeof output === 'string') {
      chatResponse = output;
    } else if (typeof output === 'object' && output !== null) {
      chatResponse = output.response || 
                    output.message || 
                    output.text || 
                    output.content ||
                    JSON.stringify(output);
    }

    // Store assistant response in memory
    await memoryService.store(chatSessionId, 'assistant', chatResponse, {
      executionId: workflowResponse.executionId,
      ...metadata,
    });

    const response: ChatResponse = {
      response: chatResponse,
      sessionId: chatSessionId,
      metadata: {
        executionId: workflowResponse.executionId,
        ...metadata,
      },
    };

    return res.json(response);
  } catch (error) {
    logger.error('Chat API error:', error);
    const errorMessage = error instanceof Error ? error.message : String(error);
    return res.status(500).json({ error: errorMessage });
  }
}
