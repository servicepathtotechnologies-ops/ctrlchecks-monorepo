// Chatbot Message API
// Handles messages from chatbot page to workflow

import { Request, Response } from 'express';
import { chatbotPageGenerator } from '../services/chatbot-page-generator';
import { getDbClient } from '../core/database/aws-db-client';
import { HybridMemoryService } from '../shared/memory';
import { config } from '../core/config';
import { logger } from '../core/logger';

/**
 * Handle chatbot message
 * POST /api/chatbot/:workflowId/message
 * 
 * Request body:
 * {
 *   "session_id": "string",
 *   "user_message": "string"
 * }
 * 
 * Response:
 * {
 *   "reply": "string"
 * }
 */
export async function handleChatbotMessage(req: Request, res: Response) {
  try {
    const { workflowId } = req.params;
    const { session_id, user_message } = req.body;

    if (!workflowId) {
      return res.status(400).json({ error: 'Workflow ID is required' });
    }

    if (!user_message || typeof user_message !== 'string') {
      return res.status(400).json({ error: 'user_message is required' });
    }

    const db = getDbClient();
    
    // Fetch workflow
    const { data: workflow, error } = await db
      .from('workflows')
      .select('id, name, nodes, edges, user_id, setup_completed, metadata')
      .eq('id', workflowId)
      .single();

    if (error || !workflow) {
      return res.status(404).json({ error: 'Workflow not found' });
    }

    const { isSetupPending } = await import('./workflow-setup-lifecycle');
    if (isSetupPending(workflow)) {
      return res.status(404).json({ error: 'Workflow not found' });
    }

    // Check if workflow is a chatbot workflow
    if (!chatbotPageGenerator.isChatbotWorkflow(workflow)) {
      return res.status(400).json({ error: 'Not a chatbot workflow' });
    }

    // Initialize memory service for conversation history
    const memoryService = new HybridMemoryService(config.supabaseUrl, config.supabaseKey);
    await memoryService.initialize();
    
    const finalSessionId = session_id || `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    await memoryService.getOrCreateSession(workflowId, finalSessionId);
    
    // Retrieve conversation history for context
    const conversationHistory = await memoryService.retrieve(finalSessionId, 10);
    
    // Store user message in memory
    await memoryService.store(finalSessionId, 'user', user_message);
    
    // Prepare input for workflow execution
    // The workflow should accept user_message as input
    const workflowInput = {
      user_message: user_message,
      session_id: finalSessionId,
      conversation_history: conversationHistory,
      _trigger: 'chatbot',
    };

    // Execute workflow by calling executeWorkflow API endpoint internally
    let workflowOutput: any = null;
    let executionError: any = null;

    try {
      // Call the execute workflow endpoint internally
      const baseUrl = process.env.PUBLIC_BASE_URL || `http://${req.get('host')}`;
      const executeUrl = `${baseUrl}/api/execute-workflow`;
      
      const executeResponse = await fetch(executeUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': req.headers.authorization || '',
        },
        body: JSON.stringify({
          workflowId,
          input: workflowInput,
        }),
      });

      if (!executeResponse.ok) {
        const errorData = await executeResponse.json().catch(() => ({}));
        executionError = { 
          code: executeResponse.status, 
          data: errorData 
        };
      } else {
        workflowOutput = await executeResponse.json();
      }
    } catch (error) {
      logger.error('Workflow execution error:', error);
      executionError = { code: 500, data: { error: 'Workflow execution failed' } };
    }

    // Extract reply from workflow response
    if (executionError) {
      return res.status(executionError.code || 500).json({
        reply: 'Sorry, I encountered an error processing your message. Please try again.',
      });
    }

    // Try to extract reply from workflow output
    let reply = '';
    
    if (workflowOutput) {
      // Check various possible response formats
      if (workflowOutput.output) {
        const output = workflowOutput.output;
        
        // Try to extract message/reply from output
        if (typeof output === 'string') {
          reply = output;
        } else if (output.message) {
          reply = output.message;
        } else if (output.reply) {
          reply = output.reply;
        } else if (output.response) {
          reply = output.response;
        } else if (output.response_text) {
          reply = output.response_text;
        } else if (output.data?.message) {
          reply = output.data.message;
        } else if (output.data?.reply) {
          reply = output.data.reply;
        } else {
          // Fallback: stringify the output
          reply = JSON.stringify(output);
        }
      } else if (workflowOutput.result) {
        const result = workflowOutput.result;
        if (typeof result === 'string') {
          reply = result;
        } else if (result.message) {
          reply = result.message;
        } else if (result.reply) {
          reply = result.reply;
        } else {
          reply = JSON.stringify(result);
        }
      } else if (workflowOutput.message) {
        reply = workflowOutput.message;
      } else if (workflowOutput.reply) {
        reply = workflowOutput.reply;
      } else if (typeof workflowOutput === 'string') {
        reply = workflowOutput;
      } else {
        // Last resort: try to find any string field
        const stringFields = Object.values(workflowOutput).filter(v => typeof v === 'string');
        reply = stringFields[0] as string || 'I received your message, but could not generate a response.';
      }
    } else {
      reply = 'I received your message, but could not generate a response.';
    }

    // Return response in required format
    return res.json({
      reply: reply || 'I received your message.',
    });
  } catch (error) {
    logger.error('Error handling chatbot message:', error);
    return res.status(500).json({
      reply: 'Sorry, an error occurred. Please try again.',
    });
  }
}
