// Chatbot Page API
// Serves static HTML pages for chatbot workflows

import { Request, Response } from 'express';
import { chatbotPageGenerator } from '../services/chatbot-page-generator';
import { getDbClient } from '../core/database/aws-db-client';
import { logger } from '../core/logger';

/**
 * Serve chatbot page for a workflow
 * GET /workflows/:workflowId/page
 * GET /workflows/:workflowId/embed (embed mode)
 */
export async function serveChatbotPage(req: Request, res: Response) {
  try {
    const { workflowId } = req.params;
    
    if (!workflowId) {
      return res.status(400).json({ error: 'Workflow ID is required' });
    }

    const db = getDbClient();
    
    // Fetch workflow
    const { data: workflow, error } = await db
      .from('workflows')
      .select('id, name, nodes, edges, is_public, auth_required, setup_completed, metadata')
      .eq('id', workflowId)
      .single();

    if (error || !workflow) {
      return res.status(404).send(`
        <html>
          <body style="font-family: sans-serif; padding: 40px; text-align: center;">
            <h1>Workflow Not Found</h1>
            <p>The requested chatbot workflow does not exist.</p>
          </body>
        </html>
      `);
    }

    const { isSetupPending } = await import('./workflow-setup-lifecycle');
    if (isSetupPending(workflow)) {
      return res.status(404).send(`
        <html>
          <body style="font-family: sans-serif; padding: 40px; text-align: center;">
            <h1>Workflow Not Found</h1>
            <p>The requested chatbot workflow does not exist.</p>
          </body>
        </html>
      `);
    }

    // Check if workflow is a chatbot workflow
    if (!chatbotPageGenerator.isChatbotWorkflow(workflow)) {
      return res.status(400).send(`
        <html>
          <body style="font-family: sans-serif; padding: 40px; text-align: center;">
            <h1>Not a Chatbot Workflow</h1>
            <p>This workflow is not configured as a chatbot workflow.</p>
          </body>
        </html>
      `);
    }

    // Check if this is an embed request
    const isEmbed = req.path.includes('/embed');
    
    // Generate chatbot page
    const baseUrl = process.env.PUBLIC_BASE_URL || `http://${req.get('host')}`;
    const endpointUrl = chatbotPageGenerator.getChatbotEndpointUrl(workflowId, baseUrl);
    const pageUrl = chatbotPageGenerator.getChatbotPageUrl(workflowId, baseUrl);
    const embedUrl = chatbotPageGenerator.getChatbotEmbedUrl(workflowId, baseUrl);
    
    // Check if memory is enabled (check for memory node in workflow)
    const hasMemoryNode = (workflow.nodes || []).some((node: any) => {
      const nodeType = node.type || node.data?.type || '';
      return nodeType.toLowerCase().includes('memory');
    });
    
    // Check if auth is enabled (check workflow settings or metadata)
    const authEnabled = workflow.is_public === false || workflow.auth_required === true;
    
    const html = chatbotPageGenerator.generateChatbotPage({
      workflowId: workflow.id,
      workflowName: workflow.name || 'Chatbot',
      endpointUrl,
      pageUrl,
      embedUrl,
      memoryEnabled: hasMemoryNode || true, // Default to true for chatbot workflows
      authEnabled,
      isEmbed,
    });

    res.setHeader('Content-Type', 'text/html');
    res.send(html);
  } catch (error) {
    logger.error('Error serving chatbot page:', error);
    res.status(500).send(`
      <html>
        <body style="font-family: sans-serif; padding: 40px; text-align: center;">
          <h1>Error</h1>
          <p>An error occurred while loading the chatbot page.</p>
        </body>
      </html>
    `);
  }
}
