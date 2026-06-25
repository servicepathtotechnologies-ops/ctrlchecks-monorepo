// Copy Template API Route
// Worker API handler

import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { logger } from '../core/logger';

export default async function copyTemplateHandler(req: Request, res: Response) {
  const db = getDbClient();

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    // Get auth token
    const authHeader = req.headers.authorization;
    if (!authHeader) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const token = authHeader.replace('Bearer ', '');
    
    // Verify user
    const { data: { user }, error: authError } = await db.auth.getUser(token);
    if (authError || !user) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const { templateId, workflowName } = req.body;

    if (!templateId) {
      return res.status(400).json({ error: 'templateId is required' });
    }

    // Get template (only active templates for users)
    const { data: template, error: templateError } = await db
      .from('templates')
      .select('*')
      .eq('id', templateId)
      .eq('is_active', true)
      .single();

    if (templateError || !template) {
      return res.status(404).json({ error: 'Template not found or inactive' });
    }

    // Create workflow from template
    const { data: workflow, error: workflowError } = await db
      .from('workflows')
      .insert({
        name: workflowName || `${template.name} (Copy)`,
        nodes: template.nodes,
        edges: template.edges,
        user_id: user.id,
        source: 'template',
        template_id: template.id,
        template_version: template.version,
        status: 'draft',
      })
      .select()
      .single();

    if (workflowError) throw workflowError;

    // Increment template use count
    await db
      .from('templates')
      .update({ use_count: (template.use_count || 0) + 1 })
      .eq('id', templateId);

    return res.status(201).json({ 
      workflow,
      message: 'Template copied successfully'
    });
  } catch (error) {
    logger.error('Copy template error:', error);
    const errorMessage = error instanceof Error ? error.message : 'Internal server error';
    return res.status(500).json({ error: errorMessage });
  }
}
