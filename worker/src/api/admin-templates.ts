// Admin Templates API Route
// Worker API handler

import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { corsHeaders } from '../shared/cors';
import { logger } from '../core/logger';

interface TemplateInput {
  name: string;
  description?: string;
  category: string;
  nodes: unknown;
  edges: unknown;
  difficulty?: 'Beginner' | 'Intermediate' | 'Advanced';
  estimated_setup_time?: number;
  tags?: string[];
  is_featured?: boolean;
  preview_image?: string;
}

export default async function adminTemplatesHandler(req: Request, res: Response) {
  const db = getDbClient();

  try {
    // Get auth token
    const authHeader = req.headers.authorization;
    if (!authHeader) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    const token = authHeader.replace('Bearer ', '');
    
    // Verify user and get user ID
    const { data: { user }, error: authError } = await db.auth.getUser(token);
    if (authError || !user) {
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // Check if user is admin
    const { data: roleData, error: roleError } = await db
      .from('user_roles')
      .select('role')
      .eq('user_id', user.id)
      .eq('role', 'admin')
      .single();

    if (roleError || !roleData) {
      return res.status(403).json({ error: 'Admin access required' });
    }

    const method = req.method;
    const templateId = req.params.id;

    // Route handling
    if (method === 'GET' && !templateId) {
      // List all templates
      const { data, error } = await db
        .from('templates')
        .select('*')
        .order('created_at', { ascending: false });

      if (error) throw error;

      return res.json({ templates: data });
    }

    if (method === 'GET' && templateId) {
      // Get single template
      const { data, error } = await db
        .from('templates')
        .select('*')
        .eq('id', templateId)
        .single();

      if (error) throw error;

      return res.json({ template: data });
    }

    if (method === 'POST') {
      // Create template
      const templateData: TemplateInput = req.body;

      if (!templateData.name || !templateData.category) {
        return res.status(400).json({ error: 'name and category are required' });
      }

      const { data, error } = await db
        .from('templates')
        .insert({
          ...templateData,
          created_by: user.id,
        })
        .select()
        .single();

      if (error) throw error;

      return res.status(201).json({ template: data });
    }

    if ((method === 'PUT' || method === 'PATCH') && templateId) {
      // Update template
      const templateData: Partial<TemplateInput> = req.body;

      const { data, error } = await db
        .from('templates')
        .update(templateData)
        .eq('id', templateId)
        .select()
        .single();

      if (error) throw error;

      return res.json({ template: data });
    }

    if (method === 'DELETE' && templateId) {
      // Delete template
      const { error } = await db
        .from('templates')
        .delete()
        .eq('id', templateId);

      if (error) throw error;

      return res.json({ success: true });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (error) {
    logger.error('Admin templates error:', error);
    const errorMessage = error instanceof Error ? error.message : String(error);
    return res.status(500).json({ error: errorMessage });
  }
}
