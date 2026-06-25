import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { listTemplatesRemote, getTemplateRemote } from '../services/workflow-crud-service-client';
import { logger } from '../core/logger';

function normalizeSearch(value: unknown): string {
  return typeof value === 'string' ? value.trim().toLowerCase() : '';
}

export default async function templatesHandler(req: Request, res: Response) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const templateId = req.params.id;
  const category = req.query.category as string | undefined;
  const search = req.query.search as string | undefined;

  // Workflow CRUD service proxy — no canary; always try remote when enabled.
  if (templateId) {
    try {
      const remote = await getTemplateRemote(templateId);
      if (remote) {
        logger.info(`[Templates] ✅ Delegated GET /templates/${templateId} to workflow-crud-service`);
        return res.json(remote);
      }
    } catch (proxyErr) {
      logger.warn('[Templates] workflow-crud-service proxy error — falling back:', proxyErr);
    }
  } else {
    try {
      const remote = await listTemplatesRemote({ category, search });
      if (remote) {
        logger.info('[Templates] ✅ Delegated GET /templates to workflow-crud-service');
        return res.json(remote);
      }
    } catch (proxyErr) {
      logger.warn('[Templates] workflow-crud-service proxy error — falling back:', proxyErr);
    }
  }

  // Local fallback — Supabase client
  const db = getDbClient();
  const normCategory = normalizeSearch(category);
  const normSearch = normalizeSearch(search);

  try {
    let query = db
      .from('templates')
      .select('*')
      .eq('is_active', true)
      .order('is_featured', { ascending: false })
      .order('created_at', { ascending: false });

    if (templateId) {
      query = (query as any).eq('id', templateId).limit(1);
    }

    if (normCategory) {
      query = (query as any).eq('category', normCategory);
    }

    const { data, error } = await (query as any);
    if (error) throw error;

    let templates = data || [];
    if (normSearch) {
      templates = templates.filter((template: any) => {
        const name = String(template.name || '').toLowerCase();
        const description = String(template.description || '').toLowerCase();
        return name.includes(normSearch) || description.includes(normSearch);
      });
    }

    if (templateId) {
      const template = templates[0] || null;
      if (!template) return res.status(404).json({ error: 'Template not found' });
      return res.json({ template });
    }

    return res.json({ templates });
  } catch (error) {
    logger.error('Templates API error:', error);
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to fetch templates',
    });
  }
}
