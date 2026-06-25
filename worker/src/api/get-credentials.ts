// Get user credentials endpoint
// Returns stored credentials for LinkedIn/Google integrations

import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { logger } from '../core/logger';

export default async function getCredentialsHandler(req: Request, res: Response) {
  const db = getDbClient();

  try {
    const { service } = req.query;

    if (!service || (service !== 'linkedin' && service !== 'google')) {
      return res.status(400).json({
        error: 'Invalid service. Must be "linkedin" or "google"',
      });
    }

    // Get current user from session
    const { data: { user }, error: authError } = await db.auth.getUser();

    if (authError || !user) {
      return res.status(401).json({
        error: 'Unauthorized',
      });
    }

    // Fetch credentials from database
    const { data, error } = await db
      .from('user_credentials')
      .select('credentials')
      .eq('user_id', user.id)
      .eq('service', service)
      .single();

    if (error) {
      if (error.code === 'PGRST116') {
        // No credentials found
        return res.json({ credentials: null });
      }
      throw error;
    }

    return res.json({
      credentials: data?.credentials || null,
    });
  } catch (error) {
    logger.error('Error getting credentials:', error);
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Failed to get credentials',
    });
  }
}
