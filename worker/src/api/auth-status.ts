import { Request, Response } from 'express';
import { queryAsService } from '../core/database/db-pool';
import { logger } from '../core/logger';

/**
 * GET /api/auth/status
 * Returns authentication status for Google and LinkedIn OAuth connections.
 * Requires authenticateUser middleware on the route.
 */
export async function authStatusHandler(req: Request, res: Response) {
  try {
    const userId = (req as any).user?.id;
    if (!userId) {
      return res.status(401).json({
        googleConnected: false,
        linkedinConnected: false,
        error: 'Unauthorized',
      });
    }

    const now = new Date();

    // Check Google OAuth connection
    const googleRows = await queryAsService(
      `SELECT expires_at FROM google_oauth_tokens WHERE user_id = $1 LIMIT 1`,
      [userId]
    ).catch(() => []);
    const googleToken = googleRows[0];
    const googleConnected = googleToken
      ? (googleToken.expires_at ? new Date(googleToken.expires_at) > now : true)
      : false;

    // Check LinkedIn OAuth connection
    const linkedinRows = await queryAsService(
      `SELECT expires_at FROM linkedin_oauth_tokens WHERE user_id = $1 LIMIT 1`,
      [userId]
    ).catch(() => []);
    const linkedinToken = linkedinRows[0];
    const linkedinConnected = linkedinToken
      ? (linkedinToken.expires_at ? new Date(linkedinToken.expires_at) > now : true)
      : false;

    return res.json({ googleConnected, linkedinConnected });
  } catch (error) {
    logger.error('[AuthStatus] Unexpected error:', error);
    return res.status(500).json({
      googleConnected: false,
      linkedinConnected: false,
      error: 'Internal server error',
    });
  }
}
