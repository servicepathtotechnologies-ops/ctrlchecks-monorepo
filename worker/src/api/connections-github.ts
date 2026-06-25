import { Request, Response } from 'express';
import { queryAsService } from '../core/database/db-pool';
import { decryptToken } from '../core/utils/token-encryption';
import { logger } from '../core/logger';

/**
 * GitHub connection status + disconnect.
 * OAuth connect flow is handled in oauth-github.ts.
 */

export async function githubStatusHandler(req: Request, res: Response) {
  try {
    const userId = (req as any).user?.id as string | undefined;
    if (!userId) return res.status(401).json({ success: false, error: 'Unauthorized' });

    const rows = await queryAsService(
      `SELECT id, provider_user_id, scope, created_at, updated_at, expires_at
       FROM social_tokens WHERE user_id = $1 AND provider = 'github' LIMIT 1`,
      [userId]
    );
    const token = rows[0];

    if (!token) {
      return res.json({ success: true, connected: false, metadata: null });
    }

    // Optionally verify token is still valid by calling GitHub API
    let githubLogin: string | null = null;
    try {
      const accessRows = await queryAsService(
        `SELECT access_token FROM social_tokens WHERE user_id = $1 AND provider = 'github' LIMIT 1`,
        [userId]
      );
      const at = accessRows[0]?.access_token;
      if (at) {
        const accessToken = decryptToken(at);
        const r = await fetch('https://api.github.com/user', {
          headers: { Authorization: `token ${accessToken}`, 'User-Agent': 'CtrlChecks' },
        });
        if (r.ok) {
          const profile: any = await r.json();
          githubLogin = profile.login || null;
        }
      }
    } catch { /* non-fatal */ }

    return res.json({
      success:   true,
      connected: true,
      metadata: {
        tokenId:          token.id,
        scope:            token.scope,
        provider_user_id: token.provider_user_id,
        login:            githubLogin,
        createdAt:        token.created_at,
        updatedAt:        token.updated_at,
        expiresAt:        token.expires_at || null,
      },
    });
  } catch (err: any) {
    logger.error('[GitHubStatus]', err.message);
    return res.status(500).json({ success: false, error: 'Internal server error' });
  }
}

export async function githubDisconnectHandler(req: Request, res: Response) {
  try {
    const userId = (req as any).user?.id as string | undefined;
    if (!userId) return res.status(401).json({ success: false, error: 'Unauthorized' });

    await queryAsService(
      `DELETE FROM social_tokens WHERE user_id = $1 AND provider = 'github'`,
      [userId]
    );

    return res.json({ success: true, message: 'GitHub account disconnected' });
  } catch (err: any) {
    logger.error('[GitHubDisconnect]', err.message);
    return res.status(500).json({ success: false, error: 'Internal server error' });
  }
}
