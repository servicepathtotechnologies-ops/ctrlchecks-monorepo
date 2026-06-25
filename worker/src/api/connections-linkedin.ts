import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { logger } from '../core/logger';

/**
 * LinkedIn connection utilities:
 * - GET /api/connections/linkedin/status
 * - POST /api/connections/linkedin/test
 * - POST /api/connections/linkedin/refresh-now
 * - DELETE /api/connections/linkedin (disconnect)
 */

export async function linkedinStatusHandler(req: Request, res: Response) {
  try {
    const db = getDbClient();

    const authHeader = req.headers.authorization;
    let userId: string | undefined;

    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.replace('Bearer ', '').trim();
      if (token) {
        const { data: { user }, error } = await db.auth.getUser(token);
        if (!error && user) {
          userId = user.id;
        }
      }
    }

    if (!userId) {
      return res.status(401).json({
        success: false,
        error: 'Unauthorized',
      });
    }

    const { data: tokenData, error: tokenError } = await db
      .from('linkedin_oauth_tokens')
      .select('id, expires_at, scope, created_at, updated_at')
      .eq('user_id', userId)
      .maybeSingle();

    if (tokenError && tokenError.code !== 'PGRST116') {
      logger.error('[LinkedInStatus] Error querying linkedin_oauth_tokens:', tokenError.message);
      return res.status(500).json({
        success: false,
        error: 'Failed to load LinkedIn connection status',
      });
    }

    const now = new Date();
    const expiresAt = tokenData?.expires_at ? new Date(tokenData.expires_at) : null;
    const fiveMinutesFromNow = new Date(now.getTime() + 5 * 60 * 1000);

    const connected = !!tokenData && (!!expiresAt ? expiresAt > now : true);
    const expiresSoon = !!expiresAt && expiresAt <= fiveMinutesFromNow;

    return res.json({
      success: true,
      connected,
      metadata: {
        tokenId: tokenData?.id,
        scope: tokenData?.scope,
        createdAt: tokenData?.created_at,
        updatedAt: tokenData?.updated_at,
        expiresAt: expiresAt?.toISOString() || null,
        expiresSoon,
      },
    });
  } catch (error) {
    logger.error('[LinkedInStatus] Unexpected error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
}

export async function linkedinDisconnectHandler(req: Request, res: Response) {
  try {
    const db = getDbClient();

    const authHeader = req.headers.authorization;
    let userId: string | undefined;

    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.replace('Bearer ', '').trim();
      if (token) {
        const { data: { user }, error } = await db.auth.getUser(token);
        if (!error && user) {
          userId = user.id;
        }
      }
    }

    if (!userId) {
      return res.status(401).json({
        success: false,
        error: 'Unauthorized',
      });
    }

    // Delete LinkedIn OAuth tokens
    const { error: tokenError } = await db
      .from('linkedin_oauth_tokens')
      .delete()
      .eq('user_id', userId);

    if (tokenError) {
      logger.error('[LinkedInDisconnect] Error deleting linkedin_oauth_tokens:', tokenError.message);
      return res.status(500).json({
        success: false,
        error: 'Failed to delete LinkedIn tokens',
      });
    }

    // Delete vault credential entry for LinkedIn
    const { error: vaultError } = await db
      .from('user_credentials')
      .delete()
      .eq('user_id', userId)
      .eq('service', 'linkedin');

    if (vaultError && vaultError.code !== 'PGRST116') {
      logger.error('[LinkedInDisconnect] Error deleting user_credentials (linkedin):', vaultError.message);
      // Non-fatal; tokens are already removed
    }

    return res.json({
      success: true,
      message: 'LinkedIn account disconnected successfully',
    });
  } catch (error) {
    logger.error('[LinkedInDisconnect] Unexpected error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
}

export async function linkedinTestHandler(req: Request, res: Response) {
  try {
    const db = getDbClient();

    const authHeader = req.headers.authorization;
    let userId: string | undefined;

    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.replace('Bearer ', '').trim();
      if (token) {
        const { data: { user }, error } = await db.auth.getUser(token);
        if (!error && user) {
          userId = user.id;
        }
      }
    }

    if (!userId) {
      return res.status(401).json({
        success: false,
        error: 'Unauthorized',
      });
    }

    // Reuse shared helper to resolve access token (includes refresh handling)
    const { getLinkedInAccessToken } = await import('../shared/linkedin-oauth');
    const accessToken = await getLinkedInAccessToken(db, userId);

    if (!accessToken) {
      return res.status(401).json({
        success: false,
        error: 'LinkedIn credentials not found. Please connect your LinkedIn account.',
      });
    }

    // Verify token validity and fetch basic profile info.
    // Via LinkedIn OIDC provider, LinkedIn exposes OIDC `userinfo`.
    // Fallback to legacy `/v2/me` for older apps/scopes.
    const tryEndpoints = [
      { name: 'userinfo', url: 'https://api.linkedin.com/v2/userinfo' },
      { name: 'me', url: 'https://api.linkedin.com/v2/me' },
    ] as const;

    let status = 0;
    let profile: any = null;
    let lastErrText = '';

    for (const ep of tryEndpoints) {
      const response = await fetch(ep.url, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          ...(ep.name === 'userinfo' ? {} : { 'X-Restli-Protocol-Version': '2.0.0' }),
        },
      });

      status = response.status;
      if (response.ok) {
        try {
          profile = await response.json();
        } catch {
          profile = null;
        }
        break;
      } else {
        lastErrText = await response.text();
        logger.warn(`[LinkedInTest] LinkedIn /v2/${ep.name} error:`, status, lastErrText.slice(0, 200));
      }
    }

    if (!profile) {
      return res.status(status || 500).json({
        success: false,
        error: 'LinkedIn API test failed',
        details: {
          status: status || 500,
        },
      });
    }

    return res.json({
      success: true,
      status: 'ok',
      profile: {
        // OIDC userinfo uses `sub`, legacy uses `id`
        id: profile?.sub || profile?.id,
        // OIDC fields
        name: profile?.name,
        given_name: profile?.given_name,
        family_name: profile?.family_name,
        email: profile?.email,
        // legacy fields
        localizedFirstName: profile?.localizedFirstName,
        localizedLastName: profile?.localizedLastName,
        localizedHeadline: profile?.headline || profile?.localizedHeadline,
      },
    });
  } catch (error) {
    logger.error('[LinkedInTest] Unexpected error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
}

export async function linkedinRefreshNowHandler(req: Request, res: Response) {
  try {
    const db = getDbClient();

    const authHeader = req.headers.authorization;
    let userId: string | undefined;

    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.replace('Bearer ', '').trim();
      if (token) {
        const { data: { user }, error } = await db.auth.getUser(token);
        if (!error && user) {
          userId = user.id;
        }
      }
    }

    if (!userId) {
      return res.status(401).json({
        success: false,
        error: 'Unauthorized',
      });
    }

    const { data: tokenRow, error: tokenError } = await db
      .from('linkedin_oauth_tokens')
      .select('refresh_token')
      .eq('user_id', userId)
      .maybeSingle();

    if (tokenError && tokenError.code !== 'PGRST116') {
      logger.error('[LinkedInRefreshNow] Error querying tokens:', tokenError.message);
      return res.status(500).json({
        success: false,
        error: 'Failed to load LinkedIn credentials',
      });
    }

    if (!tokenRow || !tokenRow.refresh_token) {
      return res.status(400).json({
        success: false,
        error: 'No LinkedIn refresh token available. Please reconnect LinkedIn.',
      });
    }

    const { getLinkedInAccessToken } = await import('../shared/linkedin-oauth');

    // Use the helper which will attempt refresh when nearing expiry;
    // by calling it immediately, we force the refresh path to run if appropriate.
    const accessToken = await getLinkedInAccessToken(db, userId);

    if (!accessToken) {
      return res.status(500).json({
        success: false,
        error: 'LinkedIn token refresh failed',
      });
    }

    return res.json({
      success: true,
      message: 'LinkedIn token refreshed successfully (if refresh token was valid).',
    });
  } catch (error) {
    logger.error('[LinkedInRefreshNow] Unexpected error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
}

