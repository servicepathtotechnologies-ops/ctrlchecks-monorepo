import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { getZohoApiBaseUrl, ZohoRegion } from '../shared/zoho-oauth';
import { logger } from '../core/logger';

/**
 * Zoho connection utilities:
 * - GET /api/connections/zoho/status
 * - POST /api/connections/zoho/connect (save credentials)
 * - POST /api/connections/zoho/test (verify credentials)
 * - DELETE /api/connections/zoho (disconnect)
 */

export async function zohoStatusHandler(req: Request, res: Response) {
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
      .from('zoho_oauth_tokens')
      .select('id, access_token, refresh_token, expires_at, region, created_at, updated_at')
      .eq('user_id', userId)
      .maybeSingle();

    if (tokenError && tokenError.code !== 'PGRST116') {
      logger.error('[ZohoStatus] Error querying zoho_oauth_tokens:', tokenError.message);
      return res.status(500).json({
        success: false,
        error: 'Failed to load Zoho connection status',
      });
    }

    const now = new Date();
    const expiresAt = tokenData?.expires_at ? new Date(tokenData.expires_at) : null;
    const fiveMinutesFromNow = new Date(now.getTime() + 5 * 60 * 1000);

    const connected = !!tokenData && !!tokenData.access_token;
    const expiresSoon = !!expiresAt && expiresAt <= fiveMinutesFromNow;

    return res.json({
      success: true,
      connected,
      metadata: {
        tokenId: tokenData?.id,
        region: tokenData?.region || 'US',
        createdAt: tokenData?.created_at,
        updatedAt: tokenData?.updated_at,
        expiresAt: expiresAt?.toISOString() || null,
        expiresSoon,
        hasRefreshToken: !!tokenData?.refresh_token,
      },
    });
  } catch (error) {
    logger.error('[ZohoStatus] Unexpected error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
}

export async function zohoConnectHandler(req: Request, res: Response) {
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

    const { clientId, clientSecret, accessToken, refreshToken, region } = req.body;

    // Validate required fields
    if (!clientId || !clientSecret || !accessToken) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: clientId, clientSecret, and accessToken are required',
      });
    }

    // Validate region
    const validRegions: ZohoRegion[] = ['US', 'EU', 'IN', 'AU', 'CN', 'JP'];
    const zohoRegion: ZohoRegion = validRegions.includes(region) ? region : 'US';

    // Test the credentials by making a simple API call
    try {
      const baseUrl = getZohoApiBaseUrl(zohoRegion);
      const testResponse = await fetch(`${baseUrl}/crm/v3/org`, {
        headers: {
          'Authorization': `Zoho-oauthtoken ${accessToken}`,
          'Content-Type': 'application/json',
        },
      });

      if (!testResponse.ok && testResponse.status !== 404) {
        // 404 is okay for org endpoint if CRM isn't set up, but other errors indicate invalid token
        const errorText = await testResponse.text();
        logger.warn('[ZohoConnect] Token test failed:', testResponse.status, errorText.slice(0, 200));
        // Still allow saving - token might be valid for other services
      }
    } catch (testError) {
      logger.warn('[ZohoConnect] Token test error (non-fatal):', testError);
      // Continue anyway - network issues shouldn't block credential saving
    }

    // Calculate expiry (Zoho tokens typically expire in 1 hour, but we'll set a conservative expiry)
    const expiresAt = new Date(Date.now() + 3600 * 1000); // 1 hour from now

    // Upsert Zoho OAuth tokens
    const { data: tokenData, error: upsertError } = await db
      .from('zoho_oauth_tokens')
      .upsert({
        user_id: userId,
        access_token: accessToken,
        refresh_token: refreshToken || null,
        expires_at: expiresAt.toISOString(),
        region: zohoRegion,
        updated_at: new Date().toISOString(),
      }, {
        onConflict: 'user_id,region',
      })
      .select()
      .single();

    if (upsertError) {
      logger.error('[ZohoConnect] Error upserting zoho_oauth_tokens:', upsertError.message);
      return res.status(500).json({
        success: false,
        error: 'Failed to save Zoho credentials',
      });
    }

    // Mirror into user_credentials vault for connector-based discovery
    const { error: vaultError } = await db
      .from('user_credentials')
      .upsert({
        user_id: userId,
        service: 'zoho',
        credentials: {
          accessToken: accessToken,
          refreshToken: refreshToken || null,
          clientId: clientId,
          clientSecret: clientSecret,
          region: zohoRegion,
          expiresAt: expiresAt.toISOString(),
        },
      }, {
        onConflict: 'user_id,service',
      });

    if (vaultError) {
      logger.warn('[ZohoConnect] Error upserting user_credentials (non-fatal):', vaultError.message);
      // Non-fatal - tokens are already saved
    }

    return res.json({
      success: true,
      message: 'Zoho credentials saved successfully',
      metadata: {
        tokenId: tokenData.id,
        region: zohoRegion,
      },
    });
  } catch (error) {
    logger.error('[ZohoConnect] Unexpected error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
}

export async function zohoTestHandler(req: Request, res: Response) {
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

    const { getZohoAccessToken } = await import('../shared/zoho-oauth');
    const { data: tokenData } = await db
      .from('zoho_oauth_tokens')
      .select('access_token, region')
      .eq('user_id', userId)
      .maybeSingle();

    if (!tokenData || !tokenData.access_token) {
      return res.status(401).json({
        success: false,
        error: 'Zoho credentials not found. Please connect your Zoho account.',
      });
    }

    const region = (tokenData.region as ZohoRegion) || 'US';
    const baseUrl = getZohoApiBaseUrl(region);

    // Test with CRM org endpoint (most common service)
    const testEndpoints = [
      { name: 'CRM Org', url: `${baseUrl}/crm/v3/org` },
      { name: 'Books Organizations', url: `${baseUrl}/books/v3/organizations` },
    ];

    let status = 0;
    let testResult: any = null;
    let lastErrText = '';

    for (const ep of testEndpoints) {
      const response = await fetch(ep.url, {
        headers: {
          'Authorization': `Zoho-oauthtoken ${tokenData.access_token}`,
          'Content-Type': 'application/json',
        },
      });

      status = response.status;
      if (response.ok) {
        try {
          testResult = await response.json();
          break;
        } catch {
          testResult = { success: true, endpoint: ep.name };
        }
        break;
      } else {
        lastErrText = await response.text();
        logger.warn(`[ZohoTest] ${ep.name} error:`, status, lastErrText.slice(0, 200));
      }
    }

    if (!testResult && status !== 404) {
      return res.status(status || 500).json({
        success: false,
        error: 'Zoho API test failed',
        details: {
          status: status || 500,
          message: lastErrText.slice(0, 200),
        },
      });
    }

    return res.json({
      success: true,
      status: 'ok',
      region: region,
      testResult: testResult || { message: 'Connection verified (endpoint returned 404 - service may not be configured)' },
    });
  } catch (error) {
    logger.error('[ZohoTest] Unexpected error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
}

export async function zohoDisconnectHandler(req: Request, res: Response) {
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

    // Delete Zoho OAuth tokens
    const { error: tokenError } = await db
      .from('zoho_oauth_tokens')
      .delete()
      .eq('user_id', userId);

    if (tokenError) {
      logger.error('[ZohoDisconnect] Error deleting zoho_oauth_tokens:', tokenError.message);
      return res.status(500).json({
        success: false,
        error: 'Failed to delete Zoho tokens',
      });
    }

    // Delete vault credential entry for Zoho
    const { error: vaultError } = await db
      .from('user_credentials')
      .delete()
      .eq('user_id', userId)
      .eq('service', 'zoho');

    if (vaultError && vaultError.code !== 'PGRST116') {
      logger.error('[ZohoDisconnect] Error deleting user_credentials (zoho):', vaultError.message);
      // Non-fatal; tokens are already removed
    }

    return res.json({
      success: true,
      message: 'Zoho account disconnected successfully',
    });
  } catch (error) {
    logger.error('[ZohoDisconnect] Unexpected error:', error);
    return res.status(500).json({
      success: false,
      error: 'Internal server error',
    });
  }
}
