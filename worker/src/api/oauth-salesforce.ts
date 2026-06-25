import { Request, Response } from 'express';
import { queryAsService } from '../core/database/db-pool';
import { encryptToken } from '../core/utils/token-encryption';
import { logger } from '../core/logger';

const SALESFORCE_CLIENT_ID = process.env.SALESFORCE_CLIENT_ID || '';
const SALESFORCE_CLIENT_SECRET = process.env.SALESFORCE_CLIENT_SECRET || '';
const SALESFORCE_LOGIN_URL = process.env.SALESFORCE_LOGIN_URL || 'https://login.salesforce.com';
const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:8080';

function getRedirectUri(req: Request): string {
  const envRedirect = process.env.SALESFORCE_OAUTH_REDIRECT_URI;
  const raw = (req.query.redirect_uri as string) || (req.body?.redirect_uri as string) || '';
  try {
    const url = new URL(raw);
    const isLocal =
      url.origin === 'http://localhost:8080' ||
      url.origin === 'http://127.0.0.1:8080' ||
      url.origin === FRONTEND_URL;
    if (isLocal && url.pathname === '/auth/salesforce/callback') return url.toString();
  } catch {
    // fall through
  }
  return envRedirect || `${FRONTEND_URL}/auth/salesforce/callback`;
}

export function salesforceAuthorizeHandler(req: Request, res: Response) {
  if (!SALESFORCE_CLIENT_ID) {
    return res.status(500).json({ error: 'SALESFORCE_CLIENT_ID not configured' });
  }

  const authUrl = new URL(`${SALESFORCE_LOGIN_URL}/services/oauth2/authorize`);
  authUrl.searchParams.set('response_type', 'code');
  authUrl.searchParams.set('client_id', SALESFORCE_CLIENT_ID);
  authUrl.searchParams.set('redirect_uri', getRedirectUri(req));
  authUrl.searchParams.set('scope', 'api refresh_token');
  authUrl.searchParams.set('prompt', 'consent');

  return res.redirect(authUrl.toString());
}

export async function salesforceCallbackHandler(req: Request, res: Response) {
  try {
    const userId = (req as any).user?.id as string | undefined;
    if (!userId) return res.status(401).json({ success: false, error: 'Unauthorized' });
    if (!SALESFORCE_CLIENT_ID || !SALESFORCE_CLIENT_SECRET) {
      return res.status(500).json({ success: false, error: 'Salesforce OAuth credentials are not configured' });
    }

    const { code } = req.body as { code?: string };
    if (!code) return res.status(400).json({ success: false, error: 'code required' });

    const tokenRes = await fetch(`${SALESFORCE_LOGIN_URL}/services/oauth2/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'authorization_code',
        code,
        client_id: SALESFORCE_CLIENT_ID,
        client_secret: SALESFORCE_CLIENT_SECRET,
        redirect_uri: getRedirectUri(req),
      }),
    });
    const tokenData: any = await tokenRes.json();
    if (!tokenRes.ok || !tokenData.access_token || !tokenData.instance_url) {
      throw new Error(tokenData.error_description || tokenData.error || 'No Salesforce access token');
    }

    const issuedAt = tokenData.issued_at
      ? new Date(Number(tokenData.issued_at)).toISOString()
      : new Date().toISOString();
    const expiresAt = new Date(Date.now() + Number(tokenData.expires_in || 7200) * 1000).toISOString();

    await queryAsService(
      `INSERT INTO salesforce_oauth_tokens (user_id, access_token, refresh_token, instance_url, issued_at, expires_at, scope, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
       ON CONFLICT (user_id)
       DO UPDATE SET access_token = EXCLUDED.access_token,
                     refresh_token = COALESCE(EXCLUDED.refresh_token, salesforce_oauth_tokens.refresh_token),
                     instance_url = EXCLUDED.instance_url,
                     issued_at = EXCLUDED.issued_at,
                     expires_at = EXCLUDED.expires_at,
                     scope = EXCLUDED.scope,
                     updated_at = NOW()`,
      [
        userId,
        encryptToken(tokenData.access_token),
        tokenData.refresh_token ? encryptToken(tokenData.refresh_token) : null,
        tokenData.instance_url,
        issuedAt,
        expiresAt,
        tokenData.scope || 'api refresh_token',
      ]
    );

    await queryAsService(
      `INSERT INTO user_credentials (user_id, service, credentials, updated_at)
       VALUES ($1, 'salesforce', $2::jsonb, NOW())
       ON CONFLICT (user_id, service)
       DO UPDATE SET credentials = EXCLUDED.credentials, updated_at = NOW()`,
      [
        userId,
        JSON.stringify({
          connected: true,
          instanceUrl: tokenData.instance_url,
          issuedAt,
          expiresAt,
          scope: tokenData.scope || 'api refresh_token',
        }),
      ]
    ).catch((err) => {
      logger.warn('[SalesforceOAuth] user_credentials mirror failed:', err.message);
    });

    return res.json({ success: true, instanceUrl: tokenData.instance_url, expiresAt });
  } catch (err: any) {
    logger.error('[SalesforceOAuth] Error:', err.message);
    return res.status(500).json({ success: false, error: err.message || 'Salesforce OAuth failed' });
  }
}
