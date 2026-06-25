import { Request, Response } from 'express';
import crypto from 'crypto';
import { queryAsService } from '../core/database/db-pool';
import { config } from '../core/config';
import { encryptToken } from '../core/utils/token-encryption';
import { logger } from '../core/logger';

const LINKEDIN_CLIENT_ID = process.env.LINKEDIN_CLIENT_ID || '';
const LINKEDIN_CLIENT_SECRET = process.env.LINKEDIN_CLIENT_SECRET || '';
const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:8080';
const LINKEDIN_SCOPES = 'openid profile email w_member_social';

function callbackUrl() {
  const configuredRedirectUri = process.env.LINKEDIN_OAUTH_REDIRECT_URI?.trim();
  if (configuredRedirectUri) return configuredRedirectUri;

  return `${config.publicBaseUrl}/api/oauth/linkedin/callback`;
}

function safeReturnTo(value: unknown): string {
  const path = typeof value === 'string' ? value : '/workflows';
  return path.startsWith('/') && !path.startsWith('//') ? path : '/workflows';
}

function signState(payload: object): string {
  const data = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig = crypto.createHmac('sha256', config.encryptionKey).update(data).digest('hex').slice(0, 16);
  return `${data}.${sig}`;
}

function verifyState(state: string): any | null {
  const [data, sig] = state.split('.');
  if (!data || !sig) return null;
  const expected = crypto.createHmac('sha256', config.encryptionKey).update(data).digest('hex').slice(0, 16);
  if (sig !== expected) return null;
  try {
    const parsed = JSON.parse(Buffer.from(data, 'base64url').toString());
    if (Date.now() - Number(parsed.ts || 0) > 10 * 60_000) return null;
    return parsed;
  } catch {
    return null;
  }
}

export function linkedInOAuthStart(req: Request, res: Response) {
  const userId = req.query.user_id as string;
  const redirectTo = safeReturnTo(req.query.redirect_to);

  if (!userId) return res.status(400).json({ error: 'user_id required' });
  if (!LINKEDIN_CLIENT_ID) return res.status(500).json({ error: 'LINKEDIN_CLIENT_ID not configured' });

  const authUrl = new URL('https://www.linkedin.com/oauth/v2/authorization');
  authUrl.searchParams.set('response_type', 'code');
  authUrl.searchParams.set('client_id', LINKEDIN_CLIENT_ID);
  authUrl.searchParams.set('redirect_uri', callbackUrl());
  authUrl.searchParams.set('scope', LINKEDIN_SCOPES);
  authUrl.searchParams.set('state', signState({ userId, redirectTo, ts: Date.now() }));

  return res.redirect(authUrl.toString());
}

export async function linkedInOAuthCallback(req: Request, res: Response) {
  const { code, state, error: oauthError } = req.query as Record<string, string>;

  if (oauthError) {
    return res.redirect(`${FRONTEND_URL}/auth/linkedin/callback?error=${encodeURIComponent(oauthError)}`);
  }

  const stateData = verifyState(state || '');
  if (!stateData) {
    return res.redirect(`${FRONTEND_URL}/auth/linkedin/callback?error=invalid_state`);
  }

  const { userId, redirectTo } = stateData;

  try {
    if (!LINKEDIN_CLIENT_ID || !LINKEDIN_CLIENT_SECRET) {
      throw new Error('LinkedIn OAuth credentials are not configured');
    }

    const tokenRes = await fetch('https://www.linkedin.com/oauth/v2/accessToken', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'authorization_code',
        code,
        redirect_uri: callbackUrl(),
        client_id: LINKEDIN_CLIENT_ID,
        client_secret: LINKEDIN_CLIENT_SECRET,
      }),
    });
    const tokenData: any = await tokenRes.json();
    if (!tokenRes.ok || !tokenData.access_token) {
      throw new Error(tokenData.error_description || tokenData.error || 'No access token from LinkedIn');
    }

    const profileRes = await fetch('https://api.linkedin.com/v2/userinfo', {
      headers: { Authorization: `Bearer ${tokenData.access_token}` },
    });
    const profile: any = profileRes.ok ? await profileRes.json() : {};
    const expiresAt = tokenData.expires_in
      ? new Date(Date.now() + Number(tokenData.expires_in) * 1000).toISOString()
      : null;
    const encryptedAccessToken = encryptToken(tokenData.access_token);
    const encryptedRefreshToken = tokenData.refresh_token ? encryptToken(tokenData.refresh_token) : null;

    await queryAsService(
      `INSERT INTO linkedin_oauth_tokens (user_id, access_token, refresh_token, token_type, expires_at, scope, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW())
       ON CONFLICT (user_id)
       DO UPDATE SET access_token = EXCLUDED.access_token,
                     refresh_token = COALESCE(EXCLUDED.refresh_token, linkedin_oauth_tokens.refresh_token),
                     token_type = EXCLUDED.token_type,
                     expires_at = EXCLUDED.expires_at,
                     scope = EXCLUDED.scope,
                     updated_at = NOW()`,
      [
        userId,
        encryptedAccessToken,
        encryptedRefreshToken,
        tokenData.token_type || 'Bearer',
        expiresAt,
        tokenData.scope || LINKEDIN_SCOPES,
      ]
    );

    await queryAsService(
      `INSERT INTO social_tokens (user_id, provider, access_token, refresh_token, provider_user_id, token_type, expires_at, scope, updated_at)
       VALUES ($1, 'linkedin', $2, $3, $4, $5, $6, $7, NOW())
       ON CONFLICT (user_id, provider)
       DO UPDATE SET access_token = EXCLUDED.access_token,
                     refresh_token = COALESCE(EXCLUDED.refresh_token, social_tokens.refresh_token),
                     provider_user_id = EXCLUDED.provider_user_id,
                     token_type = EXCLUDED.token_type,
                     expires_at = EXCLUDED.expires_at,
                     scope = EXCLUDED.scope,
                     updated_at = NOW()`,
      [
        userId,
        encryptedAccessToken,
        encryptedRefreshToken,
        profile.sub || profile.id || null,
        tokenData.token_type || 'Bearer',
        expiresAt,
        tokenData.scope || LINKEDIN_SCOPES,
      ]
    ).catch((err) => {
      logger.warn('[LinkedInOAuth] social_tokens mirror failed:', err.message);
    });

    await queryAsService(
      `INSERT INTO user_credentials (user_id, service, credentials, updated_at)
       VALUES ($1, 'linkedin', $2::jsonb, NOW())
       ON CONFLICT (user_id, service)
       DO UPDATE SET credentials = EXCLUDED.credentials, updated_at = NOW()`,
      [
        userId,
        JSON.stringify({
          connected: true,
          name: profile.name || null,
          email: profile.email || null,
          expiresAt,
          scope: tokenData.scope || LINKEDIN_SCOPES,
        }),
      ]
    ).catch((err) => {
      logger.warn('[LinkedInOAuth] user_credentials mirror failed:', err.message);
    });

    const displayName = profile.name || profile.email || '';
    const returnUrl = encodeURIComponent(safeReturnTo(redirectTo));
    return res.redirect(
      `${FRONTEND_URL}/auth/linkedin/callback?success=true&name=${encodeURIComponent(displayName)}&return_to=${returnUrl}`
    );
  } catch (err: any) {
    logger.error('[LinkedInOAuth] Error:', err.message);
    return res.redirect(`${FRONTEND_URL}/auth/linkedin/callback?error=${encodeURIComponent(err.message || 'oauth_failed')}`);
  }
}
