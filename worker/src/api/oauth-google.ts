import { Request, Response } from 'express';
import crypto from 'crypto';
import { queryAsService } from '../core/database/db-pool';
import { config } from '../core/config';
import { ensureUserRows } from '../core/database/ensure-user';
import { resolveUserIdByEmail } from '../shared/credential-resolver';
import { handleOAuthCallback } from '../services/oauth-callback-handler';
import { logger } from '../core/logger';

function googleClientId() {
  return process.env.GOOGLE_OAUTH_CLIENT_ID || config.googleOAuthClientId || '';
}

function googleClientSecret() {
  return process.env.GOOGLE_OAUTH_CLIENT_SECRET || config.googleOAuthClientSecret || '';
}

function frontendUrl() {
  return process.env.FRONTEND_URL || 'http://localhost:8080';
}

function configuredGoogleRedirectUri(): string | null {
  const value = process.env.GOOGLE_OAUTH_REDIRECT_URI?.trim();
  return value || null;
}

const GOOGLE_SCOPES = [
  'openid',
  'email',
  'profile',
  'https://www.googleapis.com/auth/spreadsheets',
  'https://www.googleapis.com/auth/documents',
  'https://www.googleapis.com/auth/drive',
  'https://www.googleapis.com/auth/calendar.events',
  'https://www.googleapis.com/auth/gmail.send',
  'https://www.googleapis.com/auth/gmail.readonly',
  'https://www.googleapis.com/auth/contacts',
  'https://www.googleapis.com/auth/tasks',
  'https://www.googleapis.com/auth/bigquery',
].join(' ');

function requestBaseUrl(req: Request): string | null {
  const host = req.get('host');
  return host ? `${req.protocol || 'http'}://${host}` : null;
}

function callbackUrl(req?: Request) {
  const googleRedirectUri = configuredGoogleRedirectUri();
  if (googleRedirectUri) return googleRedirectUri;

  const configuredBaseUrl = process.env.PUBLIC_BASE_URL || config.publicBaseUrl || 'http://localhost:3001';
  const devRequestBaseUrl = req && process.env.NODE_ENV !== 'production' ? requestBaseUrl(req) : null;
  return `${devRequestBaseUrl || configuredBaseUrl}/api/oauth/google/callback`;
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

export function googleOAuthStart(req: Request, res: Response) {
  const userId = req.query.user_id as string;
  const redirectTo = safeReturnTo(req.query.redirect_to);
  const clientId = googleClientId();

  if (!userId) return res.status(400).json({ error: 'user_id required' });
  if (!clientId) return res.status(500).json({ error: 'GOOGLE_OAUTH_CLIENT_ID not configured' });

  const authUrl = new URL('https://accounts.google.com/o/oauth2/v2/auth');
  authUrl.searchParams.set('client_id', clientId);
  authUrl.searchParams.set('redirect_uri', callbackUrl(req));
  authUrl.searchParams.set('response_type', 'code');
  authUrl.searchParams.set('scope', GOOGLE_SCOPES);
  authUrl.searchParams.set('access_type', 'offline');
  authUrl.searchParams.set('prompt', 'consent');
  authUrl.searchParams.set('include_granted_scopes', 'true');
  authUrl.searchParams.set('state', signState({ userId, redirectTo, ts: Date.now() }));

  return res.redirect(authUrl.toString());
}

export async function googleOAuthCallback(req: Request, res: Response) {
  const { code, state, error: oauthError } = req.query as Record<string, string>;

  if (oauthError) {
    return res.redirect(`${frontendUrl()}/auth/google/callback?error=${encodeURIComponent(oauthError)}`);
  }

  const stateData = verifyState(state || '');
  if (!stateData) {
    return res.redirect(`${frontendUrl()}/auth/google/callback?error=invalid_state`);
  }

  const { userId, redirectTo } = stateData;

  try {
    const clientId = googleClientId();
    const clientSecret = googleClientSecret();
    if (!clientId || !clientSecret) {
      throw new Error('Google OAuth credentials are not configured');
    }

    const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: clientId,
        client_secret: clientSecret,
        code,
        grant_type: 'authorization_code',
        redirect_uri: callbackUrl(req),
      }),
    });
    const tokenData: any = await tokenRes.json();
    if (!tokenRes.ok || !tokenData.access_token) {
      throw new Error(tokenData.error_description || tokenData.error || 'No access token from Google');
    }

    const profileRes = await fetch('https://openidconnect.googleapis.com/v1/userinfo', {
      headers: { Authorization: `Bearer ${tokenData.access_token}` },
    });
    const profile: any = profileRes.ok ? await profileRes.json() : {};
    await ensureUserRows(userId, profile.email || null, profile.name || null);

    await handleOAuthCallback({
      provider: 'google',
      userId,
      email: profile.email || undefined,
      tokenResponse: {
        ...tokenData,
        scope: tokenData.scope || GOOGLE_SCOPES,
      },
      source: 'legacy_google',
    });

    const returnUrl = encodeURIComponent(safeReturnTo(redirectTo));
    return res.redirect(
      `${frontendUrl()}/auth/google/callback?success=true&email=${encodeURIComponent(profile.email || '')}&return_to=${returnUrl}`
    );
  } catch (err: any) {
    logger.error('[GoogleOAuth] Error:', err.message);
    return res.redirect(`${frontendUrl()}/auth/google/callback?error=${encodeURIComponent(err.message || 'oauth_failed')}`);
  }
}

/**
 * DELETE /api/connections/google
 *
 * Removes Google OAuth tokens for the authenticated user across ALL Cognito
 * sub IDs that share the same email address (handles identity fragmentation
 * where the user has both an email/password sub and a Google OAuth sub).
 */
export async function googleDisconnectHandler(req: Request, res: Response) {
  try {
    const currentUserId: string = (req as any).user?.id;
    if (!currentUserId) return res.status(401).json({ success: false, error: 'Unauthenticated' });

    // Gather all sub IDs that share this user's email so we delete tokens under any of them
    const emailRow = await queryAsService<{ email: string }>(
      `SELECT email FROM users WHERE id = $1 LIMIT 1`,
      [currentUserId],
    ).catch(() => [] as { email: string }[]);

    const email = emailRow[0]?.email || '';
    const allUserIds = new Set<string>([currentUserId]);

    if (email) {
      const peers = await queryAsService<{ id: string }>(
        `SELECT id FROM users WHERE LOWER(email) = LOWER($1)`,
        [email],
      ).catch(() => [] as { id: string }[]);
      for (const r of peers) allUserIds.add(r.id);
    }

    const ids = Array.from(allUserIds);

    // Delete google_oauth_tokens for all matching subs
    await queryAsService(
      `DELETE FROM google_oauth_tokens WHERE user_id = ANY($1)`,
      [ids],
    );

    // Also clean up legacy user_credentials entries for google
    await queryAsService(
      `DELETE FROM user_credentials WHERE user_id = ANY($1) AND service = 'google'`,
      [ids],
    ).catch(() => { /* non-fatal */ });

    await queryAsService(
      `DELETE FROM social_tokens WHERE user_id = ANY($1) AND provider = 'google'`,
      [ids],
    ).catch(() => { /* non-fatal */ });

    await queryAsService(
      `DELETE FROM credential_vault WHERE user_id = ANY($1) AND key = 'google'`,
      [ids],
    ).catch(() => { /* non-fatal */ });

    logger.info(`[GoogleDisconnect] Removed Google tokens for user ${currentUserId} (checked ${ids.length} sub(s))`);
    return res.json({ success: true, message: 'Google account disconnected successfully' });
  } catch (err: any) {
    logger.error('[GoogleDisconnect] Error:', err.message);
    return res.status(500).json({ success: false, error: 'Failed to disconnect Google account' });
  }
}
