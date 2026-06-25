/**
 * Facebook OAuth Connect Flow (2nd Meta app — for workflow automation)
 *
 * This is for CONNECTING a Facebook account to use in workflows (posting, reading pages).
 * It is NOT the same as the Cognito Facebook login app.
 *
 *   GET /api/oauth/facebook/start      — redirect authenticated user → Facebook
 *   GET /api/oauth/facebook/callback   — exchange code, store token, redirect frontend
 */

import { Request, Response } from 'express';
import { queryAsService } from '../core/database/db-pool';
import { config } from '../core/config';
import { encryptToken } from '../core/utils/token-encryption';
import crypto from 'crypto';
import { logger } from '../core/logger';

const META_APP_ID     = process.env.META_APP_ID     || process.env.FACEBOOK_APP_ID     || '';
const META_APP_SECRET = process.env.META_APP_SECRET || process.env.FACEBOOK_APP_SECRET || '';
const FRONTEND_URL    = process.env.FRONTEND_URL    || 'http://localhost:8080';
const META_CONFIG_ID  = process.env.META_FACEBOOK_CONFIG_ID || process.env.FACEBOOK_CONFIG_ID || '';
const META_EXTRA_SCOPES = (process.env.META_FACEBOOK_EXTRA_SCOPES || process.env.FACEBOOK_EXTRA_SCOPES || '')
  .split(',')
  .map((scope) => scope.trim())
  .filter(Boolean);

function callbackUrl() {
  if (process.env.FACEBOOK_OAUTH_REDIRECT_URI) {
    return process.env.FACEBOOK_OAUTH_REDIRECT_URI;
  }
  return `${config.publicBaseUrl}/api/oauth/facebook/callback`;
}

function signState(payload: object): string {
  const data = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const sig  = crypto.createHmac('sha256', config.encryptionKey).update(data).digest('hex').slice(0, 16);
  return `${data}.${sig}`;
}

function verifyState(state: string): any | null {
  const [data, sig] = state.split('.');
  if (!data || !sig) return null;
  const expected = crypto.createHmac('sha256', config.encryptionKey).update(data).digest('hex').slice(0, 16);
  if (sig !== expected) return null;
  try { return JSON.parse(Buffer.from(data, 'base64url').toString()); } catch { return null; }
}

/**
 * GET /api/oauth/facebook/start?user_id=<cognito_sub>&redirect_to=<path>
 */
export function facebookOAuthStart(req: Request, res: Response) {
  const userId    = req.query.user_id    as string;
  const redirectTo = req.query.redirect_to as string || '/workflows';

  if (!userId) return res.status(400).json({ error: 'user_id required' });
  if (!META_APP_ID) return res.status(500).json({ error: 'META_APP_ID not configured' });
  if (!/^\d+$/.test(META_APP_ID)) {
    return res.status(500).json({ error: 'META_APP_ID must be the numeric Facebook App ID from Meta Developer Console' });
  }

  const state   = signState({ userId, redirectTo, ts: Date.now() });
  const authUrl = new URL('https://www.facebook.com/v19.0/dialog/oauth');
  authUrl.searchParams.set('client_id',    META_APP_ID);
  authUrl.searchParams.set('redirect_uri', callbackUrl());
  // Scopes needed for workflow automation: manage pages, post content, read page insights
  const scopes = Array.from(new Set([
    'pages_show_list',
    'pages_read_engagement',
    'pages_manage_posts',
    'public_profile',
    'email',
    ...META_EXTRA_SCOPES,
  ]));
  authUrl.searchParams.set('scope', scopes.join(','));
  if (META_CONFIG_ID) authUrl.searchParams.set('config_id', META_CONFIG_ID);
  authUrl.searchParams.set('state', state);
  authUrl.searchParams.set('response_type', 'code');

  return res.redirect(authUrl.toString());
}

/**
 * GET /api/oauth/facebook/callback?code=...&state=...
 */
export async function facebookOAuthCallback(req: Request, res: Response) {
  const { code, state, error: fbError } = req.query as Record<string, string>;

  if (fbError) {
    return res.redirect(`${FRONTEND_URL}/auth/facebook/callback?error=${encodeURIComponent(fbError)}`);
  }

  const stateData = verifyState(state || '');
  if (!stateData) {
    return res.redirect(`${FRONTEND_URL}/auth/facebook/callback?error=invalid_state`);
  }

  const { userId, redirectTo } = stateData;

  try {
    // 1. Exchange code for access token
    const tokenRes = await fetch(
      `https://graph.facebook.com/v19.0/oauth/access_token?` +
      `client_id=${META_APP_ID}&client_secret=${META_APP_SECRET}` +
      `&redirect_uri=${encodeURIComponent(callbackUrl())}&code=${code}`
    );
    const tokenData: any = await tokenRes.json();

    if (!tokenData.access_token) {
      throw new Error(tokenData.error?.message || 'No access_token from Facebook');
    }

    // 2. Get long-lived token (60-day expiry instead of 1-hour)
    const longLivedRes = await fetch(
      `https://graph.facebook.com/v19.0/oauth/access_token?` +
      `grant_type=fb_exchange_token&client_id=${META_APP_ID}&client_secret=${META_APP_SECRET}` +
      `&fb_exchange_token=${tokenData.access_token}`
    );
    const longLivedData: any = await longLivedRes.json();
    const accessToken  = longLivedData.access_token || tokenData.access_token;
    const expiresIn    = longLivedData.expires_in   || tokenData.expires_in || 3600;

    // 3. Get Facebook user profile
    const profileRes  = await fetch(
      `https://graph.facebook.com/v19.0/me?fields=id,name,email&access_token=${accessToken}`
    );
    const profile: any = await profileRes.json();

    // 4. Store in social_tokens
    const expiresAt = new Date(Date.now() + expiresIn * 1000).toISOString();
    await queryAsService(
      `INSERT INTO social_tokens (user_id, provider, access_token, provider_user_id, scope, expires_at, updated_at)
       VALUES ($1, 'facebook', $2, $3, $4, $5, NOW())
       ON CONFLICT (user_id, provider)
       DO UPDATE SET access_token       = EXCLUDED.access_token,
                     provider_user_id   = EXCLUDED.provider_user_id,
                     scope              = EXCLUDED.scope,
                     expires_at         = EXCLUDED.expires_at,
                     updated_at         = NOW()`,
      [userId, encryptToken(accessToken), String(profile.id),
       'pages_show_list,pages_read_engagement,pages_manage_posts', expiresAt]
    );

    logger.info(`[FacebookOAuth] ✅ Connected Facebook for user ${userId} (fb_id: ${profile.id})`);

    const returnUrl = encodeURIComponent(redirectTo || '/workflows');
    return res.redirect(
      `${FRONTEND_URL}/auth/facebook/callback?success=true&name=${encodeURIComponent(profile.name || '')}&return_to=${returnUrl}`
    );

  } catch (err: any) {
    logger.error('[FacebookOAuth] Error:', err.message);
    return res.redirect(
      `${FRONTEND_URL}/auth/facebook/callback?error=${encodeURIComponent(err.message || 'oauth_failed')}`
    );
  }
}
