/**
 * Twitter OAuth API Handlers
 * 
 * Handles Twitter OAuth 2.0 flow with PKCE:
 * 1. /api/oauth/twitter/authorize - Initiates OAuth flow
 * 2. /api/oauth/twitter/callback - Handles OAuth callback and token exchange
 */

import { Request, Response } from 'express';
import { handleOAuthCallback } from '../services/oauth-callback-handler';
import { getDbClient } from '../core/database/aws-db-client';
import crypto from 'crypto';
import { logger } from '../core/logger';

/**
 * Type definitions for Twitter OAuth responses
 */
interface TwitterTokenResponse {
  access_token: string;
  refresh_token?: string;
  token_type?: string;
  expires_in?: number;
  scope?: string;
}

interface TwitterUserResponse {
  data: {
    id: string;
    name: string;
    username: string;
  };
}

/**
 * Generate PKCE code verifier and challenge
 */
function generatePKCE(): { codeVerifier: string; codeChallenge: string } {
  const codeVerifier = crypto.randomBytes(32).toString('base64url');
  const codeChallenge = crypto
    .createHash('sha256')
    .update(codeVerifier)
    .digest('base64url');
  
  return { codeVerifier, codeChallenge };
}

/**
 * Initiate Twitter OAuth flow
 * GET /api/oauth/twitter/authorize?redirect_uri=...
 */
export async function twitterAuthorizeHandler(req: Request, res: Response) {
  try {
    const redirectUri = (req.query.redirect_uri as string) || process.env.TWITTER_OAUTH_REDIRECT_URI;
    
    if (!redirectUri) {
      return res.status(400).json({
        success: false,
        error: 'redirect_uri parameter is required',
      });
    }

    // Get Twitter OAuth credentials from environment
    const clientId = process.env.TWITTER_OAUTH_CLIENT_ID;

    if (!clientId) {
      return res.status(500).json({
        success: false,
        error: 'Twitter OAuth client ID not configured. Please set TWITTER_OAUTH_CLIENT_ID environment variable.',
      });
    }

    const placeholderIds = /^your[_-]?client[_-]?id$/i;
    if (placeholderIds.test(clientId.trim()) || clientId.includes('your_client')) {
      return res.status(500).json({
        success: false,
        error:
          'TWITTER_OAUTH_CLIENT_ID is still a placeholder. Set it to your real OAuth 2.0 Client ID from developer.x.com (Keys and tokens / User authentication settings), then restart the worker.',
      });
    }

    // Generate PKCE parameters
    const { codeVerifier, codeChallenge } = generatePKCE();
    
    // Generate state for CSRF protection
    const state = Buffer.from(`${Date.now()}-${Math.random()}`).toString('base64url');

    // Store code_verifier and state in session or return to client
    // For simplicity, we'll encode them in the state parameter
    // In production, use a proper session store
    const stateData = Buffer.from(JSON.stringify({ codeVerifier, state: state })).toString('base64url');

    // Scopes: tweet.write needs Elevated (or higher) access on the X app — if OAuth shows "Something went wrong"
    // after login, use a smaller set via TWITTER_OAUTH_SCOPES (see worker env.example).
    const scope =
      process.env.TWITTER_OAUTH_SCOPES?.trim() ||
      'users.read offline.access';

    // Twitter OAuth 2.0 authorization URL
    const authUrl = new URL('https://twitter.com/i/oauth2/authorize');
    authUrl.searchParams.set('response_type', 'code');
    authUrl.searchParams.set('client_id', clientId);
    authUrl.searchParams.set('redirect_uri', redirectUri);
    authUrl.searchParams.set('scope', scope);
    authUrl.searchParams.set('state', stateData);
    authUrl.searchParams.set('code_challenge', codeChallenge);
    authUrl.searchParams.set('code_challenge_method', 'S256');

    res.redirect(authUrl.toString());
  } catch (error) {
    logger.error('Twitter OAuth authorize error:', error);
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Failed to initiate Twitter OAuth',
    });
  }
}

/**
 * Handle Twitter OAuth callback and exchange code for token
 * POST /api/oauth/twitter/callback
 */
export async function twitterCallbackHandler(req: Request, res: Response) {
  try {
    const db = getDbClient();
    
    // Get user from session
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        success: false,
        error: 'Authentication required',
      });
    }

    const token = authHeader.substring(7);
    const { data: { user }, error: authError } = await db.auth.getUser(token);
    
    if (authError || !user) {
      return res.status(401).json({
        success: false,
        error: 'Invalid or expired token',
      });
    }

    const { code, state } = req.body;
    const redirect_uri = req.body.redirect_uri || process.env.TWITTER_OAUTH_REDIRECT_URI;

    if (!code) {
      return res.status(400).json({
        success: false,
        error: 'Authorization code is required',
      });
    }

    if (!redirect_uri) {
      return res.status(400).json({
        success: false,
        error: 'Redirect URI is required',
      });
    }

    // Decode state to get code_verifier
    let codeVerifier: string;
    try {
      const stateData = JSON.parse(Buffer.from(state, 'base64url').toString());
      codeVerifier = stateData.codeVerifier;
    } catch {
      return res.status(400).json({
        success: false,
        error: 'Invalid state parameter',
      });
    }

    // Get Twitter OAuth credentials
    const clientId = process.env.TWITTER_OAUTH_CLIENT_ID;
    const clientSecret = process.env.TWITTER_OAUTH_CLIENT_SECRET;

    if (!clientId || !clientSecret) {
      return res.status(500).json({
        success: false,
        error: 'Twitter OAuth credentials not configured',
      });
    }

    if (/your[_-]?client/i.test(clientId) || /your[_-]?client/i.test(clientSecret)) {
      return res.status(500).json({
        success: false,
        error:
          'Twitter OAuth env vars are still placeholders. Set TWITTER_OAUTH_CLIENT_ID and TWITTER_OAUTH_CLIENT_SECRET from developer.x.com.',
      });
    }

    // Exchange authorization code for access token
    const tokenResponse = await fetch('https://api.twitter.com/2/oauth2/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
      },
      body: new URLSearchParams({
        code,
        grant_type: 'authorization_code',
        redirect_uri: redirect_uri,
        code_verifier: codeVerifier,
      }),
    });

    if (!tokenResponse.ok) {
      const errorText = await tokenResponse.text();
      logger.error('Twitter token exchange error:', errorText);
      return res.status(tokenResponse.status).json({
        success: false,
        error: `Failed to exchange code for token: ${errorText}`,
      });
    }

    const tokenData = await tokenResponse.json() as TwitterTokenResponse;

    // Fetch user info using the access token
    let userInfo = null;
    try {
      const userResponse = await fetch('https://api.twitter.com/2/users/me?user.fields=name,username', {
        headers: {
          'Authorization': `Bearer ${tokenData.access_token}`,
        },
      });

      if (userResponse.ok) {
        const userData = await userResponse.json() as TwitterUserResponse;
        userInfo = {
          user_id_twitter: userData.data.id,
          username: userData.data.username,
          name: userData.data.name,
        };
      }
    } catch (userError) {
      logger.warn('Failed to fetch user info (non-fatal):', userError);
    }

    // Calculate expires_at
    const expiresAt = tokenData.expires_in
      ? new Date(Date.now() + tokenData.expires_in * 1000).toISOString()
      : null;

    const authenticatedUser = (req as any).user || user;
    if (authenticatedUser?.id) {
      await handleOAuthCallback({
        provider: 'twitter',
        userId: authenticatedUser.id,
        email: authenticatedUser.email,
        tokenResponse: tokenData as any,
        source: 'legacy_twitter_callback',
      });
    }

    res.json({
      success: true,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token || null,
      token_type: tokenData.token_type || 'Bearer',
      expires_in: tokenData.expires_in || null,
      expires_at: expiresAt,
      scope: tokenData.scope || null,
      user_id_twitter: userInfo?.user_id_twitter || null,
      username: userInfo?.username || null,
      name: userInfo?.name || null,
    });
  } catch (error) {
    logger.error('Twitter OAuth callback error:', error);
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Failed to process Twitter OAuth callback',
    });
  }
}
