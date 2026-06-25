/**
 * Notion OAuth API Handlers
 * 
 * Handles Notion OAuth flow:
 * 1. /api/oauth/notion/authorize - Initiates OAuth flow
 * 2. /api/oauth/notion/callback - Handles OAuth callback and token exchange
 */

import { Request, Response } from 'express';
import { handleOAuthCallback } from '../services/oauth-callback-handler';
import { getDbClient } from '../core/database/aws-db-client';
import { config } from '../core/config';
import { logger } from '../core/logger';

/**
 * Type definitions for Notion OAuth responses
 */
interface NotionTokenResponse {
  access_token: string;
  refresh_token?: string;
  token_type?: string;
  expires_in?: number;
  scope?: string;
}

interface NotionUserResponse {
  bot?: {
    id: string;
  };
  owner?: {
    workspace?: {
      id: string;
      name: string;
    };
  };
}

/**
 * Type definitions for Notion OAuth responses
 */
interface NotionTokenResponse {
  access_token: string;
  refresh_token?: string;
  token_type?: string;
  expires_in?: number;
  scope?: string;
}

interface NotionUserResponse {
  bot?: {
    id: string;
  };
  owner?: {
    workspace?: {
      id: string;
      name: string;
    };
  };
}

/**
 * Initiate Notion OAuth flow
 * GET /api/oauth/notion/authorize?redirect_uri=...
 */
export async function notionAuthorizeHandler(req: Request, res: Response) {
  try {
    const redirectUri = (req.query.redirect_uri as string) || process.env.NOTION_OAUTH_REDIRECT_URI;
    
    if (!redirectUri) {
      return res.status(400).json({
        success: false,
        error: 'redirect_uri parameter is required',
      });
    }

    // Get Notion OAuth credentials from environment
    const clientId = process.env.NOTION_OAUTH_CLIENT_ID;
    const clientSecret = process.env.NOTION_OAUTH_CLIENT_SECRET;

    if (!clientId) {
      return res.status(500).json({
        success: false,
        error: 'Notion OAuth client ID not configured. Please set NOTION_OAUTH_CLIENT_ID environment variable.',
      });
    }

    // Generate state for CSRF protection
    const state = Buffer.from(`${Date.now()}-${Math.random()}`).toString('base64');

    // Notion OAuth authorization URL
    const authUrl = new URL('https://api.notion.com/v1/oauth/authorize');
    authUrl.searchParams.set('client_id', clientId);
    authUrl.searchParams.set('redirect_uri', redirectUri);
    authUrl.searchParams.set('response_type', 'code');
    authUrl.searchParams.set('owner', 'user');
    authUrl.searchParams.set('state', state);

    // Store state in session or return it to client
    // For simplicity, we'll include it in the redirect
    res.redirect(authUrl.toString());
  } catch (error) {
    logger.error('Notion OAuth authorize error:', error);
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Failed to initiate Notion OAuth',
    });
  }
}

/**
 * Handle Notion OAuth callback and exchange code for token
 * POST /api/oauth/notion/callback
 */
export async function notionCallbackHandler(req: Request, res: Response) {
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
    const redirect_uri = req.body.redirect_uri || process.env.NOTION_OAUTH_REDIRECT_URI;

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

    // Get Notion OAuth credentials
    const clientId = process.env.NOTION_OAUTH_CLIENT_ID;
    const clientSecret = process.env.NOTION_OAUTH_CLIENT_SECRET;

    if (!clientId || !clientSecret) {
      return res.status(500).json({
        success: false,
        error: 'Notion OAuth credentials not configured',
      });
    }

    // Exchange authorization code for access token
    const tokenResponse = await fetch('https://api.notion.com/v1/oauth/token', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
      },
      body: JSON.stringify({
        grant_type: 'authorization_code',
        code,
        redirect_uri: redirect_uri,
      }),
    });

    if (!tokenResponse.ok) {
      const errorText = await tokenResponse.text();
      logger.error('Notion token exchange error:', errorText);
      return res.status(tokenResponse.status).json({
        success: false,
        error: `Failed to exchange code for token: ${errorText}`,
      });
    }

    const tokenData = await tokenResponse.json() as NotionTokenResponse;

    // Fetch workspace info using the access token
    let workspaceInfo = null;
    try {
      const workspaceResponse = await fetch('https://api.notion.com/v1/users/me', {
        headers: {
          'Authorization': `Bearer ${tokenData.access_token}`,
          'Notion-Version': '2022-06-28',
        },
      });

      if (workspaceResponse.ok) {
        const userData = await workspaceResponse.json() as NotionUserResponse;
        workspaceInfo = {
          bot_id: userData.bot?.id || null,
          workspace_id: userData.owner?.workspace?.id || null,
          workspace_name: userData.owner?.workspace?.name || null,
        };
      }
    } catch (workspaceError) {
      logger.warn('Failed to fetch workspace info (non-fatal):', workspaceError);
    }

    const authenticatedUser = (req as any).user || user;
    if (authenticatedUser?.id) {
      await handleOAuthCallback({
        provider: 'notion',
        userId: authenticatedUser.id,
        email: authenticatedUser.email,
        tokenResponse: tokenData as any,
        source: 'legacy_notion_callback',
      });
    }

    res.json({
      success: true,
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token || null,
      token_type: tokenData.token_type || 'Bearer',
      expires_in: tokenData.expires_in || null,
      scope: tokenData.scope || null,
      bot_id: workspaceInfo?.bot_id || null,
      workspace_id: workspaceInfo?.workspace_id || null,
      workspace_name: workspaceInfo?.workspace_name || null,
    });
  } catch (error) {
    logger.error('Notion OAuth callback error:', error);
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Failed to process Notion OAuth callback',
    });
  }
}
