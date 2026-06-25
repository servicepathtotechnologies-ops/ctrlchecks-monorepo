import { Request, Response } from 'express';
import { handleOAuthCallback } from '../services/oauth-callback-handler';
import { logger } from '../core/logger';

const META_APP_ID =
  process.env.META_APP_ID ||
  process.env.FACEBOOK_APP_ID ||
  process.env.INSTAGRAM_APP_ID ||
  process.env.WHATSAPP_APP_ID ||
  '';
const META_APP_SECRET =
  process.env.META_APP_SECRET ||
  process.env.FACEBOOK_APP_SECRET ||
  process.env.INSTAGRAM_APP_SECRET ||
  process.env.WHATSAPP_APP_SECRET ||
  '';

const GRAPH_VERSION = 'v19.0';

function envRedirectForPath(fallbackPath: string): string | undefined {
  if (fallbackPath === '/auth/instagram/callback') return process.env.INSTAGRAM_OAUTH_REDIRECT_URI;
  if (fallbackPath === '/auth/whatsapp/callback') return process.env.WHATSAPP_OAUTH_REDIRECT_URI;
  return undefined;
}

function getRedirectUri(req: Request, fallbackPath: string): string {
  const envRedirect = envRedirectForPath(fallbackPath);
  const raw = (req.query.redirect_uri as string) || (req.body?.redirect_uri as string) || '';
  try {
    const url = new URL(raw);
    const isLocal =
      url.origin === 'http://localhost:8080' ||
      url.origin === 'http://127.0.0.1:8080' ||
      url.origin === process.env.FRONTEND_URL;
    if (isLocal && url.pathname === fallbackPath) return url.toString();
  } catch {
    // fall through
  }
  return envRedirect || `${process.env.FRONTEND_URL || 'http://localhost:8080'}${fallbackPath}`;
}

async function exchangeCodeForLongLivedToken(code: string, redirectUri: string) {
  if (!META_APP_ID || !META_APP_SECRET) {
    throw new Error('Meta app credentials are not configured');
  }

  const shortTokenRes = await fetch(
    `https://graph.facebook.com/${GRAPH_VERSION}/oauth/access_token?` +
      new URLSearchParams({
        client_id: META_APP_ID,
        client_secret: META_APP_SECRET,
        redirect_uri: redirectUri,
        code,
      }).toString()
  );
  const shortToken: any = await shortTokenRes.json();
  if (!shortTokenRes.ok || !shortToken.access_token) {
    throw new Error(shortToken.error?.message || 'No access token from Meta');
  }

  const longTokenRes = await fetch(
    `https://graph.facebook.com/${GRAPH_VERSION}/oauth/access_token?` +
      new URLSearchParams({
        grant_type: 'fb_exchange_token',
        client_id: META_APP_ID,
        client_secret: META_APP_SECRET,
        fb_exchange_token: shortToken.access_token,
      }).toString()
  );
  const longToken: any = await longTokenRes.json();
  return {
    access_token: longToken.access_token || shortToken.access_token,
    expires_in: longToken.expires_in || shortToken.expires_in || 3600,
    token_type: longToken.token_type || shortToken.token_type || 'Bearer',
  };
}

export function instagramAuthorizeHandler(req: Request, res: Response) {
  if (!META_APP_ID) return res.status(500).json({ error: 'META_APP_ID not configured' });

  const redirectUri = getRedirectUri(req, '/auth/instagram/callback');
  const authUrl = new URL(`https://www.facebook.com/${GRAPH_VERSION}/dialog/oauth`);
  authUrl.searchParams.set('client_id', META_APP_ID);
  authUrl.searchParams.set('redirect_uri', redirectUri);
  authUrl.searchParams.set('response_type', 'code');
  authUrl.searchParams.set(
    'scope',
    [
      'public_profile',
      'pages_show_list',
      'pages_read_engagement',
      'instagram_basic',
      'instagram_content_publish',
      'business_management',
    ].join(',')
  );

  return res.redirect(authUrl.toString());
}

export async function instagramCallbackHandler(req: Request, res: Response) {
  try {
    const { code } = req.body as { code?: string };
    if (!code) return res.status(400).json({ error: 'code required' });

    const redirectUri = getRedirectUri(req, '/auth/instagram/callback');
    const tokenData = await exchangeCodeForLongLivedToken(code, redirectUri);
    const expiresAt = new Date(Date.now() + tokenData.expires_in * 1000).toISOString();

    let igAccount: any = null;
    const pagesRes = await fetch(
      `https://graph.facebook.com/${GRAPH_VERSION}/me/accounts?fields=id,name,instagram_business_account{id,username,name,profile_picture_url}&access_token=${tokenData.access_token}`
    );
    if (pagesRes.ok) {
      const pages: any = await pagesRes.json();
      igAccount = (pages.data || [])
        .map((page: any) => page.instagram_business_account)
        .find(Boolean);
    }

    const responsePayload = {
      access_token: tokenData.access_token,
      expires_at: expiresAt,
      token_type: tokenData.token_type,
      scope: 'instagram_basic,instagram_content_publish,pages_show_list,pages_read_engagement,business_management',
      ig_user_id: igAccount?.id || null,
      username: igAccount?.username || null,
      name: igAccount?.name || null,
      profile_picture_url: igAccount?.profile_picture_url || null,
    };
    const user = (req as any).user;
    if (user?.id) {
      await handleOAuthCallback({
        provider: 'instagram',
        userId: user.id,
        email: user.email,
        tokenResponse: responsePayload,
        source: 'legacy_instagram_callback',
      });
    }
    return res.json(responsePayload);
  } catch (err: any) {
    logger.error('[InstagramOAuth] Error:', err.message);
    return res.status(500).json({ error: err.message || 'Instagram OAuth failed' });
  }
}

export function whatsappAuthorizeHandler(req: Request, res: Response) {
  if (!META_APP_ID) return res.status(500).json({ error: 'META_APP_ID not configured' });

  const redirectUri = getRedirectUri(req, '/auth/whatsapp/callback');
  const authUrl = new URL(`https://www.facebook.com/${GRAPH_VERSION}/dialog/oauth`);
  authUrl.searchParams.set('client_id', META_APP_ID);
  authUrl.searchParams.set('redirect_uri', redirectUri);
  authUrl.searchParams.set('response_type', 'code');
  authUrl.searchParams.set(
    'scope',
    [
      'public_profile',
      'business_management',
      'whatsapp_business_management',
      'whatsapp_business_messaging',
    ].join(',')
  );

  return res.redirect(authUrl.toString());
}

export async function whatsappCallbackHandler(req: Request, res: Response) {
  try {
    const { code, phone_number_id, business_account_id } = req.body as {
      code?: string;
      phone_number_id?: string;
      business_account_id?: string;
    };
    if (!code) return res.status(400).json({ error: 'code required' });

    const redirectUri = getRedirectUri(req, '/auth/whatsapp/callback');
    const tokenData = await exchangeCodeForLongLivedToken(code, redirectUri);
    const expiresAt = new Date(Date.now() + tokenData.expires_in * 1000).toISOString();

    let phoneNumber: string | null = null;
    let resolvedWabaId = business_account_id || null;
    if (phone_number_id) {
      const phoneRes = await fetch(
        `https://graph.facebook.com/${GRAPH_VERSION}/${phone_number_id}?fields=display_phone_number,verified_name,whatsapp_business_account&access_token=${tokenData.access_token}`
      );
      if (phoneRes.ok) {
        const phoneData: any = await phoneRes.json();
        phoneNumber = phoneData.display_phone_number || phoneData.verified_name || null;
        resolvedWabaId = resolvedWabaId || phoneData.whatsapp_business_account?.id || null;
      }
    }

    const responsePayload = {
      access_token: tokenData.access_token,
      expires_at: expiresAt,
      token_type: tokenData.token_type,
      scope: 'business_management,whatsapp_business_management,whatsapp_business_messaging',
      phone_number_id: phone_number_id || null,
      business_account_id: resolvedWabaId,
      phone_number: phoneNumber,
    };
    const user = (req as any).user;
    if (user?.id) {
      await handleOAuthCallback({
        provider: 'whatsapp',
        userId: user.id,
        email: user.email,
        tokenResponse: responsePayload,
        source: 'legacy_whatsapp_callback',
      });
    }
    return res.json(responsePayload);
  } catch (err: any) {
    logger.error('[WhatsAppOAuth] Error:', err.message);
    return res.status(500).json({ error: err.message || 'WhatsApp OAuth failed' });
  }
}
