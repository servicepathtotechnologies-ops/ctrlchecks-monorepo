/**
 * GitHub OAuth Flow
 *
 * GitHub does NOT support OIDC, so Cognito cannot federate it directly.
 * Two flows are supported:
 *
 * 1. PRIMARY LOGIN (unauthenticated):
 *    GET /api/oauth/github/start-login   — redirect user → GitHub (no auth required)
 *    GET /api/oauth/github/callback      — exchange code, create/find Cognito user, redirect frontend
 *    POST /api/oauth/github/exchange-session — frontend exchanges UUID for Cognito tokens
 *
 * 2. CONNECT (already authenticated — link GitHub to existing account):
 *    GET /api/oauth/github/start         — redirect user → GitHub (auth required)
 *    GET /api/oauth/github/callback      — exchange code, store token, redirect frontend
 */

import { Request, Response } from 'express';
import { queryAsService } from '../core/database/db-pool';
import { resolveCanonicalUserId } from '../core/database/identity-resolver';
import { ensureUserRows } from '../core/database/ensure-user';
import { config } from '../core/config';
import { encryptToken } from '../core/utils/token-encryption';
import crypto from 'crypto';
import AWS from 'aws-sdk';
import { logger } from '../core/logger';

const GITHUB_CLIENT_ID     = process.env.GITHUB_CLIENT_ID     || '';
const GITHUB_CLIENT_SECRET = process.env.GITHUB_CLIENT_SECRET || '';
const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:8080';
const omitGitHubRedirectUri = process.env.GITHUB_OAUTH_OMIT_REDIRECT_URI === 'true';

const COGNITO_USER_POOL_ID  = process.env.COGNITO_USER_POOL_ID  || '';
// Use the backend app client (has a secret) for admin operations.
// The SPA client (COGNITO_CLIENT_ID) has no secret and is used only by the browser.
const COGNITO_ADMIN_CLIENT_ID = process.env.COGNITO_ADMIN_CLIENT_ID || process.env.COGNITO_CLIENT_ID || '';
const COGNITO_CLIENT_SECRET   = process.env.COGNITO_CLIENT_SECRET   || '';

const cognitoAdmin = new AWS.CognitoIdentityServiceProvider({
  region: process.env.AWS_REGION || 'ap-south-1',
});

// ── Short-lived in-memory session store (token exchange) ─────────────────────
// Stores Cognito tokens keyed by a UUID for 90 seconds, letting the frontend
// exchange the code for tokens without ever putting JWTs in the URL.

interface PendingSession {
  accessToken:  string;
  idToken:      string;
  refreshToken: string;
  username:     string;
  expiresAt:    number;
}

const pendingSessions = new Map<string, PendingSession>();

setInterval(() => {
  const now = Date.now();
  for (const [k, v] of pendingSessions) {
    if (v.expiresAt < now) pendingSessions.delete(k);
  }
}, 30_000);

// ── Helpers ──────────────────────────────────────────────────────────────────

function githubCallbackUrl() {
  const configuredRedirectUri = process.env.GITHUB_OAUTH_REDIRECT_URI?.trim();
  if (configuredRedirectUri) return configuredRedirectUri;

  return `${config.publicBaseUrl}/api/oauth/github/callback`;
}

function setGitHubRedirectUri(authUrl: URL) {
  const redirectUri = githubCallbackUrl();
  logger.info(`[GitHubOAuth] redirect_uri ${omitGitHubRedirectUri ? 'omitted' : redirectUri}`);
  if (!omitGitHubRedirectUri) {
    authUrl.searchParams.set('redirect_uri', redirectUri);
  }
}

function githubTokenExchangeBody(code: string) {
  const body: Record<string, string> = {
    client_id: GITHUB_CLIENT_ID,
    client_secret: GITHUB_CLIENT_SECRET,
    code,
  };

  if (!omitGitHubRedirectUri) {
    body.redirect_uri = githubCallbackUrl();
  }

  return body;
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

function computeCognitoSecretHash(username: string): string {
  if (!COGNITO_CLIENT_SECRET) return '';
  return crypto
    .createHmac('sha256', COGNITO_CLIENT_SECRET)
    .update(username + COGNITO_ADMIN_CLIENT_ID)
    .digest('base64');
}

function generatePassword(): string {
  // Cognito requires: 8+ chars, upper, lower, number, symbol
  const id = crypto.randomBytes(16).toString('hex');
  return `Gh!${id.slice(0, 8)}A1a`;
}

// ── Cognito admin helpers ─────────────────────────────────────────────────────

async function findOrCreateCognitoUser(email: string, name: string): Promise<string> {
  const sanitizedUsername = email.toLowerCase().replace(/[^a-z0-9@._\-+]/g, '_');

  // Try to find by email first
  try {
    await cognitoAdmin.adminGetUser({
      UserPoolId: COGNITO_USER_POOL_ID,
      Username:   sanitizedUsername,
    }).promise();
    // User exists
    return sanitizedUsername;
  } catch (err: any) {
    if (err.code !== 'UserNotFoundException') throw err;
  }

  // User doesn't exist — create them
  const createParams: AWS.CognitoIdentityServiceProvider.AdminCreateUserRequest = {
    UserPoolId:    COGNITO_USER_POOL_ID,
    Username:      sanitizedUsername,
    MessageAction: 'SUPPRESS',   // don't send welcome email
    UserAttributes: [
      { Name: 'email',          Value: email },
      { Name: 'email_verified', Value: 'true' },
      { Name: 'name',           Value: name || email.split('@')[0] },
    ],
  };
  await cognitoAdmin.adminCreateUser(createParams).promise();

  // Set a permanent random password so no FORCE_CHANGE_PASSWORD challenge is triggered
  const pwd = generatePassword();
  await cognitoAdmin.adminSetUserPassword({
    UserPoolId: COGNITO_USER_POOL_ID,
    Username:   sanitizedUsername,
    Password:   pwd,
    Permanent:  true,
  }).promise();

  return sanitizedUsername;
}


// ── Route handlers ────────────────────────────────────────────────────────────

/**
 * GET /api/oauth/github/start-login?redirect_to=<path>
 * Called on the sign-in page (no auth required); starts GitHub OAuth for primary login.
 */
export function githubLoginStart(req: Request, res: Response) {
  const redirectTo = req.query.redirect_to as string || '/dashboard';

  if (!GITHUB_CLIENT_ID) {
    return res.status(500).json({ error: 'GITHUB_CLIENT_ID not configured' });
  }

  const state   = signState({ mode: 'login', redirectTo, ts: Date.now() });
  const authUrl = new URL('https://github.com/login/oauth/authorize');
  authUrl.searchParams.set('client_id',    GITHUB_CLIENT_ID);
  setGitHubRedirectUri(authUrl);
  authUrl.searchParams.set('scope',        'user:email read:user');
  authUrl.searchParams.set('state',        state);

  return res.redirect(authUrl.toString());
}

/**
 * GET /api/oauth/github/start?user_id=<cognito_sub>&redirect_to=<path>
 * Called by authenticated users to CONNECT their GitHub account.
 */
export function githubOAuthStart(req: Request, res: Response) {
  const userId    = req.query.user_id   as string || (req as any).user?.id;
  const redirectTo = req.query.redirect_to as string || '/workflows';

  if (!userId) {
    return res.status(401).json({ error: 'user_id required — must be authenticated' });
  }
  if (!GITHUB_CLIENT_ID) {
    return res.status(500).json({ error: 'GITHUB_CLIENT_ID not configured' });
  }

  const state   = signState({ mode: 'connect', userId, redirectTo, ts: Date.now() });
  const authUrl = new URL('https://github.com/login/oauth/authorize');
  authUrl.searchParams.set('client_id',    GITHUB_CLIENT_ID);
  setGitHubRedirectUri(authUrl);
  authUrl.searchParams.set('scope',        'user:email read:user');
  authUrl.searchParams.set('state',        state);

  return res.redirect(authUrl.toString());
}

/**
 * GET /api/oauth/github/callback?code=...&state=...
 * Shared callback for both login and connect flows.
 */
export async function githubOAuthCallback(req: Request, res: Response) {
  const { code, state, error: oauthError } = req.query as Record<string, string>;

  if (oauthError) {
    return res.redirect(`${FRONTEND_URL}/auth/github/callback?error=${encodeURIComponent(oauthError)}`);
  }

  const stateData = verifyState(state || '');
  if (!stateData) {
    return res.redirect(`${FRONTEND_URL}/auth/github/callback?error=invalid_state`);
  }

  const { mode, userId, redirectTo } = stateData;

  try {
    // 1. Exchange code for GitHub access token
    const tokenRes = await fetch('https://github.com/login/oauth/access_token', {
      method:  'POST',
      headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' },
      body:    JSON.stringify(githubTokenExchangeBody(code)),
    });
    const tokenData: any = await tokenRes.json();

    if (!tokenData.access_token) {
      throw new Error(tokenData.error_description || 'No access_token in GitHub response');
    }

    const githubAccessToken = tokenData.access_token as string;

    // 2. Fetch GitHub user profile
    const profileRes = await fetch('https://api.github.com/user', {
      headers: { Authorization: `token ${githubAccessToken}`, 'User-Agent': 'CtrlChecks' },
    });
    const profile: any = await profileRes.json();

    // 3. Fetch primary email (may not be public on profile)
    let email = profile.email as string | null;
    if (!email) {
      const emailsRes = await fetch('https://api.github.com/user/emails', {
        headers: { Authorization: `token ${githubAccessToken}`, 'User-Agent': 'CtrlChecks' },
      });
      const emails = (await emailsRes.json()) as any[];
      email = emails.find((e) => e.primary && e.verified)?.email
           || emails.find((e) => e.primary)?.email
           || emails[0]?.email
           || null;
    }

    if (mode === 'login') {
      // ── PRIMARY LOGIN FLOW ────────────────────────────────────────────────
      if (!email) throw new Error('Could not retrieve email from GitHub account');
      if (!COGNITO_USER_POOL_ID || !COGNITO_ADMIN_CLIENT_ID) {
        throw new Error('Cognito not configured on server');
      }

      // 4a. Find or create Cognito user
      const displayName = profile.name || profile.login || email.split('@')[0];
      const pwd         = generatePassword();
      const username    = await findOrCreateCognitoUser(email, displayName);

      // 4b. Reset password so we can auth immediately
      await cognitoAdmin.adminSetUserPassword({
        UserPoolId: COGNITO_USER_POOL_ID,
        Username:   username,
        Password:   pwd,
        Permanent:  true,
      }).promise();

      // 4c. Get Cognito tokens via admin auth
      const authParams: AWS.CognitoIdentityServiceProvider.AdminInitiateAuthRequest = {
        UserPoolId: COGNITO_USER_POOL_ID,
        ClientId:   COGNITO_ADMIN_CLIENT_ID,
        AuthFlow:   'ADMIN_USER_PASSWORD_AUTH',
        AuthParameters: { USERNAME: username, PASSWORD: pwd },
      };
      const secretHash = computeCognitoSecretHash(username);
      if (secretHash) authParams.AuthParameters!['SECRET_HASH'] = secretHash;

      const authResult = await cognitoAdmin.adminInitiateAuth(authParams).promise();
      if (authResult.ChallengeName) {
        throw new Error(`Cognito challenge not handled: ${authResult.ChallengeName}. Enable ALLOW_ADMIN_USER_PASSWORD_AUTH on user pool app client.`);
      }

      const ar = authResult.AuthenticationResult!;

      // 4d. Decode the access token to extract sub (UUID for DB) and cognito:username (for Amplify)
      const jwtPayload      = JSON.parse(Buffer.from(ar.AccessToken!.split('.')[1], 'base64url').toString());
      const cognitoUserId   = jwtPayload.sub as string;
      const cognitoUsername = (jwtPayload['cognito:username'] as string) || username;

      // 4e. Resolve canonical user — if an email/password (or Google) account already exists
      // with this email, use that as the canonical ID so tokens and data are stored once.
      const canonicalUserId = await resolveCanonicalUserId(cognitoUserId, email).catch(() => cognitoUserId);

      if (canonicalUserId !== cognitoUserId) {
        // Record the link so identity_links fast-path resolves every subsequent request instantly.
        await queryAsService(
          `INSERT INTO identity_links (canonical_user_id, linked_user_id, provider)
           VALUES ($1, $2, 'github') ON CONFLICT (linked_user_id) DO NOTHING`,
          [canonicalUserId, cognitoUserId]
        ).catch(() => {});
      }

      // Ensure a users row exists for the canonical ID (idempotent upsert).
      await ensureUserRows(canonicalUserId, email, displayName).catch(() => {});

      // 4f. Store GitHub token under the canonical user ID — NOT the ephemeral Cognito sub.
      // Without this, credential lookups keyed by canonical ID would return nothing.
      await queryAsService(
        `INSERT INTO social_tokens (user_id, provider, access_token, provider_user_id, scope, updated_at)
         VALUES ($1, 'github', $2, $3, $4, NOW())
         ON CONFLICT (user_id, provider)
         DO UPDATE SET access_token     = EXCLUDED.access_token,
                       provider_user_id = EXCLUDED.provider_user_id,
                       scope            = EXCLUDED.scope,
                       updated_at       = NOW()`,
        [canonicalUserId, encryptToken(githubAccessToken), String(profile.id), tokenData.scope || 'user:email read:user']
      );

      // 4g. Store tokens in pending session (frontend exchanges this code for tokens)
      const sessionCode = crypto.randomUUID();
      pendingSessions.set(sessionCode, {
        accessToken:  ar.AccessToken!,
        idToken:      ar.IdToken!,
        refreshToken: ar.RefreshToken!,
        username:     cognitoUsername,
        expiresAt:    Date.now() + 90_000,
      });

      logger.info(`[GitHubLogin] ✅ Login via GitHub for ${email} (sub: ${cognitoUserId} → canonical: ${canonicalUserId})`);

      const returnUrl = encodeURIComponent(redirectTo || '/dashboard');
      return res.redirect(
        `${FRONTEND_URL}/auth/github/callback?mode=login&session_code=${sessionCode}&login=${encodeURIComponent(profile.login)}&return_to=${returnUrl}`
      );

    } else {
      // ── CONNECT FLOW (existing authenticated user) ────────────────────────
      const targetUserId = userId;
      if (!targetUserId) throw new Error('No user_id in state — must be authenticated to connect GitHub');

      // 4a. Upsert GitHub token
      await queryAsService(
        `INSERT INTO social_tokens (user_id, provider, access_token, provider_user_id, scope, updated_at)
         VALUES ($1, 'github', $2, $3, $4, NOW())
         ON CONFLICT (user_id, provider)
         DO UPDATE SET access_token = EXCLUDED.access_token,
                       provider_user_id = EXCLUDED.provider_user_id,
                       scope = EXCLUDED.scope,
                       updated_at = NOW()`,
        [targetUserId, encryptToken(githubAccessToken), String(profile.id), tokenData.scope || 'user:email read:user']
      );

      logger.info(`[GitHubOAuth] ✅ Connected GitHub for user ${targetUserId} (login: ${profile.login})`);

      const returnUrl = encodeURIComponent(redirectTo || '/workflows');
      return res.redirect(
        `${FRONTEND_URL}/auth/github/callback?success=true&login=${encodeURIComponent(profile.login)}&return_to=${returnUrl}`
      );
    }

  } catch (err: any) {
    logger.error('[GitHubOAuth] Error:', err.message);
    return res.redirect(
      `${FRONTEND_URL}/auth/github/callback?error=${encodeURIComponent(err.message || 'oauth_failed')}`
    );
  }
}

/**
 * POST /api/oauth/github/exchange-session
 * Body: { session_code: string }
 * Returns Cognito tokens so the frontend can inject them into Amplify storage.
 * One-time use — the session is deleted after exchange.
 */
export function githubExchangeSession(req: Request, res: Response) {
  const { session_code } = req.body as { session_code?: string };
  if (!session_code) {
    return res.status(400).json({ error: 'session_code required' });
  }

  const session = pendingSessions.get(session_code);
  if (!session) {
    return res.status(404).json({ error: 'Session not found or expired' });
  }
  if (session.expiresAt < Date.now()) {
    pendingSessions.delete(session_code);
    return res.status(410).json({ error: 'Session expired' });
  }

  pendingSessions.delete(session_code); // one-time use

  return res.json({
    success:      true,
    accessToken:  session.accessToken,
    idToken:      session.idToken,
    refreshToken: session.refreshToken,
    username:     session.username,
  });
}
