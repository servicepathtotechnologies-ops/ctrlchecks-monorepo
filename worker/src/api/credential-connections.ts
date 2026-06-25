import { Request, Response } from 'express';
import { connectionService } from '../credentials-system/connection-service';
import { authInjectionEngine } from '../credentials-system/execution-auth';
import type { CredentialAuthRequest } from '../credentials-system/execution-auth-middleware';
import { nodeRegistryService } from '../credentials-system/node-registry-service';
import { oauthService } from '../credentials-system/oauth-service';
import { getCacheRedisClient, invalidateMissingItemsCache, invalidateAllMissingItemsCaches } from '../middleware/redisGetCache';
import { logger } from '../core/logger';

function userId(req: Request): string {
  const id = (req as any).user?.id || req.query.user_id || req.body?.userId;
  if (!id || typeof id !== 'string') throw new Error('Authenticated user is required');
  return id;
}

function frontendOrigin(returnTo?: string | null): string {
  if (returnTo) {
    try {
      return new URL(returnTo).origin;
    } catch {
      // Fall back below.
    }
  }
  return process.env.FRONTEND_URL || process.env.CORS_ORIGIN || 'http://localhost:8080';
}

function oauthCallbackHtml(input: {
  type: 'oauth-success' | 'oauth-error';
  connectionId?: string;
  message?: string;
  returnTo?: string | null;
}) {
  // Redirect the popup to a same-origin frontend relay page.
  // This bypasses COOP issues (e.g. marketplace.zoom.us) that permanently sever
  // window.opener after the popup passes through a cross-origin provider domain.
  // The relay page uses BroadcastChannel (unaffected by COOP) to deliver the result.
  const origin = frontendOrigin(input.returnTo);
  const relay = new URL(`${origin}/auth/oauth-relay`);
  relay.searchParams.set('type', input.type);
  if (input.connectionId) relay.searchParams.set('connectionId', input.connectionId);
  if (input.message) relay.searchParams.set('message', input.message);
  if (input.returnTo) relay.searchParams.set('returnTo', input.returnTo);
  const relayHref = relay.toString();

  return `<!DOCTYPE html><html><head><meta http-equiv="refresh" content="0;url=${relayHref}"><title>Connecting...</title></head><body><script>window.location.replace(${JSON.stringify(relayHref)})</script><p>Completing connection...</p></body></html>`;
}

export async function credentialTypesHandler(_req: Request, res: Response) {
  res.json({ credentialTypes: connectionService.listCredentialTypes() });
}

export async function registryNodesHandler(_req: Request, res: Response) {
  res.json({ nodes: nodeRegistryService.listNodeDefinitions() });
}

export async function listConnectionsHandler(req: Request, res: Response) {
  const uid = userId(req);

  // Credential-service canary — route % of traffic to the remote service.
  // Fallback: worker vault (existing path). Rollback: CREDENTIAL_SERVICE_ENABLED=false.
  const { shouldUseCredentialService, listConnectionsRemote } = await import('../services/credential-service-client');
  if (shouldUseCredentialService(uid)) {
    const remote = await listConnectionsRemote(uid);
    if (remote !== null) {
      return res.json({ connections: remote, source: 'credential-service' });
    }
    // Remote returned null (error/timeout) → fall through to local path
    logger.warn('[listConnectionsHandler] Credential-service fallback for user:', uid);
  }

  res.json({ connections: await connectionService.listConnections(uid) });
}

export async function createConnectionHandler(req: Request, res: Response) {
  const uid = userId(req);

  // Canary: delegate to credential-service if enabled for this user
  const { shouldUseCredentialService, createConnectionRemote, isCredentialVaultWritesDisabled } = await import('../services/credential-service-client');
  if (shouldUseCredentialService(uid)) {
    const remote = await createConnectionRemote(uid, {
      name: req.body.name,
      credentialTypeId: req.body.credentialTypeId,
      provider: req.body.provider || req.body.credentialTypeId?.split('_')[0] || '',
      authType: req.body.authType || 'oauth2',
      credentials: req.body.credentials || {},
      metadata: req.body.metadata || {},
    });
    if (remote !== null) {
      const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';
      getCacheRedisClient(redisUrl).then(redis => {
        if (redis) invalidateAllMissingItemsCaches(redis).catch(() => {});
      }).catch(() => {});
      return res.status(201).json({ connection: remote, source: 'credential-service' });
    }
    if (isCredentialVaultWritesDisabled()) {
      return res.status(503).json({ error: 'Credential service unavailable', code: 'CREDENTIAL_SERVICE_UNAVAILABLE' });
    }
    logger.warn('[createConnectionHandler] Credential-service fallback for user:', uid);
  }

  const connection = await connectionService.createConnection({
    userId: uid,
    name: req.body.name,
    credentialTypeId: req.body.credentialTypeId,
    credentials: req.body.credentials || {},
    metadata: req.body.metadata || {},
  });
  // Invalidate all missing-items caches so the frontend sees connected status immediately
  const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';
  getCacheRedisClient(redisUrl).then(redis => {
    if (redis) invalidateAllMissingItemsCaches(redis).catch(() => {});
  }).catch(() => {});
  res.status(201).json({ connection });
}

export async function updateConnectionHandler(req: Request, res: Response) {
  const uid = userId(req);
  const { id } = req.params;

  const { shouldUseCredentialService, updateConnectionRemote, isCredentialVaultWritesDisabled } = await import('../services/credential-service-client');
  if (shouldUseCredentialService(uid)) {
    const remote = await updateConnectionRemote(uid, id, {
      name: req.body.name,
      credentials: req.body.credentials,
      metadata: req.body.metadata,
    });
    if (remote !== null) {
      const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';
      getCacheRedisClient(redisUrl).then(redis => {
        if (redis) invalidateAllMissingItemsCaches(redis).catch(() => {});
      }).catch(() => {});
      return res.json({ connection: remote, source: 'credential-service' });
    }
    if (isCredentialVaultWritesDisabled()) {
      return res.status(503).json({ error: 'Credential service unavailable', code: 'CREDENTIAL_SERVICE_UNAVAILABLE' });
    }
    logger.warn('[updateConnectionHandler] Credential-service fallback for user:', uid);
  }

  const connection = await connectionService.updateConnection(uid, id, {
    name: req.body.name,
    credentials: req.body.credentials,
    metadata: req.body.metadata,
  });
  // Invalidate all missing-items caches on credential update
  const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';
  getCacheRedisClient(redisUrl).then(redis => {
    if (redis) invalidateAllMissingItemsCaches(redis).catch(() => {});
  }).catch(() => {});
  res.json({ connection });
}

export async function deleteConnectionHandler(req: Request, res: Response) {
  const uid = userId(req);
  const { id } = req.params;

  const { shouldUseCredentialService, deleteConnectionByIdRemote, isCredentialVaultWritesDisabled } = await import('../services/credential-service-client');
  if (shouldUseCredentialService(uid)) {
    const ok = await deleteConnectionByIdRemote(uid, id);
    if (ok) return res.status(204).send();
    if (isCredentialVaultWritesDisabled()) {
      return res.status(503).json({ error: 'Credential service unavailable', code: 'CREDENTIAL_SERVICE_UNAVAILABLE' });
    }
    logger.warn('[deleteConnectionHandler] Credential-service fallback for user:', uid);
  }

  await connectionService.deleteConnection(uid, id);
  res.status(204).send();
}

export async function testConnectionHandler(req: Request, res: Response) {
  const uid = userId(req);
  const { id } = req.params;

  const { shouldUseCredentialService, testConnectionRemote, isCredentialVaultWritesDisabled } = await import('../services/credential-service-client');
  if (shouldUseCredentialService(uid)) {
    const remote = await testConnectionRemote(uid, id);
    if (remote !== null) return res.json({ ...remote, source: 'credential-service' });
    if (isCredentialVaultWritesDisabled()) {
      return res.status(503).json({ error: 'Credential service unavailable', code: 'CREDENTIAL_SERVICE_UNAVAILABLE' });
    }
    logger.warn('[testConnectionHandler] Credential-service fallback for user:', uid);
  }

  res.json(await connectionService.testConnection(uid, id));
}

export async function oauthStartHandler(req: Request, res: Response) {
  const result = await oauthService.start({
    userId: userId(req),
    credentialTypeId: String(req.query.credentialTypeId || req.body.credentialTypeId),
    connectionId: typeof req.query.connectionId === 'string' ? req.query.connectionId : req.body.connectionId,
    scopes: typeof req.query.scopes === 'string' ? req.query.scopes.split(',') : req.body.scopes,
    returnTo: typeof req.query.returnTo === 'string'
      ? req.query.returnTo
      : req.body.returnTo || req.headers.origin || process.env.FRONTEND_URL,
  });
  res.json(result);
}

function mapOAuthErrorToUserMessage(rawMessage: string): string {
  const lower = rawMessage.toLowerCase();
  if (lower.includes('invalid_grant') || lower.includes('invalid authorization code')) {
    return 'The authorization code was rejected by the provider. This can happen if the connection window was open too long, or if the redirect URI is not registered in your app settings. Please try connecting again.';
  }
  if (lower.includes('invalid_client') || lower.includes('client authentication failed')) {
    return 'The app credentials were rejected. Check that your OAuth Client ID and Secret are correctly configured.';
  }
  if (lower.includes('redirect_uri_mismatch') || lower.includes('redirect uri')) {
    return 'The redirect URI does not match. Ensure the callback URL registered in your provider app settings matches the backend exactly.';
  }
  if (lower.includes('invalid or expired oauth state')) {
    return 'The connection session expired. Please start the connection flow again.';
  }
  if (lower.includes('access_denied') || lower.includes('user denied')) {
    return 'Access was denied. Please approve the requested permissions in the provider login window.';
  }
  return 'The connection could not be completed. Please try again.';
}

export async function oauthCallbackHandler(req: Request, res: Response) {
  const code = String(req.query.code || req.body.code || '');
  const state = String(req.query.state || req.body.state || '');
  let result: Awaited<ReturnType<typeof oauthService.callback>>;
  try {
    result = await oauthService.callback({ code, state });
  } catch (error) {
    if (req.method === 'GET') {
      const raw = error instanceof Error ? error.message : 'OAuth connection failed';
      logger.error('[OAuthCallback] callback error:', raw, error);
      const message = mapOAuthErrorToUserMessage(raw);
      return res.status(200).send(oauthCallbackHtml({ type: 'oauth-error', message }));
    }
    throw error;
  }
  // Invalidate missing-items cache so the frontend sees the connected status immediately
  const returnTo = result.returnTo || '';
  const workflowIdMatch = returnTo.match(/\/workflow\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i);
  if (workflowIdMatch) {
    const redisUrl = process.env.REDIS_URL || 'redis://redis:6379';
    getCacheRedisClient(redisUrl).then(redis => {
      if (redis) invalidateMissingItemsCache(workflowIdMatch[1], redis).catch(() => {});
    }).catch(() => {});
  }

  if (req.method === 'GET') {
    return res.send(oauthCallbackHtml({
      type: 'oauth-success',
      connectionId: result.connectionId,
      returnTo: result.returnTo,
    }));
  }
  return res.json(result);
}

export async function oauthReconnectHandler(req: Request, res: Response) {
  const connection = await connectionService.getDecryptedConnection(userId(req), req.params.id);
  const result = await oauthService.start({
    userId: userId(req),
    credentialTypeId: connection.credentialTypeId,
    connectionId: connection.id,
    returnTo: req.body.returnTo || req.headers.origin || process.env.FRONTEND_URL,
  });
  res.json(result);
}

export async function executeAuthenticatedRequestHandler(req: Request, res: Response) {
  const credentialReq = req as CredentialAuthRequest;
  if (credentialReq.credentialAuth) {
    return res.json(await credentialReq.credentialAuth.execute(req.body.request));
  }

  const result = await authInjectionEngine.executeNodeRequest(
    {
      userId: userId(req),
      workflowId: req.body.workflowId,
      nodeId: req.body.nodeId,
      nodeType: req.body.nodeType,
      connectionId: req.body.connectionId,
    },
    req.body.request,
  );
  res.json(result);
}
