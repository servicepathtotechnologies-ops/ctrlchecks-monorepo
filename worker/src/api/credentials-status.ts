import { Request, Response } from 'express';
import { resolveCredentialDryRun, formatCredentialError } from '../services/credential-resolver';
import { requiredScopesForProvider, normalizeProvider } from '../services/credential-scope-registry';
import { logger } from '../core/logger';

export async function credentialStatusHandler(req: Request, res: Response) {
  const user = (req as any).user;
  const providerRaw = String(req.query.provider || '').trim();
  if (!user?.id) return res.status(401).json({ connected: false, reason: 'Unauthorized' });
  if (!providerRaw) return res.status(400).json({ connected: false, reason: 'provider is required' });

  const provider = normalizeProvider(providerRaw);
  const requiredScopes = requiredScopesForProvider(provider);

  try {
    const credential = await resolveCredentialDryRun({
      userId: user.id,
      provider,
      requiredScopes,
      action: 'connection_status',
    });

    return res.json({
      connected: true,
      provider,
      scopes: credential.scopes,
      expiresAt: credential.expiresAt,
      source: credential.source,
    });
  } catch (error) {
    const formatted = formatCredentialError(error, 'connection_status');
    logger.warn('[CredentialStatus] Credential is not runnable', formatted);
    return res.json({
      connected: false,
      provider,
      reason: formatted.error,
      details: formatted,
    });
  }
}

