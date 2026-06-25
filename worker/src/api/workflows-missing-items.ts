/**
 * GET /api/workflows/:workflowId/missing-items
 *
 * Returns unified list of missing credentials and sensitive inputs for a workflow.
 * Uses both discoverCredentials (wizard-phase discovery) AND executionPreflight
 * (authoritative OAuth check) so that credentials injected as string aliases
 * (e.g. credentialId:"google") are still correctly reported as missing when the
 * user hasn't actually connected the provider.
 */

import { Request, Response } from 'express';
import { getUnifiedMissingItems } from '../services/ai/credential-input-discovery';
import { executionPreflight } from '../services/execution-preflight';
import { getDbClient } from '../core/database/aws-db-client';
import { credentialRequirementForNode } from '../services/credential-scope-registry';
import { logger } from '../core/logger';

/** Human-readable provider display names */
const PROVIDER_DISPLAY: Record<string, string> = {
  google: 'Google',
  microsoft: 'Microsoft',
  slack: 'Slack',
  github: 'GitHub',
  notion: 'Notion',
  twitter: 'Twitter / X',
  instagram: 'Instagram',
  facebook: 'Facebook',
  linkedin: 'LinkedIn',
  whatsapp: 'WhatsApp',
  salesforce: 'Salesforce',
  zoho: 'Zoho',
  youtube: 'YouTube',
};

function providerDisplayName(provider: string): string {
  return PROVIDER_DISPLAY[provider.toLowerCase()] ?? (provider.charAt(0).toUpperCase() + provider.slice(1));
}

export default async function getMissingItemsHandler(req: Request, res: Response) {
  try {
    const { workflowId } = req.params;

    if (!workflowId) {
      return res.status(400).json({ error: 'workflowId is required' });
    }

    // Extract user ID from auth header (optional — preflight needs it for vault lookup)
    const db = getDbClient();
    const authHeader = req.headers.authorization;
    let userId: string | undefined;

    if (authHeader?.startsWith('Bearer ')) {
      const token = authHeader.replace('Bearer ', '').trim();
      if (token) {
        try {
          const { data: { user }, error: authError } = await db.auth.getUser(token);
          if (!authError && user) userId = user.id;
        } catch {
          // non-fatal
        }
      }
    }

    logger.info(`[MissingItems] Getting missing items for workflow ${workflowId}, userId=${userId || 'anonymous'}`);

    // ── 1. Standard unified discovery (credentials + inputs) ─────────────
    const missingItems = await getUnifiedMissingItems(workflowId, userId);

    // ── 2. Authoritative preflight credential check ───────────────────────
    // discoverCredentials() sometimes silently discards credentials when the
    // node config has credentialId set to a string alias ("google") rather than
    // a real UUID, causing a DB uuid-parse error.  executionPreflight() uses a
    // different path (credentialRequirementForNode + resolveCredentialDryRun)
    // that correctly checks the unified_credentials table by provider name.
    if (userId) {
      try {
        // Load the workflow nodes so we can run preflight
        const { data: workflowRow } = await db
          .from('workflows')
          .select('nodes, graph')
          .eq('id', workflowId)
          .single();

        if (workflowRow) {
          const graphData =
            typeof workflowRow.graph === 'string'
              ? JSON.parse(workflowRow.graph)
              : workflowRow.graph || {};
          const nodes: any[] = workflowRow.nodes || graphData.nodes || [];

          const preflightResult = await executionPreflight({
            workflowId,
            ownerId: userId,
            nodes,
          });

          if (!preflightResult.ok && preflightResult.failures.length > 0) {
            // Build a set of providers already reported as missing by discoverCredentials
            const alreadyMissingProviders = new Set(
              missingItems.credentials
                .filter((c) => c.satisfied === false)
                .map((c) => c.provider.toLowerCase())
            );

            for (const failure of preflightResult.failures) {
              const provider = failure.provider.toLowerCase();
              if (!alreadyMissingProviders.has(provider)) {
                logger.info(`[MissingItems] ⚠️ Preflight found missing credential not caught by discovery: ${provider}`);
                alreadyMissingProviders.add(provider);
                missingItems.credentials.push({
                  provider,
                  type: 'oauth',
                  nodes: [failure.nodeId],
                  fields: [],
                  displayName: providerDisplayName(provider),
                  vaultKey: provider,
                  satisfied: false,
                });
              }
            }

            // Rebuild display summary
            const missingCount = missingItems.credentials.filter((c) => c.satisfied === false).length;
            if (missingItems.display) {
              missingItems.display.summary.missingCredentialCount = missingCount;
            }
          }
        }
      } catch (preflightErr) {
        // Non-fatal — return whatever discoverCredentials found
        logger.warn('[MissingItems] Preflight check failed (non-fatal):', preflightErr);
      }
    }

    return res.json({
      success: true,
      workflowId,
      ...missingItems,
    });
  } catch (error: any) {
    logger.error('[MissingItems] Error:', error);
    return res.status(500).json({
      error: 'Failed to get missing items',
      message: error.message || 'Unknown error',
    });
  }
}
