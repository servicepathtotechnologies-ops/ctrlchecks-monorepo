/**
 * Capability Selection — Phase 1: Analyze
 *
 * POST /api/capability-selection/analyze
 *
 * Runs the new capability pipeline:
 *   1. runIntentAnalysis  → UseCaseUnit[] (one per discrete task)
 *   2. runCapabilityGrouping → CapabilityContainer[] (candidate nodes per unit)
 *
 * Returns CapabilityContainers to the frontend. No workflow graph is constructed here.
 *
 * Requirements: 2.8, 7.1, 7.3
 */

import { Response } from 'express';
import { randomUUID } from 'crypto';
import { createHash } from 'crypto';
import { buildNodeCatalogText } from '../../services/ai/node-catalog-builder';
import { runIntentAnalysis } from '../../services/ai/stages/capability-intent-analyzer';
import { runCapabilityGrouping } from '../../services/ai/stages/capability-grouper-stage';
import type { AuthenticatedRequest } from '../../core/middleware/subscription-auth';
import { logger } from '../../core/logger';

export default async function analyzeCapabilitySelection(req: AuthenticatedRequest, res: Response): Promise<void> {
  const startedAt = Date.now();

  try {
    const body = req.body as Record<string, unknown>;
    const prompt = typeof body.prompt === 'string' ? body.prompt.trim() : '';
    const bodyUserId = typeof body.userId === 'string' ? body.userId.trim() : '';
    const userId = req.user?.id || bodyUserId;
    const correlationId =
      typeof body.correlationId === 'string' && body.correlationId.trim()
        ? body.correlationId.trim()
        : randomUUID();

    if (!prompt) {
      res.status(400).json({ ok: false, code: 'MISSING_PROMPT', message: 'prompt is required' });
      return;
    }
    if (!userId) {
      res.status(401).json({ ok: false, code: 'AUTH_REQUIRED', message: 'Authenticated user is required' });
      return;
    }

    const nodeCatalog = buildNodeCatalogText();

    // Stage 1: Parse user intent into ordered use-case units
    const intentResult = await runIntentAnalysis(prompt, nodeCatalog, correlationId);
    if (!intentResult.ok) {
      res.status(422).json({
        ok: false,
        code: intentResult.code,
        message: intentResult.message,
      });
      return;
    }

    // Stage 2: For each unit, find semantically equivalent candidate nodes
    const groupingResult = await runCapabilityGrouping(intentResult.units, nodeCatalog, userId, correlationId);
    if (!groupingResult.ok) {
      res.status(422).json({
        ok: false,
        code: groupingResult.code,
        message: groupingResult.message,
      });
      return;
    }

    const durationMs = Date.now() - startedAt;

    res.status(200).json({
      correlationId,
      containers: groupingResult.containers,
      promptHash: createHash('sha256').update(prompt).digest('hex'),
      durationMs,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    logger.error('[CapabilitySelection/analyze] Unhandled error:', message);
    res.status(500).json({ ok: false, code: 'INTERNAL_ERROR', message });
  }
}
