/**
 * Save Workflow API
 * 
 * Validates and saves workflows with fail-fast validation.
 * Ensures only executable workflows are saved.
 */

import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { validateWorkflowForSave, normalizeWorkflowForSave } from '../core/validation/workflow-save-validator';
import { ErrorCode } from '../core/utils/error-codes';
import type { WorkflowBuildManifestV1 } from '../core/types/workflow-build-manifest';
import { buildSyncedGraphPayload } from './workflow-graph-state';
import { subscriptionService } from '../services/subscription-service';
import { geminiWalletService } from '../services/ai/gemini-wallet-service';
import {
  shouldUseWorkflowCrudService,
  saveWorkflowRemote,
  isWorkflowCrudLocalWritesDisabled,
} from '../services/workflow-crud-service-client';
import { logger } from '../core/logger';

interface WorkflowNode {
  id: string;
  type: string;
  data: {
    label: string;
    type: string;
    category: string;
    config: Record<string, unknown>;
  };
}

interface WorkflowEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
  targetHandle?: string;
}

/**
 * Save workflow with validation
 * POST /api/save-workflow
 */
export default async function saveWorkflowHandler(req: Request, res: Response) {
  const db = getDbClient();
  
  // Require authenticated user for workflow creation/updates.
  // Do NOT require global Google OAuth at save-time; credentials are node/provider-specific.
  let authenticatedUserId: string;
  try {
    const { requireAuthenticatedUser } = await import('../core/utils/check-google-auth');
    authenticatedUserId = await requireAuthenticatedUser(req);
  } catch (authError: any) {
    return res.status(401).json(authError);
  }

  const { workflowId, name, nodes, edges, user_id } = req.body;
  const metadata = (req.body as { metadata?: { buildManifest?: WorkflowBuildManifestV1; freezeBoundary?: { frozen?: boolean } } })
    .metadata;

  if (!name) {
    return res.status(400).json({
      code: ErrorCode.INVALID_INPUT,
      error: 'Missing required field',
      message: 'Workflow name is required',
    });
  }

  if (!nodes || !Array.isArray(nodes)) {
    return res.status(400).json({
      code: ErrorCode.INVALID_INPUT,
      error: 'Invalid workflow structure',
      message: 'Nodes must be an array',
    });
  }

  if (!edges || !Array.isArray(edges)) {
    return res.status(400).json({
      code: ErrorCode.INVALID_INPUT,
      error: 'Invalid workflow structure',
      message: 'Edges must be an array',
    });
  }

  try {
    // ✅ DEBUG: Log incoming workflow structure
    // Use category-based trigger detection to recognize ALL nodes from triggers category
    const isTrigger = (n: any) => {
      const category = n.data?.category || '';
      const nodeType = n.data?.type || n.type || '';
      return category.toLowerCase() === 'triggers' || 
             category.toLowerCase() === 'trigger' ||
             nodeType.includes('trigger') ||
             ['manual_trigger', 'webhook', 'schedule', 'interval', 'form', 'chat_trigger', 'form_trigger'].includes(nodeType);
    };
    
    logger.info('[SaveWorkflow] 📥 Received workflow:', {
      workflowId,
      name,
      nodeCount: nodes.length,
      edgeCount: edges.length,
      triggerNodes: nodes.filter(isTrigger).map(n => ({
        id: n.id,
        type: n.data?.type || n.type,
        category: n.data?.category,
        label: n.data?.label,
      })),
    });
    
    // 1. Normalize workflow (apply migrations)
    const normalized = normalizeWorkflowForSave(nodes, edges);
    
    // ✅ DEBUG: Log normalization results
    logger.info('[SaveWorkflow] 🔄 Normalized workflow:', {
      originalNodeCount: nodes.length,
      normalizedNodeCount: normalized.nodes.length,
      originalEdgeCount: edges.length,
      normalizedEdgeCount: normalized.edges.length,
      migrationsApplied: normalized.migrationsApplied,
      normalizedTriggerNodes: normalized.nodes.filter(isTrigger).map(n => ({
        id: n.id,
        type: n.data?.type || n.type,
        category: n.data?.category,
        label: n.data?.label,
      })),
    });
    
    if (normalized.migrationsApplied.length > 0) {
      logger.info('[SaveWorkflow] ✅ Applied migrations:', normalized.migrationsApplied);
    }

    // 2. Validate workflow (fail-fast)
    const validation = validateWorkflowForSave(normalized.nodes, normalized.edges, {
      buildManifest: metadata?.buildManifest as WorkflowBuildManifestV1 | undefined,
      freezeBoundary: metadata?.freezeBoundary,
    });

    if (!validation.canSave) {
      return res.status(400).json({
        code: ErrorCode.INVALID_INPUT,
        error: 'Workflow validation failed',
        message: `Cannot save workflow: ${validation.errors.join('; ')}`,
        details: {
          errors: validation.errors,
          warnings: validation.warnings,
          migrationsApplied: normalized.migrationsApplied,
        },
        hint: 'Please fix the validation errors before saving.',
      });
    }

    // 3. Log warnings (non-blocking)
    if (validation.warnings.length > 0) {
      logger.warn('[SaveWorkflow] Validation warnings:', validation.warnings);
    }

    // 3a. Workflow CRUD service canary — delegate if enabled for this userId.
    // Fires AFTER validation so the service receives pre-validated, pre-normalized data.
    if (shouldUseWorkflowCrudService(authenticatedUserId)) {
      try {
        const remote = await saveWorkflowRemote(authenticatedUserId, {
          workflowId,
          name,
          nodes: normalized.nodes,
          edges: normalized.edges,
          settings: req.body.settings ?? {},
          metadata: (req.body.metadata as Record<string, unknown> | undefined) ?? {},
        });
        if (remote) {
          logger.info(`[SaveWorkflow] ✅ Delegated to workflow-crud-service (workflowId=${remote.workflowId})`);
          return res.json({ ...remote, source: 'workflow-crud-service' });
        }
        logger.warn('[SaveWorkflow] workflow-crud-service returned null — falling back to local path');
      } catch (delegateErr) {
        logger.warn('[SaveWorkflow] workflow-crud-service delegation error — falling back:', delegateErr);
      }
      // LOCAL_WRITES_DISABLED gate — no double-write during retirement soak.
      if (isWorkflowCrudLocalWritesDisabled()) {
        return res.status(503).json({
          error: 'Workflow CRUD service unavailable',
          code: 'WORKFLOW_CRUD_SERVICE_UNAVAILABLE',
          message: 'Local writes are disabled; workflow-crud-service did not respond.',
        });
      }
    }

    // 4. Previous row (updates only): versioning + merge metadata so empty/partial client metadata does not wipe originalUserPrompt / buildAiUsage
    let previousWorkflow: Record<string, unknown> | null = null;
    if (workflowId) {
      try {
        const { data } = await db
          .from('workflows')
          .select('*')
          .eq('id', workflowId)
          .single();
        previousWorkflow = data as Record<string, unknown> | null;
      } catch (versionError) {
        logger.warn('[SaveWorkflow] Could not load previous workflow for update:', versionError);
      }
    }

    const incomingMeta = req.body.metadata;
    const incomingPlain =
      incomingMeta && typeof incomingMeta === 'object' && !Array.isArray(incomingMeta)
        ? (incomingMeta as Record<string, unknown>)
        : {};

    const mergedMetadata: Record<string, unknown> = previousWorkflow
      ? { ...((previousWorkflow.metadata || {}) as Record<string, unknown>), ...incomingPlain }
      : { ...incomingPlain };

    // 5. Prepare workflow data
    // setup_completed, setup_stage, setup_completed_at are ONLY written on INSERT (new manual workflow).
    // For UPDATE, these fields are preserved from the DB — the sole authority to change them
    // is POST /api/workflows/:id/commit-setup (workflow-setup-lifecycle.ts).
    // This prevents save-workflow from making half-configured wizard workflows visible in the UI.
    const baseWorkflowData: Record<string, unknown> = {
      name,
      nodes: normalized.nodes,
      edges: normalized.edges,
      updated_at: new Date().toISOString(),
      schema_version: 2,
      settings: (req.body.settings || {}),
      graph: buildSyncedGraphPayload(normalized.nodes, normalized.edges, mergedMetadata),
      metadata: mergedMetadata,
      user_id: authenticatedUserId,
    };

    // New manually-built workflows (no workflowId) are immediately visible.
    // Wizard-created workflows earn visibility only via commit-setup.
    const workflowData: Record<string, unknown> = workflowId
      ? baseWorkflowData
      : {
          ...baseWorkflowData,
          setup_completed: true,
          setup_stage: 'complete',
          setup_completed_at: new Date().toISOString(),
        };

    // 6. Save or update workflow
    let savedWorkflow;
    let previousDefinition: any = null;

    if (workflowId) {
      if (previousWorkflow) {
        previousDefinition = {
          name: previousWorkflow.name,
          nodes: previousWorkflow.nodes || [],
          edges: previousWorkflow.edges || [],
          status: previousWorkflow.status,
          phase: previousWorkflow.phase,
          settings: previousWorkflow.settings || {},
          graph: previousWorkflow.graph || {},
          metadata: previousWorkflow.metadata || {},
        };
      }

      // Update existing workflow
      const { data, error } = await db
        .from('workflows')
        .update(workflowData)
        .eq('id', workflowId)
        .select()
        .single();

      if (error) {
        logger.error('[SaveWorkflow] Update error:', error);
        return res.status(500).json({
          code: ErrorCode.INTERNAL_ERROR,
          error: 'Failed to update workflow',
          message: error.message,
        });
      }

      savedWorkflow = data;
    } else {
      const walletActive = await geminiWalletService.isActive(authenticatedUserId).catch(() => false);
      await subscriptionService.ensureFreeSubscription(authenticatedUserId);
      const canCreateWorkflow = walletActive || await subscriptionService.canCreateWorkflow(authenticatedUserId);
      if (!canCreateWorkflow) {
        const usage = await subscriptionService.getSubscriptionUsage(authenticatedUserId);
        return res.status(403).json({
          code: 'WORKFLOW_LIMIT_EXCEEDED',
          error: 'Workflow Limit Exceeded',
          message: `You've reached your workflow limit (${usage.workflowLimit}). Upgrade your plan to create more workflows.`,
          details: {
            workflowsUsed: usage.workflowsUsed,
            workflowLimit: usage.workflowLimit,
            remainingWorkflows: usage.remainingWorkflows,
            upgradeUrl: '/subscriptions',
          },
        });
      }

      // Create new workflow
      const { data, error } = await db
        .from('workflows')
        .insert({ ...workflowData, quota_source: walletActive ? 'gemini_wallet' : 'subscription' })
        .select()
        .single();

      if (error) {
        logger.error('[SaveWorkflow] Insert error:', error);
        return res.status(500).json({
          code: ErrorCode.INTERNAL_ERROR,
          error: 'Failed to create workflow',
          message: error.message,
        });
      }

      savedWorkflow = data;
      if (!walletActive) {
        await subscriptionService.incrementWorkflowCount(authenticatedUserId);
      }
    }

    // 🆕 VERSIONING: Create version after successful save
    try {
      const { getWorkflowVersionManager } = await import('../services/workflow-versioning');
      const versionManager = getWorkflowVersionManager();

      // Extract user ID from request
      let createdBy: string | undefined;
      try {
        const authHeader = req.headers.authorization;
        if (authHeader && authHeader.startsWith('Bearer ')) {
          const token = authHeader.replace('Bearer ', '').trim();
          const { data: { user } } = await db.auth.getUser(token);
          if (user) {
            createdBy = user.id;
          }
        }
      } catch (authError) {
        // Non-critical - continue without user ID
      }

      const currentDefinition = {
        name: savedWorkflow.name,
        nodes: savedWorkflow.nodes || [],
        edges: savedWorkflow.edges || [],
        status: savedWorkflow.status,
        phase: savedWorkflow.phase,
        settings: savedWorkflow.settings || {},
        graph: savedWorkflow.graph || {},
        metadata: savedWorkflow.metadata || {},
      };

      await versionManager.createVersion(
        savedWorkflow.id,
        currentDefinition,
        previousDefinition,
        createdBy,
        {
          description: workflowId ? 'Workflow updated' : 'Workflow created',
        }
      );
    } catch (versionError) {
      // Versioning is non-critical - log but don't fail the save
      logger.warn('[SaveWorkflow] Versioning failed (non-critical):', versionError);
    }

    // ✅ CRITICAL: Invalidate any workflow caches after save
    try {
      const { getMemoryManager } = await import('../memory');
      const memoryManager = getMemoryManager();
      // MemoryManager uses CacheManager internally - invalidate if available
      if (memoryManager && (memoryManager as any).cache) {
        (memoryManager as any).cache.invalidateWorkflow(savedWorkflow.id);
        logger.info(`[SaveWorkflow] ✅ Invalidated cache for workflow ${savedWorkflow.id}`);
      }
    } catch (cacheError) {
      // Cache invalidation is non-critical - log but don't fail
      logger.warn('[SaveWorkflow] Cache invalidation failed (non-fatal):', cacheError);
    }

    // ✅ DEBUG: Log graph hash for verification
    const graphHash = JSON.stringify({ 
      nodes: savedWorkflow.nodes?.map((n: any) => ({ id: n.id, type: n.data?.type || n.type, config: n.data?.config })), 
      edges: savedWorkflow.edges?.map((e: any) => ({ source: e.source, target: e.target }))
    });
    const hash = require('crypto').createHash('md5').update(graphHash).digest('hex').substring(0, 8);
    logger.info(`[SaveWorkflow] 💾 Workflow saved - Graph hash: ${hash}, Updated at: ${savedWorkflow.updated_at}`);

    // 6. Return success response
    return res.json({
      success: true,
      workflowId: savedWorkflow.id,
      workflow: savedWorkflow,
      validation: {
        valid: validation.valid,
        warnings: validation.warnings,
        migrationsApplied: normalized.migrationsApplied,
      },
    });
  } catch (error: any) {
    logger.error('[SaveWorkflow] Unexpected error:', error);
    return res.status(500).json({
      code: ErrorCode.INTERNAL_ERROR,
      error: 'Failed to save workflow',
      message: error.message || 'Unknown error',
    });
  }
}
