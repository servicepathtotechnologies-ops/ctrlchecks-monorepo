/**
 * Attach Node Inputs API Endpoint
 * 
 * This endpoint is called AFTER workflow generation to inject node configuration inputs
 * (templates, channels, recipients, prompts, etc.) into nodes.
 * 
 * Flow:
 * 1. User submits prompt
 * 2. Backend generates workflow graph
 * 3. Backend returns graph + required inputs + required credentials
 * 4. Frontend shows unified configuration modal
 * 5. User submits inputs �' THIS ENDPOINT
 * 6. Backend injects inputs into nodes
 * 7. Frontend calls attach-credentials
 * 8. Auto-run workflow
 *
 * **Post-freeze (topology-only):** When `metadata.freezeBoundary.frozen` is true, structural graph
 * mutations are still blocked via topology fingerprint checks. Non-credential config values remain
 * writable through this endpoint so AI-built and user-edited fields persist (no protected-config 409).
 *
 * Field-plane aligned keys (see ctrl_checks `wizard-types.ts`):
 * - `mode_<nodeId>_<fieldName>` �' `data.config._fillMode[fieldName]`
 * - `unlock_<nodeId>_<fieldName>` �' `data.config._ownershipUnlock[fieldName]` (registry unlockable credential fields only)
 * - Prefixed comprehensive ids: `cred_`, `input_`, `config_`, `resource_`, `op_` + `<nodeId>_<fieldName>`
 */

import { Request, Response } from 'express';
import { createHash } from 'crypto';
import { getDbClient } from '../core/database/aws-db-client';
import { workflowLifecycleManager } from '../services/workflow-lifecycle-manager';
import { workflowValidator } from '../services/ai/workflow-validator';
import { nodeLibrary } from '../services/nodes/node-library';
import { unifiedNormalizeNodeType, unifiedNormalizeNodeTypeString } from '../core/utils/unified-node-type-normalizer';
import { connectorRegistry } from '../services/connectors/connector-registry';
import { normalizeWorkflowGraph, validateNormalizedGraph } from '../core/utils/workflow-graph-normalizer';
import {
  diffWorkflowTopology,
  fingerprintWorkflowProtectedConfig,
  fingerprintWorkflowTopology,
  type WorkflowProtectedConfigFingerprint,
  type WorkflowTopologyFingerprint,
} from '../core/utils/workflow-topology-fingerprint';
import { executionOrderManager } from '../core/orchestration/execution-order-manager';
import { ErrorCode, createError } from '../core/utils/error-codes';
import { unifiedNodeRegistry } from '../core/registry/unified-node-registry';
import {
  coerceFieldFillModeByPolicy,
  resolveEffectiveFieldFillMode,
  isMeaningfulStaticValue,
} from '../core/utils/fill-mode-resolver';
import { unifiedGraphOrchestrator } from '../core/orchestration/unified-graph-orchestrator';
import { validateStructuralReadiness } from '../core/validation/workflow-save-validator';
import { getStructuralDiagnostics, materializeStructuralFields } from '../services/ai/structure-materializer';
import { applyStructuralIntentAlignment } from '../services/ai/intent-structural-projection';
import { hydrateRequiredConfigFromRegistryDefaults } from '../core/validation/workflow-config-hydrator';
import { isCredentialOwnership, isStructuralOwnership } from '../core/utils/field-ownership';
import {
  isConfigMetaKey,
  resolveAliasTargetFieldName,
  shouldPreserveExistingBuildtimeValue,
} from '../core/utils/attach-inputs-merge-guard';
import type { NodeInputField, NodeInputSchema } from '../core/types/unified-node-contract';
import { logger } from '../core/logger';

/**
 * Credential-class fields are usually injected via attach-credentials / vault.
 * When the user chooses "You" (manual_static), build-time AI once, or unlocks an unlockable
 * credential field, values must still persist on the node for the Properties panel.
 */
function shouldApplyCredentialOwnedFieldViaAttachInputs(
  fieldName: string,
  fieldDef: NodeInputField | undefined,
  config: Record<string, any>,
  inputSchema: NodeInputSchema | undefined,
  rawValue: unknown
): boolean {
  if (!fieldDef || !isCredentialOwnership(fieldName, fieldDef)) {
    return true;
  }
  if (!isMeaningfulStaticValue(rawValue)) {
    return false;
  }
  const mode = resolveEffectiveFieldFillMode(fieldName, inputSchema, config);
  if (mode === 'manual_static' || mode === 'buildtime_ai_once') {
    return true;
  }
  const unlockable = fieldDef.credentialTogglePolicy === 'unlockable';
  const unlocked = config?._ownershipUnlock?.[fieldName] === true;
  return unlockable && unlocked;
}
import {
  runWithBuildUsageTracking,
  snapshotBuildAiUsage,
  mergePersistedBuildAiUsage,
} from '../core/ai/build-usage-context';
import { buildPositionSnapshotFromNodes, mergePreservedNodePositions } from '../core/utils/workflow-node-position';
import { resolveWorkflowGraphState } from './workflow-graph-state';

function stableStringifyForHash(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringifyForHash(item)).join(',')}]`;
  }
  return `{${Object.keys(value as Record<string, unknown>)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringifyForHash((value as Record<string, unknown>)[key])}`)
    .join(',')}}`;
}

function hashAttachInputsPayload(params: {
  workflowId: string;
  inputs: unknown;
  originalUserPrompt?: string;
  fieldOwnershipOverrides?: unknown;
  fieldGuidanceApplied?: unknown;
  fieldGuidanceBuildAiUsage?: unknown;
}): string {
  return createHash('sha256')
    .update(stableStringifyForHash(params))
    .digest('hex');
}

export function collectEffectiveFillModesForWizard(nodes: any[]): Record<string, string> {
  return (Array.isArray(nodes) ? nodes : []).reduce((acc: Record<string, string>, node: any) => {
    const perField = (node?.data?.config?._fillMode || {}) as Record<string, unknown>;
    for (const [fieldName, mode] of Object.entries(perField)) {
      if (
        mode === 'manual_static' ||
        mode === 'runtime_ai' ||
        mode === 'buildtime_ai_once'
      ) {
        acc[`mode_${node.id}_${fieldName}`] = mode;
      }
    }
    return acc;
  }, {});
}

/**
 * Round-trip for wizard: `unlock_<nodeId>_<field>=true|false` from saved node configs.
 */
export function collectOwnershipUnlockFlagsForWizard(nodes: any[]): Record<string, 'true' | 'false'> {
  const acc: Record<string, 'true' | 'false'> = {};
  for (const node of Array.isArray(nodes) ? nodes : []) {
    const id = node?.id;
    const u = node?.data?.config?._ownershipUnlock;
    if (!id || !u || typeof u !== 'object') continue;
    for (const [fieldName, v] of Object.entries(u as Record<string, unknown>)) {
      if (v === true) {
        acc[`unlock_${id}_${fieldName}`] = 'true';
      }
    }
  }
  return acc;
}

/**
 * Apply `unlock_<nodeId>_<field>` keys from the request into `config._ownershipUnlock`.
 * Exported for unit tests; kept in sync with the attach-inputs handler loop.
 */
export function mergeOwnershipUnlockInputsForNode(
  cleanInputs: Record<string, any>,
  node: { id: string },
  nodeType: string,
  config: Record<string, any>,
  validFieldNames: Set<string>
): boolean {
  const unifiedDefForNode = unifiedNodeRegistry.get(nodeType);
  let updated = false;
  for (const [unlockKey, rawUnlock] of Object.entries(cleanInputs)) {
    if (!unlockKey.startsWith('unlock_')) continue;
    const afterUnlockPrefix = unlockKey.substring('unlock_'.length);
    const unlockNodePrefix = `${node.id}_`;
    if (!afterUnlockPrefix.startsWith(unlockNodePrefix)) continue;
    const unlockFieldName = afterUnlockPrefix.substring(unlockNodePrefix.length);
    if (!validFieldNames.has(unlockFieldName)) {
      logger.warn(`[AttachInputs] Unknown unlock field "${unlockFieldName}" for node ${node.id} (${nodeType})`);
      continue;
    }
    // Persist unlock flags as explicit ownership metadata from UI, even when registry contracts
    // evolve; downstream credential policy checks still enforce actual writable fields.
    if (!config._ownershipUnlock || typeof config._ownershipUnlock !== 'object') {
      (config as any)._ownershipUnlock = {};
    }
    const truthy =
      rawUnlock === true ||
      rawUnlock === 1 ||
      String(rawUnlock).trim() === 'true' ||
      String(rawUnlock).trim() === '1';
    if (truthy) {
      (config as any)._ownershipUnlock[unlockFieldName] = true;
    } else {
      delete (config as any)._ownershipUnlock[unlockFieldName];
    }
    updated = true;
    logger.info(`[AttachInputs] Applied ownership unlock for ${node.id}.${unlockFieldName}: ${truthy}`);
  }
  return updated;
}

export function normalizeSwitchCasesInput(raw: unknown): { value: Array<{ value: string; label?: string }>; valid: boolean } {
  let candidate: unknown = raw;
  if (typeof candidate === 'string') {
    const trimmed = candidate.trim();
    if (!trimmed) return { value: [], valid: false };
    try {
      candidate = JSON.parse(trimmed);
    } catch {
      return { value: [], valid: false };
    }
  }
  if (!Array.isArray(candidate)) return { value: [], valid: false };

  const seen = new Set<string>();
  const normalized: Array<{ value: string; label?: string }> = [];
  for (const item of candidate) {
    const rawValue =
      typeof item === 'string' ? item : item && typeof item === 'object' && (item as any).value != null ? String((item as any).value) : '';
    const value = String(rawValue || '').trim();
    if (!value || seen.has(value)) continue;
    seen.add(value);
    const label =
      item && typeof item === 'object' && typeof (item as any).label === 'string'
        ? String((item as any).label).trim() || undefined
        : undefined;
    normalized.push({ value, ...(label ? { label } : {}) });
  }

  return { value: normalized, valid: normalized.length > 0 };
}

/** Module-level in-flight deduplication map keyed by workflowId. */
const attachInputsInFlight = new Map<string, Promise<unknown>>();

/**
 * Core pipeline: runs the full attach-inputs validation + credential discovery +
 * topology fingerprint pipeline. Returns `{ statusCode, body }` for all paths
 * (success and error). Does NOT call res.json — the caller does that.
 */
async function runAttachInputsPipeline(req: Request, res: Response): Promise<{ statusCode: number; body: unknown }> {
  return runWithBuildUsageTracking(async () => {
  try {
    // ✅ CRITICAL: Get workflowId from URL params (not body)
    const workflowId = req.params.workflowId || req.body.workflowId;
    const {
      inputs,
      originalUserPrompt: originalUserPromptFromBody,
      fieldOwnershipOverrides,
      fieldGuidanceApplied,
      fieldGuidanceBuildAiUsage,
    } = req.body;
    const appliedFieldGuidanceExamples = Array.isArray(fieldGuidanceApplied)
      ? fieldGuidanceApplied
          .map((entry: any) => ({
            nodeId: String(entry?.nodeId || '').trim(),
            fieldName: String(entry?.fieldName || '').trim(),
            mode: String(entry?.mode || 'buildtime_ai_once').trim(),
            source: String(entry?.source || 'ai_field_guidance').trim(),
          }))
          .filter((entry) => entry.nodeId && entry.fieldName)
      : [];

    const trimmedOriginalFromRequest =
      typeof originalUserPromptFromBody === 'string' ? originalUserPromptFromBody.trim() : '';

    // ✅ CRITICAL: Log request for debugging
    logger.info('[AttachInputs] Request received:', {
      workflowId,
      inputsKeys: inputs ? Object.keys(inputs) : [],
      inputsCount: inputs ? Object.keys(inputs).length : 0,
      hasOriginalUserPromptHint: trimmedOriginalFromRequest.length > 0,
    });

    if (!workflowId) {
      logger.error('[AttachInputs] Missing workflowId in params and body');
      return { statusCode: 400, body: ({
        error: 'workflowId is required',
        details: 'workflowId must be provided in URL path or request body',
      }) };
    }

    if (!inputs || typeof inputs !== 'object') {
      logger.error('[AttachInputs] Invalid inputs:', typeof inputs, inputs);
      return { statusCode: 400, body: (
        createError(
          ErrorCode.INVALID_INPUT_FORMAT,
          'inputs object is required',
          { 
            received: typeof inputs,
            expected: 'object',
            workflowId,
          }
        )
      ) };
    }

    // ✅ CRITICAL: Strip any credential fields from inputs
    // ✅ COMPREHENSIVE: BUT allow question IDs that wrap nodeId + fieldName
    // Supported prefixes:
    // - input_ (current unified wizard format)
    // - cred_ / op_ / config_ / resource_ (comprehensive question IDs)
    // - ownership_ (field-ownership wizard questions — recipientSource, recipientEmails, etc.)
    const sanitizedInputs: Record<string, any> = {};
    for (const [key, value] of Object.entries(inputs)) {
      // ✅ COMPREHENSIVE: Allow comprehensive question IDs - these are handled specially
      const isComprehensiveQuestionId = 
        key.startsWith('input_') ||
        key.startsWith('cred_') ||
        key.startsWith('op_') ||
        key.startsWith('config_') ||
        key.startsWith('resource_') ||
        key.startsWith('ownership_') ||
        key.startsWith('mode_') ||
        key.startsWith('unlock_');
      
      if (isComprehensiveQuestionId) {
        // Allow comprehensive question IDs - they will be processed correctly later
        sanitizedInputs[key] = value;
        continue;
      }
      
      // Reject credential-shaped keys (but NOT comprehensive question IDs)
      const keyLower = key.toLowerCase();
      const isTokenButNotCredentialConfig =
        // Allow common non-credential config fields like maxTokens / tokenLimit
        keyLower.includes('maxtokens') ||
        keyLower.includes('tokenlimit') ||
        keyLower.includes('token_limit') ||
        keyLower.endsWith('_maxtokens') ||
        keyLower.endsWith('_tokenlimit') ||
        keyLower.endsWith('_token_limit');

      const isLegacyNodeScopedInput = /^[^_]+_[^_].+/.test(key);
      const isCredentialKey = 
        keyLower.includes('oauth') ||
        keyLower.includes('client_id') ||
        keyLower.includes('client_secret') ||
        (keyLower.includes('token') && !isTokenButNotCredentialConfig) ||
        keyLower.includes('secret') ||
        keyLower.includes('credential');
      
      if (isCredentialKey && !isLegacyNodeScopedInput) {
        logger.warn(`[AttachInputs] Rejected credential key "${key}" from inputs`);
        continue;
      }
      
      sanitizedInputs[key] = value;
    }
    
    // Use sanitized inputs
    const cleanInputs = Object.keys(sanitizedInputs).length > 0 ? sanitizedInputs : inputs;

    /** Keys that look like bare node IDs but are not nested `{ field: value }` objects — worker cannot map fields. */
    const invalidBareNodeIdInputKeys: string[] = [];
    const nodeIdLike =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    for (const [k, v] of Object.entries(cleanInputs)) {
      if (nodeIdLike.test(k) && typeof v !== 'object') {
        invalidBareNodeIdInputKeys.push(k);
      }
    }
    if (invalidBareNodeIdInputKeys.length > 0) {
      logger.warn('[AttachInputs] Ignored invalid attach-input keys (expected nested object per node):', invalidBareNodeIdInputKeys);
    }

    // Get current user
    const db = getDbClient();
    const authHeader = req.headers.authorization;
    let userId: string | undefined;

    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.replace('Bearer ', '').trim();
      if (token) {
        try {
          const { data: { user }, error: authError } = await db.auth.getUser(token);
          if (!authError && user) {
            userId = user.id;
          }
        } catch (authErr) {
          logger.warn('[AttachInputs] Auth error (non-fatal):', authErr);
        }
      }
    }

    // Fetch workflow
    const { data: workflow, error: workflowError } = await db
      .from('workflows')
      .select('*')
      .eq('id', workflowId)
      .single();

    if (workflowError || !workflow) {
      return { statusCode: 404, body: (
        createError(
          ErrorCode.WORKFLOW_NOT_FOUND,
          'Workflow not found',
          { workflowId, error: workflowError?.message }
        )
      ) };
    }

    const attachPayloadHash = hashAttachInputsPayload({
      workflowId,
      inputs: cleanInputs,
      originalUserPrompt: trimmedOriginalFromRequest,
      fieldOwnershipOverrides,
      fieldGuidanceApplied: appliedFieldGuidanceExamples,
      fieldGuidanceBuildAiUsage,
    });
    const currentGraphForIdempotency = resolveWorkflowGraphState(workflow as any);
    const currentTopologyForIdempotency = fingerprintWorkflowTopology(
      currentGraphForIdempotency.nodes || [],
      currentGraphForIdempotency.edges || []
    ).fingerprint;
    const previousAttach = (workflow as any)?.metadata?.lastAttachInputs;
    if (
      previousAttach &&
      typeof previousAttach === 'object' &&
      previousAttach.payloadHash === attachPayloadHash &&
      previousAttach.topologyFingerprint === currentTopologyForIdempotency
    ) {
      logger.info('[AttachInputs] Idempotent duplicate payload, returning cached success:', {
        workflowId,
        payloadHash: attachPayloadHash.slice(0, 12),
      });
      return { statusCode: 200, body: ({
        success: true,
        idempotent: true,
        workflowId,
        status: workflow.status,
        phase: workflow.phase,
        workflow,
      }) };
    }

    if ((workflow as any).active_execution_id || ['executing', 'running'].includes(String(workflow.phase || workflow.status || '').toLowerCase())) {
      return { statusCode: 409, body: (
        createError(
          ErrorCode.PHASE_LOCKED,
          'Workflow is currently executing. Cannot attach inputs.',
          {
            workflowId,
            activeExecutionId: (workflow as any).active_execution_id,
            currentPhase: workflow.phase || workflow.status,
          },
          true
        )
      ) };
    }

    const buildManifestFromDb = (workflow as any)?.metadata?.buildManifest;
    if (buildManifestFromDb && typeof buildManifestFromDb === 'object' && buildManifestFromDb.version === 1) {
      const { verifyBuildManifestIntegrity } = await import('../core/utils/workflow-build-manifest-utils');
      if (!verifyBuildManifestIntegrity(buildManifestFromDb)) {
        logger.warn('[AttachInputs] buildManifest integrity hash mismatch; continuing with structural alignment', {
          workflowId,
        });
      }
    }

    // Phase check: only block phases where attaching inputs is genuinely unsafe.
    // Blocklist approach � any unrecognised phase is allowed so new phases never silently break the UI.
    const currentPhase = workflow.phase || workflow.status || 'draft';
    const executingPhases = new Set(['executing', 'running']);
    logger.info(`[AttachInputs] Current workflow phase: "${currentPhase}" (phase=${workflow.phase}, status=${workflow.status})`);

    if (executingPhases.has(currentPhase)) {
      return { statusCode: 409, body: (
        createError(
          ErrorCode.PHASE_LOCKED,
          'Workflow is currently executing. Cannot attach inputs.',
          {
            currentPhase,
            workflowId,
            message: 'Workflow is currently executing. Cannot attach inputs.',
          },
          true // Recoverable - user can refresh
        )
      ) };
    }

    // ✅ ATOMIC PHASE FIX: Do NOT update phase here.
    // Phase is only advanced to 'configuring_inputs' AFTER successful graph normalization below.
    // If normalization fails, the phase must remain unchanged.

    // ✅ CRITICAL: Use centralized graph normalizer
    // Handle both workflow.graph format and direct nodes/edges format
    let normalizedGraph: ReturnType<typeof normalizeWorkflowGraph>;
    /** Fingerprint after topologyPreserve parse — final save must match this. */
    let baselineTopologyFingerprint: WorkflowTopologyFingerprint | null = null;
    let baselineProtectedConfigFingerprint: WorkflowProtectedConfigFingerprint | null = null;
    const freezeBoundary = (workflow as any)?.metadata?.freezeBoundary || null;
    const isPostFreezeReadonly = Boolean(freezeBoundary?.frozen);
    /**
     * Config normalization gate: once the workflow has been delivered to the UI
     * (indicated by buildManifest presence OR freezeBoundary), all node config
     * normalization must stop. Only edge topology fixes are allowed after this point.
     * This preserves AI-built values, user-entered values, and credentials exactly as set.
     */
    const hasBuildManifest = Boolean((workflow as any)?.metadata?.buildManifest);
    const isConfigFrozen = isPostFreezeReadonly || hasBuildManifest;
    /** Snapshot of node positions from DB row before normalization (preserve manual layout on save). */
    let attachInputsPositionSnapshot = new Map<string, { x: number; y: number }>();
    try {
      const resolvedGraphState = resolveWorkflowGraphState(workflow as any);
      if (resolvedGraphState.needsHealing) {
        logger.warn('[AttachInputs] ⚠️ Workflow graph state needed healing:', {
          workflowId,
          source: resolvedGraphState.source,
          inSync: resolvedGraphState.inSync,
          reason: resolvedGraphState.reason,
        });
      }
      const graphToNormalize = {
        nodes: resolvedGraphState.nodes || [],
        edges: resolvedGraphState.edges || [],
      };

      attachInputsPositionSnapshot = buildPositionSnapshotFromNodes(
        Array.isArray(graphToNormalize.nodes) ? graphToNormalize.nodes : []
      );
      
      // Registry-driven: fix AI nodes whose planner config matches communication inputSchema (e.g. Gmail fields on ollama)
      const graphAfterTypeReconcile = unifiedNodeRegistry.reconcileMisroutedAiCommunicationNodes({
        nodes: Array.isArray(graphToNormalize.nodes) ? graphToNormalize.nodes : [],
        edges: Array.isArray(graphToNormalize.edges) ? graphToNormalize.edges : [],
      } as any);

      // ✅ DEBUG: Log node IDs BEFORE any normalization
      const nodeIdsBeforeAnyNormalization = (graphAfterTypeReconcile.nodes || []).map((n: any) => n.id);
      const duplicatesBeforeAny = nodeIdsBeforeAnyNormalization.filter((id: string, idx: number) => 
        nodeIdsBeforeAnyNormalization.indexOf(id) !== idx
      );
      if (duplicatesBeforeAny.length > 0) {
        logger.error('[AttachInputs] 🚨 BEFORE any normalization - Duplicate node IDs from DB/frontend:', {
          workflowId,
          duplicateIds: [...new Set(duplicatesBeforeAny)],
          allNodeIds: nodeIdsBeforeAnyNormalization,
          nodeCount: (graphAfterTypeReconcile.nodes || []).length,
          uniqueNodeCount: new Set(nodeIdsBeforeAnyNormalization).size,
        });
      }
      
      // Topology-preserving parse only (no linearization / trigger stripping / switch reconcile)
      normalizedGraph = normalizeWorkflowGraph(
        {
          nodes: graphAfterTypeReconcile.nodes || [],
          edges: graphAfterTypeReconcile.edges || [],
        },
        { mode: 'topologyPreserve' }
      );
      baselineTopologyFingerprint = fingerprintWorkflowTopology(
        normalizedGraph.nodes,
        normalizedGraph.edges
      );
      baselineProtectedConfigFingerprint = fingerprintWorkflowProtectedConfig(normalizedGraph.nodes);
      logger.info('[AttachInputs] 📌 Baseline topology fingerprint (topologyPreserve):', {
        workflowId,
        fingerprint: baselineTopologyFingerprint.fingerprint,
        nodeCount: baselineTopologyFingerprint.nodeIdsSorted.length,
        edgeCount: baselineTopologyFingerprint.edgeKeysSorted.length,
      });
      if (isPostFreezeReadonly) {
        logger.info('[AttachInputs] Post-freeze readonly mode enabled');
      }
      
      // ✅ DEBUG: Log node IDs AFTER normalization
      const nodeIdsAfterNormalization = normalizedGraph.nodes.map(n => n.id);
      const duplicatesAfter = nodeIdsAfterNormalization.filter((id, idx) => 
        nodeIdsAfterNormalization.indexOf(id) !== idx
      );
      if (duplicatesAfter.length > 0) {
        logger.error('[AttachInputs] 🚨 AFTER normalization - STILL has duplicate node IDs:', {
          workflowId,
          duplicateIds: [...new Set(duplicatesAfter)],
        });
      } else {
        logger.info('[AttachInputs] ✅ After normalization - No duplicate node IDs');
      }
      
      // Validate normalized graph (should pass now since duplicates are removed)
      const validation = validateNormalizedGraph(normalizedGraph);
      if (!validation.valid) {
        logger.error('[AttachInputs] Graph validation failed after normalization:', validation.errors);
        return { statusCode: 400, body: (
          createError(
            ErrorCode.GRAPH_INVALID_STRUCTURE,
            'Workflow graph validation failed',
            {
              errors: validation.errors,
              warnings: validation.warnings,
              workflowId,
            }
          )
        ) };
      }

      // ✅ ATOMIC PHASE FIX: Advance phase ONLY after successful normalization + validation.
      // This ensures a 400 on normalization failure never mutates the phase.
      await db
        .from('workflows')
        .update({
          status: 'active',
          phase: 'configuring_inputs',
          updated_at: new Date().toISOString(),
        })
        .eq('id', workflowId);
      logger.info('[AttachInputs] ✅ Phase advanced to configuring_inputs (normalization succeeded)');
    } catch (error) {
      logger.error('[AttachInputs] Graph normalization failed:', error);
      logger.error('[AttachInputs] Workflow structure:', {
        hasGraph: !!workflow.graph,
        hasNodes: !!workflow.nodes,
        hasEdges: !!workflow.edges,
        graphType: typeof workflow.graph,
        nodesType: typeof workflow.nodes,
        edgesType: typeof workflow.edges,
        nodesIsArray: Array.isArray(workflow.nodes),
        edgesIsArray: Array.isArray(workflow.edges),
        nodesLength: Array.isArray(workflow.nodes) ? workflow.nodes.length : 'N/A',
        edgesLength: Array.isArray(workflow.edges) ? workflow.edges.length : 'N/A',
      });
      return { statusCode: 400, body: (
        createError(
          ErrorCode.GRAPH_PARSE_ERROR,
          'Failed to normalize workflow graph',
          {
            error: error instanceof Error ? error.message : String(error),
            workflowId,
            hint: 'Workflow graph format may be invalid. Ensure workflow has nodes and edges.',
            details: {
              hasGraph: !!workflow.graph,
              hasNodes: !!workflow.nodes,
              hasEdges: !!workflow.edges,
              nodesType: typeof workflow.nodes,
              edgesType: typeof workflow.edges,
            }
          }
        )
      ) };
    }

    const workflowGraph = normalizedGraph;

    // ✅ DEBUG: Log node IDs BEFORE normalization to detect duplicates from frontend
    const nodeIdsBefore = workflowGraph.nodes.map(n => n.id);
    const duplicateIdsBefore = nodeIdsBefore.filter((id, index) => nodeIdsBefore.indexOf(id) !== index);
    if (duplicateIdsBefore.length > 0) {
      logger.error('[AttachInputs] 🚨 BEFORE normalize - Duplicate node IDs detected:', {
        workflowId,
        duplicateIds: [...new Set(duplicateIdsBefore)],
        allNodeIds: nodeIdsBefore,
        nodeCount: workflowGraph.nodes.length,
        uniqueNodeCount: new Set(nodeIdsBefore).size,
      });
    } else {
      logger.info('[AttachInputs] ✅ BEFORE normalize - No duplicate node IDs detected:', {
        workflowId,
        nodeCount: workflowGraph.nodes.length,
        nodeIds: nodeIdsBefore,
      });
    }

    // ✅ CRITICAL: Config-only save normalization (preserve topology)
    const { normalizeWorkflowForSave: normalizeWorkflow } = await import('../core/validation/workflow-save-validator');
    const existingAppliedMigrations: string[] = (workflow as any)?.metadata?.appliedMigrations ?? [];
    const normalizedBeforeClone = normalizeWorkflow(workflowGraph.nodes, workflowGraph.edges, {
      structuralMode: isConfigFrozen ? 'post_freeze_readonly' : 'configOnly',
      alreadyApplied: existingAppliedMigrations,
    });
    
    // ✅ DEBUG: Log node IDs AFTER normalization to verify deduplication
    const nodeIdsAfter = normalizedBeforeClone.nodes.map(n => n.id);
    const duplicateIdsAfter = nodeIdsAfter.filter((id, index) => nodeIdsAfter.indexOf(id) !== index);
    if (duplicateIdsAfter.length > 0) {
      logger.error('[AttachInputs] 🚨 AFTER normalize - STILL has duplicate node IDs:', {
        workflowId,
        duplicateIds: [...new Set(duplicateIdsAfter)],
        allNodeIds: nodeIdsAfter,
      });
    } else {
      logger.info('[AttachInputs] ✅ AFTER normalize - Duplicates removed:', {
        workflowId,
        originalNodeCount: workflowGraph.nodes.length,
        normalizedNodeCount: normalizedBeforeClone.nodes.length,
        removedCount: workflowGraph.nodes.length - normalizedBeforeClone.nodes.length,
      });
    }
    
    if (normalizedBeforeClone.migrationsApplied.length > 0) {
      logger.info('[AttachInputs] 🔄 Applied normalizations before input injection:', normalizedBeforeClone.migrationsApplied);
      logger.info('[AttachInputs] 📊 Normalization stats:', {
        originalNodes: workflowGraph.nodes.length,
        normalizedNodes: normalizedBeforeClone.nodes.length,
        originalEdges: workflowGraph.edges.length,
        normalizedEdges: normalizedBeforeClone.edges.length,
      });
    }
    
    // ✅ CRITICAL: Clone workflow before mutation to ensure immutability
    // This prevents any accidental mutations of the original workflow definition
    const { cloneWorkflowDefinition } = await import('../core/utils/workflow-cloner');
    const clonedWorkflow = cloneWorkflowDefinition(
      normalizedBeforeClone.nodes,
      normalizedBeforeClone.edges,
      workflowId
    );
    
    logger.info('[AttachInputs] ✅ Workflow normalized and cloned before input injection (immutable operation)');

    // Inject inputs into nodes (operating on clone, not original)
    const modeDiagnostics = {
      appliedModes: [] as Array<{ nodeId: string; nodeType: string; fieldName: string; mode: string }>,
      ignoredModes: [] as Array<{ nodeId: string; nodeType: string; fieldName: string; reason: string; value?: string }>,
      unknownModeFields: [] as Array<{ nodeId: string; nodeType: string; fieldName: string }>,
      runtimeOwnedFields: [] as Array<{ nodeId: string; nodeType: string; fieldName: string }>,
      runtimeResolvedFields: [] as Array<{ nodeId: string; nodeType: string; fieldName: string }>,
      runtimeResolutionErrors: [] as Array<{ nodeId: string; nodeType: string; fieldName: string; reason: string }>,
      fallbackApplied: false,
      schemaValidationFailures: [] as Array<{ nodeId: string; nodeType: string; message: string }>,
      canonicalizationIssues: [] as Array<{ input: string; reason: string }>,
      buildtimeMergePreserved: [] as Array<{
        nodeId: string;
        nodeType: string;
        fieldName: string;
        reason: string;
      }>,
    };
    let updatedNodes: any[];
    try {
      updatedNodes = clonedWorkflow.nodes.map((node: any) => {
      const nodeType = unifiedNormalizeNodeType(node);
      const schema = nodeLibrary.getSchema(nodeType);
      
      if (!schema) {
        return node; // Skip nodes without schema
      }

      // ✅ CRITICAL: Idempotent input merging - merge with existing config
      const existingConfig = node.data?.config || {};
      const config = { ...existingConfig };
      let updated = false;
      // Fields explicitly set via config_/op_/ownership_ keys in this request.
      // The legacy-template clearing block must not overwrite values set in the same batch.
      const explicitlySetInBatch = new Set<string>();
      const modeFieldsApplied: string[] = [];
      const modeFieldsUnknown: string[] = [];
      const unifiedDefForNode = unifiedNodeRegistry.get(nodeType);
      const validFieldNames = new Set<string>([
        ...(schema?.configSchema?.required || []),
        ...Object.keys(schema?.configSchema?.optional || {}),
        ...Object.keys(unifiedDefForNode?.inputSchema || {}),
      ]);

      // Registry-driven: any node whose schema defines `cases` (e.g. switch) gets canonical case arrays.
      if (!isPostFreezeReadonly && unifiedDefForNode?.inputSchema?.cases) {
        const normalizedExisting = normalizeSwitchCasesInput((config as any).cases ?? (config as any).rules);
        const canonical = normalizedExisting.valid ? normalizedExisting.value : [];
        if (JSON.stringify((config as any).cases) !== JSON.stringify(canonical)) {
          (config as any).cases = canonical;
          updated = true;
        }
        if (JSON.stringify((config as any).rules) !== JSON.stringify(canonical)) {
          (config as any).rules = canonical;
          updated = true;
        }
      }

      // Field Ownership: unlock_<nodeId>_<fieldName> (before mode_ so one payload can unlock + set fill mode).
      if (mergeOwnershipUnlockInputsForNode(cleanInputs, node, nodeType, config, validFieldNames)) {
        updated = true;
      }

      // ✅ CRITICAL: Validate inputs are NOT credentials
      // OAuth connectors must NEVER accept credential fields via attach-inputs
      const connector = connectorRegistry.getConnectorByNodeType(nodeType);
      if (connector && connector.credentialContract.type === 'oauth') {
        // OAuth connectors should never receive credential fields as inputs
        // They are handled via OAuth button flow
      }

      // Diagnostic: log buildtime_ai_once stamp presence for observability
      const stampedBuildtimeFields = Object.entries(
        (config._fillMode as Record<string, string> | undefined) ?? {}
      )
        .filter(([, m]) => m === 'buildtime_ai_once')
        .map(([f]) => f);
      if (stampedBuildtimeFields.length > 0) {
        logger.info(
          `[AttachInputs] Node ${node.id} (${nodeType}) has ${stampedBuildtimeFields.length} buildtime_ai_once stamps:`,
          stampedBuildtimeFields
        );
      }

      // ✅ CRITICAL: Idempotent input application
      // Input format: { "nodeId_fieldName": "value" } or { "nodeId": { "fieldName": "value" } }
      // ✅ COMPREHENSIVE: Also handle question IDs: { "cred_nodeId_fieldName": "value", "op_nodeId_fieldName": "value", "config_nodeId_fieldName": "value", "resource_nodeId_fieldName": "value" }
      // Process unlock_ / mode_ before field values so credential fields resolve fill mode correctly.
      const cleanInputKeysSorted = Object.keys(cleanInputs).sort((a, b) => {
        const rank = (k: string) => (k.startsWith('unlock_') ? 0 : k.startsWith('mode_') ? 1 : 2);
        const d = rank(a) - rank(b);
        return d !== 0 ? d : a.localeCompare(b);
      });
      for (const key of cleanInputKeysSorted) {
        const rawValue = cleanInputs[key];
        let fieldName: string | null = null;
        
      // Wizard ownership: keys mode_<nodeId>_<fieldName> -> config._fillMode[fieldName] (manual_static vs runtime_ai)
      // ✅ Handle explicit fill mode keys first: mode_<nodeId>_<fieldName>
      if (key.startsWith('mode_')) {
        const afterPrefix = key.substring('mode_'.length);
        const nodeIdPrefix = `${node.id}_`;
        if (afterPrefix.startsWith(nodeIdPrefix)) {
          const modeFieldName = afterPrefix.substring(nodeIdPrefix.length);
          if (!validFieldNames.has(modeFieldName)) {
            modeFieldsUnknown.push(modeFieldName);
            modeDiagnostics.unknownModeFields.push({
              nodeId: node.id,
              nodeType,
              fieldName: modeFieldName,
            });
            logger.warn(`[AttachInputs] Unknown mode field "${modeFieldName}" for node ${node.id} (${nodeType})`);
            continue;
          }
          if (!config._fillMode || typeof config._fillMode !== 'object') {
            (config as any)._fillMode = {};
          }
          const modeValue = typeof rawValue === 'string' ? rawValue.trim() : '';
          if (modeValue === 'manual_static' || modeValue === 'runtime_ai' || modeValue === 'buildtime_ai_once') {
            const modeFieldDef = unifiedDefForNode?.inputSchema?.[modeFieldName];
            const structuralModeGuard =
              modeFieldDef && isStructuralOwnership(modeFieldName, modeFieldDef) && modeValue === 'runtime_ai'
                ? {
                    mode: 'buildtime_ai_once' as const,
                    coerced: true,
                    reason: 'structural_fields_cannot_be_runtime_owned',
                  }
                : null;
            const modePolicy = coerceFieldFillModeByPolicy(
              modeFieldName,
              structuralModeGuard?.mode || modeValue,
              unifiedDefForNode?.inputSchema,
              config as Record<string, any>
            );
            (config as any)._fillMode[modeFieldName] = modePolicy.mode;
            modeFieldsApplied.push(modeFieldName);
            modeDiagnostics.appliedModes.push({
              nodeId: node.id,
              nodeType,
              fieldName: modeFieldName,
              mode: modePolicy.mode,
            });
            if (modePolicy.coerced) {
              modeDiagnostics.ignoredModes.push({
                nodeId: node.id,
                nodeType,
                fieldName: modeFieldName,
                reason: structuralModeGuard?.reason || modePolicy.reason || 'policy_not_allowed',
                value: modeValue,
              });
              logger.warn(
                `[AttachInputs] Coerced disallowed fill mode "${modeValue}" to "${modePolicy.mode}" for ${node.id}.${modeFieldName}`
              );
            }
            if (modePolicy.mode === 'runtime_ai') {
              modeDiagnostics.runtimeOwnedFields.push({
                nodeId: node.id,
                nodeType,
                fieldName: modeFieldName,
              });
              // Clear any stored static value so AI fills it at runtime (req 4.3, 4.4)
              if ((config as any)[modeFieldName] !== undefined) {
                delete (config as any)[modeFieldName];
                updated = true;
              }
            }
            updated = true;
            logger.info(`[AttachInputs] Applied fill mode for ${node.id}.${modeFieldName}: ${modePolicy.mode}`);
          } else {
            modeDiagnostics.ignoredModes.push({
              nodeId: node.id,
              nodeType,
              fieldName: modeFieldName,
              reason: 'invalid_mode_value',
              value: modeValue,
            });
            logger.warn(`[AttachInputs] Ignored invalid fill mode "${modeValue}" for ${node.id}.${modeFieldName}`);
          }
        }
        continue;
      }

      // Migration/safety: clear legacy template-prefilled runtime fields so they
      // are resolved only at execution time.
      const def = unifiedNodeRegistry.get(nodeType);
      if (def?.inputSchema) {
        for (const fieldName of Object.keys(def.inputSchema)) {
          const mode = resolveEffectiveFieldFillMode(fieldName, def.inputSchema, config as Record<string, any>);
          if (mode !== 'runtime_ai') continue;
          // Never clear a value that was explicitly set by this request batch � the
          // user confirmed it in the Field Ownership step and it must reach the workflow.
          if (explicitlySetInBatch.has(fieldName)) continue;
          const current = config[fieldName];
          if (typeof current === 'string' && current.includes('{{')) {
            config[fieldName] = '';
            updated = true;
            logger.info(`[AttachInputs] Cleared legacy template for runtime field ${node.id}.${fieldName}`);
          }
        }
      }

      // ✅ COMPREHENSIVE: Handle question ID formats (input_*, cred_*, op_*, config_*, resource_*)
      // Format: {prefix}_{nodeId}_{fieldName}
      // Example: cred_step_hubspot_1771317308025_authType -> fieldName: authType
      // ✅ CRITICAL: Also handle cases where nodeId in question doesn't match node.id exactly
      let isFromComprehensiveQuestion = false;
      let prefix = '';
      
      if (key.startsWith('input_')) {
        prefix = 'input_';
        isFromComprehensiveQuestion = true;
      } else if (key.startsWith('cred_')) {
        prefix = 'cred_';
        isFromComprehensiveQuestion = true;
      } else if (key.startsWith('op_')) {
        prefix = 'op_';
        isFromComprehensiveQuestion = true;
      } else if (key.startsWith('config_')) {
        prefix = 'config_';
        isFromComprehensiveQuestion = true;
      } else if (key.startsWith('resource_')) {
        prefix = 'resource_';
        isFromComprehensiveQuestion = true;
      } else if (key.startsWith('ownership_')) {
        prefix = 'ownership_';
        isFromComprehensiveQuestion = true;
      }
      
      if (isFromComprehensiveQuestion && prefix) {
        // Remove prefix to get "<nodeId>_<fieldName>"
        const afterPrefix = key.substring(prefix.length);
        
        // Try exact nodeId match first
        const nodeIdPrefix = `${node.id}_`;
        if (afterPrefix.startsWith(nodeIdPrefix)) {
          fieldName = afterPrefix.substring(nodeIdPrefix.length);
          logger.info(`[AttachInputs] Detected comprehensive question ID: ${key} -> fieldName: ${fieldName} (exact nodeId match)`);
        } else {
          // ✅ SECURITY/INTEGRITY:
          // Always require exact nodeId match for prefixed keys.
          // Flexible field extraction can leak values across nodes (e.g., spreadsheetId applied to gmail).
          fieldName = null;
        }
      }
      // ✅ LEGACY: Handle nodeId_fieldName format
      else if (key.startsWith(`${node.id}_`)) {
        fieldName = key.substring(node.id.length + 1);
      }
        
        // Check if this input is for this node
        if (fieldName) {
          if (fieldName && schema.configSchema) {
            // ✅ CRITICAL: Handle authType selection - don't apply it to config, it's just a selection
            const fieldNameLower = fieldName.toLowerCase();
            if (fieldNameLower === 'authtype' || fieldName === 'authType') {
              // Store authType selection but don't apply it directly to config
              // The actual credential value will be applied based on the selected type
              logger.info(`[AttachInputs] AuthType selected: ${rawValue} for node ${node.id} (${nodeType})`);
              // Don't apply authType to config - it's just a selection indicator
              continue;
            }
            
            const fieldDef = unifiedDefForNode?.inputSchema?.[fieldName];
            if (fieldDef && isCredentialOwnership(fieldName, fieldDef)) {
              if (
                !shouldApplyCredentialOwnedFieldViaAttachInputs(
                  fieldName,
                  fieldDef,
                  config,
                  unifiedDefForNode?.inputSchema,
                  rawValue
                )
              ) {
                logger.warn(
                  `[AttachInputs] Skipped credential-owned field "${fieldName}" for node ${node.id} (${nodeType}) — use vault/attach-credentials, or choose manual / unlock + value`
                );
                continue;
              }
            }

            // SPECIAL CASE: For Google resource IDs, extract from full URLs
            let value = rawValue;
            if (typeof rawValue === 'string') {
              try {
                const { extractSpreadsheetId, extractDocumentId, extractFileId } = require('../shared/google-api-utils');

                if (fieldName === 'spreadsheetId') {
                  const extracted = extractSpreadsheetId(rawValue);
                  if (extracted && extracted !== rawValue) {
                    logger.info(`[AttachInputs] Normalized Google Sheets URL to ID for node ${node.id}`);
                    value = extracted;
                  }
                }

                if (fieldName === 'documentId') {
                  const extractedDocId = extractDocumentId(rawValue);
                  if (extractedDocId && extractedDocId !== rawValue) {
                    logger.info(`[AttachInputs] Normalized Google Docs URL to ID for node ${node.id}`);
                    value = extractedDocId;
                  }
                }

                if (fieldName === 'fileId') {
                  const extractedFileId = extractFileId(rawValue);
                  if (extractedFileId && extractedFileId !== rawValue) {
                    logger.info(`[AttachInputs] Normalized Google File URL to ID for node ${node.id}`);
                    value = extractedFileId;
                  }
                }
              } catch (extractErr) {
                logger.warn('[AttachInputs] Failed to normalize Google URL to ID:', extractErr);
              }
            }

            if (unifiedDefForNode?.inputSchema?.cases && (fieldName === 'cases' || fieldName === 'rules')) {
              const normalized = normalizeSwitchCasesInput(value);
              if (!normalized.valid) {
                modeDiagnostics.schemaValidationFailures.push({
                  nodeId: node.id,
                  nodeType,
                  message: `Invalid switch ${fieldName}: expected non-empty JSON array of case objects`,
                });
                logger.warn(`[AttachInputs] Rejected invalid switch ${fieldName} for node ${node.id}`);
                continue;
              }
              value = normalized.value;
              // Canonicalize on cases and keep legacy alias in sync.
              config.cases = normalized.value;
              config.rules = normalized.value;
            }
            if (unifiedDefForNode?.inputSchema?.expression && fieldName === 'expression' && typeof value === 'string') {
              value = value.trim();
            }

            // ── JSON string coercion for array/object fields ──────────────────────────
            // The wizard serializes array/object values to JSON strings for display.
            // Parse them back to the correct type before applying.
            const fieldSchemaType = unifiedDefForNode?.inputSchema?.[fieldName]?.type;
            if (
              typeof value === 'string' &&
              (fieldSchemaType === 'array' || fieldSchemaType === 'object')
            ) {
              const trimmed = value.trim();
              if (
                (trimmed.startsWith('[') && trimmed.endsWith(']')) ||
                (trimmed.startsWith('{') && trimmed.endsWith('}'))
              ) {
                try {
                  value = JSON.parse(trimmed);
                } catch (parseErr) {
                  logger.warn(
                    `[AttachInputs] Failed to parse JSON string for ${fieldName} on node ${node.id} (${nodeType}), skipping:`,
                    parseErr
                  );
                  continue; // skip — do not store a malformed string
                }
              }
            }

            // ✅ CRITICAL: Validate field exists in schema
            // ✅ RELAXED: Accept optional fields even if not in schema (for flexibility)
            const isRequired = schema.configSchema.required?.includes(fieldName);
            const isOptional = schema.configSchema.optional?.[fieldName];
            
            // ✅ CRITICAL: For Gmail, validate based on operation type
            if (nodeType === 'google_gmail') {
              const operation = config.operation || 'send';
              // messageId is only required for 'get' operation, not 'send'
              if (fieldName === 'messageId' && operation !== 'get') {
                logger.info(`[AttachInputs] Skipping messageId for ${operation} operation`);
                continue; // Skip messageId for non-get operations
              }
              // from is optional - OAuth account will be used if not provided
              if (fieldName === 'from' && !value) {
                logger.info(`[AttachInputs] from field empty - will use OAuth account`);
                // Still allow empty from - it's optional
              }
            }
            
            // Registry-driven alias (e.g. Slack text �' message via inputSchema.aliasOf)
            const aliasFieldDef = unifiedDefForNode?.inputSchema?.[fieldName];
            const aliasTarget = resolveAliasTargetFieldName(fieldName, aliasFieldDef as any);
            if (aliasTarget) {
              const cur = (config as any)[aliasTarget];
              if (cur === undefined || cur === null || cur === '') {
                (config as any)[aliasTarget] = value;
                updated = true;
                explicitlySetInBatch.add(aliasTarget);
                explicitlySetInBatch.add(fieldName);
                logger.info(`[AttachInputs] Mapped alias '${fieldName}' �' '${aliasTarget}' for node ${node.id} (${nodeType})`);
                continue;
              }
            }

            if (isRequired || isOptional || nodeType === 'google_gmail') {
              const existingValue = config[fieldName];
              const preserve = shouldPreserveExistingBuildtimeValue(
                fieldName,
                unifiedDefForNode?.inputSchema,
                config as Record<string, unknown>,
                existingValue,
                value
              );
              if (preserve.preserve) {
                modeDiagnostics.buildtimeMergePreserved.push({
                  nodeId: node.id,
                  nodeType,
                  fieldName,
                  reason: preserve.reason || 'buildtime_preserved',
                });
                logger.warn(
                  `[AttachInputs] Preserved existing ${fieldName} on node ${node.id} (${nodeType}): ${preserve.reason}`
                );
                continue;
              }
              if (existingValue !== value) {
                config[fieldName] = value;
                updated = true;
                explicitlySetInBatch.add(fieldName);
                logger.info(`[AttachInputs] Applied ${fieldName} to node ${node.id} (${nodeType}) - ${existingValue ? 'updated' : 'set'}`);
              } else {
                explicitlySetInBatch.add(fieldName);
                logger.info(`[AttachInputs] Field ${fieldName} unchanged for node ${node.id} (${nodeType})`);
              }
            } else {
              logger.warn(`[AttachInputs] Field ${fieldName} not in schema for ${nodeType}, skipping`);
            }
          }
        } else if (key === node.id && typeof rawValue === 'object') {
          // Nested format: { "nodeId": { "fieldName": "value" } }
          for (const [fieldName, fieldValueRaw] of Object.entries(rawValue as Record<string, any>)) {
            if (isConfigMetaKey(fieldName)) {
              if (fieldName === '_fillMode' && fieldValueRaw && typeof fieldValueRaw === 'object') {
                const existingFillMode = (config as any)._fillMode || {};
                const incomingFillMode = fieldValueRaw as Record<string, string>;
                // ✅ Never downgrade buildtime_ai_once to manual_static via auto-persist.
                // buildtime_ai_once can only be changed by an explicit user action.
                const mergedFillMode: Record<string, string> = { ...existingFillMode };
                for (const [fKey, fVal] of Object.entries(incomingFillMode)) {
                  const existing = mergedFillMode[fKey];
                  // Protect: do not overwrite buildtime_ai_once with manual_static
                  if (existing === 'buildtime_ai_once' && fVal === 'manual_static') {
                    continue; // keep buildtime_ai_once
                  }
                  mergedFillMode[fKey] = fVal;
                }
                (config as any)._fillMode = mergedFillMode;
                updated = true;
              } else if (fieldName === '_ownershipUnlock' && fieldValueRaw && typeof fieldValueRaw === 'object') {
                (config as any)._ownershipUnlock = {
                  ...((config as any)._ownershipUnlock || {}),
                  ...(fieldValueRaw as object),
                };
                updated = true;
              } else if (fieldName === '_fieldEnabled' && fieldValueRaw && typeof fieldValueRaw === 'object') {
                (config as any)._fieldEnabled = {
                  ...((config as any)._fieldEnabled || {}),
                  ...(fieldValueRaw as object),
                };
                updated = true;
              }
              continue;
            }
            if (schema.configSchema) {
              const fieldDef = unifiedDefForNode?.inputSchema?.[fieldName];
              if (fieldDef && isCredentialOwnership(fieldName, fieldDef)) {
                if (
                  !shouldApplyCredentialOwnedFieldViaAttachInputs(
                    fieldName,
                    fieldDef,
                    config,
                    unifiedDefForNode?.inputSchema,
                    fieldValueRaw
                  )
                ) {
                  logger.warn(
                    `[AttachInputs] Skipped credential-owned field "${fieldName}" for node ${node.id} (${nodeType}) — use vault/attach-credentials, or choose manual / unlock + value`
                  );
                  continue;
                }
              }
              
              // ✅ CRITICAL: Validate field exists in schema
              // ✅ RELAXED: Accept optional fields even if not in schema (for flexibility)
              const isRequired = schema.configSchema.required?.includes(fieldName);
              const isOptional = schema.configSchema.optional?.[fieldName];
              
              // SPECIAL CASE: For Google resource IDs in nested format, extract from URLs
              let fieldValue: any = fieldValueRaw;
              if (
                (fieldName === 'spreadsheetId' || fieldName === 'documentId' || fieldName === 'fileId') &&
                typeof fieldValueRaw === 'string' &&
                fieldValueRaw.trim() === ''
              ) {
                logger.info(`[AttachInputs] Skipping empty ${fieldName} for node ${node.id}`);
                continue;
              }
              if (typeof fieldValueRaw === 'string') {
                try {
                  const { extractSpreadsheetId, extractDocumentId, extractFileId } = require('../shared/google-api-utils');

                  if (fieldName === 'spreadsheetId') {
                    const extracted = extractSpreadsheetId(fieldValueRaw);
                    if (extracted && extracted !== fieldValueRaw) {
                      logger.info(`[AttachInputs] Normalized Google Sheets URL to ID for node ${node.id}`);
                      fieldValue = extracted;
                    }
                  }

                  if (fieldName === 'documentId') {
                    const extractedDocId = extractDocumentId(fieldValueRaw);
                    if (extractedDocId && extractedDocId !== fieldValueRaw) {
                      logger.info(`[AttachInputs] Normalized Google Docs URL to ID for node ${node.id}`);
                      fieldValue = extractedDocId;
                    }
                  }

                  if (fieldName === 'fileId') {
                    const extractedFileId = extractFileId(fieldValueRaw);
                    if (extractedFileId && extractedFileId !== fieldValueRaw) {
                      logger.info(`[AttachInputs] Normalized Google File URL to ID for node ${node.id}`);
                      fieldValue = extractedFileId;
                    }
                  }
                } catch (extractErr) {
                  logger.warn('[AttachInputs] Failed to normalize Google URL to ID (nested):', extractErr);
                }
              }

              if (unifiedDefForNode?.inputSchema?.cases && (fieldName === 'cases' || fieldName === 'rules')) {
                const normalized = normalizeSwitchCasesInput(fieldValue);
                if (!normalized.valid) {
                  modeDiagnostics.schemaValidationFailures.push({
                    nodeId: node.id,
                    nodeType,
                    message: `Invalid switch ${fieldName}: expected non-empty JSON array of case objects`,
                  });
                  logger.warn(`[AttachInputs] Rejected invalid switch ${fieldName} for node ${node.id} (nested)`);
                  continue;
                }
                fieldValue = normalized.value;
                config.cases = normalized.value;
                config.rules = normalized.value;
              }
              if (unifiedDefForNode?.inputSchema?.expression && fieldName === 'expression' && typeof fieldValue === 'string') {
                fieldValue = fieldValue.trim();
              }

              // ✅ CRITICAL: For Gmail, validate based on operation type
              if (nodeType === 'google_gmail') {
                const operation = config.operation || 'send';
                // messageId is only required for 'get' operation, not 'send'
                if (fieldName === 'messageId' && operation !== 'get') {
                  logger.info(`[AttachInputs] Skipping messageId for ${operation} operation`);
                  continue; // Skip messageId for non-get operations
                }
                // from is optional - OAuth account will be used if not provided
                if (fieldName === 'from' && !fieldValue) {
                  logger.info(`[AttachInputs] from field empty - will use OAuth account`);
                  // Still allow empty from - it's optional
                }
              }
              
              const nestedAliasDef = unifiedDefForNode?.inputSchema?.[fieldName];
              const nestedAliasTarget = resolveAliasTargetFieldName(fieldName, nestedAliasDef as any);
              if (nestedAliasTarget) {
                const cur = (config as any)[nestedAliasTarget];
                if (cur === undefined || cur === null || cur === '') {
                  (config as any)[nestedAliasTarget] = fieldValue;
                  updated = true;
                  logger.info(`[AttachInputs] Mapped alias '${fieldName}' �' '${nestedAliasTarget}' for node ${node.id} (${nodeType}) (nested)`);
                  continue;
                }
              }

              if (isRequired || isOptional || nodeType === 'google_gmail') {
                const existingValue = config[fieldName];

                // ✅ JSON string coercion for array/object fields in nested format
                const nestedFieldSchemaType = unifiedDefForNode?.inputSchema?.[fieldName]?.type;
                if (
                  typeof fieldValue === 'string' &&
                  (nestedFieldSchemaType === 'array' || nestedFieldSchemaType === 'object')
                ) {
                  const trimmed = fieldValue.trim();
                  if (
                    (trimmed.startsWith('[') && trimmed.endsWith(']')) ||
                    (trimmed.startsWith('{') && trimmed.endsWith('}'))
                  ) {
                    try {
                      fieldValue = JSON.parse(trimmed);
                    } catch {
                      logger.warn(`[AttachInputs] Failed to parse JSON string for ${fieldName} on node ${node.id} (nested), skipping`);
                      continue;
                    }
                  }
                }

                const preserveNested = shouldPreserveExistingBuildtimeValue(
                  fieldName,
                  unifiedDefForNode?.inputSchema,
                  config as Record<string, unknown>,
                  existingValue,
                  fieldValue
                );
                if (preserveNested.preserve) {
                  modeDiagnostics.buildtimeMergePreserved.push({
                    nodeId: node.id,
                    nodeType,
                    fieldName,
                    reason: preserveNested.reason || 'buildtime_preserved',
                  });
                  logger.warn(
                    `[AttachInputs] Preserved existing ${fieldName} on node ${node.id} (${nodeType}) (nested): ${preserveNested.reason}`
                  );
                  continue;
                }
                if (existingValue !== fieldValue) {
                  config[fieldName] = fieldValue;
                  updated = true;
                  logger.info(`[AttachInputs] Applied ${fieldName} to node ${node.id} (${nodeType}) - ${existingValue ? 'updated' : 'set'}`);
                } else {
                  logger.info(`[AttachInputs] Field ${fieldName} unchanged for node ${node.id} (${nodeType})`);
                }
              } else {
                logger.warn(`[AttachInputs] Field ${fieldName} not in schema for ${nodeType}, skipping`);
              }
            }
          }
        }
      }

      // ✅ CRITICAL: Always return node with config, even if no changes were made
      // This ensures the config is preserved in the node structure
      if (updated || Object.keys(config).length > 0) {
        if ((config as any)._fillMode && typeof (config as any)._fillMode === 'object') {
          logger.info(`[AttachInputs] Fill mode snapshot for ${node.id} (${nodeType}):`, {
            appliedFields: modeFieldsApplied,
            unknownFields: modeFieldsUnknown,
            fillMode: (config as any)._fillMode,
          });
        }
        const updatedNode = {
          ...node,
          data: {
            ...node.data,
            config,
          },
        };
        // ✅ DEBUG: Log the config being saved for this node
        if (updated) {
          const runtimeFields = modeFieldsApplied.filter((f) => (config as any)?._fillMode?.[f] === 'runtime_ai');
          for (const f of runtimeFields) {
            modeDiagnostics.runtimeResolvedFields.push({
              nodeId: node.id,
              nodeType,
              fieldName: f,
            });
          }
          if (runtimeFields.length > 0) {
            modeDiagnostics.fallbackApplied = true;
          }
          logger.info(`[AttachInputs] ✅ Node ${node.id} (${nodeType}) config updated:`, Object.keys(config).filter(k => config[k] !== undefined && config[k] !== '').map(k => `${k}=${typeof config[k] === 'string' ? config[k].substring(0, 20) : config[k]}`).join(', '));
        }
        return updatedNode;
      }

      return node;
    });
    } catch (mapError) {
      logger.error('[AttachInputs] Error during node input injection:', mapError);
      logger.error('[AttachInputs] Error details:', {
        error: mapError instanceof Error ? mapError.message : String(mapError),
        stack: mapError instanceof Error ? mapError.stack : undefined,
        nodesCount: workflowGraph.nodes.length,
        inputsCount: Object.keys(cleanInputs).length,
      });
      return { statusCode: 500, body: (
        createError(
          ErrorCode.INTERNAL_ERROR,
          'Failed to inject inputs into nodes',
          {
            error: mapError instanceof Error ? mapError.message : String(mapError),
            workflowId,
            hint: 'An error occurred while applying inputs to workflow nodes. Check server logs for details.',
          }
        )
      ) };
    }

    // ✅ FIXED: Keep ai_agent nodes as-is (don't replace with ai_chat_model)
    // ai_agent works fine with Ollama and is properly supported in the frontend
    // The ai_chat_model replacement was causing frontend validation errors
    const nodesAfterReplacement = updatedNodes;
    
    // No edge updates needed since we're keeping ai_agent nodes
    let updatedEdges = clonedWorkflow.edges.map((edge: any) => {
      // No special handling needed - keep edges as-is
      
      return edge;
    }).filter((e: any) => e !== null);

    // Use cloned workflow structure with updated nodes
    const updatedWorkflow = {
      nodes: nodesAfterReplacement,
      edges: updatedEdges, // Use updated edges
    };

    // Validate workflow structure after input injection.
    // The first normalization pass (pre-clone, line ~664) already canonicalized structure.
    // Re-normalizing here would mutate user-approved config values set by the injection phase.
    const { validateWorkflowForSave } = await import('../core/validation/workflow-save-validator');

    const saveValidation = validateWorkflowForSave(nodesAfterReplacement, updatedEdges, {
      buildManifest: (workflow as any)?.metadata?.buildManifest,
      freezeBoundary: (workflow as any)?.metadata?.freezeBoundary,
    });
    
    // ✅ CRITICAL: Only reject on truly critical errors that can't be auto-fixed
    // Normalization should have fixed duplicate triggers, invalid edges, etc.
    // Only block on errors that indicate the workflow is fundamentally broken
    const criticalSaveErrors = saveValidation.errors.filter((error: string) => {
      const errorLower = error.toLowerCase();
      // Critical errors that can't be auto-fixed:
      return errorLower.includes('no nodes') || 
             errorLower.includes('no edges') ||
             (errorLower.includes('must have exactly one trigger') && !errorLower.includes('multiple')); // Only block if NO trigger, not multiple (normalization fixes multiple)
    });
    
    if (criticalSaveErrors.length > 0) {
      logger.error('[AttachInputs] Critical save validation errors (after normalization):', criticalSaveErrors);
      logger.warn('[AttachInputs] Non-critical errors (will be auto-fixed):', saveValidation.errors.filter((e: string) => !criticalSaveErrors.includes(e)));
      return { statusCode: 400, body: (
        createError(
          ErrorCode.INVALID_INPUT,
          'Workflow validation failed',
          {
            errors: criticalSaveErrors,
            warnings: saveValidation.warnings,
            workflowId,
            hint: 'Please fix the critical validation errors before attaching inputs.',
          }
        )
      ) };
    }
    
    // Log warnings but don't block
    if (saveValidation.warnings.length > 0) {
      logger.warn('[AttachInputs] Validation warnings (non-blocking):', saveValidation.warnings);
    }
    
    // Use injected nodes/edges for rest of processing
    const normalizedWorkflow = {
      nodes: nodesAfterReplacement,
      edges: updatedEdges,
    };
    
    // ✅ CRITICAL: Relax validation - only validate structure, not required fields
    // Required fields may be filled later or have defaults
    // Use fixedWorkflow even if there are errors (validator will auto-fix issues)
    // Use normalized workflow (already fixed duplicate triggers, etc.)
    const validation = await workflowValidator.validateAndFix(
      normalizedWorkflow,
      0,
      undefined,
      undefined,
      { mode: 'topologyPreserve' }
    );

    // ✅ CRITICAL: Only reject on critical structural errors (missing nodes, invalid edges)
    // Allow validation errors that can be auto-fixed or are non-critical
    const criticalErrors = validation.errors.filter((e: any) => {
      const msg = e.message?.toLowerCase() || '';
      // Critical: missing nodes, invalid edges, duplicate IDs
      return msg.includes('missing node') || 
             msg.includes('invalid edge') || 
             msg.includes('duplicate') ||
             msg.includes('no nodes') ||
             msg.includes('no edges');
    });

    if (criticalErrors.length > 0) {
      // ✅ Log detailed validation errors
      logger.error('[AttachInputs] Critical validation errors:', criticalErrors.map((e: any) => e.message));
      logger.warn('[AttachInputs] Non-critical validation errors:', validation.errors.filter((e: any) => !criticalErrors.includes(e)).map((e: any) => e.message));
      logger.warn('[AttachInputs] Validation warnings:', validation.warnings.map((w: any) => w.message));
      
      // ✅ Return structured error only for critical issues
      return { statusCode: 400, body: (
        createError(
          ErrorCode.WORKFLOW_VALIDATION_FAILED,
          'Workflow validation failed after input injection',
          {
            errors: criticalErrors.map((e: any) => e.message),
            warnings: validation.warnings.map((w: any) => w.message),
            nonCriticalErrors: validation.errors.filter((e: any) => !criticalErrors.includes(e)).map((e: any) => e.message),
            validationResult: {
              valid: validation.valid,
              errors: validation.errors,
              warnings: validation.warnings,
            },
          }
        )
      ) };
    }

    // ✅ CRITICAL: Use fixedWorkflow even if there are non-critical errors
    // The validator auto-fixes issues, so we trust its output
    const finalWorkflow = validation.fixedWorkflow || updatedWorkflow;
    
    // Log non-critical errors as warnings
    if (validation.errors.length > 0) {
      logger.warn('[AttachInputs] Non-critical validation errors (using fixed workflow):', validation.errors.map((e: any) => e.message));
    }
    if (validation.warnings.length > 0) {
      logger.warn('[AttachInputs] Validation warnings:', validation.warnings.map((w: any) => w.message));
    }

    // ✅ CRITICAL: Apply save-time normalization to remove duplicates and fix structure
    const { normalizeWorkflowForSave: normalizeBeforeSave } = await import('../core/validation/workflow-save-validator');
    const structuralInput = {
      ...(finalWorkflow as any),
      metadata: {
        ...((workflow as any)?.metadata || {}),
        ...((finalWorkflow as any)?.metadata || {}),
        originalUserPrompt:
          trimmedOriginalFromRequest ||
          ((finalWorkflow as any)?.metadata?.originalUserPrompt as string) ||
          ((workflow as any)?.metadata?.originalUserPrompt as string) ||
          undefined,
        disableFormFieldIntentPrune:
          (finalWorkflow as any)?.metadata?.disableFormFieldIntentPrune ??
          (workflow as any)?.metadata?.disableFormFieldIntentPrune,
        generatedFrom:
          ((finalWorkflow as any)?.metadata?.generatedFrom as string) ||
          ((workflow as any)?.metadata?.generatedFrom as string) ||
          (workflow as any)?.name ||
          '',
      },
    } as any;
    // Snapshot existing credentialId values before materialization � they must survive the pipeline
    const credentialIdSnapshot = new Map<string, unknown>();
    for (const node of (structuralInput.nodes ?? [])) {
      const cid = (node as any)?.data?.config?.credentialId;
      if (cid !== undefined && cid !== null && cid !== '') {
        credentialIdSnapshot.set(String((node as any).id ?? ''), cid);
      }
    }

    const materializedWorkflow = isPostFreezeReadonly
      ? structuralInput
      : (hydrateRequiredConfigFromRegistryDefaults(
          applyStructuralIntentAlignment(materializeStructuralFields(structuralInput as any) as any) as any
        ) as any);

    // Restore any credentialIds that materialization cleared, unless the incoming payload explicitly blanked them
    if (!isPostFreezeReadonly && credentialIdSnapshot.size > 0) {
      const explicitCredentialClears = new Set<string>(
        Object.keys(cleanInputs).filter(k => k.toLowerCase().includes('credentialid') && cleanInputs[k] === '')
      );
      for (const node of (materializedWorkflow.nodes ?? [])) {
        const nodeId = String((node as any)?.id ?? '');
        const saved = credentialIdSnapshot.get(nodeId);
        if (saved && !explicitCredentialClears.has(nodeId)) {
          const cfg = (node as any)?.data?.config;
          if (cfg && (cfg.credentialId === undefined || cfg.credentialId === '' || cfg.credentialId === null)) {
            cfg.credentialId = saved;
          }
        }
      }
    }

    let metadataToPersist: Record<string, unknown> = {
      ...((workflow as any)?.metadata || {}),
      ...((materializedWorkflow as any)?.metadata || {}),
      ...(trimmedOriginalFromRequest
        ? { originalUserPrompt: trimmedOriginalFromRequest }
        : {}),
    };
    const usageSnap = snapshotBuildAiUsage();
    if (usageSnap.totals.callCount > 0) {
      metadataToPersist = {
        ...metadataToPersist,
        buildAiUsage: mergePersistedBuildAiUsage(metadataToPersist.buildAiUsage, usageSnap),
      };
    }
    const guidanceUsage =
      fieldGuidanceBuildAiUsage &&
      typeof fieldGuidanceBuildAiUsage === 'object' &&
      (fieldGuidanceBuildAiUsage as any)?.totals?.callCount > 0
        ? fieldGuidanceBuildAiUsage
        : null;
    if (guidanceUsage) {
      metadataToPersist = {
        ...metadataToPersist,
        buildAiUsage: mergePersistedBuildAiUsage(metadataToPersist.buildAiUsage, guidanceUsage as any),
      };
    }
    if (appliedFieldGuidanceExamples.length > 0) {
      const priorApplied = Array.isArray((metadataToPersist as any).fieldGuidanceAppliedFields)
        ? ((metadataToPersist as any).fieldGuidanceAppliedFields as any[])
        : [];
      const mergedApplied = [...priorApplied, ...appliedFieldGuidanceExamples];
      metadataToPersist = {
        ...metadataToPersist,
        fieldGuidanceAppliedCount: mergedApplied.length,
        fieldGuidanceAppliedFields: mergedApplied,
      };
    }

    const structuralDiagnostics = getStructuralDiagnostics(materializedWorkflow as any);
    // Accumulate migrations from both normalization passes for idempotency tracking
    const accumulatedMigrations: string[] = [
      ...existingAppliedMigrations,
      ...normalizedBeforeClone.migrationsApplied,
    ];
    const finalNormalizedForSave = normalizeBeforeSave(
      materializedWorkflow.nodes,
      materializedWorkflow.edges,
      {
        structuralMode: isConfigFrozen ? 'post_freeze_readonly' : 'configOnly',
        alreadyApplied: accumulatedMigrations,
      }
    );
    
    if (finalNormalizedForSave.migrationsApplied.length > 0) {
      logger.info('[AttachInputs] 🔄 Applied final normalizations before saving:', finalNormalizedForSave.migrationsApplied);
    }
    
    // ✅ CRITICAL: Normalize workflow graph before saving (for graph structure)
    let finalNormalizedGraph: ReturnType<typeof normalizeWorkflowGraph>;
    try {
      finalNormalizedGraph = normalizeWorkflowGraph(
        {
          nodes: finalNormalizedForSave.nodes,
          edges: finalNormalizedForSave.edges,
        },
        { mode: 'topologyPreserve' }
      );
    } catch (normalizeError) {
      logger.error('[AttachInputs] Failed to normalize final workflow:', normalizeError);
      logger.error('[AttachInputs] Final workflow structure:', {
        hasNodes: !!finalWorkflow.nodes,
        hasEdges: !!finalWorkflow.edges,
        nodesType: typeof finalWorkflow.nodes,
        edgesType: typeof finalWorkflow.edges,
        nodesIsArray: Array.isArray(finalWorkflow.nodes),
        edgesIsArray: Array.isArray(finalWorkflow.edges),
        finalWorkflowKeys: Object.keys(finalWorkflow || {}),
      });
      return { statusCode: 500, body: (
        createError(
          ErrorCode.GRAPH_PARSE_ERROR,
          'Failed to normalize workflow graph before saving',
          {
            error: normalizeError instanceof Error ? normalizeError.message : String(normalizeError),
            workflowId,
            hint: 'The workflow structure may be invalid after validation. Check server logs for details.',
          }
        )
      ) };
    }

    // Universal contract gate (registry + orchestrator): reconcile and validate
    const contractDiagnostics: {
      boundary: 'attach_inputs_save';
      branchingNodeCount: number;
      renderTypeMismatches: number;
      validationValid: boolean;
      validationErrors: string[];
      validationWarnings?: string[];
    } = {
      boundary: 'attach_inputs_save',
      branchingNodeCount: 0,
      renderTypeMismatches: 0,
      validationValid: true,
      validationErrors: [],
      validationWarnings: [],
    };

    try {
      const wfContract = {
        nodes: finalNormalizedGraph.nodes as any,
        edges: finalNormalizedGraph.edges as any,
      } as any;
      const executionOrder = executionOrderManager.initialize(wfContract);
      const validationResult = unifiedGraphOrchestrator.validateWorkflow(wfContract, executionOrder);

      contractDiagnostics.validationValid = validationResult.valid;
      contractDiagnostics.validationErrors = validationResult.errors || [];
      contractDiagnostics.validationWarnings = validationResult.warnings || [];
      contractDiagnostics.branchingNodeCount = finalNormalizedGraph.nodes.filter((n: any) => {
        const t = unifiedNormalizeNodeTypeString(n?.data?.type || n?.type || '');
        return unifiedNodeRegistry.get(t)?.isBranching === true;
      }).length;
      contractDiagnostics.renderTypeMismatches = finalNormalizedGraph.nodes.filter((n: any) => {
        const renderType = String(n?.type || '');
        const semanticType = String(n?.data?.type || '');
        return renderType !== semanticType && renderType !== 'custom' && renderType !== 'form' && renderType !== 'manual_trigger' && renderType !== 'set_variable';
      }).length;

      if (!validationResult.valid) {
        // ✅ FIX: Only block on critical structural errors (cycles, multiple triggers).
        // Orphaned nodes are non-critical — they get auto-removed during reconciliation.
        // Blocking on orphaned nodes causes 400 errors when the workflow has extra nodes
        // that weren't wired (e.g. from a previous generation with more nodes).
        const criticalErrors = (validationResult.errors || []).filter((e: string) => {
          const lower = e.toLowerCase();
          return (
            lower.includes('cycle') ||
            lower.includes('multiple trigger')
          );
        });
        const triggerReadinessErrors = (validationResult.errors || []).filter((e: string) => {
          const lower = e.toLowerCase();
          return lower.includes('no trigger') || lower.includes('missing trigger');
        });
        const orphanErrors = (validationResult.errors || []).filter((e: string) =>
          e.toLowerCase().includes('orphan')
        );

        if (criticalErrors.length > 0) {
          return { statusCode: 400, body: (
            createError(
              ErrorCode.WORKFLOW_VALIDATION_FAILED,
              'Workflow contract validation failed before save',
              {
                workflowId,
                contractDiagnostics,
              }
            )
          ) };
        }

        // Orphaned nodes only — log as warning and continue
        if (orphanErrors.length > 0) {
          logger.warn('[AttachInputs] ⚠️ Orphaned nodes detected (non-blocking, will be auto-removed):', orphanErrors);
          contractDiagnostics.validationValid = true; // treat as valid for save purposes
        }

        // Metadata/config updates (for example field ownership changes from the properties panel)
        // must remain editable while users are still assembling or testing a workflow. A missing
        // trigger is an execution-readiness issue, not a config-save blocker for attach-inputs.
        if (triggerReadinessErrors.length > 0) {
          logger.warn('[AttachInputs] Trigger readiness errors detected (non-blocking for config save):', triggerReadinessErrors);
          contractDiagnostics.validationValid = true;
        }
      }
    } catch (contractError) {
      logger.warn('[AttachInputs] Contract gate failed (non-fatal):', contractError);
    }

    // ✅ CRITICAL: Check if credentials are required BEFORE updating
    // If NO credentials are required, set status to ready_for_execution immediately
    let requiredCredentialsCount = 0;
    let missingCredentialsCount = 0;
    let credentialDiscovery: any = null;
    
    try {
      const { credentialDiscoveryPhase } = await import('../services/ai/credential-discovery-phase');
      // Use the same reconciled graph as this request (reconcileMisroutedAiCommunicationNodes + normalize).
      // discoverCredentials(workflowId) re-fetches DB, which still has pre-reconcile types until we save below —
      // that caused Gmail nodes to be classified as ollama during discovery (wrong vault / requirements).
      credentialDiscovery = await credentialDiscoveryPhase.discoverCredentials(
        {
          nodes: finalNormalizedGraph.nodes as any,
          edges: finalNormalizedGraph.edges as any,
        } as any,
        userId
      );
      
      requiredCredentialsCount = credentialDiscovery.requiredCredentials?.length || 0;
      missingCredentialsCount = credentialDiscovery.missingCredentials?.length || 0;
      
      logger.info(`[AttachInputs] Credential check: ${requiredCredentialsCount} required, ${missingCredentialsCount} missing`);
      
      // ✅ CRITICAL: Auto-inject resolved credentials into nodes
      // If credentials are already satisfied (in vault), automatically inject them into node configs
      if (credentialDiscovery.satisfiedCredentials && credentialDiscovery.satisfiedCredentials.length > 0) {
        logger.info(`[AttachInputs] Auto-injecting ${credentialDiscovery.satisfiedCredentials.length} resolved credential(s) into nodes...`);
        
        for (const satisfiedCred of credentialDiscovery.satisfiedCredentials) {
          // Find nodes that need this credential
          const nodeIds = satisfiedCred.nodeIds || [];
          
          for (const nodeId of nodeIds) {
            const node = finalNormalizedGraph.nodes.find((n: any) => n.id === nodeId);
            if (!node) continue;
            
            const nodeType = node.data?.type || node.type || '';
            
            // For OAuth-based nodes (Gmail, Sheets, etc.), inject credentialId
            if (satisfiedCred.type === 'oauth' && satisfiedCred.provider) {
              // Runtime node configs use the registry vaultKey because it is the same stable
              // key used by the dashboard catalog, credential vault, and status checks.
              const credentialId =
                String(satisfiedCred.vaultKey || '').trim() ||
                String(satisfiedCred.provider || '').trim();
              
              // Ensure node has data object with all required properties
              if (!node.data) {
                node.data = {
                  label: node.type || nodeId,
                  type: nodeType || node.type || '',
                  category: 'utility',
                  config: {}
                };
              }
              if (!node.data.config) node.data.config = {};
              
              // Only inject if credentialId is not already set
              if (!node.data.config.credentialId) {
                node.data.config.credentialId = credentialId;
                logger.info(`[AttachInputs] ✅ Auto-injected credentialId "${credentialId}" into node ${nodeId} (${nodeType}) - provider: ${satisfiedCred.provider}, type: ${satisfiedCred.type}, scopes: ${satisfiedCred.scopes?.join(', ') || 'none'}`);
              } else {
                logger.info(`[AttachInputs] ⏭️  Node ${nodeId} (${nodeType}) already has credentialId "${node.data.config.credentialId}", skipping auto-injection`);
              }
            }
          }
        }
      }
    } catch (credError) {
      logger.warn('[AttachInputs] Failed to discover credentials (non-fatal, defaulting to requiring credentials):', credError);
      // Default to requiring credentials if discovery fails
      requiredCredentialsCount = 1; // Assume credentials might be needed
      missingCredentialsCount = 1;
    }
    
    // ✅ PHASE PIPELINE: Determine the correct next phase after successful input attachment.
    // ready_for_ownership �' signals attach-credentials that freeze boundary is established.
    // ready_for_execution �' no credentials needed, workflow is ready.
    let nextStatus = 'active';
    let nextPhase = 'ready_for_ownership'; // Default: structure frozen, credentials stage can start
    const readiness = await workflowLifecycleManager.validateExecutionReady(finalNormalizedGraph as any, userId);
    if (readiness.ready) {
      nextStatus = 'active';
      nextPhase = 'ready_for_execution';
      logger.info(`[AttachInputs] Unified readiness passed - setting phase ready_for_execution`);
    } else {
      nextStatus = 'active';
      nextPhase = 'ready_for_ownership';
      logger.info(`[AttachInputs] Inputs applied - phase ready_for_ownership (credentials still needed): ${readiness.errors.join('; ')}`);
    }
    const structuralReadinessErrors = (readiness.errors || []).filter((msg) => {
      const lower = String(msg || '').toLowerCase();
      return (
        lower.includes('disconnected') ||
        lower.includes('orphan') ||
        lower.includes('no input connection') ||
        lower.includes('has no input') ||
        lower.includes('cycle') ||
        lower.includes('invalid edge')
      );
    });
    if (structuralReadinessErrors.length > 0) {
      return { statusCode: 400, body: (
        createError(
          ErrorCode.WORKFLOW_VALIDATION_FAILED,
          'Workflow graph is structurally invalid after input attachment',
          {
            workflowId,
            errors: structuralReadinessErrors,
            phase: nextPhase,
          }
        )
      ) };
    }
    if (structuralDiagnostics.unresolved.length > 0) {
      nextPhase = 'configuring_inputs';
      modeDiagnostics.schemaValidationFailures.push(
        ...structuralDiagnostics.unresolved.map((issue) => ({
          nodeId: issue.nodeId,
          nodeType: issue.nodeType,
          message: `${issue.nodeType}.${issue.fieldName} is unresolved structural input`,
        }))
      );
      logger.warn('[AttachInputs] Structural diagnostics blocked credential phase:', structuralDiagnostics.unresolved);
    }
    if (nextPhase === 'ready_for_execution') {
      const structuralGate = validateStructuralReadiness(finalNormalizedGraph.nodes as any, { strict: true });
      if (structuralGate.errors.length > 0) {
        nextPhase = 'configuring_inputs';
        modeDiagnostics.schemaValidationFailures.push(
          ...structuralGate.errors.map((message) => ({
            nodeId: '',
            nodeType: 'structural_readiness',
            message,
          }))
        );
        logger.warn('[AttachInputs] Structural readiness gate blocked ready_for_execution:', structuralGate.errors);
      }
    }
    
    // ✅ CRITICAL: Use linearized graph from normalizeWorkflowGraph (has single-trigger, single-chain enforcement)
    // This ensures workflows are saved with exactly one trigger and linear chain structure
    const nodesToSave = mergePreservedNodePositions(
      finalNormalizedGraph.nodes,
      attachInputsPositionSnapshot
    );
    const edgesToSave = finalNormalizedGraph.edges;
    // Persist all migrations applied across both normalization passes for future idempotency
    const allAppliedMigrations = Array.from(new Set([
      ...accumulatedMigrations,
      ...finalNormalizedForSave.migrationsApplied,
    ]));
    metadataToPersist = {
      ...metadataToPersist,
      appliedMigrations: allAppliedMigrations,
      lastAttachInputs: {
        payloadHash: attachPayloadHash,
        topologyFingerprint: fingerprintWorkflowTopology(nodesToSave, edgesToSave).fingerprint,
        appliedAt: new Date().toISOString(),
      },
    };
    if (isPostFreezeReadonly) {
      const baselineNodeById = new Map(
        (normalizedGraph.nodes || []).map((n: any) => [String(n?.id || ''), n])
      );
      const structuralDrifts: Array<{ nodeId: string; field: string }> = [];
      const hasChanged = (before: unknown, after: unknown): boolean => {
        if (before === after) return false;
        try {
          return JSON.stringify(before) !== JSON.stringify(after);
        } catch {
          return true;
        }
      };

      for (const node of nodesToSave as any[]) {
        const nodeId = String(node?.id || '');
        if (!nodeId) continue;
        const before = baselineNodeById.get(nodeId);
        if (!before) continue;

        const beforeConfig = (before as any)?.data?.config || {};
        const afterConfig = (node as any)?.data?.config || {};
        const nodeType = String((node as any)?.data?.type || (node as any)?.type || '');

        const protectedFields =
          nodeType === 'switch'
            ? ['cases', 'rules', 'expression']
            : nodeType === 'form'
              ? ['fields']
              : [];

        for (const field of protectedFields) {
          if (hasChanged(beforeConfig?.[field], afterConfig?.[field])) {
            structuralDrifts.push({ nodeId, field });
          }
        }
      }

      if (structuralDrifts.length > 0) {
        return { statusCode: 409, body: (
          createError(
            ErrorCode.TOPOLOGY_MUTATION_BLOCKED_CONFIGURING_INPUTS,
            'Post-freeze structural config drift detected. Switch/form structure cannot be rewritten in this phase.',
            { workflowId, drifts: structuralDrifts },
            true
          )
        ) };
      }
    }
    if (!isPostFreezeReadonly && (nextPhase === 'ready_for_ownership' || nextPhase === 'ready_for_execution')) {
      const freezeTopology = fingerprintWorkflowTopology(nodesToSave, edgesToSave);
      const freezeProtected = fingerprintWorkflowProtectedConfig(nodesToSave);
      metadataToPersist = {
        ...metadataToPersist,
        freezeBoundary: {
          frozen: true,
          frozenAt: new Date().toISOString(),
          lifecyclePhase: nextPhase,
          freezePolicy: 'topology_only' as const,
          baselineTopologyFingerprint: freezeTopology.fingerprint,
          baselineProtectedConfigFingerprint: freezeProtected.fingerprint,
        },
      };
    }

    // Skip the baseline-vs-final topology check for post-freeze readonly requests.
    // Post-freeze requests are already protected by:
    //   1. The structural drift check above (lines 1750-1796) — prevents changes to switch.cases / form.fields
    //   2. The freeze boundary check below (lines 1840-1880) — compares against the correct frozen baseline
    // The baseline-vs-final check can produce false 409s in post-freeze mode because the
    // topologyPreserve normalizer may produce different edge sourceHandle values on successive passes
    // when a switch node has more cases than connected edges (switch sourceHandle inference is not
    // perfectly idempotent in that edge case). The freeze boundary check provides equivalent safety.
    if (baselineTopologyFingerprint && !isPostFreezeReadonly) {
      const finalTopologyFingerprint = fingerprintWorkflowTopology(nodesToSave, edgesToSave);
      if (finalTopologyFingerprint.fingerprint !== baselineTopologyFingerprint.fingerprint) {
        const diff = diffWorkflowTopology(baselineTopologyFingerprint, finalTopologyFingerprint);
        logger.error('[AttachInputs] ❌ Topology mutation blocked before save:', {
          workflowId,
          diff,
        });
        return { statusCode: 409, body: (
          createError(
            ErrorCode.TOPOLOGY_MUTATION_BLOCKED_CONFIGURING_INPUTS,
            'Workflow topology must not change during input attachment (configuration phase).',
            {
              workflowId,
              baselineFingerprint: baselineTopologyFingerprint.fingerprint,
              finalFingerprint: finalTopologyFingerprint.fingerprint,
              diff,
            },
            true
          )
        ) };
      }
    }
    const finalProtectedConfigFingerprint = fingerprintWorkflowProtectedConfig(nodesToSave);
    const freezeBaselineTopology = freezeBoundary?.baselineTopologyFingerprint as string | undefined;
    if (isPostFreezeReadonly && freezeBaselineTopology) {
      const finalTopologyFingerprint = fingerprintWorkflowTopology(nodesToSave, edgesToSave);
      if (finalTopologyFingerprint.fingerprint !== freezeBaselineTopology) {
        // Allow re-freezing when workflow is ready_for_execution — this handles the case where
        // the stored fingerprint was computed with an older normalizer (e.g. edge.type was '' vs
        // 'default'). The topology hasn't actually changed; only the hashing algorithm was fixed.
        if (nextPhase === 'ready_for_execution' || nextPhase === 'ready_for_ownership') {
          logger.warn('[AttachInputs] ⚠️ Post-freeze fingerprint mismatch — re-freezing with updated topology hash:', {
            workflowId,
            oldFingerprint: freezeBaselineTopology,
            newFingerprint: finalTopologyFingerprint.fingerprint,
            nextPhase,
          });
          const freezeProtected = fingerprintWorkflowProtectedConfig(nodesToSave);
          metadataToPersist = {
            ...metadataToPersist,
            freezeBoundary: {
              ...(freezeBoundary as Record<string, unknown>),
              frozen: true,
              frozenAt: new Date().toISOString(),
              lifecyclePhase: nextPhase,
              freezePolicy: 'topology_only',
              baselineTopologyFingerprint: finalTopologyFingerprint.fingerprint,
              baselineProtectedConfigFingerprint: freezeProtected.fingerprint,
            },
          };
        } else {
          return { statusCode: 409, body: (
            createError(
              ErrorCode.TOPOLOGY_MUTATION_BLOCKED_CONFIGURING_INPUTS,
              'Post-freeze topology drift detected. Structural changes are blocked.',
              {
                workflowId,
                baselineFingerprint: freezeBaselineTopology,
                finalFingerprint: finalTopologyFingerprint.fingerprint,
              },
              true
            )
          ) };
        }
      }
    }
    // Protected-config hash is not used to 409 after freeze (topology-only freeze policy).
    // Refresh stored protected-config snapshot on each successful save while frozen (audit / UI; not enforced).
    if (isPostFreezeReadonly && freezeBoundary?.frozen) {
      metadataToPersist = {
        ...metadataToPersist,
        freezeBoundary: {
          ...(freezeBoundary as Record<string, unknown>),
          freezePolicy: 'topology_only',
          baselineProtectedConfigFingerprint: finalProtectedConfigFingerprint.fingerprint,
        },
      };
    }

    logger.info('[AttachInputs] Saving workflow with normalized structure:', {
      nodeCount: nodesToSave.length,
      edgeCount: edgesToSave.length,
      triggerNodes: nodesToSave.filter(n => {
        const category = n.data?.category || '';
        const nodeType = n.data?.type || n.type || '';
        return category.toLowerCase() === 'triggers' || 
               category.toLowerCase() === 'trigger' ||
               nodeType.includes('trigger') ||
               ['manual_trigger', 'webhook', 'schedule', 'interval', 'form', 'chat_trigger', 'workflow_trigger'].includes(nodeType);
      }).length,
      branchingNodes: contractDiagnostics.branchingNodeCount,
      contractValidationValid: contractDiagnostics.validationValid,
    });
    
    // 🆕 VERSIONING: Get previous definition before update
    let previousDefinition: any = null;
    try {
      const { data: previousWorkflow } = await db
        .from('workflows')
        .select('*')
        .eq('id', workflowId)
        .single();

      if (previousWorkflow) {
        previousDefinition = {
          name: previousWorkflow.name,
          nodes: previousWorkflow.nodes || [],
          edges: previousWorkflow.edges || [],
          status: previousWorkflow.status,
          phase: previousWorkflow.phase,
          // ✅ Use settings with fallback - column may not exist if migration not run
          settings: (previousWorkflow as any).settings || {},
          graph: (previousWorkflow as any).graph || {},
          metadata: (previousWorkflow as any).metadata || {},
        };
      }
    } catch (versionError) {
      // Non-critical - continue without previous definition
      logger.warn('[AttachInputs] Could not load previous definition for versioning:', versionError);
    }

    // ✅ CRITICAL: Update workflow graph AND status in a single atomic operation
    // Also sync phase field if it exists (for backward compatibility)
    // Note: Database uses 'nodes' and 'edges' columns, not 'graph'
    const { data: updateData, error: updateError } = await db
      .from('workflows')
      .update({
        nodes: nodesToSave,
        edges: edgesToSave,
        metadata: metadataToPersist,
        graph: {
          nodes: nodesToSave,
          edges: edgesToSave,
          metadata: metadataToPersist,
        },
        status: nextStatus, // ✅ CRITICAL: Use valid enum value ('active')
        phase: nextPhase, // ✅ CRITICAL: Use TEXT field for execution phase
        updated_at: new Date().toISOString(),
      })
      .eq('id', workflowId)
      .select('id, status, phase, nodes, edges, name, metadata')
      .single();

    if (updateError) {
      logger.error('[AttachInputs] ❌ Failed to update workflow:', {
        workflowId,
        error: updateError.message,
        errorCode: updateError.code,
        errorDetails: updateError.details,
        hint: updateError.hint,
        fullError: updateError,
      });
      
      // ✅ CRITICAL: Check if error is due to missing 'graph' column
      const isGraphColumnError = updateError.message?.includes('graph') || 
                                 updateError.message?.includes('column') ||
                                 updateError.code === '42703' || // PostgreSQL: undefined column
                                 updateError.code === 'PGRST116'; // PostgREST: column not found
      
      return { statusCode: 500, body: (
        createError(
          ErrorCode.INTERNAL_ERROR,
          'Failed to update workflow',
          { 
            error: updateError.message,
            errorCode: updateError.code,
            errorDetails: updateError.details,
            hint: isGraphColumnError 
              ? 'The workflows table may be missing the "graph" column. Check database schema.'
              : updateError.hint || 'Check server logs for detailed error information.',
            workflowId 
          }
        )
      ) };
    }

    // ✅ CRITICAL: Verify status and phase were actually persisted
    if (!updateData || updateData.status !== nextStatus || updateData.phase !== nextPhase) {
      logger.error('[AttachInputs] ❌ Status/phase update did not persist:', {
        workflowId,
        expectedStatus: nextStatus,
        expectedPhase: nextPhase,
        actualStatus: updateData?.status,
        actualPhase: updateData?.phase,
      });
      return { statusCode: 500, body: (
        createError(
          ErrorCode.INTERNAL_ERROR,
          'Workflow status/phase update did not persist',
          {
            workflowId,
            expectedStatus: nextStatus,
            expectedPhase: nextPhase,
            actualStatus: updateData?.status,
            actualPhase: updateData?.phase,
          }
        )
      ) };
    }

    // 🆕 VERSIONING: Create version after successful update
    if (updateData) {
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
          name: updateData.name || 'Workflow',
          nodes: nodesToSave,
          edges: edgesToSave,
          status: nextStatus,
          phase: nextPhase,
          // ✅ Use settings with fallback - column may not exist if migration not run
          settings: (updateData as any).settings || {},
          graph: (updateData as any).graph || { nodes: nodesToSave, edges: edgesToSave },
          metadata: (updateData as any).metadata || {},
        };

        // ✅ CRITICAL FIX: Create version only if workflow exists
        // Versioning is optional - if it fails, workflow still saves successfully
        try {
          const version = await versionManager.createVersion(
            workflowId,
            currentDefinition,
            previousDefinition,
            createdBy,
            {
              description: 'Inputs attached and workflow updated',
            }
          );
          
          if (version) {
            logger.info(`[AttachInputs] ✅ Created workflow version ${version.version} for ${workflowId}`);
          } else {
            logger.info(`[AttachInputs] ⚠️  Versioning skipped - workflow ${workflowId} not found in workflows_new table`);
          }
        } catch (versionError) {
          // Versioning is non-critical - log but don't fail the update
          logger.warn('[AttachInputs] Versioning failed (non-critical):', versionError);
        }
      } catch (versioningSetupError) {
        // Non-critical - log but don't fail the update
        logger.warn('[AttachInputs] Versioning setup failed (non-critical):', versioningSetupError);
      }
    }

    logger.info(`[AttachInputs] ✅ Workflow updated - graph saved, status set to ${nextStatus}, phase set to ${nextPhase} for workflow ${workflowId}`);

    // ✅ CRITICAL: Audit trail - log inputs attached event
    try {
      const ownershipSummary = modeDiagnostics.appliedModes.reduce((acc, item) => {
        acc[item.mode] = (acc[item.mode] || 0) + 1;
        return acc;
      }, {} as Record<string, number>);
      await db
        .from('workflow_events')
        .insert({
          workflow_id: workflowId,
          event_type: 'INPUTS_ATTACHED',
          event_data: {
            inputsCount: Object.keys(cleanInputs).length,
            nodeIds: Array.from(new Set(Object.keys(cleanInputs).map(key => {
              const match = key.match(/^(.+?)_/);
              return match ? match[1] : null;
            }).filter(Boolean))),
            requiredCredentialsCount,
            missingCredentialsCount,
            nextStatus,
            ownershipSummary,
            ownershipFieldsTouched: modeDiagnostics.appliedModes.length,
            fieldGuidanceAppliedCount: appliedFieldGuidanceExamples.length,
            fieldGuidanceAppliedFields: appliedFieldGuidanceExamples,
          },
          created_at: new Date().toISOString(),
        });
    } catch (auditError) {
      logger.warn('[AttachInputs] Failed to log audit event:', auditError);
    }

    logger.info(`[AttachInputs] Successfully injected ${Object.keys(cleanInputs).length} input(s) into workflow ${workflowId}, status: ${nextStatus}`);
    
    // ✅ DEBUG: Log the config for each node in the response
    logger.info(`[AttachInputs] 📋 Final nodes config summary:`);
    finalNormalizedGraph.nodes.forEach((node: any) => {
      const nodeType = unifiedNormalizeNodeType(node);
      const config = node.data?.config || {};
      const configKeys = Object.keys(config).filter(k => config[k] !== undefined && config[k] !== '' && !k.startsWith('_'));
      if (configKeys.length > 0) {
        logger.info(`[AttachInputs]   Node ${node.id} (${nodeType}): ${configKeys.map(k => `${k}=${typeof config[k] === 'string' && config[k].length > 30 ? config[k].substring(0, 30) + '...' : config[k]}`).join(', ')}`);
      }
    });

    const effectiveFillModes = collectEffectiveFillModesForWizard(finalNormalizedGraph.nodes as any[]);

    return { statusCode: 200, body: ({
      success: true,
      workflow: finalNormalizedGraph,
      nodes: finalNormalizedGraph.nodes,
      edges: finalNormalizedGraph.edges,
      validation: {
        valid: validation.valid,
        errors: validation.errors.map(e => e.message),
        warnings: validation.warnings.map(w => w.message),
      },
      status: nextStatus,
      phase: nextPhase,
      ready: nextPhase === 'ready_for_execution',
      message: nextPhase === 'ready_for_execution' 
        ? 'Node inputs injected successfully. Workflow is ready for execution.'
        : 'Node inputs injected successfully. Credentials required.',
      diagnostics: {
        ...modeDiagnostics,
        effectiveFillModes,
        contract: contractDiagnostics,
        freezePolicy: 'topology_only' as const,
        postFreezeReadonly: isPostFreezeReadonly,
        protectedConfigFingerprint: finalProtectedConfigFingerprint.fingerprint,
        invalidBareNodeIdInputKeys,
      },
      buildAiUsage: snapshotBuildAiUsage(),
    }) };
  } catch (error) {
    logger.error('[AttachInputs] ❌ Unhandled error:', error);
    logger.error('[AttachInputs] Error stack:', error instanceof Error ? error.stack : 'No stack trace');
    logger.error('[AttachInputs] Error details:', {
      name: error instanceof Error ? error.name : typeof error,
      message: error instanceof Error ? error.message : String(error),
      workflowId: req.params.workflowId || req.body?.workflowId,
    });
    return { statusCode: 500, body: ({
      error: 'Failed to attach inputs',
      details: error instanceof Error ? error.message : String(error),
      code: error instanceof Error && 'code' in error ? (error as any).code : 'UNKNOWN_ERROR',
      hint: 'Check server logs for detailed error information. This may be due to database connection issues, invalid workflow structure, or missing dependencies.',
    }) };
  }
  });
}

/**
 * Attach-inputs route handler with in-flight deduplication.
 *
 * When multiple concurrent POST /attach-inputs requests arrive for the same
 * workflowId, only the first runs the full pipeline. Subsequent concurrent
 * callers await the same Promise and receive the same result, preventing
 * duplicate topology fingerprint writes and `Post-freeze fingerprint mismatch`
 * warnings.
 */
export default async function attachInputsHandler(req: Request, res: Response) {
  const workflowId = req.params.workflowId || req.body?.workflowId;

  if (attachInputsInFlight.has(workflowId)) {
    const result = await attachInputsInFlight.get(workflowId)! as { statusCode: number; body: unknown };
    return res.status(result.statusCode).json(result.body);
  }

  const pipelinePromise = runAttachInputsPipeline(req, res);
  attachInputsInFlight.set(workflowId, pipelinePromise);
  try {
    const result = await pipelinePromise as { statusCode: number; body: unknown };
    return res.status(result.statusCode).json(result.body);
  } finally {
    attachInputsInFlight.delete(workflowId);
  }
}
