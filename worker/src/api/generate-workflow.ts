/**
 * Generate Workflow — Workflow Generation Pipeline Entry Point
 *
 * mode: 'analyze' → Uses WorkflowGenerationPipeline to generate plan summary
 * mode: 'refine'  → WorkflowGenerationPipeline (builds the actual workflow graph)
 * (no mode)       → WorkflowGenerationPipeline (direct generation)
 *
 * WorkflowGenerationPipeline is the ONLY pipeline — no AiFirstPipeline, no dual paths.
 *
 * Requirements: 9.1, 9.3
 */

import { Request, Response } from 'express';
import { randomUUID } from 'crypto';
import { WorkflowGenerationPipeline } from '../services/ai/pipeline/workflow-generation-pipeline';
import { runIntentStage } from '../services/ai/stages/intent-stage';
import { runIntentStageRemote } from '../services/ai/stages/intent-stage-client';
import type { StructuredIntent } from '../services/ai/stages/intent-stage';
import { runCapabilitySelectionStage } from '../services/ai/stages/capability-selection-stage';
import { runStructuralPromptStage } from '../services/ai/stages/structural-prompt-stage';
import { runStructuralPromptStageRemote } from '../services/ai/stages/structural-prompt-stage-client';
import { runNodeSelectionStage } from '../services/ai/stages/node-selection-stage';
import { buildNodeCatalogText } from '../services/ai/node-catalog-builder';
import { generateComprehensiveNodeQuestions } from '../services/ai/comprehensive-node-questions-generator';
import {
  inferLinearBranchingFromSelection,
  linearPlanChainFromSelection,
  resolvePreferredTerminalNodeType,
} from '../core/utils/workflow-build-manifest-utils';
import { unifiedNodeRegistry } from '../core/registry/unified-node-registry';
import { unifiedGraphOrchestrator } from '../core/orchestration/unified-graph-orchestrator';
import type { WorkflowNode } from '../core/types/ai-types';
import { compileSummaryV2FromWorkflow } from '../services/ai/summary-v2-compiler';
import { validateSummaryV2 } from '../core/validation/summary-v2-validator';
import { GeminiWalletError } from '../services/ai/gemini-wallet-service';
import { logger } from '../core/logger';

const pipeline = new WorkflowGenerationPipeline();

function hydratePreviewNode(nodeType: string, index: number): WorkflowNode {
  const normalizedType = unifiedNodeRegistry.resolveAlias(nodeType) || nodeType;
  const def = unifiedNodeRegistry.get(normalizedType);
  return {
    id: `preview_${index}_${normalizedType}`,
    type: normalizedType,
    data: {
      label: def?.label || normalizedType,
      type: normalizedType,
      category: def?.category || 'utility',
      config: { ...(unifiedNodeRegistry.getDefaultConfig(normalizedType) || {}) },
    },
  };
}

function parseStructuredIntentSnapshot(value: unknown): StructuredIntent | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const obj = value as Record<string, unknown>;
  const triggerType = String(obj.triggerType || '').trim();
  const allowedTrigger = new Set(['schedule', 'webhook', 'form', 'chat_trigger', 'manual_trigger']);
  if (!allowedTrigger.has(triggerType)) return undefined;
  if (!Array.isArray(obj.actions)) return undefined;
  const intentStr = String(obj.intent || '').trim();
  return {
    intent: intentStr,
    triggerType: triggerType as StructuredIntent['triggerType'],
    actions: obj.actions.map((x) => String(x || '').trim()).filter((x) => x.length > 0),
    dataFlows: Array.isArray(obj.dataFlows) ? (obj.dataFlows as StructuredIntent['dataFlows']) : [],
    constraints: Array.isArray(obj.constraints) ? obj.constraints.map((x) => String(x || '')).filter((x) => x.length > 0) : [],
    originalPrompt: String(obj.originalPrompt || intentStr).trim(),
  };
}

function resolveAnalyzeCapabilitySelections(
  steps: Array<{
    stepId: string;
    candidateNodeTypes: string[];
    defaultSuggestedNodeType: string | null;
    selectionPolicy?: { multiSelectAllowed?: boolean; required?: boolean };
  }>,
  userSelectionsByStep: Record<string, string[]> | undefined,
):
  | { ok: true; byStep: Record<string, string[]>; flat: string[] }
  | { ok: false; message: string; invalidByStep: Record<string, string[]> } {
  const hasExplicitSelections = !!(userSelectionsByStep && Object.keys(userSelectionsByStep).length > 0);
  const byStep: Record<string, string[]> = {};
  const globalAllowed = new Set(steps.flatMap((step) => step.candidateNodeTypes));
  const normalizedGlobalRequested = hasExplicitSelections
    ? [
        ...new Set(
          Object.values(userSelectionsByStep || {})
            .flatMap((raw) => (Array.isArray(raw) ? raw : []))
            .map((t) => unifiedNodeRegistry.resolveAlias(String(t || '').trim()) || String(t || '').trim())
            .filter((t) => t.length > 0),
        ),
      ]
    : [];
  const unknownRequested = normalizedGlobalRequested.filter((t) => !globalAllowed.has(t));
  if (unknownRequested.length > 0) {
    return {
      ok: false,
      message: `Invalid capability selections submitted (unknown node types: ${unknownRequested.join(', ')})`,
      invalidByStep: { _global: unknownRequested },
    };
  }
  const globallyAssigned = new Set<string>();

  for (const step of steps) {
    const allowed = new Set(step.candidateNodeTypes);
    const raw = Array.isArray(userSelectionsByStep?.[step.stepId]) ? userSelectionsByStep?.[step.stepId] : [];
    const normalized = raw
      .map((t) => unifiedNodeRegistry.resolveAlias(String(t || '').trim()) || String(t || '').trim())
      .filter((t) => t.length > 0);
    const selectedForStep = [...new Set(normalized.filter((t) => allowed.has(t)))];

    if (hasExplicitSelections) {
      const fallbackCompatible = normalizedGlobalRequested.filter((t) => {
        if (!allowed.has(t)) return false;
        const def = unifiedNodeRegistry.get(t);
        // Branching types (switch, if_else) can appear in multiple steps — don't block them
        if (def?.isBranching === true) return true;
        return !globallyAssigned.has(t);
      });
      const combined = [...new Set([...selectedForStep, ...fallbackCompatible])];
      const limited = step.selectionPolicy?.multiSelectAllowed === false ? combined.slice(0, 1) : combined;
      if (step.selectionPolicy?.required !== false && limited.length === 0) {
        return {
          ok: false,
          message: `Required capability step "${step.stepId}" has no selected registry node`,
          invalidByStep: { [step.stepId]: [] },
        };
      }
      byStep[step.stepId] = limited;
      limited.forEach((x) => globallyAssigned.add(x));
      continue;
    }

    byStep[step.stepId] = step.defaultSuggestedNodeType ? [step.defaultSuggestedNodeType] : [];
  }

  return {
    ok: true,
    byStep,
    flat: [...new Set(Object.values(byStep).flat())],
  };
}

export default async function generateWorkflow(req: Request, res: Response): Promise<void> {
  const correlationId = randomUUID();

  try {
    const body = req.body as Record<string, unknown>;
    const mode = String(body.mode || '').toLowerCase();

    const userPrompt = String(
      body.prompt ||
      body.refinedPrompt ||
      body.originalPrompt ||
      ''
    ).trim();

    if (!userPrompt) {
      res.status(400).json({ success: false, error: 'prompt is required' });
      return;
    }

    // ── mode: analyze → AI-first intent + structural prompt (replaces failing legacy clarifier) ──
    if (mode === 'analyze') {
      const nodeCatalog = buildNodeCatalogText();
      const analyzeCapabilitySelectionsByStep =
        body.capabilitySelectionsByStep && typeof body.capabilitySelectionsByStep === 'object'
          ? (body.capabilitySelectionsByStep as Record<string, string[]>)
          : undefined;
      const providedIntentSnapshot = parseStructuredIntentSnapshot(body.intentSnapshot);

      const terminalFallback = resolvePreferredTerminalNodeType();
      // Stage 1: extract structured intent (or reuse frozen snapshot from previous analyze).
      // Delegates to ai-generator when AI_GENERATOR_URL is set; falls back to in-process.
      const intentFromLlm = providedIntentSnapshot
        ? undefined
        : (await runIntentStageRemote(userPrompt, nodeCatalog, correlationId)) ??
          await runIntentStage(userPrompt, nodeCatalog, correlationId);

      if (!providedIntentSnapshot && intentFromLlm && !intentFromLlm.ok) {
        res.json({
          phase: 'summarize',
          workflowIntentPlan: {
            structuredSummary: userPrompt,
            proposedNodeChain: ['manual_trigger', terminalFallback],
            mandatoryNodeTypes: ['manual_trigger'],
          },
          matchedKeywords: [],
          mandatoryNodeTypes: ['manual_trigger'],
          capabilityOptions: [],
          intentSnapshot: null,
          correlationId,
        });
        return;
      }
      const intentForAnalyze = providedIntentSnapshot || (intentFromLlm && intentFromLlm.ok ? intentFromLlm.intent : undefined);
      if (!intentForAnalyze) {
        res.status(422).json({
          success: false,
          error: 'INTENT_FAILED',
          message: 'Unable to resolve intent for analyze flow.',
          correlationId,
        });
        return;
      }

      // Stage 2: capability options from registry
      const capabilityResult = await runCapabilitySelectionStage(intentForAnalyze, correlationId);
      const capabilityOptions = capabilityResult.ok ? capabilityResult.steps : [];
      const resolvedSelections = resolveAnalyzeCapabilitySelections(capabilityOptions, analyzeCapabilitySelectionsByStep);
      if (!resolvedSelections.ok) {
        res.status(422).json({
          success: false,
          error: 'CAPABILITY_SELECTION_FAILED',
          message: resolvedSelections.message,
          invalidSelectionsByStep: resolvedSelections.invalidByStep,
          capabilityOptions,
          correlationId,
        });
        return;
      }
      const selectedNodeConstraintsByStep = resolvedSelections.byStep;
      const selectedNodeConstraintsFlat = resolvedSelections.flat;

      // Stage 3: generate structural blueprint
      const structuralPromptConstraints = {
        selectedNodeConstraintsByStep,
        selectedNodeConstraintsFlat,
      };
      const spResult =
        (await runStructuralPromptStageRemote(intentForAnalyze, nodeCatalog, correlationId, structuralPromptConstraints)) ??
        await runStructuralPromptStage(intentForAnalyze, nodeCatalog, correlationId, structuralPromptConstraints);
      const structuredSummary = spResult.ok ? spResult.structuralPrompt : intentForAnalyze.intent;
      const structuralForSelection = spResult.ok ? spResult.structuralPrompt : undefined;

      // Stage 4: same node selection as full pipeline — registry-grounded chain (no fixed log_output suffix)
      const nodeSelectionConstraints = {
        selectedNodeConstraintsByStep,
        selectedNodeConstraintsFlat,
        requiredNodeTypes: selectedNodeConstraintsFlat,
      };
      const nsResult = await runNodeSelectionStage(
        intentForAnalyze,
        nodeCatalog,
        correlationId,
        structuralForSelection,
        nodeSelectionConstraints,
      );

      let proposedNodeChain: string[];
      if (nsResult.ok && nsResult.selectedNodes.length > 0) {
        if (inferLinearBranchingFromSelection(nsResult.selectedNodes)) {
          proposedNodeChain = linearPlanChainFromSelection(nsResult.selectedNodes);
        } else {
          proposedNodeChain = nsResult.selectedNodes.map((n) => n.type);
        }
      } else {
        const trig = intentForAnalyze.triggerType || 'manual_trigger';
        const actions = intentForAnalyze.actions
          .filter((a) => unifiedNodeRegistry.has(a))
          .slice(0, 8);
        proposedNodeChain = [trig, ...actions, terminalFallback];
      }

      const previewNodes = proposedNodeChain.map((type, index) => hydratePreviewNode(type, index));
      const initialized = unifiedGraphOrchestrator.initializeWorkflow(previewNodes);
      const reconciled = unifiedGraphOrchestrator.reconcileWorkflow(initialized.workflow);
      const validation = unifiedGraphOrchestrator.validateWorkflow(reconciled.workflow, reconciled.executionOrder);
      if (!validation.valid) {
        res.status(422).json({
          success: false,
          error: 'ORCHESTRATOR_VALIDATION_FAILED',
          message: 'Failed to compile a valid preview graph for structural summary',
          violations: validation.errors,
          correlationId,
        });
        return;
      }
      const summaryV2 = compileSummaryV2FromWorkflow(reconciled.workflow, userPrompt);
      const summaryValidation = validateSummaryV2(summaryV2);
      if (!summaryValidation.valid) {
        res.status(422).json({
          success: false,
          error: 'SUMMARY_V2_CONTRACT_FAILED',
          message: 'summaryV2 contract validation failed',
          violations: summaryValidation.errors,
          correlationId,
        });
        return;
      }

      res.json({
        phase: 'summarize',
        workflowIntentPlan: {
          structuredSummary,
          summaryV2,
          proposedNodeChain,
          mandatoryNodeTypes: [intentForAnalyze.triggerType || 'manual_trigger'],
          nodeInclusionReasons: intentForAnalyze.actions.reduce((acc: Record<string, string>, a: string) => {
            acc[a] = `Required for: ${a.replace(/_/g, ' ')}`;
            return acc;
          }, {}),
        },
        matchedKeywords: intentForAnalyze.actions,
        mandatoryNodeTypes: [intentForAnalyze.triggerType || 'manual_trigger'],
        capabilityOptions,
        appliedCapabilitySelectionsByStep: selectedNodeConstraintsByStep,
        intentSnapshot: intentForAnalyze,
        correlationId,
      });
      return;
    }

    // ── mode: refine (or no mode) → AI-First Pipeline ──────────────────────
    const userId = String(body.userId || body.user_id || 'anonymous');

    const existingWorkflow = body.existingWorkflow as any | undefined;
    const capabilitySelectionsByStep =
      body.capabilitySelectionsByStep && typeof body.capabilitySelectionsByStep === 'object'
        ? (body.capabilitySelectionsByStep as Record<string, string[]>)
        : undefined;

    const rawStreamHeader = req.headers['x-stream-progress'];
    const streamHeaderValue = Array.isArray(rawStreamHeader) ? rawStreamHeader[0] : rawStreamHeader;
    const isStreaming = ['true', '1', 'yes'].includes(String(streamHeaderValue || '').toLowerCase());
    logger.info(`[GenerateWorkflow] correlationId=${correlationId} mode=${mode || 'default'} stream=${isStreaming}`);

    if (isStreaming) {
      // ── Streaming mode: emit NDJSON stage events ──────────────────────────
      res.setHeader('Content-Type', 'application/x-ndjson; charset=utf-8');
      res.setHeader('Transfer-Encoding', 'chunked');
      res.setHeader('Cache-Control', 'no-cache, no-transform');
      res.setHeader('Connection', 'keep-alive');
      res.setHeader('X-Accel-Buffering', 'no');
      res.setHeader('X-Stream-Mode', 'ndjson');
      res.flushHeaders();

      const writeEvent = (event: object) => {
        res.write(JSON.stringify(event) + '\n');
        // Force chunk flush when available (helps behind reverse proxies).
        const flush = (res as Response & { flush?: () => void }).flush;
        if (typeof flush === 'function') {
          flush.call(res);
        }
      };

      writeEvent({ current_phase: 'initializing', progress_percentage: 1, log: 'Initializing generation pipeline...' });

      const result = await pipeline.run({
        userPrompt,
        userId,
        correlationId,
        existingWorkflow,
        capabilitySelectionsByStep,
        onStageComplete: (stageName, progress, log) => {
          writeEvent({ current_phase: stageName, progress_percentage: progress, log });
        },
      });

      if (!result.ok) {
        const failedStage = Array.isArray(result.stageTrace)
          ? [...result.stageTrace].reverse().find((s) => typeof s?.error === 'string')?.stage
          : undefined;
        writeEvent({
          status: 'error',
          error: (result as any).error ?? (result as any).code,
          message: result.message,
          stage: failedStage,
          stageTrace: result.stageTrace,
          correlationId,
        });
        res.end();
        return;
      }

      // Handle capability options needed — return early so UI can show Node_Selection_UI
      if ((result as any).needsCapabilitySelection) {
        writeEvent({
          status: 'capability_selection_needed',
          phase: 'capability_selection',
          capabilityOptions: (result as any).capabilityOptions,
          stageTrace: result.stageTrace,
          correlationId,
        });
        res.end();
        return;
      }

      // At this point result is Stage3Output
      const stage3 = result as any;

      // Finalizing sentinel before terminal payload
      writeEvent({ current_phase: 'finalizing', progress_percentage: 99, log: 'Finalizing workflow...' });

      // Build credentialStatuses for terminal payload
      const streamingCredentialStatuses = (stage3.requiredCredentials ?? []).flatMap((req: any) => {
        const credentialId = (req.vaultKey || req.provider || '').toLowerCase().trim()
          .replace(/^gmail$/, 'google');
        const status = req.satisfied ? 'resolved_connected' : 'required_missing';
        const nodeIds = Array.isArray(req.nodeIds) && req.nodeIds.length > 0
          ? req.nodeIds
          : ['unknown'];
        const displayName =
          (typeof req.displayName === 'string' && req.displayName.trim()) ||
          (typeof req.vaultKey === 'string' && req.vaultKey.trim()) ||
          (typeof req.provider === 'string' && req.provider.trim()) ||
          credentialId ||
          'Credential';
        return nodeIds.map((nodeId: string) => ({ nodeId, credentialId, status, displayName }));
      });

      // Terminal payload as a single NDJSON line
      writeEvent({
        status: 'success',
        success: true,
        phase: 'ready',
        workflow: stage3.workflow,
        validationIssues: stage3.validationIssues,
        comprehensiveQuestions: (() => {
          try {
            const qResult = generateComprehensiveNodeQuestions(stage3.workflow, {}, { mode: 'full_configuration' });
            return qResult.questions ?? [];
          } catch {
            return [];
          }
        })(),
        requiredCredentials: (stage3.requiredCredentials ?? []).map((c: any) => c.vaultKey || c.displayName || c.provider),
        missingCredentials: (stage3.missingCredentials ?? []).map((c: any) => c.vaultKey || c.displayName || c.provider),
        discoveredCredentials: stage3.missingCredentials ?? [],
        credentialStatuses: streamingCredentialStatuses,
        fieldOwnershipMap: stage3.fieldOwnershipMap,
        stageTrace: stage3.stageTrace,
        propertyPopulationSummary: stage3.propertyPopulationSummary,
        capabilityOptions: stage3.capabilityOptions,
        appliedCapabilitySelectionsByStep: stage3.appliedCapabilitySelectionsByStep,
        correlationId,
      });

      res.end();
      return;
    }

    // ── Non-streaming mode (backward-compatible) ──────────────────────────
    const result = await pipeline.run({
      userPrompt,
      userId,
      correlationId,
      existingWorkflow,
      capabilitySelectionsByStep,
    });

    if (!result.ok) {
      res.status(422).json({
        success: false,
        error: (result as any).error ?? (result as any).code,
        message: result.message,
        correlationId,
        stageTrace: result.stageTrace,
      });
      return;
    }

    // Handle capability options needed — return early so UI can show Node_Selection_UI
    if ((result as any).needsCapabilitySelection) {
      res.json({
        success: true,
        phase: 'capability_selection',
        capabilityOptions: (result as any).capabilityOptions,
        stageTrace: result.stageTrace,
        correlationId,
      });
      return;
    }

    // At this point result is Stage3Output
    const stage3ns = result as any;

    // Map requiredCredentials (with satisfied flags from vault queries) into
    // the credentialStatuses shape consumed by filterStillBlockingOAuth() in the wizard.
    const credentialStatuses = (stage3ns.requiredCredentials ?? []).flatMap((req: any) => {
      const credentialId = (req.vaultKey || req.provider || '').toLowerCase().trim()
        .replace(/^gmail$/, 'google'); // normalize gmail → google
      const status = req.satisfied ? 'resolved_connected' : 'required_missing';
      const nodeIds = Array.isArray(req.nodeIds) && req.nodeIds.length > 0
        ? req.nodeIds
        : ['unknown'];
      const displayName =
        (typeof req.displayName === 'string' && req.displayName.trim()) ||
        (typeof req.vaultKey === 'string' && req.vaultKey.trim()) ||
        (typeof req.provider === 'string' && req.provider.trim()) ||
        credentialId ||
        'Credential';
      return nodeIds.map((nodeId: string) => ({ nodeId, credentialId, status, displayName }));
    });

    res.json({
      success: true,
      // Use 'ready' so the existing field-ownership wizard activates in the UI
      // (applyUnifiedWizardFromGenerateUpdate checks for phase === 'ready')
      phase: 'ready',
      workflow: stage3ns.workflow,
      validationIssues: stage3ns.validationIssues,
      // Generate per-node questions (AI pre-fills buildtime_ai_once fields like conditions,
      // titles, switch cases; user fills manual_static; runtime_ai resolved at execution)
      comprehensiveQuestions: (() => {
        try {
          const qResult = generateComprehensiveNodeQuestions(stage3ns.workflow, {}, { mode: 'full_configuration' });
          return qResult.questions ?? [];
        } catch {
          return [];
        }
      })(),
      // Normalize to string array for UI compatibility
      requiredCredentials: (stage3ns.requiredCredentials ?? []).map((c: any) => c.vaultKey || c.displayName || c.provider),
      missingCredentials: (stage3ns.missingCredentials ?? []).map((c: any) => c.vaultKey || c.displayName || c.provider),
      // Full objects for the configure step
      discoveredCredentials: stage3ns.missingCredentials ?? [],
      credentialStatuses,
      fieldOwnershipMap: stage3ns.fieldOwnershipMap,
      stageTrace: stage3ns.stageTrace,
      propertyPopulationSummary: stage3ns.propertyPopulationSummary,
      capabilityOptions: stage3ns.capabilityOptions,
      appliedCapabilitySelectionsByStep: stage3ns.appliedCapabilitySelectionsByStep,
      correlationId,
    });
  } catch (error: any) {
    const message = error instanceof Error ? error.message : String(error);
    if (error instanceof GeminiWalletError) {
      res.status(error.code === 'GEMINI_WALLET_PROVIDER_ERROR' ? 503 : 402).json({
        success: false,
        error: error.code,
        code: error.code,
        message,
        walletStatus: error.walletStatus,
        correlationId,
      });
      return;
    }
    logger.error('[GenerateWorkflow] Unhandled error:', message, error?.stack);
    res.status(500).json({
      success: false,
      error: 'INTERNAL_ERROR',
      message,
      correlationId,
    });
  }
}
