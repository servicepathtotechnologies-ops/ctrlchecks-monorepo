/**
 * Capability Selection — Phase 3: Confirm
 *
 * POST /api/capability-selection/confirm
 *
 * Accepts the validated Workflow from the review step and passes it through
 * the existing AiFirstPipeline downstream stages: credential discovery,
 * property population, and field ownership. Any further structural mutations
 * go through unifiedGraphOrchestrator.injectNode(...) followed by validateWorkflow.
 *
 * Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6
 */

import { Response } from 'express';
import { randomUUID } from 'crypto';
import { unifiedGraphOrchestrator } from '../../core/orchestration/unified-graph-orchestrator';
import { unifiedNodeRegistry } from '../../core/registry/unified-node-registry';
import type { AuthenticatedRequest } from '../../core/middleware/subscription-auth';
import { runCredentialDiscoveryStage } from '../../services/ai/stages/credential-discovery-stage';
import { runPropertyPopulationStage } from '../../services/ai/stages/property-population-stage';
import { runFieldOwnershipStage } from '../../services/ai/stages/field-ownership-stage';
import { attachCanonicalPipelineMetadata } from '../../services/ai/ai-first-pipeline';
import { generateComprehensiveNodeQuestions } from '../../services/ai/comprehensive-node-questions-generator';
import { hydrateRequiredConfigFromRegistryDefaults } from '../../core/validation/workflow-config-hydrator';
import type { Workflow, WorkflowNode } from '../../core/types/ai-types';
import type { SwitchContext } from '../../core/orchestration/unified-graph-orchestrator';
import type { CaseNodeMapping } from '../../core/types/unified-node-contract';
import { compileSummaryV2FromWorkflow } from '../../services/ai/summary-v2-compiler';
import { validateSummaryV2 } from '../../core/validation/summary-v2-validator';
import { logger } from '../../core/logger';

/**
 * Stamp _fillMode.fields = 'buildtime_ai_once' on every form/form_trigger node
 * that has a non-empty fields array. This signals attach-inputs to preserve
 * the fields that were shown in the Field Ownership UI rather than re-deriving
 * them from intent text (which can prune fields the user already reviewed).
 */
function stampFormFieldsAsBuilt(workflow: Workflow): Workflow {
  const nodes = (workflow.nodes || []).map((node: any) => {
    const nodeType = String(node?.data?.type || node?.type || '');
    if (nodeType !== 'form' && nodeType !== 'form_trigger') return node;
    const fields = node?.data?.config?.fields;
    if (!Array.isArray(fields) || fields.length === 0) return node;
    const config = { ...(node.data.config || {}) };
    config._fillMode = { ...(config._fillMode || {}), fields: 'buildtime_ai_once' };
    return { ...node, data: { ...node.data, config } };
  });
  return { ...workflow, nodes };
}

/**
 * Build a SwitchContext from populated workflow nodes.
 *
 * After property population, each switch/if_else node has its cases filled in.
 * This function reads those cases and maps each case value to the correct
 * downstream node ID, enabling initializeWorkflow to wire branch edges correctly
 * for any depth of nesting.
 *
 * Algorithm: for each branching node, assign its N cases to the next N "slots"
 * in the downstream node list. A slot is either:
 *   - A non-branching node (leaf terminal like Gmail, Slack, log_output) = 1 slot
 *   - A branching node + all nodes it owns recursively = 1 slot (the nested branch)
 *
 * Works universally for:
 *   - Single switch/if_else
 *   - Nested switch inside switch
 *   - Mixed if_else inside switch
 *   - N levels deep, any combination
 */
function buildSwitchContextFromPopulatedNodes(nodes: WorkflowNode[]): SwitchContext | undefined {
  const getNodeType = (n: WorkflowNode): string =>
    String((n.data as any)?.type || n.type || '');

  const isBranchingNode = (n: WorkflowNode): boolean =>
    unifiedNodeRegistry.get(getNodeType(n))?.isBranching === true;

  const getCaseCount = (n: WorkflowNode): number => {
    const config = (n.data as any)?.config ?? {};
    const rawCases = config.cases ?? config.rules ?? [];
    if (Array.isArray(rawCases) && rawCases.length > 0) return rawCases.length;
    if (getNodeType(n) === 'if_else') return 2;
    return 0;
  };

  const getCaseValues = (n: WorkflowNode): string[] => {
    const config = (n.data as any)?.config ?? {};
    const rawCases = config.cases ?? config.rules ?? [];
    if (Array.isArray(rawCases) && rawCases.length > 0) {
      const vals = rawCases.map((c: any) =>
        typeof c === 'string' ? c : String(c?.value ?? c?.label ?? '')
      ).filter(Boolean);
      if (vals.length > 0) return vals;
    }
    if (getNodeType(n) === 'if_else') return ['true', 'false'];
    return [];
  };

  /**
   * Count how many nodes a branching node at startIdx "owns" (its descendants).
   * A branching node with N cases owns N downstream slots.
   * Each slot is either 1 leaf node or 1 nested branching node + its owned count.
   */
  function countOwnedNodes(startIdx: number, caseCount: number): number {
    if (caseCount === 0) return 0;
    let consumed = 0;
    let slotsAssigned = 0;
    let i = startIdx;

    while (i < nodes.length && slotsAssigned < caseCount) {
      const node = nodes[i];
      if (isBranchingNode(node)) {
        const innerCases = getCaseCount(node);
        const innerOwned = countOwnedNodes(i + 1, innerCases);
        consumed += 1 + innerOwned;
        i += 1 + innerOwned;
      } else {
        consumed += 1;
        i += 1;
      }
      slotsAssigned++;
    }
    return consumed;
  }

  const switchContexts: Array<{ switchNodeId: string; caseNodeMapping: CaseNodeMapping }> = [];

  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    if (!isBranchingNode(node)) continue;

    const caseValues = getCaseValues(node);
    if (caseValues.length === 0) continue;

    const mapping: CaseNodeMapping = {};
    let cursor = i + 1;
    let slotsAssigned = 0;

    while (cursor < nodes.length && slotsAssigned < caseValues.length) {
      const child = nodes[cursor];
      const caseValue = caseValues[slotsAssigned];

      mapping[caseValue] = { targetNodeId: child.id, targetNodeType: getNodeType(child) };

      if (isBranchingNode(child)) {
        const innerCases = getCaseCount(child);
        const innerOwned = countOwnedNodes(cursor + 1, innerCases);
        cursor += 1 + innerOwned;
      } else {
        cursor += 1;
      }
      slotsAssigned++;
    }

    if (Object.keys(mapping).length > 0) {
      switchContexts.push({ switchNodeId: node.id, caseNodeMapping: mapping });
    }
  }

  if (switchContexts.length === 0) return undefined;

  return {
    switchNodeId: switchContexts[0].switchNodeId,
    caseNodeMapping: switchContexts[0].caseNodeMapping,
    switchContexts,
  };
}

export default async function confirmCapabilityWorkflow(req: AuthenticatedRequest, res: Response): Promise<void> {
  try {
    const body = req.body as Record<string, unknown>;
    const correlationId =
      typeof body.correlationId === 'string' && body.correlationId.trim()
        ? body.correlationId.trim()
        : randomUUID();
    const workflow = body.workflow as Workflow | undefined;
    const userPrompt = typeof body.userPrompt === 'string' ? body.userPrompt.trim() : '';
    const structuralPrompt = typeof body.structuralPrompt === 'string' ? body.structuralPrompt.trim() : userPrompt;
    const bodyUserId = typeof body.userId === 'string' ? body.userId.trim() : '';
    const userId = req.user?.id || bodyUserId;

    if (!workflow || !workflow.nodes || !Array.isArray(workflow.nodes)) {
      res.status(400).json({ ok: false, code: 'MISSING_WORKFLOW', message: 'workflow is required' });
      return;
    }
    if (!userPrompt) {
      res.status(400).json({ ok: false, code: 'MISSING_PROMPT', message: 'userPrompt is required' });
      return;
    }
    if (!userId) {
      res.status(401).json({ ok: false, code: 'AUTH_REQUIRED', message: 'Authenticated user is required' });
      return;
    }

    // Validate the incoming workflow before running downstream stages (Req 6.1)
    // Note: the workflow from Phase 2 is a preview-only workflow with edges: [] —
    // we only validate that nodes are present, not that edges are wired.
    // The real graph is rebuilt after property population below.
    if (!workflow.nodes || workflow.nodes.length === 0) {
      res.status(422).json({
        ok: false,
        code: 'ORCHESTRATOR_VALIDATION_FAILED',
        message: 'Workflow has no nodes',
      });
      return;
    }

    // Stage: Property Population (Req 6.2)
    const ppResult = await runPropertyPopulationStage({
      workflow,
      userIntent: userPrompt,
      structuralPrompt,
      correlationId,
    });
    // Property population is always ok: true (soft-failing stage)
    let populatedWorkflow = ppResult.workflow;

    // ── Rebuild edges after property population ───────────────────────────
    // Property population fills switch.cases and if_else.conditions.
    // We build a SwitchContext from the populated cases so initializeWorkflow
    // can wire branch edges correctly for any depth of nesting:
    //   - single switch, single if_else
    //   - nested switch inside switch
    //   - mixed if_else inside switch
    //   - N levels deep, any combination
    try {
      const switchContext = buildSwitchContextFromPopulatedNodes(populatedWorkflow.nodes);
      const rebuilt = unifiedGraphOrchestrator.initializeWorkflow(
        populatedWorkflow.nodes,
        undefined,
        undefined,
        switchContext,
      );
      let rebuiltWorkflow = rebuilt.workflow;

      // ── Post-rebuild: remove spurious log_output fan-in edges ────────────
      // When log_output is a direct case target of a branching node (switch/if_else),
      // the EdgeReconciliationEngine Step 7 may also wire non-branching action nodes
      // (e.g. slack, gmail) to the same log_output, creating illegal fan-in.
      //
      // Rule: for each log_output that already has a legitimate case edge from a
      // branching node, remove all other (non-case) incoming edges to that log_output.
      // If log_output has NO case edge from a branching node, leave all its edges
      // intact (it is a regular terminal wired from the last action node by Step 7).
      const getNodeType = (n: any): string => String(n?.data?.type || n?.type || '');
      const logOutputNodes = rebuiltWorkflow.nodes.filter(
        (n: any) => getNodeType(n) === 'log_output'
      );
      if (logOutputNodes.length > 0) {
        const logOutputIds = new Set(logOutputNodes.map((n: any) => n.id));
        const branchingNodeIds = new Set(
          rebuiltWorkflow.nodes
            .filter((n: any) => unifiedNodeRegistry.get(getNodeType(n))?.isBranching === true)
            .map((n: any) => n.id)
        );
        // Collect log_output nodes that have at least one case edge from a branching node.
        // Only these need fan-in cleanup — others are regular terminals and must be left alone.
        const logOutputsWithCaseEdge = new Set<string>();
        const legitimateEdgeIds = new Set<string>();
        for (const edge of rebuiltWorkflow.edges) {
          if (logOutputIds.has(edge.target) && branchingNodeIds.has(edge.source)) {
            legitimateEdgeIds.add(edge.id);
            logOutputsWithCaseEdge.add(edge.target);
          }
        }
        if (logOutputsWithCaseEdge.size > 0) {
          // Remove non-legitimate incoming edges only for log_output nodes that have a case edge.
          const cleanedEdges = rebuiltWorkflow.edges.filter((e: any) => {
            if (!logOutputsWithCaseEdge.has(e.target)) return true; // not a case-targeted log_output — keep
            return legitimateEdgeIds.has(e.id); // only keep legitimate switch/if_else→log_output case edges
          });
          logger.info(`[CapabilityConfirm] log_output fan-in cleanup: ${rebuiltWorkflow.edges.length} → ${cleanedEdges.length} edges`);
          rebuiltWorkflow = { ...rebuiltWorkflow, edges: cleanedEdges };
        }
      }

      populatedWorkflow = {
        ...rebuiltWorkflow,
        metadata: populatedWorkflow.metadata,
      };
    } catch (reconcileErr) {
      logger.warn('[CapabilitySelection/confirm] Edge rebuild failed, falling back to reconcile:', reconcileErr);
      try {
        const reconciled = unifiedGraphOrchestrator.reconcileWorkflow(populatedWorkflow);
        populatedWorkflow = reconciled.workflow;
      } catch (fallbackErr) {
        logger.warn('[CapabilitySelection/confirm] Fallback reconcile also failed (non-fatal):', fallbackErr);
      }
    }

    // ── Stamp LLM-generated fields as built (no field pruning) ───────────────
    // Field normalization (materializeStructuralFields + applyStructuralIntentAlignment)
    // is intentionally skipped — it reduces LLM-generated fields to what the intent
    // text mentions, which shrinks the form from 2-3 fields to 1 and hurts UX.
    // We only apply registry defaults and stamp fields so attach-inputs preserves
    // everything the LLM generated without re-deriving.
    try {
      const withDefaults = hydrateRequiredConfigFromRegistryDefaults(populatedWorkflow as any) as any;
      populatedWorkflow = stampFormFieldsAsBuilt(withDefaults as Workflow);
    } catch (stampErr) {
      logger.warn('[CapabilityConfirm] Field stamp failed (non-fatal):', stampErr);
    }

    // Stage: Credential Discovery (Req 6.3) — non-blocking
    const cdResult = await runCredentialDiscoveryStage(populatedWorkflow, userId, correlationId);
    const requiredCredentials = cdResult.ok ? cdResult.requiredCredentials : [];
    const missingCredentials = cdResult.ok ? cdResult.missingCredentials : [];

    // Stage: Field Ownership (Req 6.4)
    const foResult = await runFieldOwnershipStage(populatedWorkflow, correlationId);

    // Generate comprehensive questions for the field-ownership wizard
    let comprehensiveQuestions: any[] = [];
    try {
      const qResult = generateComprehensiveNodeQuestions(populatedWorkflow, {}, { mode: 'full_configuration' });
      comprehensiveQuestions = qResult.questions ?? [];
    } catch {
      // non-blocking — wizard will fall back to fieldOwnershipMap synthesis
    }

    // Attach canonical pipeline metadata (Req 6.5)
    const finalWorkflow = attachCanonicalPipelineMetadata(populatedWorkflow, {
      userPrompt,
      structuralPrompt,
      correlationId,
    });

    const confirmValidation = unifiedGraphOrchestrator.validateWorkflow(finalWorkflow);
    if (!confirmValidation.valid) {
      res.status(422).json({
        ok: false,
        code: 'ORCHESTRATOR_VALIDATION_FAILED',
        message: 'Workflow validation failed before summary compilation',
        violations: confirmValidation.errors,
      });
      return;
    }
    const summaryV2 = compileSummaryV2FromWorkflow(finalWorkflow, userPrompt);
    const summaryValidation = validateSummaryV2(summaryV2);
    if (!summaryValidation.valid) {
      res.status(422).json({
        ok: false,
        code: 'SUMMARY_V2_CONTRACT_FAILED',
        message: 'summaryV2 contract validation failed',
        violations: summaryValidation.errors,
      });
      return;
    }

    res.status(200).json({
      ok: true,
      workflow: finalWorkflow,
      summaryV2,
      requiredCredentials: requiredCredentials.map((c) => c.vaultKey || c.displayName || c.provider),
      missingCredentials: missingCredentials.map((c) => c.vaultKey || c.displayName || c.provider),
      discoveredCredentials: missingCredentials,
      fieldOwnershipMap: foResult.fieldOwnershipMap,
      propertyPopulationSummary: ppResult.propertyPopulationSummary,
      comprehensiveQuestions,
      correlationId,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    logger.error('[CapabilitySelection/confirm] Unhandled error:', message);
    res.status(500).json({ ok: false, code: 'INTERNAL_ERROR', message });
  }
}
