/**
 * Capability Selection — Phase 2: Generate
 *
 * POST /api/capability-selection/generate
 *
 * Accepts Node_Selections from the frontend, reconstructs ordered NodeSelection[],
 * runs the Structural_Prompt_Generator, and returns the structural prompt and
 * validated workflow for user review.
 *
 * Requirements: 4.1, 4.2, 6.6, 7.1
 */

import { Request, Response } from 'express';
import { buildNodeCatalogText } from '../../services/ai/node-catalog-builder';
import { runCapabilityStructuralPromptStage } from '../../services/ai/stages/capability-structural-prompt-stage';
import { validateCapabilitySelections } from '../../services/ai/stages/capability-selection-validation';
import { unifiedNodeRegistry } from '../../core/registry/unified-node-registry';
import type { WorkflowNode, WorkflowEdge } from '../../core/types/ai-types';
import { compileSummaryV2FromWorkflow } from '../../services/ai/summary-v2-compiler';
import { validateSummaryV2 } from '../../core/validation/summary-v2-validator';
import type {
  NodeSelectionMap,
  CapabilityContainer,
  NodeSelection,
} from '../../services/ai/stages/capability-types';
import { logger } from '../../core/logger';

export default async function generateCapabilityWorkflow(req: Request, res: Response): Promise<void> {
  const startedAt = Date.now();

  try {
    const body = req.body as Record<string, unknown>;
    const correlationId = typeof body.correlationId === 'string' ? body.correlationId.trim() : '';
    const userPrompt = typeof body.userPrompt === 'string' ? body.userPrompt.trim() : '';
    const selections = (body.selections ?? {}) as NodeSelectionMap;
    const containers = (body.containers ?? []) as CapabilityContainer[];

    if (!userPrompt) {
      res.status(400).json({ ok: false, code: 'MISSING_PROMPT', message: 'userPrompt is required', selections });
      return;
    }
    if (!containers || containers.length === 0) {
      res.status(400).json({ ok: false, code: 'MISSING_CONTAINERS', message: 'containers is required', selections });
      return;
    }

    const missingOrInvalidSelections = containers.filter((container) => {
      const selected = selections[container.containerId];
      if (!selected) return false; // Unselected containers are allowed — user may skip them
      const canonical = unifiedNodeRegistry.resolveAlias(selected) || selected;
      // Only flag containers where the user DID make a selection but it's invalid
      return !container.candidates.some((candidate) => candidate.nodeType === canonical) || !unifiedNodeRegistry.get(canonical);
    });
    if (missingOrInvalidSelections.length > 0) {
      res.status(422).json({
        ok: false,
        code: 'INVALID_NODE_SELECTION',
        message: 'One or more selected nodes are not valid registry nodes.',
        missingContainerIds: missingOrInvalidSelections.map((container) => container.containerId),
        selections,
      });
      return;
    }

    const selectionValidation = validateCapabilitySelections(containers, selections);
    if (!selectionValidation.valid) {
      res.status(422).json({
        ok: false,
        code: selectionValidation.code,
        message: selectionValidation.message,
        missingContainers: selectionValidation.missingIntentSteps,
        triggerContainers: selectionValidation.triggerContainers,
        selectedTriggerContainers: selectionValidation.selectedTriggerContainers,
        selections,
      });
      return;
    }

    // Reconstruct ordered NodeSelection[] from selections map and containers.
    // Match containerId → nodeType, preserve useCaseUnit.orderIndex order.
    const orderedSelections: NodeSelection[] = containers
      .slice()
      .sort((a, b) => a.useCaseUnit.orderIndex - b.useCaseUnit.orderIndex)
      .filter((container) => selections[container.containerId] !== undefined)
      .map((container) => ({
        containerId: container.containerId,
        useCaseUnit: container.useCaseUnit,
        selectedNodeType: unifiedNodeRegistry.resolveAlias(selections[container.containerId]) || selections[container.containerId],
      }));

    if (orderedSelections.length === 0) {
      res.status(422).json({
        ok: false,
        code: 'NO_VALID_SELECTIONS',
        message: 'No valid selections could be matched to containers.',
        selections,
      });
      return;
    }

    // Build catalog once for this request
    const nodeCatalog = buildNodeCatalogText();

    const result = await runCapabilityStructuralPromptStage({
      userPrompt,
      orderedSelections,
      nodeCatalog,
      correlationId: correlationId || undefined,
    });

    if (!result.ok) {
      res.status(422).json({
        ok: false,
        code: result.code,
        message: result.message,
        selections, // Req 6.6 — preserve selections for retry
      });
      return;
    }

    const durationMs = Date.now() - startedAt;
    const previewNodes: WorkflowNode[] = result.selectedNodeTypes.map((nodeType, index) => {
      const canonicalType = unifiedNodeRegistry.resolveAlias(nodeType) || nodeType;
      const def = unifiedNodeRegistry.get(canonicalType);
      return {
        id: `cap_preview_${index}_${canonicalType}`,
        type: canonicalType,
        data: {
          label: def?.label || canonicalType,
          type: canonicalType,
          category: def?.category || 'utility',
          config: { ...(unifiedNodeRegistry.getDefaultConfig(canonicalType) || {}) },
        },
      };
    });

    // Build a linear preview graph without reconciliation. Branch case values are not
    // yet known at this stage (they come from property population in /confirm), so
    // calling reconcileWorkflow here causes EdgeReconciliationEngine to remove all
    // branching downstream nodes as "orphaned". The real graph is built in confirm.ts.
    const previewEdges: WorkflowEdge[] = previewNodes.slice(0, -1).map((node, index) => ({
      id: `cap_edge_${index}`,
      source: node.id,
      target: previewNodes[index + 1].id,
      sourceHandle: 'output',
      targetHandle: 'input',
    }));
    const previewWorkflow = { nodes: previewNodes, edges: previewEdges };

    const summaryV2 = compileSummaryV2FromWorkflow(previewWorkflow, userPrompt);
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
      structuralPrompt: result.structuralPrompt,
      workflow: result.workflow,
      summaryV2,
      selectedNodeTypes: result.selectedNodeTypes,
      nodeCount: result.nodeCount,
      edgeCount: result.edgeCount,
      durationMs,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    logger.error('[CapabilitySelection/generate] Unhandled error:', message);
    const body = req.body as Record<string, unknown>;
    res.status(500).json({
      ok: false,
      code: 'INTERNAL_ERROR',
      message,
      selections: (body.selections ?? {}) as NodeSelectionMap,
    });
  }
}
