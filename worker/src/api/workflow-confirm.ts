/**
 * Workflow Confirmation API
 * 
 * POST /api/workflow/confirm
 * POST /api/workflow/reject
 * 
 * Handles workflow confirmation and rejection.
 * If approved: continues pipeline execution.
 * If rejected: allows tool replacement or workflow regeneration.
 */

import { Request, Response } from 'express';
import { workflowConfirmationManager, WorkflowState } from '../services/ai/workflow-confirmation-manager';
import { toolSubstitutionEngine } from '../services/ai/tool-substitution-engine';
import { getDbClient } from '../core/database/aws-db-client';
import { nodeLibrary } from '../services/nodes/node-library';
import { unifiedNormalizeNodeType, unifiedNormalizeNodeTypeString } from '../core/utils/unified-node-type-normalizer';
import { pendingCredentialStore } from '../services/ai/pending-credential-store';
import { credentialInjector } from '../services/ai/credential-injector';
import { buildSyncedGraphPayload } from './workflow-graph-state';
import { logger } from '../core/logger';

interface ConfirmRequest {
  workflowId: string;
  approved: boolean;
  toolOverrides?: Record<string, {
    nodeId: string;
    newTool: string;
    reasoning?: string;
  }>;
  nodeOverrides?: Record<string, {
    nodeId: string;
    action: 'replace' | 'remove' | 'add';
    newNodeType?: string;
    newNodeConfig?: Record<string, any>;
    reasoning?: string;
  }>;
  feedback?: string;
}

/**
 * Apply tool overrides to workflow
 */
function applyToolOverrides(
  workflow: { nodes: any[]; edges: any[] },
  toolOverrides: Record<string, { nodeId: string; newTool: string; reasoning?: string }>
): { nodes: any[]; edges: any[] } {
  const updatedNodes = workflow.nodes.map(node => {
    const override = Object.values(toolOverrides).find(o => o.nodeId === node.id);
    if (override) {
      const nodeType = unifiedNormalizeNodeType(node);
      const newSchema = nodeLibrary.getSchema(override.newTool);
      
      if (!newSchema) {
        logger.warn(`[WorkflowConfirm] Tool override failed: ${override.newTool} not found in node library`);
        return node;
      }

      logger.info(`[WorkflowConfirm] Applying tool override: ${nodeType} → ${override.newTool} for node ${node.id}`);
      
      return {
        ...node,
        data: {
          ...node.data,
          type: override.newTool,
          label: newSchema.label || override.newTool,
          category: newSchema.category || node.data?.category,
          config: node.data?.config || {},
        },
      };
    }
    return node;
  });

  return {
    nodes: updatedNodes,
    edges: workflow.edges,
  };
}

/**
 * Apply node overrides to workflow
 */
function applyNodeOverrides(
  workflow: { nodes: any[]; edges: any[] },
  nodeOverrides: Record<string, {
    nodeId: string;
    action: 'replace' | 'remove' | 'add';
    newNodeType?: string;
    newNodeConfig?: Record<string, any>;
    reasoning?: string;
  }>
): { nodes: any[]; edges: any[] } {
  let updatedNodes = [...workflow.nodes];
  let updatedEdges = [...workflow.edges];

  Object.values(nodeOverrides).forEach(override => {
    if (override.action === 'remove') {
      // Remove node and its edges
      logger.info(`[WorkflowConfirm] Removing node ${override.nodeId}`);
      updatedNodes = updatedNodes.filter(n => n.id !== override.nodeId);
      updatedEdges = updatedEdges.filter(
        e => e.source !== override.nodeId && e.target !== override.nodeId
      );
    } else if (override.action === 'replace' && override.newNodeType) {
      // Replace node type
      const nodeIndex = updatedNodes.findIndex(n => n.id === override.nodeId);
      if (nodeIndex !== -1) {
        const node = updatedNodes[nodeIndex];
        const newSchema = nodeLibrary.getSchema(override.newNodeType);
        
        if (!newSchema) {
          logger.warn(`[WorkflowConfirm] Node override failed: ${override.newNodeType} not found in node library`);
          return;
        }

        logger.info(`[WorkflowConfirm] Replacing node ${override.nodeId}: ${unifiedNormalizeNodeType(node)} → ${override.newNodeType}`);
        
        updatedNodes[nodeIndex] = {
          ...node,
          data: {
            ...node.data,
            type: override.newNodeType,
            label: newSchema.label || override.newNodeType,
            category: newSchema.category || node.data?.category,
            config: override.newNodeConfig || node.data?.config || {},
          },
        };
      }
    } else if (override.action === 'add' && override.newNodeType) {
      // Add new node
      const newSchema = nodeLibrary.getSchema(override.newNodeType);
      if (!newSchema) {
        logger.warn(`[WorkflowConfirm] Node add failed: ${override.newNodeType} not found in node library`);
        return;
      }

      const newNodeId = `node_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      logger.info(`[WorkflowConfirm] Adding new node ${newNodeId} of type ${override.newNodeType}`);
      
      updatedNodes.push({
        id: newNodeId,
        type: 'custom',
        position: { x: 0, y: 0 },
        data: {
          type: override.newNodeType,
          label: newSchema.label || override.newNodeType,
          category: newSchema.category || 'action',
          config: override.newNodeConfig || {},
        },
      });
    }
  });

  return {
    nodes: updatedNodes,
    edges: updatedEdges,
  };
}

/**
 * Update workflow state in database
 */
async function updateWorkflowStateInDatabase(
  workflowId: string,
  state: WorkflowState,
  workflow?: { nodes: any[]; edges: any[] },
  userId?: string
): Promise<void> {
  const db = getDbClient();

  try {
    // Check if workflow exists in database
    const { data: existingWorkflow, error: fetchError } = await db
      .from('workflows')
      .select('id, status')
      .eq('id', workflowId)
      .single();

    if (fetchError && fetchError.code !== 'PGRST116') { // PGRST116 = not found
      logger.error(`[WorkflowConfirm] Error fetching workflow:`, fetchError);
      throw fetchError;
    }

    // Map WorkflowState to database status
    let dbStatus: string;
    switch (state) {
      case WorkflowState.STATE_CONFIRMED:
        dbStatus = 'active'; // or 'draft' if you want to keep it as draft until saved
        break;
      case WorkflowState.STATE_REJECTED:
        dbStatus = 'draft'; // Keep as draft when rejected
        break;
      case WorkflowState.STATE_WAITING_CONFIRMATION:
        dbStatus = 'draft';
        break;
      default:
        dbStatus = 'draft';
    }

    if (existingWorkflow) {
      // Update existing workflow
      const updateData: any = {
        status: dbStatus,
        setup_completed: false,
        setup_stage: state === WorkflowState.STATE_CONFIRMED ? 'ai_setup_pending' : 'confirmation_pending',
        updated_at: new Date().toISOString(),
      };

      // If workflow data is provided, update nodes and edges
      if (workflow) {
        updateData.nodes = workflow.nodes;
        updateData.edges = workflow.edges;
        updateData.graph = buildSyncedGraphPayload(workflow.nodes, workflow.edges);
      }

      const { error: updateError } = await db
        .from('workflows')
        .update(updateData)
        .eq('id', workflowId);

      if (updateError) {
        logger.error(`[WorkflowConfirm] Error updating workflow:`, updateError);
        throw updateError;
      }

      logger.info(`[WorkflowConfirm] ✅ Updated workflow ${workflowId} state to ${state} (DB status: ${dbStatus})`);
    } else {
      // Create new workflow entry if it doesn't exist
      if (!workflow) {
        throw new Error('Cannot create workflow without workflow data');
      }

      if (!userId) {
        // Try to get user from auth
        const { data: { user } } = await db.auth.getUser();
        if (!user) {
          throw new Error('User ID required to create workflow');
        }
        userId = user.id;
      }

      const { error: createError } = await db
        .from('workflows')
        .insert({
          id: workflowId,
          user_id: userId,
          name: `Workflow ${workflowId.substring(0, 8)}`,
          nodes: workflow.nodes,
          edges: workflow.edges,
          graph: buildSyncedGraphPayload(workflow.nodes, workflow.edges),
          status: dbStatus,
          confirmed: state === WorkflowState.STATE_CONFIRMED ? true : false,
          setup_completed: false,
          setup_stage: state === WorkflowState.STATE_CONFIRMED ? 'ai_setup_pending' : 'confirmation_pending',
        });

      if (createError) {
        logger.error(`[WorkflowConfirm] Error creating workflow:`, createError);
        throw createError;
      }

      logger.info(`[WorkflowConfirm] ✅ Created workflow ${workflowId} with state ${state} (DB status: ${dbStatus})`);
    }
  } catch (error) {
    logger.error(`[WorkflowConfirm] Database update failed:`, error);
    throw error;
  }
}

/**
 * POST /api/workflow/confirm
 * Confirm workflow and continue pipeline execution
 */
export async function confirmWorkflow(req: Request, res: Response) {
  try {
    const { workflowId, approved, toolOverrides, nodeOverrides, feedback } = req.body as ConfirmRequest;

    if (!workflowId) {
      return res.status(400).json({
        error: 'Missing required field',
        message: 'workflowId is required',
      });
    }

    if (approved === undefined) {
      return res.status(400).json({
        error: 'Missing required field',
        message: 'approved is required',
      });
    }

    logger.info(`[WorkflowConfirm] Processing confirmation for workflow ${workflowId}, approved: ${approved}`);

    // Get confirmation request
    const confirmationRequest = workflowConfirmationManager.getConfirmationRequest(workflowId);
    if (!confirmationRequest) {
      return res.status(404).json({
        error: 'Workflow confirmation request not found',
        message: `No confirmation request found for workflow ID: ${workflowId}`,
      });
    }

    // Get user ID from auth
    const supabaseClient = getDbClient();
    const { data: { user } } = await supabaseClient.auth.getUser();
    const userId = user?.id;

    // Apply overrides if provided
    let workflow = confirmationRequest.workflow;
    if (toolOverrides && Object.keys(toolOverrides).length > 0) {
      logger.info(`[WorkflowConfirm] Applying ${Object.keys(toolOverrides).length} tool override(s)`);
      
      // Use tool substitution engine for proper tool replacement
      const substitutions = Object.values(toolOverrides).map(override => ({
        nodeId: override.nodeId,
        newTool: override.newTool,
      }));
      
      const substitutionResult = await toolSubstitutionEngine.substituteTools(
        {
          nodes: workflow.nodes,
          edges: workflow.edges,
        },
        substitutions,
        userId
      );
      
      if (substitutionResult.success) {
        workflow = {
          nodes: substitutionResult.workflow.nodes || [],
          edges: substitutionResult.workflow.edges || [],
        };
        
        // Log substitution results
        substitutionResult.substitutedNodes.forEach(sub => {
          logger.info(`[WorkflowConfirm] ✅ Substituted ${sub.fromTool} → ${sub.toTool} for node ${sub.nodeId}`);
        });
        
        if (substitutionResult.warnings.length > 0) {
          logger.warn(`[WorkflowConfirm] ⚠️  Substitution warnings:`, substitutionResult.warnings);
        }
      } else {
        logger.error(`[WorkflowConfirm] ❌ Tool substitution failed:`, substitutionResult.errors);
        return res.status(400).json({
          success: false,
          error: 'Tool substitution failed',
          errors: substitutionResult.errors,
          warnings: substitutionResult.warnings,
        });
      }
    }

    if (nodeOverrides && Object.keys(nodeOverrides).length > 0) {
      logger.info(`[WorkflowConfirm] Applying ${Object.keys(nodeOverrides).length} node override(s)`);
      workflow = applyNodeOverrides(workflow, nodeOverrides);
    }

    // Submit confirmation
    const confirmationResponse = workflowConfirmationManager.submitConfirmation(
      workflowId,
      approved,
      feedback
    );

    if (!approved) {
      // Workflow rejected
      logger.info(`[WorkflowConfirm] Workflow ${workflowId} rejected by user`);
      
      // Update database state
      await updateWorkflowStateInDatabase(
        workflowId,
        WorkflowState.STATE_REJECTED,
        workflow,
        userId
      );

      return res.json({
        success: true,
        workflowId,
        state: WorkflowState.STATE_REJECTED,
        message: 'Workflow rejected. You can regenerate or modify the workflow.',
        workflow: workflow, // Return modified workflow for regeneration
      });
    }

    // ── Inject pending credentials from credential panel ──────────────────
    const pendingCreds = pendingCredentialStore.get(workflowId);
    if (pendingCreds && Object.keys(pendingCreds).length > 0) {
      // Validate no required credential field is an empty string
      const emptyFields: string[] = [];
      for (const [provider, fields] of Object.entries(pendingCreds)) {
        for (const [fieldName, value] of Object.entries(fields)) {
          if (!value || value.trim() === '') {
            emptyFields.push(`${provider}.${fieldName}`);
          }
        }
      }
      if (emptyFields.length > 0) {
        return res.status(400).json({
          error: 'Missing credential fields',
          fields: emptyFields,
          message: `The following credential fields are empty: ${emptyFields.join(', ')}`,
        });
      }

      // Get required credentials from confirmation request (backward compatible: empty array if not available)
      const requiredCredentials = (confirmationRequest as any).requiredCredentials ?? [];

      // Inject credentials into workflow nodes
      const injectionResult = credentialInjector.injectCredentials(
        workflow,
        pendingCreds,
        requiredCredentials,
      );
      if (!injectionResult.success) {
        return res.status(400).json({
          error: 'Credential injection failed',
          errors: injectionResult.errors,
        });
      }
      workflow = injectionResult.workflow;
      logger.info(`[WorkflowConfirm] ✅ Injected pending credentials for workflow ${workflowId}`);
    }

    // Workflow approved — AI-first pipeline generates directly, no continuation step needed.
    // Return the workflow from the confirmation request as-is.
    logger.info(`[WorkflowConfirm] Workflow ${workflowId} approved`);

    // Update database with confirmed workflow
    await updateWorkflowStateInDatabase(
      workflowId,
      WorkflowState.STATE_CONFIRMED,
      {
        nodes: workflow.nodes || [],
        edges: workflow.edges || [],
      },
      userId
    );

    // ✅ Set confirmed field to true
    const { error: confirmError } = await supabaseClient
      .from('workflows')
      .update({ confirmed: true })
      .eq('id', workflowId);

    if (confirmError) {
      logger.error(`[WorkflowConfirm] Error setting confirmed flag:`, confirmError);
    } else {
      logger.info(`[WorkflowConfirm] ✅ Set confirmed=true for workflow ${workflowId}`);
    }

    logger.info(`[WorkflowConfirm] ✅ Workflow ${workflowId} confirmed successfully`);

    // Clear pending credentials now that workflow is confirmed and saved
    pendingCredentialStore.clear(workflowId);
    logger.info(`[WorkflowConfirm] ✅ Cleared pending credentials for workflow ${workflowId}`);

    return res.json({
      success: true,
      workflowId,
      state: WorkflowState.STATE_CONFIRMED,
      workflow,
      errors: [],
      warnings: [],
    });
  } catch (error) {
    logger.error(`[WorkflowConfirm] Error processing confirmation:`, error);
    return res.status(500).json({
      error: 'Internal server error',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * POST /api/workflow/reject
 * Reject workflow (alias for confirm with approved=false)
 */
export async function rejectWorkflow(req: Request, res: Response) {
  // Reject is just confirm with approved=false
  return confirmWorkflow(req, res);
}

/**
 * Default export for Express route
 */
export default async function workflowConfirmHandler(req: Request, res: Response) {
  // Route based on path
  if (req.path.includes('/reject')) {
    return rejectWorkflow(req, res);
  }
  return confirmWorkflow(req, res);
}
