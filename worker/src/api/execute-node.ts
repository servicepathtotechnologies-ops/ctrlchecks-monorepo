// Execute Single Node API Route
// Used by Debug Panel for isolated node execution
// Uses the same execution engine as full workflow execution

import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { executeNode } from './execute-workflow';
import { LRUNodeOutputsCache } from '../core/cache/lru-node-outputs-cache';
import { logger } from '../core/logger';

// WorkflowNode interface must match execute-workflow.ts
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

/**
 * Execute a single node in isolation (for debug panel)
 * Uses the same execution engine as full workflow execution
 */
export default async function executeNodeHandler(req: Request, res: Response) {
  const db = getDbClient();
  const { runId, nodeId, nodeType, config: nodeConfig, input, workflowId, connectionRefs } = req.body;

  logger.info(`[DEBUG] Execute node request:`, {
    runId,
    nodeId,
    nodeType,
    workflowId,
    hasInput: !!input,
    hasConfig: !!nodeConfig,
    configKeys: nodeConfig ? Object.keys(nodeConfig) : [],
    configSpreadsheetId: nodeConfig?.spreadsheetId ? String(nodeConfig.spreadsheetId).substring(0, 50) : 'not provided',
  });

  // Validate required fields
  if (!nodeId || !nodeType || !workflowId) {
    return res.status(400).json({
      success: false,
      error: 'Missing required fields: nodeId, nodeType, and workflowId are required',
    });
  }

  const startTime = Date.now();

  try {
    // Fetch workflow to get full context (needed for template resolution)
    const { data: workflow, error: workflowError } = await db
      .from('workflows')
      .select('*')
      .eq('id', workflowId)
      .single();

    if (workflowError || !workflow) {
      logger.error('[DEBUG] Workflow fetch error:', workflowError);
      return res.status(404).json({
        success: false,
        error: 'Workflow not found',
      });
    }

    const { isSetupPending, setupPendingResponse } = await import('./workflow-setup-lifecycle');
    if (isSetupPending(workflow)) {
      return res.status(409).json({
        success: false,
        ...setupPendingResponse(workflowId),
      });
    }

    // Get user_id from workflow
    const userId = workflow.user_id;
    
    // Extract current user from Authorization header (if available)
    // This is optional - node can execute without it
    let currentUserId: string | undefined;
    try {
      const authHeader = req.headers.authorization;
      if (authHeader && authHeader.startsWith('Bearer ')) {
        const token = authHeader.replace('Bearer ', '').trim();
        if (token) {
          try {
            const { data: { user }, error: authError } = await db.auth.getUser(token);
            if (!authError && user) {
              currentUserId = user.id;
              logger.info(`[DEBUG] Current user: ${currentUserId}`);
            } else if (authError) {
              // Log auth error but don't fail - node can still execute
              logger.info(`[DEBUG] Auth error (non-fatal): ${authError.message || 'Unknown auth error'}`);
            }
          } catch (authErr: any) {
            // Handle network/connection errors gracefully
            const errorMsg = authErr?.message || 'Unknown error';
            if (errorMsg.includes('ENOTFOUND') || errorMsg.includes('fetch failed')) {
              logger.info('[DEBUG] DB connection issue - continuing without current user ID');
            } else {
              logger.info(`[DEBUG] Auth extraction error (non-fatal): ${errorMsg}`);
            }
          }
        }
      }
    } catch (error: any) {
      // Auth is optional - node can still execute without it
      const errorMsg = error?.message || 'Unknown error';
      logger.info(`[DEBUG] Auth extraction failed (non-fatal): ${errorMsg}`);
    }

    // Build node object in the format expected by executeNode
    const node: WorkflowNode = {
      id: nodeId,
      type: nodeType,
      data: {
        label: nodeId, // Fallback label
        type: nodeType,
        category: 'custom',
        config: nodeConfig || {},
      },
    };

    // If node exists in workflow, use its actual data
    const nodes = (workflow.nodes || []) as WorkflowNode[];
    const existingNode = nodes.find(n => n.id === nodeId);
    if (existingNode) {
      node.data = existingNode.data;
      // ✅ CRITICAL FIX: Prioritize provided config over saved workflow config
      // If frontend sends updated config, use it instead of stale saved config
      if (nodeConfig && Object.keys(nodeConfig).length > 0) {
        // Provided config takes precedence - merge saved config as fallback only
        node.data.config = { ...node.data.config, ...nodeConfig };
        logger.info(`[DEBUG] Using provided config (overrides saved config):`, Object.keys(nodeConfig));
      } else {
        logger.info(`[DEBUG] No provided config, using saved workflow config`);
      }
      // Merge connectionRefs from request (user's live selection in Properties Panel)
      if (connectionRefs && typeof connectionRefs === 'object' && Object.keys(connectionRefs).length > 0) {
        (node.data as any).connectionRefs = { ...((node.data as any).connectionRefs || {}), ...connectionRefs };
        logger.info(`[DEBUG] Merged connectionRefs from request:`, Object.keys(connectionRefs));
      }
    } else if (nodeConfig) {
      // Node doesn't exist in workflow, use provided config
      node.data.config = nodeConfig;
      if (connectionRefs && typeof connectionRefs === 'object' && Object.keys(connectionRefs).length > 0) {
        (node.data as any).connectionRefs = connectionRefs;
      }
    }

    // ✅ PRE-EXECUTION: Validate this node's config before running
    const { validateWorkflowConfig } = await import('../core/utils/pre-execution-validator');
    const configCheck = validateWorkflowConfig([
      { id: node.id, type: nodeType, data: { label: node.data.label, config: node.data.config as Record<string, any> } },
    ]);
    if (!configCheck.valid) {
      return res.status(400).json({
        code: 'MISSING_REQUIRED_INPUTS',
        error: 'Node has missing required fields',
        message: `This node needs configuration before it can run.`,
        hint: "Open the Properties panel and fill in the missing fields.",
        details: {
          missingInputs: configCheck.missingInputs,
          issues: configCheck.issues,
        },
      });
    }

    // ✅ UNIFIED ENGINE: Use unified execution context
    const { createUnifiedExecutionContext } = await import('../core/execution/unified-execution-engine');
    const inputObj = input && typeof input === 'object' ? input as Record<string, unknown> : {};
    
    // Use LRU cache for consistency with full workflow execution
    // Small cache size (10) is sufficient for single node execution
    const nodeOutputs = new LRUNodeOutputsCache(10, false);
    nodeOutputs.set('trigger', inputObj, true); // Mark trigger as persistent
    nodeOutputs.set('input', inputObj);
    nodeOutputs.set('$json', inputObj);
    nodeOutputs.set('json', inputObj);
    
    // Also add input properties directly for template resolution
    Object.keys(inputObj).forEach(key => {
      nodeOutputs.set(key, inputObj[key]);
    });
    
    // Create unified execution context (for consistency with full workflow)
    const unifiedContext = createUnifiedExecutionContext(input, nodeOutputs);

    // Execute the node using the same engine as full workflow
    logger.info(`[DEBUG] Executing node: ${node.data.label} (${nodeType})`);
    const output = await executeNode(
      node,
      input || {},
      nodeOutputs,
      db,
      workflowId,
      userId,
      currentUserId
    );

    const executionTime = Date.now() - startTime;

    logger.info(`[DEBUG] Node execution completed in ${executionTime}ms`);
    
    // ✅ CLEAN OUTPUT FROM CONFIG VALUES (CORE ARCHITECTURE FIX)
    // Remove config values from output to ensure only actual output data is returned
    // This prevents placeholder values and config fields from appearing in output JSON
    const { cleanOutputFromConfig } = await import('../core/utils/placeholder-filter');
    const cleanedOutput = cleanOutputFromConfig(output, node.data.config || {});
    
    // Clear cache after execution
    nodeOutputs.clear();

    // Return response in format expected by frontend
    return res.json({
      success: true,
      output: cleanedOutput,
      executionTime,
      nodeId,
      nodeType,
      runId,
    });
  } catch (error) {
    const executionTime = Date.now() - startTime;
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    
    logger.error('[DEBUG] Node execution error:', error);

    return res.status(500).json({
      success: false,
      error: errorMessage,
      executionTime,
      nodeId,
      nodeType,
      runId,
    });
  }
}
