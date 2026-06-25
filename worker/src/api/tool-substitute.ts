/**
 * Tool Substitution API
 * 
 * POST /api/workflow/tool-substitute
 * 
 * Substitutes equivalent tools in workflow without breaking graph.
 */

import { Request, Response } from 'express';
import { toolSubstitutionEngine } from '../services/ai/tool-substitution-engine';
import { getDbClient } from '../core/database/aws-db-client';
import { Workflow } from '../core/types/ai-types';
import { logger } from '../core/logger';

interface ToolSubstituteRequest {
  workflowId?: string;
  workflow?: {
    nodes: any[];
    edges: any[];
  };
  substitutions: Array<{
    nodeId: string;
    newTool: string;
  }>;
  updatePipeline?: boolean; // Whether to update pipeline after substitution
}

/**
 * POST /api/workflow/tool-substitute
 * Substitute equivalent tools in workflow
 */
export async function substituteTools(req: Request, res: Response) {
  try {
    const { workflowId, workflow: workflowData, substitutions, updatePipeline = false } = req.body as ToolSubstituteRequest;

    if (!substitutions || !Array.isArray(substitutions) || substitutions.length === 0) {
      return res.status(400).json({
        error: 'Missing required field',
        message: 'substitutions array is required',
      });
    }

    // Get user ID from auth
    const db = getDbClient();
    const { data: { user } } = await db.auth.getUser();
    const userId = user?.id;

    let workflow: Workflow;

    // Get workflow from database if workflowId provided
    if (workflowId) {
      const { data: dbWorkflow, error: fetchError } = await db
        .from('workflows')
        .select('nodes, edges')
        .eq('id', workflowId)
        .single();

      if (fetchError) {
        return res.status(404).json({
          error: 'Workflow not found',
          message: `Workflow ${workflowId} not found`,
        });
      }

      workflow = {
        nodes: dbWorkflow.nodes || [],
        edges: dbWorkflow.edges || [],
      };
    } else if (workflowData) {
      workflow = {
        nodes: workflowData.nodes || [],
        edges: workflowData.edges || [],
      };
    } else {
      return res.status(400).json({
        error: 'Missing required field',
        message: 'Either workflowId or workflow data is required',
      });
    }

    logger.info(`[ToolSubstitute] Processing ${substitutions.length} tool substitution(s)`);

    // Perform substitutions
    const result = await toolSubstitutionEngine.substituteTools(
      workflow,
      substitutions,
      userId
    );

    if (!result.success) {
      return res.status(400).json({
        success: false,
        error: 'Tool substitution failed',
        errors: result.errors,
        warnings: result.warnings,
        workflow: result.workflow,
      });
    }

    // Validate workflow after substitution
    const validation = toolSubstitutionEngine.validateWorkflowAfterSubstitution(result.workflow);
    if (!validation.valid) {
      return res.status(400).json({
        success: false,
        error: 'Workflow validation failed after substitution',
        errors: validation.errors,
        warnings: [...result.warnings, ...validation.warnings],
        workflow: result.workflow,
      });
    }

    // Update workflow in database if workflowId provided
    if (workflowId) {
      const { error: updateError } = await db
        .from('workflows')
        .update({
          nodes: result.workflow.nodes,
          edges: result.workflow.edges,
          updated_at: new Date().toISOString(),
        })
        .eq('id', workflowId);

      if (updateError) {
        logger.error(`[ToolSubstitute] Error updating workflow:`, updateError);
        return res.status(500).json({
          success: false,
          error: 'Failed to update workflow in database',
          message: updateError.message,
        });
      }
    }

    // Update pipeline if requested — no-op in AI-first pipeline (generation is direct)
    let pipelineResult = null;
    if (updatePipeline && workflowId) {
      logger.info(`[ToolSubstitute] Pipeline update requested — AI-first pipeline generates directly, no continuation needed`);
    }

    logger.info(`[ToolSubstitute] ✅ Successfully substituted ${result.substitutedNodes.length} tool(s)`);

    return res.json({
      success: true,
      workflow: result.workflow,
      substitutedNodes: result.substitutedNodes,
      credentialValidation: result.credentialValidation,
      warnings: result.warnings,
      pipelineResult: pipelineResult || undefined,
    });
  } catch (error) {
    logger.error(`[ToolSubstitute] Error processing substitution:`, error);
    return res.status(500).json({
      error: 'Internal server error',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * GET /api/workflow/tool-substitute/available/:nodeId
 * Get available tool substitutions for a node
 */
export async function getAvailableSubstitutions(req: Request, res: Response) {
  try {
    const { nodeId } = req.params;
    const { workflowId, workflow: workflowData } = req.query;

    if (!nodeId) {
      return res.status(400).json({
        error: 'Missing required parameter',
        message: 'nodeId is required',
      });
    }

    let workflow: Workflow;

    // Get workflow from database if workflowId provided
    if (workflowId && typeof workflowId === 'string') {
      const db = getDbClient();
      const { data: dbWorkflow, error: fetchError } = await db
        .from('workflows')
        .select('nodes, edges')
        .eq('id', workflowId)
        .single();

      if (fetchError) {
        return res.status(404).json({
          error: 'Workflow not found',
          message: `Workflow ${workflowId} not found`,
        });
      }

      workflow = {
        nodes: dbWorkflow.nodes || [],
        edges: dbWorkflow.edges || [],
      };
    } else if (workflowData) {
      try {
        const parsed = typeof workflowData === 'string' ? JSON.parse(workflowData) : workflowData;
        workflow = {
          nodes: parsed.nodes || [],
          edges: parsed.edges || [],
        };
      } catch (error) {
        return res.status(400).json({
          error: 'Invalid workflow data',
          message: 'workflow query parameter must be valid JSON',
        });
      }
    } else {
      return res.status(400).json({
        error: 'Missing required parameter',
        message: 'Either workflowId or workflow query parameter is required',
      });
    }

    const availableTools = toolSubstitutionEngine.getAvailableSubstitutions(nodeId, workflow);

    return res.json({
      success: true,
      nodeId,
      currentTool: workflow.nodes?.find(n => n.id === nodeId)?.data?.type || 'unknown',
      availableSubstitutions: availableTools,
    });
  } catch (error) {
    logger.error(`[ToolSubstitute] Error getting available substitutions:`, error);
    return res.status(500).json({
      error: 'Internal server error',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * Default export for Express route
 */
export default substituteTools;
