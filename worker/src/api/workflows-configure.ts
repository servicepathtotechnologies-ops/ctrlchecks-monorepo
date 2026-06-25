/**
 * POST /api/workflows/:workflowId/configure
 * 
 * Accepts credentials and node inputs, validates them, stores them temporarily,
 * then triggers injection + AI auto-config + validation
 */

import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { getUnifiedMissingItems } from '../services/ai/credential-input-discovery';
import { workflowLifecycleManager } from '../services/workflow-lifecycle-manager';
import { workflowValidator } from '../services/ai/workflow-validator';
import { Workflow } from '../core/types/ai-types';
import { credentialDiscoveryPhase } from '../services/ai/credential-discovery-phase';
import { ErrorCode } from '../core/utils/error-codes';
import { logger } from '../core/logger';

function asArray(value: any): any[] {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

async function injectSatisfiedVaultReferences(workflow: Workflow, userId?: string): Promise<Workflow> {
  const discovery = await credentialDiscoveryPhase.discoverCredentials(workflow, userId);
  const satisfied = discovery.satisfiedCredentials || [];
  if (satisfied.length === 0) return workflow;

  const nodes = workflow.nodes.map((node: any) => {
    const config = { ...(node?.data?.config || {}) };
    let updated = false;

    for (const credential of satisfied) {
      const nodeIds = (credential.nodeIds || []).map(String);
      if (!nodeIds.includes(String(node?.id || ''))) continue;

      const vaultKey = String(credential.vaultKey || credential.provider || '').trim();
      if (!vaultKey) continue;

      if (!config.credentialId) {
        config.credentialId = vaultKey;
        updated = true;
      }
      if (!config.credentialRef && credential.type === 'oauth') {
        config.credentialRef = vaultKey;
        updated = true;
      }
    }

    if (!updated) return node;
    return {
      ...node,
      data: {
        ...(node.data || {}),
        config,
      },
    };
  });

  return { ...workflow, nodes };
}

export default async function configureWorkflowHandler(req: Request, res: Response) {
  try {
    const { workflowId } = req.params;
    const { credentials, inputs } = req.body;

    if (!workflowId) {
      return res.status(400).json({
        error: 'workflowId is required',
      });
    }

    logger.info(`[ConfigureWorkflow] Configuring workflow ${workflowId}`);

    // Get current missing items to validate against
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
          logger.warn('[ConfigureWorkflow] Auth error (non-fatal):', authErr);
        }
      }
    }

    const missingItems = await getUnifiedMissingItems(workflowId, userId);

    // Validate required credentials by credential id/vault key (not provider-level).
    // Provider-only checks are ambiguous when multiple credentials share the same provider.
    const missingCredentialKeys = missingItems.credentials
      .filter(cred => !cred.satisfied)
      .map(cred => String(cred.vaultKey || cred.provider))
      .filter(Boolean);
    const providedCredentialKeys = credentials ? Object.keys(credentials) : [];
    const missingCreds = missingCredentialKeys.filter(key => !providedCredentialKeys.includes(key));

    if (missingCreds.length > 0) {
      return res.status(400).json({
        code: ErrorCode.EXECUTION_MISSING_CREDENTIALS,
        error: 'Missing required credentials',
        missingCredentials: missingCreds,
        details: `The following credentials are required but not provided: ${missingCreds.join(', ')}`,
      });
    }

    // Validate that all required inputs are provided
    const requiredInputs = missingItems.inputs.filter(input => input.required);
    const providedInputKeys = inputs ? inputs.map((i: any) => `${i.nodeId}_${i.fieldName}`) : [];
    const missingInputs = requiredInputs.filter(input => 
      !providedInputKeys.includes(`${input.nodeId}_${input.fieldName}`)
    );

    if (missingInputs.length > 0) {
      return res.status(400).json({
        code: ErrorCode.EXECUTION_MISSING_INPUTS,
        error: 'Missing required inputs',
        missingInputs: missingInputs.map(input => ({
          nodeId: input.nodeId,
          nodeType: input.nodeType,
          nodeLabel: (input as any).nodeLabel || input.nodeType || '',
          fieldName: input.fieldName,
          description: (input as any).description || '',
          required: true,
        })),
        details: `The following inputs are required but not provided: ${missingInputs.map(i => `${i.nodeId}.${i.fieldName}`).join(', ')}`,
      });
    }

    // Load workflow from database
    const { data: workflowData, error: workflowError } = await db
      .from('workflows')
      .select('*')
      .eq('id', workflowId)
      .single();

    if (workflowError || !workflowData) {
      return res.status(404).json({
        error: 'Workflow not found',
        workflowId,
      });
    }

    // Parse workflow structure
    const graphData = typeof workflowData.graph === 'string' 
      ? JSON.parse(workflowData.graph) 
      : workflowData.graph || {};
    
    const persistedMeta =
      (graphData &&
        typeof graphData === 'object' &&
        graphData.metadata &&
        typeof graphData.metadata === 'object' &&
        graphData.metadata) ||
      {};
    let workflow: Workflow = {
      nodes: asArray(workflowData.nodes).length > 0 ? asArray(workflowData.nodes) : asArray(graphData.nodes),
      edges: asArray(workflowData.edges).length > 0 ? asArray(workflowData.edges) : asArray(graphData.edges),
      metadata: {
        created_at: workflowData.created_at,
        updated_at: workflowData.updated_at,
        workflowId,
        name: workflowData.name || 'Untitled Workflow',
        ...(persistedMeta as Record<string, unknown>),
      },
    };

    // Step 1: Inject credentials
    if (credentials && Object.keys(credentials).length > 0) {
      logger.info(`[ConfigureWorkflow] Injecting ${Object.keys(credentials).length} credential(s)`);
      const credentialInjectionResult = await workflowLifecycleManager.injectCredentials(
        workflow,
        credentials,
        userId
      );
      
      if (!credentialInjectionResult.success) {
        return res.status(400).json({
          code: ErrorCode.CREDENTIAL_INJECTION_FAILED,
          error: 'Credential injection failed',
          details: credentialInjectionResult.errors || ['Unknown error'],
        });
      }
      
      workflow = credentialInjectionResult.workflow;
    }

    // Step 2: Inject node inputs
    if (inputs && inputs.length > 0) {
      logger.info(`[ConfigureWorkflow] Injecting ${inputs.length} input(s)`);
      
      workflow = {
        ...workflow,
        nodes: workflow.nodes.map(node => {
          const nodeInputs = inputs.filter((i: any) => i.nodeId === node.id);
          if (nodeInputs.length === 0) {
            return node;
          }

          const config = { ...(node.data?.config || {}) };
          for (const input of nodeInputs) {
            config[input.fieldName] = input.value;
          }

          return {
            ...node,
            data: {
              ...(node.data || {}),
              config,
            },
          };
        }),
      };
    }

    // Step 3: Attach already-satisfied vault credentials (OAuth/connected accounts).
    // The newer attach-inputs path does this automatically; /configure must keep
    // the same node-id-aligned contract.
    workflow = await injectSatisfiedVaultReferences(workflow, userId);

    // Step 4: AI auto-config for remaining non-credential fields
    logger.info(`[ConfigureWorkflow] Running AI auto-config for remaining fields`);
    try {
      const { universalNodeAIContext } = await import('../services/ai/universal-node-ai-context');
      const userPrompt = workflowData.name || 'Process workflow data';
      
      // Auto-fill each node that has empty AI-generatable fields
      workflow = {
        ...workflow,
        nodes: await Promise.all(
          workflow.nodes.map(async (node) => {
            // Skip if node already has all required fields filled
            const config = node.data?.config || {};
            const hasEmptyFields = Object.values(config).some(
              (v: any) => !v || (typeof v === 'string' && v.trim() === '')
            );
            
            if (!hasEmptyFields) {
              return node; // Node is already configured
            }
            
            // Auto-fill using AI context
            try {
              const autoFilledNode = await universalNodeAIContext.autoFillNode(
                node,
                workflow,
                userPrompt,
                {} // No previous outputs during configuration
              );
              return autoFilledNode;
            } catch (autoFillError: any) {
              logger.warn(`[ConfigureWorkflow] Auto-fill failed for node ${node.id} (non-blocking):`, autoFillError.message);
              return node; // Continue with original node if auto-fill fails
            }
          })
        ),
      };
      
      logger.info(`[ConfigureWorkflow] ✅ AI auto-config completed`);
    } catch (autoConfigError: any) {
      logger.warn(`[ConfigureWorkflow] ⚠️ AI auto-config failed (non-blocking):`, autoConfigError.message);
      // Continue without auto-config - workflow can still be saved
    }

    // Step 5: Validate workflow
    logger.info(`[ConfigureWorkflow] Validating workflow`);
    const validation = await workflowValidator.validateAndFix(workflow);
    
    if (!validation.valid) {
      return res.status(400).json({
        code: ErrorCode.WORKFLOW_VALIDATION_FAILED,
        error: 'Workflow validation failed',
        validationErrors: validation.errors || [],
        warnings: validation.warnings || [],
      });
    }

    // Step 6: Save updated workflow to database
    const updatedWorkflow = validation.fixedWorkflow || workflow;
    const readiness = await workflowLifecycleManager.validateExecutionReady(updatedWorkflow, userId);
    const finalStatus = 'active';
    const finalPhase = readiness.ready ? 'ready_for_execution' : 'ready_for_ownership';
    const metadata = {
      ...((workflowData as any).metadata || {}),
      ...((updatedWorkflow as any).metadata || {}),
      workflowId,
    };
    
    const { error: updateError } = await db
      .from('workflows')
      .update({
        nodes: updatedWorkflow.nodes,
        edges: updatedWorkflow.edges,
        graph: {
          nodes: updatedWorkflow.nodes,
          edges: updatedWorkflow.edges,
          metadata,
        },
        metadata,
        status: finalStatus,
        phase: finalPhase,
        ready_to_run: readiness.ready,
        updated_at: new Date().toISOString(),
      })
      .eq('id', workflowId);

    if (updateError) {
      logger.error('[ConfigureWorkflow] Failed to save workflow:', updateError);
      return res.status(500).json({
        code: ErrorCode.INTERNAL_ERROR,
        error: 'Failed to save workflow',
        details: updateError.message,
      });
    }

    logger.info(`[ConfigureWorkflow] ✅ Workflow ${workflowId} configured and marked as ready_to_run`);

    return res.json({
      success: true,
      workflowId,
      ready_to_run: readiness.ready,
      status: finalStatus,
      phase: finalPhase,
      workflow: updatedWorkflow,
      nodes: updatedWorkflow.nodes,
      edges: updatedWorkflow.edges,
      message: 'Workflow configured successfully',
    });
  } catch (error: any) {
    logger.error('[ConfigureWorkflow] Error:', error);
    return res.status(500).json({
      code: ErrorCode.INTERNAL_ERROR,
      error: 'Failed to configure workflow',
      message: error.message || 'Unknown error',
    });
  }
}
