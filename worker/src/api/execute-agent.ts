// Execute Agent Route
// Worker API handler
// Executes agent workflows with reasoning and action planning

import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { ReasoningEngine, ReasoningContext, Action } from '../shared/reasoning-engine';
import { HybridMemoryService } from '../shared/memory';
import { LLMAdapter } from '../shared/llm-adapter';
import { config } from '../core/config';
import { logger } from '../core/logger';

interface WorkflowNode {
  id: string;
  type: string;
  data: {
    label: string;
    type: string;
    config: Record<string, unknown>;
  };
  position: { x: number; y: number };
}

interface WorkflowEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
}

interface AgentConfig {
  goal: string;
  maxIterations: number;
  reasoningModel: string;
  actionModel: string;
  memoryEnabled: boolean;
  temperature?: number;
}

interface AgentState {
  iteration: number;
  currentState: any;
  reasoningHistory: any[];
  actionsTaken: any[];
  goalAchieved: boolean;
}

class AgentExecutor {
  private db: any;
  private reasoningEngine: ReasoningEngine;
  private llmAdapter: LLMAdapter;
  private memoryService: HybridMemoryService | null = null;

  constructor(db: any) {
    this.db = db;
    this.reasoningEngine = new ReasoningEngine();
    this.llmAdapter = new LLMAdapter();
  }

  async execute(
    workflowId: string,
    input: any,
    agentConfig: AgentConfig,
    nodes: WorkflowNode[],
    edges: WorkflowEdge[]
  ): Promise<{
    executionId: string;
    status: string;
    result: any;
    reasoning: any[];
    actions: any[];
    iterations: number;
  }> {
    // Create execution record
    const { data: execution, error: execError } = await this.db
      .from('agent_executions')
      .insert({
        workflow_id: workflowId,
        session_id: input.session_id || `session-${Date.now()}`,
        status: 'running',
        goal: agentConfig.goal,
        max_iterations: agentConfig.maxIterations,
        reasoning_steps: [],
        actions_taken: [],
        current_state: input,
      })
      .select()
      .single();

    if (execError || !execution) {
      throw new Error(`Failed to create agent execution: ${execError?.message}`);
    }

    const executionId = execution.id;
    const sessionId = execution.session_id;

    // Initialize memory if enabled
    if (agentConfig.memoryEnabled) {
      this.memoryService = new HybridMemoryService(config.supabaseUrl, config.supabaseKey);
      await this.memoryService.initialize();
      await this.memoryService.getOrCreateSession(workflowId, sessionId, input.user_id);
    }

    // Initialize agent state
    const agentState: AgentState = {
      iteration: 0,
      currentState: input,
      reasoningHistory: [],
      actionsTaken: [],
      goalAchieved: false,
    };

    try {
      // Agent execution loop
      for (let i = 0; i < agentConfig.maxIterations; i++) {
        agentState.iteration = i + 1;

        // Update execution status
        await this.db
          .from('agent_executions')
          .update({
            iteration_count: agentState.iteration,
            current_state: agentState.currentState,
            reasoning_steps: agentState.reasoningHistory,
            actions_taken: agentState.actionsTaken,
          })
          .eq('id', executionId);

        // Get available actions from workflow nodes
        const availableActions = this.getAvailableActions(nodes, agentState.currentState);

        // Build reasoning context
        const reasoningContext: ReasoningContext = {
          goal: agentConfig.goal,
          currentState: agentState.currentState,
          availableActions,
          history: agentState.reasoningHistory.map((r, idx) => ({
            step: idx + 1,
            thought: r.thought || '',
            action: r.action,
            result: r.result,
          })),
          maxSteps: 5,
        };

        // Detect provider from model name
        const detectProvider = (model: string): 'openai' | 'claude' | 'gemini' | 'ollama' => {
          if (model.startsWith('gpt-') || model.includes('openai')) return 'openai';
          if (model.startsWith('claude-') || model.includes('anthropic')) return 'claude';
          if (model.startsWith('gemini-') || model.includes('gemini')) return 'gemini';
          return 'gemini';
        };

        // Reason about next action
        const reasoningResult = await this.reasoningEngine.reason(reasoningContext, {
          model: agentConfig.reasoningModel,
          temperature: agentConfig.temperature || 0.3,
          apiKey: (agentState.currentState as any)?.apiKey,
          provider: detectProvider(agentConfig.reasoningModel),
        });

        // Store reasoning step
        agentState.reasoningHistory.push({
          iteration: agentState.iteration,
          thought: reasoningResult.reasoning[0]?.thought || 'No reasoning provided',
          action: reasoningResult.nextAction?.name,
          confidence: reasoningResult.confidence,
          shouldContinue: reasoningResult.shouldContinue,
        });

        // Check if goal is achieved
        if (!reasoningResult.shouldContinue) {
          agentState.goalAchieved = true;
          break;
        }

        // Execute next action if available
        if (reasoningResult.nextAction) {
          const actionResult = await this.executeAction(
            reasoningResult.nextAction,
            nodes,
            edges,
            agentState.currentState,
            sessionId
          );

          // Store action result
          agentState.actionsTaken.push({
            iteration: agentState.iteration,
            action: reasoningResult.nextAction.name,
            result: actionResult,
            timestamp: new Date().toISOString(),
          });

          // Update current state with action result
          agentState.currentState = {
            ...agentState.currentState,
            lastAction: reasoningResult.nextAction.name,
            lastActionResult: actionResult,
          };

          // Store in memory if enabled
          if (this.memoryService && reasoningResult.nextAction) {
            await this.memoryService.store(
              sessionId,
              'assistant',
              `Executed action: ${reasoningResult.nextAction.name}. Result: ${JSON.stringify(actionResult).substring(0, 200)}`
            );
          }
        } else {
          logger.info('No action available, stopping agent');
          break;
        }

        // Small delay between iterations
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      // Finalize execution
      const finalStatus = agentState.goalAchieved ? 'completed' : 'stopped';
      
      await this.db
        .from('agent_executions')
        .update({
          status: finalStatus,
          finished_at: new Date().toISOString(),
          final_output: agentState.currentState,
          reasoning_steps: agentState.reasoningHistory,
          actions_taken: agentState.actionsTaken,
        })
        .eq('id', executionId);

      return {
        executionId,
        status: finalStatus,
        result: agentState.currentState,
        reasoning: agentState.reasoningHistory,
        actions: agentState.actionsTaken,
        iterations: agentState.iteration,
      };
    } catch (error) {
      // Update execution with error
      await this.db
        .from('agent_executions')
        .update({
          status: 'failed',
          error: error instanceof Error ? error.message : String(error),
          finished_at: new Date().toISOString(),
        })
        .eq('id', executionId);

      throw error;
    }
  }

  private getAvailableActions(
    nodes: WorkflowNode[],
    currentState: any
  ): Action[] {
    const actions: Action[] = [];

    // Filter nodes that can be actions (not triggers, not logic-only)
    const actionNodes = nodes.filter(node => {
      const type = node.data.type;
      return !['manual_trigger', 'webhook', 'schedule', 'if_else', 'switch', 'memory'].includes(type);
    });

    for (const node of actionNodes) {
      actions.push({
        id: node.id,
        name: node.data.label,
        description: `${node.data.type} node: ${node.data.label}`,
        nodeId: node.id,
        parameters: node.data.config,
      });
    }

    return actions;
  }

  private async executeAction(
    action: Action,
    nodes: WorkflowNode[],
    edges: WorkflowEdge[],
    currentState: any,
    sessionId: string
  ): Promise<any> {
    // Find the node for this action
    const node = nodes.find(n => n.id === action.nodeId);
    if (!node) {
      throw new Error(`Node not found for action: ${action.name}`);
    }

    // Execute the node by calling execute-workflow endpoint
    // For now, return a placeholder result
    // In production, you'd call execute-workflow logic
    return {
      action: action.name,
      nodeId: action.nodeId,
      executed: true,
      result: `Action ${action.name} executed successfully`,
      timestamp: new Date().toISOString(),
    };
  }
}

export default async function executeAgent(req: Request, res: Response) {
  const db = getDbClient();

  try {
    const { workflowId, input = {}, config: agentConfig } = req.body;

    if (!workflowId) {
      return res.status(400).json({ error: "workflowId is required" });
    }

    // Fetch workflow
    const { data: workflow, error: workflowError } = await db
      .from("workflows")
      .select("*")
      .eq("id", workflowId)
      .single();

    if (workflowError || !workflow) {
      return res.status(404).json({ error: "Workflow not found" });
    }

    // Check if workflow is agent type
    if (workflow.workflow_type !== 'agent') {
      return res.status(400).json({ 
        error: "Workflow is not an agent type. Set workflow_type to 'agent'." 
      });
    }

    // Get agent config from workflow or request
      const config: AgentConfig = agentConfig || workflow.agent_config || {
      goal: "Complete the task",
      maxIterations: 10,
      reasoningModel: "qwen2.5:14b-instruct-q4_K_M",  // Use Ollama for reasoning
      actionModel: "qwen2.5:14b-instruct-q4_K_M",      // Use Ollama for actions
      memoryEnabled: true,
      temperature: 0.3,
    };

    // Ensure goal is set
    if (!config.goal) {
      config.goal = "Complete the task";
    }

    const nodes = workflow.nodes as WorkflowNode[];
    const edges = workflow.edges as WorkflowEdge[];

    // Create executor and run
    const executor = new AgentExecutor(db);
    const result = await executor.execute(workflowId, input, config, nodes, edges);

    return res.json(result);
  } catch (error) {
    logger.error("Agent execution error:", error);
    return res.status(500).json({
      error: error instanceof Error ? error.message : "Unknown error",
      status: "failed",
    });
  }
}
