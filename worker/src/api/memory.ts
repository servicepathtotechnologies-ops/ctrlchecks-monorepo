/**
 * Memory API Routes
 * Endpoints for memory and reference system
 */

import { Request, Response, Router } from 'express';
import {
  getMemoryManager,
  getReferenceBuilder,
  getWorkflowAnalyzer,
} from '../memory';
import { getPrismaClient } from '../memory';
import { logger } from '../core/logger';

const router = Router();

/**
 * POST /api/memory/store-workflow
 * Store workflow with context
 */
router.post('/store-workflow', async (req: Request, res: Response) => {
  try {
    const { id, name, definition, tags, settings } = req.body;

    if (!name || !definition) {
      return res.status(400).json({
        error: 'Missing required fields',
        message: 'name and definition are required',
      });
    }

    const memoryManager = getMemoryManager();
    const workflowId = await memoryManager.storeWorkflow({
      id,
      name,
      definition,
      tags,
      settings,
    });

    res.json({
      success: true,
      workflowId,
      message: 'Workflow stored successfully',
    });
  } catch (error: any) {
    logger.error('Error storing workflow:', error);
    res.status(500).json({
      error: 'Failed to store workflow',
      message: error.message,
    });
  }
});

/**
 * GET /api/memory/workflow/:id/context
 * Get full context for AI
 */
router.get('/workflow/:id/context', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const { intent, query } = req.query;

    const referenceBuilder = getReferenceBuilder();
    const context = await referenceBuilder.buildContext(
      id,
      (intent as any) || 'creation',
      query as string
    );

    res.json({
      success: true,
      context,
    });
  } catch (error: any) {
    logger.error('Error building context:', error);
    res.status(500).json({
      error: 'Failed to build context',
      message: error.message,
    });
  }
});

/**
 * GET /api/memory/similar/:id
 * Find similar workflows
 */
router.get('/similar/:id', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;
    const limit = parseInt(req.query.limit as string) || 5;

    const memoryManager = getMemoryManager();
    const workflow = await memoryManager.getWorkflowReference(id);

    if (!workflow) {
      return res.status(404).json({
        error: 'Workflow not found',
      });
    }

    const query = workflow.definition.name || '';
    const similar = await memoryManager.findSimilarWorkflows(query, limit);

    res.json({
      success: true,
      similar,
    });
  } catch (error: any) {
    logger.error('Error finding similar workflows:', error);
    res.status(500).json({
      error: 'Failed to find similar workflows',
      message: error.message,
    });
  }
});

/**
 * POST /api/memory/search
 * Semantic search workflows
 */
router.post('/search', async (req: Request, res: Response) => {
  try {
    const { query, limit } = req.body;

    if (!query) {
      return res.status(400).json({
        error: 'Missing query',
        message: 'query is required',
      });
    }

    const memoryManager = getMemoryManager();
    const results = await memoryManager.findSimilarWorkflows(query, limit || 5);

    res.json({
      success: true,
      results,
    });
  } catch (error: any) {
    logger.error('Error searching workflows:', error);
    res.status(500).json({
      error: 'Failed to search workflows',
      message: error.message,
    });
  }
});

/**
 * POST /api/analyze
 * Analyze workflow structure
 */
router.post('/analyze', async (req: Request, res: Response) => {
  try {
    const { workflow } = req.body;

    if (!workflow) {
      return res.status(400).json({
        error: 'Missing workflow',
        message: 'workflow definition is required',
      });
    }

    const analyzer = getWorkflowAnalyzer();
    const analysis = analyzer.analyze(workflow);

    res.json({
      success: true,
      analysis,
    });
  } catch (error: any) {
    logger.error('Error analyzing workflow:', error);
    res.status(500).json({
      error: 'Failed to analyze workflow',
      message: error.message,
    });
  }
});

/**
 * GET /api/analyze/:id/suggestions
 * Get optimization suggestions
 */
router.get('/analyze/:id/suggestions', async (req: Request, res: Response) => {
  try {
    const { id } = req.params;

    const memoryManager = getMemoryManager();
    const workflow = await memoryManager.getWorkflowReference(id);

    if (!workflow) {
      return res.status(404).json({
        error: 'Workflow not found',
      });
    }

    const analyzer = getWorkflowAnalyzer();
    const analysis = analyzer.analyze(workflow.definition);

    res.json({
      success: true,
      suggestions: analysis.optimizationSuggestions,
      issues: analysis.potentialIssues,
    });
  } catch (error: any) {
    logger.error('Error getting suggestions:', error);
    res.status(500).json({
      error: 'Failed to get suggestions',
      message: error.message,
    });
  }
});

/**
 * POST /api/execute
 * Execute and store execution data
 */
router.post('/execute', async (req: Request, res: Response) => {
  try {
    const {
      workflowId,
      status,
      inputData,
      resultData,
      startedAt,
      finishedAt,
      executionTime,
      errorMessage,
      context,
      nodeExecutions,
    } = req.body;

    if (!workflowId || !status || !startedAt) {
      return res.status(400).json({
        error: 'Missing required fields',
        message: 'workflowId, status, and startedAt are required',
      });
    }

    const memoryManager = getMemoryManager();
    const executionId = await memoryManager.storeExecution({
      workflowId,
      status,
      inputData,
      resultData,
      startedAt: new Date(startedAt),
      finishedAt: finishedAt ? new Date(finishedAt) : undefined,
      executionTime,
      errorMessage,
      context,
      nodeExecutions,
    });

    res.json({
      success: true,
      executionId,
      message: 'Execution stored successfully',
    });
  } catch (error: any) {
    logger.error('Error storing execution:', error);
    res.status(500).json({
      error: 'Failed to store execution',
      message: error.message,
    });
  }
});

/**
 * GET /api/executions/:workflowId/stats
 * Get execution statistics
 */
router.get('/executions/:workflowId/stats', async (req: Request, res: Response) => {
  try {
    const { workflowId } = req.params;

    const memoryManager = getMemoryManager();
    const statistics = await memoryManager.getExecutionStatistics(workflowId);

    res.json({
      success: true,
      statistics,
    });
  } catch (error: any) {
    logger.error('Error getting execution stats:', error);
    res.status(500).json({
      error: 'Failed to get execution statistics',
      message: error.message,
    });
  }
});

/**
 * GET /api/memory/cache/stats
 * Get cache statistics
 */
router.get('/cache/stats', async (req: Request, res: Response) => {
  try {
    const memoryManager = getMemoryManager();
    const stats = memoryManager.getCacheStats();

    res.json({
      success: true,
      stats,
    });
  } catch (error: any) {
    logger.error('Error getting cache stats:', error);
    res.status(500).json({
      error: 'Failed to get cache statistics',
      message: error.message,
    });
  }
});

/**
 * POST /api/memory/prune
 * Prune old execution data
 */
router.post('/prune', async (req: Request, res: Response) => {
  try {
    const { retentionDays } = req.body;
    const days = retentionDays || 30;

    const memoryManager = getMemoryManager();
    const deletedCount = await memoryManager.pruneOldExecutions(days);

    res.json({
      success: true,
      deletedCount,
      message: `Pruned ${deletedCount} old executions`,
    });
  } catch (error: any) {
    logger.error('Error pruning executions:', error);
    res.status(500).json({
      error: 'Failed to prune executions',
      message: error.message,
    });
  }
});

export default router;
