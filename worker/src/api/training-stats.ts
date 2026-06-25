// API endpoint for training statistics and information

import { Request, Response } from 'express';
import { workflowTrainingService } from '../services/ai/workflow-training-service';
import { trainingMonitor } from '../services/ai/training-monitor';
import { logger } from '../core/logger';

/**
 * GET /api/training/stats
 * Get training dataset statistics
 */
export async function getTrainingStats(req: Request, res: Response) {
  try {
    const stats = workflowTrainingService.getTrainingStats();
    const usageMetrics = trainingMonitor.getMetrics();
    
    if (!stats) {
      return res.status(503).json({
        error: 'Training dataset not loaded',
        message: 'The training dataset could not be loaded. Please check the data file.',
      });
    }

    return res.json({
      success: true,
      stats,
      usage: usageMetrics,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    logger.error('Error getting training stats:', error);
    return res.status(500).json({
      error: 'Failed to get training stats',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * GET /api/training/categories
 * Get all available workflow categories
 */
export async function getTrainingCategories(req: Request, res: Response) {
  try {
    const categories = workflowTrainingService.getAllCategories();
    
    return res.json({
      success: true,
      categories,
      count: categories.length,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    logger.error('Error getting training categories:', error);
    return res.status(500).json({
      error: 'Failed to get training categories',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * GET /api/training/workflows?category=...
 * Get workflows by category or all workflows
 */
export async function getTrainingWorkflows(req: Request, res: Response) {
  try {
    const { category } = req.query;
    
    let workflows;
    if (category && typeof category === 'string') {
      workflows = workflowTrainingService.getWorkflowsByCategory(category);
    } else {
      // Get all workflows (limited info for response size)
      const allWorkflows = workflowTrainingService.getTrainingStats();
      workflows = allWorkflows ? { total: allWorkflows.totalWorkflows } : null;
    }
    
    return res.json({
      success: true,
      workflows,
      category: category || 'all',
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    logger.error('Error getting training workflows:', error);
    return res.status(500).json({
      error: 'Failed to get training workflows',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * POST /api/training/similar
 * Find similar workflows for a given prompt
 */
export async function findSimilarWorkflows(req: Request, res: Response) {
  try {
    const { prompt, limit = 3 } = req.body;
    
    if (!prompt || typeof prompt !== 'string') {
      return res.status(400).json({
        error: 'Invalid request',
        message: 'Prompt is required and must be a string',
      });
    }

    const similar = workflowTrainingService.getSimilarWorkflows(
      prompt,
      typeof limit === 'number' ? limit : 3
    );
    
    return res.json({
      success: true,
      prompt,
      similarWorkflows: similar.map(w => ({
        id: w.id,
        category: w.category,
        goal: w.goal,
        systemPrompt: w.phase1.step3?.systemPrompt,
        complexity: w.phase1.step7?.complexityScore,
      })),
      count: similar.length,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    logger.error('Error finding similar workflows:', error);
    return res.status(500).json({
      error: 'Failed to find similar workflows',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * GET /api/training/examples?type=systemPrompt|requirements|nodeSelection|execution&limit=2
 * Get training examples for few-shot learning
 */
export async function getTrainingExamples(req: Request, res: Response) {
  try {
    const { type, limit = 2 } = req.query;
    const limitNum = typeof limit === 'string' ? parseInt(limit, 10) : 2;
    
    if (!type || typeof type !== 'string') {
      return res.status(400).json({
        error: 'Invalid request',
        message: 'Type parameter is required (systemPrompt, requirements, nodeSelection, or execution)',
      });
    }

    let examples: any = null;
    
    switch (type) {
      case 'systemPrompt':
        examples = workflowTrainingService.getSystemPromptExamples(limitNum);
        break;
      case 'requirements':
        examples = workflowTrainingService.getRequirementsExamples(limitNum);
        break;
      case 'nodeSelection':
        examples = workflowTrainingService.getNodeSelectionExamples(limitNum);
        break;
      case 'execution':
        examples = workflowTrainingService.getExecutionExamples(limitNum);
        break;
      default:
        return res.status(400).json({
          error: 'Invalid type',
          message: 'Type must be one of: systemPrompt, requirements, nodeSelection, execution',
        });
    }
    
    return res.json({
      success: true,
      type,
      examples,
      count: examples.length,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    logger.error('Error getting training examples:', error);
    return res.status(500).json({
      error: 'Failed to get training examples',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * GET /api/training/usage
 * Get training usage metrics and monitoring data
 */
export async function getTrainingUsage(req: Request, res: Response) {
  try {
    const metrics = trainingMonitor.getMetrics();
    const { type } = req.query;

    if (type && typeof type === 'string') {
      const typeStats = trainingMonitor.getTypeStats(
        type as 'systemPrompt' | 'requirements' | 'nodeSelection' | 'execution'
      );
      return res.json({
        success: true,
        type,
        stats: typeStats,
        timestamp: new Date().toISOString(),
      });
    }

    return res.json({
      success: true,
      metrics,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    logger.error('Error getting training usage:', error);
    return res.status(500).json({
      error: 'Failed to get training usage',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * POST /api/training/reload
 * Reload training dataset (hot reload)
 */
export async function reloadTrainingDataset(req: Request, res: Response) {
  try {
    const result = workflowTrainingService.reloadDataset();
    
    if (result.success) {
      return res.json({
        success: true,
        message: result.message,
        workflows: result.workflows,
        timestamp: new Date().toISOString(),
      });
    } else {
      return res.status(500).json({
        success: false,
        error: 'Failed to reload dataset',
        message: result.message,
        timestamp: new Date().toISOString(),
      });
    }
  } catch (error) {
    logger.error('Error reloading training dataset:', error);
    return res.status(500).json({
      error: 'Failed to reload training dataset',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

