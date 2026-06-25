import { Request, Response } from 'express';
import { startWorkflowGeneration, getSession, answerCredential } from '../orchestrator';
import { logger } from '../core/logger';

/**
 * POST /api/generate
 * Body: { prompt: string }
 *
 * Starts the planner-driven workflow generation.
 * Returns the spec, clarifications (if any), and initial session state.
 */
export async function generateHandler(req: Request, res: Response) {
  const { prompt } = req.body || {};

  if (!prompt || typeof prompt !== 'string') {
    return res.status(400).json({ error: 'prompt is required' });
  }

  try {
    const session = await startWorkflowGeneration(prompt);
    return res.json({
      sessionId: session.id,
      status: session.status,
      spec: session.spec,
      clarifications: session.clarifications,
      credentialQuestions: session.credentialQuestions,
      fieldQuestions: session.fieldQuestions,
      repairs: session.repairs,
      graph: session.graph,
    });
  } catch (error) {
    logger.error('[SmartPlanner] generate error:', error);
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Unknown error',
    });
  }
}

/**
 * POST /api/answer
 * Body: {
 *   sessionId: string;
 *   clarifications?: Record<string, string>;
 *   credentials?: Array<{ provider: string; data: Record<string, any> }>;
 * }
 *
 * For now this endpoint only accepts credential answers; clarifications
 * would require re-running the planner with a refined prompt and are left
 * for future extension.
 */
export async function answerHandler(req: Request, res: Response) {
  const { sessionId, credentials } = req.body || {};

  if (!sessionId || typeof sessionId !== 'string') {
    return res.status(400).json({ error: 'sessionId is required' });
  }

  const session = await getSession(sessionId);
  if (!session) {
    return res.status(404).json({ error: 'Session not found' });
  }

  try {
    if (Array.isArray(credentials)) {
      for (const entry of credentials) {
        if (entry && typeof entry.provider === 'string' && entry.data && typeof entry.data === 'object') {
          answerCredential(entry.provider, entry.data);
        }
      }
    }

    // Return latest view of the session
    const updated = (await getSession(sessionId)) || session;
    return res.json({
      sessionId: updated.id,
      status: updated.status,
      spec: updated.spec,
      clarifications: updated.clarifications,
      credentialQuestions: updated.credentialQuestions,
      fieldQuestions: updated.fieldQuestions,
      repairs: updated.repairs,
      graph: updated.graph,
    });
  } catch (error) {
    logger.error('[SmartPlanner] answer error:', error);
    return res.status(500).json({
      error: error instanceof Error ? error.message : 'Unknown error',
    });
  }
}

/**
 * GET /api/workflow/:sessionId
 * Returns the final (or in-progress) workflow graph for the given session.
 */
export async function getWorkflowHandler(req: Request, res: Response) {
  const { sessionId } = req.params;

  if (!sessionId) {
    return res.status(400).json({ error: 'sessionId is required' });
  }

  const session = await getSession(sessionId);
  if (!session) {
    return res.status(404).json({ error: 'Session not found' });
  }

  return res.json({
    sessionId: session.id,
    status: session.status,
    graph: session.graph,
    spec: session.spec,
    repairs: session.repairs,
  });
}

export default {
  generateHandler,
  answerHandler,
  getWorkflowHandler,
};

