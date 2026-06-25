import { Request, Response } from 'express';
import { AuthenticatedRequest, refreshToken, getSession, invalidateSession, invalidateAllUserSessions, createSession } from '../core/middleware/subscription-auth';
import { getAuditTrail } from '../core/middleware/subscription-logging';
import { getSecurityEvents } from '../core/middleware/security';
import { logger } from '../core/logger';

/**
 * Refresh JWT token endpoint
 */
export async function refreshTokenEndpoint(req: AuthenticatedRequest, res: Response) {
  return refreshToken(req, res, () => {});
}

/**
 * Get current session information
 */
export async function getSessionInfo(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        error: 'Authentication required'
      });
    }

    const sessionData = req.user.sessionId ? await getSession(req.user.sessionId) : null;

    return res.json({
      success: true,
      session: sessionData ? {
        id: sessionData.id,
        createdAt: sessionData.createdAt,
        lastActivity: sessionData.lastActivity,
        ipAddress: sessionData.ipAddress,
        userAgent: sessionData.userAgent,
        isActive: sessionData.isActive
      } : null,
      user: {
        id: req.user.id,
        email: req.user.email,
        role: req.user.role,
        subscriptionPlan: req.user.subscriptionPlan,
        workflowLimit: req.user.workflowLimit,
        tokenExp: req.user.tokenExp
      }
    });
  } catch (error: any) {
    logger.error('[Auth] Get session info error:', error);
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to retrieve session information'
    });
  }
}

/**
 * Invalidate current session (logout)
 */
export async function invalidateCurrentSession(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        error: 'Authentication required'
      });
    }

    let invalidated = false;
    if (req.user.sessionId) {
      invalidated = await invalidateSession(req.user.sessionId);
    }

    return res.json({
      success: true,
      message: 'Session invalidated successfully',
      sessionInvalidated: invalidated
    });
  } catch (error: any) {
    logger.error('[Auth] Invalidate session error:', error);
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to invalidate session'
    });
  }
}

/**
 * Invalidate all user sessions (logout from all devices)
 */
export async function invalidateAllSessions(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        error: 'Authentication required'
      });
    }

    const invalidatedCount = await invalidateAllUserSessions(req.user.id);

    return res.json({
      success: true,
      message: 'All sessions invalidated successfully',
      sessionsInvalidated: invalidatedCount
    });
  } catch (error: any) {
    logger.error('[Auth] Invalidate all sessions error:', error);
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to invalidate sessions'
    });
  }
}

/**
 * Get audit trail (admin only)
 */
export async function getAuditTrailEndpoint(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user || req.user.role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Admin access required'
      });
    }

    const {
      limit = '100',
      userId,
      operation,
      startDate,
      endDate
    } = req.query;

    const auditEntries = getAuditTrail(
      parseInt(limit as string, 10),
      userId as string,
      operation as string,
      startDate ? new Date(startDate as string) : undefined,
      endDate ? new Date(endDate as string) : undefined
    );

    return res.json({
      success: true,
      auditTrail: auditEntries,
      count: auditEntries.length,
      filters: {
        limit: parseInt(limit as string, 10),
        userId: userId || null,
        operation: operation || null,
        startDate: startDate || null,
        endDate: endDate || null
      }
    });
  } catch (error: any) {
    logger.error('[Admin] Get audit trail error:', error);
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to retrieve audit trail'
    });
  }
}

/**
 * Get security events (admin only)
 */
export async function getSecurityEventsEndpoint(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user || req.user.role !== 'admin') {
      return res.status(403).json({
        success: false,
        error: 'Admin access required'
      });
    }

    const { limit = '100' } = req.query;
    const securityEvents = getSecurityEvents(parseInt(limit as string, 10));

    return res.json({
      success: true,
      securityEvents,
      count: securityEvents.length
    });
  } catch (error: any) {
    logger.error('[Admin] Get security events error:', error);
    return res.status(500).json({
      success: false,
      error: error?.message || 'Failed to retrieve security events'
    });
  }
}

/**
 * Validate token endpoint (for frontend token validation)
 */
export async function validateToken(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user) {
      return res.status(401).json({
        success: false,
        valid: false,
        error: 'Invalid or expired token'
      });
    }

    const now = Math.floor(Date.now() / 1000);
    const timeUntilExpiry = req.user.tokenExp ? req.user.tokenExp - now : null;
    const isExpiringSoon = timeUntilExpiry ? timeUntilExpiry < 900 : false; // Less than 15 minutes

    return res.json({
      success: true,
      valid: true,
      user: {
        id: req.user.id,
        email: req.user.email,
        role: req.user.role,
        subscriptionPlan: req.user.subscriptionPlan,
        workflowLimit: req.user.workflowLimit
      },
      token: {
        expiresIn: timeUntilExpiry,
        isExpiringSoon,
        needsRefresh: isExpiringSoon
      }
    });
  } catch (error: any) {
    logger.error('[Auth] Validate token error:', error);
    return res.status(500).json({
      success: false,
      valid: false,
      error: error?.message || 'Token validation failed'
    });
  }
}
