import { Request, Response } from 'express';
import { subscriptionService } from '../services/subscription-service';
import { AuthenticatedRequest } from '../core/middleware/subscription-auth';
import { getDbClient } from '../core/database/aws-db-client';
import { queryAsService } from '../core/database/db-pool';
import { geminiWalletService } from '../services/ai/gemini-wallet-service';
import { logger } from '../core/logger';

async function ensureUserExists(userId: string, email: string): Promise<void> {
  const db = getDbClient();
  await db
    .from('users')
    .upsert({ id: userId, email, updated_at: new Date().toISOString() }, { onConflict: 'id' });
}

/**
 * GET /api/subscriptions/current
 */
export async function getCurrentSubscription(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user) {
      return res.status(401).json({ error: 'Unauthorized', code: 'AUTH_REQUIRED' });
    }

    // Auto-create user row if missing
    await ensureUserExists(req.user.id, req.user.email);

    await subscriptionService.ensureFreeSubscription(req.user.id);

    const subscription = await subscriptionService.getUserSubscription(req.user.id);
    const usage = await subscriptionService.getSubscriptionUsage(req.user.id);
    const wallet = await geminiWalletService.getState(req.user.id).catch(() => null);
    const subscriptionFrozen = Boolean(wallet?.subscriptionFrozen);
    const walletNeedsAttention = Boolean(wallet?.enabled && wallet.hasKey && ['invalid', 'quota_exceeded', 'error'].includes(wallet.status));

    if (!subscription) {
      return res.status(404).json({
        error: 'Subscription Not Found',
        message: 'No subscription found for user',
        code: 'SUBSCRIPTION_NOT_FOUND'
      });
    }

    return res.json({
      success: true,
      subscription: {
        id: subscription.id,
        planName: subscription.planName,
        status: subscription.status,
        workflowLimit: subscription.workflowLimit,
        workflowsUsed: usage.workflowsUsed,
        startedAt: subscription.startedAt,
        expiresAt: subscription.expiresAt,
        cancelledAt: subscription.cancelledAt,
        autoRenew: subscription.autoRenew
      },
      usage: {
        workflowsUsed: usage.workflowsUsed,
        workflowLimit: usage.workflowLimit,
        remainingWorkflows: usage.remainingWorkflows,
        utilizationPercentage: usage.utilizationPercentage,
        canCreateWorkflow: subscriptionFrozen || usage.remainingWorkflows > 0
      },
      billingMode: subscriptionFrozen ? 'gemini_wallet' : 'subscription',
      subscriptionFrozen,
      walletStatus: wallet?.status || 'empty',
      freezeMessage: subscriptionFrozen
        ? 'Your Gemini API key wallet is active. Subscription workflow quota is frozen while your key is used.'
        : walletNeedsAttention
          ? (wallet?.lastErrorMessage || 'Your Gemini API key wallet needs attention. Replace the key or turn off wallet mode and choose a plan.')
          : null,
    });
  } catch (error: any) {
    logger.error('[SubscriptionAPI] getCurrentSubscription error:', error);
    return res.status(500).json({
      error: 'Subscription Fetch Error',
      message: error?.message || 'Failed to fetch current subscription',
      code: 'SUBSCRIPTION_FETCH_ERROR'
    });
  }
}

/**
 * POST /api/subscriptions/cancel
 */
export async function cancelSubscription(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user) {
      return res.status(401).json({ error: 'Unauthorized', code: 'AUTH_REQUIRED' });
    }

    const { reason } = req.body;
    const result = await subscriptionService.cancelSubscription(req.user.id, reason);

    if (!result.success) {
      return res.status(400).json({
        error: 'Cancellation Failed',
        message: result.error || 'Failed to cancel subscription',
        code: result.code || 'CANCEL_FAILED'
      });
    }

    return res.json({
      success: true,
      message: 'Subscription cancelled successfully',
      subscription: result.subscription
        ? {
            id: result.subscription.id,
            planName: result.subscription.planName,
            status: result.subscription.status,
            workflowLimit: result.subscription.workflowLimit
          }
        : null
    });
  } catch (error: any) {
    logger.error('[SubscriptionAPI] cancelSubscription error:', error);
    return res.status(500).json({
      error: 'Cancellation Error',
      message: error?.message || 'Failed to cancel subscription',
      code: 'CANCEL_ERROR'
    });
  }
}

/**
 * GET /api/subscriptions/history
 */
export async function getSubscriptionHistory(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user) {
      return res.status(401).json({ error: 'Unauthorized', code: 'AUTH_REQUIRED' });
    }

    const limit = parseInt((req.query.limit as string) || '50', 10);
    const db = getDbClient();

    const { data: history, error } = await db
      .from('subscription_history')
      .select(`
        id,
        action,
        notes,
        created_at,
        from_plan:from_plan_id(name),
        to_plan:to_plan_id(name),
        payment:payment_id(razorpay_payment_id, amount_inr)
      `)
      .eq('user_id', req.user.id)
      .order('created_at', { ascending: false })
      .limit(limit);

    if (error) {
      throw new Error(`Failed to fetch subscription history: ${error.message}`);
    }

    return res.json({
      success: true,
      history: (history || []).map((item: any) => ({
        id: item.id,
        action: item.action,
        fromPlan: item.from_plan?.name || null,
        toPlan: item.to_plan?.name || null,
        paymentId: item.payment?.razorpay_payment_id || null,
        amount: item.payment?.amount_inr ? item.payment.amount_inr / 100 : null,
        notes: item.notes,
        createdAt: item.created_at
      }))
    });
  } catch (error: any) {
    logger.error('[SubscriptionAPI] getSubscriptionHistory error:', error);
    return res.status(500).json({
      error: 'History Fetch Error',
      message: error?.message || 'Failed to fetch subscription history',
      code: 'HISTORY_FETCH_ERROR'
    });
  }
}

/**
 * GET /api/subscriptions/usage
 */
export async function getSubscriptionUsage(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user) {
      return res.status(401).json({ error: 'Unauthorized', code: 'AUTH_REQUIRED' });
    }

    const usage = await subscriptionService.getSubscriptionUsage(req.user.id);

    return res.json({
      success: true,
      usage: {
        workflowsUsed: usage.workflowsUsed,
        workflowLimit: usage.workflowLimit,
        remainingWorkflows: usage.remainingWorkflows,
        utilizationPercentage: usage.utilizationPercentage,
        canCreateWorkflow: usage.remainingWorkflows > 0,
        upgradeRequired: usage.remainingWorkflows === 0
      }
    });
  } catch (error: any) {
    logger.error('[SubscriptionAPI] getSubscriptionUsage error:', error);
    return res.status(500).json({
      error: 'Usage Fetch Error',
      message: error?.message || 'Failed to fetch subscription usage',
      code: 'USAGE_FETCH_ERROR'
    });
  }
}

/**
 * POST /api/subscriptions/upgrade  (admin or payment-verified upgrade)
 */
export async function upgradeSubscription(req: AuthenticatedRequest, res: Response) {
  try {
    if (!req.user) {
      return res.status(401).json({ error: 'Unauthorized', code: 'AUTH_REQUIRED' });
    }

    const { planName, paymentId } = req.body;

    if (!planName) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'planName is required',
        code: 'MISSING_PLAN_NAME'
      });
    }

    const plan = await subscriptionService.getPlanByName(planName);
    if (!plan) {
      return res.status(400).json({
        error: 'Invalid Plan',
        message: `Plan '${planName}' not found`,
        code: 'INVALID_PLAN'
      });
    }

    if (plan.name !== 'Free' && !paymentId) {
      return res.status(400).json({
        error: 'Payment Required',
        message: 'paymentId is required for paid plans',
        code: 'PAYMENT_REQUIRED'
      });
    }

    const result = await subscriptionService.upgradeSubscription(req.user.id, planName, paymentId);

    if (!result.success) {
      return res.status(400).json({
        error: 'Upgrade Failed',
        message: result.error || 'Failed to upgrade subscription',
        code: result.code || 'UPGRADE_FAILED'
      });
    }

    return res.json({
      success: true,
      message: `Successfully upgraded to ${planName} plan`,
      subscription: result.subscription
        ? {
            id: result.subscription.id,
            planName: result.subscription.planName,
            status: result.subscription.status,
            workflowLimit: result.subscription.workflowLimit,
            workflowsUsed: result.subscription.workflowsUsed
          }
        : null
    });
  } catch (error: any) {
    logger.error('[SubscriptionAPI] upgradeSubscription error:', error);
    return res.status(500).json({
      error: 'Upgrade Error',
      message: error?.message || 'Failed to upgrade subscription',
      code: 'UPGRADE_ERROR'
    });
  }
}

/**
 * GET /api/admin/subscriptions/users  (admin only)
 */
export async function adminGetUsers(req: AuthenticatedRequest, res: Response) {
  try {
    const page = parseInt((req.query.page as string) || '1', 10);
    const limit = parseInt((req.query.limit as string) || '50', 10);
    const search = (req.query.search as string) || '';
    const offset = (page - 1) * limit;
    const safeLimit = Math.max(1, Math.min(limit, 100));
    const safeOffset = Math.max(0, offset);
    const searchPattern = `%${search}%`;

    const users = await queryAsService(
      `SELECT
         u.id,
         u.email,
         u.workflow_count,
         u.created_at,
         s.id AS subscription_id,
         s.status AS subscription_status,
         s.started_at AS subscription_started_at,
         s.expires_at AS subscription_expires_at,
         sp.name AS plan_name,
         sp.workflow_limit AS plan_workflow_limit,
         w.enabled AS wallet_enabled,
         w.status AS wallet_status,
         w.last_used_at AS wallet_last_used_at
       FROM users u
       LEFT JOIN subscriptions s
         ON s.id = u.subscription_id
       LEFT JOIN subscription_plans sp
         ON sp.id = s.plan_id
       LEFT JOIN user_ai_wallet_settings w
         ON w.user_id = u.id::text AND w.provider = 'gemini'
       WHERE ($1 = '' OR u.email ILIKE $2)
       ORDER BY u.created_at DESC
       LIMIT $3
       OFFSET $4`,
      [search, searchPattern, safeLimit, safeOffset]
    );

    const totalRows = await queryAsService<{ count: string }>(
      `SELECT COUNT(*)::text AS count
       FROM users u
       WHERE ($1 = '' OR u.email ILIKE $2)`,
      [search, searchPattern]
    );
    const total = parseInt(totalRows[0]?.count || '0', 10);

    return res.json({
      success: true,
      users: (users || []).map((user: any) => ({
        id: user.id,
        email: user.email,
        workflowCount: user.workflow_count,
        createdAt: user.created_at,
        subscription: user.subscription_id
          ? {
              id: user.subscription_id,
              planName: user.plan_name || 'Free',
              workflowLimit: user.plan_workflow_limit || 2,
              status: user.subscription_status,
              startedAt: user.subscription_started_at,
              expiresAt: user.subscription_expires_at
            }
          : null
        ,
        wallet: {
          enabled: Boolean(user.wallet_enabled),
          status: user.wallet_status || 'empty',
          lastUsedAt: user.wallet_last_used_at || null
        }
      })),
      pagination: {
        page,
        limit: safeLimit,
        total,
        totalPages: Math.ceil(total / safeLimit)
      }
    });
  } catch (error: any) {
    logger.error('[SubscriptionAPI] adminGetUsers error:', error);
    return res.status(500).json({
      error: 'Admin Users Fetch Error',
      message: error?.message || 'Failed to fetch users',
      code: 'ADMIN_USERS_FETCH_ERROR'
    });
  }
}

/**
 * POST /api/admin/subscriptions/upgrade/:userId  (admin only)
 */
export async function adminUpgradeUser(req: AuthenticatedRequest, res: Response) {
  try {
    const { userId } = req.params;
    const { planName, notes } = req.body;

    if (!planName) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'planName is required',
        code: 'MISSING_PLAN_NAME'
      });
    }

    const result = await subscriptionService.upgradeSubscription(userId, planName);

    if (!result.success) {
      return res.status(400).json({
        error: 'Admin Upgrade Failed',
        message: result.error || 'Failed to upgrade user subscription',
        code: result.code || 'ADMIN_UPGRADE_FAILED'
      });
    }

    // Log admin action
    const db = getDbClient();
    await db.from('admin_actions').insert({
      admin_user_id: req.user!.id,
      target_user_id: userId,
      action: 'subscription_upgrade',
      details: { planName, notes: notes || 'Admin upgrade', subscriptionId: result.subscription?.id },
      ip_address: req.ip,
      user_agent: req.get('User-Agent')
    });

    return res.json({
      success: true,
      message: `Successfully upgraded user to ${planName} plan`,
      subscription: result.subscription
    });
  } catch (error: any) {
    logger.error('[SubscriptionAPI] adminUpgradeUser error:', error);
    return res.status(500).json({
      error: 'Admin Upgrade Error',
      message: error?.message || 'Failed to upgrade user subscription',
      code: 'ADMIN_UPGRADE_ERROR'
    });
  }
}
