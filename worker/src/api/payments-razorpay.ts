import { Request, Response } from 'express';
import { paymentService } from '../services/payment-service';
import { subscriptionService } from '../services/subscription-service';
import { AuthenticatedRequest } from '../core/middleware/subscription-auth';
import { getDbClient } from '../core/database/aws-db-client';
import { logger } from '../core/logger';

/**
 * Ensure user exists in public.users — auto-creates if missing
 */
async function ensureUserExists(userId: string, email: string): Promise<void> {
  const db = getDbClient();
  await db
    .from('users')
    .upsert({ id: userId, email, updated_at: new Date().toISOString() }, { onConflict: 'id' });
}

/**
 * GET /api/subscriptions/plans
 * Returns all active subscription plans (public endpoint)
 */
export async function getSubscriptionPlans(req: Request, res: Response) {
  try {
    const plans = await subscriptionService.getAvailablePlans();

    return res.json({
      success: true,
      plans: plans.map(plan => ({
        id: plan.id,
        name: plan.name,
        workflowLimit: plan.workflowLimit,
        price: plan.price / 100,           // paise → rupees
        originalPrice: plan.originalPrice / 100,
        currency: plan.currency,
        features: plan.features,
        isActive: plan.isActive,
        developmentMode: plan.developmentMode,
        displayPrice: plan.price === 0 ? 'Free' : `₹${plan.price / 100}`,
        popular: plan.name === 'Pro'
      }))
    });
  } catch (error: any) {
    logger.error('[PaymentAPI] getSubscriptionPlans error:', error);
    return res.status(500).json({
      error: 'Plans Fetch Error',
      message: error?.message || 'Failed to fetch subscription plans',
      code: 'PLANS_FETCH_ERROR'
    });
  }
}

/**
 * POST /api/payments/razorpay/create-order
 * Creates a Razorpay order for a subscription upgrade
 */
export async function createRazorpayOrder(req: AuthenticatedRequest, res: Response) {
  try {
    const { planName } = req.body;

    if (!req.user) {
      return res.status(401).json({ error: 'Unauthorized', code: 'AUTH_REQUIRED' });
    }

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

    if (plan.name === 'Free') {
      return res.status(400).json({
        error: 'Invalid Plan',
        message: 'Free plan does not require payment',
        code: 'FREE_PLAN_NO_PAYMENT'
      });
    }

    // Ensure user exists in public.users (auto-create if missing)
    await ensureUserExists(req.user.id, req.user.email);

    // Ensure user has a subscription record
    await subscriptionService.ensureFreeSubscription(req.user.id);

    const order = await paymentService.createPaymentOrder(
      req.user.id,
      planName,
      req.user.email
    );

    return res.json({
      success: true,
      order: {
        id: order.id,
        amount: order.amount,
        currency: order.currency,
        planName: plan.name,
        planWorkflowLimit: plan.workflowLimit,
        developmentMode: plan.developmentMode
      },
      razorpayKeyId: process.env.RAZORPAY_KEY_ID,
      user: {
        name: req.user.email.split('@')[0],
        email: req.user.email
      }
    });
  } catch (error: any) {
    logger.error('[PaymentAPI] createRazorpayOrder error:', error);
    return res.status(500).json({
      error: 'Payment Order Failed',
      message: error?.message || 'Failed to create payment order',
      code: 'ORDER_CREATION_ERROR'
    });
  }
}

/**
 * POST /api/payments/razorpay/verify
 * Verifies Razorpay payment signature and activates subscription
 */
export async function verifyRazorpayPayment(req: AuthenticatedRequest, res: Response) {
  try {
    const { orderId, paymentId, signature } = req.body;

    if (!req.user) {
      return res.status(401).json({ error: 'Unauthorized', code: 'AUTH_REQUIRED' });
    }

    if (!orderId || !paymentId || !signature) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'orderId, paymentId, and signature are required',
        code: 'MISSING_PAYMENT_DATA'
      });
    }

    const result = await paymentService.processPaymentVerification(
      req.user.id,
      orderId,
      paymentId,
      signature
    );

    // Activate subscription — planName comes from the frontend (stored in order notes)
    if (!result.success) {
      return res.status(400).json({
        error: 'Payment Verification Failed',
        message: result.error || 'Failed to verify payment',
        code: result.code || 'VERIFICATION_FAILED'
      });
    }

    return res.json({
      success: true,
      message: 'Payment verified and subscription activated',
      subscription: result.subscription || null
    });
  } catch (error: any) {
    logger.error('[PaymentAPI] verifyRazorpayPayment error:', error);
    return res.status(500).json({
      error: 'Payment Verification Error',
      message: error?.message || 'Payment verification failed',
      code: 'VERIFICATION_ERROR'
    });
  }
}

/**
 * POST /api/payments/webhook  (raw body — registered separately in index.ts if needed)
 */
export async function handleRazorpayWebhook(req: Request, res: Response) {
  try {
    const signature = req.get('X-Razorpay-Signature') || '';
    const payload = (req.body as Buffer).toString();

    const result = await paymentService.handleWebhook(payload, signature);

    if (!result.success) {
      return res.status(400).json({ error: result.message });
    }

    return res.json({ success: true, message: result.message });
  } catch (error: any) {
    logger.error('[PaymentWebhook] error:', error);
    return res.status(500).json({ error: 'Webhook processing failed' });
  }
}
