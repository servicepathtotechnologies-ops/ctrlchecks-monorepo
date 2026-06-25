/**
 * Main Express.js Server for CtrlChecks Worker
 * Worker API — Node + Express backend
 * Gemini AI (GEMINI_API_KEY)
 * 
 * 🚨 CRITICAL: Environment variables MUST be loaded FIRST
 * The env-loader module loads dotenv synchronously before any other imports.
 * This ensures process.env is populated before config.ts and other modules read it.
 */

// ⚡ LOAD ENVIRONMENT VARIABLES FIRST - DO NOT MOVE THIS IMPORT
// This must be the very first import to ensure all environment variables are loaded
// before any other code (especially config.ts) tries to read process.env
import './core/env-loader';

// ✅ FAIL-FAST: Validate required environment variables immediately after loading.
// Missing required vars cause process.exit(1) with a clear diagnostic list.
import { assertEnv } from './core/config/env-validator';
assertEnv();

// ✅ ERROR TRACKING: Initialize Sentry immediately after env validation.
// No-op when SENTRY_DSN is absent (dev / test).
import { initSentry } from './core/sentry';
initSentry();

// ✅ CRITICAL: Initialize NodeLibrary early to ensure schemas are loaded
// This ensures nodeLibrary is initialized before any validators try to use it
import { nodeLibrary } from './services/nodes/node-library';
import { NodeSchemaRegistry } from './core/contracts/node-schema-registry';

// ✅ ROOT-LEVEL: Initialize Node Context Registry
// This ensures all node contexts are available for AI understanding
// NOTE: Import triggers initialization (singleton pattern) - no manual init needed
import { nodeContextRegistry } from './core/registry/node-context-registry';

// ✅ ARCHITECTURAL REFACTOR: Initialize UnifiedNodeRegistry (Single Source of Truth)
// NOTE: Import triggers initialization (singleton pattern) - no manual init needed
import { unifiedNodeRegistry } from './core/registry/unified-node-registry';

// ✅ VERIFICATION ONLY: Registries are already initialized via imports above
// Just verify they're working correctly (no duplicate initialization)
try {
  const contextCount = nodeContextRegistry.getAllNodeTypes().length;
  console.log(`[ServerStartup] ✅ Node Context Registry verified (${contextCount} node contexts)`);
} catch (error: any) {
  console.error('[ServerStartup] ❌ Node Context Registry verification failed:', error.message);
  throw error; // Stop boot if context registry fails - this is critical
}

// Initialize node registry on startup
console.log('[ServerStartup] 🔵 Initializing node registry...');
try {
  const registry = NodeSchemaRegistry.getInstance();
  console.log('[ServerStartup] ✅ Node registry initialized');
  
  // ✅ VERIFICATION ONLY: UnifiedNodeRegistry is already initialized via import above
  // Just verify it's working correctly (no duplicate initialization)
  try {
    const nodeCount = unifiedNodeRegistry.getAllTypes().length;
    console.log(`[ServerStartup] ✅ UnifiedNodeRegistry verified (${nodeCount} node definitions)`);
    
    // Verify critical nodes are in unified registry
    const criticalNodes = ['google_sheets', 'ai_chat_model', 'google_gmail'];
    const missingInUnified: string[] = [];
    for (const nodeType of criticalNodes) {
      if (!unifiedNodeRegistry.has(nodeType)) {
        missingInUnified.push(nodeType);
      }
    }
    if (missingInUnified.length > 0) {
      console.warn(`[ServerStartup] ⚠️  Missing in UnifiedNodeRegistry: ${missingInUnified.join(', ')}`);
    } else {
      console.log(`[ServerStartup] ✅ All critical nodes verified in UnifiedNodeRegistry`);
    }
  } catch (error: any) {
    console.error('[ServerStartup] ❌ UnifiedNodeRegistry verification failed:', error.message);
  }
  
  // Verify critical nodes are registered
  const criticalNodes = [
    'google_gmail',
    'ai_agent',
    'ai_chat_model',
    'manual_trigger',
    'chat_trigger',
  ];
  const missingNodes: string[] = [];
  
  for (const nodeType of criticalNodes) {
    const schema = registry.get(nodeType);
    if (!schema) {
      missingNodes.push(nodeType);
      console.error(`[ServerStartup] ❌ Critical node missing from registry: ${nodeType}`);
    } else {
      console.log(`[ServerStartup] ✅ Critical node registered: ${nodeType}`);
    }
  }
  
  if (missingNodes.length > 0) {
    console.error(`[ServerStartup] ❌ Missing critical nodes: ${missingNodes.join(', ')}`);
  } else {
    console.log('[ServerStartup] ✅ All critical nodes verified in registry');
  }
  
  // ✅ PRODUCTION-GRADE: Validate alias resolution on startup via unified-node-registry
  try {
    const aliasTests: Array<{ alias: string; expectedCanonical: string }> = [
      { alias: 'gmail', expectedCanonical: 'google_gmail' },
      { alias: 'email', expectedCanonical: 'google_gmail' },
      { alias: 'mail', expectedCanonical: 'google_gmail' },
    ];

    for (const { alias, expectedCanonical } of aliasTests) {
      const resolved = unifiedNodeRegistry.resolveAlias(alias);
      if (resolved !== expectedCanonical) {
        console.warn(`[ServerStartup] ⚠️  Alias "${alias}" resolved to "${resolved}" but expected "${expectedCanonical}"`);
      }
    }
  } catch (error: any) {
    // Silent catch - alias resolution works, validation is non-critical
    // Only log if it's a critical system error
    if (error.message && !error.message.includes('Alias resolution validation')) {
      console.warn(`[ServerStartup] ⚠️  Alias validation check failed: ${error.message}`);
    }
  }
} catch (error: any) {
  console.error('[ServerStartup] ❌ Failed to initialize node registry:', error.message);
}

import express, { Express, Request, Response } from 'express';
import { networkInterfaces } from 'os';
import helmet from 'helmet';
import { config } from './core/config';
import { corsMiddleware, getAllowedOrigins } from './core/middleware/cors';
import { errorHandler, asyncHandler } from './core/middleware/error-handler';
import { httpLogger } from './core/logger';

// AI: Gemini (GEMINI_API_KEY)
import { modelManager } from './services/ai/model-manager';
import { metricsTracker } from './services/ai/metrics-tracker';
import { geminiOrchestrator } from './services/ai/gemini-orchestrator';
import { LLMAdapter } from './shared/llm-adapter';

const aiLlmAdapter = new LLMAdapter();

// Import route handlers
import executeWorkflowRoute from './api/execute-workflow';
import internalEngineExecuteRoute from './api/internal-engine-execute';
import webhookTriggerRoute from './api/webhook-trigger';
import chatApiRoute from './api/chat-api';
import adminTemplatesRoute from './api/admin-templates';
import templatesRoute from './api/templates';
import adminUsersRoute from './api/admin-users';
import deleteAccountRoute from './api/delete-account';
import copyTemplateRoute from './api/copy-template';
import formTriggerRoute from './api/form-trigger';
import chatTriggerRoute from './api/chat-trigger';
import generateWorkflowRoute from './api/generate-workflow';
import analyzeCapabilitySelection from './api/capability-selection/analyze';
import generateCapabilityWorkflow from './api/capability-selection/generate';
import confirmCapabilityWorkflow from './api/capability-selection/confirm';
import executeAgentRoute from './api/execute-agent';
import chatbotRoute from './api/chatbot';
import analyzeWorkflowRequirementsRoute from './api/analyze-workflow-requirements';
import processRoute from './api/process';
import executeNodeRoute from './api/execute-node';
import testType1NodeHandler from './api/test-type1-node';
import testAllType1NodesHandler from './api/test-all-type1-nodes';
import aiGateway from './api/ai-gateway';
import aiErrorGuidanceHandler from './api/ai-error-guidance';
import { generateHandler as smartPlannerGenerate, answerHandler as smartPlannerAnswer, getWorkflowHandler as smartPlannerGetWorkflow } from './api/smart-planner';
import * as trainingStats from './api/training-stats';
import getCredentialsRoute from './api/get-credentials';
import attachCredentialsRoute from './api/attach-credentials';
import attachInputsRoute from './api/attach-inputs';
import saveWorkflowRoute from './api/save-workflow';
import { setupDraftWorkflowHandler, commitSetupWorkflowHandler } from './api/workflow-setup-lifecycle';
import getMissingItemsRoute from './api/workflows-missing-items';
import configureWorkflowRoute from './api/workflows-configure';
import { confirmWorkflow, rejectWorkflow } from './api/workflow-confirm';
import { substituteTools, getAvailableSubstitutions } from './api/tool-substitute';
import { serveChatbotPage } from './api/chatbot-page';
import { handleChatbotMessage } from './api/chatbot-message';
import { serveChatbotChat } from './api/chatbot-chat';
import * as nodeContractRoutes from './api/node-contract';
import * as workflowVersioningRoutes from './api/workflow-versioning';
import memoryRoutes from './api/memory';
import distributedExecuteWorkflow, { getExecutionStatus } from './api/distributed-execute-workflow';
import nodeDefinitionsHandler from './api/node-definitions';
import { nodesCatalogHandler } from './api/nodes-catalog';
import {
  createConnectionHandler,
  credentialTypesHandler,
  deleteConnectionHandler,
  executeAuthenticatedRequestHandler,
  listConnectionsHandler,
  oauthCallbackHandler as genericOAuthCallbackHandler,
  oauthReconnectHandler,
  oauthStartHandler,
  registryNodesHandler,
  testConnectionHandler,
  updateConnectionHandler,
} from './api/credential-connections';
import { credentialExecutionAuthMiddleware } from './credentials-system/execution-auth-middleware';
import workflowFieldOwnershipCatalogHandler from './api/workflow-field-ownership-catalog';
import { linkedinStatusHandler, linkedinTestHandler, linkedinRefreshNowHandler, linkedinDisconnectHandler } from './api/connections-linkedin';
import { githubStatusHandler, githubDisconnectHandler } from './api/connections-github';
import { makeSocialDisconnectHandler } from './api/connections-social';
import { makeOAuthTableDisconnectHandler } from './api/connections-oauth';
import { zohoStatusHandler, zohoConnectHandler, zohoTestHandler, zohoDisconnectHandler } from './api/connections-zoho';
import {
  connectionsCatalogHandler,
  connectionsStatusHandler,
  singleConnectionStatusHandler,
  logConnectionConfigReadiness,
} from './api/connections-catalog';
import { credentialStatusHandler } from './api/credentials-status';
import { authStatusHandler } from './api/auth-status';
import { transferWorkflowOwnership, transferAllWorkflows } from './api/workflow-transfer';
import saveSocialTokenRoute from './api/save-social-token';
import { notionAuthorizeHandler, notionCallbackHandler } from './api/oauth-notion';
import { twitterAuthorizeHandler, twitterCallbackHandler } from './api/oauth-twitter';
import { facebookOAuthStart, facebookOAuthCallback } from './api/oauth-facebook';
import { googleOAuthStart, googleOAuthCallback, googleDisconnectHandler } from './api/oauth-google';
import { linkedInOAuthStart, linkedInOAuthCallback } from './api/oauth-linkedin';
import {
  instagramAuthorizeHandler,
  instagramCallbackHandler,
  whatsappAuthorizeHandler,
  whatsappCallbackHandler,
} from './api/oauth-meta';
import { salesforceAuthorizeHandler, salesforceCallbackHandler } from './api/oauth-salesforce';
import { isOAuthProviderMigrated, proxyToCredentialService } from './middleware/credential-oauth-proxy';
import { createRazorpayOrder, verifyRazorpayPayment, getSubscriptionPlans } from './api/payments-razorpay';
import { getCurrentSubscription, cancelSubscription, getSubscriptionHistory, adminGetUsers, adminUpgradeUser } from './api/subscriptions';
import {
  activateGeminiWalletHandler,
  deactivateGeminiWalletHandler,
  deleteGeminiWalletHandler,
  getGeminiWalletHandler,
  saveGeminiWalletHandler,
  testGeminiWalletHandler,
} from './api/ai-wallet';
import { securityHeaders, subscriptionRateLimit, validateSubscriptionInput, developmentModeHeaders, requestLogger } from './core/middleware/security';
import { authenticateUser, requireAdmin, optionalAuth, requireRole, requireSubscriptionPlan } from './core/middleware/subscription-auth';
import { subscriptionLogger, paymentLogger, adminLogger } from './core/middleware/subscription-logging';
import { checkWorkflowLimitEndpoint, requireWorkflowCapacityForAi } from './core/middleware/workflow-limits';
import { geminiWalletContextMiddleware } from './core/middleware/gemini-wallet-context-middleware';
import { geminiWalletService } from './services/ai/gemini-wallet-service';
import { distributedRateLimit } from './core/middleware/distributed-rate-limit';
import { tierRateLimit } from './core/middleware/tier-rate-limit';
import { tracingMiddleware } from './core/observability/distributed-tracing';
import { metricsHandler, requestMetricsMiddleware } from './middleware/highScaleMetrics';
import { redisGetCache } from './middleware/redisGetCache';
import { tokenBucketRateLimiter } from './middleware/redisTokenBucket';
import { kafkaWriteQueueMiddleware } from './middleware/kafkaRequestQueue';
import { requestIdMiddleware } from './core/middleware/request-id';
import {
  refreshTokenEndpoint,
  getSessionInfo, 
  invalidateCurrentSession, 
  invalidateAllSessions, 
  getAuditTrailEndpoint, 
  getSecurityEventsEndpoint, 
  validateToken 
} from './api/auth-management';



console.log('[ServerStartup] 🔵 Creating Express app...');
const app: Express = express();
app.set('trust proxy', true);
logConnectionConfigReadiness();
console.log('[ServerStartup] ✅ Express app created');

// === REQUEST ID MIDDLEWARE (must be first) ===
app.use(requestIdMiddleware);

// === HELMET: baseline HTTP security headers ===
// Disabled directives are already handled by the securityHeaders middleware in
// security.ts (CSP, X-Frame-Options, HSTS, Referrer-Policy, X-XSS-Protection).
// Cross-Origin-* policies are disabled to preserve OAuth redirect/popup flows.
app.use(
  helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false,
    crossOriginOpenerPolicy: false,
    crossOriginResourcePolicy: false,
    frameguard: false,
    hsts: false,
    referrerPolicy: false,
    xssFilter: false,
  })
);

// === PINO-HTTP: structured JSON request/response logging with requestId ===
app.use(httpLogger);

// Middleware
console.log('[ServerStartup] 🔵 Registering middleware...');
app.use(express.json({
  limit: '50mb',
  verify: (req: any, _res, buf) => {
    req.rawBody = Buffer.from(buf);
  },
}));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(corsMiddleware);
app.use(requestMetricsMiddleware);
app.use(tokenBucketRateLimiter({
  capacity: Number(process.env.RATE_LIMIT_PER_MINUTE || 100),
  refillPerMinute: Number(process.env.RATE_LIMIT_PER_MINUTE || 100),
  skipPaths: ['/health', '/metrics'],
}));
app.use(redisGetCache({
  ttlSeconds: Number(process.env.GET_CACHE_TTL_SECONDS || 60),
  // User-facing DB reads must reflect the live database. Caching these paths
  // can turn a transient DB outage into a stale "empty dashboard" response.
  skipPaths: [
    '/health',
    '/metrics',
    '/api/credential-connections/connections',
    '/api/execution-status',
    '/api/db/workflows',
    '/api/db/executions',
    '/api/db/user_roles',
  ],
}));

// Security middleware for subscription system
app.use(securityHeaders);
app.use(developmentModeHeaders);
app.use(requestLogger);
app.use(tracingMiddleware);
app.use(validateSubscriptionInput);

console.log('[ServerStartup] ✅ Middleware registered');

app.get('/metrics', asyncHandler(metricsHandler));

// Health check (Gemini AI status)
console.log('[ServerStartup] 🔵 Registering /health endpoint...');
app.get('/health', asyncHandler(async (req: Request, res: Response) => {
  try {
    const stats = metricsTracker.getStats();
    const geminiConfigured = !!(config.geminiApiKey && config.geminiApiKey.trim().length > 0);
    const { circuitBreakerManager } = await import('./services/workflow-executor/distributed/reliability/circuit-breaker');
    const { aiSreOrchestrator } = await import('./services/ai-sre-orchestrator');
    const circuitBreakerStats = circuitBreakerManager.getAllStats();
    const openCircuits = circuitBreakerStats.filter((entry) => entry.state === 'open').length;
    const reliabilityDiagnostics = {
      strictValidation: config.reliability?.strictValidation ?? false,
      validateNodeOutput: config.reliability?.validateNodeOutput ?? false,
      aiSelfCheckEnabled: config.reliability?.aiSelfCheckEnabled ?? false,
      distributedRateLimitEnabled: config.reliability?.distributedRateLimitEnabled ?? false,
      redisSessionEnabled: config.reliability?.redisSessionEnabled ?? false,
      tracingEnabled: config.reliability?.tracingEnabled ?? false,
      dlqMandatoryRouting: config.reliability?.dlqMandatoryRouting ?? false,
      autonomousOpsEnabled: config.reliability?.autonomousOpsEnabled ?? false,
      autonomousOpsStatus: aiSreOrchestrator.getStatus(),
      circuitBreakers: {
        total: circuitBreakerStats.length,
        open: openCircuits,
      },
    };
    const { getPoolStats } = await import('./core/database/db-pool');
    const dbPool = getPoolStats();
    const dbStatus = dbPool.waitingCount > 0 ? 'degraded' : dbPool.utilization > 80 ? 'warning' : 'healthy';

    res.json({
      status: geminiConfigured ? 'healthy' : 'degraded',
      backend: 'running',
      ai: geminiConfigured ? 'gemini' : 'unconfigured',
      geminiConfigured,
      aiMetrics: {
        totalRequests: stats.totalRequests,
        successRate: `${stats.successRate.toFixed(1)}%`,
        averageResponseTime: `${stats.averageResponseTime.toFixed(0)}ms`,
      },
      database: { ...dbPool, status: dbStatus },
      timestamp: new Date().toISOString(),
      environment: process.env.NODE_ENV || 'development',
      port: config.port,
      reliability: reliabilityDiagnostics,
      endpoints: [
        '/api/execute-workflow',
        '/api/webhook-trigger',
        '/api/chat-api',
        '/api/form-trigger',
        '/api/generate-workflow',
        '/api/execute-agent',
        '/api/chatbot',
        '/api/analyze-workflow-requirements',
        '/api/ai/generate',
        '/api/ai/chat',
        '/api/ai/analyze-image',
        '/api/ai/models',
        '/api/ai/metrics',
        '/api/training/stats',
        '/api/training/categories',
        '/api/training/workflows',
        '/api/training/similar',
        '/api/training/examples',
        '/process',
        '/execute-node',
        '/api/execute-node',
        '/api/admin-templates',
        '/api/copy-template',
      ],
    });
  } catch (error) {
    console.error('[HealthCheck] ⚠️  Health check error (non-fatal):', error);
    res.json({
      status: 'degraded',
      backend: 'running',
      ai: 'unknown',
      error: error instanceof Error ? error.message : String(error),
      timestamp: new Date().toISOString(),
      environment: process.env.NODE_ENV || 'development',
      port: config.port,
    });
  }
}));
console.log('[ServerStartup] ✅ /health endpoint registered');

// ── Kubernetes-style liveness probe ──────────────────────────────────────────
// Always returns 200 while the process is alive.  Restart policy uses this.
app.get('/health/live', (_req: Request, res: Response) => {
  res.status(200).json({ status: 'live', timestamp: new Date().toISOString() });
});

// ── Readiness probe ───────────────────────────────────────────────────────────
// Returns 200 only when DB + Redis are reachable.  Load-balancers use this
// to stop routing traffic during startup or dependency outages.
app.get('/health/ready', asyncHandler(async (_req: Request, res: Response) => {
  const checks: Record<string, 'ok' | 'fail'> = {};
  let allOk = true;

  // DB check — real SELECT 1 via existing pool (honours circuit breaker)
  try {
    const { isDatabaseReachable } = await import('./core/database/db-pool');
    const ok = await Promise.race([
      isDatabaseReachable(),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 4000)),
    ]);
    checks.db = ok ? 'ok' : 'fail';
    if (!ok) allOk = false;
  } catch {
    checks.db = 'fail';
    allOk = false;
  }

  // Redis check — reuses the shared ioredis singleton (no new connection per request)
  try {
    const { isRedisAvailable } = await import('./shared/redis-client');
    const ok = await Promise.race([
      isRedisAvailable(),
      new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 2000)),
    ]);
    checks.redis = ok ? 'ok' : 'fail';
    if (!ok) allOk = false;
  } catch {
    checks.redis = 'fail';
    allOk = false;
  }

  const status = allOk ? 200 : 503;
  res.status(status).json({
    status: allOk ? 'ready' : 'not_ready',
    checks,
    timestamp: new Date().toISOString(),
  });
}));

// Gemini key-pool health — safe metrics only (no key values)
app.get('/api/health/gemini', asyncHandler(async (_req: Request, res: Response) => {
  try {
    const { getGeminiKeyPool } = await import('./services/ai/gemini-key-pool');
    const pool = getGeminiKeyPool();
    const metrics = pool.getMetrics();
    const allHealthy = metrics.every(m => m.healthy && !m.inCooldown);
    res.json({
      status: allHealthy ? 'healthy' : 'degraded',
      keyCount: pool.size,
      keys: metrics,
      timestamp: new Date().toISOString(),
    });
  } catch (err: any) {
    res.status(503).json({
      status: 'unavailable',
      error: err?.message || 'Gemini key pool not initialised',
      timestamp: new Date().toISOString(),
    });
  }
}));

// Cache-clear endpoint — clears the Gemini in-memory cache so fresh prompts are re-analyzed
app.post('/api/admin/clear-cache', asyncHandler(async (req: Request, res: Response) => {
  const { geminiOrchestrator } = require('./services/ai/gemini-orchestrator');
  geminiOrchestrator.clearCache();
  console.log('[AdminAPI] 🧹 Gemini cache cleared via /api/admin/clear-cache');
  res.json({ success: true, message: 'Gemini cache cleared. Next workflow generation will re-analyze from scratch.' });
}));

// Connection test endpoint (more detailed than health)
app.get('/api/test-connection', asyncHandler(async (req: Request, res: Response) => {
  const origins = getAllowedOrigins();
  res.json({
    success: true,
    message: 'Backend is running and reachable',
    timestamp: new Date().toISOString(),
    frontendUrl: req.headers.origin || 'unknown',
    backendUrl: `${req.protocol}://${req.get('host')}`,
    environment: process.env.NODE_ENV || 'development',
    port: config.port,
    publicBaseUrl: config.publicBaseUrl,
    corsOrigins: origins,
      endpoints: {
      health: '/health',
      chatbot: '/api/ai/chatbot/message',
      testConnection: '/api/test-connection',
      chatWebSocket: '/api/chat/health',
    },
  });
}));

// WebSocket health check endpoint
app.get('/api/chat/health', asyncHandler(async (req: Request, res: Response) => {
  try {
    const { getChatServer } = require('./services/chat/chat-server');
    const chatServer = getChatServer();
    const activeSessions = chatServer.getActiveSessions();
    
    res.json({
      status: 'ok',
      websocket: {
        initialized: true,
        path: '/ws/chat',
        activeSessions: activeSessions.length,
        sessions: activeSessions,
      },
      timestamp: new Date().toISOString(),
    });
  } catch (error: any) {
    res.status(500).json({
      status: 'error',
      error: error?.message || 'Unknown error',
      websocket: {
        initialized: false,
      },
      timestamp: new Date().toISOString(),
    });
  }
}));

// API Routes
console.log('[ServerStartup] 🔵 Registering /api/execute-workflow endpoint...');
app.post(
  '/api/execute-workflow',
  distributedRateLimit({
    endpointKey: 'execute-workflow',
    perUserLimit: 60,
    globalLimit: 1200,
    windowMs: 60_000,
  }),
  tierRateLimit('execute'),
  asyncHandler(authenticateUser),
  asyncHandler(executeWorkflowRoute)
);

// Internal: execution-engine → worker (server-to-server, x-internal-engine-key required)
console.log('[ServerStartup] 🔵 Registering /api/internal/engine-execute endpoint...');
app.post('/api/internal/engine-execute', asyncHandler(internalEngineExecuteRoute));

// 🆕 Execution Queue API
app.get('/api/execution-queue/stats', asyncHandler(async (req: Request, res: Response) => {
  const { getExecutionQueue } = await import('./services/execution-queue');
  const queue = await getExecutionQueue();
  const stats = await queue.getStats();
  res.json(stats);
}));

app.get('/api/execution-queue/job/:jobId', asyncHandler(async (req: Request, res: Response) => {
  const { jobId } = req.params;
  const { getExecutionQueue } = await import('./services/execution-queue');
  const queue = await getExecutionQueue();
  const job = await queue.getJobStatus(jobId);
  
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  res.json(job);
}));

app.post('/api/execution-queue/job/:jobId/cancel', asyncHandler(async (req: Request, res: Response) => {
  const { jobId } = req.params;
  const { getExecutionQueue } = await import('./services/execution-queue');
  const queue = await getExecutionQueue();
  const cancelled = await queue.cancelJob(jobId);
  
  if (!cancelled) {
    return res.status(400).json({ error: 'Job cannot be cancelled' });
  }
  
  res.json({ success: true, message: 'Job cancelled' });
}));

// 🆕 Workflow Logger API
app.get('/api/workflow-logs/:executionId', asyncHandler(async (req: Request, res: Response) => {
  const { executionId } = req.params;
  const { getWorkflowLogger } = await import('./services/workflow-logger');
  const logger = getWorkflowLogger();
  const logs = logger.getLogs(executionId);
  res.json({ executionId, logs, count: logs.length });
}));

app.get('/api/workflow-logs/workflow/:workflowId', asyncHandler(async (req: Request, res: Response) => {
  const { workflowId } = req.params;
  const { getWorkflowLogger } = await import('./services/workflow-logger');
  const logger = getWorkflowLogger();
  const logs = logger.getWorkflowLogs(workflowId);
  res.json({ workflowId, logs, count: logs.length });
}));

app.get('/api/workflow-logs/correlation/:correlationId', asyncHandler(async (req: Request, res: Response) => {
  const { correlationId } = req.params;
  const { getWorkflowLogger } = await import('./services/workflow-logger');
  const logger = getWorkflowLogger();
  const logs = logger.getLogsByCorrelationId(correlationId);
  res.json({ correlationId, logs, count: logs.length });
}));

// 🆕 Credential Vault API
app.post('/api/credentials/store', asyncHandler(authenticateUser), asyncHandler(async (req: Request, res: Response) => {
  const userId = (req as any).user?.id;
  const { workflowId, key, value, type, metadata } = req.body;
  
  if (!userId || !key || !value || !type) {
    return res.status(400).json({ error: 'key, value, and type are required' });
  }
  
  const { getCredentialVault } = await import('./services/credential-vault');
  const vault = getCredentialVault();
  
  const credential = await vault.store(
    { userId, workflowId },
    key,
    value,
    type,
    metadata
  );

  const { queryAsService } = await import('./core/database/db-pool');
  await queryAsService(
    `INSERT INTO user_credentials (user_id, service, credentials, updated_at)
     VALUES ($1, $2, $3::jsonb, NOW())
     ON CONFLICT (user_id, service)
     DO UPDATE SET credentials = EXCLUDED.credentials, updated_at = NOW()`,
    [
      userId,
      String(key).toLowerCase(),
      JSON.stringify({
        connected: true,
        type,
        fields: metadata?.fields || null,
        savedAt: new Date().toISOString(),
      }),
    ]
  ).catch((err) => {
    console.warn('[CredentialVault] user_credentials mirror failed:', err.message);
  });
  
  res.json({ success: true, credential: { ...credential, encryptedValue: '[REDACTED]' } });
}));

app.get('/api/credentials/retrieve/:key', asyncHandler(authenticateUser), asyncHandler(async (req: Request, res: Response) => {
  const { key } = req.params;
  const userId = (req as any).user?.id;
  const { workflowId } = req.query;
  
  if (!userId) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  
  const { getCredentialVault } = await import('./services/credential-vault');
  const vault = getCredentialVault();
  
  const value = await vault.retrieve(
    { 
      userId, 
      workflowId: (typeof workflowId === 'string' ? workflowId : undefined) 
    },
    key
  );
  
  if (!value) {
    return res.status(404).json({ error: 'Credential not found' });
  }
  
  res.json({ success: true, value: '[REDACTED]' }); // Never return actual value in API
}));

app.get('/api/credentials/list', asyncHandler(authenticateUser), asyncHandler(async (req: Request, res: Response) => {
  const userId = (req as any).user?.id;
  const { workflowId } = req.query;
  
  if (!userId) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  
  const { getCredentialVault } = await import('./services/credential-vault');
  const vault = getCredentialVault();
  
  const credentials = await vault.list({
    userId,
    workflowId: (typeof workflowId === 'string' ? workflowId : undefined),
  });
  
  res.json({ success: true, credentials });
}));

app.delete('/api/credentials/:key', asyncHandler(authenticateUser), asyncHandler(async (req: Request, res: Response) => {
  const { key } = req.params;
  const userId = (req as any).user?.id;
  const { workflowId } = req.query;
  
  if (!userId) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  
  const { getCredentialVault } = await import('./services/credential-vault');
  const vault = getCredentialVault();
  
  await vault.delete(
    { 
      userId, 
      workflowId: (typeof workflowId === 'string' ? workflowId : undefined) 
    },
    key
  );

  const { queryAsService } = await import('./core/database/db-pool');
  await queryAsService(
    `DELETE FROM user_credentials WHERE user_id = $1 AND service = $2`,
    [userId, String(key).toLowerCase()]
  ).catch(() => []);
  
  res.json({ success: true, message: 'Credential deleted' });
}));

// ✅ Node Definitions API - Backend is source of truth for node schemas
import './nodes/definitions'; // Register all node definitions
app.get('/api/node-definitions', asyncHandler(nodeDefinitionsHandler));
app.get('/api/nodes/catalog', asyncHandler(nodesCatalogHandler));
app.get('/api/credential-connections/registry/nodes', asyncHandler(registryNodesHandler));
app.get('/api/credential-connections/credential-types', asyncHandler(credentialTypesHandler));
app.get('/api/credential-connections/connections', asyncHandler(authenticateUser), asyncHandler(listConnectionsHandler));
app.post('/api/credential-connections/connections', asyncHandler(authenticateUser), asyncHandler(createConnectionHandler));
app.put('/api/credential-connections/connections/:id', asyncHandler(authenticateUser), asyncHandler(updateConnectionHandler));
app.delete('/api/credential-connections/connections/:id', asyncHandler(authenticateUser), asyncHandler(deleteConnectionHandler));
app.post('/api/credential-connections/connections/:id/test', asyncHandler(authenticateUser), asyncHandler(testConnectionHandler));
app.post('/api/credential-connections/connections/:id/reconnect', asyncHandler(authenticateUser), asyncHandler(oauthReconnectHandler));
// Generic OAuth (Slack, HubSpot, etc.) — proxy start when credentialTypeId is a migrated provider.
// Proxy checks CREDENTIAL_SERVICE_OAUTH_ENABLED + credentialTypeId; on disabled/mismatch/timeout → local.
// Phase 3: callbacks always run locally. Phase 4: callbacks proxied when PROVIDERS=*.
app.get('/api/credential-connections/oauth/start', asyncHandler(authenticateUser), proxyToCredentialService, asyncHandler(oauthStartHandler));
app.post('/api/credential-connections/oauth/start', asyncHandler(authenticateUser), proxyToCredentialService, asyncHandler(oauthStartHandler));
app.get('/api/credential-connections/oauth/callback', proxyToCredentialService, asyncHandler(genericOAuthCallbackHandler));
app.post('/api/credential-connections/oauth/callback', proxyToCredentialService, asyncHandler(genericOAuthCallbackHandler));
app.post(
  '/api/credential-connections/execute-request',
  asyncHandler(authenticateUser),
  credentialExecutionAuthMiddleware(),
  asyncHandler(executeAuthenticatedRequestHandler),
);

// Distributed Workflow Engine Routes
app.post(
  '/api/distributed-execute-workflow',
  distributedRateLimit({
    endpointKey: 'distributed-execute-workflow',
    perUserLimit: 30,
    globalLimit: 900,
    windowMs: 60_000,
  }),
  asyncHandler(distributedExecuteWorkflow)
);
app.get('/api/execution-status/:executionId', asyncHandler(getExecutionStatus));

// Auth status endpoint
app.get('/api/auth/status', authenticateUser, asyncHandler(authStatusHandler));

// Enhanced Authentication Management API endpoints
app.post('/api/auth/refresh-token', 
  subscriptionRateLimit(5, 300000), // 5 requests per 5 minutes
  subscriptionLogger('refresh-token'),
  asyncHandler(authenticateUser), 
  asyncHandler(refreshTokenEndpoint)
);
app.get('/api/auth/session', 
  subscriptionLogger('get-session'),
  asyncHandler(authenticateUser), 
  asyncHandler(getSessionInfo)
);
app.post('/api/auth/logout', 
  subscriptionLogger('logout'),
  asyncHandler(authenticateUser), 
  asyncHandler(invalidateCurrentSession)
);
app.post('/api/auth/logout-all', 
  subscriptionRateLimit(3, 300000), // 3 requests per 5 minutes
  subscriptionLogger('logout-all'),
  asyncHandler(authenticateUser), 
  asyncHandler(invalidateAllSessions)
);
app.get('/api/auth/validate', 
  subscriptionLogger('validate-token'),
  asyncHandler(authenticateUser), 
  asyncHandler(validateToken)
);

// Admin-only audit and security endpoints
app.get('/api/admin/audit-trail', 
  adminLogger('get-audit-trail'),
  asyncHandler(authenticateUser), 
  requireAdmin, 
  asyncHandler(getAuditTrailEndpoint)
);
app.get('/api/admin/security-events', 
  adminLogger('get-security-events'),
  asyncHandler(authenticateUser), 
  requireAdmin, 
  asyncHandler(getSecurityEventsEndpoint)
);

// DLQ Admin API
app.get(
  '/api/admin/dlq/stats',
  adminLogger('dlq-stats'),
  asyncHandler(authenticateUser),
  requireAdmin,
  asyncHandler(async (_req: Request, res: Response) => {
    const { getDeadLetterQueue } = await import('./services/workflow-executor/distributed/reliability/dead-letter-queue');
    const dlq = getDeadLetterQueue();
    if (!dlq.isAvailable()) {
      await dlq.initialize(config.redisUrl);
    }
    const stats = await dlq.getStats();
    res.json({ success: true, stats });
  })
);

app.get(
  '/api/admin/autonomous-ops/status',
  adminLogger('autonomous-ops-status'),
  asyncHandler(authenticateUser),
  requireAdmin,
  asyncHandler(async (_req: Request, res: Response) => {
    const { aiSreOrchestrator } = await import('./services/ai-sre-orchestrator');
    res.json({ success: true, status: aiSreOrchestrator.getStatus() });
  })
);

app.get(
  '/api/admin/dlq/jobs',
  adminLogger('dlq-jobs'),
  asyncHandler(authenticateUser),
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const { limit = '100', reason } = req.query;
    const { getDeadLetterQueue } = await import('./services/workflow-executor/distributed/reliability/dead-letter-queue');
    const dlq = getDeadLetterQueue();
    if (!dlq.isAvailable()) {
      await dlq.initialize(config.redisUrl);
    }
    const parsedLimit = Math.max(1, Math.min(500, parseInt(limit as string, 10) || 100));
    const jobs = typeof reason === 'string'
      ? await dlq.getJobsByReason(reason as any, parsedLimit)
      : await dlq.getAllJobs(parsedLimit);
    res.json({ success: true, count: jobs.length, jobs });
  })
);

app.post(
  '/api/admin/dlq/replay/:jobId',
  adminLogger('dlq-replay'),
  asyncHandler(authenticateUser),
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const { jobId } = req.params;
    const { getDeadLetterQueue } = await import('./services/workflow-executor/distributed/reliability/dead-letter-queue');
    const { createQueueClient } = await import('./services/workflow-executor/distributed/queue-client');
    const dlq = getDeadLetterQueue();
    if (!dlq.isAvailable()) {
      await dlq.initialize(config.redisUrl);
    }
    const job = await dlq.getJob(jobId);
    if (!job) {
      return res.status(404).json({ success: false, error: 'DLQ job not found' });
    }

    const queueClient = createQueueClient();
    await queueClient.connect();
    await queueClient.publishJob({
      execution_id: job.originalJob.executionId,
      node_id: job.originalJob.nodeId,
      node_type: job.originalJob.nodeType,
      retry_attempt: 0,
      job_id: `${job.originalJob.id}-replay-${Date.now()}`,
      priority: job.originalJob.priority,
    });
    await queueClient.close();
    await dlq.removeJob(jobId);

    return res.json({
      success: true,
      message: 'DLQ job replayed and removed',
      replayedJobId: jobId,
    });
  })
);

app.delete(
  '/api/admin/dlq/jobs/:jobId',
  adminLogger('dlq-delete'),
  asyncHandler(authenticateUser),
  requireAdmin,
  asyncHandler(async (req: Request, res: Response) => {
    const { jobId } = req.params;
    const { getDeadLetterQueue } = await import('./services/workflow-executor/distributed/reliability/dead-letter-queue');
    const dlq = getDeadLetterQueue();
    if (!dlq.isAvailable()) {
      await dlq.initialize(config.redisUrl);
    }
    await dlq.removeJob(jobId);
    res.json({ success: true, message: 'DLQ job removed' });
  })
);

// Social media token management endpoint
app.post('/api/social-tokens', asyncHandler(authenticateUser), asyncHandler(saveSocialTokenRoute));

// Workflow Limit Enforcement API
app.get('/api/workflows/limit-check', 
  subscriptionLogger('limit-check'),
  asyncHandler(authenticateUser), 
  asyncHandler(checkWorkflowLimitEndpoint)
);

// Subscription Management API endpoints
app.get('/api/subscriptions/plans', 
  subscriptionLogger('get-plans'),
  asyncHandler(getSubscriptionPlans)
);
app.get('/api/subscriptions/current', 
  subscriptionLogger('get-current'),
  asyncHandler(authenticateUser), 
  asyncHandler(geminiWalletContextMiddleware),
  asyncHandler(getCurrentSubscription)
);
app.post('/api/subscriptions/cancel', 
  subscriptionRateLimit(3, 300000), // 3 requests per 5 minutes
  subscriptionLogger('cancel'),
  asyncHandler(authenticateUser), 
  asyncHandler(cancelSubscription)
);
app.get('/api/subscriptions/history', 
  subscriptionLogger('get-history'),
  asyncHandler(authenticateUser),
  asyncHandler(geminiWalletContextMiddleware),
  asyncHandler(getSubscriptionHistory)
);

// Gemini BYOK wallet API
app.get('/api/ai-wallet/gemini', asyncHandler(authenticateUser), asyncHandler(geminiWalletContextMiddleware), asyncHandler(getGeminiWalletHandler));
app.put('/api/ai-wallet/gemini', asyncHandler(authenticateUser), asyncHandler(geminiWalletContextMiddleware), asyncHandler(saveGeminiWalletHandler));
app.post('/api/ai-wallet/gemini/test', asyncHandler(authenticateUser), asyncHandler(geminiWalletContextMiddleware), asyncHandler(testGeminiWalletHandler));
app.post('/api/ai-wallet/gemini/activate', asyncHandler(authenticateUser), asyncHandler(geminiWalletContextMiddleware), asyncHandler(activateGeminiWalletHandler));
app.post('/api/ai-wallet/gemini/deactivate', asyncHandler(authenticateUser), asyncHandler(geminiWalletContextMiddleware), asyncHandler(deactivateGeminiWalletHandler));
app.delete('/api/ai-wallet/gemini', asyncHandler(authenticateUser), asyncHandler(geminiWalletContextMiddleware), asyncHandler(deleteGeminiWalletHandler));

// Razorpay payment endpoints (test/live based on key pair)
app.post('/api/payments/razorpay/create-order', 
  subscriptionRateLimit(5, 60000), // 5 requests per minute
  paymentLogger('create-order'),
  asyncHandler(authenticateUser), 
  asyncHandler(createRazorpayOrder)
);
app.post('/api/payments/razorpay/verify', 
  subscriptionRateLimit(10, 60000), // 10 requests per minute
  paymentLogger('verify-payment'),
  asyncHandler(authenticateUser), 
  asyncHandler(verifyRazorpayPayment)
);

// Admin subscription management endpoints
app.get('/api/admin/subscriptions/users',
  adminLogger('get-subscription-users'),
  asyncHandler(authenticateUser),
  requireAdmin,
  asyncHandler(adminGetUsers)
);
app.post('/api/admin/subscriptions/upgrade/:userId',
  adminLogger('admin-upgrade-subscription'),
  asyncHandler(authenticateUser),
  requireAdmin,
  asyncHandler(adminUpgradeUser)
);

// Workflow ownership transfer
app.post('/api/workflows/transfer-all', asyncHandler(authenticateUser), asyncHandler(transferAllWorkflows));
app.post('/api/workflows/:workflowId/transfer-ownership', asyncHandler(authenticateUser), asyncHandler(transferWorkflowOwnership));

// LinkedIn connection DX/debugging endpoints
app.get('/api/connections/catalog', asyncHandler(connectionsCatalogHandler));
app.get('/api/connections/status', asyncHandler(authenticateUser), asyncHandler(connectionsStatusHandler));
app.get('/api/credentials/status', asyncHandler(authenticateUser), asyncHandler(credentialStatusHandler));
app.get('/api/connections/linkedin/status', asyncHandler(authenticateUser), asyncHandler(linkedinStatusHandler));
app.post('/api/connections/linkedin/test', asyncHandler(authenticateUser), asyncHandler(linkedinTestHandler));

// GitHub connection endpoints
app.get('/api/connections/github/status', asyncHandler(authenticateUser), asyncHandler(githubStatusHandler));
app.post('/api/connections/github/disconnect', asyncHandler(authenticateUser), asyncHandler(githubDisconnectHandler));
app.post('/api/connections/facebook/disconnect', asyncHandler(authenticateUser), asyncHandler(makeSocialDisconnectHandler('facebook')));
app.delete('/api/connections/facebook', asyncHandler(authenticateUser), asyncHandler(makeSocialDisconnectHandler('facebook')));
app.delete('/api/connections/instagram', asyncHandler(authenticateUser), asyncHandler(makeOAuthTableDisconnectHandler('instagram')));
app.post('/api/connections/instagram/disconnect', asyncHandler(authenticateUser), asyncHandler(makeOAuthTableDisconnectHandler('instagram')));
app.delete('/api/connections/whatsapp', asyncHandler(authenticateUser), asyncHandler(makeOAuthTableDisconnectHandler('whatsapp')));
app.post('/api/connections/whatsapp/disconnect', asyncHandler(authenticateUser), asyncHandler(makeOAuthTableDisconnectHandler('whatsapp')));

// GitHub OAuth flow (no auth middleware on callback or start-login — GitHub redirects here directly)
// start-login (Cognito sign-in via GitHub) stays on worker — NOT proxied.
import { githubOAuthStart, githubOAuthCallback, githubLoginStart, githubExchangeSession } from './api/oauth-github';
app.get('/api/oauth/github/start-login', githubLoginStart);               // primary sign-in — stays on worker
if (isOAuthProviderMigrated('github')) {
  app.get('/api/oauth/github/start',    proxyToCredentialService);         // connect flow — proxy
  app.get('/api/oauth/github/callback', proxyToCredentialService);
}
app.get('/api/oauth/github/start',       githubOAuthStart);               // local fallback
app.get('/api/oauth/github/callback',    asyncHandler(githubOAuthCallback));
app.post('/api/oauth/github/exchange-session', githubExchangeSession);    // frontend token exchange

if (isOAuthProviderMigrated('facebook')) {
  app.get('/api/oauth/facebook/start',    proxyToCredentialService);
  app.get('/api/oauth/facebook/callback', proxyToCredentialService);
}
app.get('/api/oauth/facebook/start',     facebookOAuthStart);
app.get('/api/oauth/facebook/callback',  asyncHandler(facebookOAuthCallback));

if (isOAuthProviderMigrated('google')) {
  app.get('/api/oauth/google/start',    proxyToCredentialService);
  app.get('/api/oauth/google/callback', proxyToCredentialService);
}
app.get('/api/oauth/google/start',       googleOAuthStart);
app.get('/api/oauth/google/callback',    asyncHandler(googleOAuthCallback));
app.delete('/api/connections/google',    asyncHandler(authenticateUser), asyncHandler(googleDisconnectHandler));

if (isOAuthProviderMigrated('linkedin')) {
  app.get('/api/oauth/linkedin/start',    proxyToCredentialService);
  app.get('/api/oauth/linkedin/callback', proxyToCredentialService);
}
app.get('/api/oauth/linkedin/start',     linkedInOAuthStart);
app.get('/api/oauth/linkedin/callback',  asyncHandler(linkedInOAuthCallback));
if (isOAuthProviderMigrated('instagram')) {
  app.get('/api/oauth/instagram/authorize', proxyToCredentialService);
  app.get('/api/oauth/instagram/callback',  proxyToCredentialService);
}
app.get('/api/oauth/instagram/authorize', instagramAuthorizeHandler);
app.post('/api/oauth/instagram/callback', asyncHandler(authenticateUser), asyncHandler(instagramCallbackHandler));
if (isOAuthProviderMigrated('whatsapp')) {
  app.get('/api/oauth/whatsapp/authorize', proxyToCredentialService);
  app.get('/api/oauth/whatsapp/callback',  proxyToCredentialService);
}
app.get('/api/oauth/whatsapp/authorize', whatsappAuthorizeHandler);
app.post('/api/oauth/whatsapp/callback', asyncHandler(authenticateUser), asyncHandler(whatsappCallbackHandler));
if (isOAuthProviderMigrated('salesforce')) {
  app.get('/api/oauth/salesforce/authorize', proxyToCredentialService);
  app.get('/api/oauth/salesforce/callback',  proxyToCredentialService);
}
app.get('/api/oauth/salesforce/authorize', salesforceAuthorizeHandler);
app.post('/api/oauth/salesforce/callback', asyncHandler(authenticateUser), asyncHandler(salesforceCallbackHandler));
app.delete('/api/connections/salesforce', asyncHandler(authenticateUser), asyncHandler(makeOAuthTableDisconnectHandler('salesforce')));
app.post('/api/connections/salesforce/disconnect', asyncHandler(authenticateUser), asyncHandler(makeOAuthTableDisconnectHandler('salesforce')));

// Zoho OAuth dedicated proxy routes (Phase 4) — before the legacy connections/zoho/* routes
if (isOAuthProviderMigrated('zoho')) {
  app.get('/api/oauth/zoho/authorize', proxyToCredentialService);
  app.get('/api/oauth/zoho/callback',  proxyToCredentialService);
}

// Zoho connection endpoints (legacy — stays on worker)
app.get('/api/connections/zoho/status', asyncHandler(authenticateUser), asyncHandler(zohoStatusHandler));
app.post('/api/connections/zoho/connect', asyncHandler(authenticateUser), asyncHandler(zohoConnectHandler));
app.post('/api/connections/zoho/test', asyncHandler(authenticateUser), asyncHandler(zohoTestHandler));
app.delete('/api/connections/zoho', asyncHandler(authenticateUser), asyncHandler(zohoDisconnectHandler));
app.get('/api/connections/:provider/status', asyncHandler(authenticateUser), asyncHandler(singleConnectionStatusHandler));

// Notion OAuth endpoints
if (isOAuthProviderMigrated('notion')) {
  app.get('/api/oauth/notion/authorize', proxyToCredentialService);
  app.get('/api/oauth/notion/callback',  proxyToCredentialService);
}
app.get('/api/oauth/notion/authorize', asyncHandler(notionAuthorizeHandler));
app.post('/api/oauth/notion/callback', asyncHandler(authenticateUser), asyncHandler(notionCallbackHandler));
app.delete('/api/connections/notion', asyncHandler(authenticateUser), asyncHandler(makeOAuthTableDisconnectHandler('notion')));
app.post('/api/connections/notion/disconnect', asyncHandler(authenticateUser), asyncHandler(makeOAuthTableDisconnectHandler('notion')));

// Twitter OAuth endpoints
if (isOAuthProviderMigrated('twitter')) {
  app.get('/api/oauth/twitter/authorize', proxyToCredentialService);
  app.get('/api/oauth/twitter/callback',  proxyToCredentialService);
}
app.get('/api/oauth/twitter/authorize', asyncHandler(twitterAuthorizeHandler));
app.post('/api/oauth/twitter/callback', asyncHandler(authenticateUser), asyncHandler(twitterCallbackHandler));
app.delete('/api/connections/twitter', asyncHandler(authenticateUser), asyncHandler(makeOAuthTableDisconnectHandler('twitter')));
app.post('/api/connections/twitter/disconnect', asyncHandler(authenticateUser), asyncHandler(makeOAuthTableDisconnectHandler('twitter')));
app.post('/api/connections/linkedin/refresh-now', asyncHandler(authenticateUser), asyncHandler(linkedinRefreshNowHandler));
app.delete('/api/connections/linkedin', asyncHandler(authenticateUser), asyncHandler(linkedinDisconnectHandler));

app.post('/api/webhook-trigger/:workflowId', asyncHandler(webhookTriggerRoute));
app.get('/api/webhook-trigger/:workflowId', asyncHandler(webhookTriggerRoute));
app.post('/api/chat-api', asyncHandler(chatApiRoute));

// Public active templates routes
app.get('/api/templates', asyncHandler(templatesRoute));
app.get('/api/templates/:id', asyncHandler(templatesRoute));

// Admin templates routes (with /api prefix)
app.get('/api/admin-templates', asyncHandler(adminTemplatesRoute));
app.get('/api/admin-templates/:id', asyncHandler(adminTemplatesRoute));
app.post('/api/admin-templates', asyncHandler(adminTemplatesRoute));
app.put('/api/admin-templates/:id', asyncHandler(adminTemplatesRoute));
app.delete('/api/admin-templates/:id', asyncHandler(adminTemplatesRoute));

// Admin templates routes (without /api prefix for frontend compatibility)
app.get('/admin-templates', asyncHandler(adminTemplatesRoute));
app.get('/admin-templates/:id', asyncHandler(adminTemplatesRoute));
app.post('/admin-templates', asyncHandler(adminTemplatesRoute));
app.put('/admin-templates/:id', asyncHandler(adminTemplatesRoute));
app.delete('/admin-templates/:id', asyncHandler(adminTemplatesRoute));
app.patch('/admin-templates/:id', asyncHandler(adminTemplatesRoute));

// Admin users routes (with /api prefix)
app.get('/api/admin-users', asyncHandler(adminUsersRoute));
app.get('/api/admin-users/:id', asyncHandler(adminUsersRoute));
app.patch('/api/admin-users/:id', asyncHandler(adminUsersRoute));
app.delete('/api/admin-users/:id', asyncHandler(adminUsersRoute));

// Admin users routes (without /api prefix for frontend compatibility)
app.get('/admin-users', asyncHandler(adminUsersRoute));
app.get('/admin-users/:id', asyncHandler(adminUsersRoute));
app.patch('/admin-users/:id', asyncHandler(adminUsersRoute));
app.delete('/admin-users/:id', asyncHandler(adminUsersRoute));

// Delete own account route
app.delete('/api/user/account', asyncHandler(deleteAccountRoute));

// Copy template routes
app.post('/api/copy-template', asyncHandler(copyTemplateRoute));
app.post('/copy-template', asyncHandler(copyTemplateRoute));

// Form Trigger Routes - More specific route first
app.get('/api/form-trigger/:workflowId/:nodeId', asyncHandler(formTriggerRoute));
app.post('/api/form-trigger/:workflowId/:nodeId/submit', asyncHandler(formTriggerRoute));

// Chat Trigger Routes
app.get('/api/chat-trigger/:workflowId/:nodeId', asyncHandler(chatTriggerRoute));
app.post('/api/chat-trigger/:workflowId/:nodeId/message', asyncHandler(chatTriggerRoute));

// Workflow Generation
app.post(
  '/api/generate-workflow',
  distributedRateLimit({
    endpointKey: 'generate-workflow',
    perUserLimit: 20,
    globalLimit: 300,
    windowMs: 60_000,
  }),
  tierRateLimit('generate'),
  asyncHandler(authenticateUser),
  asyncHandler(geminiWalletContextMiddleware),
  asyncHandler(requireWorkflowCapacityForAi),
  asyncHandler(generateWorkflowRoute)
);

// Capability-Based Node Selection Flow (3-phase pipeline)
app.post('/api/capability-selection/analyze', asyncHandler(authenticateUser), asyncHandler(geminiWalletContextMiddleware), asyncHandler(requireWorkflowCapacityForAi), asyncHandler(analyzeCapabilitySelection));
app.post('/api/capability-selection/generate', asyncHandler(authenticateUser), asyncHandler(geminiWalletContextMiddleware), asyncHandler(requireWorkflowCapacityForAi), asyncHandler(generateCapabilityWorkflow));
app.post('/api/capability-selection/confirm', asyncHandler(authenticateUser), asyncHandler(geminiWalletContextMiddleware), asyncHandler(requireWorkflowCapacityForAi), asyncHandler(confirmCapabilityWorkflow));
console.log('🎯 Capability Selection API available at /api/capability-selection/{analyze,generate,confirm}');

// Smart Planner–Driven Workflow Orchestration (planner decides WHAT, system decides HOW)
app.post(
  '/api/generate',
  distributedRateLimit({
    endpointKey: 'smart-generate',
    perUserLimit: 20,
    globalLimit: 300,
    windowMs: 60_000,
  }),
  asyncHandler(authenticateUser),
  asyncHandler(geminiWalletContextMiddleware),
  asyncHandler(requireWorkflowCapacityForAi),
  asyncHandler(smartPlannerGenerate)
);
app.post('/api/answer', asyncHandler(smartPlannerAnswer));
app.get('/api/workflow/:sessionId', asyncHandler(smartPlannerGetWorkflow));

// Agent Routes
app.post('/api/execute-agent', asyncHandler(executeAgentRoute));

// Chatbot
app.post('/api/chatbot', asyncHandler(chatbotRoute));
app.post('/chatbot', asyncHandler(chatbotRoute)); // Alias for frontend compatibility


// Process Route - Direct proxy to FastAPI backend
app.post('/process', asyncHandler(processRoute));

// Workflow Analysis
app.post('/api/analyze-workflow-requirements', asyncHandler(analyzeWorkflowRequirementsRoute));

// Debug Node Execution (for Debug Panel)
app.post('/execute-node', asyncHandler(executeNodeRoute));
app.post('/api/execute-node', asyncHandler(executeNodeRoute)); // Also support /api prefix

// Type 1 Node Testing — automated fixture-based tests (no credentials required)
app.post('/api/test-type1-node', asyncHandler(testType1NodeHandler));
app.post('/api/test-all-type1-nodes', asyncHandler(testAllType1NodesHandler));
console.log('[ServerStartup] ✅ Type 1 node test endpoints registered: /api/test-type1-node, /api/test-all-type1-nodes');

// Debug Gmail Send (for testing Gmail credential resolution)
import debugGmailSendRoute from './api/debug/gmail-send';
app.post('/api/debug/gmail-send', asyncHandler(debugGmailSendRoute));
console.log('🧪 Debug Gmail Send API available at /api/debug/gmail-send');

// Get User Credentials
app.get('/api/credentials', asyncHandler(getCredentialsRoute));

// Attach Credentials to Workflow
app.post('/api/workflows/:workflowId/attach-credentials', asyncHandler(attachCredentialsRoute));
console.log('🔐 Attach Credentials API available at /api/workflows/:workflowId/attach-credentials');

// Attach Node Inputs to Workflow
app.post('/api/workflows/:workflowId/attach-inputs', asyncHandler(attachInputsRoute));
console.log('🔧 Attach Inputs API available at /api/workflows/:workflowId/attach-inputs');

// Hidden AI setup draft lifecycle
app.post('/api/workflows/setup-draft', asyncHandler(setupDraftWorkflowHandler));
app.post('/api/workflows/:workflowId/commit-setup', asyncHandler(commitSetupWorkflowHandler));
console.log('🧩 Workflow Setup Lifecycle API available at /api/workflows/setup-draft and /api/workflows/:workflowId/commit-setup');

// Get Missing Items (Credentials + Sensitive Inputs)
app.get('/api/workflows/:workflowId/missing-items', asyncHandler(getMissingItemsRoute));
console.log('🔍 Missing Items API available at /api/workflows/:workflowId/missing-items');
app.get('/api/workflows/:workflowId/field-ownership-catalog', asyncHandler(workflowFieldOwnershipCatalogHandler));
console.log('🧭 Field Ownership Catalog API available at /api/workflows/:workflowId/field-ownership-catalog');

// Get last runtime-resolved inputs for a workflow (read-only observability data)
app.get('/api/workflows/:workflowId/last-resolved-inputs', asyncHandler(async (req: Request, res: Response) => {
  const { workflowId } = req.params;
  const authHeader = req.headers.authorization;
  const token = authHeader?.startsWith('Bearer ') ? authHeader.replace('Bearer ', '').trim() : '';
  if (!token) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { getDbClient } = await import('./core/database/aws-db-client');
  const db = getDbClient();
  const { data: authData, error: authError } = await db.auth.getUser(token);
  if (authError || !authData?.user?.id) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const userId = authData.user.id;
  const { data: workflow, error: workflowError } = await db
    .from('workflows')
    .select('id, user_id, setup_completed, metadata')
    .eq('id', workflowId)
    .single();
  if (workflowError || !workflow || workflow.user_id !== userId) {
    return res.status(404).json({ error: 'Workflow not found' });
  }

  const { isSetupPending } = await import('./api/workflow-setup-lifecycle');
  if (isSetupPending(workflow)) {
    return res.status(200).json({
      workflowId,
      values: {},
      executionCountScanned: 0,
      setupPending: true,
    });
  }

  const { data: executions, error: executionsError } = await db
    .from('executions')
    .select('id, started_at, status')
    .eq('workflow_id', workflowId)
    .in('status', ['success', 'failed', 'running', 'waiting'])
    .order('started_at', { ascending: false })
    .limit(10);

  if (executionsError) {
    return res.status(500).json({ error: 'Failed to load execution history' });
  }

  const values: Record<string, Record<string, {
    value: unknown;
    source?: 'static_config' | 'template' | 'deterministic_runtime' | 'runtime_ai' | 'field_directive_ai' | 'credential' | 'system';
    executionId: string;
    startedAt: string;
  }>> = {};
  const audits: Record<string, {
    executionId: string;
    startedAt: string;
    runtimeInputAudit?: unknown;
    runtimeInputHandoffAudit?: unknown;
    runtimeResolutionAudit?: unknown;
  }> = {};

  const executionStartedAt = new Map<string, string>();
  for (const execution of executions || []) {
    executionStartedAt.set(execution.id, execution.started_at);
  }
  const executionIds = Array.from(executionStartedAt.keys());

  if (executionIds.length > 0) {
    const { data: steps, error: stepsError } = await db
      .from('execution_steps')
      .select('execution_id, node_id, input_json, sequence')
      .in('execution_id', executionIds)
      .order('sequence', { ascending: true });

    if (stepsError) {
      return res.status(500).json({ error: 'Failed to load resolved execution inputs' });
    }

    for (const step of steps || []) {
      const nodeId = typeof step.node_id === 'string' ? step.node_id : undefined;
      const inputJson = step.input_json && typeof step.input_json === 'object' ? step.input_json as Record<string, unknown> : null;
      const startedAt = executionStartedAt.get(step.execution_id);
      if (!nodeId || !inputJson || !startedAt) continue;
      if (!values[nodeId]) values[nodeId] = {};
      for (const [fieldName, value] of Object.entries(inputJson)) {
        if (fieldName.startsWith('_') || values[nodeId][fieldName] !== undefined) continue;
        values[nodeId][fieldName] = {
          value,
          source: 'deterministic_runtime',
          executionId: step.execution_id,
          startedAt,
        };
      }
    }
  }

  /*
   * Legacy fallback for older execution rows that predate execution_steps input_json.
   * Keep this bounded to the already-selected 10 execution ids.
   */
  if (executionIds.length > 0) {
    const { data: executionLogs, error: logsError } = await db
      .from('executions')
      .select('id, started_at, logs')
      .in('id', executionIds);
    if (logsError) {
      return res.status(500).json({ error: 'Failed to load execution logs' });
    }

  for (const execution of executionLogs || []) {
    const logs = Array.isArray(execution.logs) ? execution.logs : [];
    for (const rawLog of logs) {
      if (!rawLog || typeof rawLog !== 'object') continue;
      const log = rawLog as any;
      const nodeId = typeof log.nodeId === 'string' ? log.nodeId : undefined;
      const resolvedInputs = log.resolvedInputs && typeof log.resolvedInputs === 'object'
        ? log.resolvedInputs
        : null;
      const resolvedInputSources = log.resolvedInputSources && typeof log.resolvedInputSources === 'object'
        ? log.resolvedInputSources
        : {};
      if (!nodeId) continue;

      if (!audits[nodeId]) {
        audits[nodeId] = {
          executionId: execution.id,
          startedAt: execution.started_at,
          runtimeInputAudit: log.runtimeInputAudit,
          runtimeInputHandoffAudit: log.runtimeInputHandoffAudit,
          runtimeResolutionAudit: log.runtimeResolutionAudit,
        };
      }

      if (!resolvedInputs) continue;

      if (!values[nodeId]) values[nodeId] = {};
      for (const [fieldName, value] of Object.entries(resolvedInputs)) {
        if (values[nodeId][fieldName] !== undefined) {
          continue; // preserve newest value only (executions are DESC)
        }
        values[nodeId][fieldName] = {
          value,
          source: (resolvedInputSources as any)?.[fieldName],
          executionId: execution.id,
          startedAt: execution.started_at,
        };
      }
    }
  }
  }

  return res.json({
    workflowId,
    values,
    audits,
    executionCountScanned: (executions || []).length,
  });
}));
console.log('🧾 Last Resolved Inputs API available at /api/workflows/:workflowId/last-resolved-inputs');

// Configure Workflow (Inject Credentials + Inputs + Auto-config + Validate)
app.post('/api/workflows/:workflowId/configure', asyncHandler(configureWorkflowRoute));
console.log('⚙️  Configure Workflow API available at /api/workflows/:workflowId/configure');

// Save Workflow (with validation and normalization)
app.post('/api/save-workflow', asyncHandler(authenticateUser), asyncHandler(saveWorkflowRoute));
console.log('💾 Save Workflow API available at /api/save-workflow');

// List Workflows (canary — try workflow-crud-service, fall back to DB)
app.get('/api/workflows', authenticateUser, asyncHandler(async (req: Request, res: Response) => {
  const userId = (req as any).user?.id;

  const { shouldUseWorkflowCrudService, listWorkflowsRemote } = await import('./services/workflow-crud-service-client');
  if (shouldUseWorkflowCrudService(userId)) {
    try {
      const remote = await listWorkflowsRemote(userId);
      if (remote) {
        console.log(`[ListWorkflows] ✅ Delegated to workflow-crud-service (userId=${userId})`);
        return res.json({ ...remote, source: 'workflow-crud-service' });
      }
      console.warn('[ListWorkflows] workflow-crud-service returned null — falling back to local path');
    } catch (delegateErr) {
      console.warn('[ListWorkflows] workflow-crud-service delegation error — falling back:', delegateErr);
    }
  }

  const { queryAsService } = await import('./core/database/db-pool');
  const rows = await queryAsService(
    `SELECT * FROM workflows WHERE user_id = $1 ORDER BY updated_at DESC`,
    [userId]
  );
  return res.json({ workflows: rows, total: rows.length });
}));
console.log('📋 List Workflows API available at GET /api/workflows');

// Get Workflow by ID (canary — try workflow-crud-service, fall back to DB)
app.get('/api/workflows/:id', authenticateUser, asyncHandler(async (req: Request, res: Response) => {
  const { id } = req.params;
  const userId = (req as any).user?.id;

  const { shouldUseWorkflowCrudService, getWorkflowRemote } = await import('./services/workflow-crud-service-client');
  if (shouldUseWorkflowCrudService(userId)) {
    try {
      const remote = await getWorkflowRemote(userId, id);
      if (remote) {
        console.log(`[GetWorkflow] ✅ Delegated to workflow-crud-service (id=${id})`);
        return res.json({ ...remote, source: 'workflow-crud-service' });
      }
      console.warn('[GetWorkflow] workflow-crud-service returned null — falling back to local path');
    } catch (delegateErr) {
      console.warn('[GetWorkflow] workflow-crud-service delegation error — falling back:', delegateErr);
    }
  }

  const { queryAsService } = await import('./core/database/db-pool');
  const rows = await queryAsService<{ id: string; user_id: string }>(
    `SELECT * FROM workflows WHERE id = $1 AND user_id = $2 LIMIT 1`,
    [id, userId]
  );
  if (!rows[0]) return res.status(404).json({ error: 'Workflow not found' });
  return res.json({ workflow: rows[0] });
}));
console.log('🔍 Get Workflow API available at GET /api/workflows/:id');

// Delete Workflow
app.delete('/api/workflows/:id', authenticateUser, asyncHandler(async (req: Request, res: Response) => {
  const { id } = req.params;
  const userId = (req as any).user?.id;

  // Workflow CRUD service canary — delegate if enabled for this userId.
  const { shouldUseWorkflowCrudService, deleteWorkflowRemote, isWorkflowCrudLocalWritesDisabled } = await import('./services/workflow-crud-service-client');
  if (shouldUseWorkflowCrudService(userId)) {
    try {
      const remote = await deleteWorkflowRemote(userId, id);
      if (remote) {
        console.log(`[DeleteWorkflow] ✅ Delegated to workflow-crud-service (id=${id})`);
        return res.json({ ...remote, source: 'workflow-crud-service' });
      }
      console.warn('[DeleteWorkflow] workflow-crud-service returned null — falling back to local path');
    } catch (delegateErr) {
      console.warn('[DeleteWorkflow] workflow-crud-service delegation error — falling back:', delegateErr);
    }
    // LOCAL_WRITES_DISABLED gate
    if (isWorkflowCrudLocalWritesDisabled()) {
      return res.status(503).json({ error: 'Workflow CRUD service unavailable', code: 'WORKFLOW_CRUD_SERVICE_UNAVAILABLE' });
    }
  }

  const { queryAsService } = await import('./core/database/db-pool');
  const rows = await queryAsService<{ id: string; user_id: string }>(
    `SELECT id, user_id FROM workflows WHERE id = $1 LIMIT 1`,
    [id]
  );
  if (!rows[0]) return res.status(404).json({ error: 'Workflow not found' });
  if (rows[0].user_id !== userId && (req as any).user?.role !== 'admin') {
    return res.status(403).json({ error: 'Forbidden' });
  }
  await queryAsService(`DELETE FROM workflows WHERE id = $1`, [id]);
  const { subscriptionService } = await import('./services/subscription-service');
  await subscriptionService.decrementWorkflowCount(userId);
  res.json({ success: true });
}));
console.log('🗑️  Delete Workflow API available at DELETE /api/workflows/:id');

// Cancel Execution
import { cancelExecutionRoute } from './api/cancel-execution';
app.post('/api/executions/:executionId/cancel', authenticateUser, asyncHandler(cancelExecutionRoute));
console.log('🛑 Cancel Execution API available at POST /api/executions/:executionId/cancel');

// Secure DB Proxy (frontend CRUD on whitelisted tables — user-scoped)
import { dbProxyGet, dbProxyPost, dbProxyUpsert, dbProxyPut, dbProxyDelete } from './api/db-proxy';
app.get('/api/db/:table',           authenticateUser, asyncHandler(dbProxyGet));
app.post('/api/db/:table/upsert',   authenticateUser, kafkaWriteQueueMiddleware(), asyncHandler(dbProxyUpsert));
app.post('/api/db/:table',          authenticateUser, kafkaWriteQueueMiddleware(), asyncHandler(dbProxyPost));
app.put('/api/db/:table',           authenticateUser, kafkaWriteQueueMiddleware(), asyncHandler(dbProxyPut));
app.put('/api/db/:table/:id',       authenticateUser, kafkaWriteQueueMiddleware(), asyncHandler(dbProxyPut));
app.delete('/api/db/:table',        authenticateUser, kafkaWriteQueueMiddleware(), asyncHandler(dbProxyDelete));
app.delete('/api/db/:table/:id',    authenticateUser, kafkaWriteQueueMiddleware(), asyncHandler(dbProxyDelete));
console.log('🔒 DB Proxy API available at /api/db/:table');

// Field Mode Toggle API (spec task 8)
import { patchWorkflowFieldMode } from './api/workflow-field-mode';
app.patch('/api/workflows/:id/nodes/:nodeId/field-mode', asyncHandler(patchWorkflowFieldMode));
console.log('🔧 Field Mode Toggle API available at PATCH /api/workflows/:id/nodes/:nodeId/field-mode');

// Workflow Confirmation API
app.post('/api/workflow/confirm', asyncHandler(confirmWorkflow));
app.post('/api/workflow/reject', asyncHandler(rejectWorkflow));
console.log('✅ Workflow Confirmation API available at /api/workflow/confirm and /api/workflow/reject');

// Workflow Credentials API (pending credential store for Continue Workflow flow)
import saveWorkflowCredentials from './api/workflow-credentials';
app.post('/api/workflow/credentials', asyncHandler(saveWorkflowCredentials));
console.log('✅ Workflow Credentials API available at /api/workflow/credentials');

// Tool Substitution API
app.post('/api/workflow/tool-substitute', asyncHandler(substituteTools));
app.get('/api/workflow/tool-substitute/available/:nodeId', asyncHandler(getAvailableSubstitutions));
console.log('🔧 Tool Substitution API available at /api/workflow/tool-substitute');

// Node Contract API (Canonical Node Library)
app.get('/api/node-contract', asyncHandler(nodeContractRoutes.getNodeContract));
app.get('/api/node-contract/version', asyncHandler(nodeContractRoutes.getNodeContractVersion));
app.get('/api/node-contract/patterns', asyncHandler(nodeContractRoutes.getNodeContractPatterns));
app.get('/api/node-contract/:nodeType', asyncHandler(nodeContractRoutes.getNodeContractByType));
console.log('📚 Node Contract API available at /api/node-contract/*');

// Workflow Versioning API
app.post('/api/workflow/version', asyncHandler(workflowVersioningRoutes.versionWorkflow));
app.get('/api/workflow/version/:versionId', asyncHandler(workflowVersioningRoutes.getVersion));
app.get('/api/workflow/version/:versionId/metadata', asyncHandler(workflowVersioningRoutes.getVersionMetadata));
app.post('/api/workflow/version/diff', asyncHandler(workflowVersioningRoutes.diffVersions));
app.get('/api/workflow/versions', asyncHandler(workflowVersioningRoutes.listVersions));

// 🆕 Enhanced Workflow Versioning API
app.get('/api/workflows/:workflowId/versions', asyncHandler(async (req: Request, res: Response) => {
  const { workflowId } = req.params;
  const { limit } = req.query;

  // Workflow CRUD service canary — delegate if enabled for this userId.
  let canaryUserId: string | undefined;
  try {
    const { requireAuthenticatedUser } = await import('./core/utils/check-google-auth');
    canaryUserId = await requireAuthenticatedUser(req);
  } catch { /* no valid JWT — skip canary, fall through */ }

  if (canaryUserId) {
    const { shouldUseWorkflowCrudService, listWorkflowVersionsRemote } = await import('./services/workflow-crud-service-client');
    if (shouldUseWorkflowCrudService(canaryUserId)) {
      try {
        const remote = await listWorkflowVersionsRemote(canaryUserId, workflowId);
        if (remote) {
          console.log(`[ListVersions] ✅ Delegated to workflow-crud-service (workflowId=${workflowId})`);
          return res.json({ workflowId, versions: remote, count: remote.length, source: 'workflow-crud-service' });
        }
        console.warn('[ListVersions] workflow-crud-service returned null — falling back to local path');
      } catch (delegateErr) {
        console.warn('[ListVersions] workflow-crud-service delegation error — falling back:', delegateErr);
      }
    }
  }

  const { getWorkflowVersionManager } = await import('./services/workflow-versioning');
  const versionManager = getWorkflowVersionManager();

  const versions = await versionManager.getVersionHistory(
    workflowId,
    limit ? parseInt(limit as string, 10) : 50
  );

  res.json({ workflowId, versions, count: versions.length });
}));

app.get('/api/workflows/:workflowId/versions/current', asyncHandler(async (req: Request, res: Response) => {
  const { workflowId } = req.params;
  
  const { getWorkflowVersionManager } = await import('./services/workflow-versioning');
  const versionManager = getWorkflowVersionManager();
  
  const version = await versionManager.getCurrentVersion(workflowId);
  
  if (!version) {
    return res.status(404).json({ error: 'No versions found for this workflow' });
  }
  
  res.json({ workflowId, version });
}));

app.get('/api/workflows/:workflowId/versions/:version', asyncHandler(async (req: Request, res: Response) => {
  const { workflowId, version } = req.params;
  
  const { getWorkflowVersionManager } = await import('./services/workflow-versioning');
  const versionManager = getWorkflowVersionManager();
  
  const versionData = await versionManager.getVersion(workflowId, parseInt(version, 10));
  
  if (!versionData) {
    return res.status(404).json({ error: 'Version not found' });
  }
  
  res.json({ workflowId, version: versionData });
}));

app.post('/api/workflows/:workflowId/versions/:version/rollback', asyncHandler(async (req: Request, res: Response) => {
  const { workflowId, version } = req.params;

  // Workflow CRUD service canary — delegate if enabled for this userId.
  let canaryUserId: string | undefined;
  try {
    const { requireAuthenticatedUser } = await import('./core/utils/check-google-auth');
    canaryUserId = await requireAuthenticatedUser(req);
  } catch { /* no valid JWT — skip canary, fall through */ }

  if (canaryUserId) {
    const { shouldUseWorkflowCrudService, rollbackWorkflowRemote, isWorkflowCrudLocalWritesDisabled } = await import('./services/workflow-crud-service-client');
    if (shouldUseWorkflowCrudService(canaryUserId)) {
      try {
        const remote = await rollbackWorkflowRemote(canaryUserId, workflowId, parseInt(version, 10));
        if (remote) {
          console.log(`[RollbackVersion] ✅ Delegated to workflow-crud-service (workflowId=${workflowId} version=${version})`);
          return res.json({ ...remote, source: 'workflow-crud-service' });
        }
        console.warn('[RollbackVersion] workflow-crud-service returned null — falling back to local path');
      } catch (delegateErr) {
        console.warn('[RollbackVersion] workflow-crud-service delegation error — falling back:', delegateErr);
      }
      // LOCAL_WRITES_DISABLED gate
      if (isWorkflowCrudLocalWritesDisabled()) {
        return res.status(503).json({ error: 'Workflow CRUD service unavailable', code: 'WORKFLOW_CRUD_SERVICE_UNAVAILABLE' });
      }
    }
  }

  const { getWorkflowVersionManager } = await import('./services/workflow-versioning');
  const versionManager = getWorkflowVersionManager();

  const result = await versionManager.rollbackToVersion(
    workflowId,
    parseInt(version, 10),
    canaryUserId
  );

  if (!result.success) {
    return res.status(400).json({ error: result.error });
  }

  res.json({ success: true, newVersion: result.newVersion });
}));

app.get('/api/workflows/:workflowId/versions/compare', asyncHandler(async (req: Request, res: Response) => {
  const { workflowId } = req.params;
  const { fromVersion, toVersion } = req.query;
  
  if (!fromVersion || !toVersion) {
    return res.status(400).json({ error: 'fromVersion and toVersion are required' });
  }
  
  const { getWorkflowVersionManager } = await import('./services/workflow-versioning');
  const versionManager = getWorkflowVersionManager();
  
  const comparison = await versionManager.compareVersions(
    workflowId,
    parseInt(fromVersion as string, 10),
    parseInt(toVersion as string, 10)
  );
  
  if (!comparison) {
    return res.status(404).json({ error: 'One or both versions not found' });
  }
  
  res.json({ workflowId, comparison });
}));

app.get('/api/workflows/:workflowId/versions/compatibility', asyncHandler(async (req: Request, res: Response) => {
  const { workflowId } = req.params;
  
  const { getWorkflowVersionManager } = await import('./services/workflow-versioning');
  const versionManager = getWorkflowVersionManager();
  
  const compatibility = await versionManager.checkExecutionCompatibility(workflowId);
  
  res.json({ workflowId, compatibility });
}));

console.log('📦 Workflow Versioning API available at /api/workflow/version/* and /api/workflows/:id/versions/*');

// Memory System API
app.use('/api/memory', memoryRoutes);
app.use('/api/analyze', memoryRoutes); // Also mount analyze endpoints
app.use('/api/execute', memoryRoutes); // Also mount execution endpoints
console.log('🧠 Memory System API available at /api/memory/*, /api/analyze/*, /api/execute/*');

// Chatbot Page Routes
app.get('/workflows/:workflowId/page', asyncHandler(serveChatbotPage));
app.get('/workflows/:workflowId/embed', asyncHandler(serveChatbotPage)); // Embed mode
app.get('/workflows/:workflowId/chat', asyncHandler(serveChatbotChat)); // N8N-style chat UI
app.post('/api/chatbot/:workflowId/message', asyncHandler(handleChatbotMessage));
console.log('💬 Chatbot page routes available at /workflows/:workflowId/page, /workflows/:workflowId/embed, /workflows/:workflowId/chat and /api/chatbot/:workflowId/message');

// AI Gateway - Unified AI Services
app.use(
  '/api/ai',
  asyncHandler(optionalAuth),
  asyncHandler(geminiWalletContextMiddleware),
  distributedRateLimit({
    endpointKey: 'ai-gateway',
    perUserLimit: 120,
    globalLimit: 3000,
    windowMs: 60_000,
  }),
  aiGateway
);
console.log('🤖 AI Gateway available at /api/ai');

// AI Error Guidance endpoint (no auth required — guidance is not sensitive)
app.post('/api/ai/error-guidance', asyncHandler(aiErrorGuidanceHandler));

// Training Statistics API
app.get('/api/training/stats', asyncHandler(trainingStats.getTrainingStats));
app.get('/api/training/categories', asyncHandler(trainingStats.getTrainingCategories));
app.get('/api/training/workflows', asyncHandler(trainingStats.getTrainingWorkflows));
app.post('/api/training/similar', asyncHandler(trainingStats.findSimilarWorkflows));
app.get('/api/training/examples', asyncHandler(trainingStats.getTrainingExamples));
app.get('/api/training/usage', asyncHandler(trainingStats.getTrainingUsage));
app.post('/api/training/reload', asyncHandler(trainingStats.reloadTrainingDataset));
console.log('📚 Training API available at /api/training/*');

// AI Endpoints (Gemini - GEMINI_API_KEY)
app.post('/api/ai/generate', distributedRateLimit({
  endpointKey: 'ai-generate',
  perUserLimit: 25,
  globalLimit: 500,
  windowMs: 60_000,
}), asyncHandler(optionalAuth), asyncHandler(geminiWalletContextMiddleware), asyncHandler(async (req: Request, res: Response) => {
  const { prompt, model, system, temperature, max_tokens } = req.body;
  if (!prompt) return res.status(400).json({ success: false, error: 'Prompt is required' });
  if (!config.geminiApiKey && !(await geminiWalletService.isActive((req as any).user?.id).catch(() => false))) {
    return res.status(503).json({ success: false, error: 'GEMINI_API_KEY not configured' });
  }
  const result = await geminiOrchestrator.processRequest('chat-generation', prompt, {
    model: model || 'gemini-3.5-flash',
    temperature,
    max_tokens,
    cache: false,
  });
  res.json({ success: true, result: typeof result === 'string' ? { content: result } : result });
}));

app.post('/api/ai/chat', distributedRateLimit({
  endpointKey: 'ai-chat',
  perUserLimit: 40,
  globalLimit: 800,
  windowMs: 60_000,
}), asyncHandler(optionalAuth), asyncHandler(geminiWalletContextMiddleware), asyncHandler(async (req: Request, res: Response) => {
  const { messages, model, temperature } = req.body;
  if (!messages || !Array.isArray(messages)) return res.status(400).json({ success: false, error: 'Messages array is required' });
  if (!config.geminiApiKey && !(await geminiWalletService.isActive((req as any).user?.id).catch(() => false))) {
    return res.status(503).json({ success: false, error: 'GEMINI_API_KEY not configured' });
  }
  const response = await aiLlmAdapter.chat('gemini', messages, {
    model: model || 'gemini-3.5-flash',
    temperature: temperature ?? 0.7,
  });
  res.json({ success: true, result: { content: response.content, usage: response.usage } });
}));

app.post('/api/ai/analyze-image', asyncHandler(async (req: Request, res: Response) => {
  res.status(501).json({ success: false, error: 'Image analysis has been removed.' });
}));

app.get('/api/ai/models', asyncHandler(async (req: Request, res: Response) => {
  const models = modelManager.getRecommendedModels().map(name => ({ name }));
  const stats = modelManager.getUsageStats();
  res.json({
    success: true,
    models,
    recommended: modelManager.getRecommendedModels(),
    usageStats: stats,
  });
}));

app.get('/api/ai/metrics', asyncHandler(async (req: Request, res: Response) => {
  const stats = metricsTracker.getStats();
  res.json({ success: true, metrics: stats });
}));

// Error handler (must be last)
app.use(errorHandler);

// === NETWORK INTERFACE DISCOVERY ===
function getNetworkAddresses(port: number): string[] {
  const interfaces = networkInterfaces();
  const addresses: string[] = [];
  
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name] || []) {
      if (iface.family === 'IPv4' && !iface.internal) {
        addresses.push(`http://${iface.address}:${port}`);
      }
    }
  }
  
  return addresses;
}

// === ENHANCED ERROR HANDLING ===
process.on('uncaughtException', (error) => {
  console.error('💥 Uncaught Exception:', error);
  // Don't exit, try to keep server running
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
});

// Initialize Ollama and start server
async function startServer() {
  // Check and apply database migrations on startup
  try {
    const { checkAllMigrations } = await import('./core/database/migration-checker');
    const migrationResults = await checkAllMigrations();
    
    for (const result of migrationResults) {
      if (result.applied) {
        console.log(`✅ [Migration] ${result.message}`);
      } else if (result.error) {
        console.warn(`⚠️  [Migration] ${result.message}: ${result.error}`);
      } else {
        console.log(`ℹ️  [Migration] ${result.message}`);
      }
    }
  } catch (error) {
    console.warn('[Migration] Failed to check migrations:', error);
    // Don't fail startup if migration check fails
  }
  console.log('[ServerStartup] 🔵 Starting server initialization...');
  
  try {
    console.log('[ServerStartup] 🤖 AI: Gemini (GEMINI_API_KEY)');
    await modelManager.initialize();
    if (config.geminiApiKey) {
      console.log('[ServerStartup] ✅ Gemini AI ready. Models:', modelManager.getRecommendedModels().join(', '));
    } else {
      console.log('[ServerStartup] ⚠️  GEMINI_API_KEY not set. AI features unavailable.');
    }
  } catch (error) {
    console.warn('[ServerStartup] ⚠️  Model manager init warning:', error);
  }

  // Start server
  const PORT = config.port;
  console.log(`[ServerStartup] 🔵 About to bind to port ${PORT}...`);

  // Print effective OAuth callback URLs so operators can verify provider app settings.
  try {
    const { logOAuthRedirectUris } = require('./credentials-system/credential-type-registry');
    logOAuthRedirectUris();
  } catch { /* non-fatal */ }

  try {
    console.log(`[ServerStartup] 🔵 Calling app.listen(${PORT}, '0.0.0.0')...`);
    const server = app.listen(PORT, '0.0.0.0', () => {
      console.log(`[ServerStartup] ✅ app.listen() callback fired - server is listening on port ${PORT}`);
      // Initialize WebSocket server for real-time visualization
      try {
        const { getExecutionStateManager } = require('./services/workflow-executor/execution-state-manager');
        const { VisualizationService } = require('./services/workflow-executor/visualization-service');
        
        const stateManager = getExecutionStateManager();
        const visualizationService = new VisualizationService(stateManager);
        visualizationService.initialize(server);

        console.log('📡 WebSocket server initialized for real-time execution visualization');
        console.log(`   WebSocket endpoint: ws://localhost:${PORT}/ws/executions`);

        // Start Redis bridge so execution events reach clients on other replicas
        import('./services/ws-redis-bridge').then(({ initWsRedisBridge }) => {
          initWsRedisBridge(visualizationService).catch((err: any) => {
            console.warn('[WsRedisBridge] Init failed (non-fatal):', err?.message);
          });
        }).catch(() => {});
      } catch (wsError: any) {
        console.warn('⚠️  WebSocket initialization failed:', wsError?.message || wsError);
        console.log('⚠️  Real-time visualization may be unavailable');
        console.log('💡 Make sure "ws" package is installed: npm install ws');
      }

      // Initialize Chat Server for chat trigger
      try {
        const { getChatServer } = require('./services/chat/chat-server');
        const chatServer = getChatServer();
        chatServer.initialize(server);
        
        console.log('💬 Chat WebSocket server initialized');
        console.log(`   Chat WebSocket endpoint: ws://localhost:${PORT}/ws/chat`);
        console.log('   ✅ Manual upgrade handling enabled');
      } catch (chatError: any) {
        console.warn('⚠️  Chat WebSocket initialization failed:', chatError?.message || chatError);
        console.log('⚠️  Chat trigger functionality may be unavailable');
      }
      const networkAddresses = getNetworkAddresses(PORT);
      
      console.log('\n' + '='.repeat(60));
      console.log('🚀 CtrlChecks Worker Backend');
      console.log('='.repeat(60));
      console.log(`✅ Server running on port ${PORT}`);
      console.log(`📡 Environment: ${process.env.NODE_ENV || 'development'}`);
      
      console.log('\n🌐 Available URLs:');
      if (!config.isProduction) {
        console.log(`   Local:    http://localhost:${PORT}`);
        console.log(`   Network:  http://127.0.0.1:${PORT}`);
      } else {
        console.log(`   Server:   ${config.publicBaseUrl || `http://localhost:${PORT}`}`);
      }
      if (networkAddresses.length > 0) {
        networkAddresses.forEach(addr => console.log(`   Network:  ${addr}`));
      }
      
      console.log('\n🛣️  API Endpoints:');
      if (!config.isProduction) {
        console.log(`   Health:   http://localhost:${PORT}/health`);
        console.log(`   Test:     http://localhost:${PORT}/api/test-connection`);
        console.log(`   Execute:  http://localhost:${PORT}/api/execute-workflow`);
        console.log(`   Chatbot:  http://localhost:${PORT}/api/chatbot`);
      }
      
      console.log('\n🔗 CORS Configuration:');
      if (!config.isProduction) {
        console.log(`   Allowed origins: http://localhost:5173, http://localhost:8080, http://127.0.0.1:5173`);
      } else {
        const origins = getAllowedOrigins();
        console.log(`   Allowed origins: ${origins.length > 0 ? origins.join(', ') : 'None configured!'}`);
      }
      if (config.corsOrigin) {
        console.log(`   Custom origin: ${config.corsOrigin}`);
      }
      
      if (config.geminiApiKey) {
        console.log(`\n🤖 AI: Gemini (GEMINI_API_KEY configured)`);
      }
      
      console.log('\n📋 All Available Endpoints:');
      console.log(`  POST /api/execute-workflow`);
      console.log(`  POST /api/distributed-execute-workflow - Distributed workflow engine`);
      console.log(`  GET  /api/execution-status/:executionId - Get execution status`);
      console.log(`  POST /api/webhook-trigger/:workflowId`);
      console.log(`  POST /api/chat-api`);
      console.log(`  GET  /api/form-trigger/:workflowId/:nodeId`);
      console.log(`  POST /api/form-trigger/:workflowId/:nodeId/submit`);
      console.log(`  POST /api/generate-workflow`);
      console.log(`  POST /api/execute-agent`);
      console.log(`  POST /api/chatbot`);
      console.log(`  POST /chatbot`);
      console.log(`  POST /api/analyze-workflow-requirements`);
      console.log(`  POST /process - Proxy to FastAPI backend`);
      console.log(`  POST /execute-node - Debug single node execution`);
      console.log(`  POST /api/execute-node - Debug single node execution`);
      console.log(`  GET  /api/admin-templates`);
      console.log(`  POST /api/copy-template`);
      console.log(`\n📦 Subscription Management API:`);
      console.log(`  GET  /api/workflows/limit-check - Check workflow creation limits`);
      console.log(`  GET  /api/subscriptions/plans - Get available subscription plans`);
      console.log(`  GET  /api/subscriptions/current - Get current user subscription`);
      console.log(`  POST /api/subscriptions/cancel - Cancel subscription`);
      console.log(`  GET  /api/subscriptions/history - Get subscription history`);
      console.log(`  POST /api/payments/razorpay/create-order - Create payment order`);
      console.log(`  POST /api/payments/razorpay/verify - Verify payment`);
      console.log(`  💰 Development pricing: ${config.developmentPricing ? '₹1 for all plans' : 'Production pricing'}`);
      console.log(`\n🔐 Authentication Management API:`);
      console.log(`  POST /api/auth/refresh-token - Refresh JWT token`);
      console.log(`  GET  /api/auth/session - Get current session info`);
      console.log(`  POST /api/auth/logout - Logout current session`);
      console.log(`  POST /api/auth/logout-all - Logout all sessions`);
      console.log(`  GET  /api/auth/validate - Validate current token`);
      console.log(`\n🛡️  Admin Security API:`);
      console.log(`  GET  /api/admin/audit-trail - Get audit trail (admin only)`);
      console.log(`  GET  /api/admin/security-events - Get security events (admin only)`);
      console.log(`  POST /api/ai/generate - Text generation`);
      console.log(`  POST /api/ai/chat - Chat completion`);
      console.log(`  POST /api/ai/analyze-image - Image analysis`);
      console.log(`  GET  /api/ai/models - List available models`);
      console.log(`  GET  /api/ai/metrics - AI performance metrics`);
      console.log(`\n📚 Training API Endpoints:`);
      console.log(`  GET  /api/training/stats - Training dataset statistics`);
      console.log(`  GET  /api/training/categories - Available workflow categories`);
      console.log(`  GET  /api/training/workflows - Get workflows by category`);
      console.log(`  POST /api/training/similar - Find similar workflows`);
      console.log(`  GET  /api/training/examples - Get training examples for few-shot learning`);
      console.log(`  GET  /api/training/usage - Get training usage metrics`);
      console.log(`  POST /api/training/reload - Reload training dataset (hot reload)`);
      console.log(`\n🤖 AI Gateway Endpoints:`);
      console.log(`  POST /api/ai/chatbot/message - Chichu chatbot`);
      console.log(`  POST /api/ai/editor/suggest-improvements - Workflow node suggestions`);
      console.log(`  POST /api/ai/builder/generate-from-prompt - Generate workflow from prompt`);
      console.log(`  POST /api/ai/ollama/generate - Direct Gemini generation (legacy path)`);
      console.log(`  GET  /api/ai/metrics - Performance metrics`);
      
      console.log('\n' + '='.repeat(60));
      console.log('✅ Backend ready to accept connections!');
      console.log('='.repeat(60) + '\n');
      
      // Start scheduler service
      if (process.env.ENABLE_SCHEDULER !== 'false') {
        console.log('[ServerStartup] 🔵 Starting scheduler services...');
        const hasRedis = !!process.env.REDIS_URL;
        (async () => {
          let dbReachable = false;
          try {
            const { isDatabaseReachable } = await import('./core/database/db-pool');
            dbReachable = await isDatabaseReachable();
          } catch (err) {
            console.warn('[ServerStartup] ⚠️  DB preflight check failed:', err);
          }

          if (!dbReachable) {
            console.warn('[ServerStartup] ⏭️  Skipping scheduler + watchdog startup because DATABASE_URL is unreachable');
            return;
          }

          // ✅ CRITICAL: Start timeout watchdog for stuck runs
          import('./services/execution/timeout-watchdog').then(({ startTimeoutWatchdog }) => {
            console.log('[ServerStartup] 🔵 Timeout watchdog module loaded, starting...');
            startTimeoutWatchdog(5 * 60 * 1000); // Check every 5 minutes
            console.log('[ServerStartup] ✅ Timeout watchdog started (checks every 5 minutes)');
          }).catch(err => {
            console.error('[ServerStartup] ⚠️  Failed to start timeout watchdog:', err);
            console.error('[ServerStartup] ⚠️  Error details:', err?.stack || err);
          });

          import('./services/scheduler').then(({ schedulerService }) => {
            console.log('[ServerStartup] 🔵 Scheduler module loaded, starting...');
            schedulerService.start().catch((err: any) => {
              console.error('[ServerStartup] ⚠️  Scheduler start failed:', err);
            });
          }).catch(err => {
            console.error('[ServerStartup] ⚠️  Failed to load scheduler module:', err);
            console.error('[ServerStartup] ⚠️  Error details:', err?.stack || err);
          });

          // Start session cleanup service (requires Redis session repository)
          if (hasRedis) {
            import('./services/session-cleanup').then(({ sessionCleanupService }) => {
              console.log('[ServerStartup] 🔵 Session cleanup service loaded, starting...');
              sessionCleanupService.start();
              console.log('[ServerStartup] ✅ Session cleanup service started');
            }).catch(err => {
              console.error('[ServerStartup] ⚠️  Failed to start session cleanup service:', err);
            });
          } else {
            console.log('[ServerStartup] ⏭️  Session cleanup disabled (REDIS_URL not set)');
          }

          import('./services/ai-sre-orchestrator').then(({ aiSreOrchestrator }) => {
            console.log('[ServerStartup] 🔵 Autonomous SRE orchestrator module loaded, starting...');
            aiSreOrchestrator.start();
            console.log('[ServerStartup] ✅ Autonomous SRE orchestrator started');
          }).catch(err => {
            console.error('[ServerStartup] ⚠️  Failed to start autonomous SRE orchestrator:', err);
          });
        })().catch((err) => {
          console.error('[ServerStartup] ⚠️  Scheduler bootstrap preflight failed:', err);
        });
      } else {
        console.log('[ServerStartup] ⏭️  Scheduler services disabled (ENABLE_SCHEDULER=false)');
      }
    });
    
    // Handle server errors
    server.on('error', (error: any) => {
      console.error('[ServerStartup] ❌ Server error event fired:', error);
      if (error.code === 'EADDRINUSE') {
        console.error(`[ServerStartup] ❌ Port ${PORT} is already in use.`);
        console.error('[ServerStartup]    Try one of these solutions:');
        console.error(`[ServerStartup]    Windows: netstat -ano | findstr :${PORT}`);
        console.error(`[ServerStartup]    Then: taskkill /PID <PID> /F`);
        console.error(`[ServerStartup]    Or change PORT in .env file`);
      } else {
        console.error('[ServerStartup] ❌ Server error:', error);
        console.error('[ServerStartup] ❌ Error code:', error.code);
        console.error('[ServerStartup] ❌ Error message:', error.message);
        console.error('[ServerStartup] ❌ Error stack:', error.stack);
      }
      process.exit(1);
    });
    
    console.log(`[ServerStartup] ✅ Server object created, listening should start soon...`);
    
  } catch (error) {
    console.error('[ServerStartup] 💥 Failed to start server (caught in try block):', error);
    console.error('[ServerStartup] 💥 Error details:', error instanceof Error ? error.stack : String(error));
    process.exit(1);
  }
}

// Start the server
console.log('[ServerStartup] 🔵 Calling startServer()...');
startServer().catch((error) => {
  console.error('[ServerStartup] ❌ Failed to start server (caught in catch):', error);
  console.error('[ServerStartup] ❌ Error details:', error instanceof Error ? error.stack : String(error));
  process.exit(1);
});

export default app;
