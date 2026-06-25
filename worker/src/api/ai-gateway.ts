// AI Gateway - Unified API for AI services (Gemini via GEMINI_API_KEY)

import { Router, Request, Response } from 'express';
import { geminiOrchestrator } from '../services/ai/gemini-orchestrator';
import { chichuChatbot } from '../services/ai/chichu-chatbot';
import { aiWorkflowEditor } from '../services/ai/workflow-editor';
import { aiPerformanceMonitor } from '../services/ai/performance-monitor';
import { config } from '../core/config';
import { LLMAdapter } from '../shared/llm-adapter';
import { unifiedGraphOrchestrator } from '../core/orchestration/unified-graph-orchestrator';
import { unifiedNodeRegistry } from '../core/registry/unified-node-registry';
import type { AiEditorRequest, AiEditorResponse } from '../core/types/ai-editor-contracts';
import type { Workflow } from '../core/types/ai-types';
import { WorkflowVersioning } from '../services/ai/workflow-versioning';
import { getDbClient } from '../core/database/aws-db-client';
import {
  buildFieldOwnershipGuidancePrompt,
  buildDeterministicFieldOwnershipGuidance,
  fallbackFieldOwnershipGuidance,
  type FieldOwnershipGuidanceSections,
} from '../services/ai/field-ownership-guidance-prompt';
import {
  buildFieldGuidanceDescription,
  mergeGuidanceWithDeterministic,
  type FieldGuidanceDescription,
} from '../core/utils/node-field-intelligence';
import { runWithBuildUsageTracking, snapshotBuildAiUsage } from '../core/ai/build-usage-context';
import {
  resolveAiEditorPrincipal,
  requireCapability,
  fetchWorkflowLifecyclePhase,
  canApplyForPhase,
} from '../services/ai/ai-editor-rbac';
import { logAiEditorEvent, hashDiff, readAiEditorAuditForWorkflow } from '../services/ai/ai-editor-audit';
import { GeminiWalletError, geminiWalletService } from '../services/ai/gemini-wallet-service';
import { GEMINI_MODELS } from '../services/ai/gemini-models';
import { logger } from '../core/logger';

const router = Router();
const llmAdapter = new LLMAdapter();
const aiEditorVersioning = new WorkflowVersioning();
const guideResponseCache = new Map<string, { expiresAt: number; guidance: FieldOwnershipGuidanceSections }>();
const GUIDE_CACHE_TTL_MS = 10 * 60 * 1000;
const FIELD_GUIDANCE_PRESENTATION_VERSION = 2;

function buildFieldWalkContextKey(field: Record<string, any>): string {
  return JSON.stringify({
    guidancePresentationVersion: FIELD_GUIDANCE_PRESENTATION_VERSION,
    operation: field.operation || '',
    selectedMode: field.selectedMode || '',
    fieldEnabled: field.fieldEnabled === true,
    fillModeDefault: field.fillModeDefault || 'manual_static',
    currentValue: field.currentValue ?? null,
    options: normalizeFieldOptions(field.options),
  });
}

async function hasGeminiAccess(req: Request): Promise<boolean> {
  if (config.geminiApiKey) return true;
  const userId = (req as any).user?.id;
  return geminiWalletService.isActive(userId).catch(() => false);
}

function enrichFieldForGuidance(nodeType: string, rawField: any): any {
  const def = unifiedNodeRegistry.get(nodeType);
  const registryField = def?.inputSchema?.[String(rawField?.fieldName || '')];
  const options = normalizeFieldOptions(
    rawField?.options ||
      rawField?.ui?.options ||
      registryField?.ui?.options ||
      (registryField as any)?.options
  );
  return {
    ...(registryField || {}),
    ...(rawField || {}),
    description: rawField?.description || registryField?.description || '',
    fieldType: rawField?.fieldType || registryField?.type || 'string',
    supportsRuntimeAI: rawField?.supportsRuntimeAI ?? registryField?.fillMode?.supportsRuntimeAI ?? true,
    supportsBuildtimeAI: rawField?.supportsBuildtimeAI ?? registryField?.fillMode?.supportsBuildtimeAI ?? true,
    fillModeDefault: rawField?.fillModeDefault || registryField?.fillMode?.default || 'manual_static',
    ownership: rawField?.ownership || registryField?.ownership || '',
    fieldIntelligence: registryField?.fieldIntelligence || rawField?.fieldIntelligence,
    fieldRelevance: rawField?.fieldRelevance,
    options,
    ui: {
      ...(registryField?.ui || {}),
      ...(rawField?.ui || {}),
      ...(options.length ? { options } : {}),
    },
  };
}

function normalizeFieldOptions(options: unknown): Array<{ label: string; value: string }> {
  if (!Array.isArray(options)) return [];
  return options
    .map((option: any) => {
      if (typeof option === 'string') return { label: option, value: option };
      if (!option || typeof option !== 'object') return null;
      const value = String(option.value ?? option.id ?? option.key ?? option.label ?? '').trim();
      if (!value) return null;
      return { label: String(option.label ?? value), value };
    })
    .filter(Boolean) as Array<{ label: string; value: string }>;
}

function buildDeterministicFieldDescriptions(args: {
  nodeType: string;
  nodeLabel: string;
  workflowOverview?: string;
  operation?: string;
  fields: any[];
}): Record<string, FieldGuidanceDescription> {
  const descriptions: Record<string, FieldGuidanceDescription> = {};
  for (const rawField of args.fields) {
    const fieldName = String(rawField?.fieldName || '');
    if (!fieldName) continue;
    const enriched = enrichFieldForGuidance(args.nodeType, rawField);
    descriptions[fieldName] = buildFieldGuidanceDescription({
      nodeType: args.nodeType,
      nodeLabel: args.nodeLabel,
      fieldName,
      field: {
        ...enriched,
        label: rawField?.label || fieldName,
        selectedMode: rawField?.selectedMode,
        fieldEnabled: rawField?.fieldEnabled,
        fieldRelevance: rawField?.fieldRelevance,
      },
      workflowGoal: args.workflowOverview,
      operation: args.operation,
      fieldRelevance: rawField?.fieldRelevance,
    });
  }
  return descriptions;
}

function sendAiGatewayError(res: Response, error: unknown) {
  if (error instanceof GeminiWalletError) {
    const status =
      error.code === 'GEMINI_WALLET_LIMIT_EXCEEDED'
        ? 402
        : error.code === 'GEMINI_WALLET_PROVIDER_ERROR'
          ? 503
          : 400;
    return res.status(status).json({
      success: false,
      error: error.message,
      code: error.code,
      walletStatus: error.walletStatus,
      actions: ['replace_gemini_key', 'deactivate_wallet'],
    });
  }
  return res.status(500).json({ success: false, error: error instanceof Error ? error.message : 'Unknown error' });
}

let initialized = false;

async function initializeAIServices() {
  if (initialized) return;
  if (!config.geminiApiKey) {
    logger.warn('⚠️  GEMINI_API_KEY not set. AI features may be unavailable.');
    initialized = true;
    return;
  }
  try {
    logger.info('🤖 Initializing AI Gateway (Gemini)...');
    initialized = true;
    logger.info('✅ AI Gateway initialized');
  } catch (error) {
    logger.error('⚠️  AI Gateway initialization failed:', error);
    initialized = true;
  }
}

initializeAIServices().catch(console.error);

// ==================== CHICHU CHATBOT ====================
router.post('/chatbot/message', async (req: Request, res: Response) => {
  try {
    const { sessionId, message, context } = req.body;
    if (!message || typeof message !== 'string') {
      return res.status(400).json({ error: 'Message is required' });
    }
    const session = sessionId || `session-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const response = await chichuChatbot.handleMessage(session, message, context);
    res.json({ success: true, ...response, sessionId: session });
  } catch (error) {
    logger.error('Chatbot error:', error);
    sendAiGatewayError(res, error);
  }
});

router.get('/chatbot/session/:sessionId/history', async (req: Request, res: Response) => {
  try {
    const history = chichuChatbot.getConversationHistory(req.params.sessionId);
    res.json({ success: true, history });
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : 'Unknown error' });
  }
});

router.delete('/chatbot/session/:sessionId', async (req: Request, res: Response) => {
  try {
    chichuChatbot.clearConversation(req.params.sessionId);
    res.json({ success: true, message: 'Conversation cleared' });
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : 'Unknown error' });
  }
});

router.post('/field-ownership-guide', async (req: Request, res: Response) => {
  try {
    const { question, context } = req.body as {
      question?: string;
      context?: Record<string, unknown>;
    };
    if (!question || typeof question !== 'string' || !question.trim()) {
      return res.status(400).json({ success: false, error: 'question is required' });
    }
    const deterministicGuidance = buildDeterministicFieldOwnershipGuidance(question.trim(), context || {});

    const authHeader = req.headers.authorization;
    const db = getDbClient();
    let userId: string | null = null;
    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.replace('Bearer ', '').trim();
      if (token) {
        const { data: authData, error: authError } = await db.auth.getUser(token);
        if (!authError && authData?.user?.id) userId = authData.user.id;
      }
    }

    const workflowId = typeof context?.workflowId === 'string' ? context.workflowId.trim() : '';
    if (workflowId && userId) {
      const { data: wf } = await db
        .from('workflows')
        .select('id,user_id')
        .eq('id', workflowId)
        .single();
      if (wf && wf.user_id && String(wf.user_id) !== String(userId)) {
        return res.status(403).json({ success: false, error: 'Forbidden workflow access' });
      }
    }

    if (!(await hasGeminiAccess(req))) {
      return res.status(200).json({ success: true, guidance: deterministicGuidance });
    }

    const prompt = buildFieldOwnershipGuidancePrompt({
      question: question.trim(),
      context: context || {},
      deterministicGuidance,
    });
    const cacheKey = JSON.stringify({
      question: question.trim(),
      workflowId,
      selectedField: (context || {}).selectedField,
    });
    const cached = guideResponseCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) {
      return res.json({ success: true, guidance: cached.guidance, cached: true });
    }
    const raw = await geminiOrchestrator.processRequest('chat-generation', prompt, {
      model: 'gemini-3.5-flash',
      temperature: 0.1,
    });
    const text = typeof raw === 'string' ? raw : (raw as any)?.content || '';

    let parsed: FieldOwnershipGuidanceSections | null = null;
    try {
      const jsonStart = text.indexOf('{');
      const jsonEnd = text.lastIndexOf('}');
      const candidate = jsonStart >= 0 && jsonEnd > jsonStart ? text.slice(jsonStart, jsonEnd + 1) : text;
      const obj = JSON.parse(candidate);
      parsed = {
        whatThisFieldDoes: String(obj.whatThisFieldDoes || ''),
        ifYouChooseYou: String(obj.ifYouChooseYou || ''),
        ifYouChooseAIBuild: String(obj.ifYouChooseAIBuild || ''),
        ifYouChooseAIRuntime: String(obj.ifYouChooseAIRuntime || ''),
        isActuallyRequired: String(obj.isActuallyRequired || ''),
        whereToGetValue: String(obj.whereToGetValue || ''),
        nextStepExpectations: String(obj.nextStepExpectations || ''),
      };
    } catch {
      parsed = null;
    }

    const guidance = parsed && Object.values(parsed).every((v) => v.trim().length > 0)
      ? {
          ...parsed,
          whatThisFieldDoes: deterministicGuidance.whatThisFieldDoes || parsed.whatThisFieldDoes,
          isActuallyRequired: deterministicGuidance.isActuallyRequired,
        }
      : deterministicGuidance;
    guideResponseCache.set(cacheKey, { expiresAt: Date.now() + GUIDE_CACHE_TTL_MS, guidance });

    res.json({ success: true, guidance });
  } catch (error) {
    logger.error('Field ownership guide error:', error);
    res.status(200).json({ success: true, guidance: fallbackFieldOwnershipGuidance() });
  }
});

// ==================== NODE DESCRIPTION ====================
router.post('/node-description', async (req: Request, res: Response) => {
  try {
    const { nodeType, nodeLabel, nodeNarrative, workflowOverview, userPrompt } = req.body as {
      nodeType?: string;
      nodeLabel?: string;
      nodeNarrative?: string;
      workflowOverview?: string;
      userPrompt?: string;
    };

    if (!nodeType || !nodeLabel) {
      return res.status(400).json({ success: false, error: 'nodeType and nodeLabel are required' });
    }

    const fallback =
      nodeNarrative ||
      `This ${nodeLabel} node performs its function as part of the automated workflow.`;

    if (!(await hasGeminiAccess(req))) {
      return res.status(200).json({ success: true, description: fallback });
    }

    const contextLines = [
      workflowOverview ? `Workflow goal: ${workflowOverview}` : null,
      userPrompt ? `User's original request: ${userPrompt}` : null,
      nodeNarrative ? `Role in this workflow: ${nodeNarrative}` : null,
    ]
      .filter(Boolean)
      .join('\n');

    const aiPrompt = `You explain workflow automation nodes to non-technical users (HR managers, students, business owners).

${contextLines}
Node: "${nodeLabel}" (technical type: ${nodeType})

Write exactly 2–3 sentences in plain, friendly English:
1. What this node does in this specific workflow — use the workflow context above, not a generic definition.
2. One sentence covering what the three ownership choices mean in plain English:
   - "You" = you manually fill in the values for this node's settings before the workflow runs
   - "AI build" = the AI fills in the values once when the workflow is being set up
   - "AI runtime" = the AI figures out the values fresh every single time the workflow runs

No bullet points. No headings. No technical jargon. Under 80 words total. Write as if explaining to a 10th-grade student.`;

    const raw = await geminiOrchestrator.processRequest(
      'chat-generation',
      { system: '', message: aiPrompt },
      { model: 'gemini-3.5-flash', temperature: 0.2, cache: false }
    );

    const text = (typeof raw === 'string' ? raw : (raw as any)?.content || '').trim();
    const description = text.length > 20 ? text : fallback;

    res.json({ success: true, description });
  } catch (error) {
    logger.error('Node description error:', error);
    res.status(200).json({
      success: true,
      description: `This ${(req.body as any)?.nodeLabel || 'node'} performs its role in the workflow automatically.`,
    });
  }
});

// ==================== FIELD DESCRIPTIONS ====================
router.post('/field-descriptions', async (req: Request, res: Response) => {
  try {
    const { nodeType, nodeLabel, nodeNarrative, workflowOverview, userPrompt, fields } = req.body as {
      nodeType?: string;
      nodeLabel?: string;
      nodeNarrative?: string;
      workflowOverview?: string;
      userPrompt?: string;
      fields?: Array<{
        fieldName: string;
        label: string;
        fieldType: string;
        description?: string;
        example?: string;
        options?: Array<{ label?: string; value?: string } | string>;
        required?: boolean;
        selectedMode?: string;
        fieldEnabled?: boolean;
        supportsRuntimeAI: boolean;
        supportsBuildtimeAI: boolean;
        fillModeDefault: string;
        ownership?: string;
        fieldRelevance?: unknown;
        currentValue?: unknown;
        operation?: string;
      }>;
    };

    if (!nodeType || !nodeLabel || !fields || fields.length === 0) {
      return res.status(400).json({ success: false, error: 'nodeType, nodeLabel, and fields are required' });
    }

    const enrichedFields = fields.map((f) => enrichFieldForGuidance(nodeType, f));
    const deterministicDescriptions = buildDeterministicFieldDescriptions({
      nodeType,
      nodeLabel,
      workflowOverview,
      operation: typeof (fields[0] as any)?.operation === 'string' ? String((fields[0] as any).operation) : undefined,
      fields: enrichedFields,
    });

    if (!(await hasGeminiAccess(req))) {
      return res.status(200).json({ success: true, descriptions: deterministicDescriptions, deterministic: true });
    }

    const contextLines = [
      workflowOverview ? `Workflow goal: ${workflowOverview}` : null,
      userPrompt ? `User's original request: ${userPrompt}` : null,
      nodeNarrative ? `This node's role: ${nodeNarrative}` : null,
    ]
      .filter(Boolean)
      .join('\n');

    const fieldsText = enrichedFields
      .map(
        (f) =>
          `- fieldName: "${f.fieldName}", label: "${f.label}", type: ${f.fieldType}, docs: "${f.description || ''}", example: "${f.example || ''}", options: ${JSON.stringify(f.options || [])}, required: ${f.required !== false}, enabledNow: ${f.fieldEnabled === true}, currentOwner: "${f.selectedMode || 'manual_static'}", supportsAIBuild: ${f.supportsBuildtimeAI}, supportsAIRun: ${f.supportsRuntimeAI}, intelligence: ${JSON.stringify(f.fieldIntelligence || {})}, relevance: ${JSON.stringify(f.fieldRelevance || {})}`
      )
      .join('\n');

    const aiPrompt = `You explain workflow automation field settings to non-technical users (business owners, students, HR managers).

${contextLines}
Node: "${nodeLabel}" (type: ${nodeType})

Important: do NOT summarize the whole node. Each answer must explain only the exact input field named by fieldName, using the workflow goal and user's request. Keep it simple enough for a beginner.

Authoritative registry guidance:
${JSON.stringify(deterministicDescriptions, null, 2)}

You may make the wording friendlier, but do not contradict the registry guidance. In particular, do not say a field can be left empty when the registry marks it dangerousIfEmpty or provides an empty/zero warning.

For each field below, generate a JSON object. Each field needs:
- "what": 1 plain sentence explaining what this exact input field controls in this workflow use case. Use the docs text only to stay accurate.
- "setupSummary": 2-4 short sentences that combine what this field does, whether it matters, empty/off behavior once, recommended owner, and example availability. Do not use separate "If off" and "If empty" wording. Keep it field-specific.
- "needed": A direct action instruction — tell the user WHAT TO DO. If the field is needed for the workflow goal: say exactly what to toggle on and enter (e.g. 'Toggle this on and enter your Sheet ID like 1BxiMVz...'). If not needed: say what to leave alone and why (e.g. 'Leave this off — it is only used when writing data, not reading'). Max 2 short sentences.
- "dataImpact": 1 plain sentence saying how enabling this field changes the data or result in later steps.
- "you": If the user picks YOU — what exactly must they type or paste, where do they get it, and when should they use this mode? Include a realistic example like 'e.g. "..."'. Specific to this workflow's goal.
- "aiBuild": If AI BUILD owns this — what exactly will AI do once during workflow setup to determine this value? Name what it will infer or look up. If supportsAIBuild is false, write 'Not available for this field.'
- "aiRun": If AI RUN owns this — what exactly will AI determine at runtime, and from what live data or context? If supportsAIRun is false, write 'Not available for this field.'
- "example": A realistic example value for this field in this workflow, starting with 'e.g. '.

Fields:
${fieldsText}

Expanded guidance keys to include for each field:
- "setupSummary": one concise paragraph for the UI summary block. It must not repeat the separate structured fields or contain vague phrases like "configured default behavior".
- "offBehavior": what happens if the field toggle is off.
- "emptyBehavior": what happens if the field is enabled but empty.
- "defaultBehaviorLabel": short concrete fallback label, such as "Optional setting skipped", "Required value", or "Default: json".
- "recommendedOwner": one of "You", "AI Build", or "AI Runtime".
- "ownerReason": why that owner is recommended.
- "validationConfidence": one of "high", "medium", or "low".
- "warnings": array of short warning strings.
- "safeValueSuggestion": string value if there is a safe suggested value, otherwise empty string.
- "actionableExample": object with "value", "displayValue", "canApply", "applyMode", "reason", and "source". Only set canApply true for safe non-secret setup values. Never invent API keys, passwords, OAuth tokens, or credentials.

Select/model rules:
- If a field has options, examples and actionableExample.value must use one of the listed option "value" strings exactly.
- For model fields, do not mention removed providers or unrelated API model IDs. Explain only the models/options attached to this current node field.
- If no listed option is appropriate, set actionableExample.canApply to false and explain that the user must choose from the dropdown.

Respond with ONLY valid JSON (no markdown, no code fences):
{
  "fieldName1": { "what": "...", "setupSummary": "...", "needed": "...", "dataImpact": "...", "you": "...", "aiBuild": "...", "aiRun": "...", "example": "...", "actionableExample": { "value": "...", "displayValue": "...", "canApply": true, "applyMode": "buildtime_ai_once", "reason": "...", "source": "ai_field_guidance" }, "offBehavior": "...", "emptyBehavior": "...", "defaultBehaviorLabel": "...", "recommendedOwner": "You", "ownerReason": "...", "validationConfidence": "high", "warnings": [], "safeValueSuggestion": "" },
  "fieldName2": { ... }
}

Rules: No technical jargon. Under 35 words per key (needed may use up to 2 sentences). Do not repeat the field label at the start. Use "you" to address the user directly. Never write "configured default behavior" or vague "default behavior" unless you name the actual default.`;

    const { raw, buildAiUsage } = await runWithBuildUsageTracking(async () => {
      const raw = await geminiOrchestrator.processRequest(
        'chat-generation',
        { system: '', message: aiPrompt },
        { model: 'gemini-3.5-flash', temperature: 0.2, cache: false, usageStage: 'field_guidance' }
      );
      return { raw, buildAiUsage: snapshotBuildAiUsage() };
    });

    const text = (typeof raw === 'string' ? raw : (raw as any)?.content || '').trim();

    let descriptions: Record<string, {
      what: string;
      setupSummary?: string;
      needed?: string;
      dataImpact?: string;
      you: string;
      aiBuild: string;
      aiRun: string;
      example: string;
    }> = {};
    try {
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        descriptions = JSON.parse(jsonMatch[0]);
      }
    } catch {
      // fall through to empty descriptions
    }

    const merged: Record<string, FieldGuidanceDescription> = {};
    for (const [fieldName, deterministic] of Object.entries(deterministicDescriptions)) {
      merged[fieldName] = mergeGuidanceWithDeterministic(deterministic, descriptions[fieldName] as any);
    }

    res.json({ success: true, descriptions: merged, buildAiUsage });
  } catch (error) {
    logger.error('Field descriptions error:', error);
    res.status(200).json({ success: true, descriptions: {} });
  }
});

// ==================== FIELD WALK-THROUGH (single-field with DB cache) ====================
router.post('/field-walk-step', async (req: Request, res: Response) => {
  try {
    const {
      workflowId,
      nodeId,
      nodeType,
      nodeLabel,
      nodeNarrative,
      workflowOverview,
      userPrompt,
      field,
    } = req.body as {
      workflowId?: string;
      nodeId?: string;
      nodeType?: string;
      nodeLabel?: string;
      nodeNarrative?: string;
      workflowOverview?: string;
      userPrompt?: string;
      field?: {
        fieldName: string;
        label: string;
        fieldType: string;
        description?: string;
        example?: string;
        options?: Array<{ label?: string; value?: string } | string>;
        required?: boolean;
        selectedMode?: string;
        fieldEnabled?: boolean;
        supportsRuntimeAI: boolean;
        supportsBuildtimeAI: boolean;
        fillModeDefault: string;
        ownership?: string;
        fieldRelevance?: unknown;
        currentValue?: unknown;
        operation?: string;
      };
    };

    if (!nodeType || !nodeLabel || !field?.fieldName) {
      return res.status(400).json({ success: false, error: 'nodeType, nodeLabel, and field.fieldName are required' });
    }

    const enrichedField = enrichFieldForGuidance(nodeType, field);
    const fieldWalkContextKey = buildFieldWalkContextKey(field as any);
    const deterministicDescription = buildFieldGuidanceDescription({
      nodeType,
      nodeLabel,
      fieldName: field.fieldName,
      field: {
        ...enrichedField,
        label: field.label || field.fieldName,
        selectedMode: field.selectedMode,
        fieldEnabled: field.fieldEnabled,
        fieldRelevance: (field as any).fieldRelevance,
      },
      workflowGoal: workflowOverview,
      operation: field.operation,
      fieldRelevance: (field as any).fieldRelevance as any,
    });

    // -- Check DB cache first --
    if (workflowId && nodeId) {
      try {
        const db = getDbClient();
        const { data: cached } = await db
          .from('field_walk_cache')
          .select('description, expires_at')
          .eq('workflow_id', workflowId)
          .eq('node_id', nodeId)
          .eq('field_name', field.fieldName)
          .gt('expires_at', new Date().toISOString())
          .maybeSingle();

        if (cached?.description) {
          const cachedDescription = cached.description as any;
          if (cachedDescription?._contextKey === fieldWalkContextKey) {
            return res.json({
              success: true,
              description: mergeGuidanceWithDeterministic(deterministicDescription, cachedDescription),
              fromCache: true,
            });
          }
        }
      } catch {
        // DB cache miss or error — fall through to Gemini
      }
    }

    if (!(await hasGeminiAccess(req))) {
      return res.json({ success: true, description: deterministicDescription, deterministic: true });
    }

    const contextLines = [
      workflowOverview ? `Workflow goal: ${workflowOverview}` : null,
      userPrompt ? `User's original request: ${userPrompt}` : null,
      nodeNarrative ? `This node's role: ${nodeNarrative}` : null,
    ]
      .filter(Boolean)
      .join('\n');

    const f = enrichedField;
    const fieldLine = `- fieldName: "${f.fieldName}", label: "${f.label}", type: ${f.fieldType}, docs: "${f.description || ''}", example: "${f.example || ''}", options: ${JSON.stringify(f.options || [])}, required: ${f.required !== false}, enabledNow: ${f.fieldEnabled === true}, currentOwner: "${f.selectedMode || 'manual_static'}", fillModeDefault: "${f.fillModeDefault || 'manual_static'}", currentValue: ${JSON.stringify((field as any).currentValue ?? null)}, operation: "${field.operation || ''}", supportsAIBuild: ${f.supportsBuildtimeAI}, supportsAIRun: ${f.supportsRuntimeAI}, intelligence: ${JSON.stringify(f.fieldIntelligence || {})}, relevance: ${JSON.stringify(f.fieldRelevance || {})}`;

    const aiPrompt = `You explain workflow automation field settings to non-technical users (business owners, students, HR managers).

${contextLines}
Node: "${nodeLabel}" (type: ${nodeType})

Important: do NOT summarize the whole node. Your answer must explain only the exact input field named by fieldName, using the workflow goal and user's request. Keep it simple enough for a beginner.

Authoritative registry guidance:
${JSON.stringify(deterministicDescription, null, 2)}

You may make the wording friendlier, but do not contradict the registry guidance. In particular, do not say the field can be left empty when the registry marks it dangerousIfEmpty or provides an empty/zero warning.

For the field below, generate a JSON object with keys:
- "what": 1 plain sentence explaining what this exact input field controls in this workflow use case.
- "setupSummary": 2-4 short sentences that combine what this field does, whether it matters, empty/off behavior once, recommended owner, and example availability. Do not use separate "If off" and "If empty" wording. Keep it field-specific.
- "needed": A direct action instruction — tell the user WHAT TO DO. If the field is needed for the workflow goal: say exactly what to toggle on and enter (e.g. 'Toggle this on and enter your Sheet ID like 1BxiMVz...'). If not needed: say what to leave alone and why (e.g. 'Leave this off — it is only used when writing data, not reading'). Max 2 short sentences.
- "dataImpact": 1 plain sentence saying how enabling this field changes the data or result in later steps.
- "you": If the user picks YOU — what exactly must they type or paste, where do they get it, and when should they use this mode? Include a realistic example like 'e.g. "..."'. Specific to this workflow's goal.
- "aiBuild": If AI BUILD owns this — what exactly will AI do once during workflow setup to determine this value? Name what it will infer or look up. If supportsAIBuild is false, write 'Not available for this field.'
- "aiRun": If AI RUN owns this — what exactly will AI determine at runtime, and from what live data or context? If supportsAIRun is false, write 'Not available for this field.'
- "example": A realistic example value for this field in this workflow, starting with 'e.g. '.

Field:
${fieldLine}

Expanded guidance keys to include:
- "setupSummary": one concise paragraph for the UI summary block. It must not repeat the separate structured fields or contain vague phrases like "configured default behavior".
- "offBehavior": what happens if the field toggle is off.
- "emptyBehavior": what happens if the field is enabled but empty.
- "defaultBehaviorLabel": short concrete fallback label, such as "Optional setting skipped", "Required value", or "Default: json".
- "recommendedOwner": one of "You", "AI Build", or "AI Runtime".
- "ownerReason": why that owner is recommended.
- "validationConfidence": one of "high", "medium", or "low".
- "warnings": array of short warning strings.
- "safeValueSuggestion": string value if there is a safe suggested value, otherwise empty string.
- "actionableExample": object with "value", "displayValue", "canApply", "applyMode", "reason", and "source". Only set canApply true for safe non-secret setup values. Never invent API keys, passwords, OAuth tokens, or credentials.

Select/model rules:
- If the field has options, examples and actionableExample.value must use one of the listed option "value" strings exactly.
- For model fields, do not mention removed providers or unrelated API model IDs. Explain only the models/options attached to this current node field.
- If no listed option is appropriate, set actionableExample.canApply to false and explain that the user must choose from the dropdown.

Respond with ONLY valid JSON (no markdown, no code fences):
{ "what": "...", "setupSummary": "...", "needed": "...", "dataImpact": "...", "you": "...", "aiBuild": "...", "aiRun": "...", "example": "...", "actionableExample": { "value": "...", "displayValue": "...", "canApply": true, "applyMode": "buildtime_ai_once", "reason": "...", "source": "ai_field_guidance" }, "offBehavior": "...", "emptyBehavior": "...", "defaultBehaviorLabel": "...", "recommendedOwner": "You", "ownerReason": "...", "validationConfidence": "high", "warnings": [], "safeValueSuggestion": "" }

Rules: No technical jargon. Under 35 words per key (needed may use up to 2 sentences). Do not repeat the field label at the start. Use "you" to address the user directly. Never write "configured default behavior" or vague "default behavior" unless you name the actual default.`;

    const { raw, buildAiUsage } = await runWithBuildUsageTracking(async () => {
      const raw = await geminiOrchestrator.processRequest(
        'chat-generation',
        { system: '', message: aiPrompt },
        { model: 'gemini-3.5-flash', temperature: 0.2, cache: false, usageStage: 'field_guidance' }
      );
      return { raw, buildAiUsage: snapshotBuildAiUsage() };
    });

    const text = (typeof raw === 'string' ? raw : (raw as any)?.content || '').trim();

    let description: Record<string, string> | null = null;
    try {
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      if (jsonMatch) description = JSON.parse(jsonMatch[0]);
    } catch {
      // fall through
    }

    const finalDescription = mergeGuidanceWithDeterministic(deterministicDescription, description as any);
    const cacheDescription = { ...finalDescription, _contextKey: fieldWalkContextKey };

    // -- Write to DB cache --
    if (finalDescription && workflowId && nodeId) {
      try {
        const db = getDbClient();
        await db
          .from('field_walk_cache')
          .upsert(
            {
              workflow_id: workflowId,
              node_id: nodeId,
              field_name: field.fieldName,
              description: cacheDescription,
              expires_at: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString(),
            },
            { onConflict: 'workflow_id,node_id,field_name', ignoreDuplicates: false }
          );
      } catch {
        // cache write failure is non-fatal
      }
    }

    res.json({ success: true, description: finalDescription, fromCache: false, buildAiUsage });
  } catch (error) {
    logger.error('Field walk-step error:', error);
    res.status(200).json({ success: true, description: null });
  }
});

// ==================== TEXT (DISABLED) ====================
router.post('/text/analyze', (req: Request, res: Response) => {
  res.status(501).json({ success: false, error: 'Text analysis has been removed.' });
});
router.post('/text/summarize', (req: Request, res: Response) => {
  res.status(501).json({ success: false, error: 'Text summarization has been removed.' });
});
router.post('/text/extract-entities', (req: Request, res: Response) => {
  res.status(501).json({ success: false, error: 'Entity extraction has been removed.' });
});

// ==================== IMAGE / AUDIO (DISABLED) ====================
router.post('/image/describe', (req: Request, res: Response) => {
  res.status(501).json({ success: false, error: 'Image description has been removed.' });
});
router.post('/image/compare', (req: Request, res: Response) => {
  res.status(501).json({ success: false, error: 'Image comparison has been removed.' });
});
router.post('/audio/transcribe', (req: Request, res: Response) => {
  res.status(501).json({ success: false, error: 'Audio transcription has been removed.' });
});

// ==================== AI WORKFLOW EDITOR ====================
router.get('/editor/capabilities', async (req: Request, res: Response) => {
  const t0 = Date.now();
  const principalResult = await resolveAiEditorPrincipal(req);
  if (!principalResult.ok) {
    return res.status(principalResult.status).json({ success: false, error: principalResult.error });
  }
  const { principal } = principalResult;
  const workflowId = typeof req.query.workflowId === 'string' ? req.query.workflowId : undefined;
  const phase = await fetchWorkflowLifecyclePhase(workflowId);
  const applyCheck = canApplyForPhase(principal, phase);
  logAiEditorEvent({
    action: 'capabilities',
    userId: principal.userId,
    workflowId,
    capabilities: Array.from(principal.capabilities),
    telemetryMs: Date.now() - t0,
    operationsSummary: 'get_capabilities',
  });
  res.json({
    success: true,
    role: principal.role,
    capabilities: Array.from(principal.capabilities),
    workflowId: workflowId || null,
    lifecyclePhase: phase,
    canApply: applyCheck.ok,
    applyBlockedReason: applyCheck.ok ? undefined : applyCheck.reason,
  });
});

router.get('/editor/audit/:workflowId', async (req: Request, res: Response) => {
  const principalResult = await resolveAiEditorPrincipal(req);
  if (!principalResult.ok) {
    return res.status(principalResult.status).json({ success: false, error: principalResult.error });
  }
  const cap = requireCapability(principalResult.principal, 'ai_editor:analyze');
  if (!cap.ok) {
    return res.status(cap.status).json({ success: false, error: cap.error });
  }
  const { workflowId } = req.params;
  const entries = readAiEditorAuditForWorkflow(workflowId, 200);
  res.json({ success: true, workflowId, entries });
});

router.post('/editor/suggest', async (req: Request, res: Response) => {
  const t0 = Date.now();
  try {
    const principalResult = await resolveAiEditorPrincipal(req);
    if (!principalResult.ok) {
      return res.status(principalResult.status).json({ success: false, error: principalResult.error });
    }
    const cap = requireCapability(principalResult.principal, 'ai_editor:suggest');
    if (!cap.ok) {
      return res.status(cap.status).json({ success: false, error: cap.error });
    }

    const body = req.body as {
      workflowId?: string;
      workflow: Workflow;
      nodeId?: string;
      prompt: string;
      conversationHistory?: Array<{ role: string; content: string }>;
    };

    if (!body || typeof body.prompt !== 'string' || !body.prompt.trim()) {
      return res.status(400).json({ success: false, error: 'prompt is required' });
    }
    if (!body.workflow || !Array.isArray(body.workflow.nodes) || !Array.isArray(body.workflow.edges)) {
      return res.status(400).json({ success: false, error: 'workflow with nodes[] and edges[] is required' });
    }

    if (!(await hasGeminiAccess(req))) {
      return res.status(503).json({ success: false, error: 'GEMINI_API_KEY not configured' });
    }

    const workflowId = body.workflowId || body.workflow.metadata?.id || 'unsaved';
    const conversationHistory = Array.isArray(body.conversationHistory) ? body.conversationHistory : [];
    const { message, operations, dryRun } = await aiWorkflowEditor.suggestWorkflowEdits(body.workflow, body.prompt, {
      focusedNodeId: body.nodeId,
      conversationHistory,
    });

    const previewValid = dryRun.errors.length === 0;
    const response: AiEditorResponse = {
      message,
      operations: operations as any,
      diff: dryRun.diff,
      updatedWorkflow: previewValid ? { workflow: dryRun.workflow } : undefined,
    };

    logAiEditorEvent({
      action: 'suggest',
      userId: principalResult.principal.userId,
      workflowId: String(workflowId),
      validationPassed: previewValid,
      operationsCount: operations.length,
      operationsSummary: operations.map((o) => o.kind).join(','),
      errors: dryRun.errors,
      warnings: dryRun.warnings,
      diffHash: hashDiff(dryRun.diff),
      promptPreview: body.prompt.slice(0, 500),
      telemetryMs: Date.now() - t0,
    });

    res.json({
      success: true,
      previewValid,
      previewErrors: dryRun.errors,
      previewWarnings: dryRun.warnings,
      result: response,
    });
  } catch (error) {
    logger.error('AI Editor suggest error:', error);
    sendAiGatewayError(res, error);
  }
});

router.post('/editor/apply', async (req: Request, res: Response) => {
  const t0 = Date.now();
  try {
    const principalResult = await resolveAiEditorPrincipal(req);
    if (!principalResult.ok) {
      return res.status(principalResult.status).json({ success: false, error: principalResult.error });
    }
    const { principal } = principalResult;

    const body = req.body as {
      workflowId?: string;
      workflow: Workflow;
      operations: any[];
      actor?: string;
      prompt?: string;
    };

    if (!body || !body.workflow || !Array.isArray(body.workflow.nodes) || !Array.isArray(body.workflow.edges)) {
      return res.status(400).json({ success: false, error: 'workflow with nodes[] and edges[] is required' });
    }
    if (!Array.isArray(body.operations) || body.operations.length === 0) {
      return res.status(400).json({ success: false, error: 'operations[] is required' });
    }

    const phase = await fetchWorkflowLifecyclePhase(body.workflowId || body.workflow.metadata?.id);
    const applyGate = canApplyForPhase(principal, phase);
    if (!applyGate.ok) {
      logAiEditorEvent({
        action: 'apply',
        userId: principal.userId,
        workflowId: body.workflowId,
        errors: [applyGate.reason],
        operationsSummary: 'blocked_rbac',
        telemetryMs: Date.now() - t0,
      });
      return res.status(403).json({ success: false, error: applyGate.reason });
    }

    const workflow: Workflow = body.workflow;
    const result = await aiWorkflowEditor.applyOperations(workflow, body.operations as any);

    const postValidate = unifiedGraphOrchestrator.validateWorkflow(
      result.workflow,
      result.executionOrder
    );
    if (!postValidate.valid) {
      result.errors.push(...postValidate.errors);
    }

    const response: AiEditorResponse = {
      message:
        result.errors.length === 0
          ? 'Applied AI editor operations successfully.'
          : 'Applied AI editor operations with validation errors.',
      operations: body.operations as any,
      diff: result.diff,
      updatedWorkflow: {
        workflow: result.workflow,
      },
    };

    if (result.errors.length > 0) {
      logAiEditorEvent({
        action: 'apply',
        userId: principal.userId,
        workflowId: body.workflowId || result.workflow.metadata?.id,
        validationPassed: false,
        operationsCount: body.operations.length,
        operationsSummary: body.operations.map((o: any) => o?.kind).join(','),
        errors: result.errors,
        warnings: result.warnings,
        diffHash: hashDiff(result.diff),
        promptPreview: body.prompt?.slice(0, 500),
        telemetryMs: Date.now() - t0,
      });
      return res.status(400).json({
        success: false,
        errors: result.errors,
        warnings: result.warnings,
        result: response,
      });
    }

    let versionId: string | undefined;
    try {
      const versionMetadata = {
        source: 'ai_editor',
        workflowId: body.workflowId || result.workflow.metadata?.id,
        actor: principal.userId,
        prompt: body.prompt,
      };
      const versionWorkflowPayload: Workflow = {
        ...result.workflow,
        metadata: {
          ...(result.workflow.metadata || {}),
          workflow_id: body.workflowId || result.workflow.metadata?.id,
        },
      };
      const ver = aiEditorVersioning.versionWorkflow(versionWorkflowPayload as any, versionMetadata);
      versionId = ver?.version_id;
    } catch (versionError) {
      logger.warn('AI Editor versioning failed (non-fatal):', versionError);
    }

    logAiEditorEvent({
      action: 'apply',
      userId: principal.userId,
      workflowId: body.workflowId || result.workflow.metadata?.id,
      versionIdOrHash: versionId,
      validationPassed: true,
      operationsCount: body.operations.length,
      operationsSummary: body.operations.map((o: any) => o?.kind).join(','),
      warnings: result.warnings,
      diffHash: hashDiff(result.diff),
      promptPreview: body.prompt?.slice(0, 500),
      telemetryMs: Date.now() - t0,
    });

    res.json({
      success: true,
      warnings: result.warnings,
      workflow: result.workflow,
      diff: result.diff,
      versionId,
      result: response,
    });
  } catch (error) {
    logger.error('AI Editor apply error:', error);
    sendAiGatewayError(res, error);
  }
});

router.post('/editor/analyze', async (req: Request, res: Response) => {
  const t0 = Date.now();
  try {
    const principalResult = await resolveAiEditorPrincipal(req);
    if (!principalResult.ok) {
      return res.status(principalResult.status).json({ success: false, error: principalResult.error });
    }
    const cap = requireCapability(principalResult.principal, 'ai_editor:analyze');
    if (!cap.ok) {
      return res.status(cap.status).json({ success: false, error: cap.error });
    }

    const body = req.body as {
      workflowId?: string;
      workflow: Workflow;
      nodeId?: string;
      prompt: string;
      conversationHistory?: Array<{ role: string; content: string }>;
    };

    if (!body || typeof body.prompt !== 'string' || !body.prompt.trim()) {
      return res.status(400).json({ success: false, error: 'prompt is required' });
    }
    if (!body.workflow || !Array.isArray(body.workflow.nodes) || !Array.isArray(body.workflow.edges)) {
      return res.status(400).json({ success: false, error: 'workflow with nodes[] and edges[] is required' });
    }

    const workflow: Workflow = body.workflow;
    const workflowId = body.workflowId || workflow.metadata?.id || 'unsaved';
    const focusedNodeId = body.nodeId;

    // Build registry context (safe projection of schemas, no secrets)
    const registryContext = aiWorkflowEditor.buildRegistryContextForWorkflow(workflow);

    // Validate workflow structure via orchestrator (DAG + edge invariants)
    const validation = unifiedGraphOrchestrator.validateWorkflow(workflow);

    const focusedNode = focusedNodeId
      ? workflow.nodes.find((n) => n.id === focusedNodeId)
      : undefined;

    const systemPrompt = [
      'You are an AI workflow editor assistant.',
      'You MUST operate in read-only analysis mode for this endpoint:',
      '- Explain what the workflow does.',
      '- Highlight structural or configuration issues.',
      '- Suggest high-level improvements.',
      'Do NOT propose raw edge lists; reason in terms of nodes and intent.',
      'Respect these invariants: single-source-of-truth registry, unified graph orchestrator, deterministic DAG, no manual edge wiring.',
    ].join(' ');

    const summaryContext = {
      workflowId,
      nodeCount: workflow.nodes.length,
      edgeCount: workflow.edges.length,
      nodeTypes: Array.from(
        new Set(
          workflow.nodes.map(
            (n) => (n.data as any)?.type || n.type,
          ),
        ),
      ),
      focusedNode: focusedNode
        ? {
            id: focusedNode.id,
            type: (focusedNode.data as any)?.type || focusedNode.type,
            label: (focusedNode.data as any)?.label,
          }
        : undefined,
      validation: {
        valid: validation.valid,
        errorCount: validation.errors.length,
        warningCount: validation.warnings.length,
      },
    };

    const historyBlock =
      Array.isArray(body.conversationHistory) && body.conversationHistory.length > 0
        ? [
            '',
            '=== RECENT CONVERSATION (continuity for follow-up questions) ===',
            'Use earlier turns when the latest user message is short or refers to prior discussion.',
            JSON.stringify(
              body.conversationHistory.map((t) => ({
                role: t.role,
                content:
                  typeof t.content === 'string' && t.content.length > 12000
                    ? `${t.content.slice(0, 12000)}\n...[truncated]`
                    : t.content,
              })),
              null,
              2
            ),
          ].join('\n')
        : '';

    const llmInput = [
      systemPrompt,
      '',
      '=== WORKFLOW SUMMARY ===',
      JSON.stringify(summaryContext, null, 2),
      '',
      '=== NODE SCHEMAS (REGISTRY PROJECTION) ===',
      JSON.stringify(registryContext.nodeSchemas, null, 2),
      historyBlock,
      '',
      '=== USER REQUEST (latest turn) ===',
      body.prompt,
    ].join('\n');

    if (!(await hasGeminiAccess(req))) {
      return res.status(503).json({
        success: false,
        error: 'GEMINI_API_KEY not configured',
      });
    }

    const rawResult = await geminiOrchestrator.processRequest('chat-generation', llmInput, {
      model: 'gemini-3.5-flash',
      temperature: 0.4,
    });

    const message =
      typeof rawResult === 'string'
        ? rawResult
        : (rawResult as any)?.content || JSON.stringify(rawResult);

    const response: AiEditorResponse = {
      message,
      operations: [],
      updatedWorkflow: {
        workflow,
      },
    };

    logAiEditorEvent({
      action: 'analyze',
      userId: principalResult.principal.userId,
      workflowId,
      validationPassed: validation.valid,
      operationsSummary: 'analyze_llm',
      errors: validation.valid ? undefined : validation.errors,
      warnings: validation.warnings,
      promptPreview: body.prompt.slice(0, 500),
      telemetryMs: Date.now() - t0,
    });

    const apiResponse: AiEditorRequest = {
      // Not strictly needed by client, but kept for contract completeness if they log it
      mode: 'analyze',
      intent: focusedNode ? 'explain_workflow' : 'suggest_improvements',
      scope: {
        workflowId,
        focusedNodeId,
      },
      prompt: body.prompt,
      workflowSnapshot: {
        workflow,
      },
    };

    res.json({
      success: true,
      request: apiResponse,
      result: response,
    });
  } catch (error) {
    logger.error('AI Editor analyze error:', error);
    sendAiGatewayError(res, error);
  }
});

router.post('/editor/suggest-improvements', async (req: Request, res: Response) => {
  try {
    const { workflow, nodeId } = req.body;
    if (!workflow || !nodeId) return res.status(400).json({ error: 'Workflow and nodeId are required' });
    const node = workflow.nodes?.find((n: any) => n.id === nodeId);
    if (!node) return res.status(404).json({ error: 'Node not found' });
    const suggestions = await aiWorkflowEditor.suggestNodeImprovements(workflow, node);
    res.json({ success: true, suggestions });
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : 'Unknown error' });
  }
});

router.post('/editor/replace-node', async (req: Request, res: Response) => {
  try {
    const { workflow, nodeId, replacementType } = req.body;
    if (!workflow || !nodeId || !replacementType) {
      return res.status(400).json({ error: 'Workflow, nodeId, and replacementType are required' });
    }
    const result = await aiWorkflowEditor.replaceNode(workflow, nodeId, replacementType);
    res.json({ ...result });
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : 'Unknown error' });
  }
});

// Registry context for a given workflow – lightweight projection from unified-node-registry
router.post('/editor/registry-context', async (req: Request, res: Response) => {
  try {
    const principalResult = await resolveAiEditorPrincipal(req);
    if (!principalResult.ok) {
      return res.status(principalResult.status).json({ success: false, error: principalResult.error });
    }
    const cap = requireCapability(principalResult.principal, 'ai_editor:analyze');
    if (!cap.ok) {
      return res.status(cap.status).json({ success: false, error: cap.error });
    }
    const { workflow } = req.body;
    if (!workflow || !Array.isArray(workflow.nodes)) {
      return res.status(400).json({ success: false, error: 'workflow with nodes[] is required' });
    }
    const context = aiWorkflowEditor.buildRegistryContextForWorkflow(workflow);
    res.json({ success: true, context });
  } catch (error) {
    logger.error('AI Editor registry-context error:', error);
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error',
    });
  }
});

router.post('/editor/code-assist', async (req: Request, res: Response) => {
  try {
    const { node, code, language } = req.body;
    if (!node || !code || !language) {
      return res.status(400).json({ error: 'Node, code, and language are required' });
    }
    const assistance = await aiWorkflowEditor.realTimeCodeAssist(node, code, language);
    res.json({ success: true, assistance });
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : 'Unknown error' });
  }
});

// ==================== WORKFLOW BUILDER ====================
router.post('/builder/generate-from-prompt', async (req: Request, res: Response) => {
  try {
    const { prompt, constraints, options } = req.body;
    if (!prompt || typeof prompt !== 'string') return res.status(400).json({ error: 'Prompt is required' });
    const { workflowLifecycleManager } = await import('../services/workflow-lifecycle-manager');
    if (options?.stream) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      const lifecycleResult = await workflowLifecycleManager.generateWorkflowGraph(prompt, {
        ...constraints,
        onProgress: (step: number, stepName: string, progress: number, details?: any) => {
          res.write(`data: ${JSON.stringify({ step, stepName, progress, details })}\n\n`);
        },
      });
      res.write(`data: ${JSON.stringify({ type: 'complete', workflow: lifecycleResult.workflow })}\n\n`);
      res.end();
    } else {
      const lifecycleResult = await workflowLifecycleManager.generateWorkflowGraph(prompt, constraints);
      res.json({
        success: true,
        workflow: lifecycleResult.workflow,
        requirements: (lifecycleResult as any).requirements || {},
        documentation: lifecycleResult.documentation,
        requiredCredentials: lifecycleResult.requiredCredentials || [],
      });
    }
  } catch (error) {
    logger.error('Workflow generation error:', error);
    sendAiGatewayError(res, error);
  }
});

router.post('/builder/improve-workflow', (req: Request, res: Response) => {
  res.status(501).json({
    success: false,
    error: 'Use /editor/suggest-improvements instead.',
    deprecated: true,
    alternative: '/editor/suggest-improvements',
  });
});

// ==================== GEMINI (replaces legacy Ollama routes) ====================
router.post('/ollama/generate', async (req: Request, res: Response) => {
  try {
    const { model, prompt, options } = req.body;
    if (!prompt) return res.status(400).json({ error: 'Prompt is required' });
    const result = await geminiOrchestrator.processRequest('chat-generation', prompt, {
      model: model || 'gemini-3.5-flash',
      ...options,
    });
    res.json({ success: true, result: typeof result === 'string' ? { content: result } : result });
  } catch (error) {
    sendAiGatewayError(res, error);
  }
});

router.post('/ollama/chat', async (req: Request, res: Response) => {
  try {
    const { model, messages, options } = req.body;
    if (!messages || !Array.isArray(messages)) return res.status(400).json({ error: 'Messages array is required' });
    if (!(await hasGeminiAccess(req))) return res.status(503).json({ success: false, error: 'GEMINI_API_KEY not configured' });
    const response = await llmAdapter.chat('gemini', messages, {
      model: model || 'gemini-3.5-flash',
      temperature: options?.temperature ?? 0.7,
      maxTokens: options?.max_tokens,
    });
    res.json({ success: true, result: { content: response.content, usage: response.usage } });
  } catch (error) {
    sendAiGatewayError(res, error);
  }
});

router.get('/ollama/models', async (req: Request, res: Response) => {
  try {
    const models = GEMINI_MODELS.map(name => ({ name }));
    res.json({ success: true, models });
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : 'Unknown error' });
  }
});

router.post('/ollama/load-model', (req: Request, res: Response) => {
  res.json({ success: true, message: 'Gemini models are API-based; no loading required.' });
});

// ==================== METRICS ====================
router.get('/metrics', async (req: Request, res: Response) => {
  try {
    const stats = aiPerformanceMonitor.getStats();
    const suggestions = aiPerformanceMonitor.getOptimizationSuggestions();
    res.json({ success: true, metrics: stats, suggestions });
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : 'Unknown error' });
  }
});

router.get('/metrics/optimization-suggestions', async (req: Request, res: Response) => {
  try {
    const suggestions = aiPerformanceMonitor.getOptimizationSuggestions();
    res.json({ success: true, suggestions });
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : 'Unknown error' });
  }
});

export default router;
