import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { type GuidedStatusContent, mapWorkflowIssueToGuidance } from '../workflow-guidance';
import { getAIGuidance } from '../ai-error-guidance';

// ── module mocks ─────────────────────────────────────────────────────────────

vi.mock('@/config/endpoints', () => ({
  ENDPOINTS: { itemBackend: 'http://test-api' },
}));

vi.mock('@/lib/workflow-guidance', () => ({
  mapWorkflowIssueToGuidance: vi.fn(),
}));

// ── helpers ──────────────────────────────────────────────────────────────────

const STATIC_FALLBACK: GuidedStatusContent = {
  title: 'Static fallback',
  description: 'Fallback from rules',
  tone: 'attention',
};

function makeFetchResponse(body: object, ok = true): Response {
  return { ok, json: vi.fn().mockResolvedValue(body) } as unknown as Response;
}

function requestBodyOf(fetchMock: ReturnType<typeof vi.fn>, callIndex = 0) {
  return JSON.parse(fetchMock.mock.calls[callIndex][1].body as string);
}

// ── setup ────────────────────────────────────────────────────────────────────

beforeEach(() => {
  vi.mocked(mapWorkflowIssueToGuidance).mockReturnValue(STATIC_FALLBACK);
});

afterEach(() => {
  vi.restoreAllMocks();
});

// ── happy path ────────────────────────────────────────────────────────────────

describe('getAIGuidance — happy path', () => {
  it('returns AI-provided title and description on successful response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      makeFetchResponse({ title: 'AI title', description: 'AI desc' })
    ));
    const result = await getAIGuidance({ code: 'ERR' });
    expect(result.title).toBe('AI title');
    expect(result.description).toBe('AI desc');
  });

  it('includes resolution and nextSteps from backend when provided', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      makeFetchResponse({
        title: 'T',
        description: 'D',
        resolution: 'Fix it',
        nextSteps: ['step1', 'step2'],
      })
    ));
    const result = await getAIGuidance({});
    expect(result.resolution).toBe('Fix it');
    expect(result.nextSteps).toEqual(['step1', 'step2']);
  });

  it('passes backend tone through to result', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      makeFetchResponse({ title: 'T', description: 'D', tone: 'success' })
    ));
    const result = await getAIGuidance({});
    expect(result.tone).toBe('success');
  });

  it('defaults tone to "attention" when backend omits it', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      makeFetchResponse({ title: 'T', description: 'D' })
    ));
    const result = await getAIGuidance({});
    expect(result.tone).toBe('attention');
  });

  it('does not call static fallback on success', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      makeFetchResponse({ title: 'T', description: 'D' })
    ));
    await getAIGuidance({});
    expect(mapWorkflowIssueToGuidance).not.toHaveBeenCalled();
  });
});

// ── fallback on invalid response shape ────────────────────────────────────────

describe('getAIGuidance — invalid shape falls back to static', () => {
  it('falls back when backend omits title', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      makeFetchResponse({ description: 'D only' })
    ));
    const result = await getAIGuidance({ code: 'E' });
    expect(result).toBe(STATIC_FALLBACK);
    expect(mapWorkflowIssueToGuidance).toHaveBeenCalledWith({ code: 'E' });
  });

  it('falls back when backend omits description', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
      makeFetchResponse({ title: 'T only' })
    ));
    const result = await getAIGuidance({});
    expect(result).toBe(STATIC_FALLBACK);
  });

  it('falls back when backend returns empty object', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(makeFetchResponse({})));
    const result = await getAIGuidance({});
    expect(result).toBe(STATIC_FALLBACK);
  });
});

// ── fallback on HTTP / network errors ────────────────────────────────────────

describe('getAIGuidance — error handling falls back to static', () => {
  it('falls back on HTTP error status (non-ok response)', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(makeFetchResponse({}, false)));
    const result = await getAIGuidance({ message: 'boom' });
    expect(result).toBe(STATIC_FALLBACK);
    expect(mapWorkflowIssueToGuidance).toHaveBeenCalledWith({ message: 'boom' });
  });

  it('falls back on network error (fetch rejects)', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('network down')));
    const result = await getAIGuidance({});
    expect(result).toBe(STATIC_FALLBACK);
  });

  it('passes the original errorData to static fallback', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('x')));
    const errorData = { code: 'MISSING_CRED', message: 'No token', hint: 'Re-auth' };
    await getAIGuidance(errorData);
    expect(mapWorkflowIssueToGuidance).toHaveBeenCalledWith(errorData);
  });
});

// ── request body — workflowContext fields ─────────────────────────────────────

describe('getAIGuidance — context merging into request body', () => {
  it('sends phase from workflowContext', async () => {
    const fetchMock = vi.fn().mockResolvedValue(makeFetchResponse({ title: 'T', description: 'D' }));
    vi.stubGlobal('fetch', fetchMock);
    await getAIGuidance({ code: 'E' }, { phase: 'execution' });
    expect(requestBodyOf(fetchMock).context.phase).toBe('execution');
  });

  it('falls back to errorData.details.phase when workflowContext.phase is absent', async () => {
    const fetchMock = vi.fn().mockResolvedValue(makeFetchResponse({ title: 'T', description: 'D' }));
    vi.stubGlobal('fetch', fetchMock);
    await getAIGuidance({ details: { phase: 'validation' } }, {});
    expect(requestBodyOf(fetchMock).context.phase).toBe('validation');
  });

  it('sends missingInputs from workflowContext', async () => {
    const fetchMock = vi.fn().mockResolvedValue(makeFetchResponse({ title: 'T', description: 'D' }));
    vi.stubGlobal('fetch', fetchMock);
    const inputs = [{ fieldName: 'apiKey', nodeLabel: 'HTTP Node' }];
    await getAIGuidance({}, { missingInputs: inputs });
    expect(requestBodyOf(fetchMock).context.missingInputs).toEqual(inputs);
  });

  it('reads missingInputs from errorData.details when workflowContext is absent', async () => {
    const fetchMock = vi.fn().mockResolvedValue(makeFetchResponse({ title: 'T', description: 'D' }));
    vi.stubGlobal('fetch', fetchMock);
    const inputs = [{ fieldName: 'token', nodeLabel: 'Auth' }];
    await getAIGuidance({ details: { missingInputs: inputs } });
    expect(requestBodyOf(fetchMock).context.missingInputs).toEqual(inputs);
  });

  it('sends undefined for missingInputs when errorData.details.missingInputs is not an array', async () => {
    const fetchMock = vi.fn().mockResolvedValue(makeFetchResponse({ title: 'T', description: 'D' }));
    vi.stubGlobal('fetch', fetchMock);
    await getAIGuidance({ details: { missingInputs: 'not-an-array' as unknown as [] } });
    expect(requestBodyOf(fetchMock).context.missingInputs).toBeUndefined();
  });

  it('sends missingCredentials from workflowContext', async () => {
    const fetchMock = vi.fn().mockResolvedValue(makeFetchResponse({ title: 'T', description: 'D' }));
    vi.stubGlobal('fetch', fetchMock);
    const creds = [{ provider: 'google', displayName: 'Google' }];
    await getAIGuidance({}, { missingCredentials: creds });
    expect(requestBodyOf(fetchMock).context.missingCredentials).toEqual(creds);
  });

  it('reads missingCredentials from errorData.details when workflowContext absent', async () => {
    const fetchMock = vi.fn().mockResolvedValue(makeFetchResponse({ title: 'T', description: 'D' }));
    vi.stubGlobal('fetch', fetchMock);
    const creds = [{ provider: 'github', displayName: 'GitHub' }];
    await getAIGuidance({ details: { missingCredentials: creds } });
    expect(requestBodyOf(fetchMock).context.missingCredentials).toEqual(creds);
  });

  it('sends executionValidationErrors from workflowContext', async () => {
    const fetchMock = vi.fn().mockResolvedValue(makeFetchResponse({ title: 'T', description: 'D' }));
    vi.stubGlobal('fetch', fetchMock);
    const errors = ['Node X has no inputs', 'Edge Y is invalid'];
    await getAIGuidance({}, { executionValidationErrors: errors });
    expect(requestBodyOf(fetchMock).context.executionValidationErrors).toEqual(errors);
  });

  it('reads executionValidationErrors from errorData.details when workflowContext absent', async () => {
    const fetchMock = vi.fn().mockResolvedValue(makeFetchResponse({ title: 'T', description: 'D' }));
    vi.stubGlobal('fetch', fetchMock);
    const errors = ['Missing trigger'];
    await getAIGuidance({ details: { executionValidationErrors: errors } });
    expect(requestBodyOf(fetchMock).context.executionValidationErrors).toEqual(errors);
  });

  it('posts to the correct endpoint URL', async () => {
    const fetchMock = vi.fn().mockResolvedValue(makeFetchResponse({ title: 'T', description: 'D' }));
    vi.stubGlobal('fetch', fetchMock);
    await getAIGuidance({});
    expect(fetchMock.mock.calls[0][0]).toBe('http://test-api/api/ai/error-guidance');
  });
});
