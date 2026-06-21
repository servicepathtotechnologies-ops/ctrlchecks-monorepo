import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  normalizeGenerateWorkflowUiError,
  buildGenerateWorkflowUiErrorMessage,
} from '../generate-workflow-error';

// ── awsClient mock (required for roles.ts) ─────────────────────────────
const { mockGetUser, mockRpc, mockEq, mockSelect, mockFrom } = vi.hoisted(() => {
  const _eq = vi.fn();
  const _select = vi.fn(() => ({ eq: _eq }));
  const _from = vi.fn(() => ({ select: _select }));
  const _getUser = vi.fn();
  const _rpc = vi.fn();
  return { mockGetUser: _getUser, mockRpc: _rpc, mockEq: _eq, mockSelect: _select, mockFrom: _from };
});

vi.mock('@/integrations/aws/client', () => ({
  awsClient: {
    auth: { getUser: mockGetUser },
    from: mockFrom,
    rpc: mockRpc,
  },
}));

import {
  hasRole,
  isAdmin,
  isModerator,
  getUserRole,
  requireAdmin,
  canAccessAdminPortal,
} from '../roles';

// ──────────────────────────────────────────────────────────────────────

describe('normalizeGenerateWorkflowUiError', () => {
  it('returns fallback message when payload is null', () => {
    const result = normalizeGenerateWorkflowUiError(null, 'fallback');
    expect(result.message).toBe('fallback');
    expect(result.code).toBeUndefined();
    expect(result.stage).toBeUndefined();
    expect(result.correlationId).toBeUndefined();
    expect(result.stageTrace).toBeUndefined();
  });

  it('returns fallback when payload is undefined', () => {
    const result = normalizeGenerateWorkflowUiError(undefined, 'default msg');
    expect(result.message).toBe('default msg');
  });

  it('uses payload.message when present', () => {
    const result = normalizeGenerateWorkflowUiError({ message: 'from payload' }, 'fallback');
    expect(result.message).toBe('from payload');
  });

  it('falls back to payload.error string when message absent', () => {
    const result = normalizeGenerateWorkflowUiError({ error: 'err msg' }, 'fallback');
    expect(result.message).toBe('err msg');
  });

  it('uses fallback when message is whitespace-only', () => {
    const result = normalizeGenerateWorkflowUiError({ message: '   ' }, 'fallback');
    expect(result.message).toBe('fallback');
  });

  it('uses fallback when both message and error absent', () => {
    const result = normalizeGenerateWorkflowUiError({}, 'fallback');
    expect(result.message).toBe('fallback');
  });

  it('sets code from payload.error when it is a string', () => {
    const result = normalizeGenerateWorkflowUiError({ error: 'ERR_CODE', message: 'msg' }, 'fb');
    expect(result.code).toBe('ERR_CODE');
  });

  it('code is undefined when payload.error is not a string', () => {
    const result = normalizeGenerateWorkflowUiError({ error: 42 }, 'fallback');
    expect(result.code).toBeUndefined();
  });

  it('sets stage from payload.stage', () => {
    const result = normalizeGenerateWorkflowUiError({ message: 'msg', stage: 'edge-reasoning' }, 'fb');
    expect(result.stage).toBe('edge-reasoning');
  });

  it('stage falls back to first stageTrace entry that has an error field', () => {
    const stageTrace = [
      { stage: 'intent', outputSummary: 'ok' },
      { stage: 'structural', error: 'boom' },
      { stage: 'edge-reasoning' },
    ];
    const result = normalizeGenerateWorkflowUiError({ stageTrace }, 'fb');
    expect(result.stage).toBe('structural');
  });

  it('stage falls back to last stageTrace entry when no errored entry exists', () => {
    const stageTrace = [{ stage: 'intent' }, { stage: 'structural' }];
    const result = normalizeGenerateWorkflowUiError({ stageTrace }, 'fb');
    expect(result.stage).toBe('structural');
  });

  it('sets correlationId from payload.correlationId', () => {
    const result = normalizeGenerateWorkflowUiError({ message: 'msg', correlationId: 'abc-123' }, 'fb');
    expect(result.correlationId).toBe('abc-123');
  });

  it('sets correlationId from snake_case payload.correlation_id as fallback', () => {
    const result = normalizeGenerateWorkflowUiError({ message: 'msg', correlation_id: 'xyz-456' }, 'fb');
    expect(result.correlationId).toBe('xyz-456');
  });

  it('correlationId is undefined when neither key is present', () => {
    const result = normalizeGenerateWorkflowUiError({ message: 'msg' }, 'fb');
    expect(result.correlationId).toBeUndefined();
  });

  it('sets stageTrace when payload.stageTrace is an array', () => {
    const stageTrace = [{ stage: 'intent' }];
    const result = normalizeGenerateWorkflowUiError({ stageTrace }, 'fb');
    expect(result.stageTrace).toEqual(stageTrace);
  });

  it('stageTrace is undefined when not an array', () => {
    const result = normalizeGenerateWorkflowUiError({ stageTrace: 'bad-value' }, 'fb');
    expect(result.stageTrace).toBeUndefined();
  });
});

describe('buildGenerateWorkflowUiErrorMessage', () => {
  it('returns message only when no stage or correlationId', () => {
    expect(buildGenerateWorkflowUiErrorMessage({ message: 'Something failed' })).toBe('Something failed');
  });

  it('appends stage when present', () => {
    expect(buildGenerateWorkflowUiErrorMessage({ message: 'fail', stage: 'intent' }))
      .toBe('fail | stage: intent');
  });

  it('appends correlationId when present', () => {
    expect(buildGenerateWorkflowUiErrorMessage({ message: 'fail', correlationId: 'corr-1' }))
      .toBe('fail | correlation: corr-1');
  });

  it('appends both stage and correlationId in order', () => {
    const result = buildGenerateWorkflowUiErrorMessage({
      message: 'fail',
      stage: 'intent',
      correlationId: 'corr-1',
    });
    expect(result).toBe('fail | stage: intent | correlation: corr-1');
  });
});

// ── roles.ts ──────────────────────────────────────────────────────────

describe('hasRole', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns false when getUser returns an error', async () => {
    mockGetUser.mockResolvedValue({ data: { user: null }, error: { message: 'auth error' } });
    expect(await hasRole('admin')).toBe(false);
  });

  it('returns false when user is null', async () => {
    mockGetUser.mockResolvedValue({ data: { user: null }, error: null });
    expect(await hasRole('admin')).toBe(false);
  });

  it('returns true when rpc returns true', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'hr-true-user' } }, error: null });
    mockRpc.mockResolvedValue({ data: true, error: null });
    expect(await hasRole('admin')).toBe(true);
  });

  it('returns false when rpc returns false', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'hr-false-user' } }, error: null });
    mockRpc.mockResolvedValue({ data: false, error: null });
    expect(await hasRole('moderator')).toBe(false);
  });

  it('returns false when rpc returns an error', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'hr-err-user' } }, error: null });
    mockRpc.mockResolvedValue({ data: null, error: { code: 'SOME_ERR', message: 'rpc failed' } });
    expect(await hasRole('user')).toBe(false);
  });

  it('caches result so rpc is only called once for the same user+role', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'cache-unique-hr-user' } }, error: null });
    mockRpc.mockResolvedValue({ data: true, error: null });
    await hasRole('admin');
    await hasRole('admin');
    expect(mockRpc).toHaveBeenCalledTimes(1);
  });
});

describe('isAdmin', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('delegates to hasRole with "admin" and returns true when rpc confirms', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'is-admin-user' } }, error: null });
    mockRpc.mockResolvedValue({ data: true, error: null });
    expect(await isAdmin()).toBe(true);
    expect(mockRpc).toHaveBeenCalledWith('has_role', { _user_id: 'is-admin-user', _role: 'admin' });
  });

  it('returns false when user does not have admin role', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'is-admin-false-user' } }, error: null });
    mockRpc.mockResolvedValue({ data: false, error: null });
    expect(await isAdmin()).toBe(false);
  });
});

describe('isModerator', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('delegates to hasRole with "moderator"', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'is-mod-user' } }, error: null });
    mockRpc.mockResolvedValue({ data: true, error: null });
    expect(await isModerator()).toBe(true);
    expect(mockRpc).toHaveBeenCalledWith('has_role', { _user_id: 'is-mod-user', _role: 'moderator' });
  });

  it('returns false when user is not a moderator', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'is-mod-false-user' } }, error: null });
    mockRpc.mockResolvedValue({ data: false, error: null });
    expect(await isModerator()).toBe(false);
  });
});

describe('getUserRole', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns null when user is not authenticated', async () => {
    mockGetUser.mockResolvedValue({ data: { user: null }, error: null });
    expect(await getUserRole()).toBeNull();
  });

  it('returns null when DB returns an error', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'gur-db-err-user' } }, error: null });
    mockEq.mockResolvedValue({ data: null, error: { code: 'DB_ERR', message: 'db error' } });
    expect(await getUserRole()).toBeNull();
  });

  it('returns null when no role rows are returned', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'gur-no-rows-user' } }, error: null });
    mockEq.mockResolvedValue({ data: [], error: null });
    expect(await getUserRole()).toBeNull();
  });

  it('returns highest-priority role when user has multiple roles', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'gur-multi-user' } }, error: null });
    mockEq.mockResolvedValue({
      data: [{ role: 'moderator' }, { role: 'admin' }, { role: 'user' }],
      error: null,
    });
    expect(await getUserRole()).toBe('admin');
  });

  it('returns the single role when user has exactly one role', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'gur-single-user' } }, error: null });
    mockEq.mockResolvedValue({ data: [{ role: 'moderator' }], error: null });
    expect(await getUserRole()).toBe('moderator');
  });

  it('prioritises admin over moderator over user', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'gur-priority-user' } }, error: null });
    mockEq.mockResolvedValue({
      data: [{ role: 'user' }, { role: 'moderator' }],
      error: null,
    });
    expect(await getUserRole()).toBe('moderator');
  });
});

describe('requireAdmin', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('resolves without error when user is an admin', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'req-admin-ok-user' } }, error: null });
    mockRpc.mockResolvedValue({ data: true, error: null });
    await expect(requireAdmin()).resolves.toBeUndefined();
  });

  it('throws when user is not an admin', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'req-admin-deny-user' } }, error: null });
    mockRpc.mockResolvedValue({ data: false, error: null });
    await expect(requireAdmin()).rejects.toThrow('Admin access required');
  });
});

describe('canAccessAdminPortal', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns true for admin role', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'portal-admin-user' } }, error: null });
    mockEq.mockResolvedValue({ data: [{ role: 'admin' }], error: null });
    expect(await canAccessAdminPortal()).toBe(true);
  });

  it('returns true for moderator role', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'portal-mod-user' } }, error: null });
    mockEq.mockResolvedValue({ data: [{ role: 'moderator' }], error: null });
    expect(await canAccessAdminPortal()).toBe(true);
  });

  it('returns false for basic user role', async () => {
    mockGetUser.mockResolvedValue({ data: { user: { id: 'portal-basic-user' } }, error: null });
    mockEq.mockResolvedValue({ data: [{ role: 'user' }], error: null });
    expect(await canAccessAdminPortal()).toBe(false);
  });
});
