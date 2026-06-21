/**
 * Tests for POST /notifications/email and POST /notifications/send (Phase 2).
 * SES client is mocked — no real AWS calls.
 */

const mockSend = jest.fn().mockResolvedValue({ MessageId: 'msg-001' });
jest.mock('@aws-sdk/client-ses', () => ({
  SESClient: jest.fn().mockImplementation(() => ({ send: mockSend })),
  SendEmailCommand: jest.fn().mockImplementation((input) => ({ _input: input })),
}));

import request from 'supertest';
import app from '../index';
import { _resetSesClient } from '../lib/ses';
import { SESClient, SendEmailCommand } from '@aws-sdk/client-ses';

describe('POST /notifications/email', () => {
  const baseEnv = { SES_FROM_EMAIL: 'noreply@ctrlchecks.ai', EXECUTION_EMAIL_NOTIFICATIONS: 'true' };

  beforeEach(() => {
    jest.clearAllMocks();
    Object.assign(process.env, baseEnv);
    (SESClient as unknown as jest.Mock).mockImplementation(() => ({ send: mockSend }));
    _resetSesClient();
  });

  afterEach(() => {
    delete process.env.SES_FROM_EMAIL;
    delete process.env.EXECUTION_EMAIL_NOTIFICATIONS;
  });

  it('sends execution_completed email and returns 200', async () => {
    const res = await request(app)
      .post('/notifications/email')
      .send({ templateId: 'execution_completed', data: { workflowName: 'My WF', executionId: 'exec-1' }, to: 'user@example.com' });

    expect(res.status).toBe(200);
    expect(res.body.status).toBe('sent');
    expect(res.body.channel).toBe('email');
    expect(typeof res.body.notificationId).toBe('string');
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it('sends correct subject for execution_completed', async () => {
    await request(app)
      .post('/notifications/email')
      .send({ templateId: 'execution_completed', data: { workflowName: 'Alpha WF', executionId: 'e-99' }, to: 'a@b.com' });

    const cmd = (SendEmailCommand as unknown as jest.Mock).mock.calls[0][0];
    expect(cmd.Message.Subject.Data).toContain('Alpha WF');
    expect(cmd.Message.Subject.Data).toContain('completed');
  });

  it('sends execution_failed email', async () => {
    const res = await request(app)
      .post('/notifications/email')
      .send({ templateId: 'execution_failed', data: { workflowName: 'Beta WF', error: 'Timeout' }, to: 'a@b.com' });

    expect(res.status).toBe(200);
    const cmd = (SendEmailCommand as unknown as jest.Mock).mock.calls[0][0];
    expect(cmd.Message.Subject.Data).toContain('failed');
    expect(cmd.Message.Body.Html.Data).toContain('Timeout');
  });

  it('sends welcome email (no EXECUTION_EMAIL_NOTIFICATIONS guard)', async () => {
    process.env.EXECUTION_EMAIL_NOTIFICATIONS = 'false';
    const res = await request(app)
      .post('/notifications/email')
      .send({ templateId: 'welcome', data: { name: 'Alice' }, to: 'alice@example.com' });

    expect(res.status).toBe(200);
    expect(res.body.status).toBe('sent');
    const cmd = (SendEmailCommand as unknown as jest.Mock).mock.calls[0][0];
    expect(cmd.Message.Body.Html.Data).toContain('Alice');
  });

  it('returns 200 status=suppressed when EXECUTION_EMAIL_NOTIFICATIONS=false', async () => {
    process.env.EXECUTION_EMAIL_NOTIFICATIONS = 'false';
    const res = await request(app)
      .post('/notifications/email')
      .send({ templateId: 'execution_completed', data: { workflowName: 'WF', executionId: 'e-1' }, to: 'u@e.com' });

    expect(res.status).toBe(200);
    expect(res.body.status).toBe('suppressed');
    expect(res.body.reason).toBe('notifications_disabled');
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('returns 503 when SES_FROM_EMAIL is not set', async () => {
    delete process.env.SES_FROM_EMAIL;
    _resetSesClient();
    const res = await request(app)
      .post('/notifications/email')
      .send({ templateId: 'execution_completed', data: {}, to: 'u@e.com' });

    expect(res.status).toBe(503);
    expect(res.body.code).toBe('SES_NOT_CONFIGURED');
    expect(mockSend).not.toHaveBeenCalled();
  });

  it('returns 400 when to is missing', async () => {
    const res = await request(app)
      .post('/notifications/email')
      .send({ templateId: 'execution_completed', data: {} });

    expect(res.status).toBe(400);
    expect(res.body.code).toBe('MISSING_TO');
  });

  it('returns 400 when templateId is invalid', async () => {
    const res = await request(app)
      .post('/notifications/email')
      .send({ templateId: 'nonexistent', data: {}, to: 'u@e.com' });

    expect(res.status).toBe(400);
    expect(res.body.code).toBe('INVALID_TEMPLATE_ID');
  });

  it('returns 502 when SES throws', async () => {
    mockSend.mockRejectedValueOnce(new Error('SES throttled'));
    const res = await request(app)
      .post('/notifications/email')
      .send({ templateId: 'execution_completed', data: { workflowName: 'WF', executionId: 'e-1' }, to: 'u@e.com' });

    expect(res.status).toBe(502);
    expect(res.body.code).toBe('SES_SEND_FAILED');
  });

  it('escapes HTML in workflowName', async () => {
    await request(app)
      .post('/notifications/email')
      .send({ templateId: 'execution_completed', data: { workflowName: '<script>xss</script>', executionId: 'e-1' }, to: 'u@e.com' });

    const cmd = (SendEmailCommand as unknown as jest.Mock).mock.calls[0][0];
    expect(cmd.Message.Body.Html.Data).not.toContain('<script>');
    expect(cmd.Message.Body.Html.Data).toContain('&lt;script&gt;');
  });
});

describe('POST /notifications/send', () => {
  const baseEnv = { SES_FROM_EMAIL: 'noreply@ctrlchecks.ai', EXECUTION_EMAIL_NOTIFICATIONS: 'true' };

  beforeEach(() => {
    jest.clearAllMocks();
    Object.assign(process.env, baseEnv);
    (SESClient as unknown as jest.Mock).mockImplementation(() => ({ send: mockSend }));
    _resetSesClient();
  });

  afterEach(() => {
    delete process.env.SES_FROM_EMAIL;
    delete process.env.EXECUTION_EMAIL_NOTIFICATIONS;
  });

  it('dispatches channel=email to SES', async () => {
    const res = await request(app)
      .post('/notifications/send')
      .send({ type: 'execution_completed', channel: 'email', to: 'u@e.com', data: { workflowName: 'WF', executionId: 'e-1' } });

    expect(res.status).toBe(200);
    expect(res.body.status).toBe('sent');
    expect(res.body.channel).toBe('email');
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it('returns 401 for channel=in_app without user header', async () => {
    const res = await request(app)
      .post('/notifications/send')
      .send({ type: 'ping', channel: 'in_app', data: {} });

    expect(res.status).toBe(401);
  });

  it('returns 400 for unknown channel', async () => {
    const res = await request(app)
      .post('/notifications/send')
      .send({ type: 'ping', channel: 'carrier_pigeon', data: {} });

    expect(res.status).toBe(400);
    expect(res.body.code).toBe('UNKNOWN_CHANNEL');
  });

  it('returns 400 for channel=email without templateId', async () => {
    const res = await request(app)
      .post('/notifications/send')
      .send({ channel: 'email', data: {}, to: 'u@e.com' });

    expect(res.status).toBe(400);
    expect(res.body.code).toBe('MISSING_FIELDS');
  });
});
