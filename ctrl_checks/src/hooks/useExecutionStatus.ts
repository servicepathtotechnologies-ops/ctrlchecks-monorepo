import { useEffect, useRef, useCallback, useState } from 'react';
import { awsClient } from '@/integrations/aws/client';
import { useWorkflowStore } from '@/stores/workflowStore';
import { useExecutionWebSocket } from './useExecutionWebSocket';
import { ENDPOINTS } from '@/config/endpoints';

const POLL_INTERVAL_MS = 3000;

function isTerminal(status: string): boolean {
  return status === 'success' || status === 'failed' || status === 'completed' || status === 'error';
}

export function useExecutionStatus() {
  const { activeExecution, updateExecutionStatus, clearActiveExecution } = useWorkflowStore();
  const executionId = activeExecution?.executionId ?? null;

  const pollTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  // Track token in state so a WS reconnect is triggered when the session resolves.
  // tokenRef was a plain ref before — changing it didn't re-render, so WS always
  // connected with token=null and fell back to polling immediately.
  const [token, setToken] = useState<string | null>(null);

  useEffect(() => {
    awsClient.auth.getSession().then(({ data }) => {
      setToken(data?.session?.access_token ?? null);
    });
  }, [executionId]);

  const stopPolling = useCallback(() => {
    if (pollTimerRef.current) {
      clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
    }
  }, []);

  const pollOnce = useCallback(async (id: string) => {
    try {
      const { data: sessionData } = await awsClient.auth.getSession();
      const tok = sessionData?.session?.access_token;
      const res = await fetch(`${ENDPOINTS.itemBackend}/api/execution-status/${id}`, {
        headers: tok ? { Authorization: `Bearer ${tok}` } : {},
      });
      if (!res.ok) return;
      const body = await res.json();
      updateExecutionStatus({
        status: body.status,
        progress: body.progress ?? 0,
        currentStep: body.current_step ?? null,
        errorMessage: body.error ?? null,
      });
      if (isTerminal(body.status)) {
        stopPolling();
        clearActiveExecution();
      }
    } catch { /* ignore */ }
  }, [updateExecutionStatus, clearActiveExecution, stopPolling]);

  const handleWsMessage = useCallback((msg: unknown) => {
    const m = msg as Record<string, unknown>;
    if (m.type === 'EXECUTION_UPDATE' || m.type === 'status') {
      const data = (m.data ?? m) as Record<string, unknown>;
      const status = String(data.status ?? '');
      if (status) {
        updateExecutionStatus({
          status: status as any,
          progress: typeof data.progress === 'number' ? data.progress : undefined,
          currentStep: typeof data.currentStep === 'string' ? data.currentStep : null,
          errorMessage: typeof data.error === 'string' ? data.error : null,
        });
        if (isTerminal(status)) {
          stopPolling();
          clearActiveExecution();
        }
      }
    }
  }, [updateExecutionStatus, clearActiveExecution, stopPolling]);

  const { connected, reconnecting } = useExecutionWebSocket({
    executionId,
    token,
    onMessage: handleWsMessage,
  });

  // Start polling when WS is disconnected, stop it when WS reconnects
  useEffect(() => {
    if (!executionId) {
      stopPolling();
      return;
    }

    if (!connected) {
      if (!pollTimerRef.current) {
        pollTimerRef.current = setInterval(() => pollOnce(executionId), POLL_INTERVAL_MS);
      }
    } else {
      stopPolling();
    }

    return () => stopPolling();
  }, [executionId, connected, pollOnce, stopPolling]);

  return { connected, reconnecting, activeExecution };
}
