// Chatbot Chat UI API
// Serves N8N-style chat UI for chatbot workflows

import { Request, Response } from 'express';
import { chatbotPageGenerator } from '../services/chatbot-page-generator';
import { getDbClient } from '../core/database/aws-db-client';
import { logger } from '../core/logger';

/**
 * Serve chat UI for a chatbot workflow
 * GET /workflows/:workflowId/chat
 */
export async function serveChatbotChat(req: Request, res: Response) {
  try {
    const { workflowId } = req.params;
    
    if (!workflowId) {
      return res.status(400).json({ error: 'Workflow ID is required' });
    }

    const db = getDbClient();
    
    // Fetch workflow
    const { data: workflow, error } = await db
      .from('workflows')
      .select('id, name, nodes, edges, is_public, auth_required, setup_completed, metadata')
      .eq('id', workflowId)
      .single();

    if (error || !workflow) {
      return res.status(404).send(`
        <html>
          <body style="font-family: sans-serif; padding: 40px; text-align: center;">
            <h1>Workflow Not Found</h1>
            <p>The requested chatbot workflow does not exist.</p>
          </body>
        </html>
      `);
    }

    const { isSetupPending } = await import('./workflow-setup-lifecycle');
    if (isSetupPending(workflow)) {
      return res.status(404).send(`
        <html>
          <body style="font-family: sans-serif; padding: 40px; text-align: center;">
            <h1>Workflow Not Found</h1>
            <p>The requested chatbot workflow does not exist.</p>
          </body>
        </html>
      `);
    }

    // Check if workflow is a chatbot workflow
    if (!chatbotPageGenerator.isChatbotWorkflow(workflow)) {
      return res.status(400).send(`
        <html>
          <body style="font-family: sans-serif; padding: 40px; text-align: center;">
            <h1>Not a Chatbot Workflow</h1>
            <p>This workflow is not configured as a chatbot workflow.</p>
          </body>
        </html>
      `);
    }

    // Generate chat UI
    const baseUrl = process.env.PUBLIC_BASE_URL || `http://${req.get('host')}`;
    const endpointUrl = chatbotPageGenerator.getChatbotEndpointUrl(workflowId, baseUrl);

    // Check if memory is enabled
    const hasMemoryNode = (workflow.nodes || []).some((node: any) => {
      const nodeType = node.type || node.data?.type || '';
      return nodeType.toLowerCase().includes('memory');
    });

    // Check if auth is enabled
    const authEnabled = workflow.is_public === false || workflow.auth_required === true;

    const html = generateN8NStyleChatUI({
      workflowId: workflow.id,
      workflowName: workflow.name || 'Chatbot',
      endpointUrl,
      memoryEnabled: hasMemoryNode,
      authEnabled,
    });

    res.setHeader('Content-Type', 'text/html');
    res.send(html);
  } catch (error) {
    logger.error('Error serving chatbot chat:', error);
    res.status(500).send(`
      <html>
        <body style="font-family: sans-serif; padding: 40px; text-align: center;">
          <h1>Error</h1>
          <p>An error occurred while loading the chat UI.</p>
        </body>
      </html>
    `);
  }
}

/**
 * Generate N8N-style chat UI
 */
function generateN8NStyleChatUI(config: {
  workflowId: string;
  workflowName: string;
  endpointUrl: string;
  memoryEnabled: boolean;
  authEnabled: boolean;
}): string {
  const { workflowId, workflowName, endpointUrl, memoryEnabled } = config;

  return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${escapeHtml(workflowName)} - Chat</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: #f5f5f5;
            height: 100vh;
            display: flex;
            flex-direction: column;
        }

        .chat-header {
            background: #fff;
            border-bottom: 1px solid #e0e0e0;
            padding: 16px 24px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        .chat-header h1 {
            font-size: 18px;
            font-weight: 600;
            color: #333;
        }

        .chat-header .status {
            font-size: 12px;
            color: #666;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #4caf50;
        }

        .chat-container {
            flex: 1;
            display: flex;
            flex-direction: column;
            max-width: 1200px;
            width: 100%;
            margin: 0 auto;
            background: #fff;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        }

        .chat-messages {
            flex: 1;
            overflow-y: auto;
            padding: 24px;
            background: #fafafa;
        }

        .message {
            margin-bottom: 16px;
            display: flex;
            animation: fadeIn 0.3s ease-in;
        }

        @keyframes fadeIn {
            from {
                opacity: 0;
                transform: translateY(10px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        .message.user {
            justify-content: flex-end;
        }

        .message.bot {
            justify-content: flex-start;
        }

        .message-content {
            max-width: 70%;
            padding: 12px 16px;
            border-radius: 8px;
            word-wrap: break-word;
            line-height: 1.5;
            font-size: 14px;
        }

        .message.user .message-content {
            background: #0066cc;
            color: white;
        }

        .message.bot .message-content {
            background: #fff;
            color: #333;
            border: 1px solid #e0e0e0;
        }

        .chat-input-container {
            padding: 16px 24px;
            background: #fff;
            border-top: 1px solid #e0e0e0;
            display: flex;
            gap: 12px;
        }

        .chat-input {
            flex: 1;
            padding: 12px 16px;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            font-size: 14px;
            outline: none;
            transition: border-color 0.2s;
        }

        .chat-input:focus {
            border-color: #0066cc;
        }

        .send-button {
            padding: 12px 24px;
            background: #0066cc;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 14px;
            font-weight: 600;
            cursor: pointer;
            transition: background-color 0.2s;
        }

        .send-button:hover:not(:disabled) {
            background: #0052a3;
        }

        .send-button:disabled {
            opacity: 0.6;
            cursor: not-allowed;
        }

        .loading-indicator {
            display: none;
            padding: 12px;
            text-align: center;
            color: #666;
            font-size: 14px;
        }

        .loading-indicator.active {
            display: block;
        }

        .error-message {
            background: #fee;
            color: #c33;
            padding: 12px 16px;
            border-radius: 8px;
            margin: 8px 24px;
            border: 1px solid #fcc;
            font-size: 14px;
        }

        .empty-state {
            text-align: center;
            color: #999;
            padding: 60px 20px;
        }

        .empty-state-icon {
            font-size: 48px;
            margin-bottom: 16px;
        }
    </style>
</head>
<body>
    <div class="chat-container">
        <div class="chat-header">
            <h1>${escapeHtml(workflowName)}</h1>
            <div class="status">
                <span class="status-dot"></span>
                <span>Online</span>
            </div>
        </div>

        <div class="chat-messages" id="chatMessages">
            <div class="empty-state">
                <div class="empty-state-icon">💬</div>
                <div>Start a conversation</div>
            </div>
        </div>

        <div class="loading-indicator" id="loadingIndicator">
            Thinking...
        </div>

        <div class="chat-input-container">
            <input
                type="text"
                class="chat-input"
                id="chatInput"
                placeholder="Type your message..."
                autocomplete="off"
            />
            <button class="send-button" id="sendButton">Send</button>
        </div>
    </div>

    <script>
        const endpointUrl = ${JSON.stringify(endpointUrl)};
        const workflowId = ${JSON.stringify(workflowId)};
        const memoryEnabled = ${JSON.stringify(memoryEnabled)};

        let sessionId = localStorage.getItem(\`chatbot_session_\${workflowId}\`) || 'session_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
        localStorage.setItem(\`chatbot_session_\${workflowId}\`, sessionId);

        const chatMessages = document.getElementById('chatMessages');
        const chatInput = document.getElementById('chatInput');
        const sendButton = document.getElementById('sendButton');
        const loadingIndicator = document.getElementById('loadingIndicator');

        function addMessage(text, isUser) {
            const emptyState = chatMessages.querySelector('.empty-state');
            if (emptyState) {
                emptyState.remove();
            }

            const messageDiv = document.createElement('div');
            messageDiv.className = 'message ' + (isUser ? 'user' : 'bot');

            const contentDiv = document.createElement('div');
            contentDiv.className = 'message-content';
            contentDiv.textContent = text;

            messageDiv.appendChild(contentDiv);
            chatMessages.appendChild(messageDiv);
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }

        function showError(message) {
            const errorDiv = document.createElement('div');
            errorDiv.className = 'error-message';
            errorDiv.textContent = message || 'An error occurred. Please try again.';
            chatMessages.appendChild(errorDiv);
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }

        async function sendMessage() {
            const message = chatInput.value.trim();
            if (!message) return;

            // Add user message
            addMessage(message, true);
            chatInput.value = '';
            sendButton.disabled = true;
            loadingIndicator.classList.add('active');

            try {
                const response = await fetch(endpointUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        session_id: sessionId,
                        message: message
                    })
                });

                if (!response.ok) {
                    throw new Error('Failed to get response');
                }

                const data = await response.json();

                // Extract reply from response
                let reply = '';
                if (data.reply) {
                    reply = data.reply;
                } else if (data.message) {
                    reply = data.message;
                } else if (data.response) {
                    reply = data.response;
                } else if (typeof data === 'string') {
                    reply = data;
                } else {
                    reply = JSON.stringify(data);
                }

                addMessage(reply, false);
            } catch (error) {
                logger.error('Chat error:', error);
                showError('Failed to send message. Please try again.');
            } finally {
                sendButton.disabled = false;
                loadingIndicator.classList.remove('active');
                chatInput.focus();
            }
        }

        sendButton.addEventListener('click', sendMessage);

        chatInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                sendMessage();
            }
        });

        // Focus input on load
        chatInput.focus();
    </script>
</body>
</html>`;
}

function escapeHtml(text: string): string {
  const map: Record<string, string> = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;',
  };
  return text.replace(/[&<>"']/g, (m) => map[m]);
}
