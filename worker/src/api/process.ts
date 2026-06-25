// Process Route - Direct proxy to FastAPI backend
// This endpoint allows frontend to call /process directly on the worker
// which then proxies to the FastAPI backend

import { Request, Response } from 'express';
import { config } from '../core/config';
import { logger } from '../core/logger';

// Get Python backend URL (FastAPI backend)
// Use FASTAPI_OLLAMA_URL from config, or fallback to PYTHON_BACKEND_URL, or default to port 8000
const PYTHON_BACKEND_URL = process.env.FASTAPI_OLLAMA_URL || 
                           process.env.PYTHON_BACKEND_URL || 
                           'http://localhost:8000';

async function proxyToPythonBackend(payload: any): Promise<{ status: number; text: string }> {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 180000); // 3 minute timeout

    logger.info(`📤 Proxying /process request to Python backend at ${PYTHON_BACKEND_URL}/process...`);

    const response = await fetch(`${PYTHON_BACKEND_URL}/process`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    const responseText = await response.text();
    
    return { status: response.status, text: responseText };
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      return {
        status: 504,
        text: JSON.stringify({
          success: false,
          error: "Request timeout. Python backend may be slow or unavailable.",
          details: "AI processing can take 10-30 seconds. Please try again."
        })
      };
    }
    
    logger.error("Error proxying to Python backend:", error);
    return {
      status: 502,
      text: JSON.stringify({
        success: false,
        error: `Failed to connect to Python backend: ${error instanceof Error ? error.message : String(error)}`,
        details: `Ensure Python backend is running at ${PYTHON_BACKEND_URL} and PYTHON_BACKEND_URL is configured correctly.`
      })
    };
  }
}

export default async function processRoute(req: Request, res: Response) {
  if (req.method !== "POST") {
    return res.status(405).json({ success: false, error: "Method not allowed. Use POST." });
  }

  try {
    const body = req.body;

    // Proxy directly to Python backend without validation
    // The FastAPI backend will handle validation
    const proxyResponse = await proxyToPythonBackend(body);
    const responseText = proxyResponse.text;
    const status = proxyResponse.status;
    
    // Try to parse as JSON, if it fails, send as text
    try {
      const jsonResponse = JSON.parse(responseText);
      res.status(status).json(jsonResponse);
    } catch {
      // If not JSON, send as text (for error responses)
      res.status(status).send(responseText);
    }
  } catch (error) {
    logger.error("Unexpected error in /process route:", error);
    return res.status(500).json({
      success: false,
      error: "Internal server error",
      details: error instanceof Error ? error.message : String(error),
    });
  }
}
