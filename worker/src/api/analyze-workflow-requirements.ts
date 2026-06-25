// Analyze Workflow Requirements Route - uses Gemini (GEMINI_API_KEY)

import { Request, Response } from 'express';
import { geminiOrchestrator } from '../services/ai/gemini-orchestrator';
import { logger } from '../core/logger';

export default async function analyzeWorkflowRequirements(req: Request, res: Response) {
  try {
    const { prompt } = req.body;

    if (!prompt) {
      return res.status(400).json({ error: 'Prompt is required' });
    }

    const systemPrompt = `
      You are an expert workflow requirements analyzer.
      Your task is to analyze a user's natural language request for a workflow and identify specific configuration values that are required to build it.
      
      Examples:
      - "Read from Google Sheet" -> Requires: "google_sheet_url" (URL) and "sheet_name" (Tab Name)
      - "Send message to Slack" -> Requires: Slack Webhook URL or Channel ID
      - "Email me everyday" -> Requires: Email Address
      
      Specific Rules:
      - For Google Sheets: ALWAYS ask for "google_sheet_url" and "sheet_name". Do NOT ask for "spreadsheet_id" directly as it's hard for users to find.
      - For Google Docs: ALWAYS ask for "google_doc_url" (the full Google Docs URL). Do NOT ask for "document_id" directly. Users should paste the full URL like: https://docs.google.com/document/d/DOCUMENT_ID/edit
      - For others: Ask for the most user-friendly identifier.
      
      Identify ONLY essential external identifiers, secrets, or specific configuration values that the user MUST provide for the workflow to function.
      Do NOT ask for generic things like "Workflow Name" or "Description".
      Do NOT ask for internal logic variables unless absolutely ambiguous.
      
      Return a JSON object with a "requirements" array.
      Each requirement should have:
      - key: string (variable name, e.g., "google_sheet_id")
      - label: string (user friendly label, e.g., "Google Sheet ID")
      - type: "text" | "number" | "select"
      - description: string (brief help text)
      - required: boolean (usually true)
      
      If no specific requirements are found, return { "requirements": [] }.
      
      Respond with VALID JSON only.
    `;

    logger.info("Analyzing workflow requirements with Gemini");
    const response = await geminiOrchestrator.processRequest('workflow-analysis', {
      prompt: `${systemPrompt}\n\nUser Request: ${prompt}`,
      temperature: 0.3,
    });
    
    // Convert response to expected format
    const responseContent = typeof response === 'string' ? response : JSON.stringify(response);

    let result;
    try {
      // Extract JSON from potential code blocks
      let jsonText = responseContent.trim();
      if (jsonText.includes('```json')) {
        jsonText = jsonText.split('```json')[1].split('```')[0].trim();
      } else if (jsonText.includes('```')) {
        jsonText = jsonText.split('```')[1].split('```')[0].trim();
      }

      result = JSON.parse(jsonText);
    } catch (e) {
      logger.error("Failed to parse JSON", responseContent);
      result = { requirements: [] }; // Fallback
    }

    return res.json(result);
  } catch (error) {
    logger.error('Error:', error);
    const errorMessage = error instanceof Error ? error.message : String(error);
    return res.status(500).json({ error: errorMessage });
  }
}
