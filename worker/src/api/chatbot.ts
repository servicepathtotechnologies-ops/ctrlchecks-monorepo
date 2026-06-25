// Chatbot Route
// Worker API handler
// Now uses Ollama for AI responses

import { Request, Response } from 'express';
import { readFileSync } from 'fs';
import { join } from 'path';
import { config } from '../core/config';
import { LLMAdapter } from '../shared/llm-adapter';
import { logger } from '../core/logger';

interface KnowledgeBase {
  product: {
    name: string;
    category: string;
    tagline: string;
    description: string;
  };
  features: {
    core: Array<{ name: string; description: string }>;
    technical: string[];
  };
  pricing: {
    plans: Array<{
      name: string;
      price: string;
      runs: string;
      features: string[];
    }>;
    note: string;
  };
  differentiation: {
    vs_zapier: string[];
    vs_n8n: string[];
    unique: string[];
  };
  use_cases: Array<{
    category: string;
    examples: string[];
  }>;
  faqs: Array<{
    question: string;
    answer: string;
    keywords: string[];
  }>;
  escalation: {
    triggers: string[];
    response: string;
  };
  personality: {
    tone: string;
    style: string;
    greeting: string;
    fallback: string;
    encouragement: string[];
  };
  conversion: {
    suggestions: {
      [key: string]: string[];
    };
    cta_phrases: string[];
  };
  contact: {
    support_email: string;
    sales_email: string;
    website: string;
  };
}

// Load knowledge base - cached
let knowledgeCache: KnowledgeBase | null = null;

function loadKnowledge(): KnowledgeBase {
  if (knowledgeCache) {
    return knowledgeCache;
  }

  try {
    const knowledgePath = join(__dirname, '../data/website_knowledge.json');
    const knowledgeText = readFileSync(knowledgePath, 'utf-8');
    knowledgeCache = JSON.parse(knowledgeText) as KnowledgeBase;
    logger.info("Knowledge base loaded and parsed successfully");
    return knowledgeCache;
  } catch (error) {
    logger.error("Failed to load knowledge base:", error);
    return getFallbackKnowledge();
  }
}

function getFallbackKnowledge(): KnowledgeBase {
  return {
    product: {
      name: "CtrlChecks",
      category: "Visual AI Workflow Automation",
      tagline: "Build automations that think. Connect anything. Automate everything.",
      description: "CtrlChecks is an AI-native workflow automation platform that lets you visually build workflows to connect apps, automate tasks, and deploy AI-powered automations without coding.",
    },
    features: {
      core: [
        { name: "Drag-and-Drop Workflow Builder", description: "Visually design workflows with an intuitive interface." },
        { name: "300+ App Integrations", description: "Connect with popular apps like Google Workspace, Slack, Salesforce, and more." },
        { name: "Built-in AI Nodes", description: "Access GPT, Gemini, and custom AI models directly in your workflows." },
        { name: "Multiple Deployment Options", description: "Deploy workflows as APIs, chatbots, or scheduled jobs." },
        { name: "No-Code with Code Support", description: "Start with no-code simplicity, but add custom code when needed." },
      ],
      technical: [],
    },
    pricing: {
      plans: [
        { name: "Free", price: "$0", runs: "500 runs/month", features: ["All core features", "Community support"] },
        { name: "Pro", price: "$29/month", runs: "10,000 runs/month", features: ["Everything in Free", "Priority support"] },
        { name: "Business", price: "$99/month", runs: "100,000 runs/month", features: ["Everything in Pro", "Team collaboration"] },
        { name: "Enterprise", price: "Custom", runs: "Unlimited", features: ["Everything in Business", "Self-hosting option"] },
      ],
      note: "All plans include access to all integrations and the workflow builder.",
    },
    differentiation: {
      vs_zapier: ["More flexible workflow logic", "AI-native architecture", "Better pricing"],
      vs_n8n: ["More intuitive user interface", "Better AI integration", "Easier onboarding"],
      unique: ["AI capabilities built into every node", "Deploy workflows as chatbots"],
    },
    use_cases: [
      { category: "Marketing", examples: ["Lead enrichment", "Social media automation"] },
      { category: "Sales", examples: ["CRM synchronization", "Lead scoring"] },
      { category: "Operations", examples: ["Customer onboarding", "Automated reporting"] },
    ],
    faqs: [
      {
        question: "Is there a free plan?",
        answer: "Yes! Our free plan includes 500 runs per month, which is perfect for getting started, testing workflows, and building small automations. You get access to all core features, including the drag-and-drop builder and all integrations.",
        keywords: ["free", "plan", "pricing", "cost"],
      },
      {
        question: "What can I build with CtrlChecks?",
        answer: "You can build almost any automation! Common examples include lead enrichment workflows, customer onboarding processes, social media posting schedules, automated reporting systems, data synchronization between apps, and AI-powered content generation.",
        keywords: ["build", "create", "make", "do", "automate"],
      },
      {
        question: "How is CtrlChecks different from Zapier or n8n?",
        answer: "CtrlChecks is AI-native, meaning AI capabilities are built into every workflow node. Compared to n8n, we have a more intuitive interface. Compared to Zapier, we offer more flexibility in workflow logic and better pricing for high-volume users.",
        keywords: ["different", "vs", "compare", "zapier", "n8n"],
      },
    ],
    escalation: {
      triggers: ["enterprise pricing", "custom pricing", "security audit", "compliance", "talk to sales"],
      response: "I'd love to help, but for detailed information about that, I think it's best if you speak directly with our team. Please contact our sales team at sales@ctrlchecks.com!",
    },
    personality: {
      tone: "friendly, helpful, professional, approachable",
      style: "conversational but clear, encouraging, non-pushy",
      greeting: "Hi there! 👋 I'm here to help you learn about CtrlChecks.",
      fallback: "Hmm, I'm not sure I have that specific information right now. But I'd be happy to help with other questions about CtrlChecks! You could ask about our features, pricing, how to get started, or what you can build.",
      encouragement: ["Great question!", "I'd be happy to help!", "That's a common question!"],
    },
    conversion: {
      suggestions: {
        pricing: ["Try free plan", "View all plans"],
        features: ["View templates", "Watch demo"],
        getting_started: ["Sign up free", "Try free plan", "Watch demo"],
        general: ["Try free plan", "View templates", "Watch demo"],
      },
      cta_phrases: ["Ready to get started?", "Want to try it out?"],
    },
    contact: {
      support_email: "support@ctrlchecks.com",
      sales_email: "sales@ctrlchecks.com",
      website: "https://ctrlchecks.com",
    },
  };
}

function shouldEscalate(message: string, knowledge: KnowledgeBase): boolean {
  const lowerMessage = message.toLowerCase();
  return knowledge.escalation.triggers.some((trigger) =>
    lowerMessage.includes(trigger.toLowerCase())
  );
}

function findMatchingFAQ(
  message: string,
  knowledge: KnowledgeBase
): { question: string; answer: string } | null {
  const lowerMessage = message.toLowerCase();

  for (const faq of knowledge.faqs) {
    if (faq.keywords.some((keyword) => lowerMessage.includes(keyword))) {
      return { question: faq.question, answer: faq.answer };
    }
  }

  for (const faq of knowledge.faqs) {
    const questionWords = faq.question.toLowerCase().split(/\s+/);
    if (questionWords.some((word) => word.length > 3 && lowerMessage.includes(word))) {
      return { question: faq.question, answer: faq.answer };
    }
  }

  return null;
}

function getSuggestions(
  message: string,
  knowledge: KnowledgeBase
): string[] {
  const lowerMessage = message.toLowerCase();

  if (lowerMessage.includes("pricing") || lowerMessage.includes("plan") || lowerMessage.includes("cost")) {
    return knowledge.conversion.suggestions.pricing || [];
  }

  if (
    lowerMessage.includes("feature") ||
    lowerMessage.includes("what can") ||
    lowerMessage.includes("build")
  ) {
    return knowledge.conversion.suggestions.features || [];
  }

  if (
    lowerMessage.includes("start") ||
    lowerMessage.includes("begin") ||
    lowerMessage.includes("get started")
  ) {
    return knowledge.conversion.suggestions.getting_started || [];
  }

  return knowledge.conversion.suggestions.general || [];
}

function buildPrompt(
  userMessage: string,
  knowledge: KnowledgeBase
): string {
  const faqSection = knowledge.faqs
    .map((faq) => `Q: ${faq.question}\nA: ${faq.answer}`)
    .join("\n\n");

  const featuresSection = knowledge.features.core
    .map((f) => `- ${f.name}: ${f.description}`)
    .join("\n");

  const pricingSection = knowledge.pricing.plans
    .map(
      (p) =>
        `- ${p.name}: ${p.price} - ${p.runs} - Features: ${p.features.join(", ")}`
    )
    .join("\n");

  return `You are a friendly and helpful website chatbot for ${knowledge.product.name}, an AI-native workflow automation platform.

YOUR PERSONALITY:
- ${knowledge.personality.tone}
- ${knowledge.personality.style}
- Be conversational, warm, and encouraging
- Make users feel comfortable asking anything
- Never be pushy or salesy

PRODUCT INFORMATION:
Name: ${knowledge.product.name}
Category: ${knowledge.product.category}
Tagline: ${knowledge.product.tagline}
Description: ${knowledge.product.description}

KEY FEATURES:
${featuresSection}

PRICING PLANS:
${pricingSection}
Note: ${knowledge.pricing.note}

FREQUENTLY ASKED QUESTIONS:
${faqSection}

DIFFERENTIATION:
vs Zapier: ${knowledge.differentiation.vs_zapier.join(", ")}
vs n8n: ${knowledge.differentiation.vs_n8n.join(", ")}
Unique: ${knowledge.differentiation.unique.join(", ")}

USE CASES:
${knowledge.use_cases.map(uc => `${uc.category}: ${uc.examples.join(", ")}`).join("\n")}

CRITICAL RULES:
1. Answer ONLY using the information provided above
2. If you don't have the exact information, use this fallback: "${knowledge.personality.fallback}"
3. Keep responses concise (2-3 short paragraphs max)
4. Use friendly, conversational language
5. Don't repeat information unnecessarily
6. Include at most one follow-up question if natural
7. Make users feel welcome and free to ask anything
8. Never hallucinate or make up information
9. If asked about enterprise/custom/compliance topics, suggest contacting sales

USER QUESTION: ${userMessage}

Provide a helpful, friendly response based on the information above.`;
}

export default async function chatbotHandler(req: Request, res: Response) {
  try {
    // Load knowledge
    let knowledge: KnowledgeBase;
    try {
      knowledge = loadKnowledge();
    } catch (error) {
      logger.error("Failed to load knowledge:", error);
      knowledge = getFallbackKnowledge();
    }

    const { message } = req.body;

    if (!message || typeof message !== "string" || !message.trim()) {
      return res.status(400).json({ error: "Message is required" });
    }

    const userMessage = message.trim();
    logger.info("Processing message:", userMessage);

    // Check escalation
    if (shouldEscalate(userMessage, knowledge)) {
      const topic = knowledge.escalation.triggers.find((t) =>
        userMessage.toLowerCase().includes(t.toLowerCase())
      ) || "that";
      
      return res.json({
        content: knowledge.escalation.response.replace("{topic}", topic),
        suggestions: ["Contact sales", "View enterprise plans"],
        escalation: true,
      });
    }

    // Try FAQ matching first (faster, more deterministic)
    const matchedFAQ = findMatchingFAQ(userMessage, knowledge);
    
    if (matchedFAQ) {
      const suggestions = getSuggestions(userMessage, knowledge);
      return res.json({
        content: matchedFAQ.answer,
        suggestions,
      });
    }

    // Use Gemini for AI responses
    const fullPrompt = buildPrompt(userMessage, knowledge);
    logger.info("Calling Gemini for chatbot response...");

    try {
      // Use Gemini chat - the buildPrompt function already includes all context
      // We'll use it as a single user message with all the context
      const llmAdapter = new LLMAdapter();
      const geminiResponse = await llmAdapter.chat(
        'gemini',
        [
          {
            role: 'user',
            content: fullPrompt,
          },
        ],
        {
          model: 'gemini-3.5-flash', // Use Flash for fast chatbot responses
          apiKey: config.geminiApiKey,
          temperature: 0.7,
          stream: false,
        }
      );

      logger.info("Gemini response received");
      
      const content = geminiResponse.content?.trim() || knowledge.personality.fallback;
      const suggestions = getSuggestions(userMessage, knowledge);

      return res.json({
        content,
        suggestions,
      });
    } catch (apiError) {
      logger.error("Ollama request failed:", apiError);
      // Fallback to FAQ or default response
      return res.json({
        content: knowledge.personality.fallback,
        suggestions: getSuggestions(userMessage, knowledge),
      });
    }
  } catch (error) {
    logger.error("Chatbot error:", error);
    return res.status(500).json({
      content:
        "Sorry, I'm having trouble responding right now. Please try again or contact our support team at support@ctrlchecks.com.",
      suggestions: [],
    });
  }
}
