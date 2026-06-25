import { Request, Response } from 'express';
import { connectorRegistry, Connector } from '../services/connectors/connector-registry';
import { queryAsService } from '../core/database/db-pool';
import { getCredentialVault } from '../services/credential-vault';
import { formatCredentialError, resolveCredentialDryRun } from '../services/credential-resolver';
import { requiredScopesForProvider } from '../services/credential-scope-registry';
import { logger } from '../core/logger';

export type CatalogAuthType =
  | 'oauth'
  | 'api_key'
  | 'webhook'
  | 'token'
  | 'basic_auth'
  | 'runtime'
  | 'manual_oauth_token';

export type OAuthFlow = 'backend_redirect' | 'frontend_code_exchange' | 'existing_connection' | 'manual_token';

export interface ConnectionCredentialField {
  name: string;
  label: string;
  type: 'text' | 'password' | 'url' | 'number';
  required: boolean;
  placeholder?: string;
  helpCategory?: string;
  docsUrl?: string;
  exampleValue?: string;
}

export interface ConnectionCatalogEntry {
  provider: string;
  vaultKey: string;
  displayName: string;
  authType: CatalogAuthType;
  credentialFields: ConnectionCredentialField[];
  nodeTypes: string[];
  connectorIds: string[];
  statusTable?: string;
  statusProvider?: string;
  connectUrl?: string;
  disconnectUrl?: string;
  callbackUrl?: string;
  frontendReturnUrl?: string;
  scopes: string[];
  requiredEnv: string[];
  configured: boolean;
  oauthImplemented: boolean;
  flow: OAuthFlow;
  setupHint?: string;
}

interface OAuthProviderMetadata {
  provider: string;
  vaultKey: string;
  displayName: string;
  clientEnv: string[];
  secretEnv?: string[];
  redirectEnv?: string;
  callbackPath?: string;
  frontendReturnPath?: string;
  statusTable?: string;
  statusProvider?: string;
  connectUrl: string;
  disconnectUrl?: string;
  scopes?: string[];
  flow: OAuthFlow;
}

const DASHBOARD_OAUTH: Record<string, OAuthProviderMetadata> = {
  google: {
    provider: 'google',
    vaultKey: 'google',
    displayName: 'Google',
    clientEnv: ['GOOGLE_OAUTH_CLIENT_ID'],
    secretEnv: ['GOOGLE_OAUTH_CLIENT_SECRET'],
    redirectEnv: 'GOOGLE_OAUTH_REDIRECT_URI',
    callbackPath: '/api/oauth/google/callback',
    frontendReturnPath: '/auth/google/callback',
    statusTable: 'google_oauth_tokens',
    connectUrl: '/api/oauth/google/start',
    disconnectUrl: '/api/connections/google',
    scopes: [
      'email',
      'profile',
      'https://www.googleapis.com/auth/gmail.send',
      'https://www.googleapis.com/auth/gmail.readonly',
      'https://www.googleapis.com/auth/drive',
      'https://www.googleapis.com/auth/documents',
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/calendar.events',
      'https://www.googleapis.com/auth/contacts',
      'https://www.googleapis.com/auth/tasks',
      'https://www.googleapis.com/auth/bigquery',
    ],
    flow: 'backend_redirect',
  },
  linkedin: {
    provider: 'linkedin',
    vaultKey: 'linkedin',
    displayName: 'LinkedIn',
    clientEnv: ['LINKEDIN_CLIENT_ID'],
    secretEnv: ['LINKEDIN_CLIENT_SECRET'],
    redirectEnv: 'LINKEDIN_OAUTH_REDIRECT_URI',
    callbackPath: '/api/oauth/linkedin/callback',
    frontendReturnPath: '/auth/linkedin/callback',
    statusTable: 'linkedin_oauth_tokens',
    connectUrl: '/api/oauth/linkedin/start',
    disconnectUrl: '/api/connections/linkedin',
    scopes: ['openid', 'profile', 'email', 'w_member_social'],
    flow: 'backend_redirect',
  },
  github: {
    provider: 'github',
    vaultKey: 'github',
    displayName: 'GitHub',
    clientEnv: ['GITHUB_CLIENT_ID'],
    secretEnv: ['GITHUB_CLIENT_SECRET'],
    redirectEnv: 'GITHUB_OAUTH_REDIRECT_URI',
    callbackPath: '/api/oauth/github/callback',
    frontendReturnPath: '/auth/github/callback',
    statusTable: 'social_tokens',
    statusProvider: 'github',
    connectUrl: '/api/oauth/github/start',
    disconnectUrl: '/api/connections/github/disconnect',
    scopes: ['user:email', 'read:user'],
    flow: 'backend_redirect',
  },
  facebook: {
    provider: 'facebook',
    vaultKey: 'facebook',
    displayName: 'Facebook',
    clientEnv: ['META_APP_ID', 'FACEBOOK_APP_ID'],
    secretEnv: ['META_APP_SECRET', 'FACEBOOK_APP_SECRET'],
    redirectEnv: 'FACEBOOK_OAUTH_REDIRECT_URI',
    callbackPath: '/api/oauth/facebook/callback',
    frontendReturnPath: '/auth/facebook/callback',
    statusTable: 'social_tokens',
    statusProvider: 'facebook',
    connectUrl: '/api/oauth/facebook/start',
    disconnectUrl: '/api/connections/facebook/disconnect',
    scopes: ['pages_show_list', 'pages_read_engagement', 'pages_manage_posts', 'public_profile', 'email'],
    flow: 'backend_redirect',
  },
  notion: {
    provider: 'notion',
    vaultKey: 'notion',
    displayName: 'Notion',
    clientEnv: ['NOTION_OAUTH_CLIENT_ID'],
    secretEnv: ['NOTION_OAUTH_CLIENT_SECRET'],
    redirectEnv: 'NOTION_OAUTH_REDIRECT_URI',
    callbackPath: '/auth/notion/callback',
    frontendReturnPath: '/auth/notion/callback',
    statusTable: 'notion_oauth_tokens',
    connectUrl: '/api/oauth/notion/authorize',
    disconnectUrl: '/api/connections/notion',
    scopes: [],
    flow: 'frontend_code_exchange',
  },
  twitter: {
    provider: 'twitter',
    vaultKey: 'twitter',
    displayName: 'Twitter / X',
    clientEnv: ['TWITTER_OAUTH_CLIENT_ID'],
    secretEnv: ['TWITTER_OAUTH_CLIENT_SECRET'],
    redirectEnv: 'TWITTER_OAUTH_REDIRECT_URI',
    callbackPath: '/auth/twitter/callback',
    frontendReturnPath: '/auth/twitter/callback',
    statusTable: 'twitter_oauth_tokens',
    connectUrl: '/api/oauth/twitter/authorize',
    disconnectUrl: '/api/connections/twitter',
    scopes: ['tweet.read', 'tweet.write', 'users.read', 'offline.access'],
    flow: 'frontend_code_exchange',
  },
  instagram: {
    provider: 'instagram',
    vaultKey: 'instagram',
    displayName: 'Instagram',
    clientEnv: ['META_APP_ID', 'INSTAGRAM_APP_ID', 'FACEBOOK_APP_ID'],
    secretEnv: ['META_APP_SECRET', 'INSTAGRAM_APP_SECRET', 'FACEBOOK_APP_SECRET'],
    redirectEnv: 'INSTAGRAM_OAUTH_REDIRECT_URI',
    callbackPath: '/auth/instagram/callback',
    frontendReturnPath: '/auth/instagram/callback',
    statusTable: 'instagram_oauth_tokens',
    connectUrl: '/api/oauth/instagram/authorize',
    disconnectUrl: '/api/connections/instagram',
    scopes: ['instagram_basic', 'instagram_content_publish', 'pages_show_list', 'pages_read_engagement'],
    flow: 'frontend_code_exchange',
  },
  salesforce: {
    provider: 'salesforce',
    vaultKey: 'salesforce',
    displayName: 'Salesforce',
    clientEnv: ['SALESFORCE_CLIENT_ID'],
    secretEnv: ['SALESFORCE_CLIENT_SECRET'],
    redirectEnv: 'SALESFORCE_OAUTH_REDIRECT_URI',
    callbackPath: '/auth/salesforce/callback',
    frontendReturnPath: '/auth/salesforce/callback',
    statusTable: 'salesforce_oauth_tokens',
    connectUrl: '/api/oauth/salesforce/authorize',
    disconnectUrl: '/api/connections/salesforce',
    scopes: ['api', 'refresh_token'],
    flow: 'frontend_code_exchange',
  },
  whatsapp: {
    provider: 'whatsapp',
    vaultKey: 'whatsapp',
    displayName: 'WhatsApp',
    clientEnv: ['META_APP_ID', 'FACEBOOK_APP_ID', 'WHATSAPP_APP_ID'],
    secretEnv: ['META_APP_SECRET', 'FACEBOOK_APP_SECRET', 'WHATSAPP_APP_SECRET'],
    redirectEnv: 'WHATSAPP_OAUTH_REDIRECT_URI',
    callbackPath: '/auth/whatsapp/callback',
    frontendReturnPath: '/auth/whatsapp/callback',
    statusTable: 'whatsapp_oauth_tokens',
    connectUrl: '/api/oauth/whatsapp/authorize',
    disconnectUrl: '/api/connections/whatsapp',
    scopes: ['business_management', 'whatsapp_business_management', 'whatsapp_business_messaging'],
    flow: 'frontend_code_exchange',
  },
  zoho: {
    provider: 'zoho',
    vaultKey: 'zoho',
    displayName: 'Zoho CRM',
    clientEnv: ['ZOHO_CLIENT_ID', 'ZOHO_OAUTH_CLIENT_ID'],
    secretEnv: ['ZOHO_CLIENT_SECRET', 'ZOHO_OAUTH_CLIENT_SECRET'],
    redirectEnv: 'ZOHO_OAUTH_REDIRECT_URI',
    callbackPath: '/auth/zoho/callback',
    frontendReturnPath: '/auth/zoho/callback',
    statusTable: 'zoho_oauth_tokens',
    connectUrl: '/api/connections/zoho/connect',
    disconnectUrl: '/api/connections/zoho',
    scopes: ['ZohoCRM.modules.ALL'],
    flow: 'existing_connection',
  },
};

const FIELD_OVERRIDES: Record<string, ConnectionCredentialField[]> = {
  smtp: [
    { name: 'host',     label: 'Host',       type: 'text',     required: true,  helpCategory: 'smtp_host' },
    { name: 'port',     label: 'Port',       type: 'number',   required: false, placeholder: '587', helpCategory: 'port' },
    { name: 'username', label: 'Username',   type: 'text',     required: true,  helpCategory: 'smtp_username' },
    { name: 'password', label: 'Password',   type: 'password', required: true,  helpCategory: 'smtp_password' },
    { name: 'from',     label: 'From Email', type: 'text',     required: false, helpCategory: 'email_address' },
  ],
  discord: [
    { name: 'botToken',   label: 'Bot Token',   type: 'password', required: false, helpCategory: 'generic_token', docsUrl: 'https://discord.com/developers/applications' },
    { name: 'webhookUrl', label: 'Webhook URL', type: 'url',      required: false, helpCategory: 'webhook_url',   docsUrl: 'https://support.discord.com/hc/en-us/articles/228383668' },
  ],
  telegram: [
    { name: 'botToken', label: 'Bot Token', type: 'password', required: true, helpCategory: 'generic_token', docsUrl: 'https://core.telegram.org/bots/features#botfather' },
  ],
  mailgun: [
    { name: 'domain', label: 'Domain',     type: 'text',     required: true, helpCategory: 'base_url' },
    { name: 'apiKey',  label: 'API Key',   type: 'password', required: true, helpCategory: 'api_key',      docsUrl: 'https://app.mailgun.com/settings/api_security' },
    { name: 'from',    label: 'From Email',type: 'text',     required: true, helpCategory: 'email_address' },
  ],
  sendgrid: [
    { name: 'apiKey', label: 'API Key',    type: 'password', required: true, helpCategory: 'api_key',      docsUrl: 'https://app.sendgrid.com/settings/api_keys' },
    { name: 'from',   label: 'From Email', type: 'text',     required: true, helpCategory: 'email_address' },
  ],
  hubspot: [
    { name: 'apiKey',      label: 'API Key',           type: 'password', required: false, helpCategory: 'api_key',      docsUrl: 'https://developers.hubspot.com/docs/api/overview' },
    { name: 'accessToken', label: 'Private App Token', type: 'password', required: false, helpCategory: 'bearer_token', docsUrl: 'https://developers.hubspot.com/docs/api/private-apps' },
  ],
  whatsapp: [
    { name: 'accessToken', label: 'Permanent Access Token', type: 'password', required: true, helpCategory: 'bearer_token', docsUrl: 'https://developers.facebook.com/docs/whatsapp/cloud-api/get-started' },
  ],
  shopify: [
    { name: 'shopDomain', label: 'Shop Domain',            type: 'text',     required: true,  placeholder: 'your-store.myshopify.com', helpCategory: 'base_url',  docsUrl: 'https://shopify.dev/docs/apps/auth/admin-app-access-tokens' },
    { name: 'apiKey',     label: 'Admin API Access Token', type: 'password', required: true,  placeholder: 'shpat_...',               helpCategory: 'api_key',   docsUrl: 'https://shopify.dev/docs/apps/auth/admin-app-access-tokens' },
  ],
  freshdesk: [
    { name: 'domain', label: 'Domain',  type: 'text',     required: true, placeholder: 'yourcompany.freshdesk.com', helpCategory: 'base_url', docsUrl: 'https://developers.freshdesk.com/api/' },
    { name: 'apiKey', label: 'API Key', type: 'password', required: true, helpCategory: 'api_key', docsUrl: 'https://developers.freshdesk.com/api/#authentication' },
  ],
  jira: [
    { name: 'baseUrl',  label: 'Base URL',    type: 'url',      required: true,  placeholder: 'https://your-domain.atlassian.net', helpCategory: 'base_url',  docsUrl: 'https://developer.atlassian.com/cloud/jira/platform/rest/v3/' },
    { name: 'email',    label: 'Email',       type: 'text',     required: true,  placeholder: 'user@company.com',                  helpCategory: 'username',  docsUrl: 'https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/' },
    { name: 'apiToken', label: 'API Token',   type: 'password', required: true,  helpCategory: 'api_key',                          docsUrl: 'https://support.atlassian.com/atlassian-account/docs/manage-api-tokens-for-your-atlassian-account/' },
  ],
  gitlab: [
    { name: 'accessToken', label: 'Personal Access Token', type: 'password', required: true,  helpCategory: 'generic_token', docsUrl: 'https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html' },
    { name: 'baseUrl',     label: 'GitLab URL',            type: 'url',      required: false, placeholder: 'https://gitlab.com', helpCategory: 'base_url', docsUrl: 'https://docs.gitlab.com/ee/api/rest/' },
  ],
  airtable: [
    { name: 'apiKey', label: 'Personal Access Token', type: 'password', required: true, helpCategory: 'api_key', docsUrl: 'https://airtable.com/create/tokens' },
  ],
  stripe: [
    { name: 'apiKey',        label: 'Secret Key',      type: 'password', required: true,  placeholder: 'sk_live_...', helpCategory: 'api_key', docsUrl: 'https://dashboard.stripe.com/apikeys' },
    { name: 'webhookSecret', label: 'Webhook Secret',  type: 'password', required: false, placeholder: 'whsec_...',   helpCategory: 'generic_credential', docsUrl: 'https://dashboard.stripe.com/webhooks' },
  ],
  ollama: [
    { name: 'baseUrl', label: 'Base URL', type: 'url', required: false, placeholder: 'http://localhost:11434', helpCategory: 'base_url', docsUrl: 'https://github.com/ollama/ollama/blob/main/docs/api.md' },
  ],
  pipedrive: [
    { name: 'apiToken', label: 'API Token', type: 'password', required: true, helpCategory: 'api_key', docsUrl: 'https://pipedrive.readme.io/docs/how-to-find-the-api-token' },
  ],
  twilio: [
    { name: 'accountSid', label: 'Account SID',  type: 'text',     required: true,  helpCategory: 'account_id',    docsUrl: 'https://console.twilio.com' },
    { name: 'authToken',  label: 'Auth Token',   type: 'password', required: true,  helpCategory: 'generic_token' },
    { name: 'from',       label: 'From Number',  type: 'text',     required: false, helpCategory: 'phone_number' },
  ],
  woocommerce: [
    { name: 'apiKey',    label: 'Consumer Key',    type: 'password', required: true,  helpCategory: 'consumer_key',    docsUrl: 'https://woocommerce.com/document/woocommerce-rest-api/' },
    { name: 'apiSecret', label: 'Consumer Secret', type: 'password', required: true,  helpCategory: 'consumer_secret', docsUrl: 'https://woocommerce.com/document/woocommerce-rest-api/' },
    { name: 'storeUrl',  label: 'Store URL',       type: 'url',      required: false, helpCategory: 'base_url' },
  ],
  activecampaign: [
    { name: 'apiKey', label: 'API Key', type: 'password', required: true, helpCategory: 'api_key',      docsUrl: 'https://www.activecampaign.com/api/overview.php' },
    { name: 'apiUrl', label: 'API URL', type: 'url',      required: true, helpCategory: 'api_endpoint', docsUrl: 'https://www.activecampaign.com/api/overview.php' },
  ],
  jenkins: [
    { name: 'baseUrl',  label: 'Base URL',  type: 'url',      required: true, helpCategory: 'base_url' },
    { name: 'username', label: 'Username',  type: 'text',     required: true, helpCategory: 'username' },
    { name: 'apiToken', label: 'API Token', type: 'password', required: true, helpCategory: 'api_key' },
  ],
  postgresql: [
    { name: 'connectionString', label: 'Connection String', type: 'password', required: true,
      helpCategory: 'connection_string',
      docsUrl: 'https://www.postgresql.org/docs/current/libpq-connect.html',
      exampleValue: 'postgresql://user:pass@host:5432/dbname' },
  ],
  mysql: [
    { name: 'connectionString', label: 'Connection String', type: 'password', required: true,
      helpCategory: 'connection_string',
      exampleValue: 'mysql://user:pass@host:3306/dbname' },
  ],
  mongodb: [
    { name: 'connectionString', label: 'Connection String', type: 'password', required: true,
      helpCategory: 'connection_string',
      docsUrl: 'https://www.mongodb.com/docs/manual/reference/connection-string/',
      exampleValue: 'mongodb+srv://user:pass@cluster.mongodb.net/dbname' },
  ],
  redis: [
    { name: 'connectionString', label: 'Connection String', type: 'password', required: true,
      helpCategory: 'connection_string',
      exampleValue: 'redis://default:pass@host:6379' },
  ],
  supabase: [
    { name: 'url',    label: 'Project URL',         type: 'url',      required: true, helpCategory: 'api_endpoint', docsUrl: 'https://supabase.com/dashboard/project/_/settings/api' },
    { name: 'apiKey', label: 'Service or Anon Key', type: 'password', required: true, helpCategory: 'api_key',      docsUrl: 'https://supabase.com/dashboard/project/_/settings/api' },
  ],
  aws: [
    { name: 'accessKeyId',     label: 'Access Key ID',     type: 'text',     required: true,  helpCategory: 'api_key',            docsUrl: 'https://console.aws.amazon.com/iam/home#/users' },
    { name: 'secretAccessKey', label: 'Secret Access Key', type: 'password', required: true,  helpCategory: 'generic_credential', docsUrl: 'https://console.aws.amazon.com/iam/home#/users' },
    { name: 'region',          label: 'Region',            type: 'text',     required: false, placeholder: 'us-east-1', helpCategory: 'region' },
  ],
  ftp: [
    { name: 'host',     label: 'Host',     type: 'text',     required: true,  helpCategory: 'host' },
    { name: 'username', label: 'Username', type: 'text',     required: true,  helpCategory: 'username' },
    { name: 'password', label: 'Password', type: 'password', required: true,  helpCategory: 'password' },
    { name: 'port',     label: 'Port',     type: 'number',   required: false, helpCategory: 'port' },
  ],
  sftp: [
    { name: 'host',       label: 'Host',        type: 'text',     required: true,  helpCategory: 'host' },
    { name: 'username',   label: 'Username',    type: 'text',     required: true,  helpCategory: 'username' },
    { name: 'password',   label: 'Password',    type: 'password', required: false, helpCategory: 'password' },
    { name: 'privateKey', label: 'Private Key', type: 'password', required: false, helpCategory: 'private_key' },
    { name: 'port',       label: 'Port',        type: 'number',   required: false, helpCategory: 'port' },
  ],
};

function publicBaseUrl() {
  return process.env.PUBLIC_BASE_URL || process.env.API_BASE_URL || 'http://localhost:3001';
}

function frontendUrl() {
  return process.env.FRONTEND_URL || 'http://localhost:8080';
}

function envAny(names: string[] = []) {
  return names.some((name) => Boolean(process.env[name]?.trim()));
}

function configured(meta: OAuthProviderMetadata) {
  return envAny(meta.clientEnv) && envAny(meta.secretEnv || []) && (!meta.redirectEnv || Boolean(process.env[meta.redirectEnv]?.trim()));
}

function callbackUrl(meta: OAuthProviderMetadata) {
  if (meta.redirectEnv && process.env[meta.redirectEnv]) return process.env[meta.redirectEnv];
  if (!meta.callbackPath) return undefined;
  const base = meta.flow === 'backend_redirect' ? publicBaseUrl() : frontendUrl();
  return `${base}${meta.callbackPath}`;
}

function displayNameFromKey(key: string) {
  return key
    .split(/[_-]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function unique<T>(values: T[]): T[] {
  return Array.from(new Set(values.filter(Boolean)));
}

function fieldForContract(connector: Connector): ConnectionCredentialField[] {
  const contract = connector.credentialContract;
  if (FIELD_OVERRIDES[contract.vaultKey]) return FIELD_OVERRIDES[contract.vaultKey];

  if (contract.type === 'webhook') {
    return [{ name: contract.credentialFieldName || 'webhookUrl', label: 'Webhook URL', type: 'url', required: contract.required }];
  }

  if (contract.type === 'token') {
    return [{ name: contract.credentialFieldName || 'token', label: 'Token', type: 'password', required: contract.required }];
  }

  if (contract.type === 'basic_auth') {
    return [
      { name: 'username', label: 'Username', type: 'text', required: true },
      { name: 'password', label: 'Password', type: 'password', required: true },
    ];
  }

  if (contract.type === 'runtime') {
    return [{ name: contract.credentialFieldName || 'connectionString', label: 'Connection String', type: 'password', required: contract.required }];
  }

  if (contract.type === 'oauth') {
    return [{ name: 'accessToken', label: 'Access Token', type: 'password', required: contract.required }];
  }

  return [{ name: contract.credentialFieldName || 'apiKey', label: 'API Key', type: 'password', required: contract.required }];
}

function manualAuthType(type: Connector['credentialContract']['type']): CatalogAuthType {
  return type === 'oauth' ? 'manual_oauth_token' : type;
}

export function getConnectionCatalog(): ConnectionCatalogEntry[] {
  const grouped = new Map<string, Connector[]>();
  for (const connector of connectorRegistry.getAllConnectors()) {
    const key = connector.credentialContract.vaultKey;
    grouped.set(key, [...(grouped.get(key) || []), connector]);
  }

  const entries: ConnectionCatalogEntry[] = [];
  for (const [vaultKey, connectors] of grouped.entries()) {
    const first = connectors[0];
    const provider = first.credentialContract.provider;
    const oauthMeta = DASHBOARD_OAUTH[vaultKey] || DASHBOARD_OAUTH[provider];
    const scopes = unique(connectors.flatMap((connector) => connector.credentialContract.scopes || []));
    const nodeTypes = unique(connectors.flatMap((connector) => connector.nodeTypes || []));
    const connectorIds = unique(connectors.map((connector) => connector.id));

    if (oauthMeta) {
      entries.push({
        provider: oauthMeta.provider,
        vaultKey,
        displayName: oauthMeta.displayName,
        authType: 'oauth',
        credentialFields: [],
        nodeTypes,
        connectorIds,
        statusTable: oauthMeta.statusTable,
        statusProvider: oauthMeta.statusProvider,
        connectUrl: oauthMeta.connectUrl,
        disconnectUrl: oauthMeta.disconnectUrl,
        callbackUrl: callbackUrl(oauthMeta),
        frontendReturnUrl: oauthMeta.frontendReturnPath ? `${frontendUrl()}${oauthMeta.frontendReturnPath}` : undefined,
        scopes: unique([...(oauthMeta.scopes || []), ...scopes]),
        requiredEnv: unique([...(oauthMeta.clientEnv || []), ...(oauthMeta.secretEnv || []), ...(oauthMeta.redirectEnv ? [oauthMeta.redirectEnv] : [])]),
        configured: configured(oauthMeta),
        oauthImplemented: true,
        flow: oauthMeta.flow,
        setupHint: configured(oauthMeta) ? undefined : `Configure ${oauthMeta.displayName} OAuth env vars, then restart the worker.`,
      });
      continue;
    }

    const contract = first.credentialContract;
    entries.push({
      provider,
      vaultKey,
      displayName: contract.displayName || displayNameFromKey(vaultKey),
      authType: manualAuthType(contract.type),
      credentialFields: fieldForContract(first),
      nodeTypes,
      connectorIds,
      scopes,
      requiredEnv: [],
      configured: true,
      oauthImplemented: false,
      flow: contract.type === 'oauth' ? 'manual_token' : 'manual_token',
      setupHint:
        contract.type === 'oauth'
          ? `${contract.displayName} does not have a worker OAuth route yet. Store an access token manually for now.`
          : undefined,
    });
  }

  return entries.sort((a, b) => a.displayName.localeCompare(b.displayName));
}

function getAuthenticatedUserId(req: Request): string | null {
  return ((req as any).user?.id || (req as any).user?.sub || null) as string | null;
}

async function tokenTableStatus(entry: ConnectionCatalogEntry, userId: string) {
  if (!entry.statusTable) return null;

  try {
    const providerClause = entry.statusTable === 'social_tokens' && entry.statusProvider ? ' AND provider = $2' : '';
    const params = entry.statusTable === 'social_tokens' && entry.statusProvider ? [userId, entry.statusProvider] : [userId];
    const selectProviderUserId = entry.statusTable === 'social_tokens' ? 'provider_user_id' : 'NULL::text AS provider_user_id';
    const rows = await queryAsService<{ expires_at?: string | null; updated_at?: string | null; provider_user_id?: string | null; scope?: string | null }>(
      `SELECT expires_at, updated_at, ${selectProviderUserId}, scope FROM ${entry.statusTable} WHERE user_id = $1${providerClause} LIMIT 1`,
      params
    );
    if (rows.length === 0) return { connected: false };

    const row = rows[0];
    const expiresAt = row.expires_at || null;
    const expired = expiresAt ? new Date(expiresAt).getTime() < Date.now() : false;
    return {
      connected: !expired,
      expiresAt,
      updatedAt: row.updated_at || null,
      providerUserId: row.provider_user_id || null,
      scope: row.scope || null,
    };
  } catch (error: any) {
    return { connected: false, error: error?.message || 'status_check_failed' };
  }
}

async function userCredentialStatus(entry: ConnectionCatalogEntry, userId: string, requireToken = false) {
  try {
    const rows = await queryAsService<{ updated_at?: string | null; credentials?: Record<string, unknown> | null }>(
      `SELECT updated_at, credentials FROM user_credentials WHERE user_id = $1 AND service = $2 LIMIT 1`,
      [userId, entry.vaultKey]
    );
    if (rows.length === 0) return { connected: false };
    const credentials = rows[0].credentials || {};
    if (requireToken && !credentials.access_token && !credentials.accessToken && !credentials.token) {
      return { connected: false };
    }
    return {
      connected: true,
      updatedAt: rows[0].updated_at || null,
      metadata: credentials,
    };
  } catch {
    return { connected: false };
  }
}

async function runtimeCredentialStatus(entry: ConnectionCatalogEntry, userId: string) {
  try {
    const requiredScopes = requiredScopesForProvider(entry.provider, entry.scopes || []);
    const credential = await resolveCredentialDryRun({
      userId,
      provider: entry.provider,
      requiredScopes,
      action: `${entry.displayName} connection status`,
    });

    return {
      connected: true,
      source: 'unified_credentials',
      expiresAt: credential.expiresAt ? credential.expiresAt.toISOString() : null,
      updatedAt: null,
      scope: credential.scopes.join(' '),
    };
  } catch (error) {
    return {
      connected: false,
      source: 'unified_credentials',
      error: formatCredentialError(error, `${entry.displayName} connection status`),
    };
  }
}

export async function connectionsCatalogHandler(_req: Request, res: Response) {
  res.json({ success: true, connections: getConnectionCatalog() });
}

export async function connectionsStatusHandler(req: Request, res: Response) {
  const userId = getAuthenticatedUserId(req);
  if (!userId) return res.status(401).json({ success: false, error: 'Unauthorized' });

  const vault = getCredentialVault();
  const statuses: Record<string, any> = {};

  for (const entry of getConnectionCatalog()) {
    let status: any = entry.oauthImplemented ? await runtimeCredentialStatus(entry, userId) : null;

    if (!entry.oauthImplemented && (!status || !status.connected)) {
      const vaultExists = await vault.exists({ userId }, entry.vaultKey).catch(() => false);
      if (vaultExists) {
        status = { connected: true, source: 'credential_vault' };
      }
    }

    if (!entry.oauthImplemented && (!status || !status.connected)) {
      const legacy = await userCredentialStatus(entry, userId, entry.oauthImplemented);
      if (legacy.connected) status = { ...legacy, source: 'user_credentials' };
    }

    statuses[entry.vaultKey] = {
      provider: entry.provider,
      vaultKey: entry.vaultKey,
      connected: Boolean(status?.connected),
      configured: entry.configured,
      authType: entry.authType,
      flow: entry.flow,
      source: status?.source || (entry.oauthImplemented ? entry.statusTable : undefined),
      expiresAt: status?.expiresAt || null,
      updatedAt: status?.updatedAt || null,
      setupStatus: entry.configured ? 'ready' : 'missing_env',
      metadata: {
        providerUserId: status?.providerUserId || undefined,
        scope: status?.scope || undefined,
        credentialError: status?.connected ? undefined : status?.error,
      },
    };
  }

  res.json({ success: true, connections: statuses });
}

export async function singleConnectionStatusHandler(req: Request, res: Response) {
  const provider = String(req.params.provider || '').toLowerCase();
  const userId = getAuthenticatedUserId(req);
  if (!userId) return res.status(401).json({ connected: false, error: 'Unauthorized' });

  const entry = getConnectionCatalog().find((item) => item.provider === provider || item.vaultKey === provider);
  if (!entry) return res.status(404).json({ connected: false, error: 'Unknown provider' });

  const fakeReq = req;
  const fakeRes = {
    json(payload: any) {
      const status = payload.connections?.[entry.vaultKey] || { connected: false };
      return res.json({ connected: status.connected, configured: status.configured, metadata: status.metadata || {} });
    },
    status(code: number) {
      res.status(code);
      return fakeRes;
    },
  } as Response;

  return connectionsStatusHandler(fakeReq, fakeRes);
}

export function logConnectionConfigReadiness() {
  const catalog = getConnectionCatalog().filter((entry) => entry.oauthImplemented);
  for (const entry of catalog) {
    let setupStatus: 'ready' | 'missing_env' | 'callback_mismatch_risk' = entry.configured ? 'ready' : 'missing_env';
    if (entry.vaultKey === 'github' && entry.callbackUrl?.includes('localhost:3001')) {
      setupStatus = 'callback_mismatch_risk';
    }
    logger.info(
      `[ConnectionCatalog] ${entry.provider}: ${setupStatus} env=[${entry.requiredEnv.join(',')}] callback=${entry.callbackUrl || 'unset'}`
    );
  }
}
