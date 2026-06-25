import { Request, Response } from 'express';
import { queryAsService } from '../core/database/db-pool';
import { logger } from '../core/logger';

type OAuthTableProvider = 'notion' | 'twitter' | 'salesforce' | 'instagram' | 'whatsapp';

const TABLE_BY_PROVIDER: Record<OAuthTableProvider, string> = {
  notion: 'notion_oauth_tokens',
  twitter: 'twitter_oauth_tokens',
  salesforce: 'salesforce_oauth_tokens',
  instagram: 'instagram_oauth_tokens',
  whatsapp: 'whatsapp_oauth_tokens',
};

export function makeOAuthTableDisconnectHandler(provider: OAuthTableProvider) {
  return async function oauthTableDisconnectHandler(req: Request, res: Response) {
    try {
      const userId = (req as any).user?.id as string | undefined;
      if (!userId) return res.status(401).json({ success: false, error: 'Unauthorized' });

      const table = TABLE_BY_PROVIDER[provider];
      await queryAsService(`DELETE FROM "${table}" WHERE user_id = $1`, [userId]);

      await Promise.all([
        queryAsService(
          `DELETE FROM user_credentials WHERE user_id = $1 AND service = $2`,
          [userId, provider]
        ).catch(() => []),
        queryAsService(
          `DELETE FROM credential_vault WHERE user_id = $1 AND key = $2`,
          [userId, provider]
        ).catch(() => []),
      ]);

      return res.json({ success: true, message: `${provider} account disconnected` });
    } catch (err: any) {
      logger.error(`[${provider}Disconnect]`, err.message);
      return res.status(500).json({ success: false, error: 'Internal server error' });
    }
  };
}
