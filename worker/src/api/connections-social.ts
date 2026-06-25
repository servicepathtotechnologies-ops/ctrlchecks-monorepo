import { Request, Response } from 'express';
import { queryAsService } from '../core/database/db-pool';
import { logger } from '../core/logger';

type SocialProvider = 'facebook' | 'instagram' | 'whatsapp';

const TABLE_BY_PROVIDER: Record<SocialProvider, string> = {
  facebook: 'social_tokens',
  instagram: 'instagram_oauth_tokens',
  whatsapp: 'whatsapp_oauth_tokens',
};

export function makeSocialDisconnectHandler(provider: SocialProvider) {
  return async function socialDisconnectHandler(req: Request, res: Response) {
    try {
      const userId = (req as any).user?.id as string | undefined;
      if (!userId) return res.status(401).json({ success: false, error: 'Unauthorized' });

      if (provider === 'facebook') {
        await queryAsService(
          `DELETE FROM social_tokens WHERE user_id = $1 AND provider = 'facebook'`,
          [userId]
        );
      } else {
        const table = TABLE_BY_PROVIDER[provider];
        await queryAsService(`DELETE FROM "${table}" WHERE user_id = $1`, [userId]);
      }

      await queryAsService(
        `DELETE FROM user_credentials WHERE user_id = $1 AND service = $2`,
        [userId, provider]
      ).catch(() => []);
      await queryAsService(
        `DELETE FROM credential_vault WHERE user_id = $1 AND key = $2`,
        [userId, provider]
      ).catch(() => []);

      return res.json({ success: true, message: `${provider} account disconnected` });
    } catch (err: any) {
      logger.error(`[${provider}Disconnect]`, err.message);
      return res.status(500).json({ success: false, error: 'Internal server error' });
    }
  };
}
