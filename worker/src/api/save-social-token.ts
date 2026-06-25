/**
 * Save Social Token API
 * 
 * Secure endpoint for saving OAuth tokens with encryption.
 * Tokens are encrypted before storage in the database.
 */

import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { saveProviderToken, SocialProvider } from '../shared/social-token-manager';
import { ErrorCode, createError } from '../core/utils/error-codes';
import { logger } from '../core/logger';

export default async function saveSocialTokenHandler(req: Request, res: Response) {
  try {
    const db = getDbClient();
    const user = (req as any).user;
    if (!user?.id) {
      return res.status(401).json({
        success: false,
        error: createError(ErrorCode.UNAUTHORIZED, 'Authentication required'),
      });
    }
    
    // Validate request body
    const { provider, access_token, refresh_token, expires_at, scope, provider_user_id } = req.body;
    
    if (!provider || !access_token) {
      return res.status(400).json({
        success: false,
        error: createError(ErrorCode.BAD_REQUEST, 'Provider and access_token are required'),
      });
    }
    
    // Validate provider
    const validProviders: SocialProvider[] = ['github', 'facebook', 'twitter', 'linkedin', 'google'];
    if (!validProviders.includes(provider)) {
      return res.status(400).json({
        success: false,
        error: createError(ErrorCode.BAD_REQUEST, `Invalid provider. Must be one of: ${validProviders.join(', ')}`),
      });
    }
    
    // Save token (encryption happens inside saveProviderToken)
    await saveProviderToken(
      db,
      user.id,
      provider,
      {
        access_token,
        refresh_token: refresh_token || null,
        expires_at: expires_at || null,
        scope: scope || null,
        provider_user_id: provider_user_id || null,
        token_type: 'Bearer',
      }
    );
    
    return res.json({
      success: true,
      message: `${provider} token saved successfully`,
    });
  } catch (error) {
    logger.error('[Save Social Token] Error:', error);
    return res.status(500).json({
      success: false,
      error: createError(ErrorCode.INTERNAL_ERROR, error instanceof Error ? error.message : 'Failed to save token'),
    });
  }
}
