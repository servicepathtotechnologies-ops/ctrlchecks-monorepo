import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { getDbPool } from '../core/database/db-pool';
import { config } from '../core/config';
import * as jwtLib from 'jsonwebtoken';
import * as AWS from 'aws-sdk';
import { logger } from '../core/logger';

// DELETE /api/user/account
// Authenticates the caller via their own JWT, wipes all user data from the DB,
// and removes the user from the Cognito User Pool so they cannot sign back in.
export default async function deleteAccountHandler(req: Request, res: Response) {
  const authHeader = req.headers.authorization;
  if (!authHeader) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const token = authHeader.replace('Bearer ', '').trim();

  // Verify the token and resolve the user ID
  const db = getDbClient();
  const { data: authData, error: authError } = await db.auth.getUser(token);
  if (authError || !authData?.user) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const userId = authData.user.id as string;

  // Extract the Cognito username from the raw JWT for the adminDeleteUser call.
  // The `cognito:username` claim is always present in Cognito access tokens.
  let cognitoUsername: string | null = null;
  try {
    const payload = jwtLib.decode(token) as Record<string, any> | null;
    cognitoUsername =
      (payload?.['cognito:username'] as string) ||
      (payload?.username as string) ||
      null;
  } catch {
    // Ignore decode errors — Cognito deletion is best-effort
  }

  try {
    // Delete all user data from the database
    await deleteUserData(userId);

    // Remove the user from Cognito so they cannot sign back in.
    // Failures are non-fatal: the DB data is already gone so the app is
    // effectively unusable for that account.
    if (config.cognitoUserPoolId && cognitoUsername) {
      try {
        const cognito = new AWS.CognitoIdentityServiceProvider({
          region: config.awsRegion || 'ap-south-1',
        });
        await cognito
          .adminDeleteUser({
            UserPoolId: config.cognitoUserPoolId,
            Username: cognitoUsername,
          })
          .promise();
      } catch (cognitoErr: any) {
        logger.error('[DeleteAccount] Cognito deletion failed (non-fatal):', cognitoErr.message);
      }
    }

    return res.json({ success: true });
  } catch (error) {
    logger.error('[DeleteAccount] Error:', error);
    const message = error instanceof Error ? error.message : String(error);
    return res.status(500).json({ error: message });
  }
}

// ---------------------------------------------------------------------------
// deleteUserData — removes every row owned by the user across all tables.
//
// Deletion order matters because of FK constraints:
//  1. Break the circular users.subscription_id → subscriptions FK
//  2. Delete tables that reference users WITHOUT ON DELETE CASCADE
//     (payments, subscription_history, admin_actions)
//  3. Delete remaining user-owned rows in any order
//  4. Delete the master user rows (auth.users cascades to public.users;
//     public.users cascades to subscriptions and identity_links)
//
// Each statement is wrapped in a SAVEPOINT so that a missing table or a
// column-type mismatch in one statement does not abort the whole transaction.
// ---------------------------------------------------------------------------
async function deleteUserData(userId: string): Promise<void> {
  const pool = getDbPool();
  const client = await pool.connect();

  let spCounter = 0;
  const tryExec = async (sql: string, params: any[] = []) => {
    const sp = `_del_${++spCounter}`;
    try {
      await client.query(`SAVEPOINT ${sp}`);
      await client.query(sql, params);
      await client.query(`RELEASE SAVEPOINT ${sp}`);
    } catch (err: any) {
      await client.query(`ROLLBACK TO SAVEPOINT ${sp}`);
      // Only log unexpected errors, not "table does not exist"
      if (
        !err.message?.includes('does not exist') &&
        !err.message?.includes('relation') &&
        !err.message?.toLowerCase().includes('undefined')
      ) {
        logger.warn('[DeleteAccount] Non-fatal SQL error:', err.message);
      }
    }
  };

  try {
    await client.query('BEGIN');

    // ── 1. Break circular FK: users.subscription_id → subscriptions ───────
    await tryExec(`UPDATE users SET subscription_id = NULL WHERE id = $1`, [userId]);

    // ── 2. Non-cascading dependents (must go before deleting from users) ───
    await tryExec(`DELETE FROM admin_actions WHERE target_user_id = $1 OR admin_user_id = $1`, [userId]);
    await tryExec(`DELETE FROM subscription_history WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM payments WHERE user_id = $1`, [userId]);

    // ── 3. OAuth / social tokens ───────────────────────────────────────────
    await tryExec(`DELETE FROM google_oauth_tokens WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM linkedin_oauth_tokens WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM notion_oauth_tokens WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM twitter_oauth_tokens WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM instagram_oauth_tokens WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM salesforce_oauth_tokens WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM social_tokens WHERE user_id = $1`, [userId]);

    // ── 4. Credentials and connections ────────────────────────────────────
    await tryExec(`DELETE FROM user_credentials WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM credential_vault WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM connections WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM oauth_states WHERE user_id = $1`, [userId]);

    // ── 5. Workflows and executions (node_executions cascade from executions)
    await tryExec(`DELETE FROM executions WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM workflows WHERE user_id = $1`, [userId]);

    // ── 6. Roles, profile, memory ─────────────────────────────────────────
    await tryExec(`DELETE FROM user_roles WHERE user_id = $1`, [userId]);
    await tryExec(`DELETE FROM profiles WHERE user_id = $1`, [userId]);

    // ── 7. Identity links (canonical has FK → users; delete before users) ─
    await tryExec(
      `DELETE FROM identity_links WHERE canonical_user_id = $1 OR linked_user_id = $1`,
      [userId],
    );

    // ── 8. Subscriptions (after payments & subscription_history are gone) ─
    await tryExec(`DELETE FROM subscriptions WHERE user_id = $1`, [userId]);

    // ── 9. Master user rows ────────────────────────────────────────────────
    // Deleting auth.users cascades to public.users (ON DELETE CASCADE).
    // We also attempt a direct delete on public.users as a fallback.
    await tryExec(`DELETE FROM users WHERE id = $1`, [userId]);
    await tryExec(`DELETE FROM auth.users WHERE id = $1`, [userId]);

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
