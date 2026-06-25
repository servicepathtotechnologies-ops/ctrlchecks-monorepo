import { Request, Response } from 'express';
import { getDbClient } from '../core/database/aws-db-client';
import { logger } from '../core/logger';

type AppRole = 'admin' | 'moderator' | 'user';
type WorkflowStatus = 'active' | 'inactive';

/** Cognito auth ban: blocks sign-in and refresh; reversible via ban_duration: 'none'. */
const ADMIN_SUSPEND_BAN_DURATION = '876000h'; // ~100 years — effectively indefinite until admin reinstates

function isUserBanned(user: any): boolean {
  const bannedUntil = user?.banned_until ? new Date(user.banned_until).getTime() : 0;
  return bannedUntil > Date.now();
}

function normalizeStatus(user: any): 'active' | 'pending' | 'disabled' {
  const now = Date.now();
  const bannedUntil = user?.banned_until ? new Date(user.banned_until).getTime() : 0;

  if (bannedUntil > now) {
    return 'disabled';
  }

  if (!user?.email_confirmed_at) {
    return 'pending';
  }

  return 'active';
}

function getDisplayName(user: any, profile?: { full_name: string | null } | null): string {
  const profileName = profile?.full_name?.trim();
  if (profileName) {
    return profileName;
  }

  const metadataName =
    (typeof user?.user_metadata?.full_name === 'string' && user.user_metadata.full_name.trim()) ||
    (typeof user?.user_metadata?.name === 'string' && user.user_metadata.name.trim());

  if (metadataName) {
    return metadataName;
  }

  const email = typeof user?.email === 'string' ? user.email : '';
  return email.split('@')[0] || 'Unknown';
}

function isSubscriptionTaken(user: any): boolean {
  const metadataCandidates = [user?.app_metadata, user?.user_metadata];

  for (const metadata of metadataCandidates) {
    if (!metadata || typeof metadata !== 'object') {
      continue;
    }

    const value =
      metadata.subscription_taken ??
      metadata.subscriptionTaken ??
      metadata.is_subscribed ??
      metadata.isSubscribed ??
      metadata.subscription_active ??
      metadata.subscriptionActive ??
      metadata.has_subscription ??
      metadata.hasSubscription ??
      metadata.plan;

    if (typeof value === 'boolean') {
      return value;
    }

    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      if (['true', 'yes', 'active', 'pro', 'premium'].includes(normalized)) {
        return true;
      }
    }
  }

  return false;
}

function parseNumber(value: unknown): number {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Math.max(0, Math.floor(value));
  }

  if (typeof value === 'string') {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return Math.max(0, Math.floor(parsed));
    }
  }

  return 0;
}

function getAiBuildCallsFromWorkflow(workflow: any): number {
  const metadata = workflow?.metadata && typeof workflow.metadata === 'object' ? workflow.metadata : {};
  const fromBuild = (metadata as any)?.buildAiUsage?.totals?.callCount;
  return parseNumber(fromBuild);
}

function getWorkflowBuildTokens(workflow: any): number {
  const metadata = workflow?.metadata && typeof workflow.metadata === 'object' ? workflow.metadata : {};

  const candidates = [
    (metadata as any)?.buildAiUsage?.totals?.totalTokens,
    workflow?.tokens_used_to_build,
    workflow?.build_tokens,
    workflow?.token_usage,
    metadata?.tokensUsedToBuild,
    metadata?.tokens_used_to_build,
    metadata?.buildTokens,
    metadata?.tokenUsage?.totalTokens,
    metadata?.tokenUsage?.total_tokens,
    metadata?.ai_usage?.totalTokens,
    metadata?.ai_usage?.total_tokens,
    metadata?.usage?.totalTokens,
    metadata?.usage?.total_tokens,
  ];

  for (const candidate of candidates) {
    const value = parseNumber(candidate);
    if (value > 0) {
      return value;
    }
  }

  return 0;
}

async function requireAdminUser(db: ReturnType<typeof getDbClient>, req: Request) {
  const authHeader = req.headers.authorization;
  if (!authHeader) {
    return { ok: false as const, error: 'Unauthorized', status: 401 };
  }

  const token = authHeader.replace('Bearer ', '').trim();
  const { data: authData, error: authError } = await db.auth.getUser(token);
  if (authError || !authData?.user) {
    return { ok: false as const, error: 'Unauthorized', status: 401 };
  }

  const requester = authData.user;
  const { data: roleData, error: roleError } = await db
    .from('user_roles')
    .select('role')
    .eq('user_id', requester.id)
    .eq('role', 'admin')
    .maybeSingle();

  if (roleError || !roleData) {
    return { ok: false as const, error: 'Admin access required', status: 403 };
  }

  return { ok: true as const, requester };
}

export default async function adminUsersHandler(req: Request, res: Response) {
  const db = getDbClient();

  try {
    const auth = await requireAdminUser(db, req);
    if (!auth.ok) {
      return res.status(auth.status).json({ error: auth.error });
    }

    const method = req.method;
    const userId = req.params.id;

    if (method === 'GET') {
      if (userId) {
        const { data: userData, error: userError } = await db.auth.admin.getUserById(userId);
        if (userError || !userData?.user) {
          return res.status(404).json({ error: 'User not found' });
        }

        const user = userData.user;

        const { data: profileRow, error: profileError } = await db
          .from('profiles')
          .select('user_id, full_name, email')
          .eq('user_id', userId)
          .maybeSingle();
        if (profileError) {
          throw profileError;
        }

        const { data: roleRows, error: rolesError } = await db
          .from('user_roles')
          .select('role')
          .eq('user_id', userId);
        if (rolesError) {
          throw rolesError;
        }

        const rolePriority: Record<AppRole, number> = { admin: 3, moderator: 2, user: 1 };
        const primaryRole = ((roleRows || []) as any[]).reduce(
          (acc: AppRole, row: any) => {
            const nextRole = row.role as AppRole;
            return rolePriority[nextRole] > rolePriority[acc] ? nextRole : acc;
          },
          'user' as AppRole
        );

        const { data: workflowRows, error: workflowsError } = await db
          .from('workflows')
          .select('id, name, status, metadata')
          .eq('user_id', userId)
          .order('created_at', { ascending: false });
        if (workflowsError) {
          throw workflowsError;
        }

        const workflowIds = (workflowRows || []).map((workflow: any) => workflow.id);
        let executionCountsByWorkflow = new Map<string, number>();

        if (workflowIds.length > 0) {
          const { data: executionRows, error: executionsError } = await db
            .from('executions')
            .select('workflow_id')
            .in('workflow_id', workflowIds);
          if (executionsError) {
            throw executionsError;
          }

          for (const execution of executionRows || []) {
            const wfId = execution.workflow_id;
            executionCountsByWorkflow.set(wfId, (executionCountsByWorkflow.get(wfId) || 0) + 1);
          }
        }

        const workflowItems = (workflowRows || []).map((workflow: any) => {
          const workflowRuns = executionCountsByWorkflow.get(workflow.id) || 0;
          return {
            id: workflow.id,
            title: workflow.name,
            /** @deprecated Use workflowRuns — kept for older admin clients */
            apiCalls: workflowRuns,
            workflowRuns,
            aiBuildCalls: getAiBuildCallsFromWorkflow(workflow),
            tokensUsedToBuild: getWorkflowBuildTokens(workflow),
            status: workflow.status === 'active' ? ('active' as WorkflowStatus) : ('inactive' as WorkflowStatus),
          };
        });

        return res.json({
          user: {
            id: user.id,
            name: getDisplayName(user, profileRow),
            email: profileRow?.email || user.email || '',
            status: normalizeStatus(user),
            suspended: isUserBanned(user),
            role: primaryRole,
            subscriptionTaken: isSubscriptionTaken(user),
            firstSignInAt: user.created_at,
            lastSignInAt: user.last_sign_in_at,
            totalWorkflowsBuilt: workflowItems.length,
            workflows: workflowItems,
          },
        });
      }

      const { data: usersData, error: usersError } = await db.auth.admin.listUsers();
      if (usersError) {
        throw usersError;
      }

      const users: any[] = usersData?.users ?? [];
      const userIds = users.map((user: any) => user.id);

      const { data: profileRows, error: profilesError } = await db
        .from('profiles')
        .select('user_id, full_name, email')
        .in('user_id', userIds);
      if (profilesError) {
        throw profilesError;
      }

      const { data: roleRows, error: rolesError } = await db
        .from('user_roles')
        .select('user_id, role')
        .in('user_id', userIds);
      if (rolesError) {
        throw rolesError;
      }

      const profileMap = new Map((profileRows || []).map((row: any) => [row.user_id, row]));
      const rolePriority: Record<AppRole, number> = { admin: 3, moderator: 2, user: 1 };
      const roleMap = new Map<string, AppRole>();

      for (const row of roleRows || []) {
        const existing = roleMap.get(row.user_id);
        if (!existing || rolePriority[row.role as AppRole] > rolePriority[existing]) {
          roleMap.set(row.user_id, row.role as AppRole);
        }
      }

      const formattedUsers = users.map((user: any) => {
        const profile: any = profileMap.get(user.id);
        const role = roleMap.get(user.id) ?? 'user';

        return {
          id: user.id,
          name: getDisplayName(user, profile),
          email: profile?.email || user.email || '',
          status: normalizeStatus(user),
          suspended: isUserBanned(user),
          role,
        };
      });

      return res.json({ users: formattedUsers });
    }

    if (method === 'PATCH' && userId) {
      const body = req.body ?? {};
      const hasSuspended = typeof body.suspended === 'boolean';
      const requestedRole = body.role as AppRole | undefined;
      const validRoles: AppRole[] = ['admin', 'moderator', 'user'];

      if (
        !hasSuspended &&
        (requestedRole === undefined || !validRoles.includes(requestedRole))
      ) {
        return res.status(400).json({
          error: 'Provide suspended (boolean) and/or role (admin | moderator | user)',
        });
      }

      if (hasSuspended) {
        if (userId === auth.requester.id) {
          return res.status(400).json({
            error: 'You cannot suspend or reinstate your own account',
          });
        }

        if (body.suspended === true) {
          const { data: adminRoleRow, error: adminCheckError } = await db
            .from('user_roles')
            .select('user_id')
            .eq('user_id', userId)
            .eq('role', 'admin')
            .maybeSingle();

          if (adminCheckError) {
            throw adminCheckError;
          }

          if (adminRoleRow) {
            return res.status(400).json({
              error: 'Cannot suspend an administrator account',
            });
          }
        }

        const { error: banError } = await db.auth.admin.updateUserById(userId, {
          ban_duration: body.suspended ? ADMIN_SUSPEND_BAN_DURATION : 'none',
        });

        if (banError) {
          throw banError;
        }
      }

      let updatedRole: AppRole | undefined;

      if (requestedRole !== undefined) {
        if (!validRoles.includes(requestedRole)) {
          return res.status(400).json({ error: 'role must be one of: admin, moderator, user' });
        }

        const { error: deleteRolesError } = await db
          .from('user_roles')
          .delete()
          .eq('user_id', userId);
        if (deleteRolesError) {
          throw deleteRolesError;
        }

        const { data: roleData, error: insertRoleError } = await db
          .from('user_roles')
          .insert({ user_id: userId, role: requestedRole })
          .select('user_id, role')
          .single();
        if (insertRoleError) {
          throw insertRoleError;
        }

        updatedRole = roleData.role as AppRole;
      }

      return res.json({
        success: true,
        ...(hasSuspended ? { suspended: body.suspended as boolean } : {}),
        ...(updatedRole !== undefined ? { role: updatedRole } : {}),
      });
    }

    if (method === 'DELETE' && userId) {
      if (userId === auth.requester.id) {
        return res.status(400).json({ error: 'You cannot delete your own account from admin panel' });
      }

      const { error } = await db.auth.admin.deleteUser(userId);
      if (error) {
        throw error;
      }

      return res.json({ success: true });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (error) {
    logger.error('Admin users error:', error);
    const message = error instanceof Error ? error.message : String(error);
    return res.status(500).json({ error: message });
  }
}
