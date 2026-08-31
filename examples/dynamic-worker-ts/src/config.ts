import path from 'node:path';
import { getCloudRunToken } from './cloudrun';

export const TASK_QUEUES = ['tenant-alpha', 'tenant-beta', 'tenant-gamma'] as const;
export const WORKFLOW_TYPE = 'customerWorkflow';

export const AUTH_MODE_STATIC = 'static';
export const AUTH_MODE_JWT = 'jwt';

export type AuthMode = typeof AUTH_MODE_STATIC | typeof AUTH_MODE_JWT;

const SAFE_TASK_QUEUE = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;

export function getTaskQueue(env: NodeJS.ProcessEnv = process.env): string {
  const taskQueue = nonEmptyEnv(env, 'VERIFY_TASK_QUEUE') ?? nonEmptyEnv(env, 'TEMPORAL_TASK_QUEUE');

  if (taskQueue === undefined) {
    throw new Error('Missing required environment variable: VERIFY_TASK_QUEUE');
  }
  if (!SAFE_TASK_QUEUE.test(taskQueue)) {
    throw new Error(
      'VERIFY_TASK_QUEUE must contain only letters, numbers, underscores, and hyphens and must start with a letter or number'
    );
  }

  return taskQueue;
}

export function getProxyAddress(env: NodeJS.ProcessEnv = process.env): string {
  return getEnv('VERIFY_PROXY_ADDR', '127.0.0.1:7243', env);
}

export function getNamespace(env: NodeJS.ProcessEnv = process.env): string {
  return getEnv('VERIFY_NAMESPACE', 'default', env);
}

export function getAuthMode(env: NodeJS.ProcessEnv = process.env): AuthMode {
  const authMode = getEnv('VERIFY_AUTH_MODE', AUTH_MODE_STATIC, env);
  switch (authMode) {
    case AUTH_MODE_STATIC:
    case AUTH_MODE_JWT:
      return authMode;
    default:
      throw new Error(`VERIFY_AUTH_MODE: invalid value "${authMode}" (want "${AUTH_MODE_STATIC}" or "${AUTH_MODE_JWT}")`);
  }
}

export async function resolveCredential(
  authMode: AuthMode,
  proxyAddr: string,
  env: NodeJS.ProcessEnv = process.env
): Promise<string> {
  switch (authMode) {
    case AUTH_MODE_STATIC: {
      const apiKey = nonEmptyEnv(env, 'VERIFY_API_KEY');
      if (apiKey === undefined) {
        throw new Error('VERIFY_API_KEY is required when VERIFY_AUTH_MODE=static');
      }
      return apiKey;
    }
    case AUTH_MODE_JWT: {
      const audience = getEnv('VERIFY_CLOUDRUN_TOKEN_AUDIENCE', proxyAddr, env);
      const token = await getCloudRunToken(audience);
      if (token === null) {
        throw new Error(
          `VERIFY_AUTH_MODE=jwt requires a Cloud Run identity token, but none is available (audience="${audience}") - this mode must run on Cloud Run/GCP`
        );
      }
      return token;
    }
  }
}

export function getBundlePath(taskQueue: string): string {
  return path.resolve(__dirname, '..', 'bundles', `${taskQueue}.js`);
}

export function getDemoRoot(): string {
  return path.resolve(__dirname, '..');
}

export function getEnv(key: string, def: string, env: NodeJS.ProcessEnv = process.env): string {
  return nonEmptyEnv(env, key) ?? def;
}

function nonEmptyEnv(env: NodeJS.ProcessEnv, key: string): string | undefined {
  const value = env[key];
  return value === undefined || value === '' ? undefined : value;
}
