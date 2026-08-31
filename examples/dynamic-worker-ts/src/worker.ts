import { readFile } from 'node:fs/promises';
import { NativeConnection, Worker } from '@temporalio/worker';
import { getAuthMode, getBundlePath, getNamespace, getProxyAddress, getTaskQueue, resolveCredential } from './config';
import { connectionTLS } from './verifytls';

export async function loadWorkflowBundle(taskQueue: string): Promise<{ bundlePath: string; code: string }> {
  const bundlePath = getBundlePath(taskQueue);

  try {
    return { bundlePath, code: await readFile(bundlePath, 'utf8') };
  } catch (error) {
    const cause = error instanceof Error ? error.message : String(error);
    throw new Error(
      `No Workflow bundle is available for Task Queue "${taskQueue}" at ${bundlePath}. Run "pnpm build:bundles" first. (${cause})`,
      { cause: error }
    );
  }
}

export async function runWorker(env: NodeJS.ProcessEnv = process.env): Promise<void> {
  const taskQueue = getTaskQueue(env);
  const { bundlePath, code } = await loadWorkflowBundle(taskQueue);

  const proxyAddr = getProxyAddress(env);
  const namespace = getNamespace(env);
  const authMode = getAuthMode(env);
  const credential = await resolveCredential(authMode, proxyAddr, env);
  const tls = connectionTLS(env);
  const connection = await NativeConnection.connect({
    address: proxyAddr,
    apiKey: credential,
    tls,
  });

  try {
    const worker = await Worker.create({
      connection,
      namespace,
      taskQueue,
      workflowBundle: { code },
    });

    console.log(
      `dynamic-worker-ts starting: proxy=${proxyAddr} namespace=${namespace} taskQueue=${taskQueue} workflowBundle=${bundlePath}`
    );
    await worker.run();
  } finally {
    await connection.close();
  }
}

if (require.main === module) {
  runWorker().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
