import { NativeConnection, Worker } from '@temporalio/worker';
import * as activities from './activities';
import { connectionTLS } from './verifytls';
import { getCloudRunToken } from './cloudrun';

// Worker auth modes, mirroring the proxy's TEMPORAL_PROXY_AUTH_MODE and
// examples/verify-worker/main.go's VERIFY_AUTH_MODE.
const AUTH_MODE_STATIC = 'static';
const AUTH_MODE_JWT = 'jwt';

function getEnv(key: string, def: string): string {
  const v = process.env[key];
  return v && v !== '' ? v : def;
}

// resolveCredential returns the credential the worker presents to the proxy,
// per VERIFY_AUTH_MODE. In "static" mode it is VERIFY_API_KEY (a shared API
// key); in "jwt" mode it is this instance's Google Cloud Run identity token,
// which the proxy validates when it runs with TEMPORAL_PROXY_AUTH_MODE=jwt.
// Mirrors examples/verify-worker/main.go's resolveCredential exactly,
// including its fail-fast behavior.
async function resolveCredential(authMode: string, proxyAddr: string): Promise<string> {
  switch (authMode) {
    case AUTH_MODE_STATIC: {
      const apiKey = process.env.VERIFY_API_KEY;
      if (!apiKey) {
        console.error('VERIFY_API_KEY is required when VERIFY_AUTH_MODE=static');
        process.exit(1);
      }
      return apiKey;
    }
    case AUTH_MODE_JWT: {
      const audience = getEnv('VERIFY_CLOUDRUN_TOKEN_AUDIENCE', proxyAddr);
      const token = await getCloudRunToken(audience);
      if (token === null) {
        console.error(
          `VERIFY_AUTH_MODE=jwt requires a Cloud Run identity token, but none is available (audience="${audience}") - this mode must run on Cloud Run/GCP`
        );
        process.exit(1);
      }
      return token;
    }
    default:
      console.error(`VERIFY_AUTH_MODE: invalid value "${authMode}" (want "${AUTH_MODE_STATIC}" or "${AUTH_MODE_JWT}")`);
      process.exit(1);
  }
}

async function run(): Promise<void> {
  const proxyAddr = getEnv('VERIFY_PROXY_ADDR', '127.0.0.1:7243');
  const namespace = getEnv('VERIFY_NAMESPACE', 'default');
  const taskQueue = getEnv('VERIFY_TASK_QUEUE', 'proxy-test-queue');
  const authMode = getEnv('VERIFY_AUTH_MODE', AUTH_MODE_STATIC);

  // The proxy accepts the credential the same way in either mode (an
  // "authorization: Bearer <cred>" header via the apiKey connection option);
  // VERIFY_AUTH_MODE only changes what that credential is, mirroring the
  // proxy's own TEMPORAL_PROXY_AUTH_MODE.
  const credential = await resolveCredential(authMode, proxyAddr);

  // Plaintext by default (matching the proxy's local-dev default); set
  // VERIFY_TLS_MODE=tls to connect to a proxy whose downstream listener
  // terminates TLS. connectionTLS() always returns an explicit value (never
  // undefined), since apiKey alone would otherwise auto-enable TLS.
  const tls = connectionTLS();

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
      workflowsPath: require.resolve('./workflows'),
      activities,
    });

    console.log(`verify-worker-ts starting: proxy=${proxyAddr} namespace=${namespace} taskQueue=${taskQueue}`);
    await worker.run();
  } finally {
    await connection.close();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
