import * as fs from 'node:fs';
import type { TLSConfig } from '@temporalio/worker';

// connectionTLS reads the VERIFY_TLS_* environment variables and returns the
// TLS option to pass to NativeConnection.connect. When VERIFY_TLS_MODE is
// unset or "plaintext" it returns `false` explicitly so apiKey does not
// implicitly enable TLS.
export function connectionTLS(env: NodeJS.ProcessEnv = process.env): TLSConfig | false {
  const mode = env.VERIFY_TLS_MODE ?? '';
  if (mode === '' || mode === 'plaintext') {
    return false;
  }
  if (mode !== 'tls') {
    throw new Error(`VERIFY_TLS_MODE: invalid value "${mode}" (want "plaintext" or "tls")`);
  }

  const skipVerify = parseBoolEnv(env, 'VERIFY_TLS_SKIP_VERIFY');
  if (skipVerify) {
    throw new Error(
      'VERIFY_TLS_SKIP_VERIFY is not supported by the TypeScript worker: the @temporalio/worker ' +
        'TLSConfig type has no certificate-verification-skip option. Use VERIFY_TLS_CA_FILE to trust ' +
        'a specific CA instead.'
    );
  }

  const tls: TLSConfig = {};

  const serverName = env.VERIFY_TLS_SERVER_NAME;
  if (serverName) {
    tls.serverNameOverride = serverName;
  }

  const caFile = env.VERIFY_TLS_CA_FILE;
  if (caFile) {
    tls.serverRootCACertificate = fs.readFileSync(caFile);
  }

  return tls;
}

function parseBoolEnv(env: NodeJS.ProcessEnv, key: string): boolean {
  const v = env[key];
  if (v === undefined || v === '') {
    return false;
  }
  switch (v) {
    case '1':
    case 't':
    case 'T':
    case 'true':
    case 'TRUE':
    case 'True':
      return true;
    case '0':
    case 'f':
    case 'F':
    case 'false':
    case 'FALSE':
    case 'False':
      return false;
    default:
      throw new Error(`${key}: invalid boolean value "${v}"`);
  }
}
