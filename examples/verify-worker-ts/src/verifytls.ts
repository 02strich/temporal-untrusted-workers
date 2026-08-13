import * as fs from 'node:fs';
import type { TLSConfig } from '@temporalio/worker';

// connectionTLS reads the VERIFY_TLS_* environment variables and returns the
// TLS option to pass to NativeConnection.connect/Connection.connect. When
// VERIFY_TLS_MODE is unset or "plaintext" it returns `false` explicitly
// (never `undefined` - an apiKey credential otherwise auto-enables TLS),
// preserving the examples' plaintext-by-default local-dev behavior. Mirrors
// examples/internal/verifytls/verifytls.go.
//
// Supported variables (all optional):
//
//   VERIFY_TLS_MODE         "plaintext" (default) | "tls"
//   VERIFY_TLS_CA_FILE      PEM file of CA certs to trust; empty = system roots
//   VERIFY_TLS_SERVER_NAME  overrides the SNI / cert hostname to verify against
//   VERIFY_TLS_SKIP_VERIFY  not supported by this worker - see below
export function connectionTLS(): TLSConfig | false {
  const mode = process.env.VERIFY_TLS_MODE ?? '';
  if (mode === '' || mode === 'plaintext') {
    return false;
  }
  if (mode !== 'tls') {
    throw new Error(`VERIFY_TLS_MODE: invalid value "${mode}" (want "plaintext" or "tls")`);
  }

  // Unlike Go's crypto/tls.Config.InsecureSkipVerify, the TypeScript SDK's
  // TLSConfig type (serverNameOverride, serverRootCACertificate,
  // clientCertPair - see packages/common/src/internal-non-workflow/tls-config.ts
  // in temporalio/sdk-typescript) has no certificate-verification-skip option
  // at all. Silently ignoring VERIFY_TLS_SKIP_VERIFY would mean a caller who
  // thinks they disabled verification is still fully verifying - a dangerous
  // silent gap - so this worker fails fast instead.
  const skipVerify = parseBoolEnv('VERIFY_TLS_SKIP_VERIFY');
  if (skipVerify) {
    throw new Error(
      'VERIFY_TLS_SKIP_VERIFY is not supported by the TypeScript worker: the @temporalio/worker ' +
        'TLSConfig type has no certificate-verification-skip option. Use VERIFY_TLS_CA_FILE to trust ' +
        'a specific CA instead, or use the Go verify-worker for this scenario.'
    );
  }

  const tls: TLSConfig = {};

  const serverName = process.env.VERIFY_TLS_SERVER_NAME;
  if (serverName) {
    tls.serverNameOverride = serverName;
  }

  const caFile = process.env.VERIFY_TLS_CA_FILE;
  if (caFile) {
    // A missing serverRootCACertificate falls back to the system root CA
    // pool, which is correct for a publicly-trusted endpoint such as
    // Temporal Cloud or a Cloud Run service's public TLS endpoint.
    tls.serverRootCACertificate = fs.readFileSync(caFile);
  }

  return tls;
}

function parseBoolEnv(key: string): boolean {
  const v = process.env[key];
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
