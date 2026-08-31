const METADATA_URL =
  'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity';

// getCloudRunToken fetches this instance's service-account identity token
// from the GCP metadata server and returns it, when running on Cloud Run (or
// any GCP compute that exposes a metadata server).
export async function getCloudRunToken(audience: string): Promise<string | null> {
  const url = `${METADATA_URL}?audience=${encodeURIComponent(audience)}`;

  let response: Response;
  try {
    response = await fetch(url, {
      headers: { 'Metadata-Flavor': 'Google' },
      signal: AbortSignal.timeout(2000),
    });
  } catch {
    return null;
  }

  const body = await response.text();
  if (!response.ok) {
    console.warn(
      `Cloud Run identity token unavailable: metadata server returned ${response.status} ${response.statusText}: ${body.trim()}`
    );
    return null;
  }

  return body.trim();
}
