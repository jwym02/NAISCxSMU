function newRequestId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

/** Optional hook for future auth — never put secrets in source. */
export function getAuthHeaders(): Record<string, string> {
  return {};
}

export async function apiFetch(
  input: RequestInfo | URL,
  init: RequestInit = {}
): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("X-Request-ID", newRequestId());
  const auth = getAuthHeaders();
  for (const [k, v] of Object.entries(auth)) {
    headers.set(k, v);
  }
  return fetch(input, { ...init, headers });
}
