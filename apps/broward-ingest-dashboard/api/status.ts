import type { IncomingMessage, ServerResponse } from "node:http";

import {
  getDashboardPool,
  readDashboardStatus,
} from "../src/server/neon-status";
import { requireBrowardNeonIdentity } from "../src/server/neon-identity";
import { createMockDashboardStatus } from "../src/shared/mock";
import {
  DASHBOARD_SCHEMA_VERSION,
  type DashboardStatus,
} from "../src/shared/status";

export interface StatusHandlerDependencies {
  readonly now: () => number;
  readonly readStatus: () => Promise<DashboardStatus>;
}

/**
 * Determine whether explicitly enabled local/preview mock mode is safe.
 *
 * Production mock mode fails closed so an operational production domain can
 * never silently display demonstration counters.
 *
 * @param environment - Runtime environment-variable record.
 * @returns True only for explicit non-production mock mode.
 */
export function isMockMode(environment: NodeJS.ProcessEnv): boolean {
  if (environment.DASHBOARD_MOCK_MODE !== "true") return false;
  if (environment.VERCEL_ENV === "production") {
    throw new Error("Dashboard mock mode is forbidden in production");
  }
  return true;
}

/**
 * Create the Vercel-compatible aggregate status request handler.
 *
 * The injected reader makes method, header, error, and privacy behavior
 * testable without a database or credentials. Errors return a fixed offline
 * response and never serialize caught messages.
 *
 * @param dependencies - Clock and aggregate status reader.
 * @returns Async Node request handler accepted by Vercel Functions.
 */
export function createStatusHandler(
  dependencies: StatusHandlerDependencies,
): (request: IncomingMessage, response: ServerResponse) => Promise<void> {
  return async (request, response) => {
    setSecurityHeaders(response);
    if (request.method !== "GET" && request.method !== "HEAD") {
      response.setHeader("allow", "GET, HEAD");
      writeJson(request, response, 405, {
        error: "Method not allowed",
      });
      return;
    }

    try {
      const status = await dependencies.readStatus();
      writeJson(request, response, 200, status);
    } catch {
      response.setHeader("retry-after", "15");
      writeJson(request, response, 503, {
        schemaVersion: DASHBOARD_SCHEMA_VERSION,
        generatedAt: new Date(dependencies.now()).toISOString(),
        health: { state: "offline" },
        error: "Aggregate status is temporarily unavailable",
      });
    }
  };
}

/**
 * Read either local mock data or the pooled Neon aggregate table.
 *
 * @returns A public dashboard status object with its data source marked.
 */
async function readConfiguredStatus(): Promise<DashboardStatus> {
  const nowMs = Date.now();
  if (isMockMode(process.env)) return createMockDashboardStatus(nowMs);
  const expectedIdentity = requireBrowardNeonIdentity(process.env);
  return readDashboardStatus(getDashboardPool(), expectedIdentity, nowMs);
}

/**
 * Apply no-store and browser hardening headers to every API response.
 *
 * @param response - Outgoing Node/Vercel response.
 */
function setSecurityHeaders(response: ServerResponse): void {
  response.setHeader("cache-control", "no-store, max-age=0");
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.setHeader("referrer-policy", "no-referrer");
  response.setHeader("x-content-type-options", "nosniff");
  response.setHeader("x-robots-tag", "noindex, nofollow, noarchive");
}

/**
 * Serialize one small public JSON response.
 *
 * @param request - Incoming request used to implement HEAD semantics.
 * @param response - Outgoing Node/Vercel response.
 * @param statusCode - HTTP status code.
 * @param payload - Aggregate-safe JSON-compatible payload.
 */
function writeJson(
  request: IncomingMessage,
  response: ServerResponse,
  statusCode: number,
  payload: object,
): void {
  const body = `${JSON.stringify(payload)}\n`;
  response.statusCode = statusCode;
  response.setHeader("content-length", Buffer.byteLength(body));
  response.end(request.method === "HEAD" ? undefined : body);
}

const handler = createStatusHandler({
  now: Date.now,
  readStatus: readConfiguredStatus,
});

export default handler;
