import {
  createServer,
  type RequestListener,
} from "node:http";
import type { AddressInfo } from "node:net";

import { describe, expect, it } from "vitest";

import {
  createStatusHandler,
  isMockMode,
} from "../api/status";
import { createMockDashboardStatus } from "../src/shared/mock";

const NOW_MS = Date.parse("2026-08-29T01:00:00.000Z");

describe("Vercel status API", () => {
  it("serves no-store aggregate JSON with no private fields", async () => {
    const handler = createStatusHandler({
      now: () => NOW_MS,
      readStatus: async () => createMockDashboardStatus(NOW_MS),
    });

    await withServer((request, response) => {
      void handler(request, response);
    }, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/api/status`);
      const body = await response.text();

      expect(response.status).toBe(200);
      expect(response.headers.get("cache-control")).toBe("no-store, max-age=0");
      expect(response.headers.get("x-content-type-options")).toBe("nosniff");
      expect(response.headers.get("x-robots-tag")).toContain("noindex");
      expect(JSON.parse(body)).toMatchObject({
        county: "Broward",
        dataSource: "mock",
        health: { state: "online" },
      });
      expect(body).not.toMatch(
        /folio|owner|address|database_url|postgresql:|artifact/iu,
      );
    });
  });

  it("returns an explicit offline response without caught error details", async () => {
    const handler = createStatusHandler({
      now: () => NOW_MS,
      readStatus: async () => {
        throw new Error(
          "postgresql://private-user:private-password@private-host/private-db",
        );
      },
    });

    await withServer((request, response) => {
      void handler(request, response);
    }, async (baseUrl) => {
      const response = await fetch(`${baseUrl}/api/status`);
      const body = await response.text();

      expect(response.status).toBe(503);
      expect(response.headers.get("retry-after")).toBe("15");
      expect(JSON.parse(body)).toEqual({
        schemaVersion: 1,
        generatedAt: "2026-08-29T01:00:00.000Z",
        health: { state: "offline" },
        error: "Aggregate status is temporarily unavailable",
      });
      expect(body).not.toContain("private");
      expect(body).not.toContain("postgresql:");
    });
  });

  it("supports HEAD and rejects mutating methods", async () => {
    const handler = createStatusHandler({
      now: () => NOW_MS,
      readStatus: async () => createMockDashboardStatus(NOW_MS),
    });

    await withServer((request, response) => {
      void handler(request, response);
    }, async (baseUrl) => {
      const head = await fetch(`${baseUrl}/api/status`, { method: "HEAD" });
      const post = await fetch(`${baseUrl}/api/status`, { method: "POST" });

      expect(head.status).toBe(200);
      expect(await head.text()).toBe("");
      expect(post.status).toBe(405);
      expect(post.headers.get("allow")).toBe("GET, HEAD");
    });
  });

  it("allows explicit local mock mode but forbids it in production", () => {
    expect(
      isMockMode({
        DASHBOARD_MOCK_MODE: "true",
        VERCEL_ENV: "preview",
      }),
    ).toBe(true);
    expect(
      isMockMode({
        DASHBOARD_MOCK_MODE: "false",
        VERCEL_ENV: "production",
      }),
    ).toBe(false);
    expect(() =>
      isMockMode({
        DASHBOARD_MOCK_MODE: "true",
        VERCEL_ENV: "production",
      }),
    ).toThrow(/forbidden/u);
  });
});

/**
 * Start a loopback-only HTTP server for one API assertion.
 *
 * @param listener - Node request listener under test.
 * @param assertion - Callback receiving the temporary server base URL.
 */
async function withServer(
  listener: RequestListener,
  assertion: (baseUrl: string) => Promise<void>,
): Promise<void> {
  const server = createServer(listener);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  try {
    const address = server.address() as AddressInfo;
    await assertion(`http://127.0.0.1:${String(address.port)}`);
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error === undefined) resolve();
        else reject(error);
      });
    });
  }
}
