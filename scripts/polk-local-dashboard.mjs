#!/usr/bin/env node

import { createServer } from "node:http";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

import {
  buildPolkLocalParityStatus,
  parsePolkStatusCliOptions,
} from "./polk-local-parity-lib.mjs";

/**
 * @typedef {import("node:http").Server} HttpServer
 */

/**
 * @typedef {object} PolkDashboardOptions
 * @property {string} host Local bind host.
 * @property {number} port Local bind port; zero is accepted for tests.
 */

/**
 * @typedef {() => Promise<import("./polk-local-parity-lib.mjs").PolkLocalParityStatus>} PolkStatusLoader
 */

/**
 * Parse local dashboard network options.
 *
 * The host defaults to loopback so the operational dashboard is not exposed to
 * the local network accidentally.
 *
 * @param {readonly string[]} argv CLI arguments after the script path.
 * @returns {PolkDashboardOptions} Validated local server options.
 */
export function parsePolkDashboardOptions(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      host: { type: "string" },
      port: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const host =
    typeof values.host === "string" ? values.host.trim() : "127.0.0.1";
  const port =
    typeof values.port === "string" ? Number.parseInt(values.port, 10) : 3889;
  if (host.length === 0) throw new Error("--host cannot be empty");
  if (!Number.isSafeInteger(port) || port < 0 || port > 65_535) {
    throw new Error("--port must be an integer from 0 through 65535");
  }
  return { host, port };
}

/**
 * Escape untrusted evidence text before embedding it into dashboard HTML.
 *
 * @param {unknown} value Candidate text.
 * @returns {string} HTML-safe text.
 */
function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

/**
 * Map a lifecycle status to a dashboard color class.
 *
 * @param {string} status Lifecycle status.
 * @returns {"ok" | "warn" | "blocked"} CSS class.
 */
function stageClass(status) {
  if (status === "complete") return "ok";
  if (
    status === "ready" ||
    status === "probed" ||
    status === "awaiting_human"
  ) {
    return "warn";
  }
  return "blocked";
}

/**
 * Render a compact dashboard from the current evidence report.
 *
 * No stage values are embedded as fallback constants. Every count and status
 * comes from the supplied report, which in turn is reconciled from local files.
 *
 * @param {import("./polk-local-parity-lib.mjs").PolkLocalParityStatus} report Current status.
 * @returns {string} Complete dashboard HTML.
 */
export function renderPolkDashboard(report) {
  const stageCards = Object.entries(report.stages)
    .map(
      ([key, stage]) => `
        <article class="stage ${stageClass(stage.status)}" data-stage="${escapeHtml(key)}">
          <div class="stage-head">
            <h2>${escapeHtml(stage.name)}</h2>
            <span class="badge">${escapeHtml(stage.status)}</span>
          </div>
          <div class="count">${stage.count === null ? "—" : escapeHtml(stage.count.toLocaleString())}</div>
          <p>${escapeHtml(stage.evidence)}</p>
        </article>`,
    )
    .join("");
  const blockerItems = report.blockers
    .map((blocker) => `<li>${escapeHtml(blocker)}</li>`)
    .join("");
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta http-equiv="refresh" content="30">
    <title>Polk local ingestion status</title>
    <style>
      :root { color-scheme: dark; font-family: ui-sans-serif, system-ui, sans-serif; }
      body { margin: 0; background: #07111f; color: #e5eefb; }
      main { width: min(1180px, calc(100% - 32px)); margin: 32px auto; }
      header { display: flex; justify-content: space-between; gap: 24px; align-items: end; }
      h1 { margin: 0; font-size: clamp(24px, 4vw, 38px); }
      header p, .stage p { color: #9fb0c7; }
      .parity { padding: 8px 12px; border-radius: 999px; background: #3b1820; color: #fecdd3; }
      .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 14px; margin: 24px 0; }
      .stage { border: 1px solid #26364c; border-left-width: 5px; border-radius: 12px; padding: 18px; background: #0d1a2b; }
      .stage.ok { border-left-color: #34d399; }
      .stage.warn { border-left-color: #fbbf24; }
      .stage.blocked { border-left-color: #fb7185; }
      .stage-head { display: flex; justify-content: space-between; gap: 12px; align-items: start; }
      h2 { margin: 0; font-size: 16px; }
      .badge { text-transform: uppercase; font-size: 11px; letter-spacing: .08em; color: #cbd5e1; }
      .count { font-size: 30px; font-weight: 750; margin: 18px 0 6px; }
      section { border: 1px solid #26364c; border-radius: 12px; padding: 18px; background: #0d1a2b; }
      li { margin: 8px 0; color: #cbd5e1; }
      footer { margin-top: 18px; color: #7f91aa; font-size: 13px; }
      a { color: #7dd3fc; }
    </style>
  </head>
  <body>
    <main>
      <header>
        <div>
          <h1>Polk County local ingestion</h1>
          <p>Evidence-only lifecycle status. Refreshes every 30 seconds.</p>
        </div>
        <div class="parity">PR #200 parity: ${report.pr200FunctionalParity ? "evidenced" : "not yet evidenced"}</div>
      </header>
      <div class="grid">${stageCards}</div>
      <section>
        <h2>Remaining blockers</h2>
        <ul>${blockerItems}</ul>
      </section>
      <footer>
        Generated ${escapeHtml(report.generatedAt)} · <a href="/api/status">JSON status</a>
      </footer>
    </main>
  </body>
</html>`;
}

/**
 * Create the loopback dashboard server without starting it.
 *
 * The injectable status loader keeps HTTP behavior testable without depending
 * on the workstation's multi-gigabyte Polk artifacts.
 *
 * @param {PolkStatusLoader} loadStatus Evidence loader.
 * @returns {HttpServer} Unstarted HTTP server.
 */
export function createPolkDashboardServer(loadStatus) {
  return createServer(async (request, response) => {
    try {
      if (request.url === "/api/status") {
        const report = await loadStatus();
        response.writeHead(200, {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store",
        });
        response.end(`${JSON.stringify(report, null, 2)}\n`);
        return;
      }
      if (request.url === "/" || request.url === "/index.html") {
        const report = await loadStatus();
        response.writeHead(200, {
          "content-type": "text/html; charset=utf-8",
          "cache-control": "no-store",
        });
        response.end(renderPolkDashboard(report));
        return;
      }
      response.writeHead(404, {
        "content-type": "application/json; charset=utf-8",
      });
      response.end('{"error":"not_found"}\n');
    } catch (caught) {
      const message = caught instanceof Error ? caught.message : String(caught);
      response.writeHead(500, {
        "content-type": "application/json; charset=utf-8",
        "cache-control": "no-store",
      });
      response.end(`${JSON.stringify({ error: message })}\n`);
    }
  });
}

/**
 * Start the local Polk evidence dashboard.
 *
 * @param {readonly string[]} argv CLI arguments after the script path.
 * @returns {Promise<HttpServer>} Listening server.
 */
export async function runPolkDashboard(argv) {
  const dashboard = parsePolkDashboardOptions(argv);
  const statusOptions = parsePolkStatusCliOptions([], process.cwd());
  statusOptions.writeOutput = false;
  const server = createPolkDashboardServer(async () => {
    const result = await buildPolkLocalParityStatus(statusOptions);
    return result.status;
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(dashboard.port, dashboard.host, () => resolve(undefined));
  });
  const address = server.address();
  const port =
    typeof address === "object" && address !== null
      ? address.port
      : dashboard.port;
  process.stdout.write(
    `${JSON.stringify({
      event: "polk_local_dashboard_listening",
      url: `http://${dashboard.host}:${port}`,
    })}\n`,
  );
  return server;
}

if (
  process.argv[1] !== undefined &&
  fileURLToPath(import.meta.url) === path.resolve(process.argv[1])
) {
  runPolkDashboard(process.argv.slice(2)).catch((caught) => {
    const message = caught instanceof Error ? caught.message : String(caught);
    process.stderr.write(
      `${JSON.stringify({ event: "polk_local_dashboard_failed", error: message })}\n`,
    );
    process.exitCode = 1;
  });
}
