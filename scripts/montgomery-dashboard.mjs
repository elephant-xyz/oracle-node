#!/usr/bin/env node
/**
 * Local HTTP runtime and static HTML generator for Montgomery County Property & Roof Dashboard.
 *
 * Usage:
 *   node scripts/montgomery-dashboard.mjs
 *   node scripts/montgomery-dashboard.mjs --port=8788 --open
 */

import { createServer } from "node:http";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

import parquet from "/Users/shogan/soofi-xyz/elephant-query-db/node_modules/@dsnp/parquetjs/dist/parquet.js";
const { ParquetReader } = parquet;

import { buildMontgomeryDashboardHtml } from "./montgomery/dashboard-ui.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const PARQUET_PATH = resolve(ROOT, "downloads/montgomery/publish/query-table.parquet");
const DASHBOARD_HTML_PATH = resolve(ROOT, "downloads/montgomery/publish/dashboard.html");

async function loadParquetRecords(limit = 2000) {
  const reader = await ParquetReader.openFile(PARQUET_PATH);
  const cursor = reader.getCursor();
  const records = [];
  let record = null;
  let count = 0;
  while ((record = await cursor.next())) {
    if (count % 150 === 0 || count < 300) {
      records.push(record);
      if (records.length >= limit) break;
    }
    count++;
  }
  await reader.close();
  return records;
}

async function main() {
  const portArg = process.argv.find((a) => a.startsWith("--port="));
  const port = portArg ? Number.parseInt(portArg.split("=")[1], 10) : 8788;
  const shouldOpen = process.argv.includes("--open");

  console.log("Loading Montgomery query-table records from Parquet...");
  const records = await loadParquetRecords();
  console.log(`Loaded ${records.length} records.`);

  const html = buildMontgomeryDashboardHtml(records);
  await mkdir(dirname(DASHBOARD_HTML_PATH), { recursive: true });
  await writeFile(DASHBOARD_HTML_PATH, html, "utf8");
  console.log(`Saved static dashboard to: ${DASHBOARD_HTML_PATH}`);

  const server = createServer((req, res) => {
    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-cache",
    });
    res.end(html);
  });

  server.listen(port, "127.0.0.1", () => {
    const url = `http://127.0.0.1:${port}/`;
    console.log(`\nMontgomery County Property & Roof Intelligence Dashboard is live!`);
    console.log(`URL: ${url}`);
    console.log(`Press Ctrl+C to stop.`);

    if (shouldOpen) {
      spawn("open", [url], { stdio: "ignore", detached: true }).unref();
    }
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
