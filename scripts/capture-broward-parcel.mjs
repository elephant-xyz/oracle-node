#!/usr/bin/env node

/**
 * Fetch one Broward appraisal JSON record and fail closed when the API returns
 * an empty `parcelInfok__BackingField`. Used to smoke-test the prepare payload
 * the existing Broward transform reads as `input.json`.
 */

import { mkdir, writeFile } from "fs/promises";
import path from "path";
import { pathToFileURL } from "url";

import {
  BROWARD_DETAIL_URL,
  browardDetailRequestBody,
  normalizeBrowardFolio,
} from "./broward-folio.mjs";

/**
 * @typedef {object} BrowardParcelEnvelope
 * @property {{ parcelInfok__BackingField?: unknown[] | null } | null} [d] - ASP.NET envelope.
 */

/**
 * @typedef {object} BrowardMultiRequestCapture
 * @property {{
 *   source_http_request?: Record<string, unknown>,
 *   response?: BrowardParcelEnvelope | unknown
 * }} [input] - elephant-cli multi-request wrapper keyed by the flow request key.
 */

/**
 * POST `getParcelInformation` for one folio.
 *
 * @param {string} folio - Normalized folio.
 * @returns {Promise<BrowardParcelEnvelope>} Parsed JSON envelope.
 */
export async function fetchBrowardParcelEnvelope(folio) {
  const response = await fetch(BROWARD_DETAIL_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      Accept: "application/json, text/javascript, */*; q=0.01",
      "X-Requested-With": "XMLHttpRequest",
      Origin: "https://web.bcpa.net",
      Referer: "https://web.bcpa.net/BcpaClient/search.aspx",
    },
    body: JSON.stringify(browardDetailRequestBody(folio)),
  });
  if (!response.ok) {
    throw new Error(
      `Broward appraiser returned HTTP ${String(response.status)} for folio ${folio}`,
    );
  }
  return /** @type {BrowardParcelEnvelope} */ (await response.json());
}

/**
 * True when the value is a non-array object.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is Record<string, unknown>} Whether the value is a JSON object.
 */
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Unwrap an elephant-cli multi-request capture to the ASP.NET envelope the
 * published Broward extractor reads as `input.json`.
 *
 * Prepare writes `{ input: { source_http_request, response } }`. The existing
 * `Counties-trasform-scripts/broward` scripts expect `d.parcelInfok__BackingField`
 * at the document root. Pass the raw envelope through unchanged.
 *
 * @param {unknown} payload - Prepare capture JSON.
 * @returns {BrowardParcelEnvelope} ASP.NET parcel envelope.
 */
export function unwrapBrowardPrepareCapture(payload) {
  if (!isRecord(payload)) {
    throw new Error("Broward prepare capture is not a JSON object");
  }
  const wrapped = payload.input;
  if (isRecord(wrapped) && wrapped.response !== undefined) {
    if (!isRecord(wrapped.response)) {
      throw new Error("Broward multi-request input.response is not a JSON object");
    }
    return /** @type {BrowardParcelEnvelope} */ (wrapped.response);
  }
  return /** @type {BrowardParcelEnvelope} */ (payload);
}

/**
 * Return the parcel record list, or throw when the source is empty.
 *
 * Empty envelopes must not be treated as a successful skip — GIS has folios
 * the appraiser does not.
 *
 * @param {BrowardParcelEnvelope} envelope - API JSON.
 * @param {string} folio - Folio used in the request.
 * @returns {unknown[]} Non-empty parcel list.
 */
export function requireParcelRecords(envelope, folio) {
  const records = envelope.d?.parcelInfok__BackingField;
  if (!Array.isArray(records) || records.length === 0) {
    throw new Error(
      `Broward appraiser returned no parcelInfok__BackingField for folio ${folio}`,
    );
  }
  return records;
}

/**
 * @param {readonly string[]} argv - CLI args after the script name.
 * @returns {{ folio: string, outputPath: string }} Options.
 */
export function parseCaptureCli(argv) {
  let folio;
  let outputPath = "downloads/broward/samples/input.json";
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const value = argv[index + 1];
    if (token === "--folio") {
      folio = value;
      index += 1;
    } else if (token === "--output") {
      outputPath = value ?? outputPath;
      index += 1;
    } else {
      throw new Error(`Unknown option: ${token}`);
    }
  }
  const normalized = normalizeBrowardFolio(folio);
  if (normalized === undefined) {
    throw new Error(
      "--folio is required and must be a 12-character alphanumeric Broward folio",
    );
  }
  return { folio: normalized, outputPath };
}

/**
 * @returns {Promise<void>}
 */
async function main() {
  const options = parseCaptureCli(process.argv.slice(2));
  const envelope = await fetchBrowardParcelEnvelope(options.folio);
  requireParcelRecords(envelope, options.folio);
  await mkdir(path.dirname(options.outputPath), { recursive: true });
  await writeFile(options.outputPath, `${JSON.stringify(envelope, null, 2)}\n`);
  console.log(
    JSON.stringify({
      level: "info",
      message: "broward_parcel_capture_complete",
      folio: options.folio,
      outputPath: options.outputPath,
      parcelCount: envelope.d?.parcelInfok__BackingField?.length ?? 0,
    }),
  );
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(JSON.stringify({ level: "error", message }));
    process.exit(1);
  });
}
