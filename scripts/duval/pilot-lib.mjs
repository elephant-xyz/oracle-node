/**
 * Duval local-pilot helpers: COJ HTML capture checks and capture-input artifacts.
 * @module scripts/duval/pilot-lib
 */

import { parsePilotArgs } from "../hillsborough/lib.mjs";
import { classifyFailure } from "../hillsborough/run-state.mjs";
import {
  COUNTY_NAME,
  SEED_COLUMNS,
  toCanonicalReDisplay,
  toCojDetailUrl,
  toText,
} from "./lib.mjs";

const BLOCKED_PAGE_PATTERN =
  /access denied|request blocked|cloudflare|attention required|just a moment|captcha|enable javascript/i;

const RE_LABEL_PATTERN =
  /id=["']ctl00_cphBody_lblRealEstateNumber["'][^>]*>\s*([^<]+)/i;

const DISPLAY_RE_PATTERN = /\b(\d{6}-\d{4})\b/;

/**
 * @param {string[]} argv
 * @returns {ReturnType<typeof parsePilotArgs>}
 */
export function parseDuvalPilotArgs(argv) {
  const options = parsePilotArgs(argv);
  const hasJobId = argv.some((arg) => arg.startsWith("--job-id="));
  if (!hasJobId) {
    options.jobId = `duval-local-${new Date().toISOString().slice(0, 10)}`;
  }
  return options;
}

/**
 * Capture-side address object. Transformed `address.json` uses `county_name`.
 * @param {Record<string, unknown>} seed
 * @returns {Record<string, unknown>}
 */
export function buildUnnormalizedAddress(seed) {
  const fullAddress = toText(seed.address);
  const lat = toText(seed.latitude) === "" ? NaN : Number(seed.latitude);
  const lon = toText(seed.longitude) === "" ? NaN : Number(seed.longitude);
  return {
    full_address: fullAddress,
    unnormalized_address: fullAddress,
    city: toText(seed.city) || null,
    state: toText(seed.state) || "FL",
    zip: toText(seed.zip) || null,
    county_jurisdiction: COUNTY_NAME,
    latitude: Number.isFinite(lat) ? lat : null,
    longitude: Number.isFinite(lon) ? lon : null,
    request_identifier: toText(seed.source_identifier || seed.parcel_id),
  };
}

/**
 * @param {Record<string, unknown>} seed
 * @returns {string}
 */
export function toCojCaptureUrl(seed) {
  const identifier = toText(seed.source_identifier);
  if (!identifier) {
    throw new Error("missing source_identifier");
  }
  const baseUrl = toText(seed.url);
  if (baseUrl.includes("Detail.aspx")) {
    let re = identifier;
    const rawQs = toText(seed.multiValueQueryString);
    if (rawQs) {
      try {
        const parsed = JSON.parse(rawQs);
        const fromSeed = parsed?.RE?.[0];
        if (fromSeed != null && String(fromSeed).trim() !== "") {
          re = String(fromSeed).trim();
        }
      } catch {
        re = identifier;
      }
    }
    return `${baseUrl.split("?")[0]}?RE=${re}`;
  }
  return toCojDetailUrl(identifier);
}

/**
 * @param {Record<string, unknown>} seed
 * @returns {Record<string, unknown>}
 */
export function buildPropertySeed(seed) {
  const identifier = toText(seed.source_identifier);
  return {
    parcel_id: toText(seed.parcel_id) || identifier,
    source_http_request: {
      method: toText(seed.method) || "GET",
      url: toCojCaptureUrl(seed),
    },
    request_identifier: identifier,
  };
}

/**
 * @param {string} html
 * @returns {string}
 */
export function extractCanonicalRe(html) {
  const labeled = String(html).match(RE_LABEL_PATTERN);
  const candidate = toText(labeled?.[1] ?? "");
  if (DISPLAY_RE_PATTERN.test(candidate)) {
    return candidate.match(DISPLAY_RE_PATTERN)?.[1] ?? "";
  }
  const fallback = String(html).match(DISPLAY_RE_PATTERN);
  if (fallback) return fallback[1];
  throw new Error("COJ detail page is missing a canonical RE Number");
}

/**
 * @param {string} html
 * @returns {void}
 */
export function assertCojDetailHtml(html) {
  const body = String(html ?? "");
  if (BLOCKED_PAGE_PATTERN.test(body)) {
    throw new Error("COJ detail page looks blocked or challenged");
  }
  if (body.trim().length === 0) {
    throw new Error("COJ detail page is empty");
  }
  extractCanonicalRe(body);
}

/**
 * @param {string} html
 * @param {unknown} sourceIdentifier
 * @returns {string}
 */
export function assertHtmlMatchesRequestedRe(html, sourceIdentifier) {
  assertCojDetailHtml(html);
  const got = extractCanonicalRe(html).replace(/-/g, "");
  const expected = toCanonicalReDisplay(sourceIdentifier).replace(/-/g, "");
  if (got !== expected) {
    throw new Error(
      `COJ RE Number ${got} does not match requested ${expected}`,
    );
  }
  return extractCanonicalRe(html);
}

/**
 * Transform output `data/address.json` must carry `county_name: "Duval"`.
 * `county_jurisdiction` is the capture-input spelling and is not accepted here:
 * the Columbia-county incident was a transform emitting the wrong county, so a
 * missing or differently-named key has to fail rather than fall back.
 *
 * @param {Record<string, unknown> | null | undefined} record
 * @returns {void}
 */
export function assertTransformedCounty(record) {
  if (!record || typeof record !== "object") {
    throw new Error(
      "transformed address is missing; expected county_name Duval",
    );
  }
  if (record.county_name !== COUNTY_NAME) {
    throw new Error(
      `transformed county_name must be ${COUNTY_NAME}, got ${String(record.county_name)}`,
    );
  }
}

/**
 * Capture input `unnormalized_address.json` carries `county_jurisdiction`.
 *
 * @param {Record<string, unknown> | null | undefined} record
 * @returns {void}
 */
export function assertCaptureCounty(record) {
  if (!record || typeof record !== "object") {
    throw new Error(
      "capture address is missing; expected county_jurisdiction Duval",
    );
  }
  if (record.county_jurisdiction !== COUNTY_NAME) {
    throw new Error(
      `county_jurisdiction must be ${COUNTY_NAME}, got ${String(record.county_jurisdiction)}`,
    );
  }
}

/**
 * @param {unknown} error
 * @returns {"transient" | "permanent" | "unknown"}
 */
export function classifyDuvalFailure(error) {
  const message = (
    error instanceof Error ? error.message : String(error)
  ).toLowerCase();
  if (
    /empty|blocked|challenged|missing a canonical re|does not match requested|http 403|http 404|http 400|missing source_identifier|county_name|county_jurisdiction|enoent/.test(
      message,
    )
  ) {
    return "permanent";
  }
  return classifyFailure(error);
}

/**
 * @param {unknown} value
 * @returns {string}
 */
export function csvEscape(value) {
  const text = value == null ? "" : String(value);
  if (/[",\n\r]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

/**
 * @param {Record<string, unknown>} seed
 * @returns {string}
 */
export function seedRowToCsv(seed) {
  const columns = [
    ...SEED_COLUMNS.filter((column) =>
      Object.prototype.hasOwnProperty.call(seed, column),
    ),
  ];
  if (!columns.includes("request_identifier")) {
    columns.push("request_identifier");
  }
  const header = columns.join(",");
  const line = columns
    .map((column) => {
      if (column === "request_identifier") {
        return csvEscape(
          seed.request_identifier ?? seed.source_identifier ?? "",
        );
      }
      return csvEscape(seed[column]);
    })
    .join(",");
  return `${header}\n${line}\n`;
}

/**
 * @param {{
 *   seedRows: number;
 *   attempted: number;
 *   success: number;
 *   failures: number;
 *   skipped?: number;
 * }} reconciled
 * @returns {void}
 */
export function assertManifestReconciled(reconciled) {
  const skipped = reconciled.skipped ?? 0;
  if (
    reconciled.attempted !==
    reconciled.success + reconciled.failures + skipped
  ) {
    throw new Error(
      `manifest attempted ${reconciled.attempted} != success ${reconciled.success} + failures ${reconciled.failures} + skipped ${skipped}`,
    );
  }
  if (reconciled.seedRows !== reconciled.attempted) {
    throw new Error(
      `manifest seedRows ${reconciled.seedRows} != attempted ${reconciled.attempted}`,
    );
  }
}
