import { createHash } from "node:crypto";
const DIRECTIONAL_WORDS = new Map([
  ["NORTH", "N"],
  ["SOUTH", "S"],
  ["EAST", "E"],
  ["WEST", "W"],
  ["NORTHEAST", "NE"],
  ["NORTHWEST", "NW"],
  ["SOUTHEAST", "SE"],
  ["SOUTHWEST", "SW"],
]);
const STREET_SUFFIX_WORDS = new Map([
  ["AVENUE", "AVE"],
  ["BOULEVARD", "BLVD"],
  ["CIRCLE", "CIR"],
  ["COURT", "CT"],
  ["DRIVE", "DR"],
  ["HIGHWAY", "HWY"],
  ["LANE", "LN"],
  ["PARKWAY", "PKWY"],
  ["PLACE", "PL"],
  ["ROAD", "RD"],
  ["STREET", "ST"],
  ["TERRACE", "TER"],
  ["TRAIL", "TRL"],
  ["WAY", "WAY"],
]);
export function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
export function readString(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}
export function readNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string") return null;
  const normalized = value.replace(/[$,]/g, "").trim();
  if (normalized.length === 0) return null;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}
export function readInteger(value) {
  const parsed = readNumber(value);
  return parsed === null ? null : Math.trunc(parsed);
}
export function readBoolean(value) {
  if (typeof value === "boolean") return value;
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  if (["true", "yes", "y", "1"].includes(normalized)) return true;
  if (["false", "no", "n", "0"].includes(normalized)) return false;
  return null;
}
export function readStringArray(value) {
  if (!Array.isArray(value)) return [];
  return value.flatMap((item) => {
    const text = readString(item);
    return text === null ? [] : [text];
  });
}
/**
 * Read an ISO-like or US-formatted date string into a `YYYY-MM-DD` value.
 *
 * @param value - Unknown source value from an artifact field.
 * @returns Normalized date string when parsing succeeds, otherwise `null`.
 */
export function readDate(value) {
  const text = readString(value);
  if (text === null) return null;
  const isoMatch = /^(\d{4})-(\d{2})-(\d{2})/.exec(text);
  if (isoMatch !== null) {
    const [, year, month, day] = isoMatch;
    if (year !== undefined && month !== undefined && day !== undefined) {
      return `${year}-${month}-${day}`;
    }
  }
  const usMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(text);
  if (usMatch === null) return null;
  const [, month, day, year] = usMatch;
  if (year === undefined || month === undefined || day === undefined)
    return null;
  return `${year}-${month.padStart(2, "0")}-${day.padStart(2, "0")}`;
}
export function readTimestamp(value) {
  const text = readString(value);
  if (text === null) return null;
  const parsed = Date.parse(text);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}
export function normalizeName(value) {
  const text = readString(value);
  if (text === null) return null;
  return text
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
export function normalizeParcelIdentifier(value) {
  const text = readString(value);
  if (text === null) return null;
  const digits = text.replace(/\D/g, "");
  if (digits.length > 0) return digits;
  return text.toUpperCase().replace(/\s+/g, "");
}
export function normalizePostalCode(value) {
  const text = readString(value);
  if (text === null) return null;
  const digits = text.replace(/\D/g, "");
  return digits.length >= 5 ? digits.slice(0, 5) : null;
}
/**
 * Extract a ZIP5 value from an address string without mistaking a street number for a ZIP.
 *
 * Use this for full address text fields. `normalizePostalCode` remains useful
 * for fields that are already supposed to contain only postal-code text.
 *
 * @param value - Unknown source value containing a full address or ZIP-like suffix.
 * @returns ZIP5 when the value contains a trailing or state-qualified ZIP, otherwise `null`.
 */
export function extractPostalCodeFromAddress(value) {
  const text = readString(value);
  if (text === null) return null;
  const stateQualifiedMatch = /\b(?:FL|FLORIDA)\s+(\d{5})(?:-\d{4})?\b/i.exec(
    text,
  );
  if (stateQualifiedMatch?.[1] !== undefined) return stateQualifiedMatch[1];
  const trailingMatch = /(?:^|\D)(\d{5})(?:-\d{4})?\s*(?:US|USA)?\s*$/i.exec(
    text,
  );
  return trailingMatch?.[1] ?? null;
}
export function normalizeAddressText(value) {
  const text = readString(value);
  if (text === null) return null;
  const tokens = text
    .toUpperCase()
    .replace(/[#.,]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .map(
      (token) =>
        DIRECTIONAL_WORDS.get(token) ?? STREET_SUFFIX_WORDS.get(token) ?? token,
    );
  return tokens.join(" ");
}
export function buildNormalizedAddressKey(value) {
  const normalized = normalizeAddressText(value);
  if (normalized === null) return null;
  return normalized.toLowerCase();
}
export function hashString(value) {
  return createHash("sha256").update(value).digest("hex");
}
export function hashJson(value) {
  return hashString(stableJsonStringify(value));
}
export function hashNormalizedAddressKey(value) {
  return value === null ? null : hashString(value.toLowerCase());
}
export function buildSourceMetadata(params) {
  return {
    source_system: params.sourceSystem,
    source_record_key: params.sourceRecordKey,
    source_record_hash: hashJson(params.sourcePayload),
    source_artifact_uri: params.sourceArtifactUri,
  };
}
export function stableJsonStringify(value) {
  if (value === null) return "null";
  if (typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableJsonStringify(entry)).join(",")}]`;
  }
  const objectValue = value;
  const entries = Object.keys(objectValue)
    .sort()
    .map(
      (key) =>
        `${JSON.stringify(key)}:${stableJsonStringify(objectValue[key])}`,
    );
  return `{${entries.join(",")}}`;
}
export function compactObject(value) {
  const entries = Object.entries(value).filter(
    ([, entryValue]) => entryValue !== undefined,
  );
  return Object.fromEntries(entries);
}
