import { createHash } from "node:crypto";

export const ADDRESS_SIGNATURE_VERSION = "v1";
export const ELEPHANT_ADDRESS_UUID_NAMESPACE =
  "47541537-6230-5494-bf31-221c5f53ccd5";

const TRAILING_STATE_ZIP_RE = /\b[A-Za-z]{2}\s+(\d{5})(?:-\d{4})?\s*$/;
const TRAILING_ZIP_RE = /\b(\d{5})(?:-\d{4})?\s*$/;

function normalizeField(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .normalize("NFKC")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function normalizePostalCode(value) {
  const digits = normalizeField(value).replace(/[-\s]/g, "").replace(/\D/g, "");
  return digits.length >= 5 ? digits.slice(0, 5) : null;
}

function serializePart(name, value) {
  return `${name}:${Buffer.byteLength(value, "utf8")}:${value}`;
}

function uuidV5(name, namespace) {
  const namespaceBytes = Buffer.from(namespace.replaceAll("-", ""), "hex");
  if (namespaceBytes.byteLength !== 16) {
    throw new Error("UUID namespace must contain exactly 16 bytes");
  }
  const digest = createHash("sha1")
    .update(namespaceBytes)
    .update(name, "utf8")
    .digest()
    .subarray(0, 16);
  digest[6] = (digest[6] & 0x0f) | 0x50;
  digest[8] = (digest[8] & 0x3f) | 0x80;
  const hex = digest.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function parseUnnormalizedAddress(value) {
  const empty = { street: null, city: null, postalCode: null };
  if (value === null || value === undefined) return empty;
  const trimmed = String(value).trim();
  if (trimmed.length === 0) return empty;

  const segments = trimmed
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
  if (segments.length === 0) return empty;

  let postalCode = null;
  const last = segments.at(-1) ?? "";
  const stateZip = TRAILING_STATE_ZIP_RE.exec(last);
  const zipOnly = TRAILING_ZIP_RE.exec(last);
  if (stateZip?.[1] !== undefined) {
    postalCode = stateZip[1];
    const head = last.replace(TRAILING_STATE_ZIP_RE, "").trim();
    if (head.length > 0) segments[segments.length - 1] = head;
    else segments.pop();
  } else if (zipOnly?.[1] !== undefined) {
    postalCode = zipOnly[1];
    const head = last.replace(TRAILING_ZIP_RE, "").trim();
    if (head.length > 0) segments[segments.length - 1] = head;
    else segments.pop();
  }

  return {
    street: segments[0] ?? null,
    city: segments.length > 1 ? segments.slice(1).join(", ") : null,
    postalCode,
  };
}

export function mintSitusAddressIdentity({ state, postalCode, street }) {
  const country = "us";
  const normalizedState = normalizeField(state);
  const normalizedStreet = normalizeField(street);
  const normalizedPostalCode = normalizePostalCode(postalCode);
  if (
    normalizedState.length === 0 ||
    normalizedStreet.length === 0 ||
    normalizedPostalCode === null
  ) {
    return null;
  }

  const signature = [
    `address:${ADDRESS_SIGNATURE_VERSION}`,
    serializePart("country", country),
    serializePart("state", normalizedState),
    serializePart("postal_code", normalizedPostalCode),
    serializePart("street", normalizedStreet),
    serializePart("unit", ""),
  ].join("|");
  return {
    signature,
    elephantToken: createHash("sha256").update(signature, "utf8").digest("hex"),
    elephantUuid: uuidV5(signature, ELEPHANT_ADDRESS_UUID_NAMESPACE),
  };
}
