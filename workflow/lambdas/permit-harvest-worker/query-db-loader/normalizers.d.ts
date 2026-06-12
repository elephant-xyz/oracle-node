import type { JsonObject, SourceMetadata, SourceSystem } from "./types.js";
export declare function isJsonObject(value: unknown): value is JsonObject;
export declare function readString(value: unknown): string | null;
export declare function readNumber(value: unknown): number | null;
export declare function readInteger(value: unknown): number | null;
export declare function readBoolean(value: unknown): boolean | null;
export declare function readStringArray(value: unknown): readonly string[];
/**
 * Read an ISO-like or US-formatted date string into a `YYYY-MM-DD` value.
 *
 * @param value - Unknown source value from an artifact field.
 * @returns Normalized date string when parsing succeeds, otherwise `null`.
 */
export declare function readDate(value: unknown): string | null;
export declare function readTimestamp(value: unknown): string | null;
export declare function normalizeName(value: unknown): string | null;
export declare function normalizeParcelIdentifier(
  value: unknown,
): string | null;
export declare function normalizePostalCode(value: unknown): string | null;
/**
 * Extract a ZIP5 value from an address string without mistaking a street number for a ZIP.
 *
 * Use this for full address text fields. `normalizePostalCode` remains useful
 * for fields that are already supposed to contain only postal-code text.
 *
 * @param value - Unknown source value containing a full address or ZIP-like suffix.
 * @returns ZIP5 when the value contains a trailing or state-qualified ZIP, otherwise `null`.
 */
export declare function extractPostalCodeFromAddress(
  value: unknown,
): string | null;
export declare function normalizeAddressText(value: unknown): string | null;
export declare function buildNormalizedAddressKey(
  value: unknown,
): string | null;
export declare function hashString(value: string): string;
export declare function hashJson(value: unknown): string;
export declare function hashNormalizedAddressKey(
  value: string | null,
): string | null;
export declare function buildSourceMetadata(params: {
  readonly sourceSystem: SourceSystem;
  readonly sourceRecordKey: string;
  readonly sourcePayload: unknown;
  readonly sourceArtifactUri: string | null;
}): SourceMetadata;
export declare function stableJsonStringify(value: unknown): string;
export declare function compactObject(value: JsonObject): JsonObject;
