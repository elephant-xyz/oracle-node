import type { PreparedRowBundle } from "./types.js";
/**
 * Map one extracted Lee Accela permit-detail artifact into logical query-db rows.
 *
 * @param params - Source artifact payload and provenance URI.
 * @returns Prepared rows for the permit, work-location address, contacts, inspections, links, and custom fields.
 */
export declare function mapLeePermitDetail(params: {
  readonly record: unknown;
  readonly artifactUri: string | null;
}): PreparedRowBundle;
