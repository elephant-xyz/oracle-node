import type { PreparedRowBundle } from "./types.js";
/**
 * Map one Lee appraiser transformed JSON file into a logical query-db row bundle.
 *
 * @param params - Transformed file name, parsed payload, artifact URI, and optional request identifier.
 * @returns Prepared rows for recognized appraisal files, or a skipped-record entry for unsupported files.
 */
export declare function mapAppraisalTransformedFile(params: {
  readonly filePath: string;
  readonly record: unknown;
  readonly artifactUri: string | null;
  readonly requestIdentifier?: string | null;
}): PreparedRowBundle;
