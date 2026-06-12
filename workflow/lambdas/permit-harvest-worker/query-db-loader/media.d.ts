export type LeeAppraisalMediaKind =
  | "APPRAISAL_FLOOR_PLAN"
  | "APPRAISAL_PHOTO"
  | "APPRAISAL_TAX_MAP";
export type LeeAppraisalMediaLink = {
  readonly kind: LeeAppraisalMediaKind;
  readonly url: string;
  readonly identityKey: string;
  readonly label: string;
  readonly preferredWidth: number | null;
};
export type AppraisalMediaFileRecordInput = {
  readonly blobUrl: string;
  readonly contentSha256: string;
  readonly contentType: string | null;
  readonly index: number;
  readonly link: LeeAppraisalMediaLink;
  readonly requestIdentifier: string;
  readonly sourceHttpRequest: Record<string, unknown> | null;
  readonly uploadedAt: string;
};
/**
 * Extract Lee Property Appraiser media URLs that represent useful parcel assets
 * rather than page chrome.
 *
 * The Lee page repeats the same photo/floor-plan URLs at thumbnail and large
 * widths. This helper deduplicates by the stable parcel media identity and keeps
 * the largest-width candidate so the downstream Blob upload stores the best
 * available image for the property.
 *
 * @param html - Captured DisplayParcel HTML from the downloader output ZIP.
 * @param pageUrl - Absolute page URL used to resolve relative media paths.
 * @returns Stable media links sorted by media kind and identity.
 */
export declare function extractLeeAppraisalMediaLinks(
  html: string,
  pageUrl?: string,
): readonly LeeAppraisalMediaLink[];
/**
 * Build the `data/file_*.json` payload used by the appraisal loader for an
 * image-like asset uploaded to Vercel Blob.
 *
 * The current logical table has `ipfs_url` as its generic stored-file URL
 * column. For this load, that value stores the Vercel Blob public URL while the
 * original county URL and Blob metadata remain preserved in `source_payload`.
 *
 * @param input - Uploaded media metadata and source media link.
 * @returns JSON object compatible with `mapAppraisalTransformedFile`.
 */
export declare function buildAppraisalMediaFileRecord(
  input: AppraisalMediaFileRecordInput,
): Record<string, unknown>;
/**
 * Build a deterministic Vercel Blob pathname for a property media asset.
 *
 * @param params - County media link, request identifier, and optional extension.
 * @returns Pathname under the `appraisal-media/lee/` namespace.
 */
export declare function buildAppraisalMediaBlobPathname(params: {
  readonly extension: string;
  readonly link: LeeAppraisalMediaLink;
  readonly requestIdentifier: string;
}): string;
/**
 * Infer a compact file extension from a response content type or URL path.
 *
 * @param params - Optional content type and original URL.
 * @returns Extension including the leading dot.
 */
export declare function inferMediaExtension(params: {
  readonly contentType: string | null;
  readonly url: string;
}): string;
