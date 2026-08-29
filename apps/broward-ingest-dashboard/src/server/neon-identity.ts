export const BROWARD_NEON_PROJECT_ID = "raspy-frost-51580436";
export const BROWARD_PRODUCTION_ENDPOINT_PREFIX = "ep-mute-leaf";

export interface BrowardNeonIdentity {
  readonly branchId: string;
  readonly endpointId: string;
}

export interface BrowardNeonIdentityRow {
  readonly project_id?: unknown;
  readonly branch_id?: unknown;
  readonly endpoint_id?: unknown;
}

/**
 * Require independently verified immutable Neon IDs for the branch named
 * `broward-ingest`.
 *
 * The IDs must come from authenticated Neon branch metadata, not from the
 * database connection being checked. Requiring both values prevents a URL
 * hostname alone from being treated as proof of a human-readable branch name.
 *
 * @param environment - Trusted server-side environment variables.
 * @returns Validated expected branch and primary endpoint IDs.
 */
export function requireBrowardNeonIdentity(
  environment: NodeJS.ProcessEnv,
): BrowardNeonIdentity {
  const branchId = environment.BROWARD_INGEST_NEON_BRANCH_ID;
  const endpointId = environment.BROWARD_INGEST_NEON_ENDPOINT_ID;
  if (typeof branchId !== "string" || !/^br-[a-z0-9-]+$/u.test(branchId)) {
    throw new Error(
      "BROWARD_INGEST_NEON_BRANCH_ID must be an independently verified br-* ID",
    );
  }
  if (
    typeof endpointId !== "string" ||
    !/^ep-[a-z0-9-]+$/u.test(endpointId) ||
    endpointId.startsWith(BROWARD_PRODUCTION_ENDPOINT_PREFIX)
  ) {
    throw new Error(
      "BROWARD_INGEST_NEON_ENDPOINT_ID must be a verified non-production ep-* ID",
    );
  }
  return { branchId, endpointId };
}

/**
 * Assert that server-reported immutable Neon metadata matches the expected
 * isolated Broward branch.
 *
 * Mismatch errors deliberately omit both expected and observed IDs so database
 * identity details cannot leak through an API or migration log.
 *
 * @param row - PostgreSQL `neon.*` settings returned by a read-only query.
 * @param expected - Independently verified immutable branch and endpoint IDs.
 * @throws When project, branch, endpoint, or production isolation differs.
 */
export function assertBrowardNeonIdentity(
  row: BrowardNeonIdentityRow | undefined,
  expected: BrowardNeonIdentity,
): void {
  if (
    row?.project_id !== BROWARD_NEON_PROJECT_ID ||
    row.branch_id !== expected.branchId ||
    row.endpoint_id !== expected.endpointId ||
    expected.endpointId.startsWith(BROWARD_PRODUCTION_ENDPOINT_PREFIX)
  ) {
    throw new Error(
      "Database identity is not the verified broward-ingest branch",
    );
  }
}
