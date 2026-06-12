import type {
  BatchUpsertCounters,
  BatchUpsertOptions,
  JsonObject,
  LogicalTableName,
  PreparedRow,
  QueryClient,
  TableWriteSpec,
  UpsertStatement,
} from "./types.js";
export declare const DEFAULT_TABLE_WRITE_SPECS: ReadonlyMap<
  LogicalTableName,
  TableWriteSpec
>;
/**
 * Build the idempotent one-row `INSERT ... ON CONFLICT DO UPDATE` statement for a prepared row.
 *
 * @param row - Prepared logical table row including source metadata and typed columns.
 * @param spec - Optional table write spec; defaults to the table's registered write spec.
 * @returns SQL text and bound values. JSONB and text-array columns are explicitly cast.
 */
export declare function buildUpsertStatement(
  row: PreparedRow,
  spec?: TableWriteSpec,
): UpsertStatement;
/**
 * Execute one prepared-row upsert without resolving foreign-key references.
 *
 * @param client - Query client with a `query` method compatible with `pg`-style clients.
 * @param row - Prepared logical table row to write.
 * @returns Rows returned by the table write spec, or an empty array when the source hash is unchanged.
 */
export declare function upsertPreparedRow<Row extends JsonObject = JsonObject>(
  client: QueryClient,
  row: PreparedRow,
): Promise<readonly Row[]>;
/**
 * Resolve source-key references into direct foreign-key columns before a row is written.
 *
 * @param client - Query client used for small source-key lookup queries.
 * @param row - Prepared logical row from a source-specific mapper.
 * @param options - Missing-reference behavior. Defaults to throwing before the upsert.
 * @returns A prepared row with resolvable direct FK columns added to `values`.
 */
export declare function resolvePreparedRowReferences(
  client: QueryClient,
  row: PreparedRow,
  options?: BatchUpsertOptions,
): Promise<PreparedRow>;
/**
 * Upsert prepared rows sequentially after resolving direct foreign-key references.
 *
 * @param client - Query client used for reference lookups and upserts.
 * @param rows - Prepared logical rows in dependency order.
 * @param options - Reference-resolution options for missing parents.
 * @returns Counters for attempted, changed, and unchanged upsert outcomes.
 */
export declare function upsertPreparedRows(
  client: QueryClient,
  rows: readonly PreparedRow[],
  options?: BatchUpsertOptions,
): Promise<BatchUpsertCounters>;
export declare function mustGetSpec(
  tableName: LogicalTableName,
): TableWriteSpec;
