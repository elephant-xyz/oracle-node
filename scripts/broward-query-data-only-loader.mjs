/**
 * In-memory Elephant CLI transform hook for Broward query-data-only workers.
 *
 * Upstream elephant-cli PR #186 established the minimal data-only behavior by
 * omitting the single `generateFactSheet(tempRoot)` call after county scripts.
 * The installed 1.58.x library does not expose that switch. This loader applies
 * the same one-call omission in memory, only inside explicitly classified
 * query-data-only child processes. It never modifies node_modules on disk.
 *
 * The exact-call and single-match checks intentionally fail closed when the
 * upstream transform implementation changes. A future public CLI option should
 * replace this compatibility hook rather than broadening its match.
 */

const TRANSFORM_MODULE_SUFFIX =
  "/@elephant-xyz/cli/dist/commands/transform/index.js";
const FACT_SHEET_CALL = "                await generateFactSheet(tempRoot);";
const DATA_ONLY_REPLACEMENT = [
  '                if (process.env.BROWARD_QUERY_DATA_ONLY !== "1") {',
  '                    throw new Error("Query-data-only loader used outside its guarded worker");',
  "                }",
  "                logger.info('Deferring HTML and fact-sheet generation for query-data-only output');",
].join("\n");

/**
 * Convert a Node loader source payload to UTF-8 text.
 *
 * @param {string | ArrayBuffer | ArrayBufferView} source - Source returned by the next loader.
 * @returns {string} JavaScript module source.
 */
function sourceText(source) {
  if (typeof source === "string") return source;
  if (source instanceof ArrayBuffer) {
    return Buffer.from(source).toString("utf8");
  }
  return Buffer.from(
    source.buffer,
    source.byteOffset,
    source.byteLength,
  ).toString("utf8");
}

/**
 * Node ESM loader hook that omits only Elephant CLI's fact-sheet call.
 *
 * @param {string} url - Fully resolved module URL.
 * @param {Record<string, unknown>} context - Node loader context.
 * @param {(url: string, context: Record<string, unknown>) => Promise<{
 *   format?: string,
 *   source?: string | ArrayBuffer | ArrayBufferView,
 *   shortCircuit?: boolean
 * }>} nextLoad - Next loader in the chain.
 * @returns {Promise<{
 *   format?: string,
 *   source?: string | ArrayBuffer | ArrayBufferView,
 *   shortCircuit?: boolean
 * }>} Original or narrowly patched module result.
 */
export async function load(url, context, nextLoad) {
  const loaded = await nextLoad(url, context);
  if (!new URL(url).pathname.endsWith(TRANSFORM_MODULE_SUFFIX)) return loaded;
  if (loaded.source === undefined) {
    throw new Error("Elephant CLI transform loader returned no source");
  }
  const source = sourceText(loaded.source);
  const matches = source.split(FACT_SHEET_CALL).length - 1;
  if (matches !== 1) {
    throw new Error(
      `Refusing query-data-only transform: expected one fact-sheet call, found ${String(matches)}`,
    );
  }
  return {
    ...loaded,
    source: source.replace(FACT_SHEET_CALL, DATA_ONLY_REPLACEMENT),
  };
}
