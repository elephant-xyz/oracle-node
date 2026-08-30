/**
 * Persistent transform worker process.
 * Keeps cheerio and the 5 Hillsborough transform scripts pre-compiled in memory.
 */

const { readFileSync } = require("node:fs");
const { join, resolve } = require("node:path");
const vm = require("node:vm");

const ROOT = resolve(__dirname, "../..");
const TRANSFORMS_ROOT = resolve(ROOT, "../Counties-trasform-scripts");
const SCRIPT_DIR = resolve(TRANSFORMS_ROOT, "hillsborough/scripts");

const TRANSFORM_SCRIPTS = [
  "ownerMapping.js",
  "structureMapping.js",
  "utilityMapping.js",
  "layoutMapping.js",
  "data_extractor.js",
];

// Pre-compile all 5 scripts once into memory
const compiledScripts = TRANSFORM_SCRIPTS.map((name) => {
  const code = readFileSync(join(SCRIPT_DIR, name), "utf8");
  return {
    name,
    script: new vm.Script(code, { filename: join(SCRIPT_DIR, name) }),
  };
});

process.on("message", (msg) => {
  if (!msg || typeof msg !== "object") return;
  const { id, parcelDir } = msg;
  try {
    process.chdir(parcelDir);
    for (const { name, script } of compiledScripts) {
      const sandbox = {
        require,
        process,
        console: { log: () => {}, warn: () => {}, error: () => {}, info: () => {} },
        Buffer,
        setTimeout,
        clearTimeout,
        __dirname: SCRIPT_DIR,
        __filename: join(SCRIPT_DIR, name),
      };
      vm.createContext(sandbox);
      script.runInContext(sandbox);
    }
    if (process.send) {
      process.send({ id, ok: true });
    }
  } catch (err) {
    if (process.send) {
      process.send({
        id,
        ok: false,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
});
