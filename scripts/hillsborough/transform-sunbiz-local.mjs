#!/usr/bin/env node
/**
 * Transform local Hillsborough Sunbiz ZIP-extract JSONL chunks into lexicon
 * class JSONL suitable for elephant-query-db Sunbiz mapping.
 */

import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

import { transformSunbizRecord } from "../transform-sunbiz-corporate-to-lexicon.mjs";

/**
 * @typedef {object} TransformOptions
 * @property {string} inputDir - Directory containing corporate-by-zip chunks + manifest.
 * @property {string} outputDir - Lexicon output root.
 * @property {number | null} maxRecords - Optional record cap.
 */

/**
 * @param {readonly string[]} argv
 * @returns {TransformOptions}
 */
export function parseTransformLocalArgs(argv) {
  const { values } = parseArgs({
    args: [...argv],
    options: {
      "input-dir": { type: "string" },
      "output-dir": { type: "string" },
      "max-records": { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  const maxRaw = values["max-records"];
  return {
    inputDir:
      typeof values["input-dir"] === "string"
        ? values["input-dir"]
        : "downloads/hillsborough/sunbiz-pilot/corporate-by-zip",
    outputDir:
      typeof values["output-dir"] === "string"
        ? values["output-dir"]
        : "downloads/hillsborough/sunbiz-pilot/business-registration-v1",
    maxRecords:
      typeof maxRaw === "string" && Number(maxRaw) > 0
        ? Math.floor(Number(maxRaw))
        : null,
  };
}

/**
 * @param {TransformOptions} options
 * @returns {Promise<object>}
 */
export async function transformLocalSunbizExtract(options) {
  const chunksDir = path.join(options.inputDir, "chunks");
  const files = (await readdir(chunksDir))
    .filter((name) => name.endsWith(".jsonl"))
    .sort();

  const classDirs = {
    company: path.join(options.outputDir, "classes", "company"),
    address: path.join(options.outputDir, "classes", "address"),
    business_registration: path.join(
      options.outputDir,
      "classes",
      "business_registration",
    ),
    business_registration_address: path.join(
      options.outputDir,
      "classes",
      "business_registration_address",
    ),
    business_registration_party: path.join(
      options.outputDir,
      "classes",
      "business_registration_party",
    ),
  };
  for (const dir of Object.values(classDirs)) {
    await mkdir(dir, { recursive: true });
  }

  /** @type {Record<string, import('node:fs').WriteStream>} */
  const writers = {};
  for (const [classType, dir] of Object.entries(classDirs)) {
    writers[classType] = createWriteStream(path.join(dir, "part-0000.jsonl"), {
      encoding: "utf8",
    });
  }

  let sourceRecordCount = 0;
  let transformedRecordCount = 0;
  let invalidRecordCount = 0;

  /**
   * @param {string} classType
   * @param {object} record
   */
  async function writeClass(classType, record) {
    const writer = writers[classType];
    if (!writer) return;
    const ok = writer.write(`${JSON.stringify(record)}\n`);
    if (!ok) {
      await new Promise((resolve) => writer.once("drain", resolve));
    }
  }

  for (const fileName of files) {
    if (
      options.maxRecords !== null &&
      sourceRecordCount >= options.maxRecords
    ) {
      break;
    }
    const reader = createInterface({
      input: createReadStream(path.join(chunksDir, fileName), {
        encoding: "utf8",
      }),
      crlfDelay: Infinity,
    });
    for await (const line of reader) {
      if (
        options.maxRecords !== null &&
        sourceRecordCount >= options.maxRecords
      ) {
        break;
      }
      const trimmed = line.trim();
      if (!trimmed) continue;
      sourceRecordCount += 1;
      try {
        const parsed = JSON.parse(trimmed);
        const bundle = transformSunbizRecord(parsed, {
          sourceDataUri: parsed.sourceFileName ?? fileName,
        });
        for (const company of bundle.companies) {
          await writeClass("company", company);
        }
        for (const address of bundle.addresses) {
          await writeClass("address", address);
        }
        for (const registration of bundle.businessRegistrations) {
          await writeClass("business_registration", registration);
        }
        for (const bridge of bundle.businessRegistrationAddresses) {
          await writeClass("business_registration_address", bridge);
        }
        for (const party of bundle.businessRegistrationParties) {
          await writeClass("business_registration_party", party);
        }
        transformedRecordCount += 1;
      } catch {
        invalidRecordCount += 1;
      }
    }
  }

  await Promise.all(
    Object.values(writers).map(
      (writer) =>
        new Promise((resolve, reject) => {
          writer.end(() => resolve(undefined));
          writer.on("error", reject);
        }),
    ),
  );

  const summary = {
    sourceRecordCount,
    transformedRecordCount,
    invalidRecordCount,
    inputDir: options.inputDir,
    outputDir: options.outputDir,
  };
  await writeFile(
    path.join(options.outputDir, "summary.json"),
    `${JSON.stringify(summary, null, 2)}\n`,
  );
  return summary;
}

async function main() {
  const options = parseTransformLocalArgs(process.argv.slice(2));
  const summary = await transformLocalSunbizExtract(options);
  console.log(
    JSON.stringify(
      { event: "hillsborough_sunbiz_transform_finished", ...summary },
      null,
      2,
    ),
  );
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
