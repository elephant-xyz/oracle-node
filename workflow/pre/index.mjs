import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import AdmZip from "adm-zip";

/**
 * @typedef {Object} PreOutput
 * @property {string} input_s3_uri - S3 URI of input created for prepare step
 * @property {string} output_prefix - S3 URI prefix where prepare should write
 */

const s3 = new S3Client({});

const resolveElephantCliBin = () => {
    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    return path.join(__dirname, "node_modules", ".bin", "elephant-cli");
};

/**
 * Build paths and derive prepare input from SQS/S3 event message.
 * - Extracts bucket/key from S3 event in SQS body
 * - Produces output prefix and input path for prepare Lambda
 *
 * @param {any} event - Original SQS body JSON (already parsed by starter)
 * @returns {Promise<PreOutput>}
 */
export const handler = async (event) => {
    const base = { component: "pre", at: new Date().toISOString() };
    let tmp;
    try {
        const rec = event?.Records?.[0];
        if (!rec?.s3?.bucket?.name || !rec?.s3?.object?.key) {
            throw new Error("Missing S3 bucket/key in message");
        }
        const bucket = rec.s3.bucket.name;
        const key = decodeURIComponent(rec.s3.object.key.replace(/\+/g, " "));
        const fileBase = path.posix.basename(key, path.extname(key));
        const outputPrefix = (process.env.OUTPUT_BASE_URI || `s3://${bucket}/outputs`).replace(/\/$/, "") + `/${fileBase}`;
        const inputS3Uri = `s3://${bucket}/${key}`;

        // Download CSV and create seed input.zip
        tmp = await fs.mkdtemp(path.join(os.tmpdir(), "pre-"));
        const csvPath = path.join(tmp, "seed.csv");
        const get = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
        const csvBytes = await get.Body?.transformToByteArray();
        if (!csvBytes) throw new Error("Failed to download input CSV");
        await fs.writeFile(csvPath, Buffer.from(csvBytes));
        const seedInputZip = path.join(tmp, "input.zip");
        const inZip = new AdmZip();
        inZip.addLocalFile(csvPath, "", "seed.csv");
        inZip.writeZip(seedInputZip);

        // Transform and validate seed
        const seedOutputZip = path.join(tmp, "seed_seed_output.zip");
        const cli = resolveElephantCliBin();
        const { spawn } = await import("child_process");
        const run = (args) => new Promise((resolve, reject) => {
            const p = spawn(resolveElephantCliBin(), args, { cwd: tmp });
            let stderr = "";
            p.stderr.on("data", d => stderr += d.toString());
            p.on("close", c => c === 0 ? resolve() : reject(new Error(stderr || `exit ${c}`)));
        });
        await run(["transform", "--input-zip", seedInputZip, "--output-zip", seedOutputZip]);
        await run(["validate", seedOutputZip]);

        // Upload seed output
        const runPrefix = outputPrefix;
        const seedOutKey = `${runPrefix.replace(/^s3:\/\//, "")}/seed_seed_output.zip`;
        const [outBucket, ...parts] = seedOutKey.split("/");
        const seedKey = parts.join("/");
        const seedBody = await fs.readFile(seedOutputZip);
        await s3.send(new PutObjectCommand({ Bucket: outBucket, Key: seedKey, Body: seedBody }));

        // Build county prep input.zip from seed output
        const seedExtractDir = path.join(tmp, "seed_extract");
        await fs.mkdir(seedExtractDir, { recursive: true });
        const seedZip = new AdmZip(seedOutputZip);
        seedZip.extractAllTo(seedExtractDir, true);
        const countyPrepZip = path.join(tmp, "county_prep_input.zip");
        const addr = path.join(seedExtractDir, "data", "unnormalized_address.json");
        const seed = path.join(seedExtractDir, "data", "property_seed.json");
        const outZip = new AdmZip();
        outZip.addLocalFile(addr, "", "unnormalized_address.json");
        outZip.addLocalFile(seed, "", "property_seed.json");
        outZip.writeZip(countyPrepZip);
        const countyPrepKeyFull = `${runPrefix.replace(/^s3:\/\//, "")}/county_prep/input.zip`;
        const countyBody = await fs.readFile(countyPrepZip);
        const [cpBucket, ...cpParts] = countyPrepKeyFull.split("/");
        await s3.send(new PutObjectCommand({ Bucket: cpBucket, Key: cpParts.join("/"), Body: countyBody }));

        const out = {
            input_s3_uri: inputS3Uri,
            output_prefix: outputPrefix,
            seed_output_s3_uri: `s3://${outBucket}/${seedKey}`,
            county_prep_input_s3_uri: `s3://${cpBucket}/${cpParts.join("/")}`
        };
        console.log(JSON.stringify({ ...base, level: "info", msg: "prepared", out }));
        return out;
    } catch (err) {
        console.error(JSON.stringify({ ...base, level: "error", msg: "failed", error: String(err) }));
        throw err;
    } finally {
        try { if (tmp) await fs.rm(tmp, { recursive: true, force: true }); } catch { }
    }
};


