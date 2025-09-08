import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import { fileURLToPath } from "url";
import { spawn } from "child_process";

const s3 = new S3Client({});

/**
 * Run a command and capture stdout/stderr.
 * @param {string} cmd
 * @param {string[]} args
 * @param {string} cwd
 */
const run = (cmd, args, cwd) => new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { cwd });
    let stdout = ""; let stderr = "";
    child.stdout.on("data", d => stdout += d.toString());
    child.stderr.on("data", d => stderr += d.toString());
    child.on("close", code => {
        (code === 0) ? resolve({ stdout, stderr }) : reject(new Error(stderr || `exit ${code}`));
    });
});

/**
 * Resolve elephant-cli bin path packaged with this Lambda.
 * @returns {string}
 */
const resolveElephantCliBin = () => {
    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const bin = path.join(__dirname, "node_modules", ".bin", "elephant-cli");
    return bin;
};

/**
 * @param {any} event - { input: original input, prepare: prepare result }
 * @returns {Promise<{status:string}>}
 */
export const handler = async (event) => {
    const base = { component: "post", at: new Date().toISOString() };
    let tmp;
    try {
        if (!event?.prepare?.output_s3_uri) throw new Error("prepare.output_s3_uri missing");
        if (!event?.seed_output_s3_uri) throw new Error("seed_output_s3_uri missing");
        const outputBase = process.env.OUTPUT_BASE_URI;
        if (!outputBase) throw new Error("OUTPUT_BASE_URI is required");

        tmp = await fs.mkdtemp(path.join(os.tmpdir(), "post-"));
        const prepared = event.prepare.output_s3_uri;
        const seedZipS3 = event.seed_output_s3_uri;
        const parse = (u) => { const m = /^s3:\/\/([^/]+)\/(.*)$/.exec(u); if (!m) throw new Error("Bad S3 URI"); return { bucket: m[1], key: m[2] }; };
        const { bucket: pBucket, key: pKey } = parse(prepared);
        const { bucket: sBucket, key: sKey } = parse(seedZipS3);
        const countyZipLocal = path.join(tmp, "county_input.zip");
        const seedZipLocal = path.join(tmp, "seed_seed_output.zip");
        const pObj = await s3.send(new GetObjectCommand({ Bucket: pBucket, Key: pKey }));
        const sObj = await s3.send(new GetObjectCommand({ Bucket: sBucket, Key: sKey }));
        const cBytes = await pObj.Body?.transformToByteArray();
        const sBytes = await sObj.Body?.transformToByteArray();
        if (!cBytes || !sBytes) throw new Error("Failed to download required zips");
        await fs.writeFile(countyZipLocal, Buffer.from(cBytes));
        await fs.writeFile(seedZipLocal, Buffer.from(sBytes));

        // Transform county using elephant-cli. Use bundled transforms.zip if present
        const countyOut = path.join(tmp, "county_output.zip");
        const cli = resolveElephantCliBin();
        const maybeTransformsZip = path.join(path.dirname(fileURLToPath(import.meta.url)), "transforms.zip");
        const transformArgs = ["transform", "--input-zip", localZip, "--output-zip", countyOut];
        try { await fs.stat(maybeTransformsZip); transformArgs.push("--scripts-zip", maybeTransformsZip); } catch { }
        await run(cli, transformArgs, tmp);
        // Validate
        await run(cli, ["validate", countyOut], tmp);

        // Hash seed and county; derive propertyCid from seed CSV if available
        const hashZip = path.join(tmp, "combined_hash.zip");
        const hashCsv = path.join(tmp, "combined_hash.csv");
        const seedHashZip = path.join(tmp, "seed_hash.zip");
        const seedHashCsv = path.join(tmp, "seed_hash.csv");
        await run(cli, ["hash", seedZipLocal, "--output-zip", seedHashZip, "--output-csv", seedHashCsv], tmp);
        let propertyCid;
        try {
            const csv = await fs.readFile(seedHashCsv, "utf8");
            propertyCid = /propertyCid,?\n([^,\n]+)/.exec(csv)?.[1];
        } catch { }
        await run(cli, ["hash", countyOut, "--output-zip", hashZip, "--output-csv", hashCsv, ...(propertyCid ? ["--property-cid", propertyCid] : [])], tmp);

        // Combine CSVs
        const combinedCsv = path.join(tmp, "combined_hash.csv");
        const seedCsvContent = await fs.readFile(seedHashCsv);
        const countyCsvContent = await fs.readFile(hashCsv);
        await fs.writeFile(combinedCsv, Buffer.concat([seedCsvContent, countyCsvContent]));

        // Prepare submission CSV
        const uploadResults = combinedCsv;
        const inputKey = path.posix.basename(event?.Records?.[0]?.s3?.object?.key || "input.csv");
        const runPrefix = `${outputBase.replace(/\/$/, "")}/${inputKey}`;
        const submissionKey = `${runPrefix}/submission_ready.csv`;
        const [outBucket, ...outKeyParts] = runPrefix.replace(/^s3:\/\//, "").split("/");
        const outKeyPrefix = outKeyParts.join("/");
        const submissionS3 = { Bucket: outBucket, Key: `${outKeyPrefix}/submission_ready.csv` };
        const submissionBody = await fs.readFile(uploadResults);
        await s3.send(new PutObjectCommand({ ...submissionS3, Body: submissionBody }));

        // Submit to blockchain
        const reqVars = ["ELEPHANT_DOMAIN", "ELEPHANT_API_KEY", "ELEPHANT_ORACLE_KEY_ID", "ELEPHANT_FROM_ADDRESS", "ELEPHANT_RPC_URL"];
        for (const v of reqVars) if (!process.env[v]) throw new Error(`${v} required`);
        await run(cli, [
            "submit-to-contract", uploadResults,
            "--domain", process.env.ELEPHANT_DOMAIN,
            "--api-key", process.env.ELEPHANT_API_KEY,
            "--oracle-key-id", process.env.ELEPHANT_ORACLE_KEY_ID,
            "--from-address", process.env.ELEPHANT_FROM_ADDRESS,
            "--rpc-url", process.env.ELEPHANT_RPC_URL,
        ], tmp);

        console.log(JSON.stringify({ ...base, level: "info", msg: "completed" }));
        return { status: "success" };
    } catch (err) {
        console.error(JSON.stringify({ ...base, level: "error", msg: "failed", error: String(err) }));
        throw err;
    } finally {
        try { if (tmp) await fs.rm(tmp, { recursive: true, force: true }); } catch { }
    }
};


