import {
  GetObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import path from "path";

/**
 * @typedef {object} S3Object
 * @property {string} key
 *
 * @typedef {object} S3Bucket
 * @property {string} name
 *
 * @typedef {object} S3Event
 * @property {{ bucket: S3Bucket, object: S3Object }} s3
 *
 * @typedef {Object} PropertyImprovementPreOutput
 * @property {string} input_csv_s3_uri - S3 URI of input CSV file
 * @property {string} output_prefix - S3 URI prefix where outputs should be written
 * @property {string} county_name - County jurisdiction name extracted from CSV or metadata
 * @property {string} county_key - County key derived from `county_name` by replacing spaces with underscores
 */

const s3 = new S3Client({});

/**
 * Extract county name from CSV filename or metadata
 * For property improvement, we expect the county name to be part of the S3 key or metadata
 *
 * @param {string} key - S3 object key
 * @param {string} bucket - S3 bucket name
 * @returns {Promise<string>} - County name
 */
async function extractCountyName(key, bucket) {
  // Try to extract from filename pattern (e.g., "lee-county-improvements.csv")
  const basename = path.posix.basename(key, path.extname(key));
  const match = basename.match(/^([a-z-]+)-county/i);
  if (match) {
    // Convert "lee" to "Lee", "miami-dade" to "Miami Dade"
    const countyName = match[1]
      .split("-")
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ");
    return countyName;
  }

  // Fallback: try to get from S3 object metadata
  try {
    const headResponse = await s3.send(
      new GetObjectCommand({ Bucket: bucket, Key: key }),
    );
    if (headResponse.Metadata?.county) {
      return headResponse.Metadata.county;
    }
  } catch (error) {
    console.warn(
      `Could not retrieve metadata for county extraction: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  // Default fallback
  return "Lee";
}

/**
 * Build paths and derive prepare input from SQS/S3 event message for property improvement workflow.
 * - Extracts bucket/key from S3 event in SQS body
 * - Produces output prefix and input path for prepare Lambda
 *
 * @param {S3Event} event - Original SQS body JSON (already parsed by starter)
 * @returns {Promise<PropertyImprovementPreOutput>}
 */
export const handler = async (event) => {
  console.log("Property Improvement Pre-processor Event:", event);
  const base = {
    component: "property-improvement-pre",
    at: new Date().toISOString(),
  };

  try {
    const rec = event;
    if (!rec?.s3?.bucket?.name || !rec?.s3?.object?.key) {
      throw new Error("Missing S3 bucket/key in message");
    }

    const bucket = rec.s3.bucket.name;
    const key = decodeURIComponent(rec.s3.object.key.replace(/\+/g, " "));
    const fileBase = path.posix.basename(key, path.extname(key));

    // Build output prefix
    const outputPrefix =
      (process.env.OUTPUT_BASE_URI || `s3://${bucket}/outputs`).replace(
        /\/$/,
        "",
      ) + `/${fileBase}`;

    // Input CSV S3 URI (this will be downloaded by prepare Lambda)
    const inputCsvS3Uri = `s3://${bucket}/${key}`;

    // Extract county name from filename or metadata
    const countyName = await extractCountyName(key, bucket);
    const countyKey = countyName.replace(/ /g, "_");

    const out = {
      input_csv_s3_uri: inputCsvS3Uri,
      output_prefix: outputPrefix,
      county_name: countyName,
      county_key: countyKey,
    };

    console.log(
      JSON.stringify({
        ...base,
        level: "info",
        msg: "property_improvement_preprocessed",
        out,
      }),
    );

    return out;
  } catch (err) {
    console.error(
      JSON.stringify({
        ...base,
        level: "error",
        msg: "failed",
        error: String(err),
      }),
    );
    throw err;
  }
};





