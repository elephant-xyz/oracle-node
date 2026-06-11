import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { describe, expect, it } from "vitest";

const execFileAsync = promisify(execFile);

const DATA_EXTRACTOR_SCRIPT = resolve(
  "transform/lee/scripts/data_extractor.js",
);

/**
 * Run the Lee data extractor in an isolated directory with a minimal parcel page.
 *
 * @param {string} html - Lee parcel HTML fixture to write as `input.html`.
 * @param {Record<string, string>} [extraFiles] - Additional fixture files keyed by filename.
 * @returns {Promise<string>} Temporary directory containing extractor output.
 */
async function runExtractor(html, extraFiles = {}) {
  const tempDir = await mkdtemp(join(tmpdir(), "lee-data-extractor-test-"));
  await writeFile(
    join(tempDir, "property_seed.json"),
    JSON.stringify({
      request_identifier: "10211376",
      parcel_id: "24452400000050020",
      source_http_request: {
        method: "GET",
        url: "https://leepa.org/Display/DisplayParcel.aspx",
      },
    }),
    "utf8",
  );
  await writeFile(
    join(tempDir, "unnormalized_address.json"),
    JSON.stringify({
      request_identifier: "10211376",
      full_address: "5255 BIG PINE WAY, FORT MYERS, FL 33907",
      latitude: 26.5528,
      longitude: -81.86819,
    }),
    "utf8",
  );
  await writeFile(join(tempDir, "input.html"), html, "utf8");
  for (const [fileName, fileContent] of Object.entries(extraFiles)) {
    await writeFile(join(tempDir, fileName), fileContent, "utf8");
  }
  await execFileAsync(process.execPath, [DATA_EXTRACTOR_SCRIPT], {
    cwd: tempDir,
    timeout: 20_000,
  });
  return tempDir;
}

/**
 * Read an output JSON file from the extractor's `data` directory.
 *
 * @param {string} tempDir - Temporary extractor work directory.
 * @param {string} fileName - File name below `data/`.
 * @returns {Promise<Record<string, unknown>>} Parsed JSON object.
 */
async function readDataJson(tempDir, fileName) {
  const parsed = JSON.parse(
    await readFile(join(tempDir, "data", fileName), "utf8"),
  );
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${fileName} did not contain a JSON object`);
  }
  return parsed;
}

describe("Lee data extractor", () => {
  it("extracts tax values from certified roll NAL fields when valueGrid is absent", async () => {
    const html = `<!doctype html>
      <html>
        <body>
          <div id="parcelLabel">STRAP: 24-45-24-00-00005.0020 Folio ID: 10211376</div>
          <select id="selectTaxYear">
            <option selected value="/Display/DisplayParcel.aspx?FolioID=10211376&amp;TaxYear=2025">2025</option>
          </select>
          <table class="appraisalDetails">
            <tr><th>1st Year Building on Tax Roll</th><td>1997</td></tr>
          </table>
          <div id="TaxRollDiv">
            Certified Roll Data NAL File Data FieldsRecord Created: 10/4/2025
            F01: 46F02: 24452400000050020F03: RF04: 2025F05: 039
            F08: 18520063F09: -6071914F10: 06
            F11: 18520063F12: 17999709F13: 18520063F14: 17999709
            F15: 0F21: 0F38: 4135580F39: 2F45: 1997F92: 194668
          </div>
        </body>
      </html>`;

    const tempDir = await runExtractor(html);
    try {
      const tax = await readDataJson(tempDir, "tax_2025.json");
      const relationship = await readDataJson(
        tempDir,
        "relationship_property_tax_1.json",
      );

      expect(tax.tax_year).toBe(2025);
      expect(tax.property_market_value_amount).toBe(18_520_063);
      expect(tax.property_assessed_value_amount).toBe(17_999_709);
      expect(tax.property_taxable_value_amount).toBe(17_999_709);
      expect(tax.school_taxable_value_amount).toBe(18_520_063);
      expect(tax.county_taxable_value_amount).toBe(17_999_709);
      expect(tax.property_land_amount).toBe(4_135_580);
      expect(tax.property_building_amount).toBe(14_384_483);
      expect(tax.first_year_building_on_tax_roll).toBe(1997);
      expect(tax.nal_fields).toMatchObject({
        F04: "2025",
        F08: "18520063",
        F38: "4135580",
      });
      expect(relationship).toEqual({
        from: { "/": "./property.json" },
        to: { "/": "./tax_2025.json" },
      });
    } finally {
      await rm(tempDir, { recursive: true, force: true });
    }
  });

  it("falls back to linked cost-card value summary when certified roll data is unavailable", async () => {
    const detailHtml = `<!doctype html>
      <html>
        <body>
          <div id="parcelLabel">STRAP: 31-45-24-25-00000.0010 Folio ID: 10217341</div>
          <select id="selectTaxYear">
            <option selected value="/Display/DisplayParcel.aspx?FolioID=10217341&amp;TaxYear=2025">2025</option>
          </select>
          <div id="TaxRollDiv">
            <div class="certifiedRollData">Certified Roll Data not found for this parcel.</div>
          </div>
        </body>
      </html>`;
    const costCardHtml = `<!doctype html>
      <html>
        <body>
          <h1>LEE COUNTY PROPERTY APPRAISER</h1>
          <div>VALUE SUMMARY</div>
          <div>Current Values</div>
          <div>2025</div>
          <div>BUILDING COST VALUE</div>
          <div>*</div>
          <div>*</div>
          <div>BUILDING EXTRA FEATURES</div>
          <div>*</div>
          <div>*</div>
          <div>LAND EXTRA FEATURES</div>
          <div>*</div>
          <div>*</div>
          <div>LAND VALUE</div>
          <div>*</div>
          <div>349,350</div>
          <div>COST APPROACH VALUE</div>
          <div>*</div>
          <div>*</div>
          <div>INCOME APPROACH VALUE</div>
          <div>700,898</div>
          <div>700,898</div>
          <div>SALES APPROACH VALUE</div>
          <div>*</div>
          <div>*</div>
          <div>MARKET VALUE</div>
          <div>*</div>
          <div>700,898</div>
        </body>
      </html>`;

    const tempDir = await runExtractor(detailHtml, {
      "cost_card.html": costCardHtml,
      "cost_card_source_http_request.json": JSON.stringify({
        method: "GET",
        url: "http://fieldcards.leepa.org/CurrentCostCard/Folio/10217341",
      }),
    });
    try {
      const tax = await readDataJson(tempDir, "tax_2025.json");

      expect(tax.tax_year).toBe(2025);
      expect(tax.property_market_value_amount).toBe(700_898);
      expect(tax.property_land_amount).toBe(349_350);
      expect(tax.property_building_amount).toBe(351_548);
      expect(tax.income_approach_value_amount).toBe(700_898);
      expect(tax.source_cost_card_file_name).toBe("cost_card.html");
      expect(tax.source_http_request).toEqual({
        method: "GET",
        url: "http://fieldcards.leepa.org/CurrentCostCard/Folio/10217341",
      });
      expect(tax.source_extraction_method).toBe(
        "lee_cost_card_value_summary",
      );
    } finally {
      await rm(tempDir, { recursive: true, force: true });
    }
  });
});
