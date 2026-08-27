/**
 * Hillsborough County local-pilot helpers.
 * @module scripts/hillsborough/lib
 */

/**
 * @typedef {object} HillsboroughSeedRow
 * @property {string} parcel_id
 * @property {string} source_identifier
 * @property {string} [folio]
 * @property {string} [display_folio]
 * @property {string} [pin]
 * @property {string} [display_pin]
 * @property {string} [address]
 * @property {string} [street]
 * @property {string} [city]
 * @property {string} [zip]
 * @property {string} [owner]
 * @property {string} [land_use]
 * @property {string} [longitude]
 * @property {string} [latitude]
 * @property {string} [parcel_polygon]
 */

export const HCPA_SEARCH_BASE =
  "https://gis.hcpafl.org/CommonServices/property/search/";

export const HCPA_PARCEL_DATA_URL = `${HCPA_SEARCH_BASE}ParcelData`;

export const GIS_PARCELS_URL =
  "https://maps.hillsboroughcounty.org/arcgis/rest/services/InfoLayers/HC_ParcelsPublic/FeatureServer/0/query";

export const COUNTY_NAME = "Hillsborough";
export const STATE_CODE = "FL";
export const JURISDICTION_KEY = "hillsborough_appraiser";

/**
 * @param {string[]} argv
 * @returns {{
 *   load: boolean;
 *   permits: boolean;
 *   limit: number;
 *   seedPath: string | null;
 *   outputRoot: string | null;
 *   skipExisting: boolean;
 *   concurrency: number;
 * }}
 */
export function parsePilotArgs(argv) {
  return {
    load: argv.includes("--load"),
    permits: argv.includes("--permits"),
    limit: Number.parseInt(
      argv.find((arg) => arg.startsWith("--limit="))?.split("=")[1] ?? "50",
      10,
    ),
    seedPath: argv.find((arg) => arg.startsWith("--seed="))?.split("=")[1] ?? null,
    outputRoot:
      argv.find((arg) => arg.startsWith("--output="))?.split("=")[1] ?? null,
    skipExisting: argv.includes("--skip-existing"),
    concurrency: Number.parseInt(
      argv.find((arg) => arg.startsWith("--concurrency="))?.split("=")[1] ?? "2",
      10,
    ),
  };
}

/**
 * Minimal CSV parser for the pilot seed (handles quoted fields).
 * @param {string} text
 * @returns {HillsboroughSeedRow[]}
 */
export function parseSeedCsvText(text) {
  const rows = [];
  /** @type {string[]} */
  const lines = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    const next = text[i + 1];
    if (ch === '"') {
      // Keep quotes in the line buffer; splitCsvLine owns unquoting.
      current += ch;
      if (inQuotes && next === '"') {
        current += next;
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (ch === "\r" && next === "\n") i += 1;
      lines.push(current);
      current = "";
      continue;
    }
    current += ch;
  }
  if (current.length > 0) lines.push(current);

  if (lines.length === 0) return rows;
  const header = splitCsvLine(lines[0]);
  for (let i = 1; i < lines.length; i += 1) {
    if (!lines[i].trim()) continue;
    const cols = splitCsvLine(lines[i]);
    /** @type {Record<string, string>} */
    const row = {};
    for (let c = 0; c < header.length; c += 1) {
      row[header[c]] = cols[c] ?? "";
    }
    if (!row.parcel_id) continue;
    rows.push(/** @type {HillsboroughSeedRow} */ (row));
  }
  return rows;
}

/**
 * @param {string} line
 * @returns {string[]}
 */
function splitCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    const next = line[i + 1];
    if (ch === '"') {
      if (inQuotes && next === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out;
}

/**
 * Escape text for HTML text nodes.
 * @param {unknown} value
 * @returns {string}
 */
export function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/**
 * Format land use the way the hillsborough transform mapping expects.
 * @param {{ code?: string, description?: string } | null | undefined} landUse
 * @returns {string}
 */
export function formatPropertyUse(landUse) {
  if (!landUse) return "";
  const code = String(landUse.code ?? "").trim();
  const description = String(landUse.description ?? "").trim();
  return `${code} ${description}`.trim();
}

/**
 * Format mailing address block from ParcelData.mailingAddress.
 * @param {{
 *   addr1?: string,
 *   addr2?: string,
 *   city?: string,
 *   state?: string,
 *   zip?: string,
 * } | null | undefined} mailing
 * @returns {string}
 */
export function formatMailingAddress(mailing) {
  if (!mailing) return "";
  const lines = [
    mailing.addr1,
    mailing.addr2,
    [mailing.city, mailing.state, mailing.zip].filter(Boolean).join(" "),
  ]
    .map((x) => String(x ?? "").trim())
    .filter(Boolean);
  return lines.join("\n");
}

/**
 * Build an HTML document that satisfies the Hillsborough cheerio transform selectors.
 * @param {Record<string, unknown>} parcel
 * @returns {string}
 */
export function buildInputHtmlFromParcelData(parcel) {
  const pc =
    /** @type {Record<string, unknown>} */ (parcel.propertyCard || {});
  const landUse =
    /** @type {{ code?: string, description?: string }} */ (
      parcel.landUse || pc.landUse || {}
    );
  const subdivision =
    /** @type {{ code?: string, description?: string }} */ (
      parcel.subdivision || pc.subdivision || {}
    );
  const displayStrap = String(
    pc.displayStrap || parcel.pin || "",
  );
  const propertyUse = formatPropertyUse(landUse);
  const siteAddress = String(parcel.siteAddress || "");
  const mailing = formatMailingAddress(
    /** @type {Parameters<typeof formatMailingAddress>[0]} */ (
      parcel.mailingAddress
    ),
  );
  const owner = String(parcel.owner || "").replace(/;\s*$/, "");
  const legal =
    String(parcel.fullLegal || pc.legalDescription || parcel.shortLegal || "");

  const valueSummary = Array.isArray(parcel.valueSummary)
    ? parcel.valueSummary
    : [];
  const salesHistory = Array.isArray(parcel.salesHistory)
    ? parcel.salesHistory
    : [];
  const permitInfo = Array.isArray(parcel.permitInfo) ? parcel.permitInfo : [];
  const landLines = Array.isArray(parcel.landLines) ? parcel.landLines : [];
  const buildings = Array.isArray(parcel.buildings) ? parcel.buildings : [];

  const taxYear =
    pc.current && typeof pc.current === "object" && "_date" in pc.current
      ? String(/** @type {{ _date?: string }} */ (pc.current)._date || "").slice(
          -4,
        )
      : "2025";

  const valueRows = valueSummary
    .map((row) => {
      const r = /** @type {Record<string, unknown>} */ (row);
      return `<tr>
  <td>${escapeHtml(r.taxDist)}</td>
  <td>$${Number(r.marketVal || 0).toLocaleString("en-US")}</td>
  <td>$${Number(r.assessedVal || 0).toLocaleString("en-US")}</td>
  <td>$${Number(r.exemptions || 0).toLocaleString("en-US")}</td>
  <td>$${Number(r.taxableVal || 0).toLocaleString("en-US")}</td>
</tr>`;
    })
    .join("\n");

  const salesRows = salesHistory
    .map((sale) => {
      const s = /** @type {Record<string, unknown>} */ (sale);
      const saleDate = String(s.saleDate || "");
      const [year, month] = saleDate.split("-");
      const bookPage = `${s.book || ""}/${s.page || ""}`;
      return `<tr>
  <td><a href="https://publicaccess.hillsclerk.com/">${escapeHtml(bookPage)}</a></td>
  <td><a href="#">${escapeHtml(s.docnum || "")}</a></td>
  <td>${escapeHtml(month || "")}</td>
  <td>${escapeHtml(year || "")}</td>
  <td>$${Number(s.salePrice || 0).toLocaleString("en-US")}</td>
  <td>${escapeHtml(s.deedType || "")}</td>
  <td>${escapeHtml(s.qualified || "")}</td>
  <td>${escapeHtml(s.vacOrImp || "")}</td>
</tr>`;
    })
    .join("\n");

  const permitRows = permitInfo
    .map((p) => {
      const row = /** @type {Record<string, unknown>} */ (p);
      return `<tr>
  <td>${escapeHtml(row.issueDate)}</td>
  <td><a href="${escapeHtml(row.permitUrl || "#")}">${escapeHtml(row.permitNum)}</a></td>
  <td>${escapeHtml(row.permitType)}</td>
  <td>${escapeHtml(row.descr)}</td>
  <td>${escapeHtml(row.estValue)}</td>
</tr>`;
    })
    .join("\n");

  const landRows = landLines
    .map((line) => {
      const l = /** @type {Record<string, unknown>} */ (line);
      const lt =
        /** @type {{ description?: string }} */ (l.landType || {});
      return `<tr>
  <td><span data-bind="text: publicLandType">${escapeHtml(lt.description || "")}</span></td>
  <td><span data-bind="text: publicUnits">${escapeHtml(l.units)}</span></td>
  <td><span data-bind="text: frontage">${escapeHtml(l.frontage)}</span></td>
  <td><span data-bind="text: depth">${escapeHtml(l.depth)}</span></td>
</tr>`;
    })
    .join("\n");

  const buildingBlocks = buildings
    .map((bldg, idx) => {
      const b = /** @type {Record<string, unknown>} */ (bldg);
      const construction = Array.isArray(b.constructionInfo)
        ? b.constructionInfo
        : [];
      const charRows = construction
        .map((ci) => {
          const c = /** @type {Record<string, unknown>} */ (ci);
          const element =
            /** @type {{ description?: string, code?: string }} */ (
              c.element || {}
            );
          const detail =
            /** @type {{ description?: string, code?: string }} */ (
              c.constructionDetail || {}
            );
          return `<tr>
  <td>${escapeHtml(element.description || element.code || "")}</td>
  <td>${escapeHtml(detail.code || "")}</td>
  <td>${escapeHtml(detail.description || "")}</td>
</tr>`;
        })
        .join("\n");
      const heated =
        b.heatedArea != null
          ? b.heatedArea
          : b.totalArea != null
            ? b.totalArea
            : "";
      return `
<h4 class="section-header">Building ${idx + 1}</h4>
<div class="section-wrap">
  <table><tbody>
    <tr><td>Actual Year Built</td><td></td><td>${escapeHtml(b.actualYearBuilt || b.yearBuilt || "")}</td></tr>
    <tr><td>Effective Year Built</td><td></td><td>${escapeHtml(b.effectiveYearBuilt || "")}</td></tr>
    <tr><td>Bedrooms</td><td></td><td>${escapeHtml(b.bedrooms)}</td></tr>
    <tr><td>Bathrooms</td><td></td><td>${escapeHtml(b.bathrooms)}</td></tr>
    <tr><td>Stories</td><td></td><td>${escapeHtml(b.stories)}</td></tr>
    <tr><td>Heated Area</td><td></td><td>${escapeHtml(heated)}</td></tr>
    ${charRows}
  </tbody></table>
  <table class="subareas"><tbody>
    <tr class="totals"><td>Total</td><td>${escapeHtml(heated)}</td></tr>
  </tbody></table>
</div>`;
    })
    .join("\n");

  const legalRows = legal
    .split(/\s{2,}|\n/)
    .filter(Boolean)
    .slice(0, 20)
    .map((line) => `<tr><td></td><td>${escapeHtml(line)}</td></tr>`)
    .join("\n");

  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8" /><title>Hillsborough Parcel ${escapeHtml(displayStrap)}</title></head>
<body>
  <h4 data-bind="html: publicOwner">${escapeHtml(owner).replace(/\n/g, "<br/>")}</h4>
  <table>
    <tr><td>PIN:</td><td data-bind="text: displayStrap">${escapeHtml(displayStrap)}</td></tr>
    <tr><td>Property Use:</td><td>${escapeHtml(propertyUse)}</td></tr>
    <tr><td>Subdivision:</td><td>${escapeHtml(subdivision.description || subdivision.code || "")}</td></tr>
  </table>
  <h5>Site Address</h5>
  <p>${escapeHtml(siteAddress)}</p>
  <h5>Mailing Address</h5>
  <p>${escapeHtml(mailing).replace(/\n/g, "<br/>")}</p>
  <table><tbody data-bind="foreach: fullLegal">${legalRows || `<tr><td></td><td>${escapeHtml(legal)}</td></tr>`}</tbody></table>

  <div class="value-summary-years"><span data-bind="text: displayedTaxYear">${escapeHtml(taxYear)}</span></div>
  <h4 class="section-header">Value Summary</h4>
  <div>
    <table><tbody>
      ${valueRows}
    </tbody></table>
  </div>

  <h4>Sales History</h4>
  <div>
    <table><tbody>
      ${salesRows}
    </tbody></table>
  </div>

  <table class="permitinfo"><tbody>
    ${permitRows}
  </tbody></table>

  <h4 class="section-header">Land</h4>
  <div>
    <table><tbody>
      ${landRows}
    </tbody></table>
  </div>

  <div data-bind="foreach: buildings()">
    ${buildingBlocks}
  </div>
</body>
</html>`;
}
