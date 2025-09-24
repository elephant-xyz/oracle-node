const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

// Load HTML
const inputPath = path.resolve("input.html");
const html = fs.readFileSync(inputPath, "utf8");
const $ = cheerio.load(html);

// Utilities
const companyIndicators = [
  "inc",
  "llc",
  "l.l.c",
  "ltd",
  "foundation",
  "alliance",
  "solutions",
  "corp",
  "corporation",
  "co",
  "company",
  "services",
  "trust",
  " tr",
  " bank",
  "association",
  "assn",
  "hoa",
  "church",
  "ministries",
  "ministry",
  "univ",
  "university",
  "college",
  "partners",
  "partner",
  "lp",
  "llp",
  "pllc",
  "holdings",
  "group",
];

function toTitleCase(str) {
  return str
    .toLowerCase()
    .replace(
      /\b([A-Za-z][A-Za-z'\-]*)/g,
      (m) => m.charAt(0).toUpperCase() + m.slice(1),
    );
}

function normalizeSpace(s) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function isCompany(nameRaw) {
  const n = " " + nameRaw.toLowerCase() + " ";
  return companyIndicators.some((ind) => n.includes(" " + ind + " "));
}

function stripSuffixes(s) {
  // Remove common suffixes like JR, SR, II, III at end of segment
  return s.replace(/\b(JR|SR|II|III|IV|V)\.?$/i, "").trim();
}

function splitCompoundAndNames(segment) {
  // Handles patterns like "John & Jane Smith" => ["John Smith", "Jane Smith"]
  const seg = normalizeSpace(segment);
  if (!seg) return [];
  // If comma present, treat as separate owner entry; return as-is
  if (seg.includes(",")) return [seg];
  // If contains ampersand, try to expand
  if (seg.includes("&")) {
    const parts = seg.split("&").map((p) => normalizeSpace(p));
    const lastToken = parts[parts.length - 1].split(" ").filter(Boolean);
    const lastName =
      lastToken.length > 1 ? lastToken[lastToken.length - 1] : "";
    const results = [];
    if (lastName) {
      // Attach lastName to any first-only tokens
      for (let i = 0; i < parts.length; i++) {
        const tokens = parts[i].split(" ").filter(Boolean);
        if (tokens.length === 1) {
          results.push(tokens[0] + " " + lastName);
        } else {
          results.push(parts[i]);
        }
      }
      return results.map(normalizeSpace);
    }
    return parts.map(normalizeSpace);
  }
  return [seg];
}

function classifyPersonName(name) {
  let first = "",
    middle = "",
    last = "";
  const s = normalizeSpace(name);
  if (!s) return null;
  if (s.includes(",")) {
    // Format: LAST, FIRST [MIDDLE]
    const parts = s.split(",");
    last = stripSuffixes(parts[0].trim());
    const rest = normalizeSpace(parts.slice(1).join(","))
      .split(" ")
      .filter(Boolean);
    if (rest.length >= 1) {
      first = rest[0];
      if (rest.length > 1) middle = rest.slice(1).join(" ");
    }
  } else {
    // Format: FIRST [MIDDLE] LAST
    const tokens = s.split(" ").filter(Boolean);
    if (tokens.length === 1) {
      // Single token cannot confidently classify as person
      return null;
    }
    first = tokens[0];
    last = stripSuffixes(tokens[tokens.length - 1]);
    if (tokens.length > 2) middle = tokens.slice(1, -1).join(" ");
  }
  first = toTitleCase(first);
  last = toTitleCase(last);
  middle = normalizeSpace(middle);
  const obj = { type: "person", first_name: first, last_name: last };
  if (middle) obj.middle_name = toTitleCase(middle);
  if (!obj.first_name || !obj.last_name) return null;
  return obj;
}

function classifyOwner(name) {
  const raw = normalizeSpace(name);
  if (!raw) return { owner: null, invalidReason: "empty" };
  if (isCompany(raw)) {
    return { owner: { type: "company", name: toTitleCase(raw) } };
  }
  const person = classifyPersonName(raw);
  if (person) return { owner: person };
  return { owner: null, invalidReason: "unclassifiable" };
}

function extractPropertyId($) {
  let id = (
    $("#hfAccount").attr("value") ||
    $("#hfAccount").val() ||
    ""
  ).trim();
  if (!id) {
    const t = normalizeSpace($("#divPropertySearch_Details_Account").text());
    const m = t.match(/Account:\s*(\d{4,})/i);
    if (m) id = m[1];
  }
  return id || "unknown_id";
}

function extractOwnerStrings($) {
  const texts = [];
  // Primary, most reliable source
  $('[data-bind="text: publicOwners"]').each((i, el) => {
    const t = normalizeSpace($(el).text());
    if (t) texts.push(t);
  });
  if (texts.length > 0) {
    const seen = new Set();
    const unique = [];
    texts.forEach((t) => {
      const k = t.toLowerCase();
      if (!seen.has(k)) {
        seen.add(k);
        unique.push(t);
      }
    });
    return unique;
  }
  // Fallback: based on label. Remove notice nodes inside the cell before reading text.
  $(".cssDetails_Top_Row").each((i, row) => {
    const label = normalizeSpace(
      $(row).find(".cssDetails_Top_Cell_Label").text(),
    ).toLowerCase();
    if (label.includes("owner")) {
      const cell = $(row).find(".cssDetails_Top_Cell_Data").first().clone();
      cell.find(".cssDetails_Top_Notice").remove();
      const data = normalizeSpace(cell.text());
      if (data) texts.push(data);
    }
  });
  const seen = new Set();
  const unique = [];
  texts.forEach((t) => {
    const k = t.toLowerCase();
    if (!seen.has(k)) {
      seen.add(k);
      unique.push(t);
    }
  });
  return unique;
}

function explodeOwnersFromText(raw) {
  const parts = raw
    .replace(/\band\b/gi, " & ")
    .replace(/\s+/g, " ")
    .split(/;|\n|\r|\||<br\s*\/?>/i)
    .map(normalizeSpace)
    .filter(Boolean);
  // Further split ampersand compounds
  const expanded = [];
  parts.forEach((p) => {
    const sub = splitCompoundAndNames(p);
    sub.forEach((s) => expanded.push(s));
  });
  return expanded.filter(Boolean);
}

function extractCurrentOwners($) {
  const ownerTexts = extractOwnerStrings($);
  const nameCandidates = [];
  ownerTexts.forEach((t) => {
    explodeOwnersFromText(t).forEach((n) => nameCandidates.push(n));
  });
  // Deduplicate by normalized name, then classify
  const dedup = [];
  const seen = new Set();
  const invalid = [];
  nameCandidates.forEach((n) => {
    const key = n.toLowerCase().trim();
    if (!key) return;
    if (seen.has(key)) return;
    seen.add(key);
    const { owner, invalidReason } = classifyOwner(n);
    if (owner) {
      // Build normalized identity for dedup across person/company objects
      let okey;
      if (owner.type === "person") {
        const full =
          `${owner.first_name} ${owner.middle_name ? owner.middle_name + " " : ""}${owner.last_name}`
            .toLowerCase()
            .trim();
        okey = `person|${full}`;
      } else {
        okey = `company|${owner.name.toLowerCase().trim()}`;
      }
      if (!dedup.some((o) => o.__key === okey)) {
        owner.__key = okey; // temp key for internal dedup
        dedup.push(owner);
      }
    } else {
      invalid.push({ raw: n, reason: invalidReason || "unknown" });
    }
  });
  // Remove temp keys
  dedup.forEach((o) => {
    delete o.__key;
  });
  return { owners: dedup, invalid };
}

function extractSaleDates($) {
  const dates = [];
  $("#divSearchDetails_Sales table tbody tr").each((i, tr) => {
    const td = $(tr).find("td").first();
    const t = normalizeSpace(td.text());
    if (t && /\d{2}\/\d{2}\/\d{4}/.test(t)) {
      const m = t.match(/(\d{2})\/(\d{2})\/(\d{4})/);
      if (m) {
        const mm = m[1],
          dd = m[2],
          yyyy = m[3];
        dates.push(`${yyyy}-${mm}-${dd}`);
      }
    }
  });
  // Unique and sort chronologically
  const unique = Array.from(new Set(dates));
  unique.sort();
  return unique;
}

// Main build
const propertyId = extractPropertyId($);
const { owners: currentOwners, invalid: invalidOwners } =
  extractCurrentOwners($);
const saleDates = extractSaleDates($);

// Build owners_by_date
const ownersByDate = {};
// If there are sale dates, assume the most recent sale date corresponds to the current owners
if (saleDates.length > 0) {
  const latest = saleDates[saleDates.length - 1];
  ownersByDate[latest] = currentOwners;
}
ownersByDate["current"] = currentOwners;

const output = {};
output[`property_${propertyId}`] = {
  owners_by_date: ownersByDate,
  invalid_owners: invalidOwners,
};

// Ensure output directory and write file
const outDir = path.resolve("owners");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "owner_data.json");
fs.writeFileSync(outPath, JSON.stringify(output, null, 2), "utf8");

// Print JSON to stdout
console.log(JSON.stringify(output, null, 2));
