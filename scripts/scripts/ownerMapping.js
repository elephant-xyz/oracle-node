const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

function normSpace(s) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function extractPropertyId($) {
  // Prefer explicit table row label "Parcel ID:"
  let id = null;
  $("tr").each((_, tr) => {
    if (id) return;
    const tds = $(tr).find("td");
    if (tds.length >= 2) {
      const label = normSpace($(tds[0]).text());
      if (/^parcel\s*id\s*:$/i.test(label) || /^parcel\s*id\b/i.test(label)) {
        const candidate = normSpace($(tds[1]).text());
        if (candidate) id = candidate;
      }
    }
  });
  // Fallbacks if not found exactly
  if (!id) {
    const el = $('*:contains("Parcel ID")')
      .filter((_, e) => /parcel\s*id/i.test($(e).text()))
      .first();
    if (el && el.length) {
      // Try next sibling text
      const sib = el.next();
      const candidate = normSpace(sib.text());
      if (candidate) id = candidate;
    }
  }
  if (!id) return "unknown_id";
  // Clean ID
  id = id.replace(/[^A-Za-z0-9_-]+/g, "");
  return id || "unknown_id";
}

function isCompanyName(name) {
  const n = (name || "").toLowerCase();
  const kws = [
    "inc",
    "inc.",
    "llc",
    "l.l.c",
    "ltd",
    "foundation",
    "alliance",
    "solutions",
    "corp",
    "corp.",
    "co",
    "co.",
    "services",
    "trust",
    "tr",
    "associates",
    "partners",
    "lp",
    "pllc",
    "pc",
    "company",
    "holdings",
    "group",
    "management",
    "properties",
    "realty",
    "capital",
  ];
  return kws.some((kw) => new RegExp(`(^|\\b)${kw}(\\b|\\.|$)`, "i").test(n));
}

function toIsoDate(s) {
  if (!s) return null;
  const str = s.trim();
  let m = str.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
  m = str.match(/^(\d{1,2})\/(\d{4})$/);
  if (m) return `${m[2]}-${m[1].padStart(2, "0")}-01`;
  const months = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
  ];
  const rxMonth = new RegExp(
    `^(${months.join("|")})\\s+(?:([0-9]{1,2}),\\s*)?([0-9]{4})$`,
    "i",
  );
  m = str.match(rxMonth);
  if (m) {
    const idx = months.findIndex((x) => x === m[1].toLowerCase()) + 1;
    const mm = String(idx).padStart(2, "0");
    const dd = m[2] ? String(parseInt(m[2], 10)).padStart(2, "0") : "01";
    return `${m[3]}-${mm}-${dd}`;
  }
  return null;
}

function splitOwnerCandidates(text) {
  const cleaned = (text || "")
    .replace(/\u00A0/g, " ")
    .replace(/[|;]+/g, "\n")
    .replace(/\s*\n+\s*/g, "\n")
    .trim();
  const parts = cleaned.split(/\n+/).filter(Boolean);
  const out = [];
  parts.forEach((p) => {
    p.split(/\s+(?:and|AND|And)\s+|\s+&\s+/).forEach((x) => {
      const z = normSpace(x);
      if (z) out.push(z);
    });
  });
  return out;
}

function parsePersonName(raw, contextHint) {
  const original = normSpace(raw);
  if (!original) return null;
  if (/,/.test(original)) {
    const [last, rest] = original.split(",");
    const tokens = normSpace(rest || "")
      .split(/\s+/)
      .filter(Boolean);
    const first = tokens[0] || "";
    const middle = tokens.slice(1).join(" ") || null;
    if (first && last)
      return {
        type: "person",
        first_name: first,
        last_name: normSpace(last),
        middle_name: middle,
      };
    return null;
  }
  const cleaned = original
    .replace(/[.,]/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
  const tokens = cleaned.split(/\s+/).filter(Boolean);
  if (tokens.length < 2) return null;
  const isUpper = cleaned === cleaned.toUpperCase();
  const inOwnerContext = contextHint && /owner/i.test(contextHint);
  if (isUpper && inOwnerContext) {
    const last = tokens[0];
    const first = tokens[1] || "";
    const middle = tokens.slice(2).join(" ") || null;
    if (first && last)
      return {
        type: "person",
        first_name: first,
        last_name: last,
        middle_name: middle,
      };
  }
  const first = tokens[0];
  const last = tokens[tokens.length - 1];
  const middle = tokens.slice(1, -1).join(" ") || null;
  if (first && last)
    return {
      type: "person",
      first_name: first,
      last_name: last,
      middle_name: middle,
    };
  return null;
}

function ownerNormKey(owner) {
  if (!owner) return "";
  if (owner.type === "company")
    return `company:${normSpace(owner.name || "").toLowerCase()}`;
  const parts = [owner.first_name, owner.middle_name, owner.last_name]
    .map((x) => normSpace(x || "").toLowerCase())
    .filter(Boolean);
  return `person:${parts.join(" ")}`;
}

function extractOwnerGroups($) {
  const groups = [];
  const labelRx =
    /^(owner\(s\)|owners?|owner\s*name\s*\d*|co-?owner|primary\s*owner)\s*:*/i;

  $("tr").each((_, tr) => {
    const tds = $(tr).find("td,th");
    if (tds.length < 2) return;
    const label = normSpace($(tds[0]).text());
    if (!labelRx.test(label)) return;

    const valueText = normSpace($(tds[1]).text());
    if (!valueText) return;

    // Find a date near this row (same or next row)
    let dateCandidate = null;
    const dateRx =
      /\b(\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{4}|\d{1,2}\/\d{4}|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*\d{4}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})\b/i;
    const rowText = normSpace($(tr).text());
    let m = rowText.match(dateRx);
    if (m) dateCandidate = toIsoDate(m[1]);
    if (!dateCandidate) {
      const nextRow = $(tr).next("tr");
      if (nextRow && nextRow.length) {
        const rowText2 = normSpace(nextRow.text());
        const m2 = rowText2.match(dateRx);
        if (m2) dateCandidate = toIsoDate(m2[1]);
      }
    }

    groups.push({
      context: label,
      valueText,
      date: dateCandidate,
      isCurrent: true,
    }); // assume this is the current owner block
  });

  // If no direct owner label rows found, try generic spans/divs where the preceding sibling label contains Owners
  if (groups.length === 0) {
    $('td:contains("Owners")').each((_, el) => {
      const $el = $(el);
      const label = normSpace($el.text());
      if (!/owners?/i.test(label)) return;
      const tr = $el.closest("tr");
      if (!tr.length) return;
      const tds = tr.find("td");
      if (tds.length >= 2) {
        const valueText = normSpace($(tds[1]).text());
        if (valueText)
          groups.push({
            context: "Owners",
            valueText,
            date: null,
            isCurrent: true,
          });
      }
    });
  }

  // Deduplicate by valueText
  const out = [];
  const seen = new Set();
  groups.forEach((g) => {
    const key = `${g.context}::${g.valueText}`.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push(g);
  });
  return out;
}

function parseOwnersFromGroup(valueText, contextHint) {
  const candidates = splitOwnerCandidates(valueText);
  const owners = [];
  const invalid = [];

  candidates.forEach((raw) => {
    if (!raw || /^none$/i.test(raw)) return;

    // If contains '&' and looks like two people glued, remove ampersand for parsing single person per record
    if (raw.includes("&")) {
      const parts = raw
        .split("&")
        .map((s) => normSpace(s))
        .filter(Boolean);
      parts.forEach((p) => {
        if (isCompanyName(p)) {
          owners.push({ type: "company", name: p });
        } else {
          const person = parsePersonName(p, contextHint || "");
          if (person && person.first_name && person.last_name)
            owners.push(person);
          else
            invalid.push({
              raw: p,
              reason: "unclassified_or_insufficient_info",
            });
        }
      });
      return;
    }

    if (isCompanyName(raw)) {
      owners.push({ type: "company", name: raw.trim() });
      return;
    }

    const person = parsePersonName(raw, contextHint || "");
    if (person && person.first_name && person.last_name) owners.push(person);
    else invalid.push({ raw, reason: "unclassified_or_insufficient_info" });
  });

  const uniq = [];
  const seen = new Set();
  for (const o of owners) {
    const key = ownerNormKey(o);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    if (o.middle_name === "") o.middle_name = null;
    uniq.push(o);
  }

  return { owners: uniq, invalid };
}

function buildOwnersByDate(groups) {
  const buckets = [];
  let placeholderIdx = 1;
  const globalInvalid = [];

  groups.forEach((g) => {
    const { owners, invalid } = parseOwnersFromGroup(g.valueText, g.context);
    invalid.forEach((inv) => globalInvalid.push(inv));
    if (owners.length === 0) return;
    let key = g.date ? g.date : `unknown_date_${placeholderIdx++}`;
    buckets.push({ key, owners, isCurrent: g.isCurrent });
  });

  // If nothing parsed, return empty skeleton
  if (buckets.length === 0) {
    return { owners_by_date: { current: [] }, invalid_owners: [] };
  }

  // Choose a current bucket (prefer a group marked current, else last)
  let currentOwners = [];
  const currentMarked = buckets.filter((b) => b.isCurrent);
  currentOwners = (
    currentMarked.length
      ? currentMarked[currentMarked.length - 1]
      : buckets[buckets.length - 1]
  ).owners;

  const dated = [];
  buckets.forEach((b) => {
    if (b.owners === currentOwners && !/^\d{4}-\d{2}-\d{2}$/.test(b.key))
      return; // avoid duplicating current unknown
    dated.push({ key: b.key, owners: b.owners });
  });

  const knownDates = dated.filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d.key));
  const unknowns = dated.filter((d) => !/^\d{4}-\d{2}-\d{2}$/.test(d.key));
  knownDates.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));

  const owners_by_date = {};
  unknowns.forEach((d) => {
    owners_by_date[d.key] = d.owners;
  });
  knownDates.forEach((d) => {
    owners_by_date[d.key] = d.owners;
  });
  owners_by_date.current = currentOwners;

  return { owners_by_date, invalid_owners: globalInvalid };
}

function main() {
  const inputPath = path.join(process.cwd(), "input.html");
  const html = fs.readFileSync(inputPath, "utf8");
  const $ = cheerio.load(html);

  const propertyIdRaw = extractPropertyId($);
  const propertyKey = `property_${propertyIdRaw}`;

  const groups = extractOwnerGroups($);
  const { owners_by_date, invalid_owners } = buildOwnersByDate(groups);

  const result = {};
  result[propertyKey] = {
    owners_by_date,
    invalid_owners: invalid_owners || [],
  };

  const outDir = path.join(process.cwd(), "owners");
  const outFile = path.join(outDir, "owner_data.json");
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outFile, JSON.stringify(result, null, 2), "utf8");

  console.log(JSON.stringify(result, null, 2));
}

main();
