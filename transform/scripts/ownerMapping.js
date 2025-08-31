const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

// Load HTML from input.html
const htmlPath = path.join(process.cwd(), "input.html");
const html = fs.readFileSync(htmlPath, "utf8");
const $ = cheerio.load(html);

// Helpers
const normSpace = (s) => (s || "").replace(/\s+/g, " ").trim();
const stripParens = (s) => normSpace((s || "").replace(/\([^)]*\)/g, ""));
const titleCaseWord = (w) => {
    if (!w) return w;
    // Keep roman numerals, initials, and all-caps acronyms as upper if <= 3 chars
    if (/^[A-Z]{1,3}$/.test(w)) return w.toUpperCase();
    // Handle hyphen and apostrophe names
    return w
        .toLowerCase()
        .split(/([-\'])/)
        .map((seg, i) =>
            i % 2 === 1 ? seg : seg.charAt(0).toUpperCase() + seg.slice(1),
        )
        .join("");
};
const titleCase = (s) => normSpace(s).split(" ").map(titleCaseWord).join(" ");

const cleanOwnerString = (s) => {
    let t = s || "";
    t = t.replace(/^FOR\s+/i, ""); // remove leading 'FOR '
    t = stripParens(t);
    // If trailing TR stands alone, drop it (trustee indicator, not part of company name)
    t = t.replace(/\bTR\.?$/i, "");
    t = t.replace(/\bC\/?O\b/gi, ""); // drop C/O markers if any
    t = normSpace(t);
    return t;
};

const hasDigits = (s) => /\d/.test(s || "");
const isLikelyAddress = (s) =>
    hasDigits(s) ||
    /(\bFL\b|\bAVE\b|\bST\b|\bRD\b|\bBLVD\b|\bDR\b|\bHWY\b|\bSUITE\b|\bUNIT\b)/i.test(
        s || "",
    );

// Extract Property ID (prefer Folio ID)
let propertyId = null;
const parcelLabel = $("#parcelLabel").text() || "";
let m = parcelLabel.match(/Folio ID:\s*(\d+)/i);
if (m) propertyId = m[1];
if (!propertyId) {
    const wholeText = $("body").text();
    m =
        wholeText.match(/Folio\s*ID[:#\s]*([0-9]{5,})/i) ||
        html.match(/FolioID=([0-9]{5,})/i);
    if (m) propertyId = m[1];
}
if (!propertyId) propertyId = "unknown_id";

// Owner extraction
const candidateNames = [];
const pushCandidate = (raw) => {
    const t = cleanOwnerString(raw);
    if (!t) return;
    if (t.length < 2) return;
    // Avoid obvious non-name phrases
    if (/^Owner Of Record/i.test(t)) return;
    if (isLikelyAddress(t)) return;
    candidateNames.push(t);
};

// 1) From explicit ownership list if present
$("#ownershipDiv li").each((_, el) => {
    const t = $(el).text();
    pushCandidate(t);
});

// 2) From Owner Of Record panel (first block of lines)
$("#divDisplayParcelOwner .textPanel div").each((_, el) => {
    const htmlBlock = $(el).html() || "";
    // Split on <br> boundaries
    const lines = htmlBlock
        .split(/<br\s*\/?\s*>/i)
        .map((x) => x.replace(/<[^>]*>/g, ""))
        .map((x) => normSpace(x))
        .filter(Boolean);
    // Consider only initial lines until we hit an address-like line
    for (const line of lines) {
        if (isLikelyAddress(line)) break;
        pushCandidate(line);
    }
});

// 3) Additional heuristic: look for labels containing 'Owner' and capture sibling text
$("*").each((_, el) => {
    const t = normSpace($(el).text());
    if (/Owner\s+Of\s+Record/i.test(t)) {
        // nearby text
        const siblingText = normSpace(
            $(el).closest("div").find(".textPanel").text(),
        );
        if (siblingText) {
            siblingText
                .split(/\n|\r|\t|\s{2,}/)
                .map(normSpace)
                .filter(Boolean)
                .forEach((frag) => {
                    if (!isLikelyAddress(frag)) pushCandidate(frag);
                });
        }
    }
});

// Deduplicate raw candidates by normalized name
const normalizeKey = (s) =>
    normSpace(s).toLowerCase().replace(/\.+/g, "").replace(/\s+/g, " ").trim();
const uniqueCandidatesMap = new Map();
for (const name of candidateNames) {
    const key = normalizeKey(name);
    if (!key) continue;
    if (!uniqueCandidatesMap.has(key)) uniqueCandidatesMap.set(key, name);
}
const uniqueCandidates = Array.from(uniqueCandidatesMap.values());

// Classification
const companyTokens = [
    "inc",
    "llc",
    "l.l.c",
    "ltd",
    "foundation",
    "alliance",
    "solutions",
    "corp",
    "co",
    "services",
    "trust",
    "tr",
    "company",
    "associates",
    "partners",
    "holdings",
    "bank",
    "n.a",
    "na",
    "assn",
    "association",
    "authority",
    "board",
    "llp",
    "pllc",
    "pc",
    "trustees",
    "properties",
    "property",
    "management",
    "group",
    "lp",
    "pl",
    "plc",
    "ministries",
    "church",
    "university",
    "school",
    "city",
    "county",
    "state",
    "dept",
    "department",
    "hoa",
    "pta",
];
const companyRegexes = companyTokens.map(
    (tok) => new RegExp(`(^|[^A-Za-z])${tok}([^A-Za-z]|$)`, "i"),
);
const isCompany = (name) => companyRegexes.some((rx) => rx.test(name));

const parsePerson = (name) => {
    let n = normSpace(name);
    // Drop common suffixes
    n = n.replace(/\b(JR|SR|III|IV|V|VI|VII|VIII|IX)\b\.?/gi, "").trim();

    // If contains comma: Last, First Middle
    if (/,/.test(n)) {
        const [last, rest] = n.split(",").map(normSpace);
        const parts = rest.split(" ").filter(Boolean);
        const first = parts.shift() || "";
        const middle = parts.join(" ") || null;
        if (last && first)
            return {
                first_name: titleCase(first),
                last_name: titleCase(last),
                middle_name: middle ? titleCase(middle) : null,
            };
        return null;
    }

    // If all uppercase, assume LAST FIRST [MIDDLE]
    if (n === n.toUpperCase()) {
        const parts = n.split(" ").filter(Boolean);
        if (parts.length >= 2) {
            const last = parts[0];
            const first = parts[1];
            const middle = parts.slice(2).join(" ") || null;
            return {
                first_name: titleCase(first),
                last_name: titleCase(last),
                middle_name: middle ? titleCase(middle) : null,
            };
        }
        return null;
    }

    // Else assume First [Middle] Last
    const parts = n.split(" ").filter(Boolean);
    if (parts.length >= 2) {
        const first = parts[0];
        const last = parts[parts.length - 1];
        const middle = parts.slice(1, -1).join(" ") || null;
        return {
            first_name: titleCase(first),
            last_name: titleCase(last),
            middle_name: middle ? titleCase(middle) : null,
        };
    }
    return null;
};

const owners = [];
const invalidOwners = [];

for (const rawName of uniqueCandidates) {
    let name = cleanOwnerString(rawName);
    if (!name) continue;

    // Handle & special case: treat as person unless company tokens are present
    if (name.includes("&")) {
        const cleaned = normSpace(name.replace(/&/g, " "));
        if (isCompany(cleaned)) {
            owners.push({ type: "company", name: titleCase(cleaned) });
            continue;
        } else {
            const p = parsePerson(cleaned);
            if (p && p.first_name && p.last_name) {
                owners.push({ type: "person", ...p });
            } else {
                invalidOwners.push({
                    raw: rawName,
                    reason: "Could not parse person name with & separator",
                });
            }
            continue;
        }
    }

    // Company classification
    if (isCompany(name)) {
        owners.push({ type: "company", name: titleCase(name) });
        continue;
    }

    // Person classification
    const p = parsePerson(name);
    if (p && p.first_name && p.last_name) {
        owners.push({ type: "person", ...p });
    } else {
        invalidOwners.push({
            raw: rawName,
            reason: "Insufficient data to classify owner",
        });
    }
}

// Deduplicate owners by normalized identity
const ownerKey = (o) => {
    if (!o) return "";
    if (o.type === "company") return "company:" + normalizeKey(o.name || "");
    const fn = normalizeKey(o.first_name || "");
    const ln = normalizeKey(o.last_name || "");
    const mn = normalizeKey(o.middle_name || "");
    return `person:${fn}|${mn}|${ln}`;
};
const dedupMap = new Map();
for (const o of owners) {
    const k = ownerKey(o);
    if (!k) continue;
    if (!dedupMap.has(k)) dedupMap.set(k, o);
}
const dedupedOwners = Array.from(dedupMap.values());

// Group by date: current owners
const ownersByDate = { current: dedupedOwners };

// Build final object
const propertyKey = `property_${propertyId}`;
const result = {};
result[propertyKey] = { owners_by_date: ownersByDate };
if (invalidOwners.length) result[propertyKey].invalid_owners = invalidOwners;

// Ensure output directory and write file
const outDir = path.join(process.cwd(), "owners");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "owner_data.json");
fs.writeFileSync(outPath, JSON.stringify(result, null, 2), "utf8");

// Print result JSON only
console.log(JSON.stringify(result, null, 2));