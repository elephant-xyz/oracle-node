/**
 * @fileoverview Plant City MaintStar Permit Portal Adapter.
 * 
 * Queries and parses permit data from the City of Plant City
 * MaintStar system (h8.maintstar.co/PlantCity).
 */

/**
 * @typedef {object} MaintStarRecordItem
 * @property {number} id
 * @property {string} number - Permit or project number (e.g. "0226-00823")
 * @property {string | null} msType - e.g. "Solar", "Electrical", "Building"
 * @property {string | null} type - Detailed permit type
 * @property {string | null} status - e.g. "Issued / Need NOC", "Open", "Closed"
 * @property {string | null} dateVal - ISO date string
 * @property {string | null} datePrefix - e.g. "Issued on", "Applied on"
 * @property {string | null} address - Full street address
 * @property {number | null} lat - Latitude
 * @property {number | null} lng - Longitude
 * @property {string | null} description
 * @property {boolean} isRoofPermit
 */

/**
 * @typedef {object} MaintStarSearchResult
 * @property {Array<MaintStarRecordItem>} records
 * @property {string} status - "ok" | "quota_exceeded" | "not_found" | "fetch_error"
 * @property {string | null} error
 */

/**
 * Normalize permit query string for MaintStar search.
 * Strips non-alphanumeric prefixes or spaces.
 * @param {string} raw
 * @returns {string}
 */
export function normalizeMaintStarQuery(raw) {
  if (!raw) return "";
  return raw.trim();
}

/**
 * Parse single MaintStar record object into normalized schema.
 * @param {Record<string, unknown>} raw
 * @returns {MaintStarRecordItem | null}
 */
export function parseMaintStarRecord(raw) {
  if (!raw || typeof raw !== "object") return null;

  const id = typeof raw.id === "number" ? raw.id : 0;
  const num = typeof raw.number === "string" ? raw.number : (typeof raw.projectNumber === "string" ? raw.projectNumber : "");
  if (!num && !id) return null;

  const msType = typeof raw.msType === "string" ? raw.msType : null;
  const type = typeof raw.type === "string" ? raw.type : null;
  const status = typeof raw.status === "string" ? raw.status : null;
  const dateVal = typeof raw.dateVal === "string" ? raw.dateVal : (typeof raw.createdDate === "string" ? raw.createdDate : null);
  const datePrefix = typeof raw.datePrefix === "string" ? raw.datePrefix : null;
  const address = typeof raw.address === "string" ? raw.address : null;
  const lat = typeof raw.lat === "number" ? raw.lat : null;
  const lng = typeof raw.lng === "number" ? raw.lng : null;
  const description = typeof raw.description === "string" ? raw.description : null;

  const typeDesc = `${msType || ""} ${type || ""} ${description || ""}`;
  const isRoof = /roof|shingle|tile|metal roof/i.test(typeDesc);

  return {
    id,
    number: num,
    msType,
    type,
    status,
    dateVal,
    datePrefix,
    address,
    lat,
    lng,
    description,
    isRoofPermit: isRoof,
  };
}

/**
 * Search Plant City MaintStar permits by query (permit number, address, or keyword).
 * Implements exponential retry backoff for rate-quota protection.
 * 
 * @param {string} query - Permit number, address, or search term
 * @param {number} [maxRetries=3]
 * @returns {Promise<MaintStarSearchResult>}
 */
export async function searchMaintStarPermits(query, maxRetries = 3) {
  const clean = normalizeMaintStarQuery(query);
  if (!clean) {
    return { records: [], status: "not_found", error: "Empty query" };
  }

  const endpoint = `https://h8.maintstar.co/PlantCity/api/Public/Record/Search?query=${encodeURIComponent(clean)}&status=both`;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(endpoint, {
        headers: {
          "Accept": "application/json",
          "X-Mst": "portal",
          "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        },
      });

      const text = await res.text();

      if (text.includes("Requests quota exceeded")) {
        if (attempt === maxRetries) {
          return {
            records: [],
            status: "quota_exceeded",
            error: "MaintStar rate quota exceeded",
          };
        }
        // Quota backoff: 2s, 4s, 6s
        await new Promise((r) => setTimeout(r, attempt * 2000));
        continue;
      }

      if (!res.ok) {
        if (attempt === maxRetries) {
          return {
            records: [],
            status: "fetch_error",
            error: `HTTP ${res.status} ${res.statusText}`,
          };
        }
        await new Promise((r) => setTimeout(r, attempt * 1000));
        continue;
      }

      /** @type {{ data?: Array<Record<string, unknown>>, total?: number }} */
      const json = JSON.parse(text);
      const rawItems = Array.isArray(json.data) ? json.data : [];
      /** @type {Array<MaintStarRecordItem>} */
      const records = [];

      for (const item of rawItems) {
        const parsed = parseMaintStarRecord(item);
        if (parsed) records.push(parsed);
      }

      return {
        records,
        status: "ok",
        error: null,
      };
    } catch (err) {
      if (attempt === maxRetries) {
        return {
          records: [],
          status: "fetch_error",
          error: err instanceof Error ? err.message : String(err),
        };
      }
      await new Promise((r) => setTimeout(r, attempt * 1200));
    }
  }

  return { records: [], status: "fetch_error", error: "Max retries exceeded" };
}
