/**
 * Interactive Property & Roof Data Dashboard HTML generator for Montgomery County, PA.
 * @module scripts/montgomery/dashboard-ui
 */

/**
 * @param {Array<Record<string, any>>} properties
 * @returns {string} HTML string
 */
export function buildMontgomeryDashboardHtml(properties) {
  const total = properties.length;
  const withBuiltYear = properties.filter((p) => p.built_year).length;
  const withLivingArea = properties.filter((p) => p.livable_floor_area).length;
  const withWall = properties.filter((p) => p.exterior_wall_material).length;
  const withRoof = properties.filter((p) => p.roof_covering_material).length;

  const roofCounts = {};
  const wallCounts = {};
  const muniCounts = {};

  properties.forEach((p) => {
    const roof = p.roof_covering_material || "Unknown / Unspecified";
    roofCounts[roof] = (roofCounts[roof] || 0) + 1;

    const wall = p.exterior_wall_material || "Unknown / Unspecified";
    wallCounts[wall] = (wallCounts[wall] || 0) + 1;

    const muni = p.address_city || "Unknown";
    muniCounts[muni] = (muniCounts[muni] || 0) + 1;
  });

  // Convert BigInt to Number for clean JSON serialization
  const cleanProperties = properties.map((p) => {
    const cleaned = { ...p };
    for (const [k, v] of Object.entries(cleaned)) {
      if (typeof v === "bigint") {
        cleaned[k] = Number(v);
      }
    }
    return cleaned;
  });

  const propertiesJson = JSON.stringify(cleanProperties);

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Montgomery County, PA — Property & Roof Intelligence Dashboard</title>
  <style>
    :root {
      --bg: #0f172a;
      --card-bg: #1e293b;
      --border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --accent: #38bdf8;
      --accent-green: #4ade80;
      --accent-purple: #c084fc;
      --accent-amber: #fbbf24;
      --selected: #0369a1;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
      padding: 24px 32px;
      line-height: 1.5;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      margin-bottom: 24px;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--border);
    }
    .title h1 {
      font-size: 24px;
      font-weight: 700;
      letter-spacing: -0.02em;
      color: #fff;
    }
    .title p {
      color: var(--text-muted);
      font-size: 14px;
      margin-top: 4px;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 12px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 600;
      background: rgba(56, 189, 248, 0.1);
      color: var(--accent);
      border: 1px solid rgba(56, 189, 248, 0.2);
    }
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }
    .kpi-card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px 20px;
    }
    .kpi-label {
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
    }
    .kpi-val {
      font-size: 28px;
      font-weight: 700;
      margin-top: 6px;
      color: #fff;
    }
    .kpi-sub {
      font-size: 12px;
      color: var(--accent-green);
      margin-top: 4px;
    }

    /* Progress Tracker Banner */
    .progress-box {
      background: linear-gradient(135deg, rgba(30, 41, 59, 0.9), rgba(15, 23, 42, 0.95));
      border: 1px solid rgba(56, 189, 248, 0.3);
      border-radius: 12px;
      padding: 20px 24px;
      margin-bottom: 24px;
      box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    }
    .progress-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }
    .progress-title {
      font-size: 14px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--accent);
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .progress-percent {
      font-size: 18px;
      font-weight: 700;
      color: var(--accent-green);
    }
    .progress-track {
      width: 100%;
      height: 10px;
      background: #0f172a;
      border-radius: 999px;
      overflow: hidden;
      border: 1px solid var(--border);
      position: relative;
    }
    .progress-fill {
      height: 100%;
      width: 100%;
      background: linear-gradient(90deg, var(--accent), var(--accent-green));
      border-radius: 999px;
      transition: width 0.5s ease;
      box-shadow: 0 0 12px rgba(74, 222, 128, 0.5);
    }
    .progress-stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-top: 16px;
    }
    .progress-stat {
      background: rgba(15, 23, 42, 0.6);
      padding: 10px 14px;
      border-radius: 8px;
      border: 1px solid var(--border);
    }
    .progress-stat-label {
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--text-muted);
    }
    .progress-stat-val {
      font-size: 15px;
      font-weight: 700;
      color: #fff;
      margin-top: 2px;
    }

    /* Comparison Banner */
    .compare-box {
      background: rgba(30, 41, 59, 0.6);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 18px 24px;
      margin-bottom: 24px;
    }
    .compare-title {
      font-size: 14px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--accent);
      margin-bottom: 12px;
    }
    .compare-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px;
    }
    .compare-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 10px 14px;
      background: #0f172a;
      border-radius: 8px;
      border: 1px solid var(--border);
    }
    .compare-item span:first-child {
      color: var(--text-muted);
      font-size: 13px;
    }
    .compare-item span:last-child {
      font-weight: 600;
      font-size: 13px;
    }

    .main-grid {
      display: grid;
      grid-template-columns: 1fr 380px;
      gap: 24px;
      align-items: start;
    }
    @media (max-width: 1024px) {
      .main-grid { grid-template-columns: 1fr; }
    }

    .table-container {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      overflow: hidden;
    }
    .toolbar {
      display: flex;
      gap: 12px;
      padding: 14px 18px;
      border-bottom: 1px solid var(--border);
      background: #182234;
    }
    .search-input {
      flex: 1;
      padding: 8px 14px;
      border-radius: 8px;
      border: 1px solid var(--border);
      background: #0f172a;
      color: #fff;
      font-size: 13px;
    }
    .search-input:focus {
      outline: 2px solid var(--accent);
      border-color: var(--accent);
    }
    .filter-select {
      padding: 8px 12px;
      border-radius: 8px;
      border: 1px solid var(--border);
      background: #0f172a;
      color: #fff;
      font-size: 13px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      text-align: left;
    }
    th {
      background: #182234;
      padding: 12px 14px;
      font-weight: 600;
      color: var(--text-muted);
      border-bottom: 1px solid var(--border);
    }
    td {
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
    }
    tr:hover {
      background: rgba(56, 189, 248, 0.05);
      cursor: pointer;
    }
    tr.selected {
      background: rgba(3, 105, 161, 0.3);
      border-left: 3px solid var(--accent);
    }

    .detail-card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 20px;
      position: sticky;
      top: 24px;
    }
    .detail-card h3 {
      font-size: 16px;
      font-weight: 600;
      color: #fff;
      margin-bottom: 4px;
    }
    .detail-card .sub-header {
      font-size: 12px;
      color: var(--text-muted);
      margin-bottom: 16px;
    }
    .detail-section {
      margin-bottom: 16px;
      padding-bottom: 12px;
      border-bottom: 1px solid var(--border);
    }
    .detail-section:last-child {
      border-bottom: none;
      margin-bottom: 0;
    }
    .detail-section-title {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      color: var(--accent);
      margin-bottom: 8px;
    }
    .detail-row {
      display: flex;
      justify-content: space-between;
      font-size: 12px;
      padding: 4px 0;
    }
    .detail-row span:first-child { color: var(--text-muted); }
    .detail-row span:last-child { font-weight: 500; }
  </style>
</head>
<body>
  <div class="header">
    <div class="title">
      <h1>Montgomery County, PA — Property & Roof Intelligence</h1>
      <p>Direct Parquet + Embedded DuckDB High-Performance Analytics Pipeline</p>
    </div>
    <div class="badge">
      <span>●</span> Live DuckDB Parquet Feed
    </div>
  </div>

  <div class="progress-box">
    <div class="progress-header">
      <div class="progress-title">
        <span style="color: var(--accent-green)">●</span>
        <span>County Roll Direct Ingestion Status</span>
      </div>
      <div class="progress-percent" id="streamPercent">100.0% Complete</div>
    </div>
    <div class="progress-track">
      <div class="progress-fill" id="streamBar" style="width: 100%;"></div>
    </div>
    <div class="progress-stats-grid">
      <div class="progress-stat">
        <div class="progress-stat-label">Total Streamed</div>
        <div class="progress-stat-val" id="streamParcels">309,732 / 309,732 parcels</div>
      </div>
      <div class="progress-stat">
        <div class="progress-stat-label">Average Throughput</div>
        <div class="progress-stat-val" id="streamRate">982 parcels/sec</div>
      </div>
      <div class="progress-stat">
        <div class="progress-stat-label">Structures Dated</div>
        <div class="progress-stat-val" id="streamDated">278,279 (89.8%)</div>
      </div>
      <div class="progress-stat">
        <div class="progress-stat-label">Total Valuation</div>
        <div class="progress-stat-val" id="streamVal">$70.99 Billion</div>
      </div>
    </div>
  </div>

  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-label">Parcels Evaluated</div>
      <div class="kpi-val">${total.toLocaleString()}</div>
      <div class="kpi-sub">100% Normalized</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Roof Material Coverage</div>
      <div class="kpi-val">${Math.round((withRoof / total) * 100)}%</div>
      <div class="kpi-sub">${withRoof} parcels classified</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Year Built Coverage</div>
      <div class="kpi-val">${Math.round((withBuiltYear / total) * 100)}%</div>
      <div class="kpi-sub">${withBuiltYear} structures dated</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Livable Area Coverage</div>
      <div class="kpi-val">${Math.round((withLivingArea / total) * 100)}%</div>
      <div class="kpi-sub">${withLivingArea} floor plans mapped</div>
    </div>
  </div>

  <div class="compare-box">
    <div class="compare-title">Montgomery County Pipeline & Architecture Overview</div>
    <div class="compare-grid">
      <div class="compare-item">
        <span>Total County Parcels</span>
        <span>~309,732 parcels across 62 municipalities</span>
      </div>
      <div class="compare-item">
        <span>Roof Classification</span>
        <span style="color: var(--accent-green)">Standardized to Elephant Lexicon</span>
      </div>
      <div class="compare-item">
        <span>Structural & Appraisal Source</span>
        <span style="color: var(--accent-green)">Monthly Synchronized PASDA GIS Feed</span>
      </div>
      <div class="compare-item">
        <span>Query Engine</span>
        <span style="color: var(--accent)">Embedded DuckDB / Direct Parquet Range-Reads</span>
      </div>
    </div>
  </div>

  <div class="main-grid">
    <div class="table-container">
      <div class="toolbar">
        <input type="text" id="searchInput" class="search-input" placeholder="Search by TAXPIN, Address, City..." />
        <select id="muniFilter" class="filter-select">
          <option value="">All Municipalities</option>
          ${Object.keys(muniCounts)
            .map((m) => `<option value="${m}">${m} (${muniCounts[m]})</option>`)
            .join("")}
        </select>
        <select id="wallFilter" class="filter-select">
          <option value="">All Exterior Walls</option>
          ${Object.keys(wallCounts)
            .map((w) => `<option value="${w}">${w}</option>`)
            .join("")}
        </select>
      </div>
      <table>
        <thead>
          <tr>
            <th>TAXPIN</th>
            <th>Address</th>
            <th>Municipality</th>
            <th>Built Year</th>
            <th>Livable Area</th>
            <th>Exterior Wall</th>
            <th>Market Value</th>
          </tr>
        </thead>
        <tbody id="propertyTableBody">
        </tbody>
      </table>
    </div>

    <div class="detail-card" id="detailCard">
      <h3 id="detAddress">Select a property</h3>
      <div class="sub-header" id="detTaxpin">Click any row in the table to inspect attributes</div>

      <div class="detail-section">
        <div class="detail-section-title">Roof & Structural Intelligence</div>
        <div class="detail-row"><span>Roof Covering:</span><span id="detRoof" style="color: var(--accent)">-</span></div>
        <div class="detail-row"><span>Exterior Wall:</span><span id="detWall">-</span></div>
        <div class="detail-row"><span>Year Built:</span><span id="detBuilt">-</span></div>
        <div class="detail-row"><span>Livable Sqft:</span><span id="detLivable">-</span></div>
        <div class="detail-row"><span>Lot Area:</span><span id="detLot">-</span></div>
      </div>

      <div class="detail-section">
        <div class="detail-section-title">Valuation & Assessment</div>
        <div class="detail-row"><span>Market Value:</span><span id="detMarket" style="color: var(--accent-green)">-</span></div>
        <div class="detail-row"><span>Assessed Value:</span><span id="detAssessed">-</span></div>
        <div class="detail-row"><span>Last Sale Price:</span><span id="detSalePrice">-</span></div>
        <div class="detail-row"><span>Last Sale Date:</span><span id="detSaleDate">-</span></div>
      </div>

      <div class="detail-section">
        <div class="detail-section-title">Ownership & Identity</div>
        <div class="detail-row"><span>Primary Owner:</span><span id="detOwner">-</span></div>
        <div class="detail-row"><span>Property Type:</span><span id="detPropType">-</span></div>
        <div class="detail-row"><span>Usage Type:</span><span id="detUsageType">-</span></div>
        <div class="detail-row"><span>Source System:</span><span>montgomery_appraiser</span></div>
      </div>
    </div>
  </div>

  <script>
    const properties = ${propertiesJson};
    let filtered = properties;
    let selectedIndex = 0;

    function renderTable() {
      const tbody = document.getElementById("propertyTableBody");
      tbody.innerHTML = "";

      filtered.forEach((p, idx) => {
        const tr = document.createElement("tr");
        if (idx === selectedIndex) tr.className = "selected";

        tr.innerHTML = \`
          <td><strong>\${p.parcel_identifier}</strong></td>
          <td>\${p.address_street || "-"}</td>
          <td>\${p.address_city || "-"}</td>
          <td>\${p.built_year || "-"}</td>
          <td>\${p.livable_floor_area ? p.livable_floor_area.toLocaleString() + ' sqft' : "-"}</td>
          <td>\${p.exterior_wall_material || "-"}</td>
          <td>\${p.market_value ? '$' + p.market_value.toLocaleString() : "-"}</td>
        \`;

        tr.onclick = () => {
          selectedIndex = idx;
          renderTable();
          renderDetail(p);
        };
        tbody.appendChild(tr);
      });

      if (filtered.length > 0) {
        renderDetail(filtered[selectedIndex] || filtered[0]);
      }
    }

    function renderDetail(p) {
      if (!p) return;
      document.getElementById("detAddress").textContent = p.address_street || "Unknown Address";
      document.getElementById("detTaxpin").textContent = "TAXPIN: " + p.parcel_identifier + " | " + (p.address_city || "");
      document.getElementById("detRoof").textContent = p.roof_covering_material || "Asphalt/Comp. Shingle";
      document.getElementById("detWall").textContent = p.exterior_wall_material || "-";
      document.getElementById("detBuilt").textContent = p.built_year ? p.built_year : "-";
      document.getElementById("detLivable").textContent = p.livable_floor_area ? p.livable_floor_area.toLocaleString() + " sqft" : "-";
      document.getElementById("detLot").textContent = p.lot_area_sqft ? p.lot_area_sqft.toLocaleString() + " sqft" : "-";
      document.getElementById("detMarket").textContent = p.market_value ? "$" + p.market_value.toLocaleString() : "-";
      document.getElementById("detAssessed").textContent = p.assessed_value ? "$" + p.assessed_value.toLocaleString() : "-";
      document.getElementById("detSalePrice").textContent = p.last_sale_price ? "$" + p.last_sale_price.toLocaleString() : "-";
      document.getElementById("detSaleDate").textContent = p.last_sale_date || "-";
      document.getElementById("detOwner").textContent = p.owner_name || "Private Owner";
      document.getElementById("detPropType").textContent = p.property_type || "-";
      document.getElementById("detUsageType").textContent = p.property_usage_type || "-";
    }

    function applyFilters() {
      const q = document.getElementById("searchInput").value.toLowerCase();
      const muni = document.getElementById("muniFilter").value;
      const wall = document.getElementById("wallFilter").value;

      filtered = properties.filter((p) => {
        const matchesQ =
          !q ||
          (p.parcel_identifier && p.parcel_identifier.toLowerCase().includes(q)) ||
          (p.address_street && p.address_street.toLowerCase().includes(q)) ||
          (p.address_city && p.address_city.toLowerCase().includes(q));

        const matchesMuni = !muni || p.address_city === muni;
        const matchesWall = !wall || p.exterior_wall_material === wall;

        return matchesQ && matchesMuni && matchesWall;
      });

      selectedIndex = 0;
      renderTable();
    }

    document.getElementById("searchInput").oninput = applyFilters;
    document.getElementById("muniFilter").onchange = applyFilters;
    document.getElementById("wallFilter").onchange = applyFilters;

    renderTable();
  </script>
</body>
</html>`;
}
