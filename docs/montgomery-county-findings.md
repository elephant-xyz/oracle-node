# Montgomery County, PA — Source Discovery Findings

Discovery date: 2026-08-29. Operator scope: **property / appraisal first**,
**building characteristics & roof data capture**, **parity with Hillsborough County, FL**,
**local workflow**. Permits and PA corporate registrations included in onboarding scope.

Machine-readable registry: [`montgomery-sources.yaml`](./montgomery-sources.yaml).

## 1. Appraiser Portal & GIS Open Data (Property)

Montgomery County, PA ("Montco") Assessment data is maintained by the **Montgomery County
Board of Assessment Appeals** and published as a unified open-data layer synced monthly
with GIS boundaries via **PASDA (Pennsylvania Spatial Data Access)** and the county's
own ArcGIS enterprise infrastructure.

- **Interactive Property Search:** `https://propertyrecords.montcopa.org/`
- **PASDA MapServer REST:**
  `https://mapservices.pasda.psu.edu/server/rest/services/pasda/MontgomeryCounty/MapServer/14`
  - Layer name: `Montgomery County - Parcels 202512`
  - Feature count: **309,732** parcels
  - Max record count: 1,000 per request
  - Pagination supported: `resultOffset`, `resultRecordCount`, `orderByFields=OBJECTID_12`
- **County ArcGIS FeatureServer:**
  `https://gis.montcopa.org/arcgis/rest/services/Parcels/Montgomery_County_Parcels/FeatureServer/10`

### Access Mode & Benchmark (2026-08-29 probe, US egress)

| Artifact | Mode | Notes |
|----------|------|-------|
| PASDA MapServer `query` (JSON) | **plain HTTP** | No CAPTCHA, no session bootstrap, fast response |
| PASDA `returnCountOnly` | plain HTTP | **309,732** features |
| 1000-record pages | plain HTTP | ~0.50–0.85 s/page |
| County Property Search UI | browser | Address/TAXPIN search |

## 2. Parcel Identifier

| Name | Format | Example | Use |
|------|--------|---------|-----|
| **TAXPIN / PARCEL** | 12 numeric digits (`##-##-#####-##-#`) | `300034228005` | Primary join key across GIS, assessment, and deeds |
| **ALT_ID / ALTERNATEI** | 8–10 chars | `30280 015` | Map block/unit identifier |
| **MUNI_CODE** | 2-digit numeric | `30` | 62 distinct municipality codes |
| **Muni_Name** | string | `Abington Township` | Plain-text municipal jurisdiction |

## 3. Structural & Building Characteristics Coverage (CAMA in Open GIS)

Unlike Chester County where building attributes reside solely in internal iasWorld CAMA,
Montgomery County includes core CAMA structural attributes directly in its monthly PASDA GIS roll:

| Attribute in Feed | Lexicon Target | Description | Sample Value |
|-------------------|----------------|-------------|--------------|
| `YEAR_BUILT` | `built_year` | Actual year structure built | `1924` |
| `YR_REM` | `effective_built_year` | Year of major remodel / addition | `1998` |
| `DEGREE_REM` | `renovation_degree` | Remodel degree code | `5` |
| `SFLA` | `livable_floor_area` | Square feet living area | `1890` |
| `COMM_AREA` / `COMM_NLA` | `commercial_building_area` | Commercial gross / net leasable area | `0` |
| `LAND_SF` | `lot_area_sqft` | Land square footage | `8000` |
| `LAND_ACRES` | `lot_size_acre` | Land acreage | `0.1837` |
| `EXTWALL` | `exterior_wall_material` | Exterior wall code (Brick, Stucco, Siding, etc.) | `5` |
| `STYLE` | `property_type` / `style` | Architectural style code | `05` |
| `STORIES` | `stories` | Number of stories | `2` |
| `BASEMENT` | `foundation_type` | Basement type code | `4` |
| `BEDROOMS` / `BATHS` | `bedroom_count` / `bathroom_count` | Room counts | `3` / `2` |
| `TOTAL_APPR` / `TOTAL_ASSE`| `market_value` / `assessed_value` | Valuation | `$161,870` |
| `DEED_BOOK` / `DEED_PAGE` | `deed_book` / `deed_page` | Recording references | `5850` / `01558` |
| `SALE_DATE` / `CONSIDERAT` | `last_sale_date` / `last_sale_price` | Sale transaction | `09/24/2012` / `$412,500` |

## 4. Municipal Permitting & Roof Intel (62 Jurisdictions)

Pennsylvania building permits are governed municipal-by-municipal. Montgomery County contains **62 municipalities**.

Top Municipalities by Volume & Commercial Density:

| Rank | Municipality | MUNI_CODE | Approx. Parcels | Portal / System |
|------|--------------|-----------|-----------------|-----------------|
| 1 | **Lower Merion Township** | `40` | ~21,000 | OpenGov / Civic Access |
| 2 | **Abington Township** | `30` | ~18,500 | Township Online Portal |
| 3 | **Cheltenham Township** | `31` | ~12,500 | OpenGov |
| 4 | **Upper Merion Township** | `58` | ~10,500 | Online Permit Services |
| 5 | **Norristown Municipality** | `13` | ~9,800 | Municipal Code Office |
| 6 | **Upper Dublin Township** | `54` | ~9,200 | CivicPlus |
| 7 | **Horsham Township** | `36` | ~8,900 | OpenGov |
| 8 | **Limerick Township** | `37` | ~8,700 | Township Services |

### Synthetic Roof Age Model

$$\text{Roof Age} = 2026 - \max(\text{Municipal Re-Roof Permit Year},\; \text{Year Remodeled (YR\_REM)},\; \text{Year Built (YEAR\_BUILT)})$$

## 5. PA Corporate Business Registration Matching

- **Source:** Pennsylvania Department of State (BCCO open data via Socrata: `https://data.pa.gov/resource/xvd7-5r2c.json`).
- **Filter:** `shortcountyname='MONTGOMERY'`
- **Scope:** ~180,000 business registration party records across Montgomery County.
- **Match Mechanism:** Direct SHA-256 situs address hash comparison against Montgomery property unnormalized addresses.
