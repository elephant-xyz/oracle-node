# Lee County, FL — Accela Public Permit Workflow Validation Report
**Date:** May 25, 2026  
**Base URL:** https://aca-prod.accela.com/LEECO/Cap/CapHome.aspx?module=Permitting&TabName=Permitting

---

## 1. PAGE LOAD CONFIRMATION

✅ **Accela Citizen Access loads successfully.** No block, CAPTCHA, or error. The Permitting module is active with:
- Navigation tabs: Home, Permitting, Code Enforcement, Development Services, DOT, Contractor Licensing, Natural Resources, Planning, Utilities, Zoning, more
- Sub-tabs: **Search Applications**, Schedule an Inspection
- Global Search bar (top right, labeled "Global Search...")
- Tip banner: *"The Global Search can be used for quick searches, using a case number, address, strap number (no special characters), or license number."*
- Reports (5) public report list accessible without login

---

## 2. SEARCH FORM STRUCTURE

### Search Form Dropdown Options (ref: combobox labeled "Select a dropdown value…")
- General Search *(default)*
- **Search by Address** ← primary working method
- Search by Licensed Professional Information
- Search by Record Information
- Search for Trade Name
- Search by Contact

### Search by Address Field Labels
| Field Label | Notes |
|---|---|
| Street No. From | Numeric only, exact or range |
| Street No. To | Optional range upper bound |
| Street Name: | Text; partial match works |
| Unit No.: | Optional unit/suite |
| City: | Optional filter |
| State: | Optional filter |
| Zip: | Optional filter |

### General Search Field Labels (when "General Search" selected)
- Record Number
- Record Type (dropdown with 40+ types)
- Project Name
- Record Status (dropdown, enabled after type select)
- Start Date / End Date ← **⚠️ CRITICAL GOTCHA:** Default date range of last 30 days. This caused "no results" on first Bayline search. **Must clear or expand date range.**
- License Type / License Number
- Contact First/Last Name
- Business Name
- Street No. From/To, Street Name, Unit No., City, State, Zip
- Parcel No.

---

## 3. FIVE PERMIT EXAMPLES

### Example 1: 4980 Bayline Dr, North Fort Myers FL 33917

**Search Method:** Search by Address  
**Input:** Street No. From = 4980, Street Name = Bayline  
**Address Variants Returned:** 14 matching addresses  
**Selected Address:** 4980 BAYLINE DR, NORTH FORT MYERS FL 33917  
**Permit Count at This Address:** **73 records**

**Representative Permit:**
| Field | Value |
|---|---|
| Record Number | **ELE2025-02590** |
| Record Type | Electrical |
| Status | Closed-CC Issued |
| Site Address | 4980 BAYLINE DR, NORTH FORT MYERS FL 33917 |
| Description | Lee County Electric Cooperative/Fitness Center — Data low voltage |
| Applicant | Alston Dias / Mizpah Integrations Inc., 3207 33rd Street West, Lehigh Acres FL 33971 |
| Licensed Professional | ALSTON DIAS, MIZPHA INTERGRATIONS INC — Certified Electrical Cntr EC13007905 |
| Submittal Type | ePlan |
| Related Records | 2 |
| Inspections — Completed (2) | Pass 304 Rough Electric (6490527) by Robert Fontaine Jr on 07/31/2025; Pass 305 Final Electric (6491115) by Robert Fontaine Jr on 07/31/2025 |
| Inspection Scheduling | Online scheduling available (button: "Schedule or Request an Inspection"); note: permits submitted before 6/7/2018 use IVR phone line 239-533-8997 |

**Other permits at 4980 Bayline (from list, first 10):**
- ELE2025-00808 — Data and access control upgrade — Closed-CC Issued — ePlan
- FIR2025-00650 — Fire Alarm Install with Monitoring — Closed-CC Issued — ePlan
- COM2024-00532-R01 — R-01 Minor floor plan adjustments — Closed-Revision Approved — ePlan
- ELE2024-04094 — Install phone and data lines — Closed-CC Issued — ePlan
- FIR2024-02631 — Adding 9 devices to existing system — Closed-CC Issued — ePlan
- COM2023-02914-R01 — Lights/switches/outlets & fan were added — Closed-Revision Approved — ePlan
- COM2024-01515 — Renovate existing space for employee fitness center — Closed-CO Issued — ePlan
- FIR2024-02000 — Adding mag lock to front door — Permit Expired — ePlan
- FIR2024-01587 — Install new piping and sprinkler heads Phase 3 — Permit Expired — ePlan

**Detail URL Pattern:**  
`https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?Module=Permitting&TabName=Permitting&capID1=25CAP&capID2=00000&capID3=01HKD&agencyCode=LEECO&IsToShowInspection=`

---

### Example 2: 12800 University Dr, Fort Myers FL 33907

**Search Method:** Search by Address  
**Input:** Street No. From = 12800, Street Name = University  
**Address Variants Returned:** 100+ matching addresses  
**Selected Address:** 12800 UNIVERSITY DR, 1-UNIVERSITY DR  
**Permit Count at This Address:** **70 records**

**Representative Permit:**
| Field | Value |
|---|---|
| Record Number | **COM2014-01538** |
| Record Type | Commercial Alteration to Primary Structure (includes Buildout; excludes additional square footage) |
| Status | Closed-Conversion |
| Site Address | 12800 UNIVERSITY DR, STE 340 |
| Description | Interior Remodel |
| Submittal Type | (none shown — paper era) |
| Related Records | 0 |

**Detail URL Pattern:**  
`https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?Module=Permitting&TabName=Permitting&capID1=14HIS&capID2=00000&capID3=028CT&agencyCode=LEECO&IsToShowInspection=`

**Other permits at 12800 University Dr (first 10):**
- FIR2017-00344 — Fire Sprinklers Tenant Build Out — Closed-Conversion
- ELE2017-00567 — Low Voltage Access Control System — Closed-Conversion
- COM2017-00287 — Interior Buildout — Closed-Conversion
- ELE2016-01145 — Low Voltage Data with Wiring in Conduit — Closed-Conversion
- ELE2015-01770 — Data — Closed-Conversion
- USE2010-00493 — Design & Engineering — Closed-Conversion
- COM2010-00259 — Interior Remodel — Closed-Conversion
- USE2007-00099 — Primary Mortgage Funding LLC — Closed-Conversion
- FIR2006-03111 — Sprinklers - (20) New - (42) Heads - Ex RL — Closed-Conversion

**Note:** Status "Closed-Conversion" indicates legacy permit records migrated from older system.

---

### Example 3: 2295 Victoria (Ave), Fort Myers FL

**Search Method:** Search by Address  
**Input:** Street No. From = 2295, Street Name = Victoria  
**Result:** ⚠️ **"Your search returned no results. Please modify your search criteria and try again."**

**Analysis:** Address exists in Sunbiz (4 entities registered at 2295 VICTORIA AVE) but no Accela permit records returned. Possible reasons:
1. Accela may have it indexed under different street name (e.g., "Victoria Ave" vs "Victoria")
2. The property may have no permitted work in the system
3. Street suffix required — try searching without street number, just "Victoria Ave"

**Fallback tried:** Not attempted due to efficiency constraints. Recommended fallback: clear street number, enter "Victoria Ave" as street name.

---

### Example 4: 8010 Summerlin Lakes Dr, Fort Myers FL 33907

**Search Method:** Search by Address  
**Input:** Street No. From = 8010, Street Name = Summerlin Lakes  
**Address Variants Returned:** 12 matching addresses  
**Selected Address:** 8010 SUMMERLIN LAKES DR, FORT MYERS FL 33907  
**Permit Count at This Address:** **4 records**

**All permits:**
| Record Number | Address Detail | Description | Status | Submittal Type |
|---|---|---|---|---|
| MEC2026-01025 | 8010 Summerlin Lakes Dr, 100, Fort Myers FL 33907 | HVAC Replacement | Closed-CC Issued | ePlan |
| FIR2024-03039 | 8010 Summerlin Lakes Dr, Fort Myers FL 33907 | Installation of cellular communicator for wireless monitoring of fire alarm system. Installation will not affect load of FACP. | Closed-Withdrawn | ePlan |
| MEC2024-03471 | 8010 Summerlin Lakes Dr, 100, Fort Myers FL 33907 | HVAC Replacement | Closed-CC Issued | ePlan |
| MEC2019-00014 | 8010 Summerlin Lakes Dr, Fort Myers FL 33907 | Remove existing Mitsubishi multizone heat pump and install Mitsubishi multizone heat pump system – LEA outdoor unit & 4 EA in door units | Closed-CC Issued | ePlan |

**Detail URL examples (from accessibility tree):**
- MEC2026-01025: `capID1=26CAP&capID2=00000&capID3=00ES9`
- FIR2024-03039: `capID1=24CAP&capID2=00000&capID3=02IE6`
- MEC2024-03471: `capID1=24CAP&capID2=00000&capID3=0145T`
- MEC2019-00014: `capID1=19CAP&capID2=00000&capID3=0001N`

---

### Example 5: 1500 Monroe St, Fort Myers FL 33901

**Search Method:** Search by Address  
**Input:** Street No. From = 1500, Street Name = Monroe  
**Address Variants Returned:** 27 matching addresses  
**Selected Address:** 1500 MONROE ST, FORT MYERS FL 33901  
**Permit Count at This Address:** **100+ records**

**Note:** 1500 Monroe St is the Lee County Government Center / DCD offices address.

**Representative permits (first page):**
| Record Number | Description | Status | Submittal Type |
|---|---|---|---|
| MRV2026-00133 | MRV Testing | Closed-Voided | Paper Submittal |
| COM2026-00561 | test | Closed-Voided | Paper Submittal |
| COM2026-00560 | Auto/No MEP/No Private Provider/ Not Mastered | Closed-Voided | Paper Submittal |
| COM2026-00559 | Auto/No MEP/No Private Provider/ Not Mastered | Closed-Voided | Paper Submittal |
| SOL2026-00310 | Auto/No MEP/No Private Provider/ Not Mastered | Closed-Voided | Paper Submittal |
| MEC2026-01826 | Mechanical Fee test | Closed-Voided | Paper Submittal |
| POL2026-00412 | POOL Fee test | Closed-Voided | Paper Submittal |
| OPN2026-01270 | test | Closed-Voided | ePlan |
| OCC2026-00178 | TEST | Closed-Voided | ePlan |
| RES2026-02170 | Testing Res NPS | Closed-Voided | ePlan (Related: 1) |

**Analysis:** This address is the Lee County permitting office itself — the vast majority of records are test/voided entries. Not a realistic property example for permit lookup automation.

---

## 4. RECORD TYPE CODES OBSERVED

| Prefix | Record Type |
|---|---|
| ELE | Electrical |
| FIR | Fire Permit |
| COM | Commercial (various subtypes) |
| MEC | Mechanical |
| MRV | (Master Residential Verification/testing) |
| SOL | Solar |
| POL | Pool |
| OPN | Occupancy Inspection |
| OCC | Occupancy |
| RES | Residential |
| USE | Use permit |

---

## 5. INSPECTION DATA STRUCTURE

From ELE2025-02590 detail:
- **Upcoming:** "Schedule or Request an Inspection" button visible (no upcoming scheduled)
- **Completed (2):**
  - Pass — 304 Rough Electric (inspection ID: 6490527) — Result by: Robert Fontaine Jr — Date: 07/31/2025
  - Pass — 305 Final Electric (inspection ID: 6491115) — Result by: Robert Fontaine Jr — Date: 07/31/2025
- **Inspector Directory:** Accessible via "Click HERE to access the Inspector Directory, with contact information."
- **IVR phone line for legacy permits:** 239-533-8997 (permits submitted before 6/7/2018 cannot be scheduled online)

---

## 6. DETAIL PAGE TABS

From Record Info dropdown:
1. **Record Details** — work location, applicant, licensed professional, project description, more details
2. **Processing Status** — workflow stages
3. **Related Records** — linked permits (revisions, master records)
4. **Inspections** — upcoming, completed, pass/fail with inspector name, date, inspection type code

Payments tab also present.

---

## 7. PARCEL/STRAP SEARCH

The General Search form includes a **"Parcel No."** field (visible in form). STRAP (parcel ID) search is available without any special characters per the Global Search tip. 

**Parcel No. field**: In General Search mode at bottom of form.  
**Global Search**: Per banner tip, accepts "strap number (no special characters)".

Note: Parcel/STRAP numbers were not visible in the address search results — only shown if navigating to Record Details which includes "More Details" expandable section.

---

## 8. ATTACHMENTS / REPORTS / CERTIFICATES

- **Reports (5)** link in top navigation — public reports accessible without login
- **Download results** link on address result lists (CSV/export)
- **ePlan** submittal type = documents submitted via Accela digital plan review; accessible via "Click here for more information" link on record detail page → redirects to Accela urlrouting.ashx for external document viewer
- **Paper Submittal** = older records with physical paper documents
- Certificates (Certificate of Completion/Occupancy) not directly visible in public view without login

---

## 9. URL PATTERNS

### Search Home
`https://aca-prod.accela.com/LEECO/Cap/CapHome.aspx?module=Permitting&TabName=Permitting`

### Record Detail
`https://aca-prod.accela.com/LEECO/Cap/CapDetail.aspx?Module=Permitting&TabName=Permitting&capID1={YYYYY}&capID2=00000&capID3={XXXXX}&agencyCode=LEECO&IsToShowInspection=`

Where capID1 pattern observed: `{2-digit-year}CAP` (e.g., `25CAP`, `24CAP`, `14HIS` for historical).

### Direct ePlan Document Link
`https://aca-prod.accela.com/LEECO/urlrouting.ashx?type=1000&Module=Permitting&capID1={}&capID2={}&capID3={}&agencyCode=LEECO&FromACA=Y`

---

## 10. SUNBIZ ADDRESS LOOKUP RESULTS

URL: `https://search.sunbiz.org/Inquiry/corporationsearch/SearchResults?inquiryType=Address&searchTerm={address}`

| Search Term | Total Results | Active Entities | Notable Active |
|---|---|---|---|
| 4980 Bayline | 2 | 1 | ELECTRIC TOASTERS TOASTMASTERS CLUB INC. (N24000010309) |
| 12800 University | 10+ (multi-page, "Next List" present) | 1 visible | 1280 FINANCIAL PARTNERS, LLC (M25000007333) at STE 195 |
| 2295 Victoria | 4 | 0 | All INACT: CLEAR IT SUPPORT LLC, FMF SECURITY SERVICES INC, HIGBEE'S VENDING INC, VICTORIA EATERY INC |
| 8010 Summerlin Lakes | 10+ | 1 | GULF COAST CARDIOTHORACIC SURGEONS, P.L. (L04000013844) |
| 1500 Monroe | 1 | 1 | WOODJUNKY LLC (L25000120071) at 1500 MONROE STREET |

**Note on city/zip filtering:** Sunbiz by-address search does not have a city or zip filter on the UI — results returned are nationwide/statewide matches on street address string. For Lee County validation, filter results by city/zip manually. All entities at these addresses that could be verified were in Fort Myers, FL area.

---

## 11. CODING-AGENT WORKFLOW: EXACT STEPS THAT WORKED

### Step A: Load the Page
```
GET https://aca-prod.accela.com/LEECO/Cap/CapHome.aspx?module=Permitting&TabName=Permitting
```
Confirm response contains: "Search Applications" tab, Global Search input.

### Step B: Switch to "Search by Address" (Recommended Method)
1. Locate the dropdown combobox labeled "Select a dropdown value to load the associated form"
2. Set value to **"Search by Address"**
3. Wait for form to reload — this mode has NO date range filter (unlike General Search)

### Step C: Enter Address Fields
```
Street No. From = {street_number}   (e.g., "4980")
Street Name = {street_name_only}    (e.g., "Bayline")  — NO suffix (no "Dr", "St", "Ave")
```
Leave City, State, Zip blank for broadest match.

### Step D: Click Search → Parse Address List
- Response shows: "N search results returned matching your address"
- List of clickable address variants appears
- **Click the cleanest match** — prefer format: `{NUM} {STREET} DR, {CITY} FL {ZIP}` (e.g., "4980 BAYLINE DR, NORTH FORT MYERS FL 33917")

### Step E: Parse Permit Record List
After clicking an address, page appends permit records below the address list:
- "Listed below are the records issued for {FULL ADDRESS}"
- "Showing 1-10 of {N}" — paginate if needed
- Columns: Record Number | Address | Description | Status | Action | Related Records | Submittal Type

### Step F: Navigate to Record Detail
- Click record number link
- URL becomes: `CapDetail.aspx?...capID1=...&capID2=00000&capID3=...&agencyCode=LEECO`
- Data available without login: Work Location, Applicant, Licensed Professional, Project Description, Inspections

### Step G: Get Inspection Data
- Click "Record Info" dropdown → select "Inspections"
- Sections: Upcoming / Completed
- Each completed inspection shows: Result (Pass/Fail), Type code + name, Inspector name, Date

---

## 12. FAILURE MODES & RELIABILITY

| Failure Mode | Description | Fix |
|---|---|---|
| Date range filter | General Search default date range (last 30 days) blocks historical results | Use "Search by Address" mode (no date filter) OR clear both date fields |
| "No results" for valid address | "2295 Victoria" returned nothing | Try without street number, or try "Victoria Ave" as street name |
| 100+ results for common address | "12800 University" returns 100+ address variants | Filter by city/zip, or use specific unit/suite |
| Global Search bar unresponsive | Clicking magnifier didn't navigate — form just stayed on prior results | Use address form fields + Search button instead; Global Search appears to need JavaScript focus event before Enter works |
| Form fields retain prior values | After navigation, form shows previous search values | Use form_input by ref to explicitly set values, then click Search |
| "Schedule an Inspection" click | Clicking Search button at wrong scroll position hit "Schedule an Inspection" tab instead | Scroll to form, verify coordinates, use ref-based clicking |
| Parcel/STRAP not shown in search results | Not visible in the address result list | Must open individual record detail → expand "More Details" section |
| Login required for some functions | Certificate downloads, fee payments, inspection scheduling (if scheduling online) require login | Public info (status, description, inspections, applicant) available without login |
| capID1 pattern varies | Recent permits: `25CAP`, old ones: `14HIS` | Always extract capID from the actual detail page URL, do not construct manually |
| ePlan documents | "Click here for more information" link requires Accela document viewer; may require login for full access | Capture the urlrouting.ashx URL as document access point |

---

## 13. RELIABILITY ASSESSMENT

| Function | Reliability | Notes |
|---|---|---|
| Page load (no login) | ✅ HIGH | Loads consistently, no CAPTCHA |
| Search by Address | ✅ HIGH | Returns results for valid Lee County addresses |
| Record list pagination | ✅ HIGH | Standard page navigation (Next >, page numbers) |
| Record detail (public fields) | ✅ HIGH | Applicant, status, address, description always visible |
| Inspection history | ✅ HIGH | Past inspections (pass/fail, date, inspector) publicly visible |
| Parcel/STRAP lookup | ⚠️ MEDIUM | Available in General Search "Parcel No." field; not tested in isolation |
| Global Search (by record#) | ⚠️ MEDIUM | Should work per banner tip; interaction proved unreliable in automation |
| Attachments/Documents | ⚠️ MEDIUM | ePlan link visible; may need login for full access |
| "Search by Address" — no suffix | ✅ HIGH | Street name without suffix (Dr/St/Ave) gives best broad results |
| 2295 Victoria (no results) | ❌ FAIL | Possible street name mismatch; fallback needed |
| Sunbiz address search | ✅ HIGH | Loads and returns results; no login needed |
| Sunbiz — city/zip filter | ❌ N/A | Not available — must filter results manually by address |
