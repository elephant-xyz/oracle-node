import { describe, expect, it } from "vitest";

import {
  POLK_PERMIT_SOURCE_REGISTRY,
  buildPolkAccelaDetailUrl,
  buildPolkPermitCandidateSql,
  buildPolkPermitEnrichmentReceipt,
  buildWinterHavenPermitSearchUrl,
  findPolkPermitSource,
  parseLakelandImsPermitDetailHtml,
  parseLakeWalesPermitDetailHtml,
  parsePolkAccelaPermitDetailHtml,
  parseWinterHavenPermitDetailHtml,
} from "../../scripts/polk/permit-enrichment.mjs";

describe("Polk permit source registry", () => {
  it("enables every independently certified anonymous adapter", () => {
    const enabled = POLK_PERMIT_SOURCE_REGISTRY.filter(
      (source) => source.adapter !== null,
    );
    expect(enabled.map((source) => source.agency)).toEqual([
      "POLK COUNTY",
      "LAKELAND",
      "WINTER HAVEN",
      "LAKE WALES",
    ]);
    expect(findPolkPermitSource("lakeland")).toMatchObject({
      status: "adapter_ready",
      adapter: "lakeland_ims_permit_detail_v1",
    });
    expect(findPolkPermitSource("winter haven")).toMatchObject({
      status: "partial_adapter_ready",
      adapter: "winter_haven_esuite_permit_detail_v1",
    });
    expect(findPolkPermitSource("haines city")).toMatchObject({
      status: "portal_verified_adapter_pending",
      adapter: null,
    });
    expect(findPolkPermitSource("unknown")).toBeNull();
  });

  it("builds the certified anonymous Accela altId lookup", () => {
    const url = new URL(buildPolkAccelaDetailUrl("BR-2021-10301"));
    expect(url.hostname).toBe("aca-prod.accela.com");
    expect(url.pathname).toBe("/POLKCO/Cap/CapDetail.aspx");
    expect(url.searchParams.get("Module")).toBe("Building");
    expect(url.searchParams.get("altId")).toBe("BR-2021-10301");
  });

  it("builds the certified Winter Haven query-parameter search", () => {
    const url = new URL(buildWinterHavenPermitSearchUrl("2025-00042002"));
    expect(url.hostname).toBe("myinspections.mywinterhaven.com");
    expect(url.searchParams.get("permitNumber")).toBe("2025-00042002");
    expect(url.searchParams.get("permitType")).toBe("-1");
    expect(url.searchParams.get("serviceAddress")).toBe("");
  });

  it("builds deterministic, agency-scoped adapter candidates", () => {
    const sql = buildPolkPermitCandidateSql(["Polk County", "O'Brien"], 25);
    expect(sql).toContain(
      "upper(trim(agency_name)) IN ('O''BRIEN', 'POLK COUNTY')",
    );
    expect(sql).toContain("LIMIT 25");
    expect(() => buildPolkPermitCandidateSql([], null)).toThrow(
      /At least one Polk permit agency/,
    );
  });
});

describe("Polk Accela detail parser", () => {
  it("promotes only visible contractor, license, parcel, status, and value evidence", () => {
    const detail = parsePolkAccelaPermitDetailHtml(`
      <html><body>
        <h1>Record BR-2021-10301:</h1>
        <h2>Residential Renovation/Addition Permit</h2>
        <div>Record Status: Closed-Complete</div>
        <h2>Work Location</h2>
        <div>3075 BAKER DAIRY RD HAINES CITY FL 33844</div>
        <h2>Record Details</h2>
        <div>Applicant: Licensed Professional:
          MARCOS DAVILA megan@mdconstructionfl.com MD CONSTRUCTION LLC
          6656 SR 544 WINTER HAVEN FL Alum Specialty Structure SCC131151708
          863-286-4248
          Project Description: New concrete slab with footings
          Owner: GRANDINETTE FRANK
        </div>
        <h2>Additional Information</h2>
        <div>Job Value($): $4,678.00</div>
        <div>Parcel Number:272722000000043010</div>
      </body></html>
    `);

    expect(detail).toMatchObject({
      permitNumber: "BR-2021-10301",
      recordType: "Residential Renovation/Addition Permit",
      recordStatus: "Closed-Complete",
      parcelIdentifier: "272722000000043010",
      workLocation: "3075 BAKER DAIRY RD HAINES CITY FL 33844",
      projectDescription: "New concrete slab with footings",
      jobValuationUsd: 4678,
      contractor: {
        businessName: "MD CONSTRUCTION LLC",
        licenseNumber: "SCC131151708",
        email: "megan@mdconstructionfl.com",
        phone: "8632864248",
      },
    });
  });

  it("normalizes colon-delimited Polk Accela license identifiers", () => {
    const detail = parsePolkAccelaPermitDetailHtml(`
      <h1>Record BR-2020-5395:</h1>
      <div>Record Status: Closed</div>
      <div>Licensed Professional:
        POWELL PHILIP JOSEPH 25383 LESLIES BUNGALOW GROUP CRC:11516516
        Project Description: Residential addition
      </div>
    `);

    expect(detail.contractor?.licenseNumber).toBe("CRC11516516");
  });

  it("does not treat fax or LLC number fragments as contractor licenses", () => {
    const faxDetail = parsePolkAccelaPermitDetailHtml(`
      <h1>Record BR-2023-11546:</h1>
      <div>Licensed Professional:
        CHRIS GREENEN builder@example.com KB HOME ORLANDO LLC
        Fax: 4075872385 Residential CRC1331406
        View Additional Licensed Professionals
        SECONDARY PERSON secondary@example.com CUSTOM PLUMBING LLC
        Plumbing CFC1432203
        Project Description: Single family residence
      </div>
    `);
    const llcDetail = parsePolkAccelaPermitDetailHtml(`
      <h1>Record BT-2026-8420:</h1>
      <div>Licensed Professional:
        John Douglas Anderson GENERX GENERATORS, LLC 13081
        Electric With Alarm EC13015062
        Project Description: Install generator
      </div>
    `);

    expect(faxDetail.contractor).toMatchObject({
      businessName: "KB HOME ORLANDO LLC",
      licenseNumber: "CRC1331406",
      licenseType: "Residential",
    });
    expect(llcDetail.contractor).toMatchObject({
      businessName: "GENERX GENERATORS, LLC",
      licenseNumber: "EC13015062",
      licenseType: "Electric With Alarm",
    });
  });

  it("keeps absent detail fields null", () => {
    expect(
      parsePolkAccelaPermitDetailHtml("<h1>No records found</h1>"),
    ).toEqual({
      permitNumber: null,
      recordType: null,
      recordStatus: null,
      parcelIdentifier: null,
      workLocation: null,
      projectDescription: null,
      jobValuationUsd: null,
      contractor: null,
    });
  });
});

describe("Polk municipal permit parsers", () => {
  it("extracts Lakeland trade contractors and Florida licenses", () => {
    const detail = parseLakelandImsPermitDetailHtml(
      `
        <h1>Permit BLD24-01247</h1>
        <div>Type: Building Commercial | New Status: Issued</div>
        <div>Address: 4730 FLORIDA AVE S Description: Commercial build</div>
        <div>Valuation: $2,500,000</div>
        <div>Building Contractor: MCINTYRE ELWELL & STRAM (CBC1258177)</div>
      `,
      "BLD24-01247",
    );
    expect(detail).toMatchObject({
      permitNumber: "BLD24-01247",
      recordType: "Building Commercial | New",
      recordStatus: "Issued",
      workLocation: "4730 FLORIDA AVE S",
      jobValuationUsd: 2500000,
      contractor: {
        businessName: "MCINTYRE ELWELL & STRAM",
        licenseNumber: "CBC1258177",
      },
    });
  });

  it("keeps Winter Haven contractor evidence null", () => {
    const detail = parseWinterHavenPermitDetailHtml(
      `
        <h1>Permit Number: 2025-00042002</h1>
        <div>Permit Type: Building Status: Permit Issued on 02/06/2025</div>
        <div>Address: 6508 RAINTREE LN NE Description: Interior remodel</div>
        <div>Valuation: $45,000</div>
        <div>Issued To: Contractor</div>
      `,
      "2025-00042002",
    );
    expect(detail).toMatchObject({
      permitNumber: "2025-00042002",
      recordType: "Building",
      recordStatus: "Permit Issued on 02/06/2025",
      contractor: null,
    });
  });

  it("keeps Lake Wales municipal contractor numbers separate from licenses", () => {
    const detail = parseLakeWalesPermitDetailHtml(
      `
        <h1>Permit Number: 202401586</h1>
        <div>Type: Mechanical Status: Issued</div>
        <div>Address: 451 EAGLE RIDGE DR Description: HVAC replacement</div>
        <div>General Contractor: 3139/NEXTECH Class: LICENSED MECHANICAL CONTRACTOR Status: ACTIVE</div>
      `,
      "202401586",
    );
    expect(detail).toMatchObject({
      permitNumber: "202401586",
      contractor: {
        businessName: "NEXTECH",
        licenseNumber: null,
        licenseType: "LICENSED MECHANICAL CONTRACTOR",
      },
    });
  });
});

describe("Polk permit enrichment receipt", () => {
  it("stays incomplete until all certified source rows are attempted", () => {
    const receipt = buildPolkPermitEnrichmentReceipt(
      {
        permitCount: 3,
        agencies: [
          { value: "POLK COUNTY", count: 2 },
          { value: "LAKELAND", count: 1 },
        ],
      },
      [
        {
          permitNumber: "BR-1",
          agency: "POLK COUNTY",
          sourceKey: "polk_county_accela",
          sourceUrl: buildPolkAccelaDetailUrl("BR-1"),
          status: "enriched",
          detail: {
            permitNumber: "BR-1",
            recordType: "Re-Roof Permit",
            recordStatus: "Issued",
            parcelIdentifier: "1",
            workLocation: "1 MAIN ST",
            projectDescription: "Reroof",
            jobValuationUsd: 10000,
            contractor: {
              businessName: "ROOF CO LLC",
              contactName: null,
              licenseNumber: "CCC1234567",
              licenseType: "Roofing",
              email: null,
              phone: null,
              raw: "ROOF CO LLC Roofing CCC1234567",
            },
          },
          error: null,
          retrievedAt: "2026-08-31T00:00:00.000Z",
        },
      ],
    );

    expect(receipt).toMatchObject({
      officialPermitCount: 3,
      adapterEligiblePermitCount: 3,
      unsupportedPermitCount: 0,
      attemptedAdapterRecords: 1,
      contractorEvidenceCount: 1,
      licenseEvidenceCount: 1,
      complete: false,
    });
    expect(receipt.blocker).toMatch(/2 adapter-eligible permit rows/);
  });

  it("stays incomplete when a certified adapter returns no detail evidence", () => {
    const receipt = buildPolkPermitEnrichmentReceipt(
      {
        permitCount: 1,
        agencies: [{ value: "POLK COUNTY", count: 1 }],
      },
      [
        {
          permitNumber: "BR-MISSING",
          agency: "POLK COUNTY",
          sourceKey: "polk_county_accela",
          sourceUrl: buildPolkAccelaDetailUrl("BR-MISSING"),
          status: "no_detail",
          detail: null,
          error: null,
          retrievedAt: "2026-08-31T00:00:00.000Z",
        },
      ],
    );

    expect(receipt.complete).toBe(false);
    expect(receipt.blocker).toMatch(/1 adapter-eligible permit row/);
  });
});
